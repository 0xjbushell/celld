// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The engine's single S3 client: the `object_store` crate `celld-ltx`
//! already links, bound to one bucket (wiki/designs/s3-client-dedup.md).
//! Replaces aws-sdk-s3. No call site streamed a body, so everything is
//! in-memory `Bytes`.
//!
//! Error contract, relied on by the self-fence: `put_cas` answers
//! `Ok(None)` only for a clean 412/409 rejection; every other failure is
//! ambiguous — the write may have committed — and surfaces as `Err`.

use anyhow::anyhow;
use anyhow::Context;
use bytes::Bytes;
use futures_util::StreamExt;
use object_store::aws::AmazonS3;
use object_store::aws::AmazonS3Builder;
use object_store::aws::S3ConditionalPut;
use object_store::path::Path;
use object_store::Attribute;
use object_store::Attributes;
use object_store::ClientOptions;
use object_store::Error;
use object_store::GetOptions;
use object_store::ObjectMeta;
use object_store::ObjectStore;
use object_store::PutMode;
use object_store::PutOptions;
use object_store::PutPayload;
use object_store::RetryConfig;
use object_store::UpdateVersion;
use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

/// Explicit credentials for a managed installation; everything else comes
/// from the standard `AWS_*` environment.
pub struct StaticCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

/// One S3-compatible bucket. Cheap to clone; each `open` builds its own
/// HTTP transport, so a dedicated instance also isolates its traffic.
#[derive(Clone)]
pub struct Bucket {
    store: Arc<AmazonS3>,
    /// Conditional writes only, built with retries OFF: a retried CAS put
    /// can land on the first attempt's own etag change and report a clean
    /// 412 — converting "may have committed" into a false rejection. The
    /// ambiguity must surface as `Err` so the caller reconciles.
    cas_store: Arc<AmazonS3>,
    /// Bucket name, for messages — the store is already bound to it.
    pub name: String,
}

impl Bucket {
    /// `app` labels this client's traffic in the User-Agent (the aws
    /// AppName format, `app/<name>`), keeping e.g. the lease safety lane
    /// observable in black-box storage traces.
    pub fn open(
        bucket: &str,
        endpoint: Option<&str>,
        region: &str,
        credentials: Option<StaticCredentials>,
        app: Option<&str>,
    ) -> anyhow::Result<Bucket> {
        // These bounds mirror the aws-sdk TimeoutConfig they replace
        // (connect 3 s / attempt 15 s / operation 30 s) — a correctness
        // condition for the node self-fence, not tuning. The read-timeout
        // knob collapses into the per-request bound.
        let mut options = ClientOptions::new()
            .with_timeout(Duration::from_secs(15))
            .with_connect_timeout(Duration::from_secs(3))
            .with_allow_http(true);
        if let Some(app) = app {
            options = options.with_user_agent(
                hyper::header::HeaderValue::from_str(&format!("celld app/{app}"))
                    .context("app user agent")?,
            );
        }
        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .with_region(region)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .with_retry(RetryConfig {
                max_retries: 2,
                retry_timeout: Duration::from_secs(30),
                ..RetryConfig::default()
            })
            .with_client_options(options);
        if let Some(endpoint) = endpoint {
            // Path-style against explicit S3-compatible endpoints, exactly
            // as the aws client's force_path_style(endpoint.is_some()).
            builder = builder
                .with_endpoint(endpoint)
                .with_virtual_hosted_style_request(false);
        } else {
            builder = builder.with_virtual_hosted_style_request(true);
        }
        if let Some(credentials) = credentials {
            builder = builder
                .with_access_key_id(credentials.access_key_id)
                .with_secret_access_key(credentials.secret_access_key);
            if let Some(token) = credentials.session_token {
                builder = builder.with_token(token);
            }
        }
        let cas_builder = builder.clone().with_retry(RetryConfig {
            max_retries: 0,
            retry_timeout: Duration::from_secs(30),
            ..RetryConfig::default()
        });
        Ok(Bucket {
            store: Arc::new(builder.build().context("build s3 client")?),
            cas_store: Arc::new(cas_builder.build().context("build s3 cas client")?),
            name: bucket.to_string(),
        })
    }

    /// Body and etag, or `None` when the key does not exist.
    pub async fn get(&self, key: &str) -> anyhow::Result<Option<(Bytes, String)>> {
        match self.store.get(&Path::from(key)).await {
            Ok(result) => {
                let etag = result.meta.e_tag.clone().unwrap_or_default();
                let bytes = result
                    .bytes()
                    .await
                    .with_context(|| format!("read body s3://{}/{key}", self.name))?;
                Ok(Some((bytes, etag)))
            }
            Err(Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(anyhow!(error).context(format!("read s3://{}/{key}", self.name))),
        }
    }

    /// Size and etag, or `None` when the key does not exist.
    pub async fn head(&self, key: &str) -> anyhow::Result<Option<(u64, String)>> {
        match self.store.head(&Path::from(key)).await {
            Ok(meta) => Ok(Some((meta.size as u64, meta.e_tag.unwrap_or_default()))),
            Err(Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(anyhow!(error).context(format!("head s3://{}/{key}", self.name))),
        }
    }

    pub async fn put(&self, key: &str, body: impl Into<PutPayload>) -> anyhow::Result<()> {
        self.store
            .put(&Path::from(key), body.into())
            .await
            .with_context(|| format!("write s3://{}/{key}", self.name))?;
        Ok(())
    }

    /// Size plus one `x-amz-meta-*` value, or `None` when the key does not
    /// exist. A plain `head` cannot see user metadata; this one can.
    pub async fn head_with_meta(
        &self,
        key: &str,
        name: &str,
    ) -> anyhow::Result<Option<(u64, Option<String>)>> {
        let options = GetOptions {
            head: true,
            ..GetOptions::default()
        };
        match self.store.get_opts(&Path::from(key), options).await {
            Ok(result) => {
                let value = result
                    .attributes
                    .get(&Attribute::Metadata(name.to_string().into()))
                    .map(|value| value.as_ref().to_string());
                Ok(Some((result.meta.size as u64, value)))
            }
            Err(Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(anyhow!(error).context(format!("head s3://{}/{key}", self.name))),
        }
    }

    /// Plain write carrying `x-amz-meta-*` user metadata.
    pub async fn put_with_meta(
        &self,
        key: &str,
        body: impl Into<PutPayload>,
        meta: &[(&'static str, &str)],
    ) -> anyhow::Result<()> {
        let mut attributes = Attributes::new();
        for (name, value) in meta {
            attributes.insert(
                Attribute::Metadata(Cow::Borrowed(name)),
                value.to_string().into(),
            );
        }
        let options = PutOptions {
            attributes,
            ..PutOptions::default()
        };
        self.store
            .put_opts(&Path::from(key), body.into(), options)
            .await
            .with_context(|| format!("write s3://{}/{key}", self.name))?;
        Ok(())
    }

    /// Conditional write. `etag: None` requires the key to be absent
    /// (If-None-Match: *); `Some` requires the current etag (If-Match).
    /// `Ok(Some(new_etag))` applied, `Ok(None)` cleanly rejected; any other
    /// failure is ambiguous and stays an error.
    pub async fn put_cas(
        &self,
        key: &str,
        body: impl Into<PutPayload>,
        etag: Option<&str>,
    ) -> anyhow::Result<Option<String>> {
        let mode = match etag {
            None => PutMode::Create,
            Some(etag) => PutMode::Update(UpdateVersion {
                e_tag: Some(etag.to_string()),
                version: None,
            }),
        };
        match self
            .cas_store
            .put_opts(&Path::from(key), body.into(), PutOptions::from(mode))
            .await
        {
            Ok(result) => Ok(Some(result.e_tag.unwrap_or_default())),
            Err(Error::Precondition { .. } | Error::AlreadyExists { .. }) => Ok(None),
            Err(error) => Err(anyhow!(error).context(format!(
                "conditional write s3://{}/{key} may have committed",
                self.name
            ))),
        }
    }

    /// Idempotent: deleting an absent key succeeds, as S3's DELETE does.
    pub async fn delete(&self, key: &str) -> anyhow::Result<()> {
        match self.store.delete(&Path::from(key)).await {
            Ok(()) | Err(Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(anyhow!(error).context(format!("delete s3://{}/{key}", self.name))),
        }
    }

    /// Every object under `prefix/`; the client paginates internally.
    pub async fn list(&self, prefix: &str) -> anyhow::Result<Vec<ObjectMeta>> {
        let path = Path::from(prefix.trim_end_matches('/'));
        let mut stream = self.store.list(Some(&path));
        let mut objects = Vec::new();
        while let Some(meta) = stream.next().await {
            objects.push(meta.with_context(|| format!("list s3://{}/{prefix}", self.name))?);
        }
        Ok(objects)
    }

    /// Does anything exist under `prefix/`? One page at most.
    pub async fn list_any(&self, prefix: &str) -> anyhow::Result<bool> {
        let path = Path::from(prefix.trim_end_matches('/'));
        match self.store.list(Some(&path)).next().await {
            None => Ok(false),
            Some(Ok(_)) => Ok(true),
            Some(Err(error)) => {
                Err(anyhow!(error).context(format!("list s3://{}/{prefix}", self.name)))
            }
        }
    }

    /// Immediate child "directories" under `prefix/` (delimiter listing),
    /// as full prefixes with the trailing slash stripped.
    pub async fn common_prefixes(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        let path = Path::from(prefix.trim_end_matches('/'));
        let result = self
            .store
            .list_with_delimiter(Some(&path))
            .await
            .with_context(|| format!("list s3://{}/{prefix}", self.name))?;
        Ok(result
            .common_prefixes
            .into_iter()
            .map(|p| p.as_ref().to_string())
            .collect())
    }

    /// The head_bucket replacement: prove the bucket is reachable and the
    /// credential is accepted with one list page.
    pub async fn validate(&self) -> anyhow::Result<()> {
        match self.store.list(None).next().await {
            None | Some(Ok(_)) => Ok(()),
            Some(Err(error)) => Err(anyhow!(error).context(format!("validate s3://{}", self.name))),
        }
    }
}

/// Was this a 401/403 — the credential itself rejected? Used by the managed
/// path to report a revoked credential rather than a flaky bucket.
pub fn is_unauthorized(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        if matches!(
            cause.downcast_ref::<Error>(),
            Some(Error::PermissionDenied { .. } | Error::Unauthenticated { .. })
        ) {
            return true;
        }
        // The list path wraps HTTP errors as Generic; the status only
        // survives in the retry error's message.
        let text = cause.to_string();
        text.contains("status 403") || text.contains("status 401")
    })
}

#[cfg(test)]
mod live_cas {
    use super::{Bucket, StaticCredentials};

    // Live CAS contract against a real S3-compatible bucket (R2). Gated on
    // CELLD_CAS_LIVE=1 so it never runs in CI; a mock cannot answer whether
    // object_store maps R2's precondition failures to Ok(None) (the fencing
    // contract) rather than Err. Run:
    //   CELLD_CAS_LIVE=1 CELLD_CAS_BUCKET=<b> CELLD_CAS_ENDPOINT=<ep> AWS_*=... \
    //     cargo test -p celld put_cas_contract -- --nocapture
    #[tokio::test]
    async fn put_cas_contract_against_real_bucket() {
        if std::env::var("CELLD_CAS_LIVE").as_deref() != Ok("1") {
            return;
        }
        let name = std::env::var("CELLD_CAS_BUCKET").expect("CELLD_CAS_BUCKET");
        let endpoint = std::env::var("CELLD_CAS_ENDPOINT").ok();
        let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "auto".into());
        let creds = StaticCredentials {
            access_key_id: std::env::var("AWS_ACCESS_KEY_ID").unwrap(),
            secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY").unwrap(),
            session_token: std::env::var("AWS_SESSION_TOKEN").ok(),
        };
        let bucket = Bucket::open(
            &name,
            endpoint.as_deref(),
            &region,
            Some(creds),
            Some("cas-test"),
        )
        .expect("open bucket");
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let key = format!("cas-probe/{nanos}");

        // 1. Create on an absent key applies.
        let e1 = bucket
            .put_cas(&key, b"v1".to_vec(), None)
            .await
            .expect("create must not error")
            .expect("fresh create must apply (Ok(Some))");
        // 2. Create over an existing key is cleanly rejected.
        assert!(
            bucket
                .put_cas(&key, b"v1b".to_vec(), None)
                .await
                .expect("create-again must not error")
                .is_none(),
            "create over an existing key must be Ok(None)"
        );
        // 3. Update with the current etag applies.
        bucket
            .put_cas(&key, b"v3".to_vec(), Some(&e1))
            .await
            .expect("update must not error")
            .expect("update with current etag must apply (Ok(Some))");
        // 4. Update with the now-stale etag is cleanly rejected — the fencing case.
        assert!(
            bucket
                .put_cas(&key, b"v4".to_vec(), Some(&e1))
                .await
                .expect("stale update must not error")
                .is_none(),
            "update with a stale etag must be Ok(None) — the fencing contract"
        );
        bucket.delete(&key).await.expect("cleanup delete");
        eprintln!("CAS verified on {name}: create / reject-create / update / reject-stale");
    }
}
