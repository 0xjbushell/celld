// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The engine's single object-store client: the `object_store` crate
//! `celld-ltx` already links, bound to one bucket. Replaces aws-sdk-s3.
//! No call site streamed a body, so everything is in-memory `Bytes`.
//!
//! Two conditional-write dialects share the surface. An `s3://` (or
//! bare) spec speaks the S3 dialect: the CAS token is the etag, sent as
//! If-Match / If-None-Match with SigV4 credentials. A `gs://` spec
//! speaks the Cloud Storage XML API dialect: the CAS token is the
//! object generation, sent as x-goog-if-generation-match with OAuth
//! credentials. The distinction is the dialect, not the endpoint — GCS
//! accepts S3-style requests on the same host but does not apply
//! If-Match to a PUT, so only the generation dialect can fence there.
//! Callers never see the difference: the token is an opaque `String` a
//! read answers and a conditional write consumes.
//!
//! Error contract, relied on by the self-fence: `put_cas` answers
//! `Ok(None)` only for a clean 412/409 rejection; every other failure is
//! ambiguous — the write may have committed — and surfaces as `Err`.
//! In the same spirit a response that carries no CAS token is an error,
//! never an empty token a later conditional write would trust.

use crate::azure_identity::AzureTokenCredentialProvider;
use anyhow::anyhow;
use anyhow::Context;
use azure_core::credentials::{Secret, TokenCredential};
use azure_identity::{
    ClientSecretCredential, DeveloperToolsCredential, ManagedIdentityCredential,
    ManagedIdentityCredentialOptions, UserAssignedId, WorkloadIdentityCredential,
    WorkloadIdentityCredentialOptions,
};
use bytes::Bytes;
use futures_util::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::aws::S3ConditionalPut;
use object_store::azure::{AzureCredentialProvider, MicrosoftAzureBuilder};
use object_store::gcp::GoogleCloudStorageBuilder;
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
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

fn ordinary_retry_config() -> RetryConfig {
    RetryConfig {
        max_retries: 2,
        retry_timeout: Duration::from_secs(30),
        ..RetryConfig::default()
    }
}

fn cas_retry_config() -> RetryConfig {
    RetryConfig {
        max_retries: 0,
        retry_timeout: Duration::from_secs(30),
        ..RetryConfig::default()
    }
}

/// Explicit credentials for a managed installation; everything else comes
/// from the standard `AWS_*` environment.
#[derive(Clone)]
pub struct StaticCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

/// Which conditional-write dialect the bucket speaks, and therefore
/// what the opaque CAS token holds: the etag on S3, the object
/// generation on GCS. GCS ignores etags on writes, so its tokens must
/// come from the generation everywhere — reads, heads, and put results.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StorageBackend {
    S3,
    Gcs,
    Azure,
}

impl StorageBackend {
    fn scheme(self) -> &'static str {
        match self {
            StorageBackend::S3 => "s3",
            StorageBackend::Gcs => "gs",
            StorageBackend::Azure => "az",
        }
    }

    /// The CAS token a read or applied write answers: etag or generation,
    /// per dialect. A response without one cannot be fenced against, so a
    /// missing or empty token is an error — never an empty string a later
    /// conditional write would send as a real precondition.
    fn token(self, e_tag: Option<String>, version: Option<String>) -> anyhow::Result<String> {
        let (token, header) = match self {
            StorageBackend::S3 | StorageBackend::Azure => (e_tag, "ETag"),
            StorageBackend::Gcs => (version, "x-goog-generation"),
        };
        match token {
            Some(token) if !token.is_empty() => Ok(token),
            _ => Err(anyhow!(
                "response carries no {header}, so there is no CAS token"
            )),
        }
    }

    /// The precondition a conditional update sends for a held token.
    fn update(self, token: &str) -> UpdateVersion {
        match self {
            StorageBackend::S3 | StorageBackend::Azure => UpdateVersion {
                e_tag: Some(token.to_string()),
                version: None,
            },
            StorageBackend::Gcs => UpdateVersion {
                e_tag: None,
                version: Some(token.to_string()),
            },
        }
    }

    fn is_clean_cas_rejection(self, error: &Error, create: bool) -> bool {
        if self != StorageBackend::Azure {
            return matches!(
                error,
                Error::Precondition { .. } | Error::AlreadyExists { .. }
            );
        }
        let source = match error {
            Error::Precondition { source, .. } | Error::AlreadyExists { source, .. } => source,
            _ => return false,
        };
        // object_store 0.11 classifies Azure errors by status and boxes its
        // private retry error. The 4xx XML body, including Code, survives in
        // that source's Display text; if it ever does not, fail ambiguous.
        let text = source.to_string();
        let Some(start) = text.find("<Code>") else {
            return false;
        };
        let code = &text[start + "<Code>".len()..];
        let Some(end) = code.find("</Code>") else {
            return false;
        };
        matches!(
            (create, &code[..end]),
            (true, "BlobAlreadyExists" | "ConditionNotMet") | (false, "ConditionNotMet")
        )
    }
}

/// One object-store bucket, optionally scoped to a key prefix. Cheap to
/// clone; each `open` builds its own HTTP transport, so a dedicated
/// instance also isolates its traffic.
#[derive(Clone)]
pub struct Bucket {
    store: Arc<dyn ObjectStore>,
    /// Conditional writes only, built with retries OFF: a retried CAS put
    /// can land on the first attempt's own token change and report a clean
    /// 412 — converting "may have committed" into a false rejection. The
    /// ambiguity must surface as `Err` so the caller reconciles.
    cas_store: Arc<dyn ObjectStore>,
    backend: StorageBackend,
    /// Bucket name, for messages — the store is already bound to it.
    pub name: String,
    /// Empty, or a slash-terminated key prefix every operation is scoped
    /// to. Call sites keep forming unprefixed keys; this type is the one
    /// place that knows where in the bucket a fleet lives.
    pub prefix: String,
}

/// Split a `[s3://|gs://|az://]NAME[/PREFIX]` bucket spec into the backend, the
/// bucket name and a normalized key prefix: empty, or slash-terminated. A
/// spec without a scheme stays S3-compatible, and a spec without a PREFIX
/// keeps every key at the bucket root, so a fleet provisioned before
/// either existed never moves its objects.
fn split_spec(spec: &str) -> (StorageBackend, &str, String) {
    let (backend, spec) = match spec.strip_prefix("gs://") {
        Some(rest) => (StorageBackend::Gcs, rest),
        None => match spec.strip_prefix("az://") {
            Some(rest) => (StorageBackend::Azure, rest),
            None => (StorageBackend::S3, spec.trim_start_matches("s3://")),
        },
    };
    let (name, prefix) = spec.split_once('/').unwrap_or((spec, ""));
    let parts = prefix.split('/').filter(|part| !part.is_empty());
    (
        backend,
        name,
        parts.map(|part| format!("{part}/")).collect(),
    )
}

fn nonempty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn parse_sas_authorization(sas: &str) -> anyhow::Result<Vec<(String, String)>> {
    let sas = sas.strip_prefix('?').unwrap_or(sas);
    if sas.is_empty() {
        anyhow::bail!("AZURE_STORAGE_SAS_KEY must contain query pairs");
    }
    sas.split('&')
        .map(|pair| {
            let (key, value) = pair
                .split_once('=')
                .ok_or_else(|| anyhow!("invalid SAS query pair: {pair:?}"))?;
            if key.is_empty() {
                anyhow::bail!("invalid SAS query pair with an empty key");
            }
            Ok((decode_sas_component(key)?, decode_sas_component(value)?))
        })
        .collect()
}

fn decode_sas_component(component: &str) -> anyhow::Result<String> {
    let bytes = component.as_bytes();
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b'%'
            && (index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit())
        {
            anyhow::bail!("invalid percent encoding in SAS query pair");
        }
    }
    percent_encoding::percent_decode_str(component)
        .decode_utf8()
        .map(Cow::into_owned)
        .map_err(|error| anyhow!("invalid UTF-8 in SAS query pair: {error}"))
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum AzureIdentityMode {
    ManagedIdentity {
        client_id: Option<String>,
    },
    WorkloadIdentity {
        tenant_id: String,
        client_id: String,
        federated_token_file: String,
    },
    ClientSecret {
        tenant_id: String,
        client_id: String,
        client_secret: String,
    },
    DeveloperTools,
}

#[derive(Clone, Debug)]
enum AzureAuth {
    Emulator,
    AccountKey(String),
    Sas(String),
    Identity(AzureIdentityMode),
}

const WORKLOAD_IDENTITY_REQUIREMENTS: &str =
    "AZURE_TENANT_ID, AZURE_CLIENT_ID, and AZURE_FEDERATED_TOKEN_FILE";
const CLIENT_SECRET_REQUIREMENTS: &str =
    "AZURE_TENANT_ID, AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET";

#[derive(Debug)]
struct AzureConfig {
    account: Option<String>,
    endpoint: Option<String>,
    auth: AzureAuth,
}

trait AzureIdentityFactory {
    fn create(&self, mode: &AzureIdentityMode) -> anyhow::Result<Arc<dyn TokenCredential>>;
}

#[derive(Debug)]
struct OfficialAzureIdentityFactory;

fn managed_identity_options(client_id: Option<String>) -> ManagedIdentityCredentialOptions {
    ManagedIdentityCredentialOptions {
        user_assigned_id: client_id.map(UserAssignedId::ClientId),
        ..Default::default()
    }
}

impl AzureIdentityFactory for OfficialAzureIdentityFactory {
    fn create(&self, mode: &AzureIdentityMode) -> anyhow::Result<Arc<dyn TokenCredential>> {
        let credential: Arc<dyn TokenCredential> = match mode {
            AzureIdentityMode::ManagedIdentity { client_id } => {
                ManagedIdentityCredential::new(Some(managed_identity_options(client_id.clone())))
                    .context("construct managed identity credential")?
            }
            AzureIdentityMode::WorkloadIdentity {
                tenant_id,
                client_id,
                federated_token_file,
            } => {
                let options = WorkloadIdentityCredentialOptions {
                    tenant_id: Some(tenant_id.clone()),
                    client_id: Some(client_id.clone()),
                    token_file_path: Some(PathBuf::from(federated_token_file)),
                    ..Default::default()
                };
                WorkloadIdentityCredential::new(Some(options))
                    .context("construct workload identity credential")?
            }
            AzureIdentityMode::ClientSecret {
                tenant_id,
                client_id,
                client_secret,
            } => ClientSecretCredential::new(
                tenant_id,
                client_id.clone(),
                Secret::new(client_secret.clone()),
                None,
            )
            .context("construct client secret credential")?,
            AzureIdentityMode::DeveloperTools => DeveloperToolsCredential::new(None)
                .context("construct developer-tools credential")?,
        };
        Ok(credential)
    }
}

fn credentials_for_auth(
    auth: &AzureAuth,
    factory: &dyn AzureIdentityFactory,
) -> anyhow::Result<Option<AzureCredentialProvider>> {
    let AzureAuth::Identity(mode) = auth else {
        return Ok(None);
    };
    Ok(Some(AzureTokenCredentialProvider::new(
        factory.create(mode)?,
    )))
}

/// Resolve Azure's selected configuration without allowing environment values
/// from another provider or authorization mode to become implicit fallbacks.
fn resolve_azure_config(
    explicit_endpoint: Option<&str>,
    get: impl Fn(&str) -> Option<String>,
) -> anyhow::Result<AzureConfig> {
    let nonempty = |name| get(name).filter(|value| !value.trim().is_empty());
    if nonempty("S3_ENDPOINT").is_some() {
        anyhow::bail!(
            "an az:// bucket cannot use S3_ENDPOINT; unset S3_ENDPOINT and use AZURE_STORAGE_ENDPOINT"
        );
    }

    let emulator = match get("AZURE_STORAGE_USE_EMULATOR") {
        Some(value) if value == "1" => true,
        Some(value) if value == "0" || value.trim().is_empty() => false,
        Some(_) => anyhow::bail!("AZURE_STORAGE_USE_EMULATOR accepts only 0 or 1"),
        None => false,
    };
    let configured_endpoint = nonempty("AZURE_STORAGE_ENDPOINT");
    if emulator {
        if explicit_endpoint.is_some() || configured_endpoint.is_some() {
            anyhow::bail!(
                "Azurite ignores --endpoint and AZURE_STORAGE_ENDPOINT; use AZURITE_BLOB_STORAGE_URL"
            );
        }
        if nonempty("CELLD_AZURE_AUTH").is_some() {
            anyhow::bail!("CELLD_AZURE_AUTH must be unset when AZURE_STORAGE_USE_EMULATOR=1");
        }
        return Ok(AzureConfig {
            account: nonempty("AZURE_STORAGE_ACCOUNT_NAME"),
            endpoint: None,
            auth: AzureAuth::Emulator,
        });
    }

    let account = nonempty("AZURE_STORAGE_ACCOUNT_NAME")
        .ok_or_else(|| anyhow!("AZURE_STORAGE_ACCOUNT_NAME is required for an az:// bucket"))?;
    let auth = nonempty("CELLD_AZURE_AUTH")
        .ok_or_else(|| anyhow!("CELLD_AZURE_AUTH is required for an az:// bucket"))?;
    let auth = match auth.as_str() {
        "account-key" => AzureAuth::AccountKey(nonempty("AZURE_STORAGE_ACCOUNT_KEY").ok_or_else(
            || anyhow!("AZURE_STORAGE_ACCOUNT_KEY is required when CELLD_AZURE_AUTH=account-key"),
        )?),
        "sas" => AzureAuth::Sas(nonempty("AZURE_STORAGE_SAS_KEY").ok_or_else(|| {
            anyhow!("AZURE_STORAGE_SAS_KEY is required when CELLD_AZURE_AUTH=sas")
        })?),
        "managed-identity" => AzureAuth::Identity(AzureIdentityMode::ManagedIdentity {
            client_id: nonempty("AZURE_CLIENT_ID"),
        }),
        "workload-identity" => AzureAuth::Identity(AzureIdentityMode::WorkloadIdentity {
            tenant_id: nonempty("AZURE_TENANT_ID").ok_or_else(|| {
                anyhow!(
                    "{WORKLOAD_IDENTITY_REQUIREMENTS} are required when CELLD_AZURE_AUTH=workload-identity"
                )
            })?,
            client_id: nonempty("AZURE_CLIENT_ID").ok_or_else(|| {
                anyhow!(
                    "{WORKLOAD_IDENTITY_REQUIREMENTS} are required when CELLD_AZURE_AUTH=workload-identity"
                )
            })?,
            federated_token_file: nonempty("AZURE_FEDERATED_TOKEN_FILE").ok_or_else(|| {
                anyhow!(
                    "{WORKLOAD_IDENTITY_REQUIREMENTS} are required when CELLD_AZURE_AUTH=workload-identity"
                )
            })?,
        }),
        "client-secret" => AzureAuth::Identity(AzureIdentityMode::ClientSecret {
            tenant_id: nonempty("AZURE_TENANT_ID").ok_or_else(|| {
                anyhow!(
                    "{CLIENT_SECRET_REQUIREMENTS} are required when CELLD_AZURE_AUTH=client-secret"
                )
            })?,
            client_id: nonempty("AZURE_CLIENT_ID").ok_or_else(|| {
                anyhow!(
                    "{CLIENT_SECRET_REQUIREMENTS} are required when CELLD_AZURE_AUTH=client-secret"
                )
            })?,
            client_secret: nonempty("AZURE_CLIENT_SECRET").ok_or_else(|| {
                anyhow!(
                    "{CLIENT_SECRET_REQUIREMENTS} are required when CELLD_AZURE_AUTH=client-secret"
                )
            })?,
        }),
        "developer-tools" => AzureAuth::Identity(AzureIdentityMode::DeveloperTools),
        _ => anyhow::bail!(
            "CELLD_AZURE_AUTH must be account-key, sas, managed-identity, workload-identity, client-secret, or developer-tools"
        ),
    };
    Ok(AzureConfig {
        account: Some(account),
        endpoint: explicit_endpoint
            .map(ToOwned::to_owned)
            .or(configured_endpoint),
        auth,
    })
}

/// Resolve the provider-owned endpoint environment after the shared parser has
/// selected a backend. An explicit CLI endpoint wins only for that provider.
pub fn endpoint_for_spec(spec: &str, explicit: Option<String>) -> anyhow::Result<Option<String>> {
    match split_spec(spec).0 {
        StorageBackend::S3 => Ok(explicit.or_else(|| nonempty_env("S3_ENDPOINT"))),
        StorageBackend::Gcs => Ok(explicit.or_else(|| nonempty_env("S3_ENDPOINT"))),
        StorageBackend::Azure => {
            if nonempty_env("S3_ENDPOINT").is_some() {
                anyhow::bail!(
                    "an az:// bucket cannot use S3_ENDPOINT; unset S3_ENDPOINT and use AZURE_STORAGE_ENDPOINT"
                );
            }
            Ok(explicit.or_else(|| nonempty_env("AZURE_STORAGE_ENDPOINT")))
        }
    }
}

/// Normalize an explicit control-plane bucket after the shared parser has
/// selected its provider. The managed service owns a whole S3 bucket; it
/// cannot issue credentials for another provider or for a key prefix.
pub fn control_plane_bucket(spec: &str) -> anyhow::Result<String> {
    let (backend, name, prefix) = split_spec(spec);
    if backend != StorageBackend::S3 {
        anyhow::bail!(
            "--control-plane storage is S3-compatible; {}:// storage is unsupported. Run without --control-plane.",
            backend.scheme()
        );
    }
    if !prefix.is_empty() {
        anyhow::bail!("--control-plane does not accept a --bucket prefix");
    }
    Ok(name.to_string())
}

pub fn scheme_for_spec(spec: &str) -> &'static str {
    split_spec(spec).0.scheme()
}

impl Bucket {
    /// `bucket` is `[s3://|gs://|az://]NAME[/PREFIX]`. With a PREFIX every key
    /// this client reads or writes lives under `PREFIX/`, so several
    /// fleets can share one bucket without colliding.
    ///
    /// A `gs://` bucket authenticates through Google Application Default
    /// Credentials (or the `GOOGLE_*` service-account environment) and
    /// takes no S3 endpoint, static credentials, or region — the bucket
    /// carries its own location.
    ///
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
        Self::open_with_gcs_builder(
            bucket,
            endpoint,
            region,
            credentials,
            app,
            GoogleCloudStorageBuilder::from_env(),
        )
    }

    /// The body of [`Self::open`], taking the base GCS builder instead of
    /// deriving it from the `GOOGLE_*` environment. A caller that passes
    /// an explicit builder is independent of that environment.
    fn open_with_gcs_builder(
        bucket: &str,
        endpoint: Option<&str>,
        region: &str,
        credentials: Option<StaticCredentials>,
        app: Option<&str>,
        gcs_builder: GoogleCloudStorageBuilder,
    ) -> anyhow::Result<Bucket> {
        let (backend, bucket, prefix) = split_spec(bucket);
        // The prefix is spliced into keys as plain text and stripped off
        // listed keys the same way. A character `object_store` would
        // percent-encode would make the two disagree, so refuse it here
        // rather than mis-parse every listing later.
        let illegal = |c: char| !c.is_ascii_alphanumeric() && !"-_./".contains(c);
        if prefix.contains(illegal) {
            anyhow::bail!("bucket prefix accepts only letters, digits and -_./: {prefix:?}");
        }
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
        let retry = ordinary_retry_config();
        let cas_retry = cas_retry_config();
        let (store, cas_store): (Arc<dyn ObjectStore>, Arc<dyn ObjectStore>) = match backend {
            StorageBackend::S3 => {
                let mut builder = AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .with_region(region)
                    .with_conditional_put(S3ConditionalPut::ETagMatch)
                    .with_retry(retry)
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
                let cas_builder = builder.clone().with_retry(cas_retry);
                (
                    Arc::new(builder.build().context("build s3 client")?),
                    Arc::new(cas_builder.build().context("build s3 cas client")?),
                )
            }
            StorageBackend::Gcs => {
                // The generation dialect only. celld's S3 client fences
                // with If-Match, which GCS does not apply to a PUT; and
                // this GCS client authenticates with OAuth, not HMAC keys.
                // So an S3 endpoint or S3 static credentials with gs:// is
                // a configuration error, not something to quietly
                // reinterpret.
                if endpoint.is_some() {
                    anyhow::bail!(
                        "a gs:// bucket takes no S3 endpoint; unset --endpoint / S3_ENDPOINT"
                    );
                }
                if credentials.is_some() {
                    anyhow::bail!(
                        "a gs:// bucket cannot use S3 static credentials; it authenticates \
                         with Google Application Default Credentials"
                    );
                }
                let builder = gcs_builder
                    .with_bucket_name(bucket)
                    .with_retry(retry)
                    .with_client_options(options);
                let cas_builder = builder.clone().with_retry(cas_retry);
                (
                    Arc::new(builder.build().context("build gcs client")?),
                    Arc::new(cas_builder.build().context("build gcs cas client")?),
                )
            }
            StorageBackend::Azure => {
                if credentials.is_some() {
                    anyhow::bail!(
                        "an az:// bucket cannot use S3 static credentials; configure CELLD_AZURE_AUTH"
                    );
                }
                let builder = Self::azure_builder(bucket, endpoint, retry, options)?;
                let cas_builder = builder.clone().with_retry(cas_retry);
                (
                    Arc::new(builder.build().context("build azure client")?),
                    Arc::new(cas_builder.build().context("build azure cas client")?),
                )
            }
        };
        Ok(Bucket {
            store,
            cas_store,
            backend,
            name: bucket.to_string(),
            prefix,
        })
    }

    fn azure_builder(
        container: &str,
        endpoint: Option<&str>,
        retry: RetryConfig,
        options: ClientOptions,
    ) -> anyhow::Result<MicrosoftAzureBuilder> {
        let config = resolve_azure_config(endpoint, |name| std::env::var(name).ok())?;
        Self::azure_builder_with_config(
            container,
            config,
            retry,
            options,
            &OfficialAzureIdentityFactory,
        )
    }

    fn azure_builder_with_config(
        container: &str,
        config: AzureConfig,
        retry: RetryConfig,
        options: ClientOptions,
        identity_factory: &dyn AzureIdentityFactory,
    ) -> anyhow::Result<MicrosoftAzureBuilder> {
        match config.auth {
            AzureAuth::Emulator => {
                let mut builder = MicrosoftAzureBuilder::new()
                    .with_container_name(container)
                    .with_use_emulator(true)
                    .with_retry(retry)
                    .with_client_options(options);
                if let Some(account) = config.account {
                    builder = builder.with_account(account);
                }
                Ok(builder)
            }
            auth => {
                let mut builder = MicrosoftAzureBuilder::new()
                    .with_account(config.account.expect("production Azure account"))
                    .with_container_name(container)
                    .with_retry(retry)
                    .with_client_options(options);
                if let Some(endpoint) = config.endpoint {
                    builder = builder.with_endpoint(endpoint).with_allow_http(true);
                }
                match auth {
                    AzureAuth::AccountKey(key) => Ok(builder.with_access_key(key)),
                    AzureAuth::Sas(sas) => {
                        Ok(builder.with_sas_authorization(parse_sas_authorization(&sas)?))
                    }
                    AzureAuth::Identity(identity) => Ok(builder.with_credentials(
                        credentials_for_auth(&AzureAuth::Identity(identity), identity_factory)?
                            .expect("identity auth creates an Azure credential provider"),
                    )),
                    AzureAuth::Emulator => unreachable!("emulator matched above"),
                }
            }
        }
    }

    /// The bucket's URL scheme, `s3` or `gs`, for operator-facing
    /// messages.
    pub fn scheme(&self) -> &'static str {
        self.backend.scheme()
    }

    /// The dialect this bucket speaks, for choosing the matching
    /// replication store.
    pub(crate) fn backend(&self) -> StorageBackend {
        self.backend
    }

    /// Scope a caller's key to this client's prefix.
    fn key(&self, key: &str) -> String {
        format!("{}{key}", self.prefix)
    }

    /// The inverse of [`Self::key`]: a listing answers with full keys, and
    /// every caller parses back the key it asked for, not the prefix.
    fn unkey<'a>(&self, key: &'a str) -> &'a str {
        key.strip_prefix(self.prefix.as_str()).unwrap_or(key)
    }

    /// Body and CAS token, or `None` when the key does not exist.
    pub async fn get(&self, key: &str) -> anyhow::Result<Option<(Bytes, String)>> {
        let key = self.key(key);
        match self.store.get(&Path::from(key.as_str())).await {
            Ok(result) => {
                let token = self
                    .backend
                    .token(result.meta.e_tag.clone(), result.meta.version.clone())
                    .with_context(|| format!("read {}://{}/{key}", self.scheme(), self.name))?;
                let bytes = result.bytes().await.with_context(|| {
                    format!("read body {}://{}/{key}", self.scheme(), self.name)
                })?;
                Ok(Some((bytes, token)))
            }
            Err(Error::NotFound { .. }) => Ok(None),
            Err(error) => {
                Err(anyhow!(error).context(format!("read {}://{}/{key}", self.scheme(), self.name)))
            }
        }
    }

    /// Size and CAS token, or `None` when the key does not exist.
    pub async fn head(&self, key: &str) -> anyhow::Result<Option<(u64, String)>> {
        let key = self.key(key);
        match self.store.head(&Path::from(key.as_str())).await {
            Ok(meta) => {
                let token = self
                    .backend
                    .token(meta.e_tag, meta.version)
                    .with_context(|| format!("head {}://{}/{key}", self.scheme(), self.name))?;
                Ok(Some((meta.size as u64, token)))
            }
            Err(Error::NotFound { .. }) => Ok(None),
            Err(error) => {
                Err(anyhow!(error).context(format!("head {}://{}/{key}", self.scheme(), self.name)))
            }
        }
    }

    pub async fn put(&self, key: &str, body: impl Into<PutPayload>) -> anyhow::Result<()> {
        let key = self.key(key);
        self.store
            .put(&Path::from(key.as_str()), body.into())
            .await
            .with_context(|| format!("write {}://{}/{key}", self.scheme(), self.name))?;
        Ok(())
    }

    /// Size plus one user-metadata value (`x-amz-meta-*` / `x-goog-meta-*`),
    /// or `None` when the key does not exist. A plain `head` cannot see
    /// user metadata; this one can.
    pub async fn head_with_meta(
        &self,
        key: &str,
        name: &str,
    ) -> anyhow::Result<Option<(u64, Option<String>)>> {
        let key = self.key(key);
        let options = GetOptions {
            head: true,
            ..GetOptions::default()
        };
        match self
            .store
            .get_opts(&Path::from(key.as_str()), options)
            .await
        {
            Ok(result) => {
                let value = result
                    .attributes
                    .get(&Attribute::Metadata(name.to_string().into()))
                    .map(|value| value.as_ref().to_string());
                Ok(Some((result.meta.size as u64, value)))
            }
            Err(Error::NotFound { .. }) => Ok(None),
            Err(error) => {
                Err(anyhow!(error).context(format!("head {}://{}/{key}", self.scheme(), self.name)))
            }
        }
    }

    /// Plain write carrying user metadata (`x-amz-meta-*` / `x-goog-meta-*`).
    pub async fn put_with_meta(
        &self,
        key: &str,
        body: impl Into<PutPayload>,
        meta: &[(&'static str, &str)],
    ) -> anyhow::Result<()> {
        let key = self.key(key);
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
            .put_opts(&Path::from(key.as_str()), body.into(), options)
            .await
            .with_context(|| format!("write {}://{}/{key}", self.scheme(), self.name))?;
        Ok(())
    }

    /// Conditional write. `token: None` requires the key to be absent;
    /// `Some` requires the current CAS token — the etag on S3 (If-Match),
    /// the generation on GCS (x-goog-if-generation-match).
    /// `Ok(Some(new_token))` applied, `Ok(None)` cleanly rejected; any
    /// other failure is ambiguous and stays an error.
    pub async fn put_cas(
        &self,
        key: &str,
        body: impl Into<PutPayload>,
        token: Option<&str>,
    ) -> anyhow::Result<Option<String>> {
        let key = self.key(key);
        let mode = match token {
            None => PutMode::Create,
            Some(token) => PutMode::Update(self.backend.update(token)),
        };
        match self
            .cas_store
            .put_opts(
                &Path::from(key.as_str()),
                body.into(),
                PutOptions::from(mode),
            )
            .await
        {
            Ok(result) => {
                // The write applied; a result without a usable token still
                // surfaces as `Err`, which callers already treat as "may
                // have committed" and reconcile.
                let token = self
                    .backend
                    .token(result.e_tag, result.version)
                    .with_context(|| {
                        format!(
                            "conditional write {}://{}/{key} applied without a CAS token",
                            self.scheme(),
                            self.name
                        )
                    })?;
                Ok(Some(token))
            }
            Err(error) if self.backend.is_clean_cas_rejection(&error, token.is_none()) => Ok(None),
            Err(error) => Err(anyhow!(error).context(format!(
                "conditional write {}://{}/{key} may have committed",
                self.scheme(),
                self.name
            ))),
        }
    }

    /// Idempotent: deleting an absent key succeeds, as S3's DELETE does.
    pub async fn delete(&self, key: &str) -> anyhow::Result<()> {
        let key = self.key(key);
        match self.store.delete(&Path::from(key.as_str())).await {
            Ok(()) | Err(Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(anyhow!(error).context(format!(
                "delete {}://{}/{key}",
                self.scheme(),
                self.name
            ))),
        }
    }

    /// Every object under `prefix/`; the client paginates internally.
    /// Listed keys come back the way the caller wrote them, because the
    /// caller parses them and knows nothing of the fleet's prefix.
    pub async fn list(&self, prefix: &str) -> anyhow::Result<Vec<ObjectMeta>> {
        let path = Path::from(self.key(prefix.trim_end_matches('/')));
        let mut stream = self.store.list(Some(&path));
        let mut objects = Vec::new();
        while let Some(meta) = stream.next().await {
            let mut meta =
                meta.with_context(|| format!("list {}://{}/{path}", self.scheme(), self.name))?;
            if !self.prefix.is_empty() {
                // The listing gave object_store a valid key, so re-parsing
                // the tail of it cannot fail.
                meta.location = Path::parse(self.unkey(meta.location.as_ref())).unwrap();
            }
            objects.push(meta);
        }
        Ok(objects)
    }

    /// Does anything exist under `prefix/`? One page at most.
    pub async fn list_any(&self, prefix: &str) -> anyhow::Result<bool> {
        let path = Path::from(self.key(prefix.trim_end_matches('/')));
        match self.store.list(Some(&path)).next().await {
            None => Ok(false),
            Some(Ok(_)) => Ok(true),
            Some(Err(error)) => Err(anyhow!(error).context(format!(
                "list {}://{}/{path}",
                self.scheme(),
                self.name
            ))),
        }
    }

    /// Immediate child "directories" under `prefix/` (delimiter listing),
    /// as full prefixes with the trailing slash stripped.
    pub async fn common_prefixes(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        let path = Path::from(self.key(prefix.trim_end_matches('/')));
        let result = self
            .store
            .list_with_delimiter(Some(&path))
            .await
            .with_context(|| format!("list {}://{}/{path}", self.scheme(), self.name))?;
        Ok(result
            .common_prefixes
            .into_iter()
            .map(|p| self.unkey(p.as_ref()).to_string())
            .collect())
    }

    /// The head_bucket replacement: prove the bucket is reachable and the
    /// credential is accepted with one list page. Scoped to the prefix, so
    /// a credential scoped to it validates too.
    pub async fn validate(&self) -> anyhow::Result<()> {
        let scope = (!self.prefix.is_empty()).then(|| Path::from(self.prefix.as_str()));
        match self.store.list(scope.as_ref()).next().await {
            None | Some(Ok(_)) => Ok(()),
            Some(Err(error)) => {
                Err(anyhow!(error).context(format!("validate {}://{}", self.scheme(), self.name)))
            }
        }
    }
}

/// The replica-lane store for a `gs://` fleet bucket: its own transport
/// and connection pool, authenticated like [`Bucket::open`]'s gs:// path
/// (OAuth via Application Default Credentials or the `GOOGLE_*` env),
/// with the same default retry policy the S3 replica lane gets from its
/// litestream-ported config in `ltx_repl`. Replica writes are plain puts,
/// so retries stay on.
pub(crate) fn gcs_replica_store(bucket: &str) -> anyhow::Result<Arc<dyn ObjectStore>> {
    gcs_replica_store_with_builder(GoogleCloudStorageBuilder::from_env(), bucket)
}

/// The replica-lane store for an `az://` fleet bucket. It is deliberately
/// separate from the control-plane store so ordinary LTX puts retain retries.
pub(crate) fn azure_replica_store(
    bucket: &str,
    endpoint: Option<&str>,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(
        Bucket::azure_builder(
            bucket,
            endpoint,
            RetryConfig::default(),
            ClientOptions::new().with_allow_http(true),
        )?
        .build()
        .context("build azure replica store")?,
    ))
}

/// The body of [`gcs_replica_store`], taking the base builder for the same
/// reason [`Bucket::open_with_gcs_builder`] does.
fn gcs_replica_store_with_builder(
    builder: GoogleCloudStorageBuilder,
    bucket: &str,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(
        builder
            .with_bucket_name(bucket)
            .build()
            .context("build gcs replica store")?,
    ))
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

    // Live CAS contract against a real bucket (R2, GCS, or Azure). Gated on
    // CELLD_CAS_LIVE=1 so it never runs in CI; a mock cannot answer whether
    // object_store maps the provider's precondition failures to Ok(None)
    // (the fencing contract) rather than Err. Run:
    //   CELLD_CAS_LIVE=1 CELLD_CAS_BUCKET=<b> CELLD_CAS_ENDPOINT=<ep> AWS_*=... \
    //     cargo test -p celld put_cas_contract -- --nocapture
    // or against GCS (Application Default Credentials, no endpoint):
    //   CELLD_CAS_LIVE=1 CELLD_CAS_BUCKET=gs://<b> \
    //     cargo test -p celld put_cas_contract -- --nocapture
    // or Azure (account-key/SAS, or Azurite):
    //   CELLD_CAS_LIVE=1 CELLD_CAS_BUCKET=az://<container> \
    //     CELLD_AZURE_AUTH=account-key AZURE_STORAGE_ACCOUNT_NAME=... \
    //     AZURE_STORAGE_ACCOUNT_KEY=... \
    //     cargo test -p celld put_cas_contract -- --nocapture
    #[tokio::test]
    async fn put_cas_contract_against_real_bucket() {
        if std::env::var("CELLD_CAS_LIVE").as_deref() != Ok("1") {
            return;
        }
        let name = std::env::var("CELLD_CAS_BUCKET").expect("CELLD_CAS_BUCKET");
        let endpoint = std::env::var("CELLD_CAS_ENDPOINT").ok();
        let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "auto".into());
        let creds = (!name.starts_with("gs://") && !name.starts_with("az://"))
            .then(|| {
                std::env::var("AWS_ACCESS_KEY_ID")
                    .ok()
                    .map(|access_key_id| StaticCredentials {
                        access_key_id,
                        secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY")
                            .expect("AWS_SECRET_ACCESS_KEY"),
                        session_token: std::env::var("AWS_SESSION_TOKEN").ok(),
                    })
            })
            .flatten();
        let bucket = Bucket::open(
            &name,
            endpoint.as_deref(),
            &region,
            creds.clone(),
            Some("cas-test-a"),
        )
        .expect("open first bucket client");
        let contender = Bucket::open(
            &name,
            endpoint.as_deref(),
            &region,
            creds,
            Some("cas-test-b"),
        )
        .expect("open independently constructed contender client");
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let key = format!("cas-probe/{nanos}");

        // Independently constructed clients race creation of the same absent key.
        let (first, second) = tokio::join!(
            bucket.put_cas(&key, b"v1".to_vec(), None),
            contender.put_cas(&key, b"v1b".to_vec(), None),
        );
        let e1 = match (first, second) {
            (Ok(Some(token)), Ok(None)) | (Ok(None), Ok(Some(token))) => token,
            outcomes => panic!(
                "concurrent creates must yield exactly one Ok(Some(token)) and one Ok(None), got {outcomes:?}"
            ),
        };
        // Update with the winning token applies.
        let e2 = bucket
            .put_cas(&key, b"v3".to_vec(), Some(&e1))
            .await
            .expect("update must not error")
            .expect("update with current etag must apply (Ok(Some))");
        assert_ne!(e2, e1, "a current update must return a new CAS token");
        // Update with the now-stale winning token is cleanly rejected — the fencing case.
        assert!(
            contender
                .put_cas(&key, b"v4".to_vec(), Some(&e1))
                .await
                .expect("stale update must not error")
                .is_none(),
            "update with a stale etag must be Ok(None) — the fencing contract"
        );
        bucket.delete(&key).await.expect("cleanup delete");
        eprintln!("CAS verified on {name}: concurrent clients create / update / reject-stale");
    }

    #[tokio::test]
    async fn azure_prefixes_are_isolated_against_real_container() {
        if std::env::var("CELLD_CAS_LIVE").as_deref() != Ok("1") {
            return;
        }
        let configured = std::env::var("CELLD_CAS_BUCKET").expect("CELLD_CAS_BUCKET");
        let Some(container) = configured.strip_prefix("az://") else {
            return;
        };
        let container = container.split('/').next().expect("Azure container");
        let endpoint = std::env::var("CELLD_CAS_ENDPOINT").ok();
        let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "auto".into());
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let first_spec = format!("az://{container}/celld-azure-prefix-a/{nanos}");
        let second_spec = format!("az://{container}/celld-azure-prefix-b/{nanos}");
        let first = Bucket::open(
            &first_spec,
            endpoint.as_deref(),
            &region,
            None,
            Some("azure-prefix-a"),
        )
        .expect("open first Azure prefix");
        let second = Bucket::open(
            &second_spec,
            endpoint.as_deref(),
            &region,
            None,
            Some("azure-prefix-b"),
        )
        .expect("open second Azure prefix");
        first
            .put("only-first", b"first".to_vec())
            .await
            .expect("write first");
        second
            .put("only-second", b"second".to_vec())
            .await
            .expect("write second");

        assert_eq!(
            first
                .list("")
                .await
                .expect("list first prefix")
                .into_iter()
                .map(|meta| meta.location.to_string())
                .collect::<Vec<_>>(),
            vec!["only-first"]
        );
        assert_eq!(
            second
                .list("")
                .await
                .expect("list second prefix")
                .into_iter()
                .map(|meta| meta.location.to_string())
                .collect::<Vec<_>>(),
            vec!["only-second"]
        );
        assert!(first
            .get("only-second")
            .await
            .expect("read first prefix")
            .is_none());
        first
            .delete("only-second")
            .await
            .expect("delete absent key in first prefix");
        assert!(
            second
                .get("only-second")
                .await
                .expect("read second prefix")
                .is_some(),
            "one prefix must not mutate the other's object"
        );

        first
            .delete("only-first")
            .await
            .expect("cleanup first prefix");
        second
            .delete("only-second")
            .await
            .expect("cleanup second prefix");
        eprintln!("Azure prefix isolation verified on {container} with scoped cleanup");
    }
}

#[cfg(all(test, celld_internal_tests))]
mod conformance_bucket_tests {
    include!(env!("CELLD_CONFORMANCE_BUCKET_TESTS"));
}

#[cfg(test)]
mod tests {
    use super::{
        cas_retry_config, control_plane_bucket, managed_identity_options, ordinary_retry_config,
        parse_sas_authorization, resolve_azure_config, split_spec, AzureAuth, AzureConfig,
        AzureIdentityFactory, AzureIdentityMode, Bucket, StorageBackend,
    };
    use async_trait::async_trait;
    use azure_core::credentials::{AccessToken, TokenCredential, TokenRequestOptions};
    use azure_core::error::ErrorKind;
    use azure_identity::UserAssignedId;
    use futures_util::stream::BoxStream;
    use object_store::azure::MicrosoftAzureBuilder;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{
        Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOpts, PutOptions, PutPayload, PutResult,
    };
    use std::collections::BTreeMap;
    use std::fmt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Copy, Debug)]
    enum InjectedPutFailure {
        Generic,
    }

    #[derive(Debug)]
    struct CountingPutStore {
        inner: InMemory,
        calls: AtomicUsize,
        failure: Option<InjectedPutFailure>,
    }

    impl CountingPutStore {
        fn new() -> Self {
            Self {
                inner: InMemory::new(),
                calls: AtomicUsize::new(0),
                failure: None,
            }
        }

        fn failing(failure: InjectedPutFailure) -> Self {
            Self {
                failure: Some(failure),
                ..Self::new()
            }
        }

        async fn seed(&self, key: &str, body: &'static [u8]) -> PutResult {
            self.inner
                .put(&Path::from(key), body.into())
                .await
                .expect("seed object")
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl fmt::Display for CountingPutStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "CountingPutStore")
        }
    }

    #[async_trait]
    impl ObjectStore for CountingPutStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.failure {
                Some(InjectedPutFailure::Generic) => Err(Error::Generic {
                    store: "injected",
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "lost response",
                    )),
                }),
                None => self.inner.put_opts(location, payload, opts).await,
            }
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOpts,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        async fn delete(&self, location: &Path) -> object_store::Result<()> {
            self.inner.delete(location).await
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    fn bucket_with_cas_store(backend: StorageBackend, cas_store: Arc<dyn ObjectStore>) -> Bucket {
        Bucket {
            store: Arc::new(InMemory::new()),
            cas_store,
            backend,
            name: "container".into(),
            prefix: String::new(),
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct AzureRequest {
        path: String,
        if_match: Option<String>,
        if_none_match: Option<String>,
    }

    async fn azure_error_bucket() -> (
        Bucket,
        Arc<Mutex<Vec<AzureRequest>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind local Azure endpoint");
        let endpoint = format!("http://{}", listener.local_addr().expect("local address"));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = requests.clone();
        let app = axum::Router::new().fallback(axum::routing::any(
            move |request: axum::http::Request<axum::body::Body>| {
                let recorded = recorded.clone();
                async move {
                    let header = |name| {
                        request
                            .headers()
                            .get(name)
                            .and_then(|value| value.to_str().ok())
                            .map(ToOwned::to_owned)
                    };
                    let path = request.uri().path().to_string();
                    recorded.lock().expect("request log").push(AzureRequest {
                        path: path.clone(),
                        if_match: header(axum::http::header::IF_MATCH),
                        if_none_match: header(axum::http::header::IF_NONE_MATCH),
                    });
                    let (status, code) = match path.as_str() {
                        "/container/clean-create" => {
                            (axum::http::StatusCode::CONFLICT, "BlobAlreadyExists")
                        }
                        "/container/clean-update" => (
                            axum::http::StatusCode::PRECONDITION_FAILED,
                            "ConditionNotMet",
                        ),
                        "/container/ambiguous-conflict" => {
                            (axum::http::StatusCode::CONFLICT, "ContainerBeingDeleted")
                        }
                        "/container/ambiguous-precondition" => (
                            axum::http::StatusCode::PRECONDITION_FAILED,
                            "LeaseIdMissing",
                        ),
                        path => panic!("unexpected Azure test path: {path}"),
                    };
                    (
                        status,
                        [(axum::http::header::CONTENT_TYPE, "application/xml")],
                        format!(
                            "<?xml version=\"1.0\" encoding=\"utf-8\"?>\
                             <Error><Code>{code}</Code><Message>injected</Message></Error>"
                        ),
                    )
                }
            },
        ));
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve local Azure endpoint");
        });
        let cas_store: Arc<dyn ObjectStore> = Arc::new(
            MicrosoftAzureBuilder::new()
                .with_account("account")
                .with_container_name("container")
                .with_endpoint(endpoint)
                .with_allow_http(true)
                .with_skip_signature(true)
                .with_retry(cas_retry_config())
                .build()
                .expect("Azure CAS store"),
        );
        (
            bucket_with_cas_store(StorageBackend::Azure, cas_store),
            requests,
            server,
        )
    }

    #[test]
    fn azure_spec_selects_azure_and_normalizes_prefix() {
        let (backend, container, prefix) = split_spec("az://fleet/a//nested/");

        assert_eq!(backend, StorageBackend::Azure);
        assert_eq!(container, "fleet");
        assert_eq!(prefix, "a/nested/");
    }

    #[test]
    fn bucket_schemes_remain_parseable() {
        assert_eq!(super::scheme_for_spec("gs://fleet"), "gs");
        assert_eq!(super::scheme_for_spec("az://container"), "az");
    }

    #[test]
    fn control_plane_bucket_normalizes_only_unprefixed_s3_specs() {
        assert_eq!(control_plane_bucket("fleet").unwrap(), "fleet");
        assert_eq!(control_plane_bucket("s3://fleet").unwrap(), "fleet");
        assert!(control_plane_bucket("s3://fleet/prefix").is_err());
        assert!(control_plane_bucket("gs://fleet").is_err());
        assert!(control_plane_bucket("az://container").is_err());
    }

    #[test]
    fn sas_authorization_decodes_pairs_after_splitting_raw_query() {
        assert_eq!(
            parse_sas_authorization("?sv=2024-11-04&sig=one%26two%3Dthree").unwrap(),
            vec![
                ("sv".to_string(), "2024-11-04".to_string()),
                ("sig".to_string(), "one&two=three".to_string()),
            ]
        );
    }

    #[test]
    fn sas_authorization_rejects_malformed_pairs() {
        assert!(parse_sas_authorization("sv=2024-11-04&sig").is_err());
        assert!(parse_sas_authorization("sv=%ZZ").is_err());
    }

    #[test]
    fn azure_etag_tokens_and_updates_fail_closed() {
        assert_eq!(
            StorageBackend::Azure
                .token(Some("\"etag\"".into()), None)
                .expect("Azure ETag is a CAS token"),
            "\"etag\""
        );
        assert!(StorageBackend::Azure.token(None, None).is_err());
        assert!(StorageBackend::Azure
            .token(Some(String::new()), None)
            .is_err());
        assert!(StorageBackend::Azure
            .token(None, Some("version".into()))
            .is_err());

        let update = StorageBackend::Azure.update("\"etag\"");
        assert_eq!(update.e_tag.as_deref(), Some("\"etag\""));
        assert_eq!(update.version, None);
    }

    #[tokio::test]
    async fn put_cas_maps_already_exists_to_one_clean_rejection() {
        let store = Arc::new(CountingPutStore::new());
        store.seed("lease", b"current").await;
        let bucket = bucket_with_cas_store(StorageBackend::S3, store.clone());

        let result = bucket
            .put_cas("lease", b"contender".to_vec(), None)
            .await
            .expect("an existing object is a clean CAS rejection");

        assert_eq!(result, None);
        assert_eq!(store.calls(), 1);
    }

    #[tokio::test]
    async fn put_cas_maps_precondition_to_one_clean_rejection() {
        let store = Arc::new(CountingPutStore::new());
        let stale = store
            .seed("lease", b"first")
            .await
            .e_tag
            .expect("in-memory store ETag");
        store.seed("lease", b"current").await;
        let bucket = bucket_with_cas_store(StorageBackend::S3, store.clone());

        let result = bucket
            .put_cas("lease", b"contender".to_vec(), Some(&stale))
            .await
            .expect("a stale ETag is a clean CAS rejection");

        assert_eq!(result, None);
        assert_eq!(store.calls(), 1);
    }

    #[tokio::test]
    async fn put_cas_surfaces_ambiguous_failure_after_one_attempt() {
        let store = Arc::new(CountingPutStore::failing(InjectedPutFailure::Generic));
        let bucket = bucket_with_cas_store(StorageBackend::Azure, store.clone());

        let error = bucket
            .put_cas("lease", b"candidate".to_vec(), None)
            .await
            .expect_err("a lost response is not a clean rejection");

        assert!(error.to_string().contains("may have committed"));
        assert_eq!(store.calls(), 1);
    }

    #[tokio::test]
    async fn azure_cas_condition_service_codes_are_clean_one_request_rejections() {
        let (bucket, requests, server) = azure_error_bucket().await;

        assert_eq!(
            bucket
                .put_cas("clean-create", b"candidate".to_vec(), None)
                .await
                .expect("BlobAlreadyExists is a clean create rejection"),
            None
        );
        assert_eq!(
            bucket
                .put_cas("clean-update", b"candidate".to_vec(), Some("\"etag\""))
                .await
                .expect("ConditionNotMet is a clean update rejection"),
            None
        );

        assert_eq!(
            *requests.lock().expect("request log"),
            vec![
                AzureRequest {
                    path: "/container/clean-create".into(),
                    if_match: None,
                    if_none_match: Some("*".into()),
                },
                AzureRequest {
                    path: "/container/clean-update".into(),
                    if_match: Some("\"etag\"".into()),
                    if_none_match: None,
                },
            ]
        );
        server.abort();
    }

    #[tokio::test]
    async fn azure_noncondition_409_and_412_remain_ambiguous_after_one_request() {
        let (bucket, requests, server) = azure_error_bucket().await;

        for (key, token) in [
            ("ambiguous-conflict", None),
            ("ambiguous-precondition", Some("\"etag\"")),
        ] {
            let error = bucket
                .put_cas(key, b"candidate".to_vec(), token)
                .await
                .expect_err("noncondition Azure service error is ambiguous");
            assert!(error.to_string().contains("may have committed"));
        }

        assert_eq!(
            *requests.lock().expect("request log"),
            vec![
                AzureRequest {
                    path: "/container/ambiguous-conflict".into(),
                    if_match: None,
                    if_none_match: Some("*".into()),
                },
                AzureRequest {
                    path: "/container/ambiguous-precondition".into(),
                    if_match: Some("\"etag\"".into()),
                    if_none_match: None,
                },
            ]
        );
        server.abort();
    }

    #[tokio::test]
    async fn azure_cas_attempts_once_on_429_and_5xx_while_ordinary_put_uses_retry_budget() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind local Azure endpoint");
        let endpoint = format!("http://{}", listener.local_addr().expect("local address"));
        let requests = Arc::new(Mutex::new(Vec::<(String, Option<String>)>::new()));
        let recorded = requests.clone();
        let app = axum::Router::new().fallback(axum::routing::any(
            move |request: axum::http::Request<axum::body::Body>| {
                let recorded = recorded.clone();
                async move {
                    let condition = request
                        .headers()
                        .get(axum::http::header::IF_NONE_MATCH)
                        .and_then(|value| value.to_str().ok())
                        .map(ToOwned::to_owned);
                    let path = request.uri().path().to_string();
                    recorded
                        .lock()
                        .expect("request log")
                        .push((path.clone(), condition));
                    if path.ends_with("/cas-throttled") {
                        axum::http::StatusCode::TOO_MANY_REQUESTS
                    } else {
                        axum::http::StatusCode::SERVICE_UNAVAILABLE
                    }
                }
            },
        ));
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve local Azure endpoint");
        });

        let builder = MicrosoftAzureBuilder::new()
            .with_account("account")
            .with_container_name("container")
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_skip_signature(true);
        let store: Arc<dyn ObjectStore> = Arc::new(
            builder
                .clone()
                .with_retry(ordinary_retry_config())
                .build()
                .expect("ordinary Azure store"),
        );
        let cas_store: Arc<dyn ObjectStore> = Arc::new(
            builder
                .with_retry(cas_retry_config())
                .build()
                .expect("Azure CAS store"),
        );
        let bucket = Bucket {
            store,
            cas_store,
            backend: StorageBackend::Azure,
            name: "container".into(),
            prefix: String::new(),
        };

        for key in ["cas-throttled", "cas-unavailable"] {
            let error = bucket
                .put_cas(key, b"candidate".to_vec(), None)
                .await
                .expect_err("Azure CAS service failure is ambiguous");
            assert!(error.to_string().contains("may have committed"));
        }
        bucket
            .put("ordinary", b"value".to_vec())
            .await
            .expect_err("ordinary Azure PUT exhausts its retry budget");

        let requests = requests.lock().expect("request log");
        let paths = requests
            .iter()
            .map(|(path, _)| path.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            paths,
            vec![
                "/container/cas-throttled",
                "/container/cas-unavailable",
                "/container/ordinary",
                "/container/ordinary",
                "/container/ordinary",
            ]
        );
        assert_eq!(requests[0].1.as_deref(), Some("*"));
        assert_eq!(requests[1].1.as_deref(), Some("*"));
        assert!(requests[2..]
            .iter()
            .all(|(_, condition)| condition.is_none()));
        drop(requests);
        server.abort();
    }

    #[test]
    fn azure_configuration_resolves_selected_mode_without_environment_races() {
        let resolve = |explicit: Option<&str>, values: &[(&str, &str)]| {
            let values = values
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect::<BTreeMap<_, _>>();
            resolve_azure_config(explicit, |name| values.get(name).cloned())
        };

        let config = resolve(
            Some("http://cli.example"),
            &[
                ("AZURE_STORAGE_ACCOUNT_NAME", "account"),
                ("AZURE_STORAGE_ENDPOINT", "http://environment.example"),
                ("CELLD_AZURE_AUTH", "account-key"),
                ("AZURE_STORAGE_ACCOUNT_KEY", "key"),
            ],
        )
        .expect("explicit endpoint and account key configuration");
        assert_eq!(config.endpoint.as_deref(), Some("http://cli.example"));
        assert_eq!(config.account.as_deref(), Some("account"));
        assert!(matches!(config.auth, AzureAuth::AccountKey(ref key) if key == "key"));

        let err = resolve(
            None,
            &[
                ("S3_ENDPOINT", "http://s3.example"),
                ("AZURE_STORAGE_ACCOUNT_NAME", "account"),
                ("CELLD_AZURE_AUTH", "account-key"),
                ("AZURE_STORAGE_ACCOUNT_KEY", "key"),
            ],
        )
        .expect_err("S3 endpoint must not leak into Azure");
        assert!(err.to_string().contains("S3_ENDPOINT"));

        let err = resolve(
            Some("http://cli.example"),
            &[("AZURE_STORAGE_USE_EMULATOR", "1")],
        )
        .expect_err("Azurite ignores CLI endpoints");
        assert!(err.to_string().contains("Azurite ignores"));
        let err = resolve(
            None,
            &[
                ("AZURE_STORAGE_USE_EMULATOR", "1"),
                ("CELLD_AZURE_AUTH", "sas"),
            ],
        )
        .expect_err("Azurite must not select production auth");
        assert!(err.to_string().contains("must be unset"));
        let err = resolve(None, &[("AZURE_STORAGE_USE_EMULATOR", "yes")])
            .expect_err("emulator flag must be binary");
        assert!(err.to_string().contains("accepts only 0 or 1"));

        let err =
            resolve(None, &[("CELLD_AZURE_AUTH", "account-key")]).expect_err("account is required");
        assert!(err.to_string().contains("AZURE_STORAGE_ACCOUNT_NAME"));
        let err = resolve(
            None,
            &[
                ("AZURE_STORAGE_ACCOUNT_NAME", "account"),
                ("AZURE_STORAGE_ACCOUNT_KEY", "key"),
            ],
        )
        .expect_err("auth mode is required");
        assert!(err.to_string().contains("CELLD_AZURE_AUTH"));
        let err = resolve(
            None,
            &[
                ("AZURE_STORAGE_ACCOUNT_NAME", "account"),
                ("CELLD_AZURE_AUTH", "account-key"),
                ("AZURE_STORAGE_SAS_KEY", "unselected"),
            ],
        )
        .expect_err("SAS must not satisfy account-key mode");
        assert!(err.to_string().contains("AZURE_STORAGE_ACCOUNT_KEY"));
        let err = resolve(
            None,
            &[
                ("AZURE_STORAGE_ACCOUNT_NAME", "account"),
                ("CELLD_AZURE_AUTH", "sas"),
                ("AZURE_STORAGE_ACCOUNT_KEY", "unselected"),
            ],
        )
        .expect_err("account key must not satisfy SAS mode");
        assert!(err.to_string().contains("AZURE_STORAGE_SAS_KEY"));
        let config = resolve(
            None,
            &[
                ("AZURE_STORAGE_ACCOUNT_NAME", "account"),
                ("CELLD_AZURE_AUTH", "managed-identity"),
            ],
        )
        .expect("system-assigned identity is selected");
        assert!(matches!(
            config.auth,
            AzureAuth::Identity(AzureIdentityMode::ManagedIdentity { client_id: None })
        ));
        let config = resolve(
            None,
            &[
                ("AZURE_STORAGE_ACCOUNT_NAME", "account"),
                ("CELLD_AZURE_AUTH", "managed-identity"),
                ("AZURE_CLIENT_ID", "user-assigned-client-id"),
            ],
        )
        .expect("user-assigned identity is selected");
        assert!(matches!(
            config.auth,
            AzureAuth::Identity(AzureIdentityMode::ManagedIdentity {
                client_id: Some(ref client_id)
            }) if client_id == "user-assigned-client-id"
        ));

        for (mode, requirements, required_variables) in [
            (
                "workload-identity",
                super::WORKLOAD_IDENTITY_REQUIREMENTS,
                &[
                    "AZURE_TENANT_ID",
                    "AZURE_CLIENT_ID",
                    "AZURE_FEDERATED_TOKEN_FILE",
                ][..],
            ),
            (
                "client-secret",
                super::CLIENT_SECRET_REQUIREMENTS,
                &["AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET"][..],
            ),
        ] {
            for omitted in required_variables {
                let mut values = vec![
                    ("AZURE_STORAGE_ACCOUNT_NAME", "account"),
                    ("CELLD_AZURE_AUTH", mode),
                    ("AZURE_TENANT_ID", "tenant"),
                    ("AZURE_CLIENT_ID", "client"),
                    ("AZURE_FEDERATED_TOKEN_FILE", "/var/run/token"),
                    ("AZURE_CLIENT_SECRET", "secret"),
                    ("AZURE_STORAGE_ACCOUNT_KEY", "unselected-account-key"),
                    ("AZURE_STORAGE_SAS_KEY", "sv=1&sig=unselected"),
                ];
                values.retain(|(name, _)| name != omitted);
                let err = resolve(None, &values)
                    .expect_err("an incomplete selected identity mode must not fall back");
                assert_eq!(
                    err.to_string(),
                    format!("{requirements} are required when CELLD_AZURE_AUTH={mode}"),
                    "{mode} must name its exact required set when {omitted} is absent"
                );
            }
        }
        let workload = resolve(
            None,
            &[
                ("AZURE_STORAGE_ACCOUNT_NAME", "account"),
                ("CELLD_AZURE_AUTH", "workload-identity"),
                ("AZURE_TENANT_ID", "tenant"),
                ("AZURE_CLIENT_ID", "client"),
                ("AZURE_FEDERATED_TOKEN_FILE", "/var/run/token"),
            ],
        )
        .expect("complete workload identity configuration");
        assert!(matches!(
            workload.auth,
            AzureAuth::Identity(AzureIdentityMode::WorkloadIdentity {
                ref tenant_id,
                ref client_id,
                ref federated_token_file,
            }) if tenant_id == "tenant" && client_id == "client" && federated_token_file == "/var/run/token"
        ));
        let client_secret = resolve(
            None,
            &[
                ("AZURE_STORAGE_ACCOUNT_NAME", "account"),
                ("CELLD_AZURE_AUTH", "client-secret"),
                ("AZURE_TENANT_ID", "tenant"),
                ("AZURE_CLIENT_ID", "client"),
                ("AZURE_CLIENT_SECRET", "secret"),
            ],
        )
        .expect("complete client secret configuration");
        assert!(matches!(
            client_secret.auth,
            AzureAuth::Identity(AzureIdentityMode::ClientSecret {
                ref tenant_id,
                ref client_id,
                ref client_secret,
            }) if tenant_id == "tenant" && client_id == "client" && client_secret == "secret"
        ));
        assert!(matches!(
            resolve(
                None,
                &[
                    ("AZURE_STORAGE_ACCOUNT_NAME", "account"),
                    ("CELLD_AZURE_AUTH", "developer-tools"),
                ],
            )
            .expect("developer tools is explicit")
            .auth,
            AzureAuth::Identity(AzureIdentityMode::DeveloperTools)
        ));

        let err = resolve(
            None,
            &[
                ("AZURE_STORAGE_ACCOUNT_NAME", "account"),
                ("CELLD_AZURE_AUTH", "unknown-mode"),
            ],
        )
        .expect_err("unknown authentication mode fails before I/O");
        assert!(err
            .to_string()
            .contains("CELLD_AZURE_AUTH must be account-key, sas, managed-identity, workload-identity, client-secret, or developer-tools"));
    }

    #[derive(Debug, Default)]
    struct RecordingIdentityFactory {
        calls: Mutex<Vec<AzureIdentityMode>>,
    }

    impl AzureIdentityFactory for RecordingIdentityFactory {
        fn create(&self, mode: &AzureIdentityMode) -> anyhow::Result<Arc<dyn TokenCredential>> {
            self.calls.lock().expect("calls lock").push(mode.clone());
            anyhow::bail!("identity factory should only be needed for Entra modes")
        }
    }

    #[test]
    fn account_key_and_sas_builders_never_construct_an_entra_credential() {
        let factory = RecordingIdentityFactory::default();

        Bucket::azure_builder_with_config(
            "container",
            AzureConfig {
                account: Some("account".into()),
                endpoint: None,
                auth: AzureAuth::AccountKey("key".into()),
            },
            object_store::RetryConfig::default(),
            object_store::ClientOptions::new(),
            &factory,
        )
        .expect("account-key builder");
        Bucket::azure_builder_with_config(
            "container",
            AzureConfig {
                account: Some("account".into()),
                endpoint: None,
                auth: AzureAuth::Sas("sv=1&sig=signature".into()),
            },
            object_store::RetryConfig::default(),
            object_store::ClientOptions::new(),
            &factory,
        )
        .expect("SAS builder");
        assert!(factory.calls.lock().expect("calls lock").is_empty());
    }

    #[test]
    fn managed_identity_options_map_client_ids_without_selecting_a_default() {
        assert!(managed_identity_options(None).user_assigned_id.is_none());
        assert!(matches!(
            managed_identity_options(Some("user-assigned-client-id".into())).user_assigned_id,
            Some(UserAssignedId::ClientId(ref client_id))
                if client_id == "user-assigned-client-id"
        ));
    }

    #[derive(Debug)]
    struct FakeTokenCredential;

    #[async_trait]
    impl TokenCredential for FakeTokenCredential {
        async fn get_token(
            &self,
            _: &[&str],
            _: Option<TokenRequestOptions<'_>>,
        ) -> azure_core::Result<AccessToken> {
            Err(azure_core::Error::with_message(
                ErrorKind::Credential,
                "token was not expected during construction".to_string(),
            ))
        }
    }

    #[derive(Debug, Default)]
    struct WorkingIdentityFactory;

    impl AzureIdentityFactory for WorkingIdentityFactory {
        fn create(&self, _: &AzureIdentityMode) -> anyhow::Result<Arc<dyn TokenCredential>> {
            Ok(Arc::new(FakeTokenCredential))
        }
    }

    #[test]
    fn ordinary_and_cas_builders_share_one_adapter_but_separate_construction_does_not() {
        let auth = AzureAuth::Identity(AzureIdentityMode::DeveloperTools);
        let factory = WorkingIdentityFactory;
        let make_builder = || {
            Bucket::azure_builder_with_config(
                "container",
                AzureConfig {
                    account: Some("account".into()),
                    endpoint: None,
                    auth: auth.clone(),
                },
                object_store::RetryConfig::default(),
                object_store::ClientOptions::new(),
                &factory,
            )
            .expect("Azure builder")
        };

        let builder = make_builder();
        let ordinary = builder.clone().build().expect("ordinary store");
        let cas = builder.build().expect("CAS store");
        assert!(Arc::ptr_eq(ordinary.credentials(), cas.credentials()));

        let first = make_builder().build().expect("first independent store");
        let second = make_builder().build().expect("second independent store");
        assert!(!Arc::ptr_eq(first.credentials(), second.credentials()));
    }
}
