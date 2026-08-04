//! Production bucket deployment adapters reused by the clean-sheet host.

use crate::deploy;
use crate::js::WorkerConfigOptions;
use crate::protocol::{DeployPointer, Manifest};
use anyhow::{bail, Context};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::Client;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::time::Duration;
use tracing::info;

pub async fn s3_client(endpoint: Option<&str>, region: &str) -> Client {
    s3_client_with_credentials(endpoint, region, None).await
}

/// Build the authority-heartbeat client on its own HTTP connection pool.
///
/// Node lease traffic must not queue behind ordinary ownership, deployment,
/// or replica requests. The distinct application name also makes the safety
/// lane observable in black-box storage traces.
pub async fn s3_lease_client_with_credentials(
    endpoint: Option<&str>,
    region: &str,
    managed: Option<&crate::control_plane::ManagedStorageConfig>,
) -> Client {
    s3_client_with_transport(endpoint, region, managed, true).await
}

pub async fn s3_client_with_credentials(
    endpoint: Option<&str>,
    region: &str,
    managed: Option<&crate::control_plane::ManagedStorageConfig>,
) -> Client {
    s3_client_with_transport(endpoint, region, managed, false).await
}

async fn s3_client_with_transport(
    endpoint: Option<&str>,
    region: &str,
    managed: Option<&crate::control_plane::ManagedStorageConfig>,
    isolated_transport: bool,
) -> Client {
    let timeouts = aws_config::timeout::TimeoutConfig::builder()
        .connect_timeout(Duration::from_secs(3))
        .read_timeout(Duration::from_secs(10))
        .operation_attempt_timeout(Duration::from_secs(15))
        .operation_timeout(Duration::from_secs(30))
        .build();
    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .timeout_config(timeouts)
        .region(aws_config::Region::new(region.to_string()));
    if isolated_transport {
        let http_client = aws_smithy_http_client::Builder::new()
            .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
                aws_smithy_http_client::tls::rustls_provider::CryptoMode::AwsLc,
            ))
            .build_https();
        loader = loader
            .http_client(http_client)
            .app_name(aws_config::AppName::new("celld-lease").expect("valid lease app name"));
    }
    if let Some(endpoint) = endpoint {
        loader = loader.endpoint_url(endpoint);
    }
    if let Some(managed) = managed {
        loader = loader.credentials_provider(aws_credential_types::Credentials::new(
            managed.access_key_id.clone(),
            managed.secret_access_key.clone(),
            managed.session_token.clone(),
            None,
            "managed-installation",
        ));
    }
    let shared = loader.load().await;
    let config = aws_sdk_s3::config::Builder::from(&shared)
        .force_path_style(endpoint.is_some())
        .build();
    Client::from_conf(config)
}

pub async fn validate_bucket(client: &Client, bucket: &str) -> anyhow::Result<()> {
    client
        .head_bucket()
        .bucket(bucket)
        .send()
        .await
        .with_context(|| format!("bucket unavailable or inaccessible: s3://{bucket}"))?;
    Ok(())
}

/// Validate storage issued by the Managed Control Plane and preserve the
/// operator-visible failure vocabulary. Newly issued provider credentials can
/// take a moment to propagate, so only the final rejection is authoritative.
pub async fn validate_managed_bucket(client: &Client, bucket: &str) -> anyhow::Result<()> {
    const RETRIES: u32 = 5;
    for attempt in 1..=RETRIES {
        match validate_managed_bucket_once(client, bucket, attempt == RETRIES).await {
            Ok(()) => return Ok(()),
            Err(error) if attempt == RETRIES => return Err(error),
            Err(_) => {
                info!(bucket, attempt, "storage credential not accepted yet; retrying");
                tokio::time::sleep(Duration::from_millis(500 * u64::from(attempt))).await;
            }
        }
    }
    unreachable!("loop returns on the final attempt")
}

async fn validate_managed_bucket_once(
    client: &Client,
    bucket: &str,
    report: bool,
) -> anyhow::Result<()> {
    match client.head_bucket().bucket(bucket).send().await {
        Ok(_) => Ok(()),
        Err(SdkError::ServiceError(error))
            if matches!(error.raw().status().as_u16(), 401 | 403) =>
        {
            if report {
                crate::control_plane::report_managed_runtime_state(
                    crate::control_plane::ManagedRuntimeState::CredentialRevoked,
                );
                bail!("managed storage credential was rejected or revoked for s3://{bucket}");
            }
            bail!("managed storage credential was not accepted yet for s3://{bucket}");
        }
        Err(error) => {
            if report {
                crate::control_plane::report_managed_runtime_state(
                    crate::control_plane::ManagedRuntimeState::BucketUnavailable,
                );
            }
            Err(error).with_context(|| format!("bucket unavailable or inaccessible: s3://{bucket}"))
        }
    }
}

#[derive(Deserialize)]
pub(crate) struct DiagnosticNode {
    pub(crate) node: String,
    pub(crate) expires_ms: u64,
    pub(crate) addr: String,
    #[serde(default)]
    pub(crate) probe_public_key: String,
    #[serde(default)]
    pub(crate) peer_protocol: u16,
    #[serde(default)]
    pub(crate) load: crate::ownership_store::NodeLoadWire,
}

pub async fn diagnose(
    client: &Client,
    bucket: &str,
    peers: Vec<String>,
    unsafe_public_advertise: bool,
) -> anyhow::Result<()> {
    validate_bucket(client, bucket).await?;
    println!("ok bucket s3://{bucket}");

    let enumerated = peers.is_empty();
    let peers = if enumerated {
        let peers = diagnostic_node_ids(client, bucket).await?;
        println!("ok fleet {} node lease(s) enumerated", peers.len());
        peers
    } else {
        peers
    };
    if peers.is_empty() {
        return Ok(());
    }
    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("build peer diagnostic client")?;
    let auth = crate::peer_auth::PeerAuth::new(
        crate::peer_auth::load_existing(client, bucket).await?,
        "diagnostic",
    )?;
    let mut failures = 0_usize;
    let mut expired = 0_usize;
    for peer in peers {
        let node = match diagnostic_node(client, bucket, &peer).await {
            Ok(Some(node)) => node,
            Ok(None) if enumerated => {
                expired += 1;
                println!("skip peer {peer}: lease is expired");
                continue;
            }
            Ok(None) => {
                failures += 1;
                eprintln!("fail peer {peer}: node {peer} lease is expired");
                continue;
            }
            Err(error) => {
                failures += 1;
                eprintln!("fail peer {peer}: {error}");
                continue;
            }
        };
        let advertise = match crate::startup::parse_advertise(&node.addr) {
            Ok(advertise) => advertise,
            Err(error) => {
                failures += 1;
                eprintln!(
                    "fail peer {peer}: malformed advertise address {:?}: {error}",
                    node.addr
                );
                continue;
            }
        };
        if advertise.is_public_ip() && !unsafe_public_advertise {
            failures += 1;
            eprintln!(
                "fail peer {peer}: unsafe public advertise address {}; use a private overlay or --unsafe-public-advertise",
                node.addr
            );
            continue;
        }
        if let Err(error) = crate::peer_probe::probe(&http, &node, &auth).await {
            failures += 1;
            eprintln!("fail peer {peer} at {}: {error}", node.addr);
            continue;
        }
        let load_age_ms = if node.load.sampled_ms == 0 {
            "unknown".to_string()
        } else {
            crate::ownership_store::now_ms()
                .saturating_sub(node.load.sampled_ms)
                .to_string()
        };
        println!(
            "ok peer {} at {} (signed direct probe) protocol={} resident_cells={} \
             websockets={} rss_bytes={} cpu_percent={:.2} fds={}/{} pressured={} \
             shed_cells={} load_age_ms={}",
            node.node,
            node.addr,
            node.peer_protocol,
            node.load.resident_cells,
            node.load.host_websockets,
            node.load.rss_bytes,
            node.load.cpu_percent_x100 as f64 / 100.0,
            node.load.open_fds,
            node.load.fd_limit,
            node.load.pressured,
            node.load.shed_cells,
            load_age_ms,
        );
    }
    if expired > 0 {
        println!("ok fleet skipped {expired} expired node lease(s)");
    }
    if failures > 0 {
        bail!("fleet diagnostics failed for {failures} peer(s)");
    }
    Ok(())
}

async fn diagnostic_node(
    client: &Client,
    bucket: &str,
    peer: &str,
) -> anyhow::Result<Option<DiagnosticNode>> {
    let key = format!("nodes/{peer}.json");
    let node: DiagnosticNode = serde_json::from_str(&get_string(client, bucket, &key).await?)
        .with_context(|| format!("decode s3://{bucket}/{key}"))?;
    if node.node != peer {
        bail!(
            "node lease {key} identifies unexpected node {:?}",
            node.node
        );
    }
    if node.expires_ms <= crate::ownership_store::now_ms() {
        return Ok(None);
    }
    if node.addr.is_empty() {
        bail!("node {peer} lease has no advertised address");
    }
    Ok(Some(node))
}

async fn diagnostic_node_ids(client: &Client, bucket: &str) -> anyhow::Result<Vec<String>> {
    let mut nodes = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let mut request = client.list_objects_v2().bucket(bucket).prefix("nodes/");
        if let Some(token) = &token {
            request = request.continuation_token(token);
        }
        let page = request.send().await.context("enumerate node leases")?;
        for object in page.contents() {
            let Some(node) = object
                .key()
                .and_then(|key| key.strip_prefix("nodes/"))
                .and_then(|key| key.strip_suffix(".json"))
            else {
                continue;
            };
            if !node.is_empty() {
                nodes.push(node.to_string());
            }
        }
        match page.next_continuation_token() {
            Some(next) => token = Some(next.to_string()),
            None => break,
        }
    }
    nodes.sort();
    nodes.dedup();
    Ok(nodes)
}

pub async fn run_deploy(arguments: Vec<String>) -> anyhow::Result<()> {
    let Some(mut options) = deploy::options_from_arguments(arguments)? else {
        deploy::print_help();
        return Ok(());
    };
    let env = |name: &str| {
        std::env::var(name)
            .ok()
            .filter(|value| !value.trim().is_empty())
    };
    if options.bucket.is_none() {
        options.bucket =
            env("CELLD_BUCKET").map(|value| value.trim_start_matches("s3://").to_string());
    }
    if options.endpoint.is_none() {
        options.endpoint = env("S3_ENDPOINT");
    }
    if !options.dry_run && options.bucket.is_none() {
        bail!("celld deploy requires --bucket s3://NAME (or CELLD_BUCKET)");
    }
    let built = deploy::build(&options)?;
    built.report();
    if options.dry_run {
        println!(
            "Current Version ID: {} (dry run; nothing written)",
            built.version
        );
        return Ok(());
    }

    let bucket = options.bucket.expect("validated deployment bucket");
    let region = options
        .region
        .or_else(|| env("AWS_REGION"))
        .or_else(|| env("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|| "us-east-1".to_string());
    let client = s3_client(options.endpoint.as_deref(), &region).await;
    validate_bucket(&client, &bucket).await?;
    let started = std::time::Instant::now();
    deploy::write(&client, &bucket, &built).await?;
    println!(
        "Uploaded {} ({:.2} sec)",
        built.script_name,
        started.elapsed().as_secs_f64()
    );
    println!("  s3://{bucket}/{}", built.prefix);
    println!("Current Version ID: {}", built.version);
    println!("Nodes load a deployment at startup; restart them to serve this version.");
    Ok(())
}

async fn get_string(client: &Client, bucket: &str, key: &str) -> anyhow::Result<String> {
    let object = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .with_context(|| format!("read s3://{bucket}/{key}"))?;
    let bytes = object
        .body
        .collect()
        .await
        .with_context(|| format!("read body s3://{bucket}/{key}"))?
        .into_bytes();
    String::from_utf8(bytes.to_vec()).context("deployment module is not UTF-8")
}

pub async fn load_current_worker(
    client: &Client,
    bucket: &str,
    node: String,
) -> anyhow::Result<LoadedDeployment> {
    load_worker_from_pointer(client, bucket, "deploy/current.json", node).await
}

pub async fn load_named_worker(
    client: &Client,
    bucket: &str,
    script: &str,
    node: String,
) -> anyhow::Result<LoadedDeployment> {
    load_worker_from_pointer(
        client,
        bucket,
        &format!("deploy/{script}/current.json"),
        node,
    )
    .await
}

async fn load_worker_from_pointer(
    client: &Client,
    bucket: &str,
    pointer_key: &str,
    node: String,
) -> anyhow::Result<LoadedDeployment> {
    let pointer: DeployPointer =
        serde_json::from_str(&get_string(client, bucket, pointer_key).await?)
            .with_context(|| format!("decode {pointer_key}"))?;
    let manifest: Manifest = serde_json::from_str(
        &get_string(client, bucket, &format!("{}/manifest.json", pointer.prefix)).await?,
    )
    .context("decode deployment manifest")?;
    let src = match manifest.main_module.as_deref() {
        Some(main) => get_string(client, bucket, &format!("{}/{main}", pointer.prefix)).await?,
        None if manifest.assets.is_some() => {
            // Ingress is handled by the immutable asset resolver. Keeping a
            // synthetic Worker makes the runtime construction path uniform
            // and is a fail-closed guard if an asset-only request escapes it.
            "export default { fetch() { return new Response('Not found', { status: 404 }); } };"
                .to_string()
        }
        None => bail!("deployment has neither a main module nor assets"),
    };
    let mut text = Vec::new();
    for module in &manifest.modules {
        if manifest.main_module.as_deref() == Some(module.name.as_str()) {
            continue;
        }
        let source = get_string(
            client,
            bucket,
            &format!("{}/{}", pointer.prefix, module.name),
        )
        .await?;
        text.push((format!("./{}", module.name), source));
    }
    let do_bindings = bindings(&manifest, "durable_object_namespace")
        .filter_map(|binding| {
            Some((
                binding.get("name")?.as_str()?.to_string(),
                binding.get("class_name")?.as_str()?.to_string(),
            ))
        })
        .collect();
    let r2_bindings = bindings(&manifest, "r2_bucket")
        .filter_map(|binding| binding.get("name")?.as_str().map(str::to_string))
        .collect();
    let ai_binding = configured_ai_binding(
        bindings(&manifest, "ai")
            .find_map(|binding| binding.get("name")?.as_str().map(str::to_string)),
    );
    let services = service_bindings(&manifest);
    let vars = worker_vars(&manifest)?;
    let compat = crate::worker_compat(&manifest.raw_metadata);
    let assets = match &manifest.assets {
        Some(reference) => Some(
            crate::assets::AssetResolver::load(
                client,
                bucket,
                &pointer.prefix,
                reference,
                manifest.main_module.is_none(),
            )
            .await?,
        ),
        None => None,
    };
    let asset_binding = assets
        .as_ref()
        .and_then(crate::assets::AssetResolver::binding_name)
        .map(str::to_string);
    let script_name = manifest.script_name.clone();
    Ok(LoadedDeployment {
        options: WorkerConfigOptions {
            src,
            script_name: script_name.clone(),
            do_classes: manifest.do_classes,
            bindings: do_bindings,
            r2_bindings,
            ai_binding,
            vars,
            node,
            text,
            compat,
        },
        script_name,
        asset_binding,
        assets,
        services,
    })
}

/// Apply celld's manifest-first precedence for the optional AI binding.
pub fn configured_ai_binding(manifest_binding: Option<String>) -> Option<String> {
    manifest_binding
        .or_else(|| std::env::var("CELLD_AI_BINDING").ok())
        .or_else(|| std::env::var_os("CELLD_AI_URL").map(|_| "AI".to_string()))
}

pub struct LoadedDeployment {
    pub options: WorkerConfigOptions,
    pub script_name: String,
    pub asset_binding: Option<String>,
    pub assets: Option<crate::assets::AssetResolver>,
    pub services: Vec<(String, String, Option<String>)>,
}

fn bindings<'a>(
    manifest: &'a Manifest,
    kind: &'a str,
) -> impl Iterator<Item = &'a serde_json::Value> {
    manifest
        .raw_metadata
        .get("bindings")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(move |binding| {
            binding.get("type").and_then(serde_json::Value::as_str) == Some(kind)
        })
}

fn service_bindings(manifest: &Manifest) -> Vec<(String, String, Option<String>)> {
    bindings(manifest, "service")
        .filter_map(|binding| {
            Some((
                binding.get("name")?.as_str()?.to_string(),
                binding.get("service")?.as_str()?.to_string(),
                binding
                    .get("entrypoint")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string),
            ))
        })
        .collect()
}

fn worker_vars(manifest: &Manifest) -> anyhow::Result<Vec<(String, String)>> {
    let mut vars = BTreeMap::new();
    for binding in bindings(manifest, "plain_text") {
        if let (Some(name), Some(value)) = (
            binding.get("name").and_then(serde_json::Value::as_str),
            binding.get("text").and_then(serde_json::Value::as_str),
        ) {
            vars.insert(name.to_string(), value.to_string());
        }
    }
    if let Ok(path) = std::env::var("CELLD_VARS_FILE") {
        let contents = std::fs::read_to_string(&path)
            .with_context(|| format!("read Worker vars file {path}"))?;
        for line in contents.lines().map(str::trim) {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some((name, raw)) = line.split_once('=') else {
                continue;
            };
            let name = name.trim();
            if name.is_empty() {
                continue;
            }
            let raw = raw.trim();
            let value = raw
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
                .or_else(|| {
                    raw.strip_prefix('\'')
                        .and_then(|value| value.strip_suffix('\''))
                })
                .unwrap_or(raw);
            vars.insert(name.to_string(), value.to_string());
        }
    }
    for (name, value) in std::env::vars() {
        if let Some(name) = name
            .strip_prefix("CELLD_VAR_")
            .filter(|name| !name.is_empty())
        {
            vars.insert(name.to_string(), value);
        }
    }
    Ok(vars.into_iter().collect())
}
