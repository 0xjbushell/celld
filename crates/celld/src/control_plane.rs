use anyhow::{anyhow, Context};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use crate::protocol::{asset_blob_key, AssetIndex, DeployPointer, Manifest};
use futures_util::{SinkExt, StreamExt};
use rand::RngCore;
use rusqlite::{types::ValueRef, Connection, OpenFlags, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tracing::{info, warn};

const DEFAULT_CONTROL_URL: &str = "https://celld.dev";
const DEFAULT_ENVIRONMENT: &str = "prod";
const MAX_PRESENCE_CELLS: usize = 50;
const MAX_PRESENCE_CELL_ID_BYTES: usize = 200;
const MAX_EXPLORER_CELL_PAGE: i32 = 100;
const MAX_EXPLORER_TABLES: usize = 100;
const MAX_EXPLORER_COLUMNS: usize = 32;
const MAX_EXPLORER_ROWS: usize = 25;
const MAX_EXPLORER_VALUE_BYTES: usize = 2 * 1024;
const MAX_EXPLORER_RESPONSE_BYTES: usize = 96 * 1024;

fn restart_on_deployment_enabled() -> bool {
    !std::env::var("CELLD_CLOUD_RESTART_ON_DEPLOY")
        .is_ok_and(|value| value.eq_ignore_ascii_case("off"))
}

#[cfg(unix)]
fn restart_for_deployment() -> ! {
    use std::os::unix::process::CommandExt;
    use std::process::Command;

    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => {
            warn!(
                event = "control_plane_restart_exec_failed",
                %error,
                "could not locate celld for deployment reload; exiting for an external supervisor"
            );
            std::process::exit(75);
        }
    };
    let error = Command::new(executable)
        .args(std::env::args_os().skip(1))
        .exec();
    warn!(
        event = "control_plane_restart_exec_failed",
        %error,
        "could not re-exec celld for deployment reload; exiting for an external supervisor"
    );
    std::process::exit(75);
}

#[cfg(not(unix))]
fn restart_for_deployment() -> ! {
    std::process::exit(75);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManagedRuntimeState {
    Connected,
    ControlPlaneUnavailable,
    BucketUnavailable,
    CredentialRevoked,
}

impl ManagedRuntimeState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Connected => "connected",
            Self::ControlPlaneUnavailable => "control_plane_unavailable",
            Self::BucketUnavailable => "bucket_unavailable",
            Self::CredentialRevoked => "credential_revoked",
        }
    }
}

static MANAGED_RUNTIME_STATE: OnceLock<Mutex<Option<ManagedRuntimeState>>> = OnceLock::new();

pub fn report_managed_runtime_state(state: ManagedRuntimeState) {
    let mut current = MANAGED_RUNTIME_STATE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap();
    if *current == Some(state) {
        return;
    }
    *current = Some(state);
    drop(current);
    let managed_state = state.as_str();
    match state {
        ManagedRuntimeState::Connected => info!(
            event = "managed_runtime_state",
            managed_state,
            "Managed Control Plane connected"
        ),
        ManagedRuntimeState::ControlPlaneUnavailable => info!(
            event = "managed_runtime_state",
            managed_state,
            "Managed Control Plane unavailable; bucket-backed serving continues"
        ),
        ManagedRuntimeState::BucketUnavailable => warn!(
            event = "managed_runtime_state",
            managed_state,
            "fleet bucket unavailable; serving cannot start"
        ),
        // Name the remedy: this condition can be cleared by a restart or an
        // explicit credential refresh.
        ManagedRuntimeState::CredentialRevoked => warn!(
            event = "managed_runtime_state",
            managed_state,
            "managed storage credential was rejected; restart celld to fetch a \
             fresh one, or run `celld credentials refresh` if a rotation is pending"
        ),
    }
}

#[derive(Debug)]
struct ManagedControlCredentialRevoked;

impl std::fmt::Display for ManagedControlCredentialRevoked {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(
            "Managed Control Plane credential was rejected or revoked.\n\
             Restart celld to fetch a fresh credential. If that does not help, \
             run `celld credentials refresh`, and `celld diagnose` to check \
             bucket access.",
        )
    }
}

impl std::error::Error for ManagedControlCredentialRevoked {}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ConnectOptions {
    control_url: String,
    environment: String,
    force: bool,
    byo_storage: Option<ByoStorageConfig>,
}

#[derive(Clone, Deserialize, Serialize)]
struct CloudConfig {
    #[serde(default = "installation_schema_version")]
    version: u32,
    #[serde(alias = "instance_id")]
    installation_id: String,
    #[serde(alias = "instance_token")]
    installation_token: Option<String>,
    #[serde(default = "initial_credential_version")]
    credential_version: u64,
    fleet_id: Option<String>,
    environment_id: Option<String>,
    control_url: String,
    environment: String,
    #[serde(default)]
    storage: Option<ManagedStorageConfig>,
    #[serde(default)]
    byo_storage: Option<ByoStorageConfig>,
    #[serde(default)]
    credential_handoff_id: Option<String>,
    #[serde(default)]
    previous_credential: Option<PreviousCredential>,
    #[serde(default)]
    pending_claim: Option<PendingClaim>,
}

#[derive(Clone, Deserialize, Serialize)]
struct PreviousCredential {
    installation_token: String,
    credential_version: u64,
    storage: ManagedStorageConfig,
}

#[derive(Clone, Deserialize, Serialize)]
struct PendingClaim {
    verification_uri_complete: String,
    expires_at_ms: u64,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct ManagedStorageConfig {
    pub bucket: String,
    pub endpoint: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(default)]
    pub session_token: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct ByoStorageConfig {
    pub bucket: String,
    #[serde(default)]
    pub endpoint: Option<String>,
    pub region: String,
}

#[derive(Clone)]
pub enum InstallationStorageConfig {
    Managed(ManagedStorageConfig),
    Byo(ByoStorageConfig),
}

#[derive(Serialize)]
struct CreateClaimRequest<'a> {
    instance_id: &'a str,
    instance_name: &'a str,
    environment: &'a str,
    version: &'static str,
    platform: String,
    storage_origin: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    bucket: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    endpoint: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    region: Option<&'a str>,
}

#[derive(Deserialize)]
struct CreateClaimResponse {
    device_code: String,
    verification_uri_complete: String,
    expires_in: u64,
    interval: u64,
}

#[derive(Serialize)]
struct PollClaimRequest<'a> {
    device_code: &'a str,
}

#[derive(Deserialize)]
struct PollClaimResponse {
    status: String,
    #[serde(default = "managed_storage_origin")]
    storage_origin: String,
    credential_handoff_id: Option<String>,
    credential_version: Option<u64>,
    instance_token: Option<String>,
    fleet_id: Option<String>,
    environment_id: Option<String>,
    bucket: Option<String>,
    endpoint: Option<String>,
    region: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
    interval: Option<u64>,
}

#[derive(Deserialize)]
struct CredentialRotationResponse {
    status: String,
    credential_handoff_id: String,
    credential_version: u64,
    instance_token: String,
    bucket: String,
    endpoint: String,
    region: String,
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

#[derive(Deserialize)]
struct AgentCommandEnvelope {
    command: AgentCommand,
}

#[derive(Deserialize)]
struct AgentCommand {
    id: String,
    #[serde(rename = "type")]
    kind: String,
    deployment: AgentDeployment,
}

#[derive(Deserialize)]
struct AgentDeployment {
    script_name: String,
    version: String,
    manifest: Manifest,
    pointer: DeployPointer,
    modules: Vec<AgentModule>,
    #[serde(default)]
    assets: Option<AgentAssets>,
}

#[derive(Deserialize)]
struct AgentModule {
    name: String,
    sha256: String,
    download_url: String,
}

#[derive(Deserialize)]
struct AgentAssets {
    index_download_url: String,
    blob_download_base_url: String,
    sha256: String,
    file_count: u32,
    total_bytes: u64,
}

#[derive(Serialize)]
struct CompleteCommandRequest<'a> {
    success: bool,
    error: Option<&'a str>,
}

struct AppliedDeployment {
    id: String,
    script_name: String,
}

pub async fn handle_connect_command(
    arguments: impl IntoIterator<Item = String>,
) -> anyhow::Result<()> {
    let mut options = options_from_env()?;
    let mut args = arguments.into_iter();
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--env" => {
                options.environment = args.next().context("--env requires a value")?;
            }
            "--control-url" => {
                options.control_url = args.next().context("--control-url requires a value")?;
            }
            "--force" => options.force = true,
            "--help" | "-h" => {
                if let Some(argument) = args.next() {
                    return Err(anyhow!(
                        "unexpected argument after connect --help: {argument}"
                    ));
                }
                print_connect_help();
                return Ok(());
            }
            _ => return Err(anyhow!("unknown connect option: {argument}")),
        }
    }
    validate_environment(&options.environment)?;
    connect(options, true).await
}

/// `celld token` — print the fleet's deploy token and the exact commands to
/// use it.
///
/// Wrangler expects deployment credentials in environment variables, so this
/// command exposes the connected fleet's short-lived token in a shell-friendly
/// form.
pub async fn handle_token_command(
    arguments: impl IntoIterator<Item = String>,
) -> anyhow::Result<()> {
    let arguments = arguments.into_iter().collect::<Vec<_>>();
    if matches!(arguments.as_slice(), [a] if a == "--help" || a == "-h") {
        println!(
            "Print this fleet's deploy token, and how to deploy with it.\n\n\
             USAGE:\n  celld token [--format shell]\n\n\
             OPTIONS:\n  --format shell  Print only shell exports, suitable for `eval`.\n"
        );
        return Ok(());
    }
    let shell_only = match arguments.as_slice() {
        [] => false,
        [format, value] if format == "--format" && value == "shell" => true,
        _ => {
            return Err(anyhow!(
                "expected `celld token` or `celld token --format shell`; \
                 run `celld token --help` for usage"
            ))
        }
    };
    let path = config_path()?;
    let contents = std::fs::read_to_string(&path).with_context(|| {
        format!(
            "read {}. This celld installation is not enrolled yet — run `celld` \
             and approve the link it prints.",
            path.display()
        )
    })?;
    let config: CloudConfig =
        serde_json::from_str(&contents).with_context(|| format!("decode {}", path.display()))?;
    let token = config.installation_token.as_deref().context(
        "this celld installation is not enrolled in the Managed Control Plane — \
         run `celld` and approve the link it prints",
    )?;
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/api/agent/deploy-token", config.control_url))
        .bearer_auth(token)
        .send()
        .await
        .context("request a deploy token")?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!(
            "could not get a deploy token ({status}): {body}\n\
             If this installation was disconnected, run `celld` to re-enrol."
        ));
    }
    #[derive(serde::Deserialize)]
    struct DeployTokenResponse {
        token: String,
        account_id: String,
        api_base_url: String,
    }
    let issued: DeployTokenResponse =
        response.json().await.context("decode deploy token")?;
    print!(
        "{}",
        render_deploy_token(
            &issued.token,
            &issued.account_id,
            &issued.api_base_url,
            shell_only,
        )
    );
    Ok(())
}

fn render_deploy_token(
    token: &str,
    account_id: &str,
    api_base_url: &str,
    shell_only: bool,
) -> String {
    let exports = format!(
        "export CLOUDFLARE_API_TOKEN={}\n\
         export CLOUDFLARE_ACCOUNT_ID={}\n\
         export CLOUDFLARE_API_BASE_URL={}\n",
        shell_quote(token),
        shell_quote(account_id),
        shell_quote(api_base_url),
    );
    if shell_only {
        return exports;
    }
    // Say what to do next, not merely what happened.
    format!(
        "Deploy token for this fleet:\n\n  {token}\n\n\
         Deploy your app with:\n\n{exports}npx wrangler@4 deploy\n"
    )
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

pub async fn handle_credentials_command(
    arguments: impl IntoIterator<Item = String>,
) -> anyhow::Result<()> {
    let arguments = arguments.into_iter().collect::<Vec<_>>();
    if matches!(arguments.as_slice(), [argument] if argument == "--help" || argument == "-h") {
        print_credentials_help();
        return Ok(());
    }
    if !matches!(arguments.as_slice(), [command] if command == "refresh") {
        return Err(anyhow!(
            "expected `celld credentials refresh`; run `celld credentials --help` for usage"
        ));
    }
    refresh_credentials().await
}

pub async fn handle_disconnect_command(
    arguments: impl IntoIterator<Item = String>,
) -> anyhow::Result<()> {
    let arguments = arguments.into_iter().collect::<Vec<_>>();
    if matches!(arguments.as_slice(), [argument] if argument == "--help" || argument == "-h") {
        print_disconnect_help();
        return Ok(());
    }
    if !matches!(arguments.as_slice(), [confirmation] if confirmation == "--yes") {
        return Err(anyhow!(
            "disconnect revokes every process sharing this installation; rerun `celld disconnect --yes` to confirm"
        ));
    }
    let path = config_path()?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?;
    let archive = disconnect_at_path(path, &client).await?;
    println!(
        "Disconnected this installation from the Managed Control Plane.\nArchived its revoked local record at {}.",
        archive.display()
    );
    Ok(())
}

async fn disconnect_at_path(path: PathBuf, client: &reqwest::Client) -> anyhow::Result<PathBuf> {
    let _lock = acquire_enrollment_lock(&path).await?;
    let contents =
        std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    let config: CloudConfig =
        serde_json::from_str(&contents).with_context(|| format!("decode {}", path.display()))?;
    let token = config
        .installation_token
        .as_deref()
        .context("this celld installation is not enrolled in the Managed Control Plane")?;
    let response = client
        .post(format!(
            "{}/api/agent/credentials/revoke",
            config.control_url
        ))
        .bearer_auth(token)
        .json(&serde_json::json!({ "installation_id": config.installation_id }))
        .send()
        .await
        .context("revoke managed installation")?;
    if !response.status().is_success() {
        return Err(anyhow!(
            "managed installation revocation returned {}",
            response.status()
        ));
    }

    // Keep a recoverable, permission-preserving record rather than deleting
    // secrets in place. The provider credential is already revoked, and bare
    // `celld` will create a fresh installation record on its next start.
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .context("managed installation path has no UTF-8 file name")?;
    let archive = path.with_file_name(format!("{file_name}.disconnected-{}", current_time_ms()));
    std::fs::rename(&path, &archive).with_context(|| {
        format!(
            "archive revoked installation record as {}",
            archive.display()
        )
    })?;
    Ok(archive)
}

async fn refresh_credentials() -> anyhow::Result<()> {
    let path = config_path()?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?;
    refresh_credentials_at_path(path, &client).await
}

async fn refresh_credentials_at_path(
    path: PathBuf,
    client: &reqwest::Client,
) -> anyhow::Result<()> {
    let _lock = acquire_enrollment_lock(&path).await?;
    let contents =
        std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    let mut config: CloudConfig =
        serde_json::from_str(&contents).with_context(|| format!("decode {}", path.display()))?;
    let token = config
        .installation_token
        .as_deref()
        .context("this celld installation is not enrolled in the Managed Control Plane")?;
    let current_storage = config
        .storage
        .as_ref()
        .context("managed installation record has no storage credentials")?;

    if let Some(handoff_id) = config.credential_handoff_id.clone() {
        acknowledge_credential_handoff(client, &config, &handoff_id)
            .await
            .context("finish previously saved credential refresh")?;
        config.credential_handoff_id = None;
        config.previous_credential = None;
        save_config(&path, &config)?;
        println!(
            "Managed credential refresh completed (version {}).",
            config.credential_version
        );
        return Ok(());
    }

    let response = client
        .post(format!("{}/api/agent/credentials/next", config.control_url))
        .bearer_auth(token)
        .json(&serde_json::json!({}))
        .send()
        .await
        .context("request staged managed credential")?;
    if response.status() == reqwest::StatusCode::NO_CONTENT {
        println!("No managed credential rotation is pending.");
        return Ok(());
    }
    if response.status() == reqwest::StatusCode::UNAUTHORIZED {
        return Err(anyhow!(
            "this installation credential is revoked; enroll a new installation"
        ));
    }
    if !response.status().is_success() {
        return Err(anyhow!(
            "credential refresh endpoint returned {}",
            response.status()
        ));
    }
    let replacement: CredentialRotationResponse = response
        .json()
        .await
        .context("decode staged managed credential")?;
    validate_credential_rotation(&replacement, &config, current_storage)?;

    config.previous_credential = Some(PreviousCredential {
        installation_token: token.to_string(),
        credential_version: config.credential_version,
        storage: current_storage.clone(),
    });
    config.installation_token = Some(replacement.instance_token);
    config.credential_version = replacement.credential_version;
    config.storage = Some(ManagedStorageConfig {
        bucket: replacement.bucket,
        endpoint: replacement.endpoint,
        region: replacement.region,
        access_key_id: replacement.access_key_id,
        secret_access_key: replacement.secret_access_key,
        session_token: replacement.session_token,
    });
    config.credential_handoff_id = Some(replacement.credential_handoff_id.clone());

    // The replacement is persisted before acknowledgement. If this process is
    // killed after the rename, the marker makes the acknowledgement retryable.
    // The managed control plane keeps both token hashes valid until this
    // acknowledgement succeeds.
    save_config(&path, &config)?;
    acknowledge_credential_handoff(client, &config, &replacement.credential_handoff_id)
        .await
        .context(
            "replacement credential is saved, but acknowledgement is pending; rerun `celld credentials refresh`",
        )?;
    config.credential_handoff_id = None;
    config.previous_credential = None;
    save_config(&path, &config)?;
    println!(
        "Managed credential refresh completed (version {}).",
        config.credential_version
    );
    Ok(())
}

fn validate_credential_rotation(
    replacement: &CredentialRotationResponse,
    config: &CloudConfig,
    current: &ManagedStorageConfig,
) -> anyhow::Result<()> {
    if replacement.status != "rotation_pending" {
        return Err(anyhow!(
            "unexpected credential refresh status: {}",
            replacement.status
        ));
    }
    if replacement.credential_version != config.credential_version.saturating_add(1) {
        return Err(anyhow!(
            "credential refresh version is not the next installation version"
        ));
    }
    if replacement.bucket != current.bucket
        || replacement.endpoint != current.endpoint
        || replacement.region != current.region
    {
        return Err(anyhow!(
            "credential refresh attempted to change the installation storage target"
        ));
    }
    for (name, value) in [
        (
            "credential handoff",
            replacement.credential_handoff_id.as_str(),
        ),
        ("installation token", replacement.instance_token.as_str()),
        ("storage access key", replacement.access_key_id.as_str()),
        ("storage secret key", replacement.secret_access_key.as_str()),
    ] {
        if value.is_empty() || value.len() > 500 {
            return Err(anyhow!("credential refresh returned an invalid {name}"));
        }
    }
    Ok(())
}

pub async fn connect_on_startup_with_storage(
    byo_storage: Option<ByoStorageConfig>,
) -> anyhow::Result<()> {
    let mut options = options_from_env()?;
    options.byo_storage = byo_storage;
    validate_environment(&options.environment)?;
    connect(options, true).await
}

pub fn installation_storage() -> anyhow::Result<InstallationStorageConfig> {
    let config = connected_config()?.context("managed installation is not connected")?;
    match (config.storage, config.byo_storage) {
        (Some(storage), None) => Ok(InstallationStorageConfig::Managed(storage)),
        (None, Some(storage)) => Ok(InstallationStorageConfig::Byo(storage)),
        (None, None) => Err(anyhow!("managed installation record has no storage target")),
        (Some(_), Some(_)) => Err(anyhow!(
            "managed installation record contains conflicting storage targets"
        )),
    }
}

pub struct PresenceRuntime {
    pub s3: S3Client,
    pub repl: Arc<crate::replication::NodeRepl>,
    pub bucket: String,
    pub litestream: String,
    pub endpoint: Option<String>,
    pub region: String,
    pub storage_credentials: Option<crate::replication::StorageCredentials>,
    pub node_session_id: String,
    pub advertise: String,
    pub listen: String,
    pub runtime_ready: Arc<AtomicBool>,
    pub owned_cells: Arc<AtomicUsize>,
    pub owned_cell_inventory: Arc<Mutex<HashMap<String, u64>>>,
    pub lease_manager: Arc<crate::ownership::NodeLeaseManager>,
}

pub fn start_presence_agent(runtime: PresenceRuntime) -> bool {
    let config = match connected_config() {
        Ok(Some(config)) => config,
        Ok(None) => return false,
        Err(error) => {
            warn!(event = "control_plane_presence_configuration_error", %error);
            return false;
        }
    };
    let hostname = machine_hostname();
    tokio::spawn(async move {
        let mut consecutive_failures = 0_u32;
        loop {
            let started = Instant::now();
            let result = presence_session(&config, &runtime, &hostname).await;
            if started.elapsed() >= Duration::from_secs(30) {
                consecutive_failures = 0;
            }
            match result {
                Ok(()) => {}
                Err(error) if consecutive_failures == 0 => {
                    report_managed_runtime_state(
                        if error.is::<ManagedControlCredentialRevoked>() {
                            ManagedRuntimeState::CredentialRevoked
                        } else {
                            ManagedRuntimeState::ControlPlaneUnavailable
                        },
                    );
                    info!(
                        event = "control_plane_presence_unavailable",
                        %error,
                        "Managed Control Plane presence unavailable; bucket-backed serving continues"
                    );
                }
                Err(_) => {}
            }
            consecutive_failures = consecutive_failures.saturating_add(1);
            let exponent = consecutive_failures.saturating_sub(1).min(5);
            let seconds = 2_u64.saturating_mul(1_u64 << exponent).min(60);
            let jitter_ms = (rand::random::<u16>() as u64) % 1000;
            tokio::time::sleep(Duration::from_millis(seconds * 1000 + jitter_ms)).await;
        }
    });
    true
}

async fn presence_session(
    config: &CloudConfig,
    runtime: &PresenceRuntime,
    hostname: &str,
) -> anyhow::Result<()> {
    let PresenceRuntime {
        s3,
        bucket,
        node_session_id,
        advertise,
        listen,
        runtime_ready,
        owned_cells,
        owned_cell_inventory,
        lease_manager,
        ..
    } = runtime;
    let request = presence_request(
        config,
        node_session_id,
        advertise,
        listen,
        hostname,
    )?;
    let (mut socket, _) = match tokio_tungstenite::connect_async(request).await {
        Ok(connected) => connected,
        Err(tokio_tungstenite::tungstenite::Error::Http(response))
            if matches!(response.status().as_u16(), 401 | 403) =>
        {
            return Err(ManagedControlCredentialRevoked.into());
        }
        Err(error) => {
            return Err(error).context("connect Managed Control Plane presence");
        }
    };
    report_managed_runtime_state(ManagedRuntimeState::Connected);
    info!(
        event = "control_plane_presence_connected",
        %node_session_id,
        %advertise,
        "node session connected to the Managed Control Plane"
    );
    let heartbeat_period = std::env::var("CELLD_PRESENCE_HEARTBEAT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| (50..=30_000).contains(value))
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_secs(30));
    let mut heartbeat = tokio::time::interval(heartbeat_period);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let shadow_presence = std::env::var("CELLD_PRESENCE_SHADOW").as_deref() == Ok("on");
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                let lease_shadow = if shadow_presence {
                    Some(lease_manager.shadow_observation().await)
                } else {
                    None
                };
                let lease = lease_manager.metrics().await;
                let lifecycle_shadow = lease_manager.shadow_decisions();
                let activity = crate::advisory_activity().snapshot();
                let (cells, cells_truncated) = {
                    let inventory = owned_cell_inventory.lock().unwrap();
                    let mut cells = inventory
                        .iter()
                        .filter(|(id, _)| id.len() <= MAX_PRESENCE_CELL_ID_BYTES)
                        .map(|(id, epoch)| serde_json::json!({
                            "id": id,
                            "epoch": epoch,
                        }))
                        .collect::<Vec<_>>();
                    cells.sort_by(|left, right| {
                        left["id"].as_str().cmp(&right["id"].as_str())
                    });
                    let truncated = cells.len() < inventory.len() ||
                        cells.len() > MAX_PRESENCE_CELLS;
                    cells.truncate(MAX_PRESENCE_CELLS);
                    (cells, truncated)
                };
                let mut message = serde_json::json!({
                    "serving": runtime_ready.load(Ordering::Relaxed),
                    "owned_cells": owned_cells.load(Ordering::Relaxed),
                    "cells": cells,
                    "cells_truncated": cells_truncated,
                    "lease": {
                        "active": lease.active,
                        "active_cells": lease.active_cells,
                        "active_ms": lease.active_ms,
                        "class_a_writes": lease.class_a_writes,
                        "class_b_reads": lease.class_b_reads,
                    },
                    "activity": {
                        "acquired": activity.acquired,
                        "proxied": activity.proxied,
                        "expired_owner_leases": activity.expired_owner_leases,
                        "restored": activity.restored,
                        "advanced_epochs": activity.advanced_epochs,
                    },
                });
                if let Some(observation) = lease_shadow {
                    message["lease_shadow"] = serde_json::to_value(observation)?;
                }
                if !lifecycle_shadow.decisions.is_empty() || lifecycle_shadow.dropped > 0 {
                    message["lazy_lease_shadow"] = serde_json::to_value(lifecycle_shadow)?;
                }
                socket
                    .send(tokio_tungstenite::tungstenite::Message::Text(
                        message.to_string().into(),
                    ))
                    .await?;
            }
            message = socket.next() => {
                match message {
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Ping(payload))) => {
                        socket.send(
                            tokio_tungstenite::tungstenite::Message::Pong(payload)
                        ).await?;
                    }
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                        let message = serde_json::from_str::<serde_json::Value>(&text).ok();
                        let message_type = message.as_ref()
                            .and_then(|value| value.get("type"))
                            .and_then(|kind| kind.as_str())
                            .map(str::to_string);
                        match message_type.as_deref() {
                            Some("explorer_request") => {
                                let response = handle_explorer_request(
                                    message.as_ref().unwrap(),
                                    runtime,
                                ).await;
                                socket
                                    .send(tokio_tungstenite::tungstenite::Message::Text(
                                        response.to_string().into(),
                                    ))
                                    .await?;
                            }
                            Some("deployment") => {
                                let client = reqwest::Client::builder()
                                    .timeout(Duration::from_secs(30))
                                    .build()?;
                                if poll_and_apply(&client, config, s3, bucket).await?.is_some()
                                    && runtime_ready.load(Ordering::SeqCst)
                                    && restart_on_deployment_enabled()
                                {
                                    restart_for_deployment();
                                }
                            }
                            Some("deployment_current")
                                if runtime_ready.load(Ordering::SeqCst)
                                    && restart_on_deployment_enabled() =>
                            {
                                restart_for_deployment();
                            }
                            _ => {}
                        }
                    }
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_))) | None => {
                        return Err(anyhow!("Managed Control Plane closed presence"));
                    }
                    Some(Ok(_)) => {}
                    Some(Err(error)) => return Err(error.into()),
                }
            }
        }
    }
}

async fn handle_explorer_request(
    message: &serde_json::Value,
    runtime: &PresenceRuntime,
) -> serde_json::Value {
    let PresenceRuntime {
        s3,
        repl,
        bucket,
        litestream,
        endpoint,
        region,
        storage_credentials,
        owned_cell_inventory,
        ..
    } = runtime;
    let request_id = message
        .get("request_id")
        .and_then(|value| value.as_str())
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
        .unwrap_or("");
    let operation = message.get("operation").and_then(|value| value.as_str());
    let result = match operation {
        Some("list_cells") => {
            let cursor = match message.get("cursor") {
                None | Some(serde_json::Value::Null) => None,
                Some(serde_json::Value::String(cursor)) if cursor.len() <= 2_048 => {
                    Some(cursor.as_str())
                }
                _ => return explorer_error(request_id, "invalid_request"),
            };
            list_durable_cells(s3, bucket, cursor).await
        }
        Some("inspect_cell") => {
            let Some(cell) = message
                .get("cell_id")
                .and_then(|value| value.as_str())
                .filter(|cell| valid_explorer_cell_id(cell))
            else {
                return explorer_error(request_id, "invalid_request");
            };
            let table = match message.get("table") {
                None | Some(serde_json::Value::Null) => None,
                Some(serde_json::Value::String(table))
                    if !table.is_empty() && table.len() <= 200 =>
                {
                    Some(table.clone())
                }
                _ => return explorer_error(request_id, "invalid_request"),
            };
            let active_epoch = owned_cell_inventory.lock().unwrap().get(cell).copied();
            if let Some(epoch) = active_epoch {
                let repl = repl.clone();
                let cell = cell.to_string();
                match tokio::task::spawn_blocking(move || {
                    let Some(snapshot) = repl.snapshot_active(&cell, epoch)? else {
                        return Ok(None);
                    };
                    inspect_snapshot(snapshot, &cell, table.as_deref(), "active").map(Some)
                })
                .await
                {
                    Ok(Ok(Some(result))) => Ok(result),
                    Ok(Ok(None)) => {
                        return explorer_error(request_id, "active_snapshot_unavailable");
                    }
                    Ok(Err(error)) => Err(error.context("inspect active cell snapshot")),
                    Err(error) => Err(anyhow!("active inspection task failed: {error}")),
                }
            } else {
                match crate::replication::restore_snapshot(
                    s3,
                    litestream,
                    bucket,
                    cell,
                    endpoint.as_deref(),
                    region,
                    storage_credentials.as_ref(),
                )
                .await
                {
                    Ok(Some(snapshot)) => {
                        let cell = cell.to_string();
                        match tokio::task::spawn_blocking(move || {
                            inspect_snapshot(snapshot, &cell, table.as_deref(), "replicated")
                        })
                        .await
                        {
                            Ok(result) => result,
                            Err(error) => Err(anyhow!("inspection task failed: {error}")),
                        }
                    }
                    Ok(None) => return explorer_error(request_id, "snapshot_not_found"),
                    Err(error) => Err(error.context("restore replicated snapshot")),
                }
            }
        }
        _ => return explorer_error(request_id, "invalid_request"),
    };
    match result {
        Ok(result) => serde_json::json!({
            "type": "explorer_response",
            "request_id": request_id,
            "ok": true,
            "result": result,
        }),
        Err(error) => {
            let stable_error = if error.chain().any(|cause| cause.to_string() == "table_not_found") {
                "table_not_found"
            } else if error
                .chain()
                .any(|cause| cause.to_string() == "table_not_previewable")
            {
                "table_not_previewable"
            } else {
                "inspection_failed"
            };
            warn!(
                event = "control_plane_explorer_request_failed",
                request_id,
                %error,
                "read-only cell inspection failed"
            );
            explorer_error(request_id, stable_error)
        }
    }
}

fn explorer_error(request_id: &str, error: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "explorer_response",
        "request_id": request_id,
        "ok": false,
        "error": error,
    })
}

fn valid_explorer_cell_id(cell: &str) -> bool {
    !cell.is_empty()
        && cell.len() <= MAX_PRESENCE_CELL_ID_BYTES
        && !cell.contains('/')
        && !cell.contains('\\')
        && !cell.contains('?')
        && !cell.contains('#')
        && !cell.contains("..")
        && cell.bytes().all(|byte| byte.is_ascii_graphic())
}

async fn list_durable_cells(
    s3: &S3Client,
    bucket: &str,
    cursor: Option<&str>,
) -> anyhow::Result<serde_json::Value> {
    let mut request = s3
        .list_objects_v2()
        .bucket(bucket)
        .prefix("cells/")
        .delimiter("/")
        .max_keys(MAX_EXPLORER_CELL_PAGE);
    if let Some(cursor) = cursor {
        request = request.continuation_token(cursor);
    }
    let page = request.send().await?;
    let cells = page
        .common_prefixes()
        .iter()
        .filter_map(|prefix| prefix.prefix())
        .filter_map(|prefix| {
            prefix
                .strip_prefix("cells/")
                .and_then(|cell| cell.strip_suffix('/'))
        })
        .filter(|cell| valid_explorer_cell_id(cell))
        .map(str::to_string)
        .collect::<Vec<_>>();
    Ok(serde_json::json!({
        "cells": cells,
        "next_cursor": page.next_continuation_token(),
    }))
}

fn inspect_snapshot(
    snapshot: crate::replication::RestoredSnapshot,
    cell: &str,
    selected_table: Option<&str>,
    source: &str,
) -> anyhow::Result<serde_json::Value> {
    let connection = Connection::open_with_flags(
        snapshot.path(),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let mut schema = connection.prepare(
        "SELECT name, type, sql
           FROM sqlite_schema
          WHERE type IN ('table', 'view')
            AND name NOT LIKE 'sqlite_%'
          ORDER BY name
          LIMIT ?1",
    )?;
    let mut rows = schema.query([i64::try_from(MAX_EXPLORER_TABLES + 1)?])?;
    let mut tables = Vec::new();
    let mut schema_bytes = 0_usize;
    let mut tables_truncated = false;
    while let Some(row) = rows.next()? {
        if tables.len() >= MAX_EXPLORER_TABLES {
            tables_truncated = true;
            break;
        }
        let name: String = row.get(0)?;
        let kind: String = row.get(1)?;
        let sql: Option<String> = row.get(2)?;
        let entry = serde_json::json!({
            "name": name,
            "kind": kind,
            "sql": sql.map(|sql| truncate_utf8(&sql, 4 * 1024).0),
        });
        let entry_bytes = serde_json::to_vec(&entry)?.len();
        if schema_bytes + entry_bytes > MAX_EXPLORER_RESPONSE_BYTES / 2 {
            tables_truncated = true;
            break;
        }
        schema_bytes += entry_bytes;
        tables.push(entry);
    }
    drop(rows);
    drop(schema);
    let preview = match selected_table {
        Some(table) => Some(preview_table(&connection, table)?),
        None => None,
    };
    Ok(serde_json::json!({
        "cell_id": cell,
        "epoch": snapshot.epoch,
        "source": source,
        "tables": tables,
        "tables_truncated": tables_truncated,
        "preview": preview,
    }))
}

fn preview_table(connection: &Connection, table: &str) -> anyhow::Result<serde_json::Value> {
    let kind = connection
        .query_row(
            "SELECT type FROM sqlite_schema WHERE name = ?1 AND type IN ('table', 'view')",
            [table],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .context("table_not_found")?;
    if kind != "table" {
        anyhow::bail!("table_not_previewable");
    }
    let quoted = table.replace('"', "\"\"");
    let mut statement = connection.prepare(&format!(
        "SELECT * FROM \"{quoted}\" LIMIT {}",
        MAX_EXPLORER_ROWS + 1,
    ))?;
    let columns = statement
        .column_names()
        .iter()
        .take(MAX_EXPLORER_COLUMNS)
        .map(|name| truncate_utf8(name, 200).0)
        .collect::<Vec<_>>();
    let column_count = columns.len();
    let all_column_count = statement.column_count();
    let mut query = statement.query([])?;
    let mut rows = Vec::new();
    let mut response_bytes = 0_usize;
    let mut truncated = all_column_count > MAX_EXPLORER_COLUMNS;
    while let Some(row) = query.next()? {
        if rows.len() >= MAX_EXPLORER_ROWS {
            truncated = true;
            break;
        }
        let mut values = Vec::with_capacity(column_count);
        for index in 0..column_count {
            values.push(explorer_value(row.get_ref(index)?));
        }
        let row_bytes = serde_json::to_vec(&values)?.len();
        if response_bytes + row_bytes > MAX_EXPLORER_RESPONSE_BYTES / 2 {
            truncated = true;
            break;
        }
        response_bytes += row_bytes;
        rows.push(values);
    }
    Ok(serde_json::json!({
        "table": table,
        "columns": columns,
        "rows": rows,
        "truncated": truncated,
    }))
}

fn explorer_value(value: ValueRef<'_>) -> serde_json::Value {
    match value {
        ValueRef::Null => serde_json::json!({ "type": "null", "value": null }),
        ValueRef::Integer(value) => {
            serde_json::json!({ "type": "integer", "value": value.to_string() })
        }
        ValueRef::Real(value) => {
            serde_json::json!({ "type": "real", "value": value.to_string() })
        }
        ValueRef::Text(value) => {
            let text = String::from_utf8_lossy(value);
            let (text, truncated) = truncate_utf8(&text, MAX_EXPLORER_VALUE_BYTES);
            serde_json::json!({
                "type": "text",
                "value": text,
                "bytes": value.len(),
                "truncated": truncated,
            })
        }
        ValueRef::Blob(value) => {
            let shown = &value[..value.len().min(MAX_EXPLORER_VALUE_BYTES)];
            serde_json::json!({
                "type": "blob",
                "value": hex(shown),
                "bytes": value.len(),
                "truncated": shown.len() < value.len(),
            })
        }
    }
}

fn truncate_utf8(value: &str, max_bytes: usize) -> (String, bool) {
    if value.len() <= max_bytes {
        return (value.to_string(), false);
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    (value[..end].to_string(), true)
}

fn hex(value: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value {
        encoded.push(DIGITS[(byte >> 4) as usize] as char);
        encoded.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn presence_request(
    config: &CloudConfig,
    node_session_id: &str,
    advertise: &str,
    listen: &str,
    hostname: &str,
) -> anyhow::Result<tokio_tungstenite::tungstenite::http::Request<()>> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::http::header::{HeaderName, HeaderValue, AUTHORIZATION};

    let token = config
        .installation_token
        .as_deref()
        .context("managed installation has no token")?;
    let mut url = url::Url::parse(&config.control_url)?;
    url.set_scheme(if url.scheme() == "https" { "wss" } else { "ws" })
        .map_err(|_| anyhow!("invalid Managed Control Plane WebSocket scheme"))?;
    url.set_path("/api/agent/presence");
    url.query_pairs_mut()
        .append_pair("node_session_id", node_session_id)
        .append_pair("advertise", advertise)
        .append_pair("listen", listen);
    let mut request = url.as_str().into_client_request()?;
    request.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}"))?,
    );
    request.headers_mut().insert(
        HeaderName::from_static("x-cells-version"),
        HeaderValue::from_static(env!("CARGO_PKG_VERSION")),
    );
    request.headers_mut().insert(
        HeaderName::from_static("x-cells-capabilities"),
        HeaderValue::from_static("assets-v1,sqlite-explorer-v1,sqlite-explorer-v2"),
    );
    request.headers_mut().insert(
        HeaderName::from_static("x-cells-hostname"),
        HeaderValue::from_str(hostname)?,
    );
    request.headers_mut().insert(
        HeaderName::from_static("x-cells-os"),
        HeaderValue::from_static(std::env::consts::OS),
    );
    request.headers_mut().insert(
        HeaderName::from_static("x-cells-arch"),
        HeaderValue::from_static(std::env::consts::ARCH),
    );
    Ok(request)
}

pub fn start_deploy_agent(s3: S3Client, bucket: String, runtime_ready: Arc<AtomicBool>) -> bool {
    let config = match connected_config() {
        Ok(Some(config)) => config,
        Ok(None) => return false,
        Err(error) => {
            warn!(event = "control_plane_agent_configuration_error", %error);
            return false;
        }
    };
    tokio::spawn(async move {
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
        {
            Ok(client) => client,
            Err(error) => {
                warn!(event = "control_plane_agent_client_error", %error);
                return;
            }
        };
        let mut consecutive_failures = 0_u32;
        loop {
            let mut delay = Duration::from_secs(60);
            match poll_and_apply(&client, &config, &s3, &bucket).await {
                Ok(Some(applied)) => {
                    if consecutive_failures > 0 {
                        info!(
                            event = "control_plane_agent_reconnected",
                            failures = consecutive_failures,
                            "Managed Control Plane connection restored"
                        );
                    }
                    consecutive_failures = 0;
                    if runtime_ready.load(Ordering::SeqCst)
                        && restart_on_deployment_enabled()
                    {
                        info!(
                            event = "control_plane_restart_for_deploy",
                            deployment_id = %applied.id,
                            script_name = %applied.script_name,
                            "deployment applied; restarting celld to load it"
                        );
                        restart_for_deployment();
                    }
                }
                Ok(None) => {
                    if consecutive_failures > 0 {
                        info!(
                            event = "control_plane_agent_reconnected",
                            failures = consecutive_failures,
                            "Managed Control Plane connection restored"
                        );
                    }
                    consecutive_failures = 0;
                }
                Err(error) => {
                    if error.is::<ManagedControlCredentialRevoked>() {
                        report_managed_runtime_state(ManagedRuntimeState::CredentialRevoked);
                    }
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    if consecutive_failures == 1 {
                        info!(
                            event = "control_plane_agent_unavailable",
                            %error,
                            "Managed Control Plane unavailable; bucket-backed serving continues"
                        );
                    }
                    let exponent = consecutive_failures.saturating_sub(1).min(5);
                    let seconds = 2_u64.saturating_mul(1_u64 << exponent).min(60);
                    let jitter_ms = (rand::random::<u16>() as u64) % 1000;
                    delay = Duration::from_millis(seconds * 1000 + jitter_ms);
                }
            }
            tokio::time::sleep(delay).await;
        }
    });
    true
}

pub async fn wait_for_initial_deployment(s3: &S3Client, bucket: &str) -> anyhow::Result<()> {
    let config = connected_config()?.context("managed installation is not connected")?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    println!("Waiting for the first Managed Control Plane deployment...");
    println!(
        "Deploy from {}/control; this process will start automatically.",
        config.control_url.trim_end_matches('/')
    );
    loop {
        if deployment_exists(s3, bucket).await? {
            return Ok(());
        }
        if poll_and_apply(&client, &config, s3, bucket).await?.is_some() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn deployment_exists(s3: &S3Client, bucket: &str) -> anyhow::Result<bool> {
    let response = s3
        .list_objects_v2()
        .bucket(bucket)
        .prefix("deploy/")
        .send()
        .await
        .context("discover managed deployment")?;
    Ok(response
        .contents()
        .iter()
        .filter_map(|object| object.key())
        .any(|key| {
            key == "deploy/current.json"
                || key
                    .strip_prefix("deploy/")
                    .is_some_and(is_named_current_pointer)
        }))
}

fn is_named_current_pointer(key: &str) -> bool {
    let mut parts = key.split('/');
    matches!(
        (parts.next(), parts.next(), parts.next()),
        (Some(_), Some("current.json"), None)
    )
}

fn connected_config() -> anyhow::Result<Option<CloudConfig>> {
    let path = config_path()?;
    if !path.exists() {
        return Ok(None);
    }
    let contents =
        std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    let config: CloudConfig =
        serde_json::from_str(&contents).with_context(|| format!("decode {}", path.display()))?;
    if config.installation_token.is_some() {
        Ok(Some(config))
    } else {
        Ok(None)
    }
}

async fn poll_and_apply(
    client: &reqwest::Client,
    config: &CloudConfig,
    s3: &S3Client,
    bucket: &str,
) -> anyhow::Result<Option<AppliedDeployment>> {
    let token = config
        .installation_token
        .as_deref()
        .context("cloud config has no instance token")?;
    let response = client
        .post(format!("{}/api/agent/commands/next", config.control_url))
        .bearer_auth(token)
        .json(&serde_json::json!({}))
        .send()
        .await
        .context("poll deployment command")?;
    if response.status() == reqwest::StatusCode::NO_CONTENT {
        return Ok(None);
    }
    if matches!(response.status().as_u16(), 401 | 403) {
        return Err(ManagedControlCredentialRevoked.into());
    }
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!("deployment poll returned {status}: {body}"));
    }
    let envelope: AgentCommandEnvelope =
        response.json().await.context("decode deployment command")?;
    let command = envelope.command;
    if command.kind != "deploy" {
        return Err(anyhow!(
            "unsupported control-plane command: {}",
            command.kind
        ));
    }

    let result = apply_deployment(client, token, s3, bucket, &command.deployment).await;
    let failure = result.as_ref().err().map(|error| format!("{error:#}"));
    let completion = client
        .post(format!(
            "{}/api/agent/commands/{}/complete",
            config.control_url, command.id
        ))
        .bearer_auth(token)
        .json(&CompleteCommandRequest {
            success: result.is_ok(),
            error: failure.as_deref(),
        })
        .send()
        .await
        .context("report deployment completion")?;
    if !completion.status().is_success() {
        let status = completion.status();
        let body = completion.text().await.unwrap_or_default();
        return Err(anyhow!("deployment completion returned {status}: {body}"));
    }
    result?;
    Ok(Some(AppliedDeployment {
        id: command.id,
        script_name: command.deployment.script_name,
    }))
}

async fn apply_deployment(
    client: &reqwest::Client,
    token: &str,
    s3: &S3Client,
    bucket: &str,
    deployment: &AgentDeployment,
) -> anyhow::Result<()> {
    if deployment.pointer.version != deployment.version
        || deployment.manifest.version != deployment.version
        || deployment.manifest.script_name != deployment.script_name
    {
        return Err(anyhow!("control-plane deployment metadata is inconsistent"));
    }
    for feature in &deployment.manifest.required_features {
        if feature != "assets-v1" {
            return Err(anyhow!("unsupported deployment feature: {feature}"));
        }
    }

    let mut asset_files = 0_u32;
    let mut asset_bytes = 0_u64;
    let mut asset_blobs = 0_usize;
    if let Some(reference) = &deployment.manifest.assets {
        let assets = deployment
            .assets
            .as_ref()
            .context("asset manifest has no control-plane transport")?;
        if reference.index != "assets.json"
            || reference.sha256 != assets.sha256
            || reference.file_count != assets.file_count
            || reference.total_bytes != assets.total_bytes
        {
            return Err(anyhow!("control-plane asset metadata is inconsistent"));
        }
        let response = client
            .get(&assets.index_download_url)
            .bearer_auth(token)
            .send()
            .await
            .context("download asset index")?;
        if !response.status().is_success() {
            return Err(anyhow!(
                "asset index download returned {}",
                response.status()
            ));
        }
        let index_bytes = response.bytes().await?;
        let index_hash = format!("{:x}", Sha256::digest(&index_bytes));
        if index_hash != assets.sha256 {
            return Err(anyhow!("asset index checksum mismatch"));
        }
        let index: AssetIndex =
            serde_json::from_slice(&index_bytes).context("decode asset index")?;
        if index.schema_version != 1
            || index.entries.len() != assets.file_count as usize
            || index.entries.values().map(|entry| entry.bytes).sum::<u64>() != assets.total_bytes
        {
            return Err(anyhow!("asset index counts do not match deployment"));
        }

        let mut unique = std::collections::BTreeMap::<String, u64>::new();
        for (path, entry) in &index.entries {
            validate_asset_index_entry(path, &entry.sha256, entry.bytes)?;
            if let Some(previous) = unique.insert(entry.sha256.clone(), entry.bytes) {
                if previous != entry.bytes {
                    return Err(anyhow!(
                        "asset digest has conflicting sizes: {}",
                        entry.sha256
                    ));
                }
            }
        }
        for (sha256, bytes) in &unique {
            let key =
                asset_blob_key(sha256).ok_or_else(|| anyhow!("invalid asset digest: {sha256}"))?;
            let existing = s3.head_object().bucket(bucket).key(&key).send().await;
            if let Ok(existing) = existing {
                if existing.content_length().unwrap_or(-1) == *bytes as i64
                    && existing.metadata().and_then(|metadata| metadata.get("sha256"))
                        == Some(sha256)
                {
                    continue;
                }
                tracing::warn!(
                    event = "asset_blob_repair_required",
                    %sha256,
                    "asset blob exists with invalid size or checksum metadata"
                );
            }

            let response = client
                .get(format!(
                    "{}/{}",
                    assets.blob_download_base_url.trim_end_matches('/'),
                    sha256
                ))
                .bearer_auth(token)
                .send()
                .await
                .with_context(|| format!("download asset blob {sha256}"))?;
            if !response.status().is_success() {
                return Err(anyhow!(
                    "asset blob {sha256} download returned {}",
                    response.status()
                ));
            }
            let body = response.bytes().await?;
            if body.len() as u64 != *bytes {
                return Err(anyhow!("asset blob {sha256} size mismatch"));
            }
            let hash = format!("{:x}", Sha256::digest(&body));
            if hash != *sha256 {
                return Err(anyhow!("asset blob {sha256} checksum mismatch"));
            }
            put_asset_object(s3, bucket, &key, sha256, body.to_vec()).await?;
        }
        put_bucket_object(
            s3,
            bucket,
            &format!("{}/assets.json", deployment.pointer.prefix),
            index_bytes.to_vec(),
        )
        .await?;
        asset_files = assets.file_count;
        asset_bytes = assets.total_bytes;
        asset_blobs = unique.len();
    } else if deployment.assets.is_some() {
        return Err(anyhow!(
            "control-plane sent asset transport for a manifest without assets"
        ));
    }

    for module in &deployment.modules {
        let response = client
            .get(&module.download_url)
            .bearer_auth(token)
            .send()
            .await
            .with_context(|| format!("download module {}", module.name))?;
        if !response.status().is_success() {
            return Err(anyhow!(
                "module {} download returned {}",
                module.name,
                response.status()
            ));
        }
        let bytes = response.bytes().await?;
        let hash = format!("{:x}", Sha256::digest(&bytes));
        if hash != module.sha256 {
            return Err(anyhow!("module {} checksum mismatch", module.name));
        }
        put_bucket_object(
            s3,
            bucket,
            &format!("{}/{}", deployment.pointer.prefix, module.name),
            bytes.to_vec(),
        )
        .await?;
    }
    put_bucket_object(
        s3,
        bucket,
        &format!("{}/manifest.json", deployment.pointer.prefix),
        serde_json::to_vec_pretty(&deployment.manifest)?,
    )
    .await?;
    put_bucket_object(
        s3,
        bucket,
        &format!("deploy/{}/current.json", deployment.script_name),
        serde_json::to_vec_pretty(&deployment.pointer)?,
    )
    .await?;
    // This fleet-wide pointer is the sole application selector. The named
    // pointer above remains only for resolving service-binding components and
    // for migrating buckets written by older celld releases.
    put_bucket_object(
        s3,
        bucket,
        "deploy/current.json",
        serde_json::to_vec_pretty(&deployment.pointer)?,
    )
    .await?;
    info!(
        event = "control_plane_deployment_applied",
        script_name = %deployment.script_name,
        version = %deployment.version,
        modules = deployment.modules.len(),
        asset_files,
        asset_bytes,
        asset_blobs,
        bucket,
        "deployment artifacts written to fleet bucket"
    );
    Ok(())
}

fn validate_asset_index_entry(path: &str, sha256: &str, bytes: u64) -> anyhow::Result<()> {
    if !path.starts_with('/')
        || path.len() > 1024
        || path.contains('\\')
        || path.contains('\0')
        || path
            .split('/')
            .skip(1)
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(anyhow!("invalid asset path in index: {path:?}"));
    }
    if asset_blob_key(sha256).is_none() {
        return Err(anyhow!("invalid asset digest in index: {sha256:?}"));
    }
    if bytes > 25 * 1024 * 1024 {
        return Err(anyhow!("asset exceeds runtime size limit: {path:?}"));
    }
    Ok(())
}

async fn put_asset_object(
    s3: &S3Client,
    bucket: &str,
    key: &str,
    sha256: &str,
    body: Vec<u8>,
) -> anyhow::Result<()> {
    s3.put_object()
        .bucket(bucket)
        .key(key)
        .metadata("sha256", sha256)
        .body(ByteStream::from(body))
        .send()
        .await
        .with_context(|| format!("write s3://{bucket}/{key}"))?;
    Ok(())
}

async fn put_bucket_object(
    s3: &S3Client,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
) -> anyhow::Result<()> {
    s3.put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body))
        .send()
        .await
        .with_context(|| format!("write s3://{bucket}/{key}"))?;
    Ok(())
}

async fn connect(options: ConnectOptions, wait: bool) -> anyhow::Result<()> {
    let path = config_path()?;
    connect_at_path(path, options, wait).await
}

async fn connect_at_path(path: PathBuf, options: ConnectOptions, wait: bool) -> anyhow::Result<()> {
    let _lock = match try_acquire_enrollment_lock(&path).await? {
        Some(lock) => lock,
        None => wait_for_shared_enrollment(&path).await?,
    };
    let mut config = load_or_create_config(&path, &options)?;
    let enrollment_byo = match (
        config.storage.as_ref(),
        config.byo_storage.as_ref(),
        options.byo_storage.as_ref(),
    ) {
        (Some(_), None, Some(_)) => {
            return Err(anyhow!(
                "this installation is enrolled with managed storage; disconnect before changing storage origin"
            ));
        }
        (None, Some(current), Some(requested)) if current != requested => {
            return Err(anyhow!(
                "the requested BYO bucket does not match this installation enrollment"
            ));
        }
        (None, Some(current), _) => Some(current.clone()),
        (None, None, requested) => requested.cloned(),
        (Some(_), None, None) => None,
        _ => {
            return Err(anyhow!(
                "managed installation record contains an invalid storage target"
            ));
        }
    };
    if config.installation_token.is_some()
        && (config.storage.is_some() || config.byo_storage.is_some())
        && !options.force
    {
        validate_requested_storage(&config, options.byo_storage.as_ref())?;
        retry_credential_handoff_ack(path.clone(), config.clone());
        info!(
            event = "control_plane_already_connected",
            installation_id = %config.installation_id,
            environment = %config.environment,
            control_url = %config.control_url,
            "celld is already connected to the Managed Control Plane"
        );
        return Ok(());
    }

    config.control_url = normalized_control_url(&options.control_url)?;
    config.environment = options.environment.clone();
    config.installation_token = None;
    config.fleet_id = None;
    config.environment_id = None;
    config.credential_handoff_id = None;
    config.pending_claim = None;
    config.storage = None;
    config.byo_storage = enrollment_byo;
    save_config(&path, &config)?;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(if wait { 15 } else { 3 }))
        .build()?;
    let claim_url = format!("{}/api/instances/claims", config.control_url);
    let instance_name = instance_name();
    let response = client
        .post(claim_url)
        .json(&CreateClaimRequest {
            instance_id: &config.installation_id,
            instance_name: &instance_name,
            environment: &config.environment,
            version: env!("CARGO_PKG_VERSION"),
            platform: format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
            storage_origin: if config.byo_storage.is_some() {
                "byo"
            } else {
                "managed_r2"
            },
            bucket: config
                .byo_storage
                .as_ref()
                .map(|storage| storage.bucket.as_str()),
            endpoint: config
                .byo_storage
                .as_ref()
                .and_then(|storage| storage.endpoint.as_deref()),
            region: config
                .byo_storage
                .as_ref()
                .map(|storage| storage.region.as_str()),
        })
        .send()
        .await
        .context("request activation link")?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!("activation endpoint returned {status}: {body}"));
    }
    let claim: CreateClaimResponse = response.json().await.context("decode activation link")?;
    config.pending_claim = Some(PendingClaim {
        verification_uri_complete: claim.verification_uri_complete.clone(),
        expires_at_ms: current_time_ms().saturating_add(claim.expires_in.saturating_mul(1000)),
    });
    save_config(&path, &config)?;

    info!(
        event = "control_plane_claim_pending",
        installation_id = %config.installation_id,
        environment = %config.environment,
        verification_url = %claim.verification_uri_complete,
        expires_in_seconds = claim.expires_in,
        "authenticate to connect this celld instance"
    );
    println!(
        "\nConnect this celld instance to the Managed Control Plane:\n\n  {}\n\nEnvironment: {}\n",
        claim.verification_uri_complete, config.environment
    );

    if wait {
        wait_for_approval(client, path, config, claim).await
    } else {
        tokio::spawn(async move {
            if let Err(error) = wait_for_approval(client, path, config, claim).await {
                warn!(event = "control_plane_claim_failed", %error);
            }
        });
        Ok(())
    }
}

async fn wait_for_approval(
    client: reqwest::Client,
    path: PathBuf,
    mut config: CloudConfig,
    claim: CreateClaimResponse,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(claim.expires_in);
    let poll_url = format!("{}/api/instances/claims/token", config.control_url);
    let mut interval = claim.interval.max(1);
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_secs(interval)).await;
        let response = client
            .post(&poll_url)
            .json(&PollClaimRequest {
                device_code: &claim.device_code,
            })
            .send()
            .await
            .context("poll activation")?;
        if response.status() == reqwest::StatusCode::GONE {
            return Err(anyhow!("activation link expired"));
        }
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("activation poll returned {status}: {body}"));
        }
        let result: PollClaimResponse =
            response.json().await.context("decode activation status")?;
        if result.status == "pending" {
            interval = result.interval.unwrap_or(interval).max(1);
            continue;
        }
        if result.status != "connected" {
            return Err(anyhow!("unexpected activation status: {}", result.status));
        }
        config.installation_token = Some(
            result
                .instance_token
                .context("activation response omitted instance token")?,
        );
        config.credential_version = result.credential_version.unwrap_or(1);
        config.fleet_id = Some(
            result
                .fleet_id
                .context("activation response omitted fleet")?,
        );
        config.environment_id = Some(
            result
                .environment_id
                .context("activation response omitted environment")?,
        );
        let bucket = result
            .bucket
            .context("activation response omitted bucket")?;
        let region = result
            .region
            .context("activation response omitted bucket region")?;
        match result.storage_origin.as_str() {
            "managed_r2" => {
                if config.byo_storage.is_some() {
                    return Err(anyhow!(
                        "activation changed the requested BYO bucket to managed storage"
                    ));
                }
                config.storage = Some(ManagedStorageConfig {
                    bucket,
                    endpoint: result
                        .endpoint
                        .context("activation response omitted bucket endpoint")?,
                    region,
                    access_key_id: result
                        .access_key_id
                        .context("activation response omitted storage access key")?,
                    secret_access_key: result
                        .secret_access_key
                        .context("activation response omitted storage secret key")?,
                    session_token: result.session_token,
                });
                config.credential_handoff_id = Some(
                    result
                        .credential_handoff_id
                        .context("activation response omitted credential handoff")?,
                );
            }
            "byo" => {
                let storage = ByoStorageConfig {
                    bucket,
                    endpoint: result.endpoint,
                    region,
                };
                if config.byo_storage.as_ref() != Some(&storage) {
                    return Err(anyhow!(
                        "activation returned a different BYO storage target"
                    ));
                }
                if result.credential_handoff_id.is_some()
                    || result.access_key_id.is_some()
                    || result.secret_access_key.is_some()
                    || result.session_token.is_some()
                {
                    return Err(anyhow!(
                        "activation returned managed credentials for a BYO bucket"
                    ));
                }
                config.storage = None;
                config.byo_storage = Some(storage);
                config.credential_handoff_id = None;
            }
            origin => return Err(anyhow!("activation returned unknown storage origin: {origin}")),
        }
        config.pending_claim = None;
        save_config(&path, &config)?;
        if let Some(handoff_id) = config.credential_handoff_id.clone() {
            match acknowledge_credential_handoff(&client, &config, &handoff_id).await {
                Ok(()) => {
                    config.credential_handoff_id = None;
                    save_config(&path, &config)?;
                }
                Err(error) => {
                    info!(
                        event = "control_plane_credential_ack_deferred",
                        %error,
                        "managed credential is saved; delivery acknowledgement will retry in the background"
                    );
                    retry_credential_handoff_ack(path.clone(), config.clone());
                }
            }
        }
        info!(
            event = "control_plane_connected",
            installation_id = %config.installation_id,
            fleet_id = %config.fleet_id.as_deref().unwrap_or_default(),
            environment_id = %config.environment_id.as_deref().unwrap_or_default(),
            environment = %config.environment,
            "celld connected to the Managed Control Plane"
        );
        println!(
            "Connected to the Managed Control Plane ({}).\n\
             Keep celld running. In your app directory, run `celld token`, apply the \
             exports it prints, then run `npx wrangler@4 deploy`.",
            config.environment
        );
        return Ok(());
    }
    // Make expiry visible so an unattended process cannot keep displaying a
    // dead approval URL.
    println!(
        "\nThe activation link expired before it was approved.\n\
         Restart celld to get a new one.\n"
    );
    Err(anyhow!(
        "activation link expired before approval; restart celld for a new link"
    ))
}

fn options_from_env() -> anyhow::Result<ConnectOptions> {
    Ok(ConnectOptions {
        control_url: std::env::var("CELLD_CONTROL_URL")
            .unwrap_or_else(|_| DEFAULT_CONTROL_URL.to_string()),
        environment: std::env::var("CELLD_ENV").unwrap_or_else(|_| DEFAULT_ENVIRONMENT.to_string()),
        force: false,
        byo_storage: None,
    })
}

fn config_path() -> anyhow::Result<PathBuf> {
    let base = if let Some(path) = std::env::var_os("CELLD_CONFIG_DIR") {
        PathBuf::from(path)
    } else if let Some(path) = std::env::var_os("XDG_CONFIG_HOME") {
        PathBuf::from(path).join("celld")
    } else if let Some(path) = std::env::var_os("HOME") {
        PathBuf::from(path).join(".config").join("celld")
    } else {
        std::env::current_dir()?.join(".celld")
    };
    Ok(base.join("cloud.json"))
}

fn load_or_create_config(path: &Path, options: &ConnectOptions) -> anyhow::Result<CloudConfig> {
    if path.exists() {
        let contents =
            std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        return serde_json::from_str(&contents)
            .with_context(|| format!("decode {}", path.display()));
    }
    let config = CloudConfig {
        version: installation_schema_version(),
        installation_id: random_instance_id(),
        installation_token: None,
        credential_version: initial_credential_version(),
        fleet_id: None,
        environment_id: None,
        control_url: normalized_control_url(&options.control_url)?,
        environment: options.environment.clone(),
        storage: None,
        byo_storage: None,
        credential_handoff_id: None,
        previous_credential: None,
        pending_claim: None,
    };
    save_config(path, &config)?;
    Ok(config)
}

fn retry_credential_handoff_ack(path: PathBuf, config: CloudConfig) {
    let Some(handoff_id) = config.credential_handoff_id.clone() else {
        return;
    };
    tokio::spawn(async move {
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
        {
            Ok(client) => client,
            Err(_) => return,
        };
        if acknowledge_credential_handoff(&client, &config, &handoff_id)
            .await
            .is_err()
        {
            return;
        }
        let Ok(_lock) = acquire_enrollment_lock(&path).await else {
            return;
        };
        let Ok(contents) = std::fs::read_to_string(&path) else {
            return;
        };
        let Ok(mut current) = serde_json::from_str::<CloudConfig>(&contents) else {
            return;
        };
        if current.credential_handoff_id.as_deref() != Some(&handoff_id) {
            return;
        }
        current.credential_handoff_id = None;
        current.previous_credential = None;
        if let Err(error) = save_config(&path, &current) {
            warn!(
                event = "control_plane_credential_ack_save_failed",
                %error,
                "credential delivery was acknowledged but the local marker could not be cleared"
            );
        }
    });
}

async fn acknowledge_credential_handoff(
    client: &reqwest::Client,
    config: &CloudConfig,
    handoff_id: &str,
) -> anyhow::Result<()> {
    let token = config
        .installation_token
        .as_deref()
        .context("managed installation has no token")?;
    let response = client
        .post(format!("{}/api/agent/credentials/ack", config.control_url))
        .bearer_auth(token)
        .json(&serde_json::json!({ "handoff_id": handoff_id }))
        .send()
        .await
        .context("acknowledge managed credential delivery")?;
    if !response.status().is_success() {
        return Err(anyhow!(
            "credential acknowledgement returned {}",
            response.status()
        ));
    }
    Ok(())
}

fn save_config(path: &Path, config: &CloudConfig) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .context("cloud config has no parent directory")?;
    std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    let temporary = path.with_extension("json.tmp");
    std::fs::write(&temporary, serde_json::to_vec_pretty(config)?)
        .with_context(|| format!("write {}", temporary.display()))?;
    restrict_permissions(&temporary)?;
    std::fs::rename(&temporary, path).with_context(|| format!("replace {}", path.display()))?;
    Ok(())
}

#[cfg(unix)]
fn restrict_permissions(path: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn restrict_permissions(_path: &Path) -> anyhow::Result<()> {
    Ok(())
}

fn random_instance_id() -> String {
    let mut bytes = [0_u8; 16];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    let suffix = bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("inst_{suffix}")
}

fn installation_schema_version() -> u32 {
    1
}

fn initial_credential_version() -> u64 {
    1
}

fn managed_storage_origin() -> String {
    "managed_r2".to_string()
}

fn validate_requested_storage(
    config: &CloudConfig,
    requested: Option<&ByoStorageConfig>,
) -> anyhow::Result<()> {
    match (config.storage.as_ref(), config.byo_storage.as_ref(), requested) {
        (Some(_), None, None) | (None, Some(_), None) => Ok(()),
        (Some(_), None, Some(_)) => Err(anyhow!(
            "this installation is enrolled with managed storage; disconnect before changing storage origin"
        )),
        (None, Some(current), Some(requested)) if current == requested => Ok(()),
        (None, Some(_), Some(_)) => Err(anyhow!(
            "the requested BYO bucket does not match this installation enrollment"
        )),
        _ => Err(anyhow!(
            "managed installation record contains an invalid storage target"
        )),
    }
}

struct EnrollmentLock {
    _file: File,
}

async fn try_acquire_enrollment_lock(config: &Path) -> anyhow::Result<Option<EnrollmentLock>> {
    let (file, lock_path) = open_enrollment_lock(config).await?;
    tokio::task::spawn_blocking(move || match fs2::FileExt::try_lock_exclusive(&file) {
        Ok(()) => Ok(Some(EnrollmentLock { _file: file })),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
        Err(error) => Err(error).with_context(|| format!("lock {}", lock_path.display())),
    })
    .await
    .context("join enrollment try-lock task")?
}

async fn acquire_enrollment_lock(config: &Path) -> anyhow::Result<EnrollmentLock> {
    let (file, lock_path) = open_enrollment_lock(config).await?;
    tokio::task::spawn_blocking(move || {
        fs2::FileExt::lock_exclusive(&file)
            .with_context(|| format!("lock {}", lock_path.display()))?;
        Ok(EnrollmentLock { _file: file })
    })
    .await
    .context("join enrollment lock task")?
}

async fn open_enrollment_lock(config: &Path) -> anyhow::Result<(File, PathBuf)> {
    let lock_path = config.with_extension("lock");
    let parent = lock_path
        .parent()
        .context("managed installation lock has no parent directory")?
        .to_path_buf();
    tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(&parent).with_context(|| format!("create {}", parent.display()))?;
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .with_context(|| format!("open {}", lock_path.display()))?;
        Ok((file, lock_path))
    })
    .await
    .context("join enrollment lock open task")?
}

async fn wait_for_shared_enrollment(path: &Path) -> anyhow::Result<EnrollmentLock> {
    loop {
        if let Ok(contents) = std::fs::read_to_string(path) {
            if let Ok(config) = serde_json::from_str::<CloudConfig>(&contents) {
                if config.installation_token.is_some()
                    && (config.storage.is_some() || config.byo_storage.is_some())
                {
                    return acquire_enrollment_lock(path).await;
                }
                if let Some(pending) = config.pending_claim {
                    if pending.expires_at_ms > current_time_ms() {
                        println!(
                            "\nConnect this celld installation to the Managed Control Plane:\n\n  {}\n\nEnvironment: {}\n",
                            pending.verification_uri_complete, config.environment
                        );
                        return acquire_enrollment_lock(path).await;
                    }
                }
            }
        }
        if let Some(lock) = try_acquire_enrollment_lock(path).await? {
            return Ok(lock);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn normalized_control_url(value: &str) -> anyhow::Result<String> {
    let value = value.trim_end_matches('/');
    let url = url::Url::parse(value).context("invalid control plane URL")?;
    if url.scheme() != "https"
        && !(url.scheme() == "http" && matches!(url.host_str(), Some("localhost" | "127.0.0.1")))
    {
        return Err(anyhow!("control plane URL must use HTTPS"));
    }
    if url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
        return Err(anyhow!(
            "control plane URL must not contain a path, query, or fragment"
        ));
    }
    Ok(value.to_string())
}

fn validate_environment(value: &str) -> anyhow::Result<()> {
    if value.is_empty() || value.len() > 32 {
        return Err(anyhow!("environment must be 1–32 characters"));
    }
    let mut characters = value.chars();
    if !characters
        .next()
        .is_some_and(|character| character.is_ascii_lowercase())
        || !characters.all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
        })
    {
        return Err(anyhow!(
            "environment must start with a lowercase letter and contain only lowercase letters, numbers, and hyphens"
        ));
    }
    Ok(())
}

fn instance_name() -> String {
    std::env::var("CELLD_INSTANCE_NAME")
        .unwrap_or_else(|_| machine_hostname())
}

fn machine_hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .ok()
        .or_else(|| {
            std::process::Command::new("hostname")
                .output()
                .ok()
                .filter(|output| output.status.success())
                .and_then(|output| String::from_utf8(output.stdout).ok())
        })
        .map(|value| value.trim().to_string())
        .filter(|value| {
            !value.is_empty() &&
                value.len() <= 100 &&
                value.is_ascii() &&
                !value.chars().any(char::is_control)
        })
        .unwrap_or_else(|| "celld".to_string())
}

fn print_connect_help() {
    println!(
        "Connect this celld installation to the Managed Control Plane.\n\nUSAGE:\n  celld connect [--env prod] [--control-url https://celld.dev] [--force]"
    );
}

fn print_credentials_help() {
    println!(
        "Refresh a staged Managed Control Plane credential.\n\nUSAGE:\n  celld credentials refresh\n\nStop every celld process sharing this installation before refreshing."
    );
}

fn print_disconnect_help() {
    println!(
        "Revoke and disconnect this Managed Control Plane installation.\n\nUSAGE:\n  celld disconnect --yes\n\nStop every celld process sharing this installation first. Other installations in the fleet are unaffected."
    );
}

#[cfg(test)]
mod tests {
    use super::{
        disconnect_at_path, normalized_control_url, presence_request, refresh_credentials_at_path,
        try_acquire_enrollment_lock, validate_environment, CloudConfig, ManagedRuntimeState,
        ManagedStorageConfig,
    };
    use axum::http::{HeaderMap, StatusCode};
    use axum::response::IntoResponse;
    use axum::routing::post;
    use axum::{Json, Router};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn managed_runtime_state_vocabulary_is_stable() {
        assert_eq!(ManagedRuntimeState::Connected.as_str(), "connected");
        assert_eq!(
            ManagedRuntimeState::ControlPlaneUnavailable.as_str(),
            "control_plane_unavailable",
        );
        assert_eq!(
            ManagedRuntimeState::BucketUnavailable.as_str(),
            "bucket_unavailable",
        );
        assert_eq!(
            ManagedRuntimeState::CredentialRevoked.as_str(),
            "credential_revoked",
        );
    }

    #[test]
    fn environment_names_are_safe_slugs() {
        assert!(validate_environment("prod").is_ok());
        assert!(validate_environment("preview-12").is_ok());
        assert!(validate_environment("Production").is_err());
        assert!(validate_environment("../prod").is_err());
    }

    #[test]
    fn deploy_token_shell_output_is_safe_to_eval() {
        let output = super::render_deploy_token(
            "header.payload'quoted",
            "environment-123",
            "https://celld.dev/client/v4",
            true,
        );
        assert_eq!(
            output,
            "export CLOUDFLARE_API_TOKEN='header.payload'\"'\"'quoted'\n\
             export CLOUDFLARE_ACCOUNT_ID='environment-123'\n\
             export CLOUDFLARE_API_BASE_URL='https://celld.dev/client/v4'\n"
        );
        assert!(!output.contains("Deploy token for"));
        assert!(!output.contains("wrangler deploy"));
    }

    #[test]
    fn byo_claim_contains_only_a_storage_descriptor() {
        let storage = super::ByoStorageConfig {
            bucket: "customer-bucket".to_string(),
            endpoint: Some("https://objects.example.com".to_string()),
            region: "us-west-2".to_string(),
        };
        let request = super::CreateClaimRequest {
            instance_id: "inst_byo_test",
            instance_name: "laptop",
            environment: "prod",
            version: "test",
            platform: "test-platform".to_string(),
            storage_origin: "byo",
            bucket: Some(&storage.bucket),
            endpoint: storage.endpoint.as_deref(),
            region: Some(&storage.region),
        };
        let value = serde_json::to_value(request).unwrap();
        assert_eq!(value["storage_origin"], "byo");
        assert_eq!(value["bucket"], "customer-bucket");
        assert_eq!(value["endpoint"], "https://objects.example.com");
        assert_eq!(value["region"], "us-west-2");
        assert!(value.get("access_key_id").is_none());
        assert!(value.get("secret_access_key").is_none());
        assert!(value.get("session_token").is_none());
    }

    #[test]
    fn enrolled_storage_origin_and_byo_target_cannot_change_implicitly() {
        let byo = super::ByoStorageConfig {
            bucket: "customer-bucket".to_string(),
            endpoint: Some("https://objects.example.com".to_string()),
            region: "us-west-2".to_string(),
        };
        let mut config = CloudConfig {
            version: super::installation_schema_version(),
            installation_id: "inst_byo_test".to_string(),
            installation_token: Some("control-token".to_string()),
            credential_version: 1,
            fleet_id: Some("fleet-byo".to_string()),
            environment_id: Some("environment-byo".to_string()),
            control_url: "https://celld.dev".to_string(),
            environment: "prod".to_string(),
            storage: None,
            byo_storage: Some(byo.clone()),
            credential_handoff_id: None,
            previous_credential: None,
            pending_claim: None,
        };
        assert!(super::validate_requested_storage(&config, None).is_ok());
        assert!(super::validate_requested_storage(&config, Some(&byo)).is_ok());
        let other = super::ByoStorageConfig {
            bucket: "other-bucket".to_string(),
            ..byo
        };
        assert!(super::validate_requested_storage(&config, Some(&other)).is_err());

        config.byo_storage = None;
        config.storage = Some(ManagedStorageConfig {
            bucket: "managed-bucket".to_string(),
            endpoint: "https://managed.example.com".to_string(),
            region: "auto".to_string(),
            access_key_id: "managed-access".to_string(),
            secret_access_key: "managed-secret".to_string(),
            session_token: None,
        });
        assert!(super::validate_requested_storage(&config, Some(&other)).is_err());
    }

    #[test]
    fn presence_request_contains_a_complete_websocket_handshake() {
        use tokio_tungstenite::tungstenite::http::header::{
            AUTHORIZATION, CONNECTION, HOST, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_VERSION, UPGRADE,
        };

        let config = CloudConfig {
            version: super::installation_schema_version(),
            installation_id: "inst_presence_request".to_string(),
            installation_token: Some("presence-control-token".to_string()),
            credential_version: 1,
            fleet_id: Some("fleet-presence".to_string()),
            environment_id: Some("environment-presence".to_string()),
            control_url: "https://celld.dev".to_string(),
            environment: "prod".to_string(),
            storage: None,
            byo_storage: None,
            credential_handoff_id: None,
            previous_credential: None,
            pending_claim: None,
        };
        let request = presence_request(
            &config,
            "node-session-123",
            "127.0.0.1:8080",
            "127.0.0.1:8080",
            "host-one",
        )
        .unwrap();
        assert_eq!(request.uri().scheme_str(), Some("wss"));
        assert_eq!(request.uri().path(), "/api/agent/presence");
        assert!(request
            .uri()
            .query()
            .unwrap()
            .contains("node_session_id=node-session-123"));
        assert_eq!(request.headers()[HOST], "celld.dev");
        assert_eq!(request.headers()[CONNECTION], "Upgrade");
        assert_eq!(request.headers()[UPGRADE], "websocket");
        assert_eq!(request.headers()[SEC_WEBSOCKET_VERSION], "13");
        assert!(!request.headers()[SEC_WEBSOCKET_KEY].is_empty());
        assert_eq!(
            request.headers()[AUTHORIZATION],
            "Bearer presence-control-token"
        );
        assert!(request.headers().contains_key("x-cells-version"));
        assert_eq!(
            request.headers()["x-cells-capabilities"],
            "assets-v1,sqlite-explorer-v1,sqlite-explorer-v2"
        );
        assert_eq!(request.headers()["x-cells-hostname"], "host-one");
        assert_eq!(request.headers()["x-cells-os"], std::env::consts::OS);
        assert_eq!(request.headers()["x-cells-arch"], std::env::consts::ARCH);
    }

    #[test]
    fn control_url_requires_https_except_local_development() {
        assert_eq!(
            normalized_control_url("https://celld.dev/").unwrap(),
            "https://celld.dev"
        );
        assert!(normalized_control_url("http://localhost:5173").is_ok());
        assert!(normalized_control_url("http://celld.dev").is_err());
        assert!(normalized_control_url("https://celld.dev/path").is_err());
    }

    #[test]
    fn legacy_instance_record_decodes_as_an_installation_record() {
        let config: CloudConfig = serde_json::from_value(serde_json::json!({
            "instance_id": "inst_12345678",
            "instance_token": "token",
            "fleet_id": "fleet",
            "environment_id": "environment",
            "control_url": "https://celld.dev",
            "environment": "prod"
        }))
        .unwrap();
        assert_eq!(config.version, 1);
        assert_eq!(config.installation_id, "inst_12345678");
        assert_eq!(config.installation_token.as_deref(), Some("token"));
        assert!(config.storage.is_none());
    }

    #[tokio::test]
    async fn enrollment_lock_serializes_processes_and_recovers_on_drop() {
        let directory =
            std::env::temp_dir().join(format!("celld-lock-test-{}", super::random_instance_id()));
        let config = directory.join("cloud.json");
        let first = try_acquire_enrollment_lock(&config)
            .await
            .unwrap()
            .expect("first lock");
        assert!(try_acquire_enrollment_lock(&config)
            .await
            .unwrap()
            .is_none());
        drop(first);
        assert!(try_acquire_enrollment_lock(&config)
            .await
            .unwrap()
            .is_some());
        let _ = std::fs::remove_dir_all(directory);
    }

    #[tokio::test]
    async fn completed_installation_start_does_not_contact_control_plane() {
        let directory = std::env::temp_dir().join(super::random_instance_id());
        let path = directory.join("cloud.json");
        let config = super::CloudConfig {
            version: super::installation_schema_version(),
            installation_id: "inst_offline_start".to_string(),
            installation_token: Some("installation-secret".to_string()),
            credential_version: 1,
            fleet_id: Some("fleet-offline".to_string()),
            environment_id: Some("environment-offline".to_string()),
            control_url: "http://127.0.0.1:1".to_string(),
            environment: "prod".to_string(),
            storage: Some(super::ManagedStorageConfig {
                bucket: "fleet-bucket".to_string(),
                endpoint: "https://example.r2.cloudflarestorage.com".to_string(),
                region: "auto".to_string(),
                access_key_id: "access-key".to_string(),
                secret_access_key: "storage-secret".to_string(),
                session_token: None,
            }),
            byo_storage: None,
            credential_handoff_id: Some("provision_pending_delivery".to_string()),
            previous_credential: None,
            pending_claim: None,
        };
        super::save_config(&path, &config).unwrap();

        super::connect_at_path(
            path.clone(),
            super::ConnectOptions {
                control_url: "http://127.0.0.1:1".to_string(),
                environment: "prod".to_string(),
                force: false,
                byo_storage: None,
            },
            true,
        )
        .await
        .expect("cached installation starts without contacting the unavailable endpoint");

        let stored: super::CloudConfig =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(
            stored.installation_token.as_deref(),
            Some("installation-secret")
        );
        assert_eq!(
            stored.credential_handoff_id.as_deref(),
            Some("provision_pending_delivery")
        );
        let _ = std::fs::remove_dir_all(directory);
    }

    #[tokio::test]
    async fn credential_refresh_saves_before_ack_and_retries_without_restaging() {
        let next_calls = Arc::new(AtomicUsize::new(0));
        let ack_calls = Arc::new(AtomicUsize::new(0));
        let next_counter = next_calls.clone();
        let ack_counter = ack_calls.clone();
        let app = Router::new()
            .route(
                "/api/agent/credentials/next",
                post(move |headers: HeaderMap| {
                    let next_counter = next_counter.clone();
                    async move {
                        assert_eq!(
                            headers
                                .get("authorization")
                                .and_then(|value| value.to_str().ok()),
                            Some("Bearer old-control-token")
                        );
                        next_counter.fetch_add(1, Ordering::SeqCst);
                        Json(serde_json::json!({
                            "status": "rotation_pending",
                            "credential_handoff_id": "rotate_installation_v2",
                            "credential_version": 2,
                            "instance_token": "new-control-token",
                            "bucket": "fleet-bucket",
                            "endpoint": "http://127.0.0.1:9000",
                            "region": "auto",
                            "access_key_id": "new-access-key",
                            "secret_access_key": "new-storage-secret"
                        }))
                    }
                }),
            )
            .route(
                "/api/agent/credentials/ack",
                post(move |headers: HeaderMap| {
                    let ack_counter = ack_counter.clone();
                    async move {
                        assert_eq!(
                            headers
                                .get("authorization")
                                .and_then(|value| value.to_str().ok()),
                            Some("Bearer new-control-token")
                        );
                        if ack_counter.fetch_add(1, Ordering::SeqCst) == 0 {
                            StatusCode::SERVICE_UNAVAILABLE.into_response()
                        } else {
                            Json(serde_json::json!({ "ok": true })).into_response()
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let directory = std::env::temp_dir().join(format!(
            "celld-rotation-test-{}",
            super::random_instance_id()
        ));
        let path = directory.join("cloud.json");
        let config = CloudConfig {
            version: super::installation_schema_version(),
            installation_id: "inst_rotation_test".to_string(),
            installation_token: Some("old-control-token".to_string()),
            credential_version: 1,
            fleet_id: Some("fleet-test".to_string()),
            environment_id: Some("environment-test".to_string()),
            control_url: format!("http://{address}"),
            environment: "prod".to_string(),
            storage: Some(ManagedStorageConfig {
                bucket: "fleet-bucket".to_string(),
                endpoint: "http://127.0.0.1:9000".to_string(),
                region: "auto".to_string(),
                access_key_id: "old-access-key".to_string(),
                secret_access_key: "old-storage-secret".to_string(),
                session_token: None,
            }),
            byo_storage: None,
            credential_handoff_id: None,
            previous_credential: None,
            pending_claim: None,
        };
        super::save_config(&path, &config).unwrap();
        let client = reqwest::Client::new();

        let first = refresh_credentials_at_path(path.clone(), &client).await;
        assert!(first
            .unwrap_err()
            .to_string()
            .contains("acknowledgement is pending"));
        let saved: CloudConfig =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(saved.credential_version, 2);
        assert_eq!(
            saved.installation_token.as_deref(),
            Some("new-control-token")
        );
        assert_eq!(
            saved.credential_handoff_id.as_deref(),
            Some("rotate_installation_v2")
        );
        let previous = saved
            .previous_credential
            .as_ref()
            .expect("old credential retained until acknowledgement");
        assert_eq!(previous.installation_token, "old-control-token");
        assert_eq!(previous.credential_version, 1);
        assert_eq!(previous.storage.access_key_id, "old-access-key");
        assert_eq!(
            saved.storage.as_ref().unwrap().access_key_id,
            "new-access-key".to_string()
        );

        refresh_credentials_at_path(path.clone(), &client)
            .await
            .unwrap();
        let completed: CloudConfig =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(completed.credential_handoff_id, None);
        assert!(completed.previous_credential.is_none());
        assert_eq!(next_calls.load(Ordering::SeqCst), 1);
        assert_eq!(ack_calls.load(Ordering::SeqCst), 2);

        server.abort();
        let _ = std::fs::remove_dir_all(directory);
    }

    #[tokio::test]
    async fn disconnect_revokes_before_archiving_the_local_record() {
        let revoke_calls = Arc::new(AtomicUsize::new(0));
        let calls = revoke_calls.clone();
        let app = Router::new().route(
            "/api/agent/credentials/revoke",
            post(
                move |headers: HeaderMap, Json(body): Json<serde_json::Value>| {
                    let calls = calls.clone();
                    async move {
                        assert_eq!(
                            headers
                                .get("authorization")
                                .and_then(|value| value.to_str().ok()),
                            Some("Bearer installation-control-token")
                        );
                        calls.fetch_add(1, Ordering::SeqCst);
                        match body["installation_id"].as_str() {
                            Some("inst_disconnect_test") => {
                                Json(serde_json::json!({ "ok": true })).into_response()
                            }
                            Some("inst_disconnect_unauthorized") => {
                                StatusCode::UNAUTHORIZED.into_response()
                            }
                            other => panic!("unexpected installation ID: {other:?}"),
                        }
                    }
                },
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let directory = std::env::temp_dir().join(format!(
            "celld-disconnect-test-{}",
            super::random_instance_id()
        ));
        let path = directory.join("cloud.json");
        let config = CloudConfig {
            version: super::installation_schema_version(),
            installation_id: "inst_disconnect_test".to_string(),
            installation_token: Some("installation-control-token".to_string()),
            credential_version: 4,
            fleet_id: Some("fleet-test".to_string()),
            environment_id: Some("environment-test".to_string()),
            control_url: format!("http://{address}"),
            environment: "prod".to_string(),
            storage: Some(ManagedStorageConfig {
                bucket: "fleet-bucket".to_string(),
                endpoint: "http://127.0.0.1:9000".to_string(),
                region: "auto".to_string(),
                access_key_id: "access-key".to_string(),
                secret_access_key: "storage-secret".to_string(),
                session_token: None,
            }),
            byo_storage: None,
            credential_handoff_id: None,
            previous_credential: None,
            pending_claim: None,
        };
        super::save_config(&path, &config).unwrap();

        let archive = disconnect_at_path(path.clone(), &reqwest::Client::new())
            .await
            .unwrap();
        assert!(!path.exists());
        assert!(archive.exists());
        assert!(archive
            .file_name()
            .and_then(|value| value.to_str())
            .is_some_and(|value| value.starts_with("cloud.json.disconnected-")));
        assert_eq!(revoke_calls.load(Ordering::SeqCst), 1);
        let archived: CloudConfig =
            serde_json::from_str(&std::fs::read_to_string(&archive).unwrap()).unwrap();
        assert_eq!(archived.installation_id, "inst_disconnect_test");

        let unauthorized_path = directory.join("cloud-unauthorized.json");
        let mut unauthorized = config;
        unauthorized.installation_id = "inst_disconnect_unauthorized".to_string();
        super::save_config(&unauthorized_path, &unauthorized).unwrap();
        let error = disconnect_at_path(unauthorized_path.clone(), &reqwest::Client::new())
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("revocation returned 401 Unauthorized"));
        assert!(unauthorized_path.exists());
        assert_eq!(revoke_calls.load(Ordering::SeqCst), 2);

        server.abort();
        let _ = std::fs::remove_dir_all(directory);
    }
}
