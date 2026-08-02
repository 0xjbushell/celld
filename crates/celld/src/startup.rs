use anyhow::{anyhow, Context};
use std::collections::HashMap;
use std::fmt;
use std::net::{IpAddr, SocketAddr};
use tokio::net::{TcpListener, TcpSocket};
use tracing::warn;

const DEFAULT_REGION: &str = "us-east-1";
const AUTO_LISTEN_START: u16 = 8080;
const AUTO_LISTEN_END: u16 = 8099;
/// `TcpListener::bind` inherits socket2's historical backlog of 128. That is
/// smaller than a routine reconnect cohort at the target fleet density: 1,200
/// chat clients returning together filled every node's accept queue and
/// produced connection resets while the runtime itself remained healthy.
/// Request the common Linux `somaxconn` default; the kernel still applies its
/// configured ceiling.
const LISTEN_BACKLOG: u32 = 4_096;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CredentialSource {
    StandardAwsChain,
    ManagedInstallation,
}

#[derive(Clone, PartialEq, Eq)]
pub enum Listen {
    AutoLoopback,
    Explicit(String),
}

impl fmt::Debug for Listen {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AutoLoopback => formatter.write_str("AutoLoopback"),
            Self::Explicit(address) => formatter.debug_tuple("Explicit").field(address).finish(),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct RuntimeSettings {
    pub control_plane: bool,
    pub bucket: Option<String>,
    pub endpoint: Option<String>,
    pub region: String,
    pub credential_source: CredentialSource,
    pub listen: Listen,
    pub advertise: Option<String>,
    pub unsafe_public_advertise: bool,
    pub managed_identity: Option<String>,
}

// Keep this implementation explicit so adding a secret-bearing field cannot
// accidentally expose it through a derived Debug implementation.
impl fmt::Debug for RuntimeSettings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeSettings")
            .field("control_plane", &self.control_plane)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("credential_source", &self.credential_source)
            .field("listen", &self.listen)
            .field("advertise", &self.advertise)
            .field("unsafe_public_advertise", &self.unsafe_public_advertise)
            .field("managed_identity", &self.managed_identity)
            .finish()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Action {
    Run(RuntimeSettings),
    Diagnose {
        settings: RuntimeSettings,
        peers: Vec<String>,
    },
    DiagnoseHelp,
    Help,
    Version,
    Connect(Vec<String>),
    Credentials(Vec<String>),
    Token(Vec<String>),
    Disconnect(Vec<String>),
    Deploy(Vec<String>),
}

/// How peers should reach this node.
///
/// Usually an IP socket address, but a **hostname is allowed**, because most
/// container and PaaS environments hand out a stable NAME and an unstable or
/// ambiguous IP. Requiring a literal IP makes celld unusable when every VM
/// reports the same private address and peers instead rely on DNS.
///
/// The value is passed through verbatim to peers, who dial it over HTTP, so
/// a name resolves wherever the peer is rather than wherever we are.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Advertise {
    Addr(SocketAddr),
    Name { host: String, port: u16 },
}

impl Advertise {
    /// Human-readable reachability of this address, for `celld diagnose`.
    /// A name is deliberately reported as unknown rather than resolved here:
    /// what matters is whether a PEER can reach it, which we cannot infer.
    pub fn scope(&self) -> &'static str {
        match self {
            Self::Addr(address) => advertise_scope(address.ip()),
            Self::Name { .. } => "resolved by each peer",
        }
    }

    pub fn is_public_ip(&self) -> bool {
        matches!(self, Self::Addr(address) if advertise_scope(address.ip()) == "operator-routed network")
    }
}

impl fmt::Display for Advertise {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Addr(address) => write!(formatter, "{address}"),
            Self::Name { host, port } => write!(formatter, "{host}:{port}"),
        }
    }
}

fn advertise_scope(address: IpAddr) -> &'static str {
    match address {
        IpAddr::V4(address) if address.is_loopback() => "same host only",
        IpAddr::V4(address) if address.is_private() => "private network",
        IpAddr::V4(address) if address.is_link_local() => "local link only",
        IpAddr::V6(address) if address.is_loopback() => "same host only",
        IpAddr::V6(address) if address.is_unique_local() => "private network",
        IpAddr::V6(address) if address.is_unicast_link_local() => "local link only",
        _ => "operator-routed network",
    }
}

#[derive(Debug)]
pub struct BoundListener {
    pub listener: TcpListener,
    pub listen: SocketAddr,
    pub advertise: Advertise,
}

pub fn action_from_process() -> anyhow::Result<Action> {
    let environment = std::env::vars().collect::<HashMap<_, _>>();
    parse_action(std::env::args().skip(1), &environment)
}

fn parse_action(
    arguments: impl IntoIterator<Item = String>,
    environment: &HashMap<String, String>,
) -> anyhow::Result<Action> {
    let mut arguments = arguments.into_iter().collect::<Vec<_>>();
    if matches!(arguments.first().map(String::as_str), Some("connect")) {
        return Ok(Action::Connect(arguments[1..].to_vec()));
    }
    if matches!(arguments.first().map(String::as_str), Some("credentials")) {
        return Ok(Action::Credentials(arguments[1..].to_vec()));
    }
    if matches!(arguments.first().map(String::as_str), Some("token")) {
        return Ok(Action::Token(arguments[1..].to_vec()));
    }
    if matches!(arguments.first().map(String::as_str), Some("disconnect")) {
        return Ok(Action::Disconnect(arguments[1..].to_vec()));
    }
    if matches!(arguments.first().map(String::as_str), Some("deploy")) {
        return Ok(Action::Deploy(arguments[1..].to_vec()));
    }
    let diagnose = matches!(arguments.first().map(String::as_str), Some("diagnose"));
    if diagnose {
        arguments.remove(0);
        if matches!(
            arguments.first().map(String::as_str),
            Some("--help" | "-h" | "help")
        ) {
            if arguments.len() != 1 {
                return Err(anyhow!(
                    "unexpected argument after diagnose help: {}",
                    arguments[1]
                ));
            }
            return Ok(Action::DiagnoseHelp);
        }
    }
    if matches!(
        arguments.first().map(String::as_str),
        Some("--help" | "-h" | "help")
    ) {
        if arguments.len() != 1 {
            return Err(anyhow!("unexpected argument after help: {}", arguments[1]));
        }
        return Ok(Action::Help);
    }
    if matches!(
        arguments.first().map(String::as_str),
        Some("--version" | "-V" | "version")
    ) {
        if arguments.len() != 1 {
            return Err(anyhow!(
                "unexpected argument after version: {}",
                arguments[1]
            ));
        }
        return Ok(Action::Version);
    }

    let runtime_arguments_present = !arguments.is_empty();
    let env = |name: &str| {
        environment
            .get(name)
            .map(String::as_str)
            .filter(|value| !value.trim().is_empty())
    };
    // Standalone is the public default. Managed startup remains an explicit
    // compatibility path while the managed product is developed separately;
    // no ordinary invocation may discover or contact it implicitly.
    let environment_control_plane = match env("CELLD_CLOUD") {
        Some(value) if value.eq_ignore_ascii_case("on") => Some(true),
        Some(value) if value.eq_ignore_ascii_case("off") => Some(false),
        Some(value) => {
            return Err(anyhow!(
                "CELLD_CLOUD must be `on` or `off`, not {value:?}"
            ))
        }
        None => None,
    };
    let mut control_plane_override = None;
    let mut bucket = env("CELLD_BUCKET").map(parse_bucket).transpose()?;
    let mut endpoint = env("S3_ENDPOINT").map(normalize_endpoint).transpose()?;
    let mut region = env("AWS_REGION")
        .or_else(|| env("AWS_DEFAULT_REGION"))
        .unwrap_or(DEFAULT_REGION)
        .to_string();
    let mut listen = env("CELLD_ADDR").map(str::to_string);
    let mut advertise = env("CELLD_ADVERTISE").map(str::to_string);
    let mut unsafe_public_advertise = match env("CELLD_UNSAFE_PUBLIC_ADVERTISE") {
        Some(value) if value.eq_ignore_ascii_case("on") => true,
        Some(value) if value.eq_ignore_ascii_case("off") => false,
        Some(value) => anyhow::bail!(
            "CELLD_UNSAFE_PUBLIC_ADVERTISE must be `on` or `off`, not {value:?}"
        ),
        None => false,
    };
    let mut peers = Vec::new();

    let mut args = arguments.into_iter();
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--control-plane" => select_control_plane(&mut control_plane_override, true)?,
            "--no-control-plane" => {
                select_control_plane(&mut control_plane_override, false)?
            }
            "--bucket" => {
                bucket = Some(parse_bucket(
                    &args.next().context("--bucket requires a value")?,
                )?)
            }
            "--endpoint" => {
                endpoint = Some(normalize_endpoint(
                    &args.next().context("--endpoint requires a value")?,
                )?)
            }
            "--region" => {
                region = args.next().context("--region requires a value")?;
            }
            "--listen" => {
                listen = Some(args.next().context("--listen requires a value")?);
            }
            "--advertise" => {
                advertise = Some(args.next().context("--advertise requires a value")?);
            }
            "--unsafe-public-advertise" => unsafe_public_advertise = true,
            "--peer" if diagnose => {
                let peer = args.next().context("--peer requires a node-session ID")?;
                validate_peer_id(&peer)?;
                peers.push(peer);
            }
            _ => {
                return Err(anyhow!(
                    "unknown command or option: {argument}; run `celld --help` for usage"
                ))
            }
        }
    }

    if region.trim().is_empty()
        || !region
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '-')
    {
        return Err(anyhow!(
            "region must contain only ASCII letters, numbers, and hyphens"
        ));
    }
    if !diagnose
        && !runtime_arguments_present
        && bucket.is_none()
        && control_plane_override.is_none()
        && environment_control_plane.is_none()
    {
        return Ok(Action::Help);
    }
    let control_plane = control_plane_override
        .or(environment_control_plane)
        .unwrap_or(false);
    if !control_plane && bucket.is_none() {
        return Err(anyhow!(
            "standalone operation requires --bucket (or CELLD_BUCKET)"
        ));
    }

    let listen = match listen {
        Some(address) => Listen::Explicit(address),
        None if control_plane => Listen::AutoLoopback,
        None => Listen::Explicit("127.0.0.1:8080".to_string()),
    };
    if let Some(address) = advertise.as_deref() {
        parse_advertise(address)?;
    }

    let credential_source = if !control_plane || bucket.is_some() {
        CredentialSource::StandardAwsChain
    } else {
        CredentialSource::ManagedInstallation
    };
    let settings = RuntimeSettings {
        control_plane,
        bucket,
        endpoint,
        region,
        credential_source,
        listen,
        advertise,
        unsafe_public_advertise,
        managed_identity: None,
    };
    if diagnose {
        Ok(Action::Diagnose { settings, peers })
    } else {
        Ok(Action::Run(settings))
    }
}

fn select_control_plane(selected: &mut Option<bool>, requested: bool) -> anyhow::Result<()> {
    if selected.is_some_and(|current| current != requested) {
        return Err(anyhow!("--control-plane conflicts with --no-control-plane"));
    }
    *selected = Some(requested);
    Ok(())
}

/// Descriptors one resident cell holds: its SQLite database, WAL, and shared-
/// memory file, plus what replication keeps open for them. Measured at ~7 on
/// Linux; budgeted at 8.
const FDS_PER_CELL: u64 = 8;

/// Below this the node cannot reach the density the runtime is designed for,
/// so say so at startup rather than at the failing activation.
const MIN_RESIDENT_CELLS: u64 = 1_000;

/// Raise the descriptor limit to its hard ceiling, and warn when even that
/// bounds residency. Left at the stock 1024 a node stops near 120 resident
/// cells and reports it as `unable to open database file` from whichever
/// activation happens to be next — an error that names neither the limit nor
/// the cell count it implies. litestream inherits this limit as our child.
pub fn raise_file_limit() {
    let mut limit = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return;
    }
    if limit.rlim_cur < limit.rlim_max {
        let raised = libc::rlimit { rlim_cur: limit.rlim_max, ..limit };
        unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raised) };
        // Re-read rather than assume the raise took: macOS reports an
        // unlimited hard limit, refuses it as a soft limit, and caps the
        // process at kern.maxfilesperproc. Trusting the requested value there
        // silences this warning in exactly the case it exists to report.
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
            return;
        }
    }
    let cells = limit.rlim_cur / FDS_PER_CELL;
    if cells < MIN_RESIDENT_CELLS {
        warn!(
            event = "file_limit_bounds_residency",
            open_files = limit.rlim_cur,
            resident_cells = cells,
            "the open-file limit caps this node near {cells} resident cells; \
             raise it (ulimit -n {}, or LimitNOFILE= in the unit file)",
            MIN_RESIDENT_CELLS * FDS_PER_CELL
        );
    }
}

fn bind_tcp_listener(address: SocketAddr) -> std::io::Result<TcpListener> {
    let socket = match address {
        SocketAddr::V4(_) => TcpSocket::new_v4()?,
        SocketAddr::V6(_) => TcpSocket::new_v6()?,
    };
    #[cfg(unix)]
    socket.set_reuseaddr(true)?;
    socket.bind(address)?;
    socket.listen(LISTEN_BACKLOG)
}

pub async fn bind_listener(settings: &RuntimeSettings) -> anyhow::Result<BoundListener> {
    let (listener, listen) = match &settings.listen {
        Listen::Explicit(address) => {
            let address = parse_socket_address(address, "--listen")?;
            let listener = bind_tcp_listener(address)
                .with_context(|| format!("bind --listen {address}"))?;
            let listen = listener.local_addr().context("read bound listen address")?;
            (listener, listen)
        }
        Listen::AutoLoopback => {
            let mut last_error = None;
            let mut bound = None;
            for port in AUTO_LISTEN_START..=AUTO_LISTEN_END {
                let address = SocketAddr::from(([127, 0, 0, 1], port));
                match bind_tcp_listener(address) {
                    Ok(listener) => {
                        bound = Some((listener, address));
                        break;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
                        last_error = Some(error);
                    }
                    Err(error) => {
                        return Err(error).with_context(|| format!("bind {address}"));
                    }
                }
            }
            bound.ok_or_else(|| {
                anyhow!(
                    "no free managed demo port in 127.0.0.1:{AUTO_LISTEN_START}-{AUTO_LISTEN_END}: {}",
                    last_error
                        .map(|error| error.to_string())
                        .unwrap_or_else(|| "range exhausted".to_string())
                )
            })?
        }
    };

    let advertise = if let Some(address) = settings.advertise.as_deref() {
        parse_advertise(address)?
    } else if listen.ip().is_unspecified() {
        return Err(anyhow!(
            "--advertise is required when --listen uses an unspecified address ({listen}).\n\
             Give the address peers should dial, for example --advertise \
             {}:{} or --advertise node1.example.com:{}",
            "10.0.0.1",
            listen.port(),
            listen.port()
        ));
    } else {
        Advertise::Addr(listen)
    };
    if advertise.is_public_ip() && !settings.unsafe_public_advertise {
        return Err(anyhow!(
            "--advertise {advertise} is a public IP address; peer HTTP carries \
             application data without built-in TLS. Use a private overlay, or \
             pass --unsafe-public-advertise to accept this exposure explicitly"
        ));
    }

    Ok(BoundListener {
        listener,
        listen,
        advertise,
    })
}

fn parse_bucket(value: &str) -> anyhow::Result<String> {
    let value = value.trim();
    let name = if let Some(name) = value.strip_prefix("s3://") {
        name
    } else if value.contains("://") {
        return Err(anyhow!("bucket must be a name or s3:// URL"));
    } else {
        value
    };
    if name.is_empty()
        || name.contains('/')
        || name.contains('?')
        || name.contains('#')
        || name.chars().any(char::is_whitespace)
    {
        return Err(anyhow!(
            "bucket must identify one bucket without a path, query, or fragment"
        ));
    }
    Ok(name.to_string())
}

fn normalize_endpoint(value: &str) -> anyhow::Result<String> {
    let value = value.trim_end_matches('/');
    let url = url::Url::parse(value).context("invalid S3 endpoint URL")?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(anyhow!(
            "S3 endpoint must be an HTTP(S) origin without a query or fragment"
        ));
    }
    Ok(value.to_string())
}

fn parse_socket_address(value: &str, option: &str) -> anyhow::Result<SocketAddr> {
    value
        .parse()
        .with_context(|| format!("{option} must be an IP socket address such as 127.0.0.1:8080"))
}

/// Parse `--advertise`, accepting `IP:PORT` or `HOST:PORT`.
pub fn parse_advertise(value: &str) -> anyhow::Result<Advertise> {
    let value = value.trim();
    if let Ok(address) = value.parse::<SocketAddr>() {
        if address.port() == 0 {
            return Err(anyhow!("--advertise must use a nonzero port"));
        }
        if address.ip().is_unspecified() || address.ip().is_multicast() {
            return Err(anyhow!(
                "--advertise must be a concrete unicast address, not {address}"
            ));
        }
        if matches!(address.ip(), IpAddr::V4(ip) if ip.is_broadcast()) {
            return Err(anyhow!("--advertise cannot use the IPv4 broadcast address"));
        }
        return Ok(Advertise::Addr(address));
    }
    // Not an IP socket address, so treat it as HOST:PORT. Rejecting names
    // outright is what made celld unusable wherever nodes have a stable name
    // and no stable IP.
    let (host, port) = value.rsplit_once(':').ok_or_else(|| {
        anyhow!("--advertise must be HOST:PORT or IP:PORT, such as node1.example.com:8080")
    })?;
    let port: u16 = port.parse().with_context(|| {
        format!("--advertise port {port:?} is not a number between 1 and 65535")
    })?;
    if port == 0 {
        return Err(anyhow!("--advertise must use a nonzero port"));
    }
    if !is_dns_name(host) {
        return Err(anyhow!(
            "--advertise host {host:?} is not a valid hostname or IP address"
        ));
    }
    Ok(Advertise::Name { host: host.to_string(), port })
}

/// A conservative hostname check: labels of letters, digits and hyphens,
/// separated by dots. Bracketed IPv6 (`[::1]:8080`) is handled by the
/// SocketAddr branch above, so anything reaching here is a name.
fn is_dns_name(host: &str) -> bool {
    !host.is_empty()
        && host.len() <= 253
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
        })
}

fn validate_peer_id(value: &str) -> anyhow::Result<()> {
    if value.is_empty()
        || value.len() > 200
        || value.contains('/')
        || value.contains('?')
        || value.contains('#')
        || value.chars().any(char::is_whitespace)
    {
        return Err(anyhow!(
            "--peer must be one node-session ID without a path or whitespace"
        ));
    }
    Ok(())
}

pub fn print_help() {
    println!(
        r#"celld — self-hosted Durable Objects

USAGE:
  celld --bucket s3://NAME [OPTIONS]
  celld deploy [PROJECT] --bucket s3://NAME [OPTIONS]
  celld diagnose --bucket s3://NAME [OPTIONS] [--peer NODE_ID]...

OPTIONS:
  --bucket s3://NAME     Fleet bucket; uses the standard AWS credential chain
  --endpoint URL         Optional S3-compatible endpoint
  --region REGION        Storage region (default: AWS_REGION or us-east-1)
  --listen IP:PORT       Listener; explicit conflicts fail (default: 127.0.0.1:8080)
  --advertise ADDR:PORT  Address peers can reach: IP:PORT or HOST:PORT
                         (required when --listen is 0.0.0.0 or ::)
  --unsafe-public-advertise
                         Permit a public peer IP without built-in TLS
  -h, --help             Show this help
  -V, --version          Show the celld version

ENVIRONMENT:
  CELLD_BUCKET                    Fleet bucket; same as --bucket
  S3_ENDPOINT                     S3-compatible endpoint; same as --endpoint
  AWS_REGION, AWS_DEFAULT_REGION  Storage region (default: us-east-1)
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN
                                  Explicit credentials in the standard AWS chain
  CELLD_ADDR                      Listener; same as --listen
  CELLD_ADVERTISE                 Peer-reachable address; same as --advertise
  CELLD_UNSAFE_PUBLIC_ADVERTISE   `on` permits a literal public peer IP
  CELLD_NODE                      Node-session ID (default: generated)
  CELLD_WATCH                     Local SQLite/Litestream working directory
  LITESTREAM_BIN, CELLD_ESBUILD   Override helper executable paths
  CELLD_WORKERS                   Stateless Worker pool size (default: 16)
  CELLD_ACTIVATIONS               Concurrent cold activations (default: min(workers, 128))
  CELLD_HIBERNATIONS              Concurrent replica-safe hibernations (default: 4)
  CELLD_MAX_COHOSTED              Service-binding isolate limit (default: 8)
  CELLD_VARS_FILE, CELLD_VAR_*    Worker variable overrides
  CELLD_ASSET_CACHE_DIR           Downloaded static-asset cache directory
  CELLD_ASSET_CACHE_BYTES         Asset cache limit (default: 536870912)

TUNING:
  CELLD_TTL_MS                    Node lease lifetime (default: 10000)
  CELLD_LEASE_LINGER_MS           Empty-node lease linger (default: lease TTL)
  CELLD_IDLE_EVICT_S              Idle-cell eviction age (default: 300)
  CELLD_MAX_RESIDENT_CELLS        Enable resident-cell pressure shedding
  CELLD_RESIDENT_LOW_WATER        Resident recovery target (default: 80%).
                                  This, not MAX_RESIDENT_CELLS, is the usable
                                  ceiling: residency pressure sheds to it and
                                  holds there, so the gap is capacity you do
                                  not get. Lab: widening 900->980 of a 1000
                                  ceiling doubled throughput on an
                                  oversubscribed working set, at the cost of
                                  more capacity-wait timeouts.
  CELLD_PRESSURE_OWNERSHIP         release to rebalance, sticky to cache locally
  CELLD_MAX_RSS_MB                Linux RSS pressure threshold
  CELLD_MAX_CPU_PERCENT           Linux CPU pressure threshold
  CELLD_ALARM_RESIDENT_MS         Near-alarm residency window (default: 3600000)
  CELLD_WAKER_TICK_MS             Orphan-alarm scan interval (default: 60000)
  CELLD_V8_HEAP_LIMIT_MB          Per-isolate V8 heap limit
  CELLD_FETCH_TIMEOUT_S           Outbound fetch timeout (default: 120)
  CELLD_HANDLER_BUDGET_S          JavaScript handler budget (default: 300)
  CELLD_TOKIO_THREADS             Tokio runtime worker threads
  GOMEMLIMIT                      Litestream Go heap limit (default child: 512MiB)
  RUST_LOG                        Runtime log filter (default: info)

EXPERIMENTAL:
  CELLD_LAZY_NODE_LEASE           Node lease lifecycle: on or shadow
  CELLD_AI_BINDING, CELLD_AI_URL  AI binding name and endpoint

Deploy an application to the bucket first, then start every node with the
same bucket settings. Nodes discover one another through bucket leases; there
is no join command.

Documentation: https://celld.dev/docs"#
    );
}

pub fn print_version() {
    // Debug builds compile in the `#[cfg(debug_assertions)]` isolate-startup
    // test hooks; the marker lets the black-box suite skip tests that need them.
    let profile = if cfg!(debug_assertions) { " (debug)" } else { "" };
    println!("celld {}{profile}", env!("CARGO_PKG_VERSION"));
}

pub fn print_diagnose_help() {
    println!(
        "celld diagnose — non-mutating fleet connectivity checks\n\n\
USAGE:\n  celld diagnose [OPTIONS] [--peer NODE_ID]...\n\n\
OPTIONS:\n  --bucket s3://NAME     Fleet bucket; uses the standard AWS credential chain\n  --endpoint URL         Optional S3-compatible endpoint\n  --region REGION        Storage region\n  --listen IP:PORT       Verify that this listener can be bound\n  --advertise ADDR:PORT  Validate the address peers should use\n  --peer NODE_ID         Verify one node with a signed direct probe; repeatable\n  --unsafe-public-advertise\n                         Permit a public peer IP without built-in TLS\n  -h, --help             Show this help\n\n\
Without --peer the command enumerates every node lease in the bucket. It reports\n\
expired, malformed, unreachable, and protocol-incompatible peers independently.\n\
The command reads bucket/node records and opens direct probes; it does not\n\
acquire a node lease or change cell ownership."
    );
}

#[cfg(test)]
mod tests {
    use super::{parse_action, Action, CredentialSource, Listen, RuntimeSettings};
    use std::collections::HashMap;

    fn env(values: &[(&str, &str)]) -> HashMap<String, String> {
        values
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    fn run(action: Action) -> RuntimeSettings {
        match action {
            Action::Run(settings) => settings,
            other => panic!("expected run action, got {other:?}"),
        }
    }

    #[test]
    fn bare_start_shows_standalone_help() {
        assert_eq!(
            parse_action(Vec::new(), &HashMap::new()).unwrap(),
            Action::Help
        );
    }

    #[test]
    fn explicit_managed_byo_bucket_uses_the_standard_aws_credential_chain() {
        let settings = run(parse_action(
            vec![
                "--control-plane".to_string(),
                "--bucket".to_string(),
                "s3://customer-bucket".to_string(),
                "--endpoint".to_string(),
                "https://objects.example.com".to_string(),
                "--region".to_string(),
                "us-west-2".to_string(),
            ],
            &HashMap::new(),
        )
        .unwrap());
        assert!(settings.control_plane);
        assert_eq!(settings.bucket.as_deref(), Some("customer-bucket"));
        assert_eq!(
            settings.credential_source,
            CredentialSource::StandardAwsChain
        );
        assert_eq!(settings.listen, Listen::AutoLoopback);
    }

    #[test]
    fn standalone_requires_only_a_bucket() {
        assert!(
            parse_action(
                vec!["--listen".to_string(), "127.0.0.1:9000".to_string()],
                &HashMap::new()
            )
                .unwrap_err()
                .to_string()
                .contains("--bucket")
        );
        let settings = run(parse_action(
            vec![
                "--bucket".to_string(),
                "s3://fleet".to_string(),
            ],
            &HashMap::new(),
        )
        .unwrap());
        assert_eq!(settings.bucket.as_deref(), Some("fleet"));
    }

    #[test]
    fn standalone_diagnostics_require_a_bucket() {
        let action = parse_action(
            vec![
                "diagnose".to_string(),
                "--bucket".to_string(),
                "s3://fleet".to_string(),
                "--peer".to_string(),
                "node_one".to_string(),
                "--peer".to_string(),
                "node_two".to_string(),
            ],
            &HashMap::new(),
        )
        .unwrap();
        let Action::Diagnose { settings, peers } = action else {
            panic!("expected diagnose action");
        };
        assert!(!settings.control_plane);
        assert_eq!(settings.bucket.as_deref(), Some("fleet"));
        assert_eq!(peers, ["node_one", "node_two"]);
    }

    #[test]
    fn command_line_overrides_compatibility_environment() {
        let settings = run(parse_action(
            vec![
                "--bucket".to_string(),
                "s3://cli-bucket".to_string(),
                "--endpoint".to_string(),
                "https://cli.example.com/".to_string(),
                "--listen".to_string(),
                "127.0.0.1:9000".to_string(),
            ],
            &env(&[
                ("CELLD_BUCKET", "env-bucket"),
                ("S3_ENDPOINT", "https://env.example.com"),
                ("CELLD_ADDR", "127.0.0.1:8000"),
            ]),
        )
        .unwrap());
        assert_eq!(settings.bucket.as_deref(), Some("cli-bucket"));
        assert_eq!(
            settings.endpoint.as_deref(),
            Some("https://cli.example.com")
        );
        assert_eq!(
            settings.listen,
            Listen::Explicit("127.0.0.1:9000".to_string())
        );
        assert_eq!(
            settings.credential_source,
            CredentialSource::StandardAwsChain
        );
    }

    #[test]
    fn compatibility_environment_still_starts_standalone() {
        let settings = run(parse_action(
            Vec::new(),
            &env(&[
                ("CELLD_CLOUD", "off"),
                ("CELLD_BUCKET", "fleet"),
                ("CELLD_ADDR", "127.0.0.1:8123"),
            ]),
        )
        .unwrap());
        assert!(!settings.control_plane);
        assert_eq!(settings.bucket.as_deref(), Some("fleet"));
    }

    #[test]
    fn unknown_arguments_and_malformed_values_fail() {
        assert!(parse_action(vec!["--wat".to_string()], &HashMap::new()).is_err());
        assert!(parse_action(
            vec!["--script".to_string(), "old-selector".to_string()],
            &HashMap::new(),
        )
        .is_err());
        assert!(parse_action(
            vec!["--bucket".to_string(), "s3://bucket/path".to_string()],
            &HashMap::new(),
        )
        .is_err());
        assert!(parse_action(
            vec!["--advertise".to_string(), "0.0.0.0:8080".to_string()],
            &HashMap::new(),
        )
        .is_err());
        assert!(parse_action(
            vec!["--advertise".to_string(), "127.0.0.1:0".to_string()],
            &HashMap::new(),
        )
        .is_err());
        assert!(parse_action(
            vec![
                "diagnose".to_string(),
                "--peer".to_string(),
                "../node".to_string(),
            ],
            &HashMap::new(),
        )
        .is_err());
        assert!(parse_action(
            vec!["--peer".to_string(), "node".to_string()],
            &HashMap::new(),
        )
        .is_err());
    }

    #[test]
    fn connect_arguments_are_kept_separate_from_runtime_arguments() {
        assert_eq!(
            parse_action(
                vec![
                    "connect".to_string(),
                    "--env".to_string(),
                    "preview".to_string()
                ],
                &HashMap::new(),
            )
            .unwrap(),
            Action::Connect(vec!["--env".to_string(), "preview".to_string()])
        );
    }

    #[test]
    fn credential_arguments_are_kept_separate_from_runtime_arguments() {
        assert_eq!(
            parse_action(
                vec!["credentials".to_string(), "refresh".to_string()],
                &HashMap::new(),
            )
            .unwrap(),
            Action::Credentials(vec!["refresh".to_string()])
        );
    }

    #[test]
    fn disconnect_arguments_are_kept_separate_from_runtime_arguments() {
        assert_eq!(
            parse_action(
                vec!["disconnect".to_string(), "--yes".to_string()],
                &HashMap::new(),
            )
            .unwrap(),
            Action::Disconnect(vec!["--yes".to_string()])
        );
    }

    #[test]
    fn debug_output_contains_configuration_but_no_credential_values() {
        let settings = run(parse_action(
            vec![
                "--bucket".to_string(),
                "fleet".to_string(),
            ],
            &HashMap::new(),
        )
        .unwrap());
        let output = format!("{settings:?}");
        assert!(output.contains("StandardAwsChain"));
        assert!(!output.contains("access_key"));
        assert!(!output.contains("secret"));
    }

    #[test]
    fn advertise_accepts_hostnames_and_ips() {
        use super::{parse_advertise, Advertise};
        assert_eq!(
            parse_advertise("10.0.0.4:8080").unwrap(),
            Advertise::Addr("10.0.0.4:8080".parse().unwrap())
        );
        // The case that mattered: a stable name, no stable IP.
        assert_eq!(
            parse_advertise("node1.example.com:8000").unwrap(),
            Advertise::Name { host: "node1.example.com".into(), port: 8000 }
        );
        assert_eq!(
            parse_advertise("[::1]:8080").unwrap(),
            Advertise::Addr("[::1]:8080".parse().unwrap())
        );
        for bad in [
            "node1.example.com",   // no port
            "node1.example.com:0", // zero port
            "node1.example.com:99999",
            "0.0.0.0:8080",        // unspecified
            "-bad.example.com:80", // leading hyphen
            "bad_host.example:80", // underscore is not a DNS label char
            ":8080",
        ] {
            assert!(parse_advertise(bad).is_err(), "expected {bad:?} to be rejected");
        }
    }

    #[tokio::test]
    async fn auto_loopback_reserves_distinct_ports_for_two_processes() {
        let settings = run(parse_action(
            vec!["--control-plane".to_string()],
            &HashMap::new(),
        ).unwrap());
        let first = super::bind_listener(&settings).await.unwrap();
        let second = super::bind_listener(&settings).await.unwrap();
        assert!(first.listen.ip().is_loopback());
        assert!(second.listen.ip().is_loopback());
        assert_ne!(first.listen, second.listen);
        assert_eq!(second.listen.port(), first.listen.port() + 1);
        assert_eq!(first.advertise, super::Advertise::Addr(first.listen));
        assert_eq!(second.advertise, super::Advertise::Addr(second.listen));
    }

    #[tokio::test]
    async fn explicit_listen_conflict_fails_without_scanning() {
        let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = occupied.local_addr().unwrap();
        let mut settings = run(parse_action(
            vec!["--control-plane".to_string()],
            &HashMap::new(),
        ).unwrap());
        settings.listen = Listen::Explicit(address.to_string());
        let error = super::bind_listener(&settings).await.unwrap_err();
        assert!(error.to_string().contains("bind --listen"));
    }

    #[tokio::test]
    async fn unspecified_listener_requires_an_advertised_address() {
        let mut settings = run(parse_action(
            vec!["--control-plane".to_string()],
            &HashMap::new(),
        ).unwrap());
        settings.listen = Listen::Explicit("0.0.0.0:0".to_string());
        let error = super::bind_listener(&settings).await.unwrap_err();
        assert!(error.to_string().contains("--advertise is required"));
    }

    #[cfg(celld_internal_tests)]
    mod private {
        include!(env!("CELLD_CONFORMANCE_STARTUP_TESTS"));
    }
}
