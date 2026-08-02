//! celld — the cell runtime. A stateless **Worker pool** (round-robin isolates)
//! fronts a registry of **per-cell DO isolates**: `env.DO.get(id).fetch()` routes
//! through the host (`__do_call`), which activates the cell in its own isolate
//! (local) or proxies to the owning peer (remote). Cells hibernate when idle
//! (isolate thread exits + replication released) and wake on the next request
//! — requests are the ONLY wake source: a cell with a pending alarm is kept
//! resident, because hibernation would discard its one wakeup path.
//! The bucket is the deploy channel; the per-node lease is the only heartbeat.
mod assets;
mod js;
mod asyncrt;
mod storage;
mod ownership;
mod peer_auth;
mod peer_probe;
mod protocol;
mod replication;
mod control_plane;
mod deploy;
mod startup;
mod wake;
#[cfg(all(test, celld_internal_tests))]
mod fault;

use anyhow::Context;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::Client;
use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use crate::protocol::{DeployPointer, Manifest};
use celld_logic::routing;
use celld_logic::pressure::Load;
use celld_logic::pressure::PressureConfig;
use js::CellJob;
use rand::RngCore;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tracing::debug;
use tracing::info;
use tracing::warn;

/// A request for one Worker isolate (stateless; any isolate serves any request).
enum WorkerJob {
    Fetch { queued_at: Instant,
            url: String, method: String, body: Vec<u8>, headers: Vec<(String, String)>,
            request_id: Option<js::RequestId>,
            reply: tokio::sync::oneshot::Sender<anyhow::Result<js::HttpResponse>> },
    /// A service binding with `entrypoint = "Name"` calling one of its
    /// methods. Arguments and result are V8 structured-clone bytes.
    Rpc { entrypoint: String, method: String, args: Vec<u8>,
          reply: tokio::sync::oneshot::Sender<anyhow::Result<Vec<u8>>> },
}

struct Cell {
    tx: mpsc::Sender<CellJob>,
    activity: Arc<CellActivity>,
    next_alarm_ms: Arc<std::sync::atomic::AtomicI64>,
    /// The alarm this cell restored with, latched once at activation. The
    /// live mirror above is -1 by the time the sweep first sees a cell whose
    /// alarm fired immediately, which is exactly when the wake entry that
    /// woke it would otherwise be orphaned.
    activation_alarm_ms: Arc<std::sync::atomic::AtomicI64>,
}

const CELL_CLOSING: usize = 1;
const CELL_ACTIVITY_UNIT: usize = 2;

struct CellActivity {
    state: AtomicUsize,
}

pub(crate) struct CellActivityGuard {
    activity: Arc<CellActivity>,
}

/// Cancels the Worker request if Hyper drops the ingress future because the
/// client disconnected before a response was published. This adds no task or
/// channel to the request path: dropping the stack guard only marks the
/// request id for the isolate's existing event-loop poll.
struct IngressAbortGuard(Option<js::RequestId>);

impl IngressAbortGuard {
    fn new(request_id: js::RequestId) -> Self {
        Self(Some(request_id))
    }

    fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for IngressAbortGuard {
    fn drop(&mut self) {
        if let Some(request_id) = self.0 {
            js::abort_request(request_id);
        }
    }
}

#[derive(Default)]
struct AdvisoryActivity {
    acquired: AtomicU64,
    proxied: AtomicU64,
    expired_owner_leases: AtomicU64,
    restored: AtomicU64,
    advanced_epochs: AtomicU64,
}

#[derive(Clone, Copy)]
struct AdvisoryActivitySnapshot {
    acquired: u64,
    proxied: u64,
    expired_owner_leases: u64,
    restored: u64,
    advanced_epochs: u64,
}

impl AdvisoryActivity {
    fn record_acquisition(&self, epoch: u64, replaced_stale_owner: bool) {
        self.acquired.fetch_add(1, Ordering::Relaxed);
        if replaced_stale_owner {
            self.expired_owner_leases.fetch_add(1, Ordering::Relaxed);
        }
        if epoch > 1 {
            self.advanced_epochs.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_restore(&self) {
        self.restored.fetch_add(1, Ordering::Relaxed);
    }

    fn record_proxy(&self) {
        self.proxied.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> AdvisoryActivitySnapshot {
        AdvisoryActivitySnapshot {
            acquired: self.acquired.load(Ordering::Relaxed),
            proxied: self.proxied.load(Ordering::Relaxed),
            expired_owner_leases: self.expired_owner_leases.load(Ordering::Relaxed),
            restored: self.restored.load(Ordering::Relaxed),
            advanced_epochs: self.advanced_epochs.load(Ordering::Relaxed),
        }
    }
}

fn advisory_activity() -> &'static AdvisoryActivity {
    static ACTIVITY: std::sync::OnceLock<AdvisoryActivity> = std::sync::OnceLock::new();
    ACTIVITY.get_or_init(AdvisoryActivity::default)
}

impl CellActivity {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: AtomicUsize::new(0),
        })
    }

    fn try_acquire(self: &Arc<Self>) -> Option<CellActivityGuard> {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if state & CELL_CLOSING != 0 {
                return None;
            }
            match self.state.compare_exchange_weak(
                state,
                state + CELL_ACTIVITY_UNIT,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(CellActivityGuard {
                        activity: self.clone(),
                    });
                }
                Err(current) => state = current,
            }
        }
    }

    /// Reserve an otherwise idle cell for a top-level Worker request.
    ///
    /// The permit travels with the queued job and remains held until that job
    /// has completely left the cell event loop. Without it, a fast client can
    /// receive one response and enqueue the next Worker request while the
    /// prior event is still draining; the second request then appears as an
    /// unsupported reentrant Worker fetch.
    fn try_acquire_idle(self: &Arc<Self>) -> Option<CellActivityGuard> {
        self.state
            .compare_exchange(
                0,
                CELL_ACTIVITY_UNIT,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .ok()
            .map(|_| CellActivityGuard {
                activity: self.clone(),
            })
    }

    fn begin_close(&self) -> bool {
        self.state
            .compare_exchange(0, CELL_CLOSING, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Revert a `begin_close` that has not led to registry removal yet. Only
    /// valid for the caller that won the CAS.
    fn abort_close(&self) {
        self.state.store(0, Ordering::Release);
    }
}

impl Drop for CellActivityGuard {
    fn drop(&mut self) {
        let previous = self
            .activity
            .state
            .fetch_sub(CELL_ACTIVITY_UNIT, Ordering::AcqRel);
        debug_assert_eq!(previous & CELL_CLOSING, 0);
        debug_assert!(previous >= CELL_ACTIVITY_UNIT);
    }
}

impl Cell {
    fn route(&self) -> Option<CellRoute> {
        Some(CellRoute::Local {
            tx: self.tx.clone(),
            _activity: self.activity.try_acquire()?,
        })
    }
}

// Remote cell activation holds a stateless isolate while ownership and restore
// complete. This is I/O concurrency, not a CPU-sized execution pool.
const DEFAULT_STATELESS_WORKERS: usize = 16;
// Keep bulk ownership/restore work below the point where it can starve the
// node's authoritative bucket heartbeat. This is intentionally independent
// of the stateless Worker pool: operators can add warm-request concurrency
// without multiplying cold R2/Litestream concurrency without bound.
const DEFAULT_MAX_CONCURRENT_ACTIVATIONS: usize = 128;
const DEFAULT_MAX_CONCURRENT_HIBERNATIONS: usize = 4;

/// Shared-queue pool of stateless Worker isolates (scale via `CELLD_WORKERS`).
///
/// A queue per isolate creates head-of-line blocking: one slow remote request
/// strands every later request round-robined to that isolate even while another
/// isolate is idle. A single receiver lets the next available isolate take the
/// oldest request.
pub(crate) struct WorkerPool {
    tx: mpsc::Sender<WorkerJob>,
}

impl WorkerPool {
    fn new(
        config: Arc<js::WorkerConfig>,
        workers: usize,
        node: &str,
        region: &str,
    ) -> Self {
        debug_assert!(workers > 0);
        let (tx, rx) = mpsc::channel();
        let rx = Arc::new(Mutex::new(rx));
        let node: Arc<str> = node.into();
        let region: Arc<str> = region.into();
        for _ in 0..workers {
            spawn_worker_with(
                config.clone(),
                rx.clone(),
                node.clone(),
                region.clone(),
            );
        }
        Self { tx }
    }

    fn send(&self, job: WorkerJob) -> Result<(), ()> {
        self.tx.send(job).map_err(|_| ())
    }
}

/// Default ON: run the worker `fetch` inside a resident cell isolate so a local
/// `env.NS.get(id).fetch()` resolves IN-ISOLATE, skipping the worker→cell
/// `__do_call` double-hop (the async park + second isolate). Measured 2.5-5.4×
/// faster than the pool on the DO path, and faster even for cross-cell requests.
/// Pure workers (no resident cells) and WS upgrades fall back to the pool.
/// Everything the router needs, shared (Arc) across tasks.
struct Ctx {
    c: Client,
    repl: Arc<replication::NodeRepl>,
    http: reqwest::Client,
    registry: Mutex<HashMap<String, Cell>>,
    // The sans-IO per-scope coordination truth (`celld_logic::lifecycle::Cell`):
    // phase, residency, serving. Activation drives it through `on_event`, so
    // production runs the lifecycle the simulator tests. `registry` still holds
    // the live isolate `tx` (which the pure Cell cannot own); this holds the
    // decision state that `epochs`/`hibernated_owned`/`owed_activations` track
    // ad hoc today, and later slices fold those in.
    lifecycle_cells: Mutex<HashMap<String, celld_logic::lifecycle::Cell>>,
    epochs: Mutex<HashMap<String, u64>>,
    // Pressure-sticky hibernation keeps this process's fenced ownership and
    // can restore the same epoch without another bucket CAS while the
    // continuously renewed node lease remains authoritative.
    hibernated_owned: Mutex<HashMap<String, u64>>,
    owner_cache: Mutex<HashMap<String, ownership::ResolvedOwner>>,
    // Cell ownership changes only through a fenced epoch transition, while a
    // node lease is renewed frequently and is shared by every cell on that
    // node. Cache and coalesce the lease separately so route refresh costs one
    // bucket read per node, not one read per cell.
    node_cache: Mutex<HashMap<String, ownership::NodeRec>>,
    // A missing or expired node record is also a shared observation. Without
    // a short negative cache, every waiter behind `node_resolving` repeats the
    // same GET serially, turning an unavailable owner into a many-second
    // convoy. Cell acquisition still re-reads the lease before takeover, so
    // this only coalesces routing hints; it never weakens fencing.
    node_unavailable_cache: Mutex<HashMap<String, Instant>>,
    node_resolving: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    capacity_peers: tokio::sync::Mutex<ownership::CapacityPeerCache>,
    node_ttl_ms: u64,
    // A remote abort can race ahead of the original proxy request. Keep a
    // short-lived tombstone and serialize the target-cell enqueue with abort
    // delivery so the owner cannot lose that cancellation.
    remote_aborts: Mutex<HashMap<js::RequestId, std::time::Instant>>,
    // per-scope activation latch: concurrent cold-starts of the SAME cell
    // serialize on it (one activates, the rest wait then find it resident);
    // different cells activate concurrently.
    activating: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    activation_slots: Arc<tokio::sync::Semaphore>,
    resident_reservations: Arc<AtomicUsize>,
    resident_capacity_changed: Arc<tokio::sync::Notify>,
    // Cells whose ownership CAS won but whose activation then failed. Holding
    // ownership without running the cell strands its alarm: the waker cannot
    // see it (no wake entry yet) and dead-node reconciliation cannot see it
    // (this node's lease is alive). Ownership you do not honour is v1 failure
    // class 5 — the sweep retries these until they activate.
    owed_activations: Mutex<std::collections::HashSet<String>>,
    bucket: String,
    endpoint: Option<String>,
    region: String,
    storage_credentials: Option<replication::StorageCredentials>,
    litestream: String,
    node: String,
    advertise: String,
    peer_auth: Arc<peer_auth::PeerAuth>,
    worker_config: Arc<js::WorkerConfig>,
    owned_cells: Arc<AtomicUsize>,
    owned_cell_inventory: Arc<Mutex<HashMap<String, u64>>>,
    lease_manager: Arc<ownership::NodeLeaseManager>,
    node_load: Arc<ownership::NodeLoadState>,
    admission_pressure: AtomicBool,
    /// `pressure_config` with the residency ceiling removed, so the sampled
    /// admission latch carries only what sampling is actually needed for.
    admission_pressure_config: PressureConfig,
    shedding: AtomicBool,
    pressure_config: PressureConfig,
    pressure_ownership: PressureOwnership,
    lazy_leases: bool,
    websocket_counts: Mutex<HashMap<String, usize>>,
    /// Concurrent outbound WebSockets per cell, capped because each one pins
    /// its cell resident for the life of the connection.
    outbound_websockets: Mutex<HashMap<String, usize>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PressureOwnership {
    /// Releasing ownership lets demand rebalance onto newly added capacity.
    Release,
    /// Retaining ownership makes pressure eviction a local residency-cache
    /// operation and avoids distributed coordination on every cache miss.
    Sticky,
}

impl PressureOwnership {
    fn from_environment() -> anyhow::Result<Self> {
        match std::env::var("CELLD_PRESSURE_OWNERSHIP") {
            Ok(value) if value == "release" => Ok(Self::Release),
            Ok(value) if value == "sticky" => Ok(Self::Sticky),
            Ok(value) => anyhow::bail!(
                "CELLD_PRESSURE_OWNERSHIP must be `release` or `sticky`, got {value:?}"
            ),
            Err(std::env::VarError::NotPresent) => Ok(Self::Release),
            Err(error) => Err(error.into()),
        }
    }
}

/// The sans-IO `PressureConfig` classifier lives in `celld_logic::pressure`;
/// only its construction reads the environment, so that adapter stays here at
/// the I/O edge as a free function (it cannot be an inherent method on a type
/// from another crate).
fn pressure_config_from_environment() -> anyhow::Result<PressureConfig> {
    let resident_high = parse_positive_environment::<usize>(
        "CELLD_MAX_RESIDENT_CELLS",
    )?;
    let resident_low = match (
        resident_high,
        parse_positive_environment::<usize>("CELLD_RESIDENT_LOW_WATER")?,
    ) {
        (Some(high), Some(low)) if low < high => low,
        (Some(high), Some(low)) => anyhow::bail!(
            "CELLD_RESIDENT_LOW_WATER ({low}) must be below \
             CELLD_MAX_RESIDENT_CELLS ({high})"
        ),
        (Some(high), None) => high.saturating_mul(4) / 5,
        (None, Some(_)) => anyhow::bail!(
            "CELLD_RESIDENT_LOW_WATER requires CELLD_MAX_RESIDENT_CELLS"
        ),
        (None, None) => 0,
    };
    let rss_high_bytes = parse_positive_environment::<u64>("CELLD_MAX_RSS_MB")?
        .map(|megabytes| megabytes.saturating_mul(1024 * 1024));
    let cpu_high_x100 = parse_positive_environment::<u64>("CELLD_MAX_CPU_PERCENT")?
        .map(|percent| percent.saturating_mul(100));
    Ok(PressureConfig {
        resident_high,
        resident_low,
        rss_high_bytes,
        cpu_high_x100,
    })
}

fn parse_positive_environment<T>(name: &str) -> anyhow::Result<Option<T>>
where
    T: std::str::FromStr + PartialOrd + Default + std::fmt::Display,
    T::Err: std::fmt::Display,
{
    let Some(value) = std::env::var(name).ok() else {
        return Ok(None);
    };
    let parsed = value
        .parse::<T>()
        .map_err(|error| anyhow::anyhow!("{name} must be a positive number: {error}"))?;
    if parsed <= T::default() {
        anyhow::bail!("{name} must be greater than zero, not {parsed}");
    }
    Ok(Some(parsed))
}

#[derive(Default)]
struct ProcessLoadSampler {
    previous_cpu_ticks: Option<u64>,
    previous_sample: Option<std::time::Instant>,
}

impl ProcessLoadSampler {
    fn sample_cpu_percent_x100(&mut self) -> u64 {
        let Some(ticks) = process_cpu_ticks() else {
            return 0;
        };
        let now = std::time::Instant::now();
        let value = match (self.previous_cpu_ticks, self.previous_sample) {
            (Some(previous_ticks), Some(previous_sample)) => {
                let elapsed = previous_sample.elapsed().as_secs_f64();
                let ticks_per_second = clock_ticks_per_second() as f64;
                if elapsed > 0.0 && ticks_per_second > 0.0 {
                    (((ticks.saturating_sub(previous_ticks)) as f64
                        / ticks_per_second
                        / elapsed)
                        * 10_000.0) as u64
                } else {
                    0
                }
            }
            _ => 0,
        };
        self.previous_cpu_ticks = Some(ticks);
        self.previous_sample = Some(now);
        value
    }
}

#[cfg(target_os = "linux")]
fn process_cpu_ticks() -> Option<u64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let fields = stat.get(stat.rfind(')')? + 2..)?.split_whitespace().collect::<Vec<_>>();
    Some(fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?)
}

#[cfg(not(target_os = "linux"))]
fn process_cpu_ticks() -> Option<u64> {
    None
}

#[cfg(unix)]
fn clock_ticks_per_second() -> u64 {
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    u64::try_from(ticks).ok().filter(|ticks| *ticks > 0).unwrap_or(100)
}

#[cfg(not(unix))]
fn clock_ticks_per_second() -> u64 {
    100
}

#[cfg(target_os = "linux")]
fn resident_memory_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|stat| stat.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map(|pages| pages.saturating_mul(page_size()))
        .unwrap_or(0)
}

#[cfg(not(target_os = "linux"))]
fn resident_memory_bytes() -> u64 {
    0
}

#[cfg(target_os = "linux")]
fn page_size() -> u64 {
    let bytes = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    u64::try_from(bytes).ok().filter(|bytes| *bytes > 0).unwrap_or(4096)
}

#[cfg(target_os = "linux")]
fn open_file_descriptors() -> u64 {
    std::fs::read_dir("/proc/self/fd")
        .ok()
        .map(|entries| entries.count() as u64)
        .unwrap_or(0)
}

#[cfg(not(target_os = "linux"))]
fn open_file_descriptors() -> u64 {
    0
}

#[cfg(unix)]
fn file_descriptor_limit() -> u64 {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } == 0 {
        limit.rlim_cur
    } else {
        0
    }
}

#[cfg(not(unix))]
fn file_descriptor_limit() -> u64 {
    0
}

#[derive(Clone)]
struct AppState {
    workers: Arc<WorkerPool>,
    cx: Arc<Ctx>,
    assets: Option<assets::AssetResolver>,
}

enum CellRoute {
    Local {
        tx: mpsc::Sender<CellJob>,
        _activity: CellActivityGuard,
    },
    Remote(ownership::ResolvedOwner),
}

const DURABLE_OBJECT_ROUTING_ERROR_MARKER: &str = "__CELLD_DO_ROUTING_ERROR__:";
const STALE_ROUTE_HEADER: &str = "x-cells-route-error";
const STALE_ROUTE_VALUE: &str = "stale-owner";
const REMOTE_ABORT_TTL: Duration = Duration::from_secs(600);

#[derive(Debug)]
struct StaleRoute {
    scope: String,
}

impl std::fmt::Display for StaleRoute {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "stale route for {}", self.scope)
    }
}

impl std::error::Error for StaleRoute {}

#[derive(Debug)]
struct CapacityExhausted {
    scope: String,
}

impl std::fmt::Display for CapacityExhausted {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "resident capacity exhausted for {}", self.scope)
    }
}

impl std::error::Error for CapacityExhausted {}

fn stale_route_response(scope: &str) -> Response {
    Response::builder()
        .status(StatusCode::CONFLICT)
        .header(STALE_ROUTE_HEADER, STALE_ROUTE_VALUE)
        .body(Body::from(
            serde_json::json!({
                "error": "stale_owner",
                "scope": scope,
            })
            .to_string(),
        ))
        .unwrap()
}

fn response_is_stale(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get(STALE_ROUTE_HEADER)
        .is_some_and(|value| value == STALE_ROUTE_VALUE)
}

fn prune_remote_aborts(aborts: &mut HashMap<js::RequestId, std::time::Instant>) {
    aborts.retain(|_, created| created.elapsed() < REMOTE_ABORT_TTL);
}

fn owner_unreachable(
    scope: &str,
    owner: &str,
    source: reqwest::Error,
) -> anyhow::Error {
    let cause = std::error::Error::source(&source)
        .map(ToString::to_string)
        .unwrap_or_else(|| source.to_string());
    warn!(
        %scope,
        %owner,
        error = %source,
        %cause,
        connect = source.is_connect(),
        timeout = source.is_timeout(),
        request = source.is_request(),
        body = source.is_body(),
        decode = source.is_decode(),
        "peer owner unreachable"
    );
    let detail = serde_json::json!({
        "scope": scope,
        "owner": owner,
    });
    anyhow::Error::new(source).context(format!(
        "{DURABLE_OBJECT_ROUTING_ERROR_MARKER}{detail}",
    ))
}

/// Drop a cached node record so the next resolution re-reads it. A node's
/// address is otherwise cached until its lease expires, which would make a
/// failed route sticky for the whole lease window — including across the
/// redispatch that is supposed to escape it.
fn forget_node_route(cx: &Ctx, node: &str) {
    cx.node_cache.lock().unwrap().remove(node);
    cx.node_unavailable_cache.lock().unwrap().remove(node);
}

/// How far a failed peer dispatch got, which is what decides whether it may be
/// re-sent. Only a connect-phase transport error is known to have put no bytes
/// on the wire; anything else may have run on the owner already.
fn classify_attempt(error: Option<&anyhow::Error>) -> Option<routing::Attempt> {
    let error = error?;
    if error.downcast_ref::<StaleRoute>().is_some() {
        return Some(routing::Attempt::NotOwner);
    }
    let connect = error
        .downcast_ref::<reqwest::Error>()
        .is_some_and(reqwest::Error::is_connect);
    Some(if connect {
        routing::Attempt::NeverConnected
    } else {
        routing::Attempt::Ambiguous
    })
}

fn stale_owner_unavailable(scope: &str, owner: &str) -> anyhow::Error {
    let detail = serde_json::json!({
        "scope": scope,
        "owner": owner,
    });
    anyhow::Error::new(StaleRoute {
        scope: scope.to_string(),
    })
    .context(format!("{DURABLE_OBJECT_ROUTING_ERROR_MARKER}{detail}"))
}

struct HostWebSocketGuard {
    cx: Arc<Ctx>,
    scope: String,
}

impl HostWebSocketGuard {
    fn new(cx: Arc<Ctx>, scope: &str) -> Self {
        *cx.websocket_counts
            .lock()
            .unwrap()
            .entry(scope.to_string())
            .or_default() += 1;
        Self {
            cx,
            scope: scope.to_string(),
        }
    }
}

impl Drop for HostWebSocketGuard {
    fn drop(&mut self) {
        let mut counts = self.cx.websocket_counts.lock().unwrap();
        let Some(count) = counts.get_mut(&self.scope) else {
            return;
        };
        *count = count.saturating_sub(1);
        if *count == 0 {
            counts.remove(&self.scope);
        }
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        .unwrap().as_millis() as i64
}

/// Durable Object namespace bindings from the deployed metadata.
fn worker_do_bindings(manifest: &Manifest) -> Vec<(String, String)> {
    manifest.raw_metadata
        .get("bindings").and_then(|v| v.as_array())
        .map(|a| a.iter()
            .filter(|b| b.get("type").and_then(|t| t.as_str())
                == Some("durable_object_namespace"))
            .filter_map(|b| Some((
                b.get("name")?.as_str()?.to_string(),
                b.get("class_name")?.as_str()?.to_string())))
        .collect())
        .unwrap_or_default()
}

/// R2 bucket bindings from the deployed metadata.
fn worker_r2_bindings(manifest: &Manifest) -> Vec<String> {
    manifest.raw_metadata
        .get("bindings").and_then(|v| v.as_array())
        .map(|a| a.iter()
            .filter(|b| b.get("type").and_then(|t| t.as_str()) == Some("r2_bucket"))
            .filter_map(|b| b.get("name")?.as_str().map(str::to_string))
        .collect())
        .unwrap_or_default()
}

/// The AI binding from the deployed metadata, if any.
fn worker_ai_binding(manifest: &Manifest) -> Option<String> {
    manifest.raw_metadata
        .get("bindings").and_then(|v| v.as_array())
        .and_then(|a| a.iter()
            .find(|b| b.get("type").and_then(|t| t.as_str()) == Some("ai"))
            .and_then(|b| b.get("name")?.as_str().map(str::to_string)))
}

/// `[[services]]` from the deployed metadata: (binding name, target script).
/// Wrangler emits these as bindings of type "service" with a `service` field.
fn worker_services(
    manifest: &Manifest,
) -> Vec<(String, String, Option<String>)> {
    let mut out = Vec::new();
    if let Some(bindings) = manifest.raw_metadata.get("bindings").and_then(|v| v.as_array()) {
        for binding in bindings {
            if binding.get("type").and_then(|t| t.as_str()) != Some("service") {
                continue;
            }
            let (Some(name), Some(service)) = (
                binding.get("name").and_then(|v| v.as_str()),
                binding.get("service").and_then(|v| v.as_str()),
            ) else {
                continue;
            };
            let entrypoint = binding
                .get("entrypoint")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            out.push((name.to_string(), service.to_string(), entrypoint));
        }
    }
    out
}

fn worker_vars(manifest: &Manifest) -> Vec<(String, String)> {
    let mut vars = HashMap::new();
    if let Some(bindings) = manifest.raw_metadata.get("bindings").and_then(|v| v.as_array()) {
        for binding in bindings {
            if binding.get("type").and_then(|v| v.as_str()) != Some("plain_text") {
                continue;
            }
            if let (Some(name), Some(value)) = (
                binding.get("name").and_then(|v| v.as_str()),
                binding.get("text").and_then(|v| v.as_str()),
            ) {
                vars.insert(name.to_string(), value.to_string());
            }
        }
    }
    if let Ok(path) = std::env::var("CELLD_VARS_FILE") {
        match std::fs::read_to_string(&path) {
            Ok(contents) => {
                for line in contents.lines() {
                    let line = line.trim();
                    if line.is_empty() || line.starts_with('#') { continue; }
                    let Some((name, raw)) = line.split_once('=') else { continue };
                    let name = name.trim();
                    if name.is_empty() { continue; }
                    let raw = raw.trim();
                    let value = raw.strip_prefix('"').and_then(|v| v.strip_suffix('"'))
                        .or_else(|| raw.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')))
                        .unwrap_or(raw);
                    vars.insert(name.to_string(), value.to_string());
                }
            }
            Err(e) => warn!(%e, %path, "could not read CELLD_VARS_FILE"),
        }
    }
    for (name, value) in std::env::vars() {
        if let Some(name) = name.strip_prefix("CELLD_VAR_") {
            if !name.is_empty() { vars.insert(name.to_string(), value); }
        }
    }
    let mut vars = vars.into_iter().collect::<Vec<_>>();
    vars.sort_by(|a, b| a.0.cmp(&b.0));
    vars
}

/// Compatibility switches from the manifest's flags and date. Each switch
/// follows Workerd's compatibility-date.capnp: an explicit enable/disable
/// flag wins, otherwise the compatibility date decides.
pub(crate) fn worker_compat(metadata: &serde_json::Value) -> js::Compat {
    let flags = metadata
        .get("compatibility_flags")
        .and_then(|value| value.as_array());
    let has_flag = |name: &str| {
        flags.is_some_and(|flags| {
            flags
                .iter()
                .any(|flag| flag.as_str().is_some_and(|flag| flag == name))
        })
    };
    let date = metadata
        .get("compatibility_date")
        .and_then(|value| value.as_str());
    let switch = |enable: &str, disable: &str, since: &str| {
        if has_flag(enable) {
            return true;
        }
        if has_flag(disable) {
            return false;
        }
        date.is_some_and(|date| date >= since)
    };
    js::Compat {
        delete_all_deletes_alarm: switch(
            "delete_all_deletes_alarm",
            "delete_all_preserves_alarm",
            "2026-02-24",
        ),
        // Obsolete opt-in with no date; `extends DurableObject` is the way.
        js_rpc: has_flag("js_rpc"),
        // The *removal* is dated, so the helpers exist before 2024-03-26.
        fetcher_get_put_delete: !switch(
            "fetcher_no_get_put_delete",
            "fetcher_has_get_put_delete",
            "2024-03-26",
        ),
        // Opt-in only for now: flipping the default would change what every
        // existing binary handler receives.
        websocket_standard_binary_type: has_flag("websocket_standard_binary_type"),
    }
}

/// A stateless Worker isolate for `config`. Used for the primary Worker and
/// for each co-hosted service-binding target.
fn spawn_worker_with(
    config: Arc<js::WorkerConfig>,
    rx: Arc<Mutex<mpsc::Receiver<WorkerJob>>>,
    node: Arc<str>,
    region: Arc<str>,
) {
    thread::spawn(move || {
        asyncrt::init();
        let mut w = match js::Worker::load_config(config, &[]) {
            Ok(w) => w,
            Err(e) => { warn!(%e, "worker isolate load failed"); return; }
        };
        loop {
            let job = match rx.lock().unwrap().recv() {
                Ok(job) => job,
                Err(_) => return,
            };
            match job {
                WorkerJob::Fetch {
                    queued_at, url, method, body, headers, request_id, reply,
                } => {
                    let queue_wait_us = queued_at.elapsed().as_micros() as u64;
                    let execution_started = Instant::now();
                    let result = w.fetch_and_reply_id(
                        &url, &method, &body, &headers, request_id, reply);
                    let execution_us = execution_started.elapsed().as_micros() as u64;
                    if let Some(request_id) = request_id {
                        info!(
                            event = "worker_fetch_timing",
                            outcome = if result.is_ok() { "completed" } else { "reload_error" },
                            request_id = %js::request_id_string(request_id),
                            node = %node,
                            region = %region,
                            runtime_version = env!("CARGO_PKG_VERSION"),
                            total_us = queued_at.elapsed().as_micros() as u64,
                            queue_wait_us,
                            execution_us,
                            "stateless Worker fetch completed"
                        );
                    }
                    if let Err(error) = result {
                        warn!(%error, "worker isolate reload failed");
                        break;
                    }
                }
                WorkerJob::Rpc { entrypoint, method, args, reply } => {
                    let _ = reply.send(
                        w.dispatch_entrypoint_rpc(&entrypoint, &method, args));
                }
            }
        }
    });
}

/// Spawn a cell's DO isolate on its own thread. It serves `dispatch_to` for its
/// one scope and self-polls its alarm every 500ms between jobs. Thread exit
/// (Shutdown / channel close) drops the isolate — that IS hibernation.
/// Fire the cell's due alarm, if any, applying the retry policy. Called from
/// the idle timeout AND after each job: a cell under steady sub-500ms traffic
/// never hits the timeout, and the reentrant path only covers requests still
/// in flight — without the post-job check, traffic starves alarms forever.
fn fire_due_alarm(sc: &str, w: &mut js::Worker, rx: &mpsc::Receiver<CellJob>) {
    let now = now_ms();
    if let Some((scheduled_at, retry)) = storage::due_alarm_entry(sc, now) {
        storage::begin_alarm_handler(sc, scheduled_at);
        match w.fire_alarm(sc, retry, rx) {
            Ok(()) => {
                info!(cell = %sc, "alarm fired");
                storage::finish_alarm_handler(sc, true, now);
            }
            Err(e) => {
                let counts_against_limit = e.counts_against_limit();
                warn!(%e, cell = %sc, counts_against_limit, "alarm failed; backing off");
                storage::finish_alarm_handler_with_retry_policy(
                    sc,
                    false,
                    now,
                    counts_against_limit,
                );
            }
        }
    }
}

struct CellIsolateStartupTiming {
    started: Instant,
    scope: String,
    node: String,
    region: String,
    epoch: u64,
    fresh: bool,
}

impl CellIsolateStartupTiming {
    fn new(cx: &Ctx, scope: &str, epoch: u64, fresh: bool) -> Self {
        Self {
            started: Instant::now(),
            scope: scope.to_string(),
            node: cx.node.clone(),
            region: cx.region.clone(),
            epoch,
            fresh,
        }
    }

    fn emit(&self, outcome: &str, failure_phase: &str) {
        info!(
            event = "cell_isolate_startup_timing",
            outcome,
            failure_phase,
            scope = %self.scope,
            node = %self.node,
            region = %self.region,
            runtime_version = env!("CARGO_PKG_VERSION"),
            epoch = self.epoch,
            fresh = self.fresh,
            total_us = self.started.elapsed().as_micros() as u64,
            "cell isolate startup completed"
        );
    }
}

fn spawn_cell(
    scope: &str,
    db_path: &str,
    cx: &Arc<Ctx>,
    epoch: u64,
    fresh: bool,
) -> (
    Cell,
    tokio::sync::oneshot::Receiver<anyhow::Result<()>>,
) {
    let (tx, rx) = mpsc::channel::<CellJob>();
    let (startup_tx, startup_rx) = tokio::sync::oneshot::channel();
    let activity = CellActivity::new();
    let thread_activity = activity.clone();
    let next_alarm_ms = Arc::new(std::sync::atomic::AtomicI64::new(-1));
    let thread_next_alarm_ms = next_alarm_ms.clone();
    let activation_alarm_ms = Arc::new(std::sync::atomic::AtomicI64::new(-1));
    let thread_activation_alarm_ms = activation_alarm_ms.clone();
    let (sc, db, config) = (
        scope.to_string(),
        db_path.to_string(),
        cx.worker_config.clone(),
    );
    let startup_timing = CellIsolateStartupTiming::new(cx, scope, epoch, fresh);
    thread::spawn(move || {
        asyncrt::init();
        #[cfg(debug_assertions)]
        if let Ok(barrier) = std::env::var("CELLD_TEST_CELL_STARTUP_BARRIER") {
            while !std::path::Path::new(&barrier).exists() {
                thread::sleep(Duration::from_millis(10));
            }
        }
        if let Err(e) = storage::open(&sc, &db) {
            warn!(%e, cell = %sc, "storage open");
            startup_timing.emit("error", "storage_open");
            let _ = startup_tx.send(Err(e.context("cell storage open failed")));
            return;
        }
        #[cfg(debug_assertions)]
        if std::env::var("CELLD_TEST_CELL_STARTUP_FAILURE").as_deref() == Ok("1") {
            let error = anyhow::anyhow!("injected cell isolate startup failure");
            warn!(%error, cell = %sc, "cell isolate load failed");
            startup_timing.emit("error", "worker_load");
            storage::close(&sc);
            let _ = startup_tx.send(Err(error));
            return;
        }
        js::touch(&sc);
        let mut w = match js::Worker::load_config(config, std::slice::from_ref(&sc)) {
            Ok(w) => w,
            Err(e) => {
                warn!(%e, cell = %sc, "cell isolate load failed");
                startup_timing.emit("error", "worker_load");
                storage::close(&sc);
                let _ = startup_tx.send(Err(e.context("cell isolate load failed")));
                return;
            }
        };
        match storage::get_actor_name(&sc) {
            Ok(Some(name)) => {
                if let Err(e) = w.set_id_name(&sc, &name) {
                    warn!(%e, cell = %sc, "restore actor name");
                    startup_timing.emit("error", "actor_name_restore");
                    storage::close(&sc);
                    let _ = startup_tx.send(Err(e.context("restore actor name")));
                    return;
                }
            }
            Ok(None) => {}
            Err(e) => {
                warn!(%e, cell = %sc, "read actor name");
                startup_timing.emit("error", "actor_name_read");
                storage::close(&sc);
                let _ = startup_tx.send(Err(e.context("read actor name")));
                return;
            }
        }
        let alarm_mirror = thread_next_alarm_ms.clone();
        storage::watch_alarm(&sc, thread_next_alarm_ms);
        thread_activation_alarm_ms
            .store(alarm_mirror.load(Ordering::Acquire), Ordering::Release);
        startup_timing.emit("ready", "");
        let _ = startup_tx.send(Ok(()));
        loop {
            match rx.recv_timeout(Duration::from_millis(500)) {
                Ok(CellJob::Shutdown) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                Ok(job) => {
                    let Some(_activity) = thread_activity.try_acquire() else {
                        break;
                    };
                    match job {
                        CellJob::Fetch {
                            request_id,
                            scope,
                            name,
                            url,
                            method,
                            body,
                            headers,
                            reply,
                        } => {
                            if let Some(name) = name {
                                if let Err(error) = w.set_id_name(&scope, &name) {
                                    let _ = reply.send(Err(error));
                                    continue;
                                }
                            }
                            w.dispatch_to_and_reply(
                                &scope,
                                js::FetchRequest {
                                    url: &url,
                                    method: &method,
                                    body: &body,
                                    headers: &headers,
                                    request_id,
                                },
                                &rx,
                                reply,
                            );
                        }
                        CellJob::WorkerFetch {
                            request_id,
                            url,
                            method,
                            body,
                            headers,
                            inline_activity: _inline_activity,
                            fallback_workers: _fallback_workers,
                            reply,
                        } => {
                            if let Err(error) = w.worker_fetch_and_reply(
                                js::FetchRequest {
                                    url: &url,
                                    method: &method,
                                    body: &body,
                                    headers: &headers,
                                    request_id: Some(request_id),
                                },
                                &rx,
                                reply,
                            ) {
                                warn!(%error, cell = %sc, "cell worker isolate reload failed");
                                break;
                            }
                        }
                        CellJob::AbortFetch { .. } => {}
                        CellJob::Rpc {
                            scope,
                            name,
                            method,
                            args,
                            reply,
                        } => {
                            let result = name
                                .as_deref()
                                .map_or(Ok(()), |name| w.set_id_name(&scope, name))
                                .and_then(|()| w.dispatch_rpc_data(&scope, &method, args, &rx));
                            let _ = reply.send(result);
                        }
                        CellJob::WsOpen {
                            scope,
                            ws_id,
                            protocol,
                            reply,
                        } => {
                            let _ = reply.send(w.dispatch_ws_open(
                                &scope,
                                ws_id,
                                &protocol,
                                &rx,
                            ));
                        }
                        CellJob::WsMessage {
                            scope,
                            ws_id,
                            data,
                            reply,
                        } => {
                            let _ = reply.send(w.dispatch_ws(&scope, ws_id, data, &rx));
                        }
                        CellJob::WsClosed {
                            scope,
                            ws_id,
                            code,
                            reason,
                            was_clean,
                            reply,
                        } => {
                            let _ = reply.send(w.dispatch_ws_closed(
                                &scope,
                                ws_id,
                                code,
                                &reason,
                                was_clean,
                                &rx,
                            ));
                        }
                        CellJob::Shutdown => unreachable!("shutdown handled before activity pin"),
                    }
                    // Steady traffic keeps recv_timeout from ever timing out;
                    // the atomic mirror makes this per-job check free.
                    let due = alarm_mirror.load(Ordering::Acquire);
                    if due >= 0 && due <= now_ms() {
                        fire_due_alarm(&sc, &mut w, &rx);
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    let Some(_activity) = thread_activity.try_acquire() else {
                        break;
                    };
                    fire_due_alarm(&sc, &mut w, &rx);
                }
            }
        }
        storage::unwatch_alarm(&sc);
        storage::close(&sc);
    });
    (
        Cell {
            tx,
            activity,
            next_alarm_ms,
            activation_alarm_ms,
        },
        startup_rx,
    )
}

async fn resolve_owner_cached(
    cx: &Ctx,
    scope: &str,
) -> anyhow::Result<Option<ownership::ResolvedOwner>> {
    Ok(match resolve_owner_cached_lookup(cx, scope).await?.lookup {
        OwnerLookup::Live(owner) => Some(owner),
        OwnerLookup::Missing | OwnerLookup::Unavailable => None,
    })
}

enum OwnerLookup {
    Live(ownership::ResolvedOwner),
    /// The authoritative ownership GET returned 404. A conditional epoch-one
    /// create can use that observation without repeating the missing GET.
    Missing,
    /// An ownership record exists but does not currently resolve to a live
    /// node. Acquisition must reread it for its ETag and fencing epoch.
    Unavailable,
}

struct OwnerLookupResult {
    lookup: OwnerLookup,
    owner_cache_hit: bool,
    node_cache_hit: bool,
    node_lease_consulted: bool,
    ownership_read_us: u64,
    node_lease_lookup_us: u64,
}

const NODE_UNAVAILABLE_CACHE_MS: u64 = 250;

struct CellRouteTiming {
    started: Instant,
    latch_wait_us: u64,
    ownership_read_us: u64,
    node_lease_lookup_us: u64,
    capacity_lookup_us: u64,
    capacity_wait_us: u64,
    activation_slot_wait_us: u64,
    lease_permit_us: u64,
    ownership_acquire_us: u64,
    replica_discovery_us: u64,
    restore_us: u64,
    isolate_startup_us: u64,
    registry_insert_us: u64,
    owner_cache_hit: bool,
    node_cache_hit: bool,
    node_lease_consulted: bool,
}

const ALARM_WAKE_ACTIVATION_CONCURRENCY: usize = 16;
/// Maintenance ticks (5 s each) between local-cache prunes. The walk is
/// O(cached cells), so it must not run on every tick.
const CACHE_PRUNE_EVERY_TICKS: u32 = 12;
/// Default ceiling for preserved hibernation replicas. Generous enough that
/// ordinary fleets never evict, small enough that an oversubscribed node
/// cannot fill its disk with copies of cells it no longer serves.
const DEFAULT_LOCAL_CACHE_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const RESIDENT_CAPACITY_WAIT_TIMEOUT: Duration = Duration::from_secs(20);
/// A cell's isolate startup (open storage, load the module, restore the actor
/// name) must complete within this bound. An unbounded wait let a startup that
/// blocked — e.g. on a DB read a live-sync replicator was holding — wedge
/// activation, and the request behind it, forever; fail closed instead so the
/// cell fails the activation and can be retried rather than hanging.
const ISOLATE_STARTUP_TIMEOUT: Duration = Duration::from_secs(120);

/// Run one tier-2 alarm wake batch concurrently. Restoring a hibernated cell
/// is dominated by object-store round trips and the Litestream restore
/// subprocess; processing the owner's due heap serially makes alarm lateness
/// grow by one full restore per cell. The same activations already run
/// concurrently when they are request-driven, so bound the scheduler path to
/// make progress without an unbounded restore burst.
async fn run_alarm_wake_batch<T, R, F, Fut>(items: Vec<T>, activate: F) -> Vec<R>
where
    F: FnMut(T) -> Fut,
    Fut: std::future::Future<Output = R>,
{
    use futures_util::stream::{self, StreamExt};

    stream::iter(items)
        .map(activate)
        .buffer_unordered(ALARM_WAKE_ACTIVATION_CONCURRENCY)
        .collect()
        .await
}

async fn check_hibernation_replicas(
    cx: &Arc<Ctx>,
    candidates: Vec<(String, u64)>,
    concurrency: usize,
) -> Vec<(String, u64, bool)> {
    use futures_util::stream::{self, StreamExt};

    stream::iter(candidates)
        .map(|(scope, epoch)| {
            let cx = cx.clone();
            async move {
                let replicated = replication::epoch_replicated(
                    &cx.c,
                    &cx.bucket,
                    &scope,
                    epoch,
                ).await;
                (scope, epoch, replicated)
            }
        })
        .buffered(concurrency)
        .collect()
        .await
}

impl CellRouteTiming {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            latch_wait_us: 0,
            ownership_read_us: 0,
            node_lease_lookup_us: 0,
            capacity_lookup_us: 0,
            capacity_wait_us: 0,
            activation_slot_wait_us: 0,
            lease_permit_us: 0,
            ownership_acquire_us: 0,
            replica_discovery_us: 0,
            restore_us: 0,
            isolate_startup_us: 0,
            registry_insert_us: 0,
            owner_cache_hit: false,
            node_cache_hit: false,
            node_lease_consulted: false,
        }
    }

    fn emit(
        &self,
        cx: &Ctx,
        scope: &str,
        outcome: &str,
        failure_phase: &str,
        owner: Option<(&str, u64)>,
        fresh: bool,
    ) {
        let (owner_node, epoch) = owner.unwrap_or(("", 0));
        info!(
            event = "cell_route_timing",
            outcome,
            failure_phase,
            scope,
            node = %cx.node,
            region = %cx.region,
            runtime_version = env!("CARGO_PKG_VERSION"),
            owner_node,
            epoch,
            fresh,
            owner_cache_hit = self.owner_cache_hit,
            node_cache_hit = self.node_cache_hit,
            node_lease_consulted = self.node_lease_consulted,
            total_us = self.started.elapsed().as_micros() as u64,
            latch_wait_us = self.latch_wait_us,
            ownership_read_us = self.ownership_read_us,
            node_lease_lookup_us = self.node_lease_lookup_us,
            capacity_lookup_us = self.capacity_lookup_us,
            capacity_wait_us = self.capacity_wait_us,
            activation_slot_wait_us = self.activation_slot_wait_us,
            lease_permit_us = self.lease_permit_us,
            ownership_acquire_us = self.ownership_acquire_us,
            replica_discovery_us = self.replica_discovery_us,
            restore_us = self.restore_us,
            isolate_startup_us = self.isolate_startup_us,
            registry_insert_us = self.registry_insert_us,
            "cell route resolved"
        );
    }
}

async fn resolve_owner_cached_lookup(
    cx: &Ctx,
    scope: &str,
) -> anyhow::Result<OwnerLookupResult> {
    let cached = {
        let cache = cx.owner_cache.lock().unwrap();
        if let Some(owner) = cache.get(scope).cloned() {
            if owner.expires_ms > now_ms().max(0) as u64 {
                return Ok(OwnerLookupResult {
                    lookup: OwnerLookup::Live(owner),
                    owner_cache_hit: true,
                    node_cache_hit: false,
                    node_lease_consulted: false,
                    ownership_read_us: 0,
                    node_lease_lookup_us: 0,
                });
            }
            Some(owner)
        } else {
            None
        }
    };
    let mut ownership_read_us = 0;
    let (node, epoch) = match cached {
        Some(owner) => (owner.node, owner.epoch),
        None => {
            let started = Instant::now();
            let owner = ownership::read_owner(&cx.c, &cx.bucket, scope).await?;
            ownership_read_us = started.elapsed().as_micros() as u64;
            let Some(owner) = owner else {
                return Ok(OwnerLookupResult {
                    lookup: OwnerLookup::Missing,
                    owner_cache_hit: false,
                    node_cache_hit: false,
                    node_lease_consulted: false,
                    ownership_read_us,
                    node_lease_lookup_us: 0,
                });
            };
            if owner.node.is_empty() {
                return Ok(OwnerLookupResult {
                    lookup: OwnerLookup::Unavailable,
                    owner_cache_hit: false,
                    node_cache_hit: false,
                    node_lease_consulted: false,
                    ownership_read_us,
                    node_lease_lookup_us: 0,
                });
            }
            (owner.node, owner.epoch)
        }
    };
    let node_started = Instant::now();
    let (record, node_cache_hit) = resolve_node_cached(cx, &node).await?;
    let node_lease_lookup_us = node_started.elapsed().as_micros() as u64;
    let Some(record) = record else {
        return Ok(OwnerLookupResult {
            lookup: OwnerLookup::Unavailable,
            owner_cache_hit: false,
            node_cache_hit,
            node_lease_consulted: true,
            ownership_read_us,
            node_lease_lookup_us,
        });
    };
    let owner = ownership::ResolvedOwner {
        node: record.node,
        addr: record.addr,
        expires_ms: record.expires_ms,
        epoch,
        peer_protocol: record.peer_protocol,
    };
    cx.owner_cache
        .lock()
        .unwrap()
        .insert(scope.into(), owner.clone());
    Ok(OwnerLookupResult {
        lookup: OwnerLookup::Live(owner),
        owner_cache_hit: false,
        node_cache_hit,
        node_lease_consulted: true,
        ownership_read_us,
        node_lease_lookup_us,
    })
}

async fn resolve_node_cached(
    cx: &Ctx,
    node: &str,
) -> anyhow::Result<(Option<ownership::NodeRec>, bool)> {
    {
        let cache = cx.node_cache.lock().unwrap();
        if let Some(record) = cache.get(node).cloned() {
            if record.expires_ms > now_ms().max(0) as u64 {
                return Ok((Some(record), true));
            }
        }
    }
    {
        let mut unavailable = cx.node_unavailable_cache.lock().unwrap();
        if unavailable
            .get(node)
            .is_some_and(|expires| *expires > Instant::now())
        {
            return Ok((None, true));
        }
        unavailable.remove(node);
    }
    let latch = cx
        .node_resolving
        .lock()
        .unwrap()
        .entry(node.to_string())
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone();
    let _guard = latch.lock_owned().await;
    {
        let cache = cx.node_cache.lock().unwrap();
        if let Some(record) = cache.get(node).cloned() {
            if record.expires_ms > now_ms().max(0) as u64 {
                return Ok((Some(record), true));
            }
        }
    }
    {
        let mut unavailable = cx.node_unavailable_cache.lock().unwrap();
        if unavailable
            .get(node)
            .is_some_and(|expires| *expires > Instant::now())
        {
            return Ok((None, true));
        }
        unavailable.remove(node);
    }
    let record = ownership::resolve_node(&cx.c, &cx.bucket, node).await?;
    let mut cache = cx.node_cache.lock().unwrap();
    match &record {
        Some(record) => {
            cache.insert(node.to_string(), record.clone());
            cx.node_unavailable_cache.lock().unwrap().remove(node);
        }
        None => {
            cache.remove(node);
            cx.node_unavailable_cache.lock().unwrap().insert(
                node.to_string(),
                Instant::now() + Duration::from_millis(NODE_UNAVAILABLE_CACHE_MS),
            );
        }
    }
    Ok((record, false))
}

fn remote_route(owner: ownership::ResolvedOwner) -> anyhow::Result<CellRoute> {
    if owner.expires_ms <= now_ms().max(0) as u64 {
        anyhow::bail!("node {} has an expired peer lease", owner.node);
    }
    if owner.peer_protocol != peer_auth::PROTOCOL_VERSION {
        anyhow::bail!(
            "node {} speaks incompatible peer protocol {} (this node supports {})",
            owner.node,
            owner.peer_protocol,
            peer_auth::PROTOCOL_VERSION,
        );
    }
    debug!(
        owner = %owner.node,
        epoch = owner.epoch,
        protocol = owner.peer_protocol,
        "resolved remote cell owner"
    );
    Ok(CellRoute::Remote(owner))
}

/// RAII tokens held for a cold activation's duration: the per-cell
/// serialization guard and the global activation-concurrency slot. Both exist
/// only to be dropped when the activation finishes.
struct ActivationHold {
    // Preserve the original parameter drop order: release the fleet-wide
    // concurrency slot before the per-cell serialization latch.
    _slot: tokio::sync::OwnedSemaphorePermit,
    _guard: tokio::sync::OwnedMutexGuard<()>,
    _resident_reservation: Option<ResidentReservation>,
}

struct ResidentReservation {
    pending: Arc<AtomicUsize>,
    changed: Arc<tokio::sync::Notify>,
}

impl Drop for ResidentReservation {
    fn drop(&mut self) {
        self.pending.fetch_sub(1, Ordering::Relaxed);
        self.changed.notify_waiters();
    }
}

fn reserve_resident(cx: &Ctx) -> Option<ResidentReservation> {
    // Sampled pressure is RSS/CPU only. Residency is counted exactly here, so
    // admitting against a five-second-old view of it would refuse cells this
    // node has already made room for.
    let pressured = cx.admission_pressure.load(Ordering::Acquire);
    loop {
        let pending = cx.resident_reservations.load(Ordering::Acquire);
        if !celld_logic::pressure::may_admit(
            cx.owned_cells.load(Ordering::Acquire),
            pending,
            cx.pressure_config.resident_high,
            pressured,
        ) {
            return None;
        }
        if cx.resident_reservations.compare_exchange_weak(
            pending,
            pending.saturating_add(1),
            Ordering::AcqRel,
            Ordering::Acquire,
        ).is_ok() {
            return Some(ResidentReservation {
                pending: cx.resident_reservations.clone(),
                changed: cx.resident_capacity_changed.clone(),
            });
        }
    }
}

/// What the ownership acquisition established about this activation. These
/// three always travel together: the fenced epoch, whether the record was
/// created by us, and whether we seized the cell from another node.
struct ActivationFacts {
    epoch: u64,
    fresh: bool,
    took_over: bool,
}

async fn finish_activation(
    cx: Arc<Ctx>,
    scope: String,
    facts: ActivationFacts,
    lease_permit: ownership::ActivationPermit,
    _hold: ActivationHold,
    mut timing: CellRouteTiming,
) -> anyhow::Result<CellRoute> {
    use celld_logic::lifecycle::Effect;
    use celld_logic::lifecycle::Event;
    let ActivationFacts { epoch, fresh, took_over } = facts;
    // Drive the sans-IO lifecycle through restore→resident→serving alongside the
    // I/O below, so production runs the transitions the simulator tests. The
    // owned epoch is already held (acquisition, or a sticky restore); the etag
    // is unused until a later slice wires fencing off this Cell. Alarm truth is
    // deferred to the alarm-ownership slice (`RestoreOk{alarm: None}`).
    let mut lc = celld_logic::lifecycle::Cell::owned(&cx.node, epoch, String::new());
    let activated = match cx
        .repl
        .activate(replication::ActivationOptions {
            client: &cx.c,
            litestream: &cx.litestream,
            bucket: &cx.bucket,
            cell: &scope,
            epoch,
            fresh,
            took_over,
            endpoint: cx.endpoint.as_deref(),
            region: &cx.region,
            credentials: cx.storage_credentials.as_ref(),
        })
        .await
    {
        Ok(activated) => activated,
        Err(error) => {
            timing.emit(
                &cx,
                &scope,
                "activation_error",
                "replication",
                Some((&cx.node, epoch)),
                fresh,
            );
            return Err(error);
        }
    };
    timing.replica_discovery_us = activated.replica_discovery_us;
    timing.restore_us = activated.restore_us;
    let db = activated.path.to_string_lossy().into_owned();
    // Restore is done; seed the Cell's alarm mirror from the durable truth the
    // restore loaded (read directly — the isolate has not opened `scope` yet),
    // then the machine calls for isolate startup next.
    let alarm = storage::persisted_alarm(&db, &scope).map(|(at_ms, generation, retry, counted_retry)| {
        celld_logic::lifecycle::AlarmLocal {
            gen: generation as u64,
            due_wall_ms: at_ms.max(0) as u64,
            retry,
            counted_retry,
        }
    });
    assert_eq!(
        lc.on_event(0, 0, Event::RestoreOk { alarm }),
        vec![Effect::StartRuntime { epoch }],
        "activation lifecycle diverged from the sans-IO machine at restore",
    );
    // A failed isolate must never become a resident route. In particular, a
    // deployment that removes a Durable Object class can encounter older
    // cells during alarm/dead-node reconciliation; registering before
    // actor-name restoration completes leaves a dead sender that poisons
    // unrelated top-level Worker requests and consumes resident capacity
    // indefinitely.
    let isolate_started = Instant::now();
    let (cell, startup) = spawn_cell(&scope, &db, &cx, epoch, fresh);
    let startup = match tokio::time::timeout(ISOLATE_STARTUP_TIMEOUT, startup).await {
        Ok(received) => received
            .map_err(|error| anyhow::anyhow!("cell isolate startup channel closed: {error}"))
            .and_then(|result| result),
        Err(_) => Err(anyhow::anyhow!(
            "cell isolate startup timed out after {ISOLATE_STARTUP_TIMEOUT:?}"
        )),
    };
    timing.isolate_startup_us = isolate_started.elapsed().as_micros() as u64;
    if let Err(error) = startup {
        timing.emit(
            &cx,
            &scope,
            "activation_error",
            "isolate_startup",
            Some((&cx.node, epoch)),
            fresh,
        );
        return Err(error);
    }
    // The isolate is up; the machine calls for publication (the commit point).
    assert_eq!(
        lc.on_event(0, 0, Event::RuntimeReady),
        vec![Effect::PublishResident { epoch }],
        "activation lifecycle diverged from the sans-IO machine at runtime startup",
    );
    let registry_started = Instant::now();
    let mut reg = cx.registry.lock().unwrap();
    if let Some(existing) = reg.get(&scope) {
        let route = existing
            .route()
            .context("resident cell began closing during serialized activation")?;
        let _ = cell.tx.send(CellJob::Shutdown);
        timing.registry_insert_us = registry_started.elapsed().as_micros() as u64;
        timing.emit(
            &cx,
            &scope,
            "resident_race",
            "",
            Some((&cx.node, epoch)),
            fresh,
        );
        return Ok(route);
    }
    let route = cell
        .route()
        .context("new cell began closing before registry insertion")?;
    reg.insert(scope.clone(), cell);
    timing.registry_insert_us = registry_started.elapsed().as_micros() as u64;
    drop(reg);
    // Published: the registry insert is the commit point and fuses serve with
    // publish. `DeleteWakeEntries` stays deferred to the sweep this slice.
    assert_eq!(
        lc.on_event(0, 0, Event::PublishOk),
        vec![Effect::StartServing { epoch }, Effect::DeleteWakeEntries],
        "activation lifecycle diverged from the sans-IO machine at publication",
    );
    cx.owed_activations.lock().unwrap().remove(&scope);
    cx.owned_cell_inventory
        .lock()
        .unwrap()
        .insert(scope.clone(), epoch);
    lease_permit.commit();
    cx.owned_cells.fetch_add(1, Ordering::Relaxed);
    // The cell is the per-scope truth now: Owned, resident, serving.
    debug_assert!(lc.ready());
    cx.lifecycle_cells.lock().unwrap().insert(scope.clone(), lc);
    timing.emit(
        &cx,
        &scope,
        "activated",
        "",
        Some((&cx.node, epoch)),
        fresh,
    );
    Ok(route)
}

/// Ensure `scope` runs somewhere: resident here → its cell tx; owned by a live
/// peer → that address; otherwise acquire/wake (fresh epoch) + restore + spawn
/// its isolate locally. Activation runs OUTSIDE the registry lock.
async fn ensure_cell(cx: &Arc<Ctx>, scope: &str) -> anyhow::Result<CellRoute> {
    ensure_cell_with_admission(cx, scope, false, false).await
}

async fn ensure_cell_for_request(cx: &Arc<Ctx>, scope: &str) -> anyhow::Result<CellRoute> {
    ensure_cell_with_admission(cx, scope, false, true).await
}

async fn ensure_cell_with_admission(
    cx: &Arc<Ctx>,
    scope: &str,
    force_admit: bool,
    wait_for_capacity: bool,
) -> anyhow::Result<CellRoute> {
    if let Some(route) = cx
        .registry
        .lock()
        .unwrap()
        .get(scope)
        .and_then(Cell::route)
    {
        return Ok(route);
    }
    let mut timing = CellRouteTiming::new();
    // serialize activation of THIS scope (no thundering herd of self-proxies)
    let latch = cx.activating.lock().unwrap()
        .entry(scope.to_string())
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))).clone();
    let latch_started = Instant::now();
    let activation_guard = latch.lock_owned().await;
    timing.latch_wait_us = latch_started.elapsed().as_micros() as u64;
    // recheck: a prior holder of the latch may have activated it already
    if let Some(route) = cx
        .registry
        .lock()
        .unwrap()
        .get(scope)
        .and_then(Cell::route)
    {
        timing.emit(
            cx,
            scope,
            "resident_after_wait",
            "",
            None,
            false,
        );
        return Ok(route);
    }
    // Bound the complete nonresident route, not just Litestream restore. A
    // cold request may perform ownership, node-lease, capacity, and replica
    // bucket I/O before it knows whether this process will activate or proxy
    // the cell. Acquiring later would still allow a large Worker pool to
    // saturate the authority store ahead of the node heartbeat.
    let activation_slot_started = Instant::now();
    let activation_slot = cx
        .activation_slots
        .clone()
        .acquire_owned()
        .await
        .expect("activation semaphore closed");
    timing.activation_slot_wait_us = activation_slot_started.elapsed().as_micros() as u64;
    // A pressure-sticky hibernation is a residency-cache miss, not a new
    // ownership event. This process still holds the continuously renewed node
    // lease and the exact fenced epoch, so restore locally without asking the
    // bucket to release and reacquire the same owner.
    let retained_epoch = cx.hibernated_owned.lock().unwrap().remove(scope);
    if let Some(epoch) = retained_epoch {
        let capacity_wait_started = Instant::now();
        let resident_reservation = loop {
            let changed = cx.resident_capacity_changed.notified();
            if let Some(reservation) = reserve_resident(cx) {
                break reservation;
            }
            // Demand has consumed the high/low gap before the five-second
            // sampler observed it. Keep at most the activation budget queued
            // here and ask the maintenance loop to free another cache batch;
            // leaking this transient state as HTTP 500 makes a healthy cache
            // look unavailable under churn.
            cx.shedding.store(true, Ordering::Release);
            let elapsed = capacity_wait_started.elapsed();
            if elapsed >= RESIDENT_CAPACITY_WAIT_TIMEOUT {
                cx.hibernated_owned
                    .lock()
                    .unwrap()
                    .insert(scope.to_string(), epoch);
                timing.capacity_wait_us = elapsed.as_micros() as u64;
                timing.emit(
                    cx,
                    scope,
                    "capacity_exhausted",
                    "capacity_wait",
                    Some((&cx.node, epoch)),
                    false,
                );
                anyhow::bail!(
                    "timed out waiting for resident capacity to restore a sticky cell"
                );
            }
            let remaining = RESIDENT_CAPACITY_WAIT_TIMEOUT.saturating_sub(elapsed);
            let _ = tokio::time::timeout(
                remaining.min(Duration::from_secs(5)),
                changed,
            ).await;
        };
        timing.capacity_wait_us = capacity_wait_started.elapsed().as_micros() as u64;
        let lease_started = Instant::now();
        let lease_permit = match cx.lease_manager.begin_activation().await {
            Ok(permit) => permit,
            Err(error) => {
                cx.hibernated_owned
                    .lock()
                    .unwrap()
                    .insert(scope.to_string(), epoch);
                timing.lease_permit_us = lease_started.elapsed().as_micros() as u64;
                timing.emit(
                    cx,
                    scope,
                    "activation_error",
                    "lease_permit",
                    Some((&cx.node, epoch)),
                    false,
                );
                return Err(error);
            }
        };
        timing.lease_permit_us = lease_started.elapsed().as_micros() as u64;
        cx.owed_activations
            .lock()
            .unwrap()
            .insert(scope.to_string());
        let activation = tokio::spawn(finish_activation(
            cx.clone(),
            scope.to_string(),
            // Sticky retention: this node held its lease and its exact fenced
            // epoch throughout, so it never seized the cell from anyone.
            ActivationFacts { epoch, fresh: false, took_over: false },
            lease_permit,
            ActivationHold {
                _guard: activation_guard,
                _slot: activation_slot,
                _resident_reservation: Some(resident_reservation),
            },
            timing,
        ));
        let result = activation
            .await
            .map_err(|error| anyhow::anyhow!("cell activation task failed: {error}"))?;
        if result.is_err() {
            cx.hibernated_owned
                .lock()
                .unwrap()
                .insert(scope.to_string(), epoch);
        }
        return result;
    }
    let owner_lookup = match resolve_owner_cached_lookup(cx, scope).await {
        Ok(owner) => owner,
        Err(error) => {
            timing.emit(
                cx,
                scope,
                "route_error",
                "ownership_lookup",
                None,
                false,
            );
            return Err(error);
        }
    };
    timing.owner_cache_hit = owner_lookup.owner_cache_hit;
    timing.node_cache_hit = owner_lookup.node_cache_hit;
    timing.node_lease_consulted = owner_lookup.node_lease_consulted;
    timing.ownership_read_us = owner_lookup.ownership_read_us;
    timing.node_lease_lookup_us = owner_lookup.node_lease_lookup_us;
    if let OwnerLookup::Live(owner) = &owner_lookup.lookup {
        if owner.node != cx.node {
            let route = match remote_route(owner.clone()) {
                Ok(route) => route,
                Err(error) => {
                    timing.emit(
                        cx,
                        scope,
                        "route_error",
                        "remote_route",
                        Some((&owner.node, owner.epoch)),
                        false,
                    );
                    return Err(error);
                }
            };
            if !timing.owner_cache_hit {
                timing.emit(
                    cx,
                    scope,
                    "remote_owner",
                    "",
                    Some((&owner.node, owner.epoch)),
                    false,
                );
            }
            return Ok(route);
        }
    }
    let owned_locally = matches!(
        &owner_lookup.lookup,
        OwnerLookup::Live(owner) if owner.node == cx.node
    );
    let local_resident_cells = cx.owned_cells.load(Ordering::Acquire);
    let pending_residents = cx.resident_reservations.load(Ordering::Acquire);
    // Reserve before choosing local admission. Without this atomic claim,
    // concurrent cold requests can all observe the same spare slot and race
    // through activation. A capacity-selected peer passes `force_admit` to
    // suppress another forwarding hop, but it still needs a real slot; if its
    // advertised capacity went stale, the caller retries another peer.
    let mut resident_reservation = reserve_resident(cx);
    let hard_capacity = resident_reservation.is_none();
    let balance_new_cell = cx.pressure_config.resident_low > 0
        && local_resident_cells.saturating_add(pending_residents)
            >= cx.pressure_config.resident_low;
    // A real local owner cannot balance its nonresident cell to a capacity
    // peer without first relinquishing the fenced ownership record. This state
    // occurs after a post-CAS activation failure and while retrying activation
    // debt. Keep it local and let a foreground request wait for shedding;
    // epoch-zero capacity candidates remain free to traverse other peers.
    if !force_admit && !owned_locally && (hard_capacity || balance_new_cell) {
        let below_resident_cells = (!hard_capacity).then_some(local_resident_cells);
        let capacity_started = Instant::now();
        let peer = match ownership::capacity_peer(
            &mut *cx.capacity_peers.lock().await,
            &cx.c,
            &cx.bucket,
            &cx.node,
            below_resident_cells,
            cx.node_ttl_ms,
        )
        .await
        {
            Ok(peer) => peer,
            Err(error) => {
                timing.capacity_lookup_us = capacity_started.elapsed().as_micros() as u64;
                timing.emit(
                    cx,
                    scope,
                    "route_error",
                    "capacity_lookup",
                    None,
                    false,
                );
                return Err(error);
            }
        };
        if let Some(peer) = peer {
            timing.capacity_lookup_us = capacity_started.elapsed().as_micros() as u64;
            let route = match remote_route(peer.clone()) {
                Ok(route) => route,
                Err(error) => {
                    timing.emit(
                        cx,
                        scope,
                        "route_error",
                        "remote_route",
                        Some((&peer.node, peer.epoch)),
                        false,
                    );
                    return Err(error);
                }
            };
            timing.emit(
                cx,
                scope,
                "capacity_peer",
                "",
                Some((&peer.node, peer.epoch)),
                false,
            );
            return Ok(route);
        }
        timing.capacity_lookup_us = capacity_started.elapsed().as_micros() as u64;
    }
    if resident_reservation.is_none() && wait_for_capacity {
        let capacity_wait_started = Instant::now();
        loop {
            let changed = cx.resident_capacity_changed.notified();
            if let Some(reservation) = reserve_resident(cx) {
                resident_reservation = Some(reservation);
                break;
            }
            // Every advertised peer may transiently be at its high watermark
            // while idle cells are already eligible for the asynchronous
            // pressure sweep. Keep this request inside the activation budget,
            // ask the local maintenance loop to make room, and wait for its
            // completion notification instead of leaking cache churn as a
            // Worker-visible 500. A forced peer admission still rejects
            // immediately so the ingress can traverse another candidate.
            cx.shedding.store(true, Ordering::Release);
            let elapsed = capacity_wait_started.elapsed();
            if elapsed >= RESIDENT_CAPACITY_WAIT_TIMEOUT {
                timing.capacity_wait_us = elapsed.as_micros() as u64;
                timing.emit(
                    cx,
                    scope,
                    "capacity_exhausted",
                    "capacity_wait",
                    None,
                    false,
                );
                return Err(anyhow::Error::new(CapacityExhausted {
                    scope: scope.to_string(),
                }));
            }
            let remaining = RESIDENT_CAPACITY_WAIT_TIMEOUT.saturating_sub(elapsed);
            let _ = tokio::time::timeout(remaining.min(Duration::from_secs(5)), changed).await;
        }
        timing.capacity_wait_us = capacity_wait_started.elapsed().as_micros() as u64;
    }
    if resident_reservation.is_none() {
        timing.emit(cx, scope, "capacity_exhausted", "", None, false);
        return Err(anyhow::Error::new(CapacityExhausted {
            scope: scope.to_string(),
        }));
    }
    let resident_reservation = resident_reservation
        .expect("a locally admitted activation holds a resident reservation");
    // Ours (or unowned): every activation, including a same-node process
    // restart, atomically claims a fresh epoch before creating its db file.
    let lease_started = Instant::now();
    let lease_permit = match cx.lease_manager.begin_activation().await {
        Ok(permit) => permit,
        Err(error) => {
            timing.lease_permit_us = lease_started.elapsed().as_micros() as u64;
            timing.emit(
                cx,
                scope,
                "activation_error",
                "lease_permit",
                None,
                false,
            );
            return Err(error);
        }
    };
    timing.lease_permit_us = lease_started.elapsed().as_micros() as u64;
    let fresh = matches!(owner_lookup.lookup, OwnerLookup::Missing);
    let acquire_started = Instant::now();
    // One executor drives the sans-IO acquisition machine for both create and
    // takeover — production runs the same logic the simulator tests.
    let acquired_result = ownership::acquire_via_core(
        &cx.c,
        &cx.bucket,
        scope,
        &cx.node,
        fresh,
    ).await;
    let acquired = match acquired_result {
        Ok(acquired) => acquired,
        Err(error) => {
            timing.ownership_acquire_us = acquire_started.elapsed().as_micros() as u64;
            timing.emit(
                cx,
                scope,
                "activation_error",
                "ownership_acquire",
                None,
                fresh,
            );
            return Err(error);
        }
    };
    timing.ownership_acquire_us = acquire_started.elapsed().as_micros() as u64;
    let (epoch, took_over) = match acquired {
        Some(ownership::Acquired { epoch, took_over }) => (epoch, took_over),
        None => { // lost the create race; whoever won owns it (maybe us)
            cx.owner_cache.lock().unwrap().remove(scope);
            let owner = resolve_owner_cached(cx, scope)
                .await?
                .context("ownership race winner has no live route")?;
            if owner.node == cx.node {
                anyhow::bail!("ownership points to this node before the cell is resident");
            }
            let route = remote_route(owner.clone())?;
            timing.emit(
                cx,
                scope,
                "race_winner",
                "",
                Some((&owner.node, owner.epoch)),
                fresh,
            );
            return Ok(route);
        }
    };
    cx.epochs.lock().unwrap().insert(scope.to_string(), epoch);
    // From here the bucket records us as owner. Any failure below leaves the
    // cell owned but not running, so record the debt before we can fail.
    cx.owed_activations.lock().unwrap().insert(scope.to_string());
    let activation = tokio::spawn(finish_activation(
        cx.clone(),
        scope.to_string(),
        ActivationFacts { epoch, fresh, took_over },
        lease_permit,
        ActivationHold {
            _guard: activation_guard,
            _slot: activation_slot,
            _resident_reservation: Some(resident_reservation),
        },
        timing,
    ));
    activation
        .await
        .map_err(|error| anyhow::anyhow!("cell activation task failed: {error}"))?
}

/// Route one DO call: run it in the local cell isolate, or proxy to the owner.
#[derive(Clone, Copy)]
struct DoRequest<'a> {
    scope: &'a str,
    name: Option<&'a str>,
    url: &'a str,
    method: &'a str,
    body: &'a [u8],
    headers: &'a [(String, String)],
    request_id: Option<js::RequestId>,
}

#[derive(Clone, Copy)]
struct PeerClient<'a> {
    http: &'a reqwest::Client,
    auth: &'a peer_auth::PeerAuth,
    storage: &'a aws_sdk_s3::Client,
    bucket: &'a str,
}

struct WebsocketRouteTiming {
    started: Instant,
    route_resolution_us: u64,
    dispatch_us: u64,
    attempts: u8,
}

impl WebsocketRouteTiming {
    fn emit(
        &self,
        cx: &Ctx,
        request: DoRequest<'_>,
        outcome: &str,
        route: &str,
        peer_node: &str,
    ) {
        let request_id = request
            .request_id
            .map(js::request_id_string)
            .unwrap_or_default();
        info!(
            event = "websocket_route_timing",
            outcome,
            route,
            peer_node,
            scope = request.scope,
            request_id,
            node = %cx.node,
            region = %cx.region,
            runtime_version = env!("CARGO_PKG_VERSION"),
            attempts = self.attempts,
            total_us = self.started.elapsed().as_micros() as u64,
            route_resolution_us = self.route_resolution_us,
            dispatch_us = self.dispatch_us,
            "WebSocket cell request resolved"
        );
    }
}

async fn run_do_cancellable(
    cx: &Arc<Ctx>,
    request: DoRequest<'_>,
    mut cancel: Option<tokio::sync::oneshot::Receiver<()>>,
) -> anyhow::Result<js::HttpResponse> {
    let mut websocket_timing = request.headers.iter().any(|(name, value)| {
        name.eq_ignore_ascii_case("upgrade") && value.eq_ignore_ascii_case("websocket")
    }).then(|| WebsocketRouteTiming {
        started: Instant::now(),
        route_resolution_us: 0,
        dispatch_us: 0,
        attempts: 0,
    });
    let mut dispatcher = routing::Dispatcher::default();
    let mut rejected_capacity_peers = HashSet::new();
    loop {
        if let Some(timing) = websocket_timing.as_mut() {
            timing.attempts = timing.attempts.saturating_add(1);
        }
        let route_started = Instant::now();
        let route = match ensure_cell_for_request(cx, request.scope).await {
            Ok(route) => route,
            Err(error) => {
                if let Some(timing) = websocket_timing.as_mut() {
                    timing.route_resolution_us +=
                        route_started.elapsed().as_micros() as u64;
                    timing.emit(cx, request, "route_error", "", "");
                }
                return Err(error);
            }
        };
        if let Some(timing) = websocket_timing.as_mut() {
            timing.route_resolution_us += route_started.elapsed().as_micros() as u64;
        }
        match route {
            CellRoute::Local { tx, _activity } => {
                let dispatch_started = Instant::now();
                let result = run_do_local(cx, tx, _activity, request, cancel).await;
                if let Some(timing) = websocket_timing.as_mut() {
                    timing.dispatch_us += dispatch_started.elapsed().as_micros() as u64;
                    timing.emit(
                        cx,
                        request,
                        if result.is_ok() { "ok" } else { "error" },
                        "local",
                        &cx.node,
                    );
                }
                return result;
            }
            CellRoute::Remote(owner) => {
                if owner.epoch == 0 && rejected_capacity_peers.contains(&owner.node) {
                    ownership::reject_capacity_peer(
                        &mut *cx.capacity_peers.lock().await,
                        &owner.node,
                    );
                    continue;
                }
                let dispatch_started = Instant::now();
                let result = match (request.request_id, cancel.as_mut()) {
                    (Some(request_id), Some(cancel)) => {
                        let proxied = proxy(PeerClient {
                            http: &cx.http,
                            auth: &cx.peer_auth,
                            storage: &cx.c,
                            bucket: &cx.bucket,
                        }, &owner, request);
                        tokio::pin!(proxied);
                        tokio::select! {
                            result = &mut proxied => result,
                            cancelled = cancel => {
                                if cancelled.is_ok() {
                                    send_remote_abort(
                                        cx, &owner, request.scope, request_id,
                                    ).await;
                                    Err(anyhow::anyhow!("The client has disconnected"))
                                } else {
                                    proxied.await
                                }
                            }
                        }
                    }
                    _ => proxy(PeerClient {
                        http: &cx.http,
                        auth: &cx.peer_auth,
                        storage: &cx.c,
                        bucket: &cx.bucket,
                    }, &owner, request).await,
                };
                if let Some(timing) = websocket_timing.as_mut() {
                    timing.dispatch_us += dispatch_started.elapsed().as_micros() as u64;
                }
                if result.is_err() {
                    cx.owner_cache.lock().unwrap().remove(request.scope);
                }
                if let Some(attempt) = classify_attempt(result.as_ref().err()) {
                    // An admission rejection is not the owner failing: try the
                    // next peer without spending the stale-route budget.
                    if attempt == routing::Attempt::NotOwner && owner.epoch == 0 {
                        rejected_capacity_peers.insert(owner.node.clone());
                        ownership::reject_capacity_peer(
                            &mut *cx.capacity_peers.lock().await,
                            &owner.node,
                        );
                        continue;
                    }
                    if let routing::Next::Redispatch { forget_node } =
                        dispatcher.on_failure(attempt)
                    {
                        if forget_node {
                            forget_node_route(cx, &owner.node);
                        }
                        continue;
                    }
                }
                let result = result.map_err(|error| {
                    if error.downcast_ref::<StaleRoute>().is_some() {
                        stale_owner_unavailable(request.scope, &owner.addr)
                    } else {
                        error
                    }
                });
                if let Some(timing) = websocket_timing.as_ref() {
                    timing.emit(
                        cx,
                        request,
                        if result.is_ok() { "ok" } else { "error" },
                        "remote",
                        &owner.node,
                    );
                }
                return result;
            }
        }
    }
}

async fn run_do_local(
    cx: &Arc<Ctx>,
    tx: mpsc::Sender<CellJob>,
    _activity: CellActivityGuard,
    request: DoRequest<'_>,
    cancel: Option<tokio::sync::oneshot::Receiver<()>>,
) -> anyhow::Result<js::HttpResponse> {
    let (reply, rx) = tokio::sync::oneshot::channel();
    let send_fetch = || {
        tx.send(CellJob::Fetch {
            request_id: request.request_id,
            scope: request.scope.into(),
            name: request.name.map(str::to_owned),
            url: request.url.into(),
            method: request.method.into(),
            body: request.body.to_vec(),
            headers: request.headers.to_vec(),
            reply,
        })
        .map_err(|_| anyhow::anyhow!("cell channel closed"))
    };
    if let Some(request_id) = request.request_id {
        let mut aborts = cx.remote_aborts.lock().unwrap();
        prune_remote_aborts(&mut aborts);
        if aborts.remove(&request_id).is_some() {
            return Err(anyhow::anyhow!("The client has disconnected"));
        }
        // Keep this lock through send: an abort either records its tombstone
        // first, or observes this Fetch as already queued.
        send_fetch()?;
    } else {
        send_fetch()?;
    }
    let cancel_task = match (request.request_id, cancel) {
        (Some(request_id), Some(cancel)) => {
            let abort_tx = tx.clone();
            Some(tokio::spawn(async move {
                if cancel.await.is_ok() {
                    let _ = abort_tx.send(CellJob::AbortFetch { request_id });
                }
            }))
        }
        _ => None,
    };
    let result = rx.await.map_err(|_| anyhow::anyhow!("cell dropped"));
    if let Some(cancel_task) = cancel_task {
        cancel_task.abort();
    }
    if let Some(request_id) = request.request_id {
        cx.remote_aborts.lock().unwrap().remove(&request_id);
    }
    // Output gate: the response may acknowledge alarms this event armed;
    // hold it until their wake-entry PUTs are durable. Drain even on an
    // error result — arms can commit before a handler throws.
    let gate = js::drain_arm_gates(request.scope).await;
    let response = result??;
    if let Err(error) = gate {
        anyhow::bail!("setAlarm durability gate: {error}");
    }
    Ok(response)
}

async fn send_remote_abort(
    cx: &Ctx,
    owner: &ownership::ResolvedOwner,
    scope: &str,
    request_id: js::RequestId,
) {
    let request_id = js::request_id_string(request_id);
    let path = format!("/__abort/{scope}/{request_id}");
    let Ok(request) = cx.peer_auth.sign(
        cx.http.post(format!("http://{}{}", owner.addr, path)),
        "POST",
        &path,
        &[],
        &owner.node,
    ) else {
        return;
    };
    // Cancellation is best-effort, but a lost abort leaves the owner running
    // work nobody wants while holding that cell's single-threaded isolate.
    // Worth a line in the journal rather than nothing at all.
    if let Err(error) = request.body(Vec::new()).send().await {
        debug!(%scope, owner = %owner.addr, %error, "remote abort not delivered");
    }
}

async fn proxy(
    peer: PeerClient<'_>,
    owner: &ownership::ResolvedOwner,
    request: DoRequest<'_>,
) -> anyhow::Result<js::HttpResponse>
{
    use base64::Engine;
    let request_id = request.request_id.map(js::request_id_string);
    let b = serde_json::json!({
        "name": request.name,
        "url": request.url,
        "method": request.method,
        "bodyBase64": base64::engine::general_purpose::STANDARD.encode(request.body),
        "headers": request.headers,
        "requestId": request_id,
        "admit": owner.epoch == 0,
    }).to_string();
    let path = format!("/__do/{}", request.scope);
    let request_builder = peer.auth.sign(
        peer.http.post(format!("http://{}{}", owner.addr, path)),
        "POST",
        &path,
        b.as_bytes(),
        &owner.node,
    )?;
    let r = request_builder
        .body(b)
        .send()
        .await
        .map_err(|error| owner_unreachable(request.scope, &owner.addr, error))?;
    peer_auth::validate_response(r.headers())?;
    if response_is_stale(r.headers()) {
        return Err(anyhow::Error::new(StaleRoute {
            scope: request.scope.to_string(),
        }));
    }
    let status = r.status().as_u16();
    let streamed = r
        .headers()
        .get("x-celld-body-stream")
        .is_some_and(|value| value == "1");
    let headers = r.headers().iter().map(|(name, value)| (
        name.as_str().to_string(),
        value.to_str().unwrap_or_default().to_string(),
    )).filter(|(name, _)| name != "x-celld-body-stream").collect();
    let mut ws: Option<js::WsTarget> = r.headers().get("x-celld-ws-target")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| serde_json::from_str(value).ok());
    if let Some(target) = ws.as_mut() {
        // The peer may have activated a hibernated cell while serving this
        // request, advancing its epoch beyond the route we used to reach it.
        // New peers stamp that post-activation epoch into the target. Older
        // peers leave it absent, in which case refresh ownership rather than
        // opening the host tunnel with a stale epoch.
        let response_epoch = target.peer_epoch.filter(|epoch| *epoch > 0);
        let websocket_owner = if response_epoch.is_none() {
            ownership::resolve_owner(peer.storage, peer.bucket, request.scope)
                .await?
                .filter(|resolved| resolved.node == owner.node)
                .context("peer returned a WebSocket before ownership became visible")?
        } else {
            owner.clone()
        };
        target.peer_node = Some(websocket_owner.node);
        target.peer_addr = Some(websocket_owner.addr);
        target.peer_epoch = Some(response_epoch.unwrap_or(websocket_owner.epoch));
    }
    let (body, stream) = if streamed {
        (Vec::new(), Some(js::reqwest_response_stream(r)))
    } else {
        // A body that fails mid-read is a truncated response, not an empty
        // one. Defaulting it to `Vec::new()` handed the caller a 200 with no
        // content and no way to tell that apart from a cell that legitimately
        // returned nothing -- silent corruption on the ordinary proxy path.
        // The owner already ran the request, so this is an ambiguous failure:
        // it must surface, and `classify_attempt` must not redispatch it.
        (
            r.bytes()
                .await
                .map_err(|error| {
                    owner_unreachable(request.scope, &owner.addr, error)
                })?
                .to_vec(),
            None,
        )
    };
    // The owner signals WebSocket acceptance as 200 + x-celld-ws-target
    // on the wire: a bare 101 on this non-upgrade POST does not survive
    // standards-compliant intermediaries between peers. Restore the 101
    // the worker expects here.
    let status = if ws.is_some() && status == 200 { 101 } else { status };
    let response = js::HttpResponse {
        status,
        body,
        stream,
        headers,
        ws,
    };
    advisory_activity().record_proxy();
    Ok(response)
}

/// GC summary for one dead process generation's markers.
#[derive(Default)]
struct MarkerGcSummary {
    markers: usize,
    retired: usize,
    failures: usize,
}

const MARKER_GC_CONCURRENCY: usize = 64;

/// Retire the `node-cells/` markers of indexed dead generations. Pure
/// garbage collection: arm-time wake entries make a dead generation's armed
/// alarms discoverable by `due_scan` with no activation, and ownership
/// takeover is lazy on the next request, so the index has no reader beyond
/// this GC. Each marker deletes independently; completed deletions survive
/// a slow tail or elected-waker loss.
async fn gc_indexed_dead_node_markers(
    cx: Arc<Ctx>,
    indexed: HashMap<String, ownership::IndexedOwnership>,
) -> HashMap<String, MarkerGcSummary> {
    use futures_util::stream::{self, StreamExt};

    let mut summaries: HashMap<String, MarkerGcSummary> = HashMap::new();
    let mut work = Vec::new();
    for (node, indexed) in indexed {
        summaries.entry(node.clone()).or_default().markers = indexed.markers.len();
        for marker in indexed.markers {
            work.push((node.clone(), marker));
        }
    }
    let marker_count = work.len();
    let started = Instant::now();
    info!(
        event = "dead_node_marker_gc_started",
        generations = summaries.len(),
        markers = marker_count,
        "dead-node marker GC started"
    );
    let mut results = stream::iter(work)
        .map(|(node, marker)| {
            let cx = cx.clone();
            async move {
                let result = ownership::delete_node_cell_markers(
                    &cx.c,
                    &cx.bucket,
                    vec![marker],
                )
                .await;
                (node, result)
            }
        })
        .buffer_unordered(MARKER_GC_CONCURRENCY);
    while let Some((node, result)) = results.next().await {
        let summary = summaries.get_mut(&node).unwrap();
        match result {
            Ok(()) => summary.retired += 1,
            Err(error) => {
                if summary.failures == 0 {
                    warn!(%node, error = format!("{error:#}"), "marker GC delete failed");
                }
                summary.failures += 1;
            }
        }
    }
    info!(
        event = "dead_node_marker_gc",
        generations = summaries.len(),
        markers = marker_count,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "dead-node marker GC complete"
    );
    summaries
}

async fn run_rpc(cx: &Arc<Ctx>, scope: &str, name: Option<&str>, method: &str,
                 args: js::RpcData)
    -> anyhow::Result<js::RpcData>
{
    use base64::Engine;
    let mut dispatcher = routing::Dispatcher::default();
    let mut rejected_capacity_peers = HashSet::new();
    loop {
        match ensure_cell_for_request(cx, scope).await? {
            CellRoute::Local { tx, _activity } => {
                let (reply, rx) = tokio::sync::oneshot::channel();
                tx.send(CellJob::Rpc {
                    scope: scope.into(),
                    name: name.map(str::to_owned),
                    method: method.into(),
                    args,
                    reply,
                })
                .map_err(|_| anyhow::anyhow!("cell channel closed"))?;
                let result = rx.await.map_err(|_| anyhow::anyhow!("cell dropped"));
                // Output gate: hold the RPC reply until this event's
                // wake-entry PUTs are durable (see run_do_local).
                let gate = js::drain_arm_gates(scope).await;
                let reply = result??;
                if let Err(error) = gate {
                    anyhow::bail!("setAlarm durability gate: {error}");
                }
                return Ok(reply);
            }
            CellRoute::Remote(owner) => {
                if owner.epoch == 0 && rejected_capacity_peers.contains(&owner.node) {
                    ownership::reject_capacity_peer(
                        &mut *cx.capacity_peers.lock().await,
                        &owner.node,
                    );
                    continue;
                }
                // Structured-clone payloads ride the JSON envelope base64'd;
                // the response comes back as raw clone bytes.
                let sc = matches!(args, js::RpcData::V8(_));
                let body = match &args {
                    js::RpcData::Json(json) => serde_json::json!({
                        "name": name,
                        "method": method,
                        "admit": owner.epoch == 0,
                        "args": serde_json::from_str::<serde_json::Value>(json)
                            .unwrap_or_else(|_| serde_json::json!([])),
                    }),
                    js::RpcData::V8(bytes) => serde_json::json!({
                        "name": name,
                        "method": method,
                        "admit": owner.epoch == 0,
                        "sc": base64::engine::general_purpose::STANDARD
                            .encode(bytes),
                    }),
                };
                let body = body.to_string();
                let path = format!("/__rpc/{scope}");
                let request = cx.peer_auth.sign(
                    cx.http.post(format!("http://{}{}", owner.addr, path)),
                    "POST",
                    &path,
                    body.as_bytes(),
                    &owner.node,
                )?;
                let sent = request
                    .header("content-type", "application/json")
                    .body(body)
                    .send()
                    .await;
                let r = match sent {
                    Ok(r) => r,
                    Err(error) => {
                        let error = owner_unreachable(scope, &owner.addr, error);
                        // A connection that was never established carried no
                        // request bytes, so re-sending cannot double-apply.
                        let attempt = classify_attempt(Some(&error))
                            .unwrap_or(routing::Attempt::Ambiguous);
                        let routing::Next::Redispatch { forget_node } =
                            dispatcher.on_failure(attempt)
                        else {
                            return Err(error);
                        };
                        cx.owner_cache.lock().unwrap().remove(scope);
                        if forget_node {
                            forget_node_route(cx, &owner.node);
                        }
                        continue;
                    }
                };
                peer_auth::validate_response(r.headers())?;
                if response_is_stale(r.headers()) {
                    cx.owner_cache.lock().unwrap().remove(scope);
                    if owner.epoch == 0 {
                        rejected_capacity_peers.insert(owner.node.clone());
                        ownership::reject_capacity_peer(
                            &mut *cx.capacity_peers.lock().await,
                            &owner.node,
                        );
                        continue;
                    }
                    if dispatcher.on_failure(routing::Attempt::NotOwner)
                        != routing::Next::Fail
                    {
                        continue;
                    }
                    return Err(stale_owner_unavailable(scope, &owner.addr));
                }
                if !r.status().is_success() {
                    cx.owner_cache.lock().unwrap().remove(scope);
                    anyhow::bail!(
                        "{}",
                        r.text()
                            .await
                            .unwrap_or_else(|_| "remote RPC failed".into())
                    );
                }
                return Ok(if sc {
                    js::RpcData::V8(r.bytes().await?.to_vec())
                } else {
                    js::RpcData::Json(r.text().await?)
                });
            }
        }
    }
}

async fn s3(
    endpoint: Option<&str>,
    region: &str,
    managed: Option<&control_plane::ManagedStorageConfig>,
    isolated_transport: bool,
) -> Client {
    // Bound EVERY storage operation. The SDK's default connect timeout only
    // covers new connections: a black-holed POOLED connection has no read
    // timeout and waits forever. That is what let a partitioned node's lease
    // renewal hang indefinitely on 2026-08-01 while routing calls (which
    // opened fresh connections) failed in ~10 s. Deadline-bound decisions
    // must never be able to wait on storage without limit; the per-call
    // timeouts and the lease watchdog are independent layers of that rule.
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

async fn validate_bucket(c: &Client, bucket: &str, managed: bool) -> anyhow::Result<()> {
    // A credential the control plane has just issued is not always usable on
    // the first call because the provider needs a moment to propagate it.
    // Retry a rejection briefly before believing it.
    const RETRIES: u32 = 5;
    for attempt in 1..=RETRIES {
        match validate_bucket_once(c, bucket, managed && attempt == RETRIES).await {
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

/// One `HeadBucket`. `report` gates the control-plane state report so the
/// intermediate retries do not announce a revoked credential that is merely
/// slow to propagate.
async fn validate_bucket_once(c: &Client, bucket: &str, report: bool) -> anyhow::Result<()> {
    let managed = report;
    match c.head_bucket().bucket(bucket).send().await {
        Ok(_) => Ok(()),
        Err(SdkError::ServiceError(error))
            if matches!(error.raw().status().as_u16(), 401 | 403) =>
        {
            if managed {
                control_plane::report_managed_runtime_state(
                    control_plane::ManagedRuntimeState::CredentialRevoked,
                );
                anyhow::bail!(
                    "managed storage credential was rejected or revoked for s3://{bucket}"
                );
            }
            anyhow::bail!("storage credentials were rejected for s3://{bucket}");
        }
        Err(error) => {
            if managed {
                control_plane::report_managed_runtime_state(
                    control_plane::ManagedRuntimeState::BucketUnavailable,
                );
            }
            Err(error).with_context(|| format!("bucket unavailable or inaccessible: s3://{bucket}"))
        }
    }
}

async fn run_deploy(arguments: Vec<String>) -> anyhow::Result<()> {
    let Some(mut options) = deploy::options_from_arguments(arguments)? else {
        deploy::print_help();
        return Ok(());
    };
    // The runtime honors CELLD_BUCKET and S3_ENDPOINT; deploy must match, or
    // an operator who exported them per the docs writes to the wrong cloud.
    let env = |name: &str| {
        std::env::var(name).ok().filter(|v| !v.trim().is_empty())
    };
    if options.bucket.is_none() {
        options.bucket = env("CELLD_BUCKET")
            .map(|v| v.trim_start_matches("s3://").to_string());
    }
    if options.endpoint.is_none() {
        options.endpoint = env("S3_ENDPOINT");
    }
    if !options.dry_run && options.bucket.is_none() {
        anyhow::bail!(
            "celld deploy requires --bucket s3://NAME (or CELLD_BUCKET)"
        );
    }
    let built = deploy::build(&options)?;
    built.report();
    if options.dry_run {
        println!("Current Version ID: {} (dry run; nothing written)", built.version);
        return Ok(());
    }

    let bucket = options
        .bucket
        .expect("non-dry-run deploy validated its bucket");
    let endpoint = options.endpoint;
    let region = options
        .region
        .or_else(|| std::env::var("AWS_REGION").ok())
        .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
        .unwrap_or_else(|| "us-east-1".to_string());
    let client = s3(endpoint.as_deref(), &region, None, false).await;
    validate_bucket(&client, &bucket, false).await?;
    let started = std::time::Instant::now();
    deploy::write(&client, &bucket, &built).await?;
    println!(
        "Uploaded {} ({:.2} sec)",
        built.script_name,
        started.elapsed().as_secs_f64()
    );
    println!("  s3://{bucket}/{}", built.prefix);
    println!("Current Version ID: {}", built.version);
    // No URL: celld routes nothing, and the pointer is read at startup, so
    // saying "deployed" without this would overstate what just happened.
    println!("Nodes load a deployment at startup; restart them to serve this version.");
    Ok(())
}

async fn run_diagnostics(
    settings: startup::RuntimeSettings,
    peers: Vec<String>,
) -> anyhow::Result<()> {
    let unsafe_public_advertise = settings.unsafe_public_advertise;
    let bound = startup::bind_listener(&settings).await?;
    println!("ok listen {}", bound.listen);
    println!(
        "ok advertise {} ({}; direct reachability is not inferred)",
        bound.advertise,
        bound.advertise.scope()
    );
    let _listener = bound.listener;

    let installation_storage = if settings.control_plane {
        Some(control_plane::installation_storage().context(
            "managed diagnostics require an existing enrollment; run `celld` to enroll first",
        )?)
    } else {
        None
    };
    let managed_storage = installation_storage.as_ref().and_then(|storage| match storage {
        control_plane::InstallationStorageConfig::Managed(storage) => Some(storage),
        control_plane::InstallationStorageConfig::Byo(_) => None,
    });
    let enrolled_byo = installation_storage.as_ref().and_then(|storage| match storage {
        control_plane::InstallationStorageConfig::Managed(_) => None,
        control_plane::InstallationStorageConfig::Byo(storage) => Some(storage),
    });
    let bucket = managed_storage
        .map(|storage| storage.bucket.clone())
        .or_else(|| enrolled_byo.map(|storage| storage.bucket.clone()))
        .or(settings.bucket)
        .context("no storage bucket is configured")?;
    let endpoint = managed_storage
        .map(|storage| storage.endpoint.clone())
        .or_else(|| enrolled_byo.and_then(|storage| storage.endpoint.clone()))
        .or(settings.endpoint);
    let region = managed_storage
        .map(|storage| storage.region.clone())
        .or_else(|| enrolled_byo.map(|storage| storage.region.clone()))
        .unwrap_or(settings.region);
    let client = s3(endpoint.as_deref(), &region, managed_storage, false).await;
    validate_bucket(&client, &bucket, managed_storage.is_some()).await?;
    println!("ok bucket s3://{bucket}");

    let enumerated = peers.is_empty();
    let peers = if enumerated {
        let peers = ownership::diagnostic_node_ids(&client, &bucket).await?;
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
    let auth = peer_auth::PeerAuth::new(
        peer_auth::load_existing(&client, &bucket).await?,
        "diagnostic",
    )?;
    let mut failures = 0_usize;
    let mut expired = 0_usize;
    for peer in peers {
        let node = match ownership::diagnostic_node(&client, &bucket, &peer).await {
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
        let advertise = match startup::parse_advertise(&node.addr) {
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
                "fail peer {peer}: unsafe public advertise address {}; \
                 use a private overlay or --unsafe-public-advertise",
                node.addr
            );
            continue;
        }
        if let Err(error) = peer_probe::probe(&http, &node, &auth).await {
            failures += 1;
            eprintln!("fail peer {peer} at {}: {error}", node.addr);
            continue;
        }
        let load_age_ms = if node.load.sampled_ms == 0 {
            "unknown".to_string()
        } else {
            (now_ms().max(0) as u64)
                .saturating_sub(node.load.sampled_ms)
                .to_string()
        };
        println!(
            "ok peer {} at {} (signed direct probe) protocol={} \
             resident_cells={} websockets={} rss_bytes={} cpu_percent={:.2} \
             fds={}/{} pressured={} shed_cells={} load_age_ms={}",
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
        anyhow::bail!("fleet diagnostics failed for {failures} peer(s)");
    }
    Ok(())
}

fn random_node_session_id() -> String {
    let mut bytes = [0_u8; 16];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    let suffix = bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("node_{suffix}")
}

async fn get_string(c: &Client, bucket: &str, key: &str) -> anyhow::Result<String> {
    let o = c.get_object().bucket(bucket).key(key).send().await?;
    let b = o.body.collect().await?.into_bytes();
    Ok(String::from_utf8_lossy(&b).to_string())
}

async fn load_deploy_from_pointer(
    c: &Client,
    bucket: &str,
    pointer_key: &str,
) -> anyhow::Result<(
    Manifest,
    String,
    Vec<(String, String)>,
    Option<assets::AssetResolver>,
)> {
    let ptr: DeployPointer =
        serde_json::from_str(&get_string(c, bucket, pointer_key).await?)?;
    let manifest: Manifest = serde_json::from_str(
        &get_string(c, bucket, &format!("{}/manifest.json", ptr.prefix)).await?,
    )?;
    let src = match manifest.main_module.as_deref() {
        Some(main_module) => {
            get_string(c, bucket, &format!("{}/{}", ptr.prefix, main_module)).await?
        }
        None if manifest.assets.is_some() => {
            // Keep the isolate construction path uniform. Ingress returns an
            // asset response (or an asset-only 404) before this fallback is
            // reached; the synthetic handler is only a fail-closed guard.
            "export default { fetch() { return new Response('Not found', { status: 404 }); } };"
                .to_string()
        }
        None => anyhow::bail!("deployment has neither a main module nor assets"),
    };
    // Non-main modules are wrangler Text/Data siblings (e.g. `import md from
    // './x.md'`). Fetch each; the worker imports it as `./<name>`.
    let mut text = Vec::new();
    for m in &manifest.modules {
        if manifest.main_module.as_deref() == Some(m.name.as_str()) {
            continue;
        }
        let content = get_string(c, bucket, &format!("{}/{}", ptr.prefix, m.name)).await?;
        text.push((format!("./{}", m.name), content));
    }
    let asset_resolver = match &manifest.assets {
        Some(reference) => Some(
            assets::AssetResolver::load(
                c,
                bucket,
                &ptr.prefix,
                reference,
                manifest.main_module.is_none(),
            )
            .await?,
        ),
        None => None,
    };
    Ok((manifest, src, text, asset_resolver))
}

/// Load a named component for a Worker service binding.
async fn load_deploy(
    c: &Client,
    bucket: &str,
    script: &str,
) -> anyhow::Result<(
    Manifest,
    String,
    Vec<(String, String)>,
    Option<assets::AssetResolver>,
)> {
    load_deploy_from_pointer(c, bucket, &format!("deploy/{script}/current.json")).await
}

/// Resolve the fleet's one current application.
///
/// Buckets written before the fleet-wide pointer was introduced may contain
/// several named pointers. For that one-way migration, select the most
/// recently modified pointer. New deployments always commit
/// `deploy/current.json`, so no script selector is exposed to the operator.
async fn load_current_deploy(
    c: &Client,
    bucket: &str,
) -> anyhow::Result<(
    Manifest,
    String,
    Vec<(String, String)>,
    Option<assets::AssetResolver>,
)> {
    let objects = c
        .list_objects_v2()
        .bucket(bucket)
        .prefix("deploy/")
        .send()
        .await
        .context("discover current deployment")?;
    let mut candidates = Vec::new();
    for object in objects.contents() {
        let Some(key) = object.key() else {
            continue;
        };
        let global = key == "deploy/current.json";
        let named = key
            .strip_prefix("deploy/")
            .is_some_and(is_named_current_pointer);
        if global || named {
            let modified = object
                .last_modified()
                .map(|time| (time.secs(), time.subsec_nanos()))
                .unwrap_or_default();
            // Prefer the global pointer if an S3-compatible backend reports
            // identical modification times at its available precision.
            candidates.push((modified, global, key.to_string()));
        }
    }
    let (_, global, key) = candidates
        .into_iter()
        .max()
        .context("fleet bucket contains no deployment at deploy/current.json")?;
    if !global {
        info!(
            event = "legacy_deployment_pointer_selected",
            pointer = %key,
            "using the latest deployment from a pre-single-application bucket"
        );
    }
    load_deploy_from_pointer(c, bucket, &key).await
}

fn is_named_current_pointer(key: &str) -> bool {
    let mut parts = key.split('/');
    matches!(
        (parts.next(), parts.next(), parts.next()),
        (Some(_), Some("current.json"), None)
    )
}

fn http_response(response: js::HttpResponse) -> Response {
    // A WebSocket acceptance travels as 200 + x-celld-ws-target: the /__do
    // exchange is a plain POST, and intermediaries drop headers on an
    // out-of-context 101. The proxying client restores the 101.
    let status = if response.ws.is_some() && response.status == 101 {
        200
    } else {
        response.status
    };
    let mut builder = Response::builder().status(status);
    for (name, value) in response.headers {
        if matches!(name.as_str(), "connection" | "content-length" | "transfer-encoding") {
            continue;
        }
        let Ok(name) = axum::http::HeaderName::from_bytes(name.as_bytes()) else { continue };
        let Ok(value) = axum::http::HeaderValue::from_str(&value) else { continue };
        builder = builder.header(name, value);
    }
    if let Some(ws) = response.ws {
        if let Ok(value) = axum::http::HeaderValue::from_str(
            &serde_json::to_string(&ws).unwrap_or_default(),
        ) {
            builder = builder.header("x-celld-ws-target", value);
        }
    }
    let body = match response.stream {
        Some(stream) => {
            use tokio_stream::StreamExt;
            let stream = tokio_stream::wrappers::ReceiverStream::new(stream)
                .map(|chunk| chunk.map_err(std::io::Error::other));
            Body::from_stream(stream)
        }
        None => Body::from(response.body),
    };
    builder.body(body).unwrap()
}


/// Ingress: hand the request to any Worker isolate (round-robin). The Worker's
/// `env.DO.get(id).fetch()` routes to a cell via `__do_call`/`run_do`.
struct WebsocketConnectionOutcome<'a> {
    outcome: &'a str,
    route: &'a str,
    scope: &'a str,
    status: u16,
}

fn emit_websocket_connection_timing(
    cx: &Ctx,
    request_id: js::RequestId,
    started: Instant,
    body_read_us: u64,
    worker_dispatch_us: u64,
    event_outcome: WebsocketConnectionOutcome<'_>,
) {
    let WebsocketConnectionOutcome {
        outcome,
        route,
        scope,
        status,
    } = event_outcome;
    info!(
        event = "websocket_connection_timing",
        outcome,
        route,
        scope,
        request_id = %js::request_id_string(request_id),
        node = %cx.node,
        region = %cx.region,
        runtime_version = env!("CARGO_PKG_VERSION"),
        status,
        total_us = started.elapsed().as_micros() as u64,
        body_read_us,
        worker_dispatch_us,
        "WebSocket connection resolved"
    );
}

async fn handle(State(st): State<AppState>, mut req: axum::extract::Request) -> Response {
    let is_websocket = fastwebsockets::upgrade::is_upgrade_request(&req);
    let websocket_started = is_websocket.then(Instant::now);
    if (req.method() == axum::http::Method::GET ||
        req.method() == axum::http::Method::HEAD)
        && !is_websocket
    {
        let path = req.uri().path();
        let head = req.method() == axum::http::Method::HEAD;
        if let Some(resolver) = &st.assets {
            if !resolver.should_run_worker_first(path) {
                match resolver
                    .ingress_response(path, req.uri().query(), head, req.headers())
                    .await
                {
                    Ok(Some(response)) => return response,
                    Ok(None) if resolver.asset_only() => {
                        return (StatusCode::NOT_FOUND, "Not found").into_response();
                    }
                    Ok(None) => {}
                    Err(error) => {
                        warn!(
                            event = "asset_response_failed",
                            path,
                            %error,
                            "active deployment asset could not be served"
                        );
                        return (
                            StatusCode::BAD_GATEWAY,
                            "Active deployment asset is unavailable",
                        )
                            .into_response();
                    }
                }
            }
        }
    }
    let url = format!("http://cell{}", req.uri());
    let method = req.method().to_string();
    let headers = req.headers().iter().map(|(name, value)| (
        name.as_str().to_string(),
        value.to_str().unwrap_or_default().to_string(),
    )).collect::<Vec<_>>();
    let upgrade = if is_websocket {
        match fastwebsockets::upgrade::upgrade(&mut req) {
            Ok(upgrade) => Some(upgrade),
            Err(e) => return (StatusCode::BAD_REQUEST, format!("ws upgrade: {e}")).into_response(),
        }
    } else {
        None
    };
    let body_started = Instant::now();
    let body = axum::body::to_bytes(req.into_body(), usize::MAX).await
        .map(|b| b.to_vec()).unwrap_or_default();
    let body_read_us = body_started.elapsed().as_micros() as u64;
    let (reply, rx) = tokio::sync::oneshot::channel();
    let request_id = js::next_request_id();
    // Run the worker `fetch` inside a resident cell isolate, so
    // `env.NS.get(ownScope)` resolves in-isolate with no `__do_call` hop.
    // Falls back to the stateless worker pool otherwise.
    let cell_route = if upgrade.is_none() {
        let reg = st.cx.registry.lock().unwrap();
        let n = reg.len();
        if n == 0 {
            None // pure worker (no resident cells): use the pool
        } else {
            // Round-robin the landing cell so routing load spreads across cells
            // rather than concentrating on one.
            static RR: AtomicUsize = AtomicUsize::new(0);
            let idx = RR.fetch_add(1, Ordering::Relaxed) % n;
            reg.values()
                .skip(idx)
                .chain(reg.values().take(idx))
                .find_map(|cell| {
                    cell.activity
                        .try_acquire_idle()
                        .map(|activity| (cell.tx.clone(), activity))
                })
        }
    } else { None };
    let worker_started = Instant::now();
    let sent = match cell_route {
        Some((tx, inline_activity)) => tx
            .send(CellJob::WorkerFetch {
                request_id,
                url,
                method,
                body,
                headers,
                inline_activity,
                fallback_workers: st.workers.clone(),
                reply,
            })
            .map_err(|_| ()),
        None => st.workers.send(WorkerJob::Fetch {
            queued_at: worker_started,
            url, method, body, headers, request_id: Some(request_id), reply,
        })
            .map_err(|_| ()),
    };
    if sent.is_err() {
        if let Some(started) = websocket_started {
            emit_websocket_connection_timing(
                &st.cx,
                request_id,
                started,
                body_read_us,
                worker_started.elapsed().as_micros() as u64,
                WebsocketConnectionOutcome {
                    outcome: "worker_gone",
                    route: "",
                    scope: "",
                    status: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                },
            );
        }
        return (StatusCode::INTERNAL_SERVER_ERROR, "worker gone").into_response();
    }
    let mut abort_guard = IngressAbortGuard::new(request_id);
    let result = rx.await;
    let worker_dispatch_us = worker_started.elapsed().as_micros() as u64;
    abort_guard.disarm();
    match result {
        Ok(Ok(result)) => {
            if let (101, Some(target), Some((response, fut))) =
                (result.status, result.ws.clone(), upgrade)
            {
                if let Some(started) = websocket_started {
                    emit_websocket_connection_timing(
                        &st.cx,
                        request_id,
                        started,
                        body_read_us,
                        worker_dispatch_us,
                        WebsocketConnectionOutcome {
                            outcome: "accepted",
                            route: if target.peer_node.is_some() {
                                "remote"
                            } else {
                                "local"
                            },
                            scope: &target.scope,
                            status: 101,
                        },
                    );
                }
                let cx = st.cx.clone();
                tokio::spawn(async move {
                    let ws = match fut.await {
                        Ok(ws) => ws,
                        Err(e) => { warn!(%e, "ws upgrade fut"); return; }
                    };
                    if target.peer_node.is_some() {
                        if let Err(error) = remote_ws_task(cx, target, ws).await {
                            warn!(
                                error = %format!("{error:#}"),
                                "remote WebSocket tunnel failed"
                            );
                        }
                    } else {
                        ws_task(cx, target.scope, target.id, ws).await;
                    }
                });
                response.map(axum::body::Body::new)
            } else {
                if let Some(started) = websocket_started {
                    emit_websocket_connection_timing(
                        &st.cx,
                        request_id,
                        started,
                        body_read_us,
                        worker_dispatch_us,
                        WebsocketConnectionOutcome {
                            outcome: "rejected",
                            route: "",
                            scope: "",
                            status: result.status,
                        },
                    );
                }
                http_response(result)
            }
        }
        Ok(Err(e)) => {
            if let Some(started) = websocket_started {
                emit_websocket_connection_timing(
                    &st.cx,
                    request_id,
                    started,
                    body_read_us,
                    worker_dispatch_us,
                    WebsocketConnectionOutcome {
                        outcome: "worker_error",
                        route: "",
                        scope: "",
                        status: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                    },
                );
            }
            warn!(%e, "worker error");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("worker error: {e}"),
            )
                .into_response()
        }
        Err(_) => {
            if let Some(started) = websocket_started {
                emit_websocket_connection_timing(
                    &st.cx,
                    request_id,
                    started,
                    body_read_us,
                    worker_dispatch_us,
                    WebsocketConnectionOutcome {
                        outcome: "no_reply",
                        route: "",
                        scope: "",
                        status: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                    },
                );
            }
            (StatusCode::INTERNAL_SERVER_ERROR, "no reply").into_response()
        }
    }
}

async fn peer_probe_handle(
    State(st): State<AppState>,
    request: axum::extract::Request,
) -> Response {
    if request.method() != axum::http::Method::GET {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    let Some(challenge) = request
        .headers()
        .get("x-cells-probe-challenge")
        .and_then(|value| value.to_str().ok())
    else {
        return (StatusCode::BAD_REQUEST, "missing probe challenge").into_response();
    };
    match peer_probe::respond(&st.cx.node, &st.cx.advertise, challenge) {
        Ok(response) => axum::Json(response).into_response(),
        Err(_) => (StatusCode::BAD_REQUEST, "invalid probe challenge").into_response(),
    }
}

async fn require_peer_auth(
    State(st): State<AppState>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let (parts, body) = request.into_parts();
    let body = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(body) => body,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                "could not read authenticated peer request body",
            )
                .into_response()
        }
    };
    let path_and_query = parts
        .uri
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or_else(|| parts.uri.path());
    if let Err(error) = st.cx.peer_auth.verify(
        &parts.method,
        path_and_query,
        &parts.headers,
        &body,
        &st.cx.node,
    ) {
        let mut response = (error.status(), error.message()).into_response();
        response.headers_mut().insert(
            axum::http::HeaderName::from_static(peer_auth::RESPONSE_VERSION_HEADER),
            axum::http::HeaderValue::from_static(peer_auth::PROTOCOL_VERSION_TEXT),
        );
        if matches!(error, peer_auth::VerifyError::WrongTarget) {
            response.headers_mut().insert(
                axum::http::HeaderName::from_static(STALE_ROUTE_HEADER),
                axum::http::HeaderValue::from_static(STALE_ROUTE_VALUE),
            );
        }
        return response;
    }
    let request = axum::extract::Request::from_parts(parts, Body::from(body));
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        axum::http::HeaderName::from_static(peer_auth::RESPONSE_VERSION_HEADER),
        axum::http::HeaderValue::from_static(peer_auth::PROTOCOL_VERSION_TEXT),
    );
    response
}

/// Internal cross-node endpoint: a peer proxies a DO call here. We own (or will
/// activate) the scope; run it locally via the same `run_do` router.
async fn do_handle(
    State(st): State<AppState>,
    axum::extract::Path(scope): axum::extract::Path<String>,
    body: String,
) -> Response {
    use base64::Engine;
    let v: serde_json::Value = serde_json::from_str(&body).unwrap_or(serde_json::json!({}));
    let url = v.get("url").and_then(|x| x.as_str()).unwrap_or("http://cell/");
    let name = v.get("name").and_then(|x| x.as_str());
    let method = v.get("method").and_then(|x| x.as_str()).unwrap_or("GET");
    let body = v
        .get("bodyBase64")
        .and_then(|value| value.as_str())
        .and_then(|value| base64::engine::general_purpose::STANDARD.decode(value).ok())
        .or_else(|| {
            v.get("body")
                .and_then(|value| value.as_str())
                .map(|value| value.as_bytes().to_vec())
        })
        .unwrap_or_default();
    let request_id = v
        .get("requestId")
        .and_then(|value| value.as_str())
        .and_then(js::parse_request_id);
    let force_admit = v.get("admit").and_then(|value| value.as_bool()).unwrap_or(false);
    let headers: Vec<(String, String)> = serde_json::from_value(
        v.get("headers").cloned().unwrap_or_else(|| serde_json::json!([])),
    ).unwrap_or_default();
    let result = match ensure_cell_with_admission(
        &st.cx,
        &scope,
        force_admit,
        !force_admit,
    ).await {
        Ok(CellRoute::Local { tx, _activity }) => {
            let epoch = st.cx.epochs.lock().unwrap().get(&scope).copied()
                .context("locally routed cell has no fencing epoch");
            let result = run_do_local(
                &st.cx,
                tx,
                _activity,
                DoRequest {
                    scope: &scope,
                    name,
                    url,
                    method,
                    body: &body,
                    headers: &headers,
                    request_id,
                },
                None,
            )
            .await;
            match (result, epoch) {
                (Ok(mut response), Ok(epoch)) => {
                    if let Some(target) = response.ws.as_mut() {
                        // A remote ingress uses this exact epoch for /__ws.
                        // Capture it after activation; its cached route may
                        // describe the preceding hibernated epoch.
                        target.peer_epoch = Some(epoch);
                    }
                    Ok(response)
                }
                (Err(error), _) | (_, Err(error)) => Err(error),
            }
        }
        Ok(CellRoute::Remote(_)) => return stale_route_response(&scope),
        Err(error) => Err(error),
    };
    match result {
        Ok(result) => {
            let streamed = result.stream.is_some();
            let mut response = http_response(result);
            if streamed {
                response.headers_mut().insert(
                    axum::http::HeaderName::from_static("x-celld-body-stream"),
                    axum::http::HeaderValue::from_static("1"),
                );
            }
            response
        }
        Err(error) if error.downcast_ref::<CapacityExhausted>().is_some() => {
            stale_route_response(&scope)
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("do error: {e}")).into_response(),
    }
}

async fn abort_do_handle(
    State(st): State<AppState>,
    axum::extract::Path((scope, request_id)): axum::extract::Path<(String, String)>,
) -> Response {
    let Some(request_id) = js::parse_request_id(&request_id) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    match ensure_cell(&st.cx, &scope).await {
        Ok(CellRoute::Local { tx, .. }) => {
            let mut aborts = st.cx.remote_aborts.lock().unwrap();
            prune_remote_aborts(&mut aborts);
            aborts.insert(request_id, std::time::Instant::now());
            let _ = tx.send(CellJob::AbortFetch { request_id });
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(CellRoute::Remote(_)) => stale_route_response(&scope),
        Err(error) => {
            warn!(%error, %scope, "resolve request abort target");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "could not resolve request abort target",
            )
                .into_response()
        }
    }
}

async fn rpc_handle(
    State(st): State<AppState>,
    axum::extract::Path(scope): axum::extract::Path<String>,
    body: String,
) -> Response {
    use base64::Engine;
    let v: serde_json::Value = serde_json::from_str(&body).unwrap_or(serde_json::json!({}));
    let name = v.get("name").and_then(|x| x.as_str());
    let method = v.get("method").and_then(|x| x.as_str()).unwrap_or("");
    let force_admit = v.get("admit").and_then(|value| value.as_bool()).unwrap_or(false);
    // `sc` carries base64 structured-clone bytes; `args` is the legacy JSON
    // flavor. The response mirrors the request: raw clone bytes or JSON text.
    let args = match v.get("sc").and_then(|x| x.as_str()) {
        Some(sc) => js::RpcData::V8(
            base64::engine::general_purpose::STANDARD.decode(sc)
                .unwrap_or_default(),
        ),
        None => js::RpcData::Json(
            v.get("args").cloned().unwrap_or_else(|| serde_json::json!([]))
                .to_string(),
        ),
    };
    let result = match ensure_cell_with_admission(
        &st.cx,
        &scope,
        force_admit,
        !force_admit,
    ).await {
        Ok(CellRoute::Local { tx, _activity }) => {
            let (reply, rx) = tokio::sync::oneshot::channel();
            let sent = tx
                .send(CellJob::Rpc {
                    scope: scope.clone(),
                    name: name.map(str::to_owned),
                    method: method.to_string(),
                    args,
                    reply,
                })
                .map_err(|_| anyhow::anyhow!("cell channel closed"));
            match sent {
                Ok(()) => match rx.await {
                    Ok(result) => result,
                    Err(_) => Err(anyhow::anyhow!("cell dropped")),
                },
                Err(error) => Err(error),
            }
        }
        Ok(CellRoute::Remote(_)) => return stale_route_response(&scope),
        Err(error) => Err(error),
    };
    match result {
        Ok(js::RpcData::Json(value)) => Response::builder()
            .status(StatusCode::OK).body(Body::from(value)).unwrap(),
        Ok(js::RpcData::V8(bytes)) => Response::builder()
            .status(StatusCode::OK).body(Body::from(bytes)).unwrap(),
        Err(error) if error.downcast_ref::<CapacityExhausted>().is_some() => {
            stale_route_response(&scope)
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("rpc error: {e}")).into_response(),
    }
}

/// WebSocket upgrade for `scope`. The socket is held by a host task decoupled
/// from the cell isolate — so the cell can hibernate while the socket lives,
/// and an incoming message re-activates it. (Slice: host binds the socket to
/// the scope directly; the CF `acceptWebSocket`/101 handshake is a later
/// refinement. Outbound is drained after each inbound message — server-push
/// without an inbound message is a follow-up needing a split reader/writer.)
#[derive(serde::Deserialize)]
struct PeerWsQuery {
    id: u64,
    epoch: u64,
}

async fn ws_handle(
    State(st): State<AppState>,
    axum::extract::Path(scope): axum::extract::Path<String>,
    axum::extract::Query(query): axum::extract::Query<PeerWsQuery>,
    incoming: fastwebsockets::upgrade::IncomingUpgrade,
) -> Response {
    if st.cx.epochs.lock().unwrap().get(&scope) != Some(&query.epoch) {
        return stale_route_response(&scope);
    }
    let (response, fut) = match incoming.upgrade() {
        Ok(x) => x,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("ws upgrade: {e}")).into_response(),
    };
    let cx = st.cx.clone();
    let ws_id = query.id;
    tokio::spawn(async move {
        let ws = match fut.await { Ok(ws) => ws, Err(e) => { warn!(%e, "ws upgrade fut"); return; } };
        ws_task(cx, scope, ws_id, ws).await;
    });
    response.map(axum::body::Body::new)
}

async fn remote_ws_task<S>(
    cx: Arc<Ctx>,
    target: js::WsTarget,
    mut client: fastwebsockets::WebSocket<S>,
) -> anyhow::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use fastwebsockets::{Frame, FragmentCollector, OpCode};
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
    use tokio_tungstenite::tungstenite::protocol::CloseFrame;
    use tokio_tungstenite::tungstenite::Message;

    let node = target
        .peer_node
        .as_deref()
        .context("remote WebSocket target has no node")?;
    let addr = target
        .peer_addr
        .as_deref()
        .context("remote WebSocket target has no address")?;
    let epoch = target
        .peer_epoch
        .context("remote WebSocket target has no owner epoch")?;
    let path = format!("/__ws/{}?id={}&epoch={epoch}", target.scope, target.id);
    let mut request = format!("ws://{addr}{path}")
        .into_client_request()
        .context("build peer WebSocket request")?;
    request.headers_mut().extend(
        cx.peer_auth
            .signed_headers("GET", &path, &[], node)
            .context("authenticate peer WebSocket request")?,
    );
    let (peer, response) = tokio_tungstenite::connect_async(request)
        .await
        .with_context(|| format!("connect WebSocket tunnel to peer {node} at {addr}"))?;
    peer_auth::validate_response(response.headers())?;

    client.set_auto_close(false);
    let mut client = FragmentCollector::new(client);
    let (mut peer_write, mut peer_read) = peer.split();
    let mut client_open = true;
    info!(scope = %target.scope, ws_id = target.id, %node, "remote WebSocket tunnel open");
    loop {
        tokio::select! {
            frame = client.read_frame(), if client_open => {
                let frame = match frame {
                    Ok(frame) => frame,
                    Err(_) => break,
                };
                let message = match frame.opcode {
                    OpCode::Text => Message::Text(
                        String::from_utf8_lossy(&frame.payload).into_owned().into(),
                    ),
                    OpCode::Binary => Message::Binary(frame.payload.to_vec().into()),
                    OpCode::Ping => Message::Ping(frame.payload.to_vec().into()),
                    OpCode::Pong => Message::Pong(frame.payload.to_vec().into()),
                    OpCode::Close => {
                        let (code, reason, _) = websocket_close_details(&frame.payload);
                        client_open = false;
                        Message::Close(Some(CloseFrame {
                            code: CloseCode::from(if code == 1005 { 1000 } else { code }),
                            reason: reason.into(),
                        }))
                    }
                    _ => continue,
                };
                if peer_write.send(message).await.is_err() {
                    if client_open {
                        let _ = client
                            .write_frame(Frame::close(1012, b"owner unavailable"))
                            .await;
                        let _ = tokio::time::timeout(
                            Duration::from_secs(1),
                            client.read_frame(),
                        )
                        .await;
                    }
                    break;
                }
            }
            message = peer_read.next() => {
                let message = match message {
                    Some(Ok(message)) => message,
                    Some(Err(error)) => {
                        warn!(
                            %error,
                            scope = %target.scope,
                            ws_id = target.id,
                            %node,
                            "remote WebSocket owner connection failed"
                        );
                        if client_open {
                            let close_result = client
                                .write_frame(Frame::close(1012, b"owner unavailable"))
                                .await;
                            warn!(
                                ?close_result,
                                scope = %target.scope,
                                ws_id = target.id,
                                %node,
                                "sent owner-loss close to WebSocket client"
                            );
                            let _ = tokio::time::timeout(
                                Duration::from_secs(1),
                                client.read_frame(),
                            )
                            .await;
                        }
                        break;
                    }
                    None => {
                        if client_open {
                            let close_result = client
                                .write_frame(Frame::close(1012, b"owner unavailable"))
                                .await;
                            warn!(
                                ?close_result,
                                scope = %target.scope,
                                ws_id = target.id,
                                %node,
                                "sent owner-loss close to WebSocket client"
                            );
                            let _ = tokio::time::timeout(
                                Duration::from_secs(1),
                                client.read_frame(),
                            )
                            .await;
                        }
                        break;
                    }
                };
                let (frame, keep_open) = match message {
                    Message::Text(text) => (Frame::text(text.as_bytes().to_vec().into()), true),
                    Message::Binary(bytes) => (Frame::binary(bytes.to_vec().into()), true),
                    Message::Ping(bytes) => (
                        Frame::new(true, OpCode::Ping, None, bytes.to_vec().into()),
                        true,
                    ),
                    Message::Pong(bytes) => (
                        Frame::new(true, OpCode::Pong, None, bytes.to_vec().into()),
                        true,
                    ),
                    Message::Close(frame) => {
                        let (code, reason) = frame
                            .map(|frame| (u16::from(frame.code), frame.reason.to_string()))
                            .unwrap_or((1000, String::new()));
                        (Frame::close(code, reason.as_bytes()), false)
                    }
                    Message::Frame(_) => continue,
                };
                client.write_frame(frame).await.context("write client WebSocket frame")?;
                if !keep_open {
                    break;
                }
            }
        }
    }
    info!(scope = %target.scope, ws_id = target.id, %node, "remote WebSocket tunnel closed");
    Ok(())
}

/// Handshake budget for an outbound WebSocket.
const DEFAULT_OUTBOUND_WS_CONNECT_MS: u64 = 10_000;

fn outbound_ws_connect_timeout() -> Duration {
    static TIMEOUT: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *TIMEOUT.get_or_init(|| {
        Duration::from_millis(
            std::env::var("CELLD_OUTBOUND_WS_CONNECT_MS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(DEFAULT_OUTBOUND_WS_CONNECT_MS),
        )
    })
}
/// Default per-cell ceiling on concurrent outbound WebSockets.
const DEFAULT_MAX_OUTBOUND_WEBSOCKETS: usize = 32;

fn max_outbound_websockets() -> usize {
    static LIMIT: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *LIMIT.get_or_init(|| {
        std::env::var("CELLD_MAX_OUTBOUND_WEBSOCKETS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_MAX_OUTBOUND_WEBSOCKETS)
    })
}

/// One cell's claim on an outbound socket slot, released on drop so a failed
/// handshake or a panicking task cannot leak the budget.
struct OutboundWsSlot {
    cx: Arc<Ctx>,
    scope: String,
}

impl OutboundWsSlot {
    fn claim(cx: &Arc<Ctx>, scope: &str) -> Option<Self> {
        let mut counts = cx.outbound_websockets.lock().unwrap();
        // Node-wide first, counted in pinned CELLS: one socket is enough to
        // pin a cell, so a per-cell ceiling bounds nothing here. A cell that
        // already holds one is not a new pin.
        if !counts.contains_key(scope)
            && !celld_logic::evict::may_pin_outbound(
                counts.len(),
                cx.pressure_config.resident_high,
            )
        {
            return None;
        }
        let count = counts.entry(scope.to_string()).or_default();
        if *count >= max_outbound_websockets() {
            return None;
        }
        *count += 1;
        Some(Self { cx: cx.clone(), scope: scope.to_string() })
    }
}

impl Drop for OutboundWsSlot {
    fn drop(&mut self) {
        let mut counts = self.cx.outbound_websockets.lock().unwrap();
        let Some(count) = counts.get_mut(&self.scope) else { return };
        *count = count.saturating_sub(1);
        if *count == 0 {
            counts.remove(&self.scope);
        }
    }
}

/// Where an outbound socket's inbound events go.
///
/// A Durable Object socket must survive between events and revive a hibernated
/// cell, so its events are pushed in as `CellJob`s. A Worker socket has no cell
/// to push into and no isolate to address; it lives inside one request's region
/// and the isolate pulls its events, which is the same lifetime Cloudflare
/// gives a Worker socket. One task, one protocol loop, two deliveries.
enum WsSink {
    Cell { cx: Arc<Ctx>, scope: String },
    Isolate(js::WsPullSender),
}

impl WsSink {
    async fn open(&self, ws_id: u64, protocol: String) -> anyhow::Result<()> {
        match self {
            WsSink::Cell { cx, scope } => run_ws_open(cx, scope, ws_id, protocol).await,
            WsSink::Isolate(tx) => tx
                .send(js::WsPull::Open(protocol))
                .map_err(|_| anyhow::anyhow!("isolate stopped reading")),
        }
    }

    async fn message(&self, ws_id: u64, data: js::WsIn) -> anyhow::Result<()> {
        match self {
            WsSink::Cell { cx, scope } => run_ws(cx, scope, ws_id, data).await,
            WsSink::Isolate(tx) => tx
                .send(match data {
                    js::WsIn::Text(text) => js::WsPull::Text(text),
                    js::WsIn::Binary(bytes) => js::WsPull::Binary(bytes),
                })
                .map_err(|_| anyhow::anyhow!("isolate stopped reading")),
        }
    }

    async fn closed(
        &self,
        ws_id: u64,
        code: u16,
        reason: String,
        was_clean: bool,
    ) -> anyhow::Result<()> {
        match self {
            WsSink::Cell { cx, scope } => {
                run_ws_closed(cx, scope, ws_id, code, reason, was_clean).await
            }
            WsSink::Isolate(tx) => tx
                .send(js::WsPull::Close(code, reason, was_clean))
                .map_err(|_| anyhow::anyhow!("isolate stopped reading")),
        }
    }

    fn scope(&self) -> &str {
        match self {
            WsSink::Cell { scope, .. } => scope,
            WsSink::Isolate(_) => "",
        }
    }
}

/// One outbound handshake's inputs, grouped so the task reads as one thing
/// rather than an eight-argument call.
struct OutboundWs {
    sink: WsSink,
    ws_id: u64,
    url: String,
    protocols: Vec<String>,
    extra_headers: Vec<(String, String)>,
    /// A `fetch()` upgrade wants the response a declining server sent.
    want_response: bool,
    reply: tokio::sync::oneshot::Sender<anyhow::Result<js::OutboundWsOpen>>,
}

async fn outbound_ws_task(
    cx: Arc<Ctx>,
    outbound: OutboundWs,
) -> anyhow::Result<()> {
    let OutboundWs {
        sink,
        ws_id,
        url,
        protocols,
        extra_headers,
        want_response,
        reply,
    } = outbound;
    let scope = sink.scope().to_string();
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::http::header::{HeaderValue, SEC_WEBSOCKET_PROTOCOL};
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
    use tokio_tungstenite::tungstenite::protocol::CloseFrame;
    use tokio_tungstenite::tungstenite::Message;

    let mut request = url.into_client_request().context("build outbound WebSocket request")?;
    for (name, value) in &extra_headers {
        // The handshake headers are the client's to own; an application header
        // must not be able to rewrite them.
        if matches!(
            name.to_ascii_lowercase().as_str(),
            "upgrade" | "connection" | "sec-websocket-key" | "sec-websocket-version"
                | "sec-websocket-protocol" | "host"
        ) {
            continue;
        }
        let (Ok(name), Ok(value)) = (
            tokio_tungstenite::tungstenite::http::header::HeaderName::from_bytes(
                name.as_bytes(),
            ),
            HeaderValue::from_str(value),
        ) else {
            continue;
        };
        request.headers_mut().insert(name, value);
    }
    if !protocols.is_empty() {
        request.headers_mut().insert(
            SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_str(&protocols.join(", "))
                .context("invalid WebSocket subprotocol")?,
        );
    }
    // Bound the handshake. The JS promise is registered as a waitUntil, so an
    // unbounded connect to a black-holed address holds the originating event
    // open for as long as the peer stays silent.
    let connect_timeout = outbound_ws_connect_timeout();
    let connected = tokio::time::timeout(
        connect_timeout,
        tokio_tungstenite::connect_async(request),
    ).await;
    let (mut socket, response) = match connected {
        Ok(Ok(value)) => value,
        // A server that answers without upgrading is not a failure: `fetch`
        // returns that response unchanged, exactly as workerd does.
        Ok(Err(tokio_tungstenite::tungstenite::Error::Http(http))) if want_response => {
            let status = http.status().as_u16();
            let headers = http
                .headers()
                .iter()
                .map(|(name, value)| (
                    name.as_str().to_string(),
                    value.to_str().unwrap_or_default().to_string(),
                ))
                .collect();
            let body = http.body().clone().unwrap_or_default();
            let _ = reply.send(Ok(js::OutboundWsOpen {
                protocol: None,
                declined: Some(js::DeclinedUpgrade { status, headers, body }),
            }));
            return Ok(());
        }
        Ok(Err(error)) => {
            let _ = reply.send(Err(anyhow::anyhow!(error)));
            return Ok(());
        }
        Err(_) => {
            let _ = reply.send(Err(anyhow::anyhow!(
                "outbound WebSocket handshake timed out after {}ms",
                connect_timeout.as_millis(),
            )));
            return Ok(());
        }
    };
    let protocol = response.headers().get(SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok()).map(str::to_string);
    if protocol.as_ref().is_some_and(|selected| !protocols.iter().any(|p| p == selected)) {
        let error = anyhow::anyhow!("server selected an unrequested WebSocket subprotocol");
        let _ = reply.send(Err(error));
        return Ok(());
    }

    // Only a cell-backed socket pins anything: a Worker socket has no cell to
    // hold resident, and dies with its request.
    let cell_backed = matches!(sink, WsSink::Cell { .. });
    let _host_websocket = cell_backed
        .then(|| HostWebSocketGuard::new(cx.clone(), &scope));
    // An outbound socket is not hibernatable, so it pins its cell resident for
    // as long as it is open. Without a cap, a loop of `new WebSocket(...)`
    // consumes a node's residency budget from inside one cell.
    let _outbound = match cell_backed
        .then(|| OutboundWsSlot::claim(&cx, &scope))
    {
        Some(None) => {
            let _ = reply.send(Err(anyhow::anyhow!(
                "outbound WebSocket refused: cell limit is {}, and a node may \
                 pin at most {}% of its residency ceiling",
                max_outbound_websockets(),
                celld_logic::evict::MAX_OUTBOUND_PIN_PERCENT,
            )));
            return Ok(());
        }
        claimed => claimed.flatten(),
    };
    let (otx, mut orx) = tokio::sync::mpsc::unbounded_channel::<js::WsOut>();
    if cell_backed {
        js::ws_register_outbound(ws_id, &scope);
    }
    js::ws_register(ws_id, otx);
    if let Err(error) = sink.open(
        ws_id,
        protocol.as_deref().unwrap_or_default().to_string(),
    ).await {
        js::ws_unregister(ws_id);
        let _ = reply.send(Err(error));
        return Ok(());
    }
    if reply.send(Ok(js::OutboundWsOpen { protocol, declined: None })).is_err() {
        js::ws_unregister(ws_id);
        return Ok(());
    }
    info!(%scope, ws_id, "outbound WebSocket open");

    let mut close_code = 1006;
    let mut close_reason = String::new();
    let mut close_was_clean = false;
    loop {
        tokio::select! {
            incoming = socket.next() => match incoming {
                Some(Ok(Message::Text(text))) => {
                    if let Err(error) = sink.message(
                        ws_id,
                        js::WsIn::Text(text.to_string()),
                    ).await {
                        warn!(%error, %scope, ws_id, "outbound WebSocket message dispatch failed");
                        break;
                    }
                }
                Some(Ok(Message::Binary(bytes))) => {
                    if let Err(error) = sink.message(
                        ws_id,
                        js::WsIn::Binary(bytes.to_vec()),
                    ).await {
                        warn!(%error, %scope, ws_id, "outbound WebSocket message dispatch failed");
                        break;
                    }
                }
                Some(Ok(Message::Ping(payload))) => {
                    if socket.send(Message::Pong(payload)).await.is_err() { break; }
                }
                Some(Ok(Message::Close(frame))) => {
                    if let Some(frame) = frame {
                        close_code = u16::from(frame.code);
                        close_reason = frame.reason.to_string();
                    } else {
                        close_code = 1005;
                    }
                    close_was_clean = true;
                    break;
                }
                Some(Ok(_)) => {}
                Some(Err(error)) => {
                    warn!(%error, %scope, ws_id, "outbound WebSocket read failed");
                    break;
                }
                None => break,
            },
            outgoing = orx.recv() => match outgoing {
                Some(js::WsOut::Text(text)) => {
                    if socket.send(Message::Text(text.into())).await.is_err() { break; }
                }
                Some(js::WsOut::Binary(data)) => {
                    if socket.send(Message::Binary(data.into())).await.is_err() { break; }
                }
                Some(js::WsOut::Close(code, reason)) => {
                    let frame = CloseFrame {
                        code: CloseCode::from(code),
                        reason: reason.clone().into(),
                    };
                    let _ = socket.send(Message::Close(Some(frame))).await;
                    close_code = code;
                    close_reason = reason;
                    close_was_clean = true;
                    break;
                }
                None => break,
            }
        }
    }
    if let Err(error) = sink.closed(
        ws_id,
        close_code,
        close_reason,
        close_was_clean,
    ).await {
        warn!(%error, %scope, ws_id, "outbound WebSocket close dispatch failed");
    }
    js::ws_unregister(ws_id);
    info!(%scope, ws_id, "outbound WebSocket closed");
    Ok(())
}

async fn run_ws_open(
    cx: &Arc<Ctx>,
    scope: &str,
    ws_id: u64,
    protocol: String,
) -> anyhow::Result<()> {
    match ensure_cell(cx, scope).await? {
        CellRoute::Local { tx, _activity } => {
            let (reply, rx) = tokio::sync::oneshot::channel();
            tx.send(CellJob::WsOpen {
                scope: scope.into(),
                ws_id,
                protocol,
                reply,
            })
            .map_err(|_| anyhow::anyhow!("cell channel closed"))?;
            rx.await.map_err(|_| anyhow::anyhow!("cell dropped"))?
        }
        // An outbound socket is opened from an event running on this node, so
        // its cell is local by construction. Reaching this arm means ownership
        // moved between the connect and the open, and swallowing it would
        // strand a live transport against a cell that cannot see it.
        CellRoute::Remote(_) => {
            anyhow::bail!("outbound websocket for a cell this node no longer owns")
        }
    }
}

async fn ws_task<S>(cx: Arc<Ctx>, scope: String, ws_id: u64, ws: fastwebsockets::WebSocket<S>)
where S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin
{
    use fastwebsockets::OpCode;
    let _host_websocket = HostWebSocketGuard::new(cx.clone(), &scope);
    let mut ws = ws;
    // A DO close handler may choose the response close code/reason. The
    // library's automatic echo would race and mask that application response.
    ws.set_auto_close(false);
    let mut ws = fastwebsockets::FragmentCollector::new(ws);
    let (otx, mut orx) = tokio::sync::mpsc::unbounded_channel::<js::WsOut>();
    js::ws_register(ws_id, otx);
    info!(%scope, ws_id, "ws open");
    let mut closed = false;
    let mut close_code = 1006;
    let mut close_reason = String::new();
    let mut close_was_clean = false;
    loop {
        tokio::select! {
            frame = ws.read_frame() => {
                let frame = match frame { Ok(frame) => frame, Err(_) => break };
                match frame.opcode {
                    OpCode::Text => {
                        let msg = String::from_utf8_lossy(&frame.payload).into_owned();
                        let dispatch = run_ws(&cx, &scope, ws_id, js::WsIn::Text(msg));
                        tokio::pin!(dispatch);
                        loop {
                            tokio::select! {
                                result = &mut dispatch => {
                                    if let Err(e) = result {
                                        warn!(%e, %scope, ws_id, "ws message dispatch failed");
                                        closed = true;
                                    }
                                    break;
                                }
                                out = orx.recv() => {
                                    let Some(out) = out else { closed = true; break };
                                    if !write_ws_out(&mut ws, out).await {
                                        closed = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    OpCode::Binary => {
                        let dispatch = run_ws(
                            &cx,
                            &scope,
                            ws_id,
                            js::WsIn::Binary(frame.payload.to_vec()),
                        );
                        tokio::pin!(dispatch);
                        loop {
                            tokio::select! {
                                result = &mut dispatch => {
                                    if let Err(e) = result {
                                        warn!(%e, %scope, ws_id, "ws binary dispatch failed");
                                        closed = true;
                                    }
                                    break;
                                }
                                out = orx.recv() => {
                                    let Some(out) = out else { closed = true; break };
                                    if !write_ws_out(&mut ws, out).await {
                                        closed = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    OpCode::Close => {
                        (close_code, close_reason, close_was_clean) =
                            websocket_close_details(&frame.payload);
                        break;
                    }
                    _ => {}
                }
            }
            out = orx.recv() => {
                let Some(out) = out else { break };
                if !write_ws_out(&mut ws, out).await { break; }
            }
        }
        if closed { break; }
    }
    if let Err(e) = run_ws_closed(
        &cx,
        &scope,
        ws_id,
        close_code,
        close_reason.clone(),
        close_was_clean,
    ).await {
        warn!(%e, %scope, ws_id, "ws close dispatch failed");
    }
    let mut handler_sent_close = false;
    while let Ok(out) = orx.try_recv() {
        handler_sent_close |= matches!(out, js::WsOut::Close(_, _));
        if !write_ws_out(&mut ws, out).await {
            break;
        }
    }
    if close_was_clean && !handler_sent_close {
        let response_code = if close_code == 1005 { 1000 } else { close_code };
        let _ = write_ws_out(
            &mut ws,
            js::WsOut::Close(response_code, close_reason),
        ).await;
    }
    js::ws_unregister(ws_id);
    info!(%scope, ws_id, "ws closed");
}

fn websocket_close_details(payload: &[u8]) -> (u16, String, bool) {
    match payload {
        [] => (1005, String::new(), true),
        [_] => (1002, String::new(), false),
        [first, second, reason @ ..] => match std::str::from_utf8(reason) {
            Ok(reason) => (
                u16::from_be_bytes([*first, *second]),
                reason.to_string(),
                true,
            ),
            Err(_) => (1007, String::new(), false),
        },
    }
}

async fn write_ws_out<S>(
    ws: &mut fastwebsockets::FragmentCollector<S>,
    out: js::WsOut,
) -> bool
where S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin
{
    use fastwebsockets::Frame;
    let keep_open = matches!(&out, js::WsOut::Text(_) | js::WsOut::Binary(_));
    let frame = match out {
        js::WsOut::Text(text) => Frame::text(text.into_bytes().into()),
        js::WsOut::Binary(data) => Frame::binary(data.into()),
        js::WsOut::Close(code, reason) => Frame::close(code, reason.as_bytes()),
    };
    ws.write_frame(frame).await.is_ok() && keep_open
}

/// Route a WS message into the (re-activated if hibernated) cell isolate.
async fn run_ws(
    cx: &Arc<Ctx>,
    scope: &str,
    ws_id: u64,
    data: js::WsIn,
) -> anyhow::Result<()> {
    match ensure_cell(cx, scope).await? {
        CellRoute::Local { tx, _activity } => {
            let (reply, rx) = tokio::sync::oneshot::channel();
            tx.send(CellJob::WsMessage {
                scope: scope.into(),
                ws_id,
                data,
                reply,
            })
            .map_err(|_| anyhow::anyhow!("cell channel closed"))?;
            rx.await.map_err(|_| anyhow::anyhow!("cell dropped"))?
        }
        // The socket outlived this node's ownership of the cell. Reporting
        // success here would tell the client its message was delivered while
        // dropping it, which is silent data loss on a path with no response
        // left to signal through. The caller warns and closes instead, so the
        // client reconnects and lands on the current owner. Eviction skips
        // cells with attached sockets, so this is a guard rather than a live
        // path -- and a guard that claims success cannot be observed failing.
        CellRoute::Remote(_) => {
            anyhow::bail!("websocket message for a cell this node no longer owns")
        }
    }
}

async fn run_ws_closed(
    cx: &Arc<Ctx>,
    scope: &str,
    ws_id: u64,
    code: u16,
    reason: String,
    was_clean: bool,
) -> anyhow::Result<()> {
    match ensure_cell(cx, scope).await? {
        CellRoute::Local { tx, _activity } => {
            let (reply, rx) = tokio::sync::oneshot::channel();
            tx.send(CellJob::WsClosed {
                scope: scope.into(),
                ws_id,
                code,
                reason,
                was_clean,
                reply,
            })
                .map_err(|_| anyhow::anyhow!("cell channel closed"))?;
            rx.await.map_err(|_| anyhow::anyhow!("cell dropped"))?
        }
        // A close for a cell that moved: the owner will observe the socket's
        // absence on its own. Nothing to deliver, so this one really is a
        // no-op -- but say so, rather than leaving a bare Ok.
        CellRoute::Remote(_) => Ok(()),
    }
}

pub fn run() -> anyhow::Result<()> {
    if replication::is_litestream_supervisor_invocation() {
        return replication::run_litestream_supervisor();
    }
    // Two TLS stacks are in the dependency graph (aws-lc-rs via the AWS SDK,
    // ring via reqwest/tungstenite), so rustls cannot pick a process default
    // by itself — without this, the first TLS handshake on a worker thread
    // (the presence WebSocket to https://celld.dev) panics in release builds.
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .ok();
    // Configurable worker-thread count so the thread-local-pool model can be
    // swept (CELLD_TOKIO_THREADS); defaults to tokio's num_cpus.
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if let Some(n) = std::env::var("CELLD_TOKIO_THREADS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|&value| value > 0)
    {
        builder.worker_threads(n);
    }
    builder.build()?.block_on(async_main())
}

async fn async_main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "info".into()))
        .init();
    asyncrt::set_host_handle(tokio::runtime::Handle::current());
    let settings = match startup::action_from_process()? {
        startup::Action::Help => {
            startup::print_help();
            return Ok(());
        }
        startup::Action::Version => {
            startup::print_version();
            return Ok(());
        }
        startup::Action::DiagnoseHelp => {
            startup::print_diagnose_help();
            return Ok(());
        }
        startup::Action::Diagnose { settings, peers } => {
            run_diagnostics(settings, peers).await?;
            return Ok(());
        }
        startup::Action::Connect(arguments) => {
            control_plane::handle_connect_command(arguments).await?;
            return Ok(());
        }
        startup::Action::Token(arguments) => {
            control_plane::handle_token_command(arguments).await?;
            return Ok(());
        }
        startup::Action::Credentials(arguments) => {
            control_plane::handle_credentials_command(arguments).await?;
            return Ok(());
        }
        startup::Action::Disconnect(arguments) => {
            control_plane::handle_disconnect_command(arguments).await?;
            return Ok(());
        }
        startup::Action::Deploy(arguments) => {
            run_deploy(arguments).await?;
            return Ok(());
        }
        startup::Action::Run(settings) => settings,
    };
    startup::raise_file_limit();
    // Reserve the listener before enrollment, bucket access, replication, or
    // ownership side effects. Two bare local starts therefore deterministically
    // choose distinct ports.
    let bound = startup::bind_listener(&settings).await?;
    let listen = bound.listen.to_string();
    let advertise = bound.advertise.to_string();
    let listener = bound.listener;
    println!("Reserved listener {listen} (serving begins after a deployment is loaded).");

    let installation_storage = if settings.control_plane {
        let requested_byo = settings
            .bucket
            .as_ref()
            .map(|bucket| control_plane::ByoStorageConfig {
                bucket: bucket.clone(),
                endpoint: settings.endpoint.clone(),
                region: settings.region.clone(),
            });
        control_plane::connect_on_startup_with_storage(requested_byo).await?;
        Some(control_plane::installation_storage()?)
    } else {
        None
    };
    let managed_storage = installation_storage.as_ref().and_then(|storage| match storage {
        control_plane::InstallationStorageConfig::Managed(storage) => Some(storage),
        control_plane::InstallationStorageConfig::Byo(_) => None,
    });
    let enrolled_byo = installation_storage.as_ref().and_then(|storage| match storage {
        control_plane::InstallationStorageConfig::Managed(_) => None,
        control_plane::InstallationStorageConfig::Byo(storage) => Some(storage),
    });

    let bucket = managed_storage
        .map(|storage| storage.bucket.clone())
        .or_else(|| enrolled_byo.map(|storage| storage.bucket.clone()))
        .or_else(|| settings.bucket.clone())
        .context("no storage bucket is configured")?;
    let endpoint = managed_storage
        .map(|storage| storage.endpoint.clone())
        .or_else(|| enrolled_byo.and_then(|storage| storage.endpoint.clone()))
        .or_else(|| settings.endpoint.clone());
    let region = managed_storage
        .map(|storage| storage.region.clone())
        .or_else(|| enrolled_byo.map(|storage| storage.region.clone()))
        .unwrap_or_else(|| settings.region.clone());
    let storage_credentials = managed_storage.map(|storage| {
        replication::StorageCredentials {
            access_key_id: storage.access_key_id.clone(),
            secret_access_key: storage.secret_access_key.clone(),
            session_token: storage.session_token.clone(),
        }
    });
    let c = s3(endpoint.as_deref(), &region, managed_storage, false).await;
    validate_bucket(&c, &bucket, managed_storage.is_some()).await?;
    if settings.control_plane {
        control_plane::wait_for_initial_deployment(&c, &bucket).await?;
    }
    let (manifest, src, text, asset_resolver) = load_current_deploy(&c, &bucket)
        .await
        .with_context(|| format!("load current deployment from s3://{bucket}"))?;

    // Node-lease renewal is the authority heartbeat for every cell on this
    // process. Give it a distinct HTTP client and connection pool so a cold
    // activation storm cannot strand the renewal behind ordinary ownership,
    // replica-discovery, and restore traffic. The lease runtime already has
    // its own OS thread and Tokio runtime; sharing `c` here would reconnect
    // that safety-critical lane at the transport pool.
    let lease_client = s3(endpoint.as_deref(), &region, managed_storage, true).await;

    let runtime_ready = Arc::new(AtomicBool::new(false));
    let script = manifest.script_name.clone();
    let deploy_version = manifest.version.clone();
    debug!(script = %manifest.script_name, version = %manifest.version, main = ?manifest.main_module,
           do_classes = ?manifest.do_classes, text_modules = text.len(), "loaded deploy from bucket");

    // --- per-node lease (the only heartbeat) ---
    let node = std::env::var("CELLD_NODE").unwrap_or_else(|_| random_node_session_id());
    let peer_auth = Arc::new(peer_auth::PeerAuth::new(
        peer_auth::load_or_create(&c, &bucket).await?,
        node.clone(),
    )?);
    let probe_public_key = peer_probe::install_signer()?;
    let ttl_ms: u64 = std::env::var("CELLD_TTL_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(10_000);
    let lease_mode = match std::env::var("CELLD_LAZY_NODE_LEASE").as_deref() {
        Ok("on") => ownership::LeaseLifecycleMode::Lazy,
        Ok("shadow") => ownership::LeaseLifecycleMode::Shadow,
        _ => ownership::LeaseLifecycleMode::Continuous,
    };
    let lease_linger_ms = std::env::var("CELLD_LEASE_LINGER_MS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(if settings.control_plane { 0 } else { ttl_ms });
    let lease_manager = ownership::NodeLeaseManager::start(ownership::NodeLeaseOptions {
        client: &lease_client,
        bucket: &bucket,
        node: &node,
        addr: &advertise,
        probe_public_key: &probe_public_key,
        ttl_ms,
        linger_ms: lease_linger_ms,
        mode: lease_mode,
    })
    .await?;
    let node_load = lease_manager.load_state();
    let pressure_config = pressure_config_from_environment()?;
    let pressure_ownership = PressureOwnership::from_environment()?;
    if pressure_ownership == PressureOwnership::Sticky
        && lease_mode == ownership::LeaseLifecycleMode::Lazy
    {
        anyhow::bail!(
            "CELLD_PRESSURE_OWNERSHIP=sticky requires a continuously renewed node lease"
        );
    }

    let litestream = litestream_binary()?;
    let watch = std::env::var("CELLD_WATCH").unwrap_or_else(|_| {
        std::env::temp_dir()
            .join("celld")
            .join(&node)
            .to_string_lossy()
            .into_owned()
    });
    let repl = Arc::new(replication::NodeRepl::start(
        &litestream,
        &watch,
        &bucket,
        endpoint.as_deref(),
        &region,
        storage_credentials.as_ref(),
    )?);
    let repl_health = Arc::downgrade(&repl);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(250));
        loop {
            tick.tick().await;
            let Some(repl) = repl_health.upgrade() else {
                return;
            };
            match repl.process_status() {
                Ok(None) => {}
                Ok(Some(status)) => {
                    eprintln!(
                        "SELF-FENCE: replication process exited unexpectedly: {status}"
                    );
                    std::process::exit(3);
                }
                Err(source) => {
                    eprintln!(
                        "SELF-FENCE: replication process health check failed: {source}"
                    );
                    std::process::exit(3);
                }
            }
        }
    });
    let bindings = worker_do_bindings(&manifest);
    let r2_bindings = worker_r2_bindings(&manifest);
    let ai_binding = worker_ai_binding(&manifest)
        .or_else(|| std::env::var("CELLD_AI_BINDING").ok())
        .or_else(|| std::env::var_os("CELLD_AI_URL").map(|_| "AI".to_string()));
    let vars = worker_vars(&manifest);
    let compat = worker_compat(&manifest.raw_metadata);
    let owned_cells = Arc::new(AtomicUsize::new(0));
    let owned_cell_inventory = Arc::new(Mutex::new(HashMap::new()));
    debug!(vars = vars.len(), "loaded worker variable bindings");

    js::Engine::init(); // V8 global init — ONCE per process, before any isolate

    let services = worker_services(&manifest);
    if !services.is_empty() {
        info!(services = services.len(), "loaded service bindings");
    }
    let worker_config = Arc::new(
        js::WorkerConfig::new(js::WorkerConfigOptions {
            src,
            script_name: manifest.script_name.clone(),
            do_classes: manifest.do_classes.clone(),
            bindings,
            r2_bindings,
            ai_binding,
            vars,
            node: node.clone(),
            text,
            compat,
        })
        .with_services(services.clone())
        .with_asset_binding(
            asset_resolver
                .as_ref()
                .and_then(|resolver| resolver.binding_name())
                .map(str::to_string),
        )
        // Worker Loader (Code Mode) is off unless the fleet opts in by naming
        // the binding. Experimental.
        .with_loader(
            std::env::var("CELLD_WORKER_LOADER")
                .ok()
                .filter(|name| !name.is_empty()),
        ),
    );
    let n_workers: usize = std::env::var("CELLD_WORKERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_STATELESS_WORKERS);
    let max_concurrent_activations: usize = std::env::var("CELLD_ACTIVATIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or_else(|| n_workers.min(DEFAULT_MAX_CONCURRENT_ACTIVATIONS));
    let cx = Arc::new(Ctx {
        c: c.clone(),
        repl: repl.clone(),
        http: reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .build()
            .context("build peer HTTP client")?,
        registry: Mutex::new(HashMap::new()),
        lifecycle_cells: Mutex::new(HashMap::new()),
        epochs: Mutex::new(HashMap::new()),
        hibernated_owned: Mutex::new(HashMap::new()),
        owner_cache: Mutex::new(HashMap::new()),
        node_cache: Mutex::new(HashMap::new()),
        node_unavailable_cache: Mutex::new(HashMap::new()),
        node_resolving: Mutex::new(HashMap::new()),
        capacity_peers: tokio::sync::Mutex::new(
            ownership::CapacityPeerCache::default(),
        ),
        node_ttl_ms: ttl_ms,
        remote_aborts: Mutex::new(HashMap::new()),
        activating: Mutex::new(HashMap::new()),
        activation_slots: Arc::new(tokio::sync::Semaphore::new(
            max_concurrent_activations,
        )),
        resident_reservations: Arc::new(AtomicUsize::new(0)),
        resident_capacity_changed: Arc::new(tokio::sync::Notify::new()),
        owed_activations: Mutex::new(std::collections::HashSet::new()),
        bucket: bucket.clone(), endpoint: endpoint.clone(), region: region.clone(),
        storage_credentials: storage_credentials.clone(), litestream: litestream.clone(),
        node: node.clone(),
        advertise: advertise.clone(),
        peer_auth,
        worker_config,
        owned_cells: owned_cells.clone(),
        owned_cell_inventory: owned_cell_inventory.clone(),
        lease_manager: lease_manager.clone(),
        node_load,
        admission_pressure: AtomicBool::new(false),
        admission_pressure_config: PressureConfig {
            resident_high: None,
            ..pressure_config
        },
        shedding: AtomicBool::new(false),
        pressure_config,
        pressure_ownership,
        lazy_leases: lease_mode == ownership::LeaseLifecycleMode::Lazy,
        websocket_counts: Mutex::new(HashMap::new()),
        outbound_websockets: Mutex::new(HashMap::new()),
    });

    // Worker pool (stateless tier — scale with CELLD_WORKERS).
    let workers = Arc::new(WorkerPool::new(
        cx.worker_config.clone(),
        n_workers,
        &node,
        &region,
    ));
    debug!(n_workers, max_concurrent_activations, "worker pool up");

    // Service-binding co-hosting. Every script reachable through [[services]]
    // is loaded from the same bucket and gets its own pool in this process, so
    // `env.NAME.fetch()` is a local isolate call rather than a network hop.
    // Resolution is breadth-first over the whole graph: a target's own service
    // bindings are resolved too, otherwise `env.OTHER` inside it would be
    // silently undefined. `visited` breaks cycles (A->B->A is legal at runtime
    // — it is just a nested fetch — but must not loop while loading) and
    // CELLD_MAX_COHOSTED bounds how many isolates one manifest can pull in.
    let max_cohosted: usize = std::env::var("CELLD_MAX_COHOSTED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);
    let mut service_pools: HashMap<String, Arc<WorkerPool>> = HashMap::new();
    let mut asset_resolvers: HashMap<String, assets::AssetResolver> = HashMap::new();
    if let Some(resolver) = asset_resolver.clone() {
        asset_resolvers.insert(manifest.script_name.clone(), resolver);
    }
    let mut visited: std::collections::BTreeSet<String> =
        [manifest.script_name.clone()].into_iter().collect();
    let mut queue: std::collections::VecDeque<String> = services
        .iter()
        .map(|(_, script, _)| script.clone())
        .collect();
    while let Some(target) = queue.pop_front() {
        if target == manifest.script_name {
            // A self-binding needs no second config; reuse the primary pool.
            service_pools.insert(target, workers.clone());
            continue;
        }
        if !visited.insert(target.clone()) {
            continue; // already co-hosted, or currently being loaded
        }
        if service_pools.len() >= max_cohosted {
            warn!(script = %target, max = max_cohosted,
                "service binding co-hosting limit reached; calls will fail");
            continue;
        }
        let (target_manifest, target_src, target_text, target_assets) =
            match load_deploy(&c, &bucket, &target).await {
                Ok(loaded) => loaded,
                Err(error) => {
                    warn!(%error, script = %target,
                        "service binding target failed to load; calls will fail");
                    continue;
                }
            };
        // A co-hosted target keeps its own bindings. Handing it empty vectors
        // would silently strip its Durable Object namespaces and R2 buckets,
        // so `env.NS` inside it would be undefined.
        let target_services = worker_services(&target_manifest);
        for (_, next, _) in &target_services {
            queue.push_back(next.clone());
        }
        let target_config = Arc::new(
            js::WorkerConfig::new(js::WorkerConfigOptions {
                src: target_src,
                script_name: target_manifest.script_name.clone(),
                do_classes: target_manifest.do_classes.clone(),
                bindings: worker_do_bindings(&target_manifest),
                r2_bindings: worker_r2_bindings(&target_manifest),
                ai_binding: worker_ai_binding(&target_manifest),
                vars: worker_vars(&target_manifest),
                node: node.clone(),
                text: target_text,
                compat: crate::worker_compat(&target_manifest.raw_metadata),
            })
            .with_services(target_services)
            .with_asset_binding(
                target_assets
                    .as_ref()
                    .and_then(|resolver| resolver.binding_name())
                    .map(str::to_string),
            ),
        );
        let pool = Arc::new(WorkerPool::new(target_config, 2, &node, &region));
        info!(script = %target, "co-hosted service binding target");
        if let Some(resolver) = target_assets {
            asset_resolvers.insert(target.clone(), resolver);
        }
        service_pools.insert(target, pool);
    }

    let (atx, mut arx) = tokio::sync::mpsc::unbounded_channel::<js::AssetCallReq>();
    js::set_asset_call_tx(atx);
    tokio::spawn(async move {
        while let Some(req) = arx.recv().await {
            let Some(resolver) = asset_resolvers.get(&req.script).cloned() else {
                let _ = req.reply.send(Err(anyhow::anyhow!(
                    "no asset resolver for script {}",
                    req.script
                )));
                continue;
            };
            tokio::spawn(async move {
                let response = resolver
                    .binding_response(&req.url, &req.method, &req.headers)
                    .await;
                let _ = req.reply.send(response);
            });
        }
    });

    // Entrypoint RPC over a service binding, routed to the same pools.
    let (rtx, mut rrx) = tokio::sync::mpsc::unbounded_channel::<js::SvcRpcReq>();
    js::set_svc_rpc_tx(rtx);
    let rpc_pools = service_pools.clone();
    tokio::spawn(async move {
        while let Some(req) = rrx.recv().await {
            let Some(pool) = rpc_pools.get(&req.script).cloned() else {
                let _ = req.reply.send(Err(anyhow::anyhow!(
                    "no service binding target for script {}", req.script)));
                continue;
            };
            let (reply, result) = tokio::sync::oneshot::channel();
            if pool.send(WorkerJob::Rpc {
                entrypoint: req.entrypoint, method: req.method,
                args: req.args, reply,
            }).is_err() {
                let _ = req.reply.send(Err(anyhow::anyhow!("service worker gone")));
                continue;
            }
            tokio::spawn(async move {
                let _ = req.reply.send(match result.await {
                    Ok(value) => value,
                    Err(error) => Err(anyhow::anyhow!("service dropped: {error}")),
                });
            });
        }
    });

    let (stx, mut srx) = tokio::sync::mpsc::unbounded_channel::<js::SvcCallReq>();
    js::set_svc_call_tx(stx);
    tokio::spawn(async move {
        while let Some(req) = srx.recv().await {
            let Some(pool) = service_pools.get(&req.script).cloned() else {
                let _ = req.reply.send(Err(anyhow::anyhow!(
                    "no service binding target for script {}", req.script)));
                continue;
            };
            let (reply, result) = tokio::sync::oneshot::channel();
            // An id makes the target's request abortable from this thread.
            let request_id = js::next_request_id();
            if pool.send(WorkerJob::Fetch {
                queued_at: Instant::now(),
                url: req.url, method: req.method, body: req.body,
                headers: req.headers, request_id: Some(request_id), reply,
            }).is_err() {
                let _ = req.reply.send(Err(anyhow::anyhow!("service worker gone")));
                continue;
            }
            let cancel = req.cancel;
            let caller = req.reply;
            tokio::spawn(async move {
                // Stop waiting as soon as the caller's signal aborts. The
                // target isolate is not yet told to abort its own request.
                let response = match cancel {
                    Some(cancel) => tokio::select! {
                        settled = result => match settled {
                            Ok(response) => response,
                            Err(error) => Err(anyhow::anyhow!(
                                "service dropped: {error}")),
                        },
                        _ = cancel => {
                            // Reaches the target's request.signal, so a
                            // hanging handler stops rather than running on.
                            js::abort_request(request_id);
                            Err(anyhow::anyhow!("The client has disconnected"))
                        },
                    },
                    None => match result.await {
                        Ok(response) => response,
                        Err(error) => Err(anyhow::anyhow!(
                            "service dropped: {error}")),
                    },
                };
                let _ = caller.send(response);
            });
        }
    });

    // DO-call router: the Worker isolates' `__do_call` op hands off here.
    let (dtx, mut drx) = tokio::sync::mpsc::unbounded_channel::<js::DoCallReq>();
    js::set_do_call_tx(dtx);
    let rcx = cx.clone();
    tokio::spawn(async move {
        while let Some(req) = drx.recv().await {
            let cx_ = rcx.clone();
            tokio::spawn(async move {
                let res = run_do_cancellable(
                    &cx_,
                    DoRequest {
                        scope: &req.scope,
                        name: req.name.as_deref(),
                        url: &req.url,
                        method: &req.method,
                        body: &req.body,
                        headers: &req.headers,
                        request_id: req.request_id,
                    },
                    req.cancel,
                )
                .await;
                let _ = req.reply.send(res);
            });
        }
    });

    let (rtx, mut rrx) = tokio::sync::mpsc::unbounded_channel::<js::RpcCallReq>();
    js::set_rpc_call_tx(rtx);
    let rpcx = cx.clone();
    tokio::spawn(async move {
        while let Some(req) = rrx.recv().await {
            let cx_ = rpcx.clone();
            tokio::spawn(async move {
                let res = run_rpc(
                    &cx_, &req.scope, req.name.as_deref(), &req.method, req.args,
                ).await;
                let _ = req.reply.send(res);
            });
        }
    });

    let (wtx, mut wrx) = tokio::sync::mpsc::unbounded_channel::<js::OutboundWsReq>();
    js::set_outbound_ws_tx(wtx);
    let wcx = cx.clone();
    tokio::spawn(async move {
        while let Some(req) = wrx.recv().await {
            let cx = wcx.clone();
            tokio::spawn(async move {
                let sink = match req.pull {
                    Some(pull) => WsSink::Isolate(pull),
                    None => WsSink::Cell { cx: cx.clone(), scope: req.scope },
                };
                if let Err(error) = outbound_ws_task(cx, OutboundWs {
                    sink,
                    ws_id: req.id,
                    url: req.url,
                    protocols: req.protocols,
                    extra_headers: req.headers,
                    want_response: req.want_response,
                    reply: req.reply,
                }).await {
                    warn!(%error, "outbound WebSocket failed");
                }
            });
        }
    });

    // Eviction sweep: hibernate cells idle past the threshold — shut the isolate
    // thread, release replication. A later request re-activates (fresh epoch).
    let idle_evict_s: u64 = std::env::var("CELLD_IDLE_EVICT_S")
        .ok().and_then(|v| v.parse().ok()).unwrap_or(300);
    let max_concurrent_hibernations: usize = std::env::var("CELLD_HIBERNATIONS")
        .ok().and_then(|v| v.parse().ok()).filter(|n| *n > 0)
        .unwrap_or(DEFAULT_MAX_CONCURRENT_HIBERNATIONS);
    let hibernation_slots = Arc::new(tokio::sync::Semaphore::new(
        max_concurrent_hibernations,
    ));
    let hibernations_in_flight = Arc::new(AtomicUsize::new(0));

    // Capacity sampling is a control loop, not eviction bookkeeping. Keep it
    // independent of the per-cell maintenance sweep below: alarm
    // reconciliation and replica checks perform remote I/O and can take
    // minutes at fleet density, while admission and lease advertisements need
    // a fresh host sample every five seconds.
    let load_cx = cx.clone();
    let load_hibernations_in_flight = hibernations_in_flight.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut process_load = ProcessLoadSampler::default();
        loop {
            tick.tick().await;
            let resident_cells = load_cx
                .owned_cells
                .load(Ordering::Relaxed)
                .saturating_sub(load_hibernations_in_flight.load(Ordering::Relaxed));
            let host_websockets = load_cx
                .websocket_counts
                .lock()
                .unwrap()
                .values()
                .copied()
                .sum();
            let rss_bytes = resident_memory_bytes();
            let cpu_percent_x100 = process_load.sample_cpu_percent_x100();
            let open_fds = open_file_descriptors();
            let fd_limit = file_descriptor_limit();
            let was_shedding = load_cx.shedding.load(Ordering::Acquire);
            let pressure = load_cx.pressure_config.state(
                Load { resident_cells, rss_bytes, cpu_percent_x100 },
                was_shedding,
            );
            // Residency is deliberately excluded: `reserve_resident` counts
            // it exactly. Latching a sampled view of a number we already know
            // only delays recovery by up to a full sampling period.
            let admission = load_cx.admission_pressure_config.state(
                Load { resident_cells: 0, rss_bytes, cpu_percent_x100 },
                load_cx.admission_pressure.load(Ordering::Acquire),
            );
            load_cx.admission_pressure
                .store(admission.admission_blocked, Ordering::Release);
            load_cx.shedding.store(pressure.shedding, Ordering::Release);
            load_cx.node_load.update(ownership::NodeLoadSample {
                resident_cells,
                host_websockets,
                rss_bytes,
                cpu_percent_x100,
                open_fds,
                fd_limit,
                pressured: pressure.admission_blocked,
            });
            if pressure.shedding && !was_shedding {
                warn!(
                    reason = pressure.trigger.unwrap_or("hysteresis"),
                    resident_cells,
                    rss_bytes,
                    cpu_percent_x100,
                    "node entered capacity pressure"
                );
            } else if !pressure.shedding && was_shedding {
                load_cx.resident_capacity_changed.notify_waiters();
                info!(
                    resident_cells,
                    rss_bytes,
                    cpu_percent_x100,
                    "node capacity pressure relieved"
                );
            }
        }
    });

    let scx = cx.clone();
    let flusher = std::sync::Arc::new(wake::WakeFlusher::new());
    // Tier-2 wake runs on its own fast tick: re-activate hibernated cells
    // whose flushed alarm is due. Sharing the 5-second maintenance cycle
    // queued due cells behind entry maintenance and eviction — measured as
    // 12-25 second cycles and a 52-second wake-lateness p99 during
    // generation-sized re-arm waves. Activation is the normal CAS + restore
    // + spawn path; the spawned cell's own scheduler fires the alarm, and
    // the flusher's next pass deletes the entry once the alarm is consumed.
    let wake_cx = cx.clone();
    let tier2_flusher = flusher.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Guards pin woken cells past one eviction tick (5 s), so the sweep
        // cannot reap a cell in the same window it woke, before its first
        // alarm poll ever runs.
        let mut wake_guards: Vec<(Instant, CellRoute)> = Vec::new();
        loop {
            tick.tick().await;
            wake_guards.retain(|(held, _)| held.elapsed() < Duration::from_secs(6));
            let due = tier2_flusher.due_cells(now_ms());
            if due.is_empty() {
                continue;
            }
            let batch_started = Instant::now();
            let candidates = due.len();
            let results = run_alarm_wake_batch(due, {
                let wake_cx = wake_cx.clone();
                move |scope| {
                    let wake_cx = wake_cx.clone();
                    async move {
                        if wake_cx.registry.lock().unwrap().contains_key(&scope) {
                            // Resident again; its own scheduler owns it.
                            return (scope, None);
                        }
                        let result = ensure_cell(&wake_cx, &scope).await;
                        (scope, Some(result))
                    }
                }
            })
            .await;
            let mut local = 0usize;
            let mut remote = 0usize;
            let mut resident = 0usize;
            let mut errors = 0usize;
            for (scope, result) in results {
                match result {
                    None => resident += 1,
                    Some(Ok(route @ CellRoute::Local { .. })) => {
                        js::touch(&scope);
                        wake_guards.push((Instant::now(), route));
                        local += 1;
                        info!(%scope, "alarm wake activated hibernated cell");
                    }
                    Some(Ok(CellRoute::Remote(_))) => {
                        tier2_flusher.forget(&scope);
                        remote += 1;
                    }
                    Some(Err(e)) => {
                        errors += 1;
                        warn!(%scope, error = %e, "alarm wake failed");
                    }
                }
            }
            info!(
                event = "alarm_wake_batch",
                node = %wake_cx.node,
                region = %wake_cx.region,
                runtime_version = env!("CARGO_PKG_VERSION"),
                candidates,
                local,
                remote,
                resident,
                errors,
                concurrency = ALARM_WAKE_ACTIVATION_CONCURRENCY,
                elapsed_ms = batch_started.elapsed().as_millis() as u64,
                "alarm wake batch completed"
            );
        }
    });
    // Arm-time wake durability: a committed alarm that tightens the durable
    // bound PUTs its entry before the app's setAlarm/transaction ack.
    js::set_arm_gate(js::ArmGate {
        c: cx.c.clone(),
        bucket: cx.bucket.clone(),
        flusher: flusher.clone(),
    });
    let sweep_flusher = flusher.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut unreplicated_epochs = std::collections::HashMap::<String, u64>::new();
        let mut cache_prune_ticks: u32 = 0;
        // `0` disables the bound for operators who would rather spend disk
        // than ever pay a remote restore.
        let local_cache_max_bytes = match std::env::var("CELLD_LOCAL_CACHE_MAX_BYTES") {
            Ok(value) => value.parse::<u64>().ok().filter(|bytes| *bytes > 0),
            Err(_) => Some(DEFAULT_LOCAL_CACHE_MAX_BYTES),
        };
        loop {
            tick.tick().await;
            let load = scx.node_load.snapshot();
            // The sampler is intentionally independent of this potentially
            // slow R2 sweep, so its resident count can be one tick old. Use
            // the live counters for eviction budgeting and retain only its
            // host resource samples.
            let resident_cells = scx
                .owned_cells
                .load(Ordering::Relaxed)
                .saturating_sub(hibernations_in_flight.load(Ordering::Relaxed));
            let pressure_load = Load {
                resident_cells,
                rss_bytes: load.rss_bytes,
                cpu_percent_x100: load.cpu_percent_x100,
            };
            let was_shedding = scx.shedding.load(Ordering::Acquire);
            let pressure_reason = was_shedding
                .then(|| scx.pressure_config.shedding_trigger(pressure_load, true))
                .flatten();
            let pressure_target = pressure_reason.map(|reason| {
                scx.pressure_config.release_target(resident_cells, reason)
            });
            // Every committed alarm change reaches the bucket within one tick,
            // so a wake hint survives fence, crash, and deploy. This reads only
            // the lock-free next_alarm_ms mirror, never the request path.
            let cycle_started = Instant::now();
            let alarms: Vec<(String, i64, i64, Arc<std::sync::atomic::AtomicI64>)> =
                scx.registry.lock().unwrap()
                .iter()
                .map(|(s, c)| (
                    s.clone(),
                    c.next_alarm_ms.load(Ordering::Acquire),
                    c.activation_alarm_ms.load(Ordering::Acquire),
                    c.activation_alarm_ms.clone(),
                ))
                .collect();
            // A generation-sized wave re-arms hundreds of cells at once, and
            // one moved entry is two R2 ops. Serial per-cell awaits measured
            // 6-11 s of flush ahead of everything behind this loop; run the
            // per-cell maintenance with bounded concurrency instead.
            let sync_wait_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            {
                use futures_util::stream::{self, StreamExt};
                let mut passes = stream::iter(alarms)
                    .map(|(scope, ms, activated_with, activation_latch)| {
                        let scx = scx.clone();
                        let sweep_flusher = sweep_flusher.clone();
                        let sync_wait_count = sync_wait_count.clone();
                        async move {
                            // A cell this process did not hibernate carries
                            // an entry it has no record of. Adopt it from the
                            // alarm the cell restored with, so a consume
                            // deletes it instead of leaving it to be
                            // re-LISTed and re-woken forever. The decision is
                            // sans-IO (`celld_logic::wake::should_adopt_hint`).
                            if celld_logic::wake::should_adopt_hint(
                                sweep_flusher.tracks(&scope),
                                activated_with,
                            ) {
                                sweep_flusher.adopt(&scope, activated_with);
                            }
                            // A consumed alarm's entry may be deleted only
                            // once the consuming commit is replicated:
                            // sync-wait the cell's db first. Losing that
                            // commit after the delete would leave replicated
                            // truth armed with no entry — an unrecoverable
                            // lost wake. A failed wait defers the delete one
                            // tick; an absent control socket falls back to
                            // the historical ungated delete.
                            let consume_durable = if ms < 0 && sweep_flusher.tracks(&scope) {
                                let epoch = scx
                                    .epochs
                                    .lock()
                                    .unwrap()
                                    .get(&scope)
                                    .copied()
                                    .unwrap_or(1);
                                sync_wait_count.fetch_add(1, Ordering::Relaxed);
                                match scx
                                    .repl
                                    .sync_wait(&scope, epoch, Duration::from_secs(10))
                                    .await
                                {
                                    replication::SyncWait::Durable => true,
                                    replication::SyncWait::Unsupported => true,
                                    replication::SyncWait::Failed => false,
                                }
                            } else {
                                true
                            };
                            sweep_flusher
                                .reconcile(&scx.c, &scx.bucket, &scope, ms, consume_durable)
                                .await;
                            // The activation hint is consumed exactly once.
                            // Left latched, every later cycle re-adopts a
                            // phantom entry for this resident cell and
                            // re-consumes it — a sync-wait plus an R2 delete
                            // per cell per cycle, forever while resident. The
                            // decision is sans-IO (`celld_logic::wake::
                            // hint_consumed`), pinned by the op-quiescence
                            // DST invariant.
                            if celld_logic::wake::hint_consumed(
                                ms,
                                sweep_flusher.tracks(&scope),
                            ) {
                                activation_latch.store(-1, Ordering::Release);
                            }
                        }
                    })
                    .buffer_unordered(16);
                while passes.next().await.is_some() {}
            }
            let sync_waits = sync_wait_count.load(Ordering::Relaxed);
            let flush_ms = cycle_started.elapsed().as_millis() as u64;
            // Retry activations we owe. A cell whose ownership CAS won and
            // whose activation then failed is invisible to every
            // reconciliation path — no wake entry for the waker, and a live
            // lease hides it from dead-node reconciliation — so nothing but
            // this retries it. Without this retry, reconciliation leaves
            // alarms stranded under a live owner.
            let owed: Vec<String> = {
                let owed = scx.owed_activations.lock().unwrap();
                owed.iter()
                    .filter(|s| !scx.registry.lock().unwrap().contains_key(*s))
                    .cloned()
                    .collect()
            };
            for scope in owed {
                match ensure_cell(&scx, &scope).await {
                    Ok(CellRoute::Local { .. }) => {
                        info!(%scope, "activation retry succeeded");
                    }
                    Ok(CellRoute::Remote(_)) => {
                        // someone else owns it now; the debt is not ours
                        scx.owed_activations.lock().unwrap().remove(&scope);
                    }
                    Err(e) => warn!(%scope, error = %e, "activation retry failed"),
                }
            }
            let owed_done_ms = cycle_started.elapsed().as_millis() as u64;
            // Bound the local hibernation cache. Preserving a replica per
            // hibernated cell is what makes a same-node wake a rename
            // instead of 46 storage round trips, but it grows with every
            // cell this node has ever hosted. Eviction is always safe: the
            // cache duplicates bucket state, so its only cost is a future
            // restore. Runs on a slow multiple of the maintenance tick —
            // this walks the tree.
            cache_prune_ticks = cache_prune_ticks.saturating_add(1);
            if let Some(max_bytes) = local_cache_max_bytes {
                if cache_prune_ticks >= CACHE_PRUNE_EVERY_TICKS {
                    cache_prune_ticks = 0;
                    let repl = scx.repl.clone();
                    let pruned = tokio::task::spawn_blocking(move || {
                        repl.prune_local_cache(max_bytes)
                    })
                    .await;
                    if let Ok((kept, evicted, bytes)) = pruned {
                        if evicted > 0 {
                            info!(
                                event = "local_cache_pruned",
                                node = %scx.node,
                                kept,
                                evicted,
                                bytes,
                                max_bytes,
                                "pruned least-recently-used hibernation caches"
                            );
                        }
                    }
                }
            }
            // Attribution for maintenance stalls. Quiet cycles stay
            // silent; the tier-2 wake batch runs on its own fast tick and
            // reports its own timing.
            if sync_waits > 0 || flush_ms > 250 {
                info!(
                    event = "maintenance_cycle_timing",
                    node = %scx.node,
                    tracked_alarm_cells = sweep_flusher.due_cells(i64::MAX).len(),
                    sync_waits,
                    flush_ms,
                    owed_ms = owed_done_ms.saturating_sub(flush_ms),
                    cycle_ms = cycle_started.elapsed().as_millis() as u64,
                    "maintenance cycle timing"
                );
            }
            let mut scopes: Vec<(String, u64)> = scx
                .registry
                .lock()
                .unwrap()
                .keys()
                .map(|scope| (scope.clone(), js::idle_secs(scope).unwrap_or(0)))
                .collect();
            scopes.sort_by_key(|entry| std::cmp::Reverse(entry.1));
            // The eviction budget is sans-IO (`celld_logic::evict::PressureBudget`);
            // this sweep is its executor. The reserve (hibernation concurrency)
            // lets a pinned, busy, or stale candidate be skipped without starving
            // the pass; the real cut is spent only when a cell is actually removed.
            let mut pressure_budget = pressure_target.map(|target| {
                celld_logic::evict::PressureBudget::new(
                    resident_cells,
                    target,
                    max_concurrent_hibernations,
                )
            });
            // Phase 1 is sans-IO (`celld_logic::evict::plan_candidates`); the
            // executor gathers the snapshot under its locks, and this selects
            // candidates and nominates against the budget.
            let cells: Vec<celld_logic::evict::CellState> = scopes
                .iter()
                .map(|(scope, idle)| celld_logic::evict::CellState {
                    scope: scope.clone(),
                    idle_s: *idle,
                    has_regular_websocket: js::has_regular_websocket(scope),
                    websocket_count: scx
                        .websocket_counts
                        .lock()
                        .unwrap()
                        .get(scope)
                        .copied()
                        .unwrap_or(0),
                    epoch: scx.epochs.lock().unwrap().get(scope).copied().unwrap_or(1),
                })
                .collect();
            let plan = celld_logic::evict::plan_candidates(
                &cells,
                idle_evict_s,
                scx.lazy_leases,
                scx.pressure_ownership == PressureOwnership::Release,
                &mut pressure_budget,
            );
            let pressure_candidates = plan.pressure_candidates;
            let replica_candidates = plan.replica_candidates;
            // Replica verification is remote I/O. Running it inline made the
            // semaphore below ineffective and serialized an entire pressure
            // sweep to one R2 LIST at a time.
            let mut replica_checks: HashMap<String, (u64, bool)> =
                check_hibernation_replicas(
                    &scx,
                    replica_candidates,
                    max_concurrent_hibernations,
                ).await.into_iter()
                    .map(|(scope, epoch, replicated)| (scope, (epoch, replicated)))
                    .collect();
            for (scope, idle) in scopes {
                let Some((checked_epoch, replicated)) = replica_checks.remove(&scope) else {
                    continue;
                };
                let pressure_evict = pressure_candidates.contains(&scope)
                    && pressure_budget.as_ref().is_some_and(|b| b.may_evict());
                // Regular WebSockets retain JS listener closures and pin the
                // actor. Hibernatable sockets normally live in host metadata
                // and allow isolate eviction.
                if js::has_regular_websocket(&scope) { continue; }
                // A live host transport cannot move with ownership. Ordinary
                // idle hibernation may retain it on a continuously leased node.
                // Pressure can do the same in sticky-cache mode; a release
                // policy must instead choose another cell whose transport can
                // move with ownership.
                let pressure_handoff =
                    pressure_evict && scx.pressure_ownership == PressureOwnership::Release;
                // Not redundant with the same condition in `plan_candidates`.
                // The plan is built before `check_hibernation_replicas`, which
                // is remote I/O, so a socket attached during that window would
                // otherwise ride a stale plan into a handoff. This reads the
                // live count at the moment of eviction.
                if (scx.lazy_leases || pressure_handoff)
                    && scx
                        .websocket_counts
                        .lock()
                        .unwrap()
                        .get(&scope)
                        .is_some_and(|count| *count > 0)
                {
                    continue;
                }
                let latch = scx
                    .activating
                    .lock()
                    .unwrap()
                    .entry(scope.clone())
                    .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                    .clone();
                let cleanup_slot = hibernation_slots
                    .clone()
                    .acquire_owned()
                    .await
                    .expect("hibernation semaphore closed");
                let activation_guard = latch.lock_owned().await;
                if !scx.registry.lock().unwrap().contains_key(&scope) {
                    continue;
                }
                let epoch = scx.epochs.lock().unwrap().get(&scope).copied().unwrap_or(1);
                // Never delete local state the bucket does not hold. On a
                // healthy node this LIST always passes; when replication is
                // broken (wedged litestream, credentials, mac sync defect)
                // the cell stays resident instead of becoming unrestorable.
                // The decision is sans-IO (`celld_logic::evict::replica_gate`);
                // this sweep is its executor, holding the per-epoch warn dedup.
                use celld_logic::evict::ReplicaGate;
                match celld_logic::evict::replica_gate(epoch, checked_epoch, replicated) {
                    ReplicaGate::StaleCheck => continue,
                    ReplicaGate::Unreplicated => {
                        if unreplicated_epochs.get(&scope) != Some(&epoch) {
                            warn!(
                                %scope,
                                epoch,
                                "hibernate blocked: no bucket replica for epoch; repeated retries suppressed"
                            );
                            unreplicated_epochs.insert(scope.clone(), epoch);
                        }
                        continue;
                    }
                    ReplicaGate::Durable => {}
                }
                unreplicated_epochs.remove(&scope);
                let (cell, current_idle) = {
                    let mut registry = scx.registry.lock().unwrap();
                    let Some(cell) = registry.get(&scope) else {
                        continue;
                    };
                    // An armed alarm keeps its cell resident unless wake
                    // mode is on, the alarm is beyond the stay-resident
                    // break-even, and its wake entry is DURABLE in the bucket
                    // (fail closed: no entry, no eviction). Shadow mode
                    // records what the pin costs instead.
                    let next_alarm_ms = cell.next_alarm_ms.load(Ordering::Acquire);
                    // The decision is sans-IO (`celld_logic::evict::alarm_pin`);
                    // this sweep is its executor, sampling the alarm clock and
                    // performing the durable-coverage GET the gate reads.
                    let covered =
                        next_alarm_ms >= 0 && sweep_flusher.covered(&scope, next_alarm_ms);
                    let pin = celld_logic::evict::alarm_pin(
                        next_alarm_ms,
                        now_ms(),
                        wake::resident_ms(),
                        covered,
                    );
                    if pin == celld_logic::evict::AlarmPin::Hold {
                        sweep_flusher.log_pinned(&scope, next_alarm_ms, idle);
                        continue;
                    }
                    // Atomically exclude queued, running, waitUntil, RPC, and
                    // WebSocket events before removing the routing entry.
                    if !cell.activity.begin_close() {
                        continue;
                    }
                    // The sweep captured `idle` before replica verification,
                    // which can take minutes at fleet density. A request may
                    // have completed while that remote I/O was in flight:
                    // once close exclusion is held, re-read its last touch and
                    // abort ordinary eviction if the cell became active.
                    // Pressure eviction intentionally ignores recency.
                    let current_idle = js::idle_secs(&scope).unwrap_or(0);
                    // Post-close is sans-IO (`decide_post_close`): re-read idle
                    // and the alarm AFTER winning the close — the cell may have
                    // become active or re-armed during the replica I/O, and
                    // evicting on stale coverage invites a stale-truth refire.
                    let alarm_unchanged =
                        cell.next_alarm_ms.load(Ordering::Acquire) == next_alarm_ms;
                    if celld_logic::evict::decide_post_close(
                        current_idle,
                        idle_evict_s,
                        pressure_evict,
                        alarm_unchanged,
                    ) == celld_logic::evict::PostClose::Abort
                    {
                        cell.activity.abort_close();
                        continue;
                    }
                    (
                        registry
                            .remove(&scope)
                            .expect("cell inspected under the same registry lock"),
                        current_idle,
                    )
                };
                if pressure_evict {
                    if let Some(b) = pressure_budget.as_mut() {
                        b.commit();
                    }
                }
                hibernations_in_flight.fetch_add(1, Ordering::Relaxed);
                scx.owner_cache.lock().unwrap().remove(&scope);
                // Leaving residency: drop the per-scope lifecycle truth so it
                // never goes stale. Re-activation rebuilds it in finish_activation.
                scx.lifecycle_cells.lock().unwrap().remove(&scope);
                let _ = cell.tx.send(CellJob::Shutdown);
                drop(cell);
                let cleanup_cx = scx.clone();
                let cleanup_in_flight = hibernations_in_flight.clone();
                tokio::spawn(async move {
                    let _slot = cleanup_slot;
                    let _activation_guard = activation_guard;
                    // Preserve the local replica unless this eviction hands
                    // ownership away. Idle hibernation keeps ownership, so
                    // the next activation on this node can rename the file
                    // into place instead of paying a full remote restore —
                    // measured at 46 sequential storage round trips.
                    let preserve_local = !pressure_evict
                        || cleanup_cx.pressure_ownership == PressureOwnership::Sticky;
                    cleanup_cx.repl.hibernate(&scope, epoch, preserve_local).await;
                    let released = if pressure_evict
                        && cleanup_cx.pressure_ownership == PressureOwnership::Release
                    {
                        match ownership::relinquish_cell(
                            &cleanup_cx.c,
                            &cleanup_cx.bucket,
                            &scope,
                            &cleanup_cx.node,
                            epoch,
                        )
                        .await
                        {
                            Ok(true) => true,
                            Ok(false) => {
                                warn!(
                                    %scope,
                                    epoch,
                                    "pressure shed lost ownership CAS after hibernation"
                                );
                                false
                            }
                            Err(error) => {
                                warn!(
                                    %error,
                                    %scope,
                                    epoch,
                                    "pressure shed could not release ownership after hibernation"
                                );
                                false
                            }
                        }
                    } else {
                        false
                    };
                    if pressure_evict {
                        cleanup_cx.node_load.record_shed();
                    }
                    if pressure_evict
                        && cleanup_cx.pressure_ownership == PressureOwnership::Sticky
                    {
                        cleanup_cx
                            .hibernated_owned
                            .lock()
                            .unwrap()
                            .insert(scope.clone(), epoch);
                    }
                    cleanup_cx.lease_manager.release_cell();
                    cleanup_cx.owned_cell_inventory.lock().unwrap().remove(&scope);
                    cleanup_cx.owned_cells.fetch_sub(1, Ordering::Relaxed);
                    cleanup_cx.resident_capacity_changed.notify_waiters();
                    cleanup_in_flight.fetch_sub(1, Ordering::Relaxed);
                    if released {
                        info!(
                            %scope,
                            epoch,
                            idle_s = idle,
                            "hibernated and released under pressure"
                        );
                    } else if pressure_evict {
                        info!(
                            %scope,
                            epoch,
                            idle_s = idle,
                            "hibernated under pressure; ownership remains sticky"
                        );
                    } else {
                        info!(%scope, idle_s = current_idle, "hibernated (idle)");
                    }
                });
            }
        }
    });

    // Drain on SIGTERM so every armed resident cell's wake entry reaches the
    // bucket before the process dies and a deploy or stop cannot orphan an
    // alarm. Installed only when wake mode is active.
    let dcx = cx.clone();
    let dflusher = flusher.clone();
    tokio::spawn(async move {
        let mut term = match tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::terminate(),
        ) {
            Ok(term) => term,
            Err(e) => { warn!(%e, "sigterm drain unavailable"); return; }
        };
        term.recv().await;
        let alarms: Vec<(String, i64)> = dcx.registry.lock().unwrap()
            .iter()
            .map(|(s, c)| (s.clone(), c.next_alarm_ms.load(Ordering::Acquire)))
            .collect();
        for (scope, ms) in &alarms {
            // Same replication gate as the sweep: deleting a consumed
            // alarm's entry with the consuming commit unreplicated at
            // process exit is a lost wake.
            let consume_durable = if *ms < 0 && dflusher.tracks(scope) {
                let epoch =
                    dcx.epochs.lock().unwrap().get(scope).copied().unwrap_or(1);
                !matches!(
                    dcx.repl.sync_wait(scope, epoch, Duration::from_secs(10)).await,
                    replication::SyncWait::Failed
                )
            } else {
                true
            };
            dflusher.reconcile(&dcx.c, &dcx.bucket, scope, *ms, consume_durable).await;
        }
        info!(cells = alarms.len(), "wake entries drained; exiting on SIGTERM");
        std::process::exit(0);
    });

    // One leased waker per fleet lists due wake buckets each tick and revives
    // cells with no live owner. The lease is advisory: it avoids every node
    // polling, while concurrent wakers still race activation CAS harmlessly.
    let wcx = cx.clone();
    let wake_flusher = flusher.clone();
    tokio::spawn(async move {
        let tick_ms: u64 = std::env::var("CELLD_WAKER_TICK_MS").ok()
            .and_then(|v| v.parse().ok()).unwrap_or(60_000);
        let lease_ttl_ms = tick_ms.saturating_mul(3).min(i64::MAX as u64);
        let renew_ms = (lease_ttl_ms / 3).max(1);
        let mut tick = tokio::time::interval(Duration::from_millis(tick_ms));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut guards: Vec<CellRoute> = Vec::new();
        // Nodes whose cells were swept but whose expired lease record could
        // not yet be retired. Retrying retirement must not repeat the
        // fleet-wide cell scan.
        let mut swept: std::collections::HashSet<String> = std::collections::HashSet::new();
        // Incomplete generations remain durable and retryable, but a permanent
        // incompatibility (for example an old class absent from the current
        // deployment) must not turn every waker tick into another restore
        // storm. Deploy/restart creates a fresh waker and therefore retries
        // immediately; within one process, failures back off exponentially.
        let mut reconciliation_retries: HashMap<String, (String, u32, Instant)> = HashMap::new();
        loop {
            tick.tick().await;
            guards.clear();
            if !wake::try_hold_waker(
                &wcx.c, &wcx.bucket, &wcx.node, now_ms(), lease_ttl_ms as i64,
            ).await {
                continue;
            }
            // Dead-node reconciliation can legitimately outlive the ordinary
            // waker tick: a large bucket may require hundreds of LIST pages
            // before ownership records can be grouped. Renew independently
            // while the pass is in flight, and cancel the work if renewal
            // loses its conditional-write race. Otherwise another node can
            // acquire the expired role and multiply an already expensive scan.
            let renew_c = wcx.c.clone();
            let renew_bucket = wcx.bucket.clone();
            let renew_node = wcx.node.clone();
            let (lease_lost_tx, mut lease_lost_rx) = tokio::sync::oneshot::channel();
            let renewer = tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(renew_ms)).await;
                    if !wake::try_hold_waker(
                        &renew_c, &renew_bucket, &renew_node, now_ms(), lease_ttl_ms as i64,
                    ).await {
                        let _ = lease_lost_tx.send(());
                        break;
                    }
                }
            });
            let work = async {
            // Dead-node reconciliation is garbage collection, not recovery.
            // Arm-time wake entries guarantee every acked arm is covered in
            // the bucket before the application learns it exists, so a dead
            // generation's armed alarms are found by the due scan below with
            // no activation; ownership takeover is lazy on the next request.
            // What a dead generation leaves behind is index debris: its
            // node-cells/ markers (written by pre-GC deployments; new
            // processes no longer write them) and its node-session record.
            //
            // Advisory, like every other wake tier: a skewed clock that
            // declares a healthy node dead deletes only that node's markers,
            // and a renewed node writes no markers to lose.
            let dead_nodes = ownership::dead_nodes(&wcx.c, &wcx.bucket, now_ms() as u64).await;
            let dead_node_names: HashSet<&str> =
                dead_nodes.iter().map(|node| node.node.as_str()).collect();
            reconciliation_retries.retain(|node, _| dead_node_names.contains(node.as_str()));
            let retry_now = Instant::now();
            let indexed: HashMap<String, String> = dead_nodes
                .iter()
                .filter(|node| {
                    !swept.contains(&node.node) &&
                        reconciliation_retries.get(&node.node)
                            .is_none_or(|(generation, _, retry_at)| {
                                generation != &node.ownership_index_generation ||
                                    *retry_at <= retry_now
                            }) &&
                        !node.ownership_index_generation.is_empty()
                })
                .map(|node| (node.node.clone(), node.ownership_index_generation.clone()))
                .collect();
            let mut indexed_owned = None;
            if !indexed.is_empty() {
                match ownership::cells_indexed_by_nodes(&wcx.c, &wcx.bucket, &indexed).await {
                    Ok(owned) => indexed_owned = Some(owned),
                    Err(error) => {
                        warn!(%error, nodes = indexed.len(), "indexed dead-node scan failed");
                    }
                }
            }
            let gc_started = Instant::now();
            let mut gc_summaries = if let Some(owned) = indexed_owned {
                gc_indexed_dead_node_markers(wcx.clone(), owned).await
            } else {
                HashMap::new()
            };
            let gc_elapsed_ms = gc_started.elapsed().as_millis() as u64;
            for dead_node in dead_nodes {
                let node = dead_node.node;
                let generation = dead_node.ownership_index_generation;
                if !swept.contains(&node) {
                    // Generation-less records predate the marker index: they
                    // have no debris beyond the node record itself.
                    let (markers, retired, failures, complete) = if generation.is_empty() {
                        (0, 0, 0, true)
                    } else {
                        let Some(summary) = gc_summaries.remove(&node) else {
                            continue;
                        };
                        let complete = summary.failures == 0;
                        (summary.markers, summary.retired, summary.failures, complete)
                    };
                    info!(
                        event = "dead_node_reconciliation",
                        %node,
                        markers,
                        retired,
                        failures,
                        elapsed_ms = gc_elapsed_ms,
                        complete,
                        "dead-node marker GC complete"
                    );
                    // Retired markers are already durable deletions; an
                    // incomplete pass retries only the survivors, with the
                    // same backoff that bounds a persistently failing store.
                    if !complete {
                        let failure_count = reconciliation_retries
                            .get(&node)
                            .filter(|(retry_generation, _, _)| retry_generation == &generation)
                            .map_or(1, |(_, count, _)| count.saturating_add(1));
                        let retry_ms = celld_logic::dead_node_reconciliation::retry_delay_ms(
                            tick_ms,
                            failure_count,
                        );
                        reconciliation_retries.insert(
                            node.clone(),
                            (
                                generation,
                                failure_count,
                                Instant::now() + Duration::from_millis(retry_ms),
                            ),
                        );
                        warn!(
                            event = "dead_node_reconciliation_retry_scheduled",
                            %node,
                            failure_count,
                            retry_ms,
                            "incomplete dead-node marker GC scheduled with backoff"
                        );
                        continue;
                    }
                    reconciliation_retries.remove(&node);
                    swept.insert(node.clone());
                }
                match ownership::retire_dead_node(
                    &wcx.c,
                    &wcx.bucket,
                    &node,
                    now_ms() as u64,
                ).await {
                    Ok(_) => { swept.remove(&node); }
                    Err(error) => {
                        warn!(%node, %error, "dead-node lease retirement failed");
                    }
                }
            }

            for (scope, entry_due) in
                wake::due_scan(&wcx.c, &wcx.bucket, now_ms()).await
            {
                if wcx.registry.lock().unwrap().contains_key(&scope) {
                    continue;
                }
                if matches!(
                    ownership::resolve_owner(&wcx.c, &wcx.bucket, &scope).await,
                    Ok(Some(_))
                ) {
                    continue; // a live owner's heap or boot scan covers it
                }
                match ensure_cell(&wcx, &scope).await {
                    Ok(route @ CellRoute::Local { .. }) => {
                        js::touch(&scope);
                        // own the entry that woke us, so the sweep deletes
                        // it whether the alarm fires or the truth turns
                        // out to have nothing armed at all
                        wake_flusher.adopt(&scope, entry_due);
                        guards.push(route);
                        info!(%scope, "waker revived orphaned alarm cell");
                    }
                    Ok(CellRoute::Remote(_)) => {}
                    Err(e) => warn!(%scope, error = %e, "waker revival failed"),
                }
            }
            };
            tokio::pin!(work);
            tokio::select! {
                biased;
                result = &mut lease_lost_rx => {
                    warn!(
                        channel_closed = result.is_err(),
                        "waker lease renewal failed; cancelling in-flight reconciliation"
                    );
                }
                _ = &mut work => {}
            }
            renewer.abort();
        }
    });

    // At boot, one paginated LIST of the wake prefix revives cells whose
    // alarms came due while no process was watching. Entries are hints: a cell
    // whose restored truth has nothing due simply re-hibernates.
    let bcx = cx.clone();
    let boot_flusher = flusher.clone();
    tokio::spawn(async move {
        let due = wake::due_scan(&bcx.c, &bcx.bucket, now_ms()).await;
        if due.is_empty() {
            debug!("wake boot scan complete: no due entries");
        } else {
            info!(count = due.len(), "wake boot scan found due alarms");
        }
        for (scope, entry_due) in due {
            match ensure_cell(&bcx, &scope).await {
                Ok(CellRoute::Local { .. }) => {
                    boot_flusher.adopt(&scope, entry_due);
                    info!(%scope, "boot scan woke cell with due alarm");
                }
                Ok(CellRoute::Remote(_)) => {
                    // a lease still looks live (possibly a dead node's,
                    // until its TTL lapses) — the waker tick retries
                    info!(%scope, "boot scan deferred to apparent owner");
                }
                Err(e) => warn!(%scope, error = %e, "boot scan wake failed"),
            }
        }
    });

    let _keep = repl; // its Drop kills the node litestream

    let state = AppState {
        workers,
        cx: cx.clone(),
        assets: asset_resolver,
    };
    let peer_routes = Router::new()
        .route("/__celld/probe", any(peer_probe_handle))
        .route("/__ws/:scope", any(ws_handle))
        .route("/__do/:scope", any(do_handle))
        .route("/__abort/:scope/:request_id", any(abort_do_handle))
        .route("/__rpc/:scope", any(rpc_handle))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            require_peer_auth,
        ));
    let app = Router::new()
        .merge(peer_routes)
        .fallback(any(handle))
        .with_state(state);
    runtime_ready.store(true, Ordering::SeqCst);
    if settings.control_plane {
        control_plane::start_deploy_agent(c.clone(), bucket.clone(), runtime_ready.clone());
        control_plane::start_presence_agent(control_plane::PresenceRuntime {
            s3: c,
            repl: cx.repl.clone(),
            bucket,
            litestream,
            endpoint,
            region,
            storage_credentials,
            node_session_id: node,
            advertise: advertise.clone(),
            listen: listen.clone(),
            runtime_ready,
            owned_cells,
            owned_cell_inventory,
            lease_manager,
        });
    }
    info!("serving {script}@{deploy_version} — http://{listen}/");
    axum::serve(listener, app).await?;
    Ok(())
}

/// Litestream is an ordinary external dependency, like esbuild: found on
/// PATH, overridable with LITESTREAM_BIN. The daemon verifies it at boot
/// and crashes immediately rather than at the first cell request;
/// subcommands like `celld deploy` never call this.
fn litestream_binary() -> anyhow::Result<String> {
    let litestream =
        std::env::var("LITESTREAM_BIN").unwrap_or_else(|_| "litestream".to_string());
    let output = std::process::Command::new(&litestream)
        .arg("version")
        .output()
        .with_context(|| {
            format!(
                "litestream not found at {litestream:?}; install litestream \
                 (https://litestream.io) or set LITESTREAM_BIN"
            )
        })?;
    let version = String::from_utf8_lossy(&output.stdout);
    info!(litestream = %litestream, version = %version.trim(), "litestream");
    Ok(litestream)
}

#[cfg(test)]
mod compatibility_tests {
    use super::{
        websocket_close_details, worker_compat,
        AdvisoryActivity, CellActivity,
    };
    use serde_json::json;

    #[test]
    fn delete_all_alarm_behavior_follows_date_and_explicit_flags() {
        let deletes = |m: &serde_json::Value| {
            worker_compat(m).delete_all_deletes_alarm
        };
        assert!(!deletes(&json!({})));
        assert!(!deletes(&json!({ "compatibility_date": "2026-02-23" })));
        assert!(deletes(&json!({ "compatibility_date": "2026-02-24" })));
        assert!(deletes(&json!({
            "compatibility_date": "2025-01-01",
            "compatibility_flags": ["delete_all_deletes_alarm"],
        })));
        assert!(!deletes(&json!({
            "compatibility_date": "2027-01-01",
            "compatibility_flags": ["delete_all_preserves_alarm"],
        })));
    }

    #[test]
    fn js_rpc_and_fetcher_helper_flags_follow_date_and_explicit_flags() {
        // js_rpc is an obsolete opt-in with no enabling date.
        assert!(!worker_compat(&json!({})).js_rpc);
        assert!(
            !worker_compat(&json!({ "compatibility_date": "2030-01-01" }))
                .js_rpc,
        );
        assert!(
            worker_compat(&json!({ "compatibility_flags": ["js_rpc"] }))
                .js_rpc,
        );
        // The deprecated stub HTTP helpers exist before 2024-03-26 and can
        // be forced either way by flag.
        let helpers = |m: &serde_json::Value| {
            worker_compat(m).fetcher_get_put_delete
        };
        assert!(helpers(&json!({})));
        assert!(helpers(&json!({ "compatibility_date": "2024-03-25" })));
        assert!(!helpers(&json!({ "compatibility_date": "2024-03-26" })));
        assert!(helpers(&json!({
            "compatibility_date": "2025-01-01",
            "compatibility_flags": ["fetcher_has_get_put_delete"],
        })));
        assert!(!helpers(&json!({
            "compatibility_date": "2020-01-01",
            "compatibility_flags": ["fetcher_no_get_put_delete"],
        })));
    }

    #[test]
    fn advisory_activity_is_cumulative_and_separate_from_authority() {
        let activity = AdvisoryActivity::default();
        activity.record_acquisition(1, false);
        activity.record_acquisition(3, true);
        activity.record_restore();
        activity.record_proxy();
        activity.record_proxy();

        let snapshot = activity.snapshot();
        assert_eq!(snapshot.acquired, 2);
        assert_eq!(snapshot.proxied, 2);
        assert_eq!(snapshot.expired_owner_leases, 1);
        assert_eq!(snapshot.restored, 1);
        assert_eq!(snapshot.advanced_epochs, 1);
    }

    #[test]
    fn websocket_close_details_preserve_peer_code_reason_and_cleanliness() {
        assert_eq!(
            websocket_close_details(&[0x03, 0xe8, b'b', b'y', b'e']),
            (1000, "bye".to_string(), true),
        );
        assert_eq!(
            websocket_close_details(&[]),
            (1005, String::new(), true),
        );
        assert_eq!(
            websocket_close_details(&[0x03]),
            (1002, String::new(), false),
        );
        assert_eq!(
            websocket_close_details(&[0x03, 0xe8, 0xff]),
            (1007, String::new(), false),
        );
    }

    #[test]
    fn cell_close_excludes_queued_and_running_events() {
        let activity = CellActivity::new();
        let queued = activity.try_acquire().expect("queue pin");
        assert!(activity.try_acquire_idle().is_none());
        assert!(!activity.begin_close());
        let running = activity.try_acquire().expect("handler pin");
        drop(queued);
        assert!(!activity.begin_close());
        drop(running);
        let inline = activity.try_acquire_idle().expect("idle inline pin");
        assert!(activity.try_acquire_idle().is_none());
        assert!(!activity.begin_close());
        drop(inline);
        assert!(activity.begin_close());
        assert!(activity.try_acquire().is_none());
        assert!(activity.try_acquire_idle().is_none());
        assert!(!activity.begin_close());
    }
}

#[cfg(all(test, celld_internal_tests))]
mod private_tests {
    include!(env!("CELLD_CONFORMANCE_MAIN_TESTS"));
}
