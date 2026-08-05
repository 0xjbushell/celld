// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Runnable celld vertical slice.
//!
//! One actor serializes every event through `celld-logic`; the actor polls its
//! mailbox, timers, and in-flight effect futures together. This is the
//! execution shape required for monotonic lease ticks to fence the node even
//! when a storage operation remains hung, without spawning a task per effect.

use anyhow::Context as _;
use base64::Engine as _;
use celld::assets::AssetResolver;
use celld::fleet;
use celld::js::{
    ArmGate, AssetCallReq, Compat, DoCallReq, HttpResponse, RpcCallReq, SvcCallReq, SvcRpcReq,
    WorkerConfigOptions, WsOut,
};
use celld::ownership_store::{now_ms, S3Ownership};
use celld::peer_auth::{self, PeerAuth};
use celld::runtime::{CohostedWorker, Replication, RuntimeFetch, RuntimeManager, RuntimeOptions};
use celld_logic::{
    on_event, CasGuard, CasOutcome, Config, Effect, Event, Failure, LeaseCasOutcome, NodeLeaseMode,
    NodeLeaseRecord, NodeLeaseSpec, OpId, OwnerRecord, OwnershipOnEvict, Phase, RequestError,
    Route, State, StopCause, Timer, WebSocketKind, WorkerRoute,
};
use futures_util::stream::{FuturesUnordered, StreamExt};
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::{Bytes, Frame, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use rand::RngCore;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_util::time::{delay_queue, DelayQueue};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum TimerSlot {
    NodeLeaseRenew,
    NodeLeaseFence,
    CellAlarm(String),
    /// Keyed by operation, deliberately. Every other timer here coalesces
    /// because only its newest arming matters; a deadline is the opposite --
    /// each watches a different outstanding operation, and a shared slot
    /// would let arming one silently cancel another, leaving every activation
    /// but the most recent with nothing watching it.
    OperationDeadline(OpId),
}

impl TimerSlot {
    fn of(timer: &Timer) -> Self {
        match timer {
            Timer::NodeLeaseRenew { .. } => Self::NodeLeaseRenew,
            Timer::NodeLeaseFence { .. } => Self::NodeLeaseFence,
            Timer::CellAlarm { cell, .. } => Self::CellAlarm(cell.clone()),
            Timer::OperationDeadline { op } => Self::OperationDeadline(*op),
        }
    }
}

struct MemoryOwnership {
    node: String,
    owners: BTreeMap<String, OwnerRecord>,
    leases: BTreeMap<String, NodeLeaseRecord>,
    next_etag: u64,
}

#[derive(Clone)]
enum Ownership {
    Memory(Arc<Mutex<MemoryOwnership>>),
    S3(Arc<S3Ownership>),
}

impl Ownership {
    async fn read_owner(&self, cell: &str) -> Result<Option<OwnerRecord>, Failure> {
        match self {
            Self::Memory(memory) => Ok(memory.lock().await.owners.get(cell).cloned()),
            Self::S3(s3) => s3.read_owner(cell).await.map_err(|error| {
                eprintln!("celld ownership read failed: {error:#}");
                Failure::Definite
            }),
        }
    }

    async fn read_node_lease(&self, owner: &str) -> Result<Option<NodeLeaseRecord>, Failure> {
        match self {
            Self::Memory(memory) => Ok(memory.lock().await.leases.get(owner).cloned()),
            Self::S3(s3) => s3.read_node_lease(owner).await.map_err(|error| {
                eprintln!("celld node lease read failed: {error:#}");
                Failure::Definite
            }),
        }
    }

    async fn read_capacity_peers(&self) -> Result<Vec<celld_logic::CapacityPeer>, Failure> {
        match self {
            // The in-memory adapter is a single-node development mode. It has
            // no external membership enumeration to offer.
            Self::Memory(_) => Ok(Vec::new()),
            Self::S3(s3) => s3.read_capacity_peers().await.map_err(|error| {
                eprintln!("celld capacity peer read failed: {error:#}");
                Failure::Definite
            }),
        }
    }

    async fn read_self_node_lease(&self, node: &str) -> Result<Option<NodeLeaseRecord>, Failure> {
        match self {
            Self::Memory(memory) => Ok(memory.lock().await.leases.get(node).cloned()),
            Self::S3(s3) => s3.read_self_node_lease(node).await.map_err(|error| {
                eprintln!("celld self node lease read failed: {error:#}");
                Failure::Definite
            }),
        }
    }

    async fn cas_owner(
        &self,
        cell: &str,
        guard: CasGuard,
        epoch: u64,
    ) -> Result<CasOutcome, Failure> {
        match self {
            Self::Memory(memory) => {
                let mut memory = memory.lock().await;
                let allowed = match guard {
                    CasGuard::Absent => !memory.owners.contains_key(cell),
                    CasGuard::Match(expected) => memory
                        .owners
                        .get(cell)
                        .is_some_and(|owner| owner.etag == expected),
                };
                if allowed {
                    let etag = format!("e{}", memory.next_etag);
                    memory.next_etag += 1;
                    let node = memory.node.clone();
                    memory.owners.insert(
                        cell.into(),
                        OwnerRecord {
                            node: Some(node),
                            epoch,
                            etag,
                        },
                    );
                }
                Ok(if allowed {
                    CasOutcome::Applied
                } else {
                    CasOutcome::Rejected
                })
            }
            Self::S3(s3) => s3.cas_owner(cell, guard, epoch).await.map_err(|error| {
                // Any transport or 5xx failure may have happened after S3
                // committed. The core reconciles by reading the owner again.
                eprintln!("celld ownership CAS ambiguous: {error:#}");
                Failure::Ambiguous
            }),
        }
    }

    async fn release_owner(&self, cell: &str, epoch: u64) -> Result<CasOutcome, Failure> {
        match self {
            Self::Memory(memory) => {
                let mut memory = memory.lock().await;
                let node = memory.node.clone();
                let releasable = memory.owners.get(cell).is_some_and(|owner| {
                    owner.node.as_deref() == Some(node.as_str()) && owner.epoch == epoch
                });
                if releasable {
                    let etag = format!("e{}", memory.next_etag);
                    memory.next_etag += 1;
                    memory.owners.insert(
                        cell.into(),
                        OwnerRecord {
                            node: None,
                            epoch,
                            etag,
                        },
                    );
                }
                Ok(if releasable {
                    CasOutcome::Applied
                } else {
                    CasOutcome::Rejected
                })
            }
            // A release that may or may not have committed needs no
            // reconciliation: the record either still names this node, and the
            // next eviction releases it again, or it does not, and the cell is
            // already free. Either way nothing is owed.
            Self::S3(s3) => s3.release_owner(cell, epoch).await.map_err(|error| {
                eprintln!("celld ownership release failed: {error:#}");
                Failure::Definite
            }),
        }
    }

    async fn cas_node_lease(
        &self,
        guard: CasGuard,
        mut record: NodeLeaseRecord,
    ) -> Result<LeaseCasOutcome, Failure> {
        match self {
            Self::Memory(memory) => {
                let mut memory = memory.lock().await;
                let current = memory.leases.get(&record.node);
                let allowed = match guard {
                    CasGuard::Absent => current.is_none(),
                    CasGuard::Match(expected) => {
                        current.is_some_and(|lease| lease.etag == expected)
                    }
                };
                if !allowed {
                    return Ok(LeaseCasOutcome::Rejected);
                }
                let etag = format!("e{}", memory.next_etag);
                memory.next_etag += 1;
                record.etag = etag.clone();
                memory.leases.insert(record.node.clone(), record);
                Ok(LeaseCasOutcome::Applied { etag })
            }
            Self::S3(s3) => s3.cas_node_lease(guard, &record).await.map_err(|error| {
                eprintln!("celld node-lease CAS ambiguous: {error:#}");
                Failure::Ambiguous
            }),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::Memory(_) => "memory",
            Self::S3(_) => "s3",
        }
    }
}

enum Message {
    /// A periodic resource sample. The measuring is the shell's job; every
    /// decision that follows belongs to the core.
    SampleLoad,
    Request {
        cell: String,
        capacity_handoff: bool,
        reply: oneshot::Sender<Result<Routed, RequestError>>,
    },
    WorkerRequest {
        reply: oneshot::Sender<WorkerRouted>,
    },
    ActivityFinished {
        request: u64,
        // The activity drop also observes the cell's alarm. Folded in here so a
        // request's completion costs the serial core one message, not two — the
        // hot path for every routed DO call.
        cell: String,
        alarm_at_ms: Option<i64>,
        alarm_covered: bool,
    },
    /// A local request whose handler advanced its cell's committed-write
    /// position wrote: open the output gate and hold the response until durable.
    GateWrite {
        request: u64,
        position: u64,
        reply: oneshot::Sender<Result<(), RequestError>>,
    },
    /// A `webSocketMessage` finished; hand its captured outbound frames to the
    /// cell's output gate. `write_position` present means the handler wrote, so
    /// the frames are held behind that write until it is durable; absent means
    /// flush them (behind any earlier still-pending write, else immediately).
    WsOutput {
        request: u64,
        scope: String,
        frames: Vec<(u64, WsOut)>,
        write_position: Option<u64>,
        reply: oneshot::Sender<()>,
    },
    WebSocketOpened {
        cell: String,
        websocket: u64,
        kind: WebSocketKind,
        reply: oneshot::Sender<bool>,
    },
    WebSocketClosed {
        cell: String,
        websocket: u64,
    },
    AlarmObserved {
        cell: String,
        at_ms: Option<i64>,
        covered: bool,
    },
    WakeHint {
        cell: String,
    },
    Evict {
        cell: String,
        reply: oneshot::Sender<()>,
    },
    InvalidateRemote {
        cell: String,
        node: String,
        epoch: u64,
        reply: oneshot::Sender<()>,
    },
    Snapshot {
        reply: oneshot::Sender<String>,
    },
    Health {
        reply: oneshot::Sender<bool>,
    },
    Presence {
        reply: oneshot::Sender<celld_logic::PresenceSnapshot>,
    },
}

struct Routed {
    request: u64,
    route: Route,
}

// Read only by the orphaned `worker_route`; removed with the landing-cell
// machinery in the DO-dispatch refactor ([[designs/do-fast-path]]).
#[allow(dead_code)]
struct WorkerRouted {
    request: u64,
    route: Option<WorkerRoute>,
}

/// Cancels a Worker request when Hyper drops the ingress future before the
/// response is published. The isolate observes this through its existing
/// event-loop poll, so cancellation adds no task to the request path.
struct IngressAbortGuard(Option<celld::js::RequestId>);

impl IngressAbortGuard {
    fn new(request: celld::js::RequestId) -> Self {
        Self(Some(request))
    }

    fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for IngressAbortGuard {
    fn drop(&mut self) {
        if let Some(request) = self.0 {
            celld::js::abort_request(request);
        }
    }
}

struct ActivityGuard {
    tx: mpsc::UnboundedSender<Message>,
    request: u64,
    runtime: Option<RuntimeManager>,
    cell: String,
}

impl Drop for ActivityGuard {
    fn drop(&mut self) {
        let at_ms = self
            .runtime
            .as_ref()
            .and_then(|runtime| runtime.alarm(&self.cell));
        let covered = self
            .runtime
            .as_ref()
            .is_some_and(|runtime| runtime.alarm_covered(&self.cell, at_ms));
        let _ = self.tx.send(Message::ActivityFinished {
            request: self.request,
            cell: self.cell.clone(),
            alarm_at_ms: at_ms,
            alarm_covered: covered,
        });
    }
}

#[derive(Clone, Copy)]
enum RouteStage {
    OwnershipRead,
    NodeLeaseLookup,
    OwnershipAcquire,
    Restore,
    IsolateStartup,
    RegistryInsert,
}

struct EffectTiming {
    cell: String,
    stage: RouteStage,
    elapsed_us: u64,
}

struct CompletedEffect {
    event: Event,
    timing: Option<EffectTiming>,
}

impl CompletedEffect {
    fn plain(event: Event) -> Self {
        Self {
            event,
            timing: None,
        }
    }

    fn timed(event: Event, cell: String, stage: RouteStage, started: Instant) -> Self {
        Self {
            event,
            timing: Some(EffectTiming {
                cell,
                stage,
                elapsed_us: started.elapsed().as_micros() as u64,
            }),
        }
    }
}

struct CellRouteTiming {
    started: Instant,
    activation_started: bool,
    capacity_wait_started: Option<Instant>,
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
    fresh: Option<bool>,
}

impl CellRouteTiming {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            activation_started: false,
            capacity_wait_started: None,
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
            fresh: None,
        }
    }

    fn effect_started(&mut self) {
        if !self.activation_started {
            self.activation_started = true;
            self.activation_slot_wait_us = self.started.elapsed().as_micros() as u64;
        }
        if let Some(started) = self.capacity_wait_started.take() {
            self.capacity_wait_us = self
                .capacity_wait_us
                .saturating_add(started.elapsed().as_micros() as u64);
        }
    }

    fn record(&mut self, stage: RouteStage, elapsed_us: u64) {
        let field = match stage {
            RouteStage::OwnershipRead => &mut self.ownership_read_us,
            RouteStage::NodeLeaseLookup => &mut self.node_lease_lookup_us,
            RouteStage::OwnershipAcquire => &mut self.ownership_acquire_us,
            RouteStage::Restore => &mut self.restore_us,
            RouteStage::IsolateStartup => &mut self.isolate_startup_us,
            RouteStage::RegistryInsert => &mut self.registry_insert_us,
        };
        *field = field.saturating_add(elapsed_us);
    }
}

type EffectFuture = Pin<Box<dyn Future<Output = CompletedEffect> + Send>>;
type ConnectionFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type DoCallFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type AssetCallFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type WebSocketFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type CachePruneFuture = Pin<
    Box<dyn Future<Output = (u64, Result<(usize, usize, u64), tokio::task::JoinError>)> + Send>,
>;

#[derive(Clone)]
struct AppHandle {
    tx: mpsc::UnboundedSender<Message>,
    runtime: Option<RuntimeManager>,
    assets: Arc<HashMap<String, AssetResolver>>,
    asset_script: Option<Arc<str>>,
    peer_http: reqwest::Client,
    peer_auth: Arc<PeerAuth>,
    advertise: String,
    websockets: mpsc::UnboundedSender<WebSocketFuture>,
    /// Whether the RPO=0 output gate is armed: hold a local write's response
    /// until its cell is proven durable. On by default; set `CELLD_OUTPUT_GATE=0`
    /// to acknowledge writes without proving them durable. The core and its DST
    /// are unconditional.
    output_gate: bool,
    /// Concurrent outbound WebSockets one cell may hold, for the refusal
    /// message; the core enforces it.
    max_outbound_websockets: usize,
}

impl AppHandle {
    async fn request(&self, cell: String) -> Result<Routed, RequestError> {
        self.request_with_mode(cell, false).await
    }

    async fn capacity_request(&self, cell: String) -> Result<Routed, RequestError> {
        self.request_with_mode(cell, true).await
    }

    async fn request_with_mode(
        &self,
        cell: String,
        capacity_handoff: bool,
    ) -> Result<Routed, RequestError> {
        let (reply, receive) = oneshot::channel();
        if self
            .tx
            .send(Message::Request {
                cell,
                capacity_handoff,
                reply,
            })
            .is_err()
        {
            return Err(RequestError::NodeFenced);
        }
        receive.await.unwrap_or(Err(RequestError::NodeFenced))
    }

    // Orphaned by the always-pool Worker entry below. The whole landing-cell
    // machinery (this, `Message::WorkerRequest`, `WorkerRouted`,
    // `fetch_worker_on_cell`, `CellJob::WorkerFetch`, the logic `worker_request`,
    // and the inline co-hosted-write gate) is removed together in the DO-dispatch
    // refactor ([[designs/do-fast-path]]) so the RPO=0 co-hosted path is reworked
    // coherently rather than unpicked here.
    #[allow(dead_code)]
    async fn worker_route(&self) -> anyhow::Result<WorkerRouted> {
        let (reply, receive) = oneshot::channel();
        self.tx
            .send(Message::WorkerRequest { reply })
            .map_err(|_| anyhow::anyhow!("core stopped before Worker routing"))?;
        receive
            .await
            .context("core stopped while routing Worker request")
    }

    async fn fetch_worker(
        &self,
        url: String,
        method: String,
        body: Vec<u8>,
        headers: Vec<(String, String)>,
    ) -> anyhow::Result<HttpResponse> {
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Worker runtime unavailable"))?;
        let js_request = celld::js::next_request_id();
        // The Worker entry is stateless — run it in the pool, always. Routing it
        // to a "landing cell" existed only to make an `env.NS.get(id)` call
        // resolve inline; but a cell runs one fetch at a time, so under any real
        // concurrency the entry can't be on the cell it calls, and the routing
        // round-trip is paid for nothing — on top of the DO call's own routing.
        // A DO call reaches its owning cell (with fencing and the output gate) on
        // the host path, so one routing round-trip per request, never two.
        let mut abort = IngressAbortGuard::new(js_request);
        let result = runtime
            .fetch_worker_pool(url, method, body, headers, js_request)
            .await;
        abort.disarm();
        result
    }

    fn activity(&self, request: u64, cell: String) -> ActivityGuard {
        ActivityGuard {
            tx: self.tx.clone(),
            request,
            runtime: self.runtime.clone(),
            cell,
        }
    }

    /// The output gate. The caller invokes this only for a request whose
    /// handler advanced the cell's committed position, so the response is held
    /// until the core proves the cell durable; `Ok(())` releases it, `Err`
    /// fails it. Called before the activity guard drops, so the request stays
    /// pinned across the wait.
    async fn gate_write(&self, request: u64, position: u64) -> Result<(), RequestError> {
        let (reply, receive) = oneshot::channel();
        if self
            .tx
            .send(Message::GateWrite {
                request,
                position,
                reply,
            })
            .is_err()
        {
            return Err(RequestError::NodeFenced);
        }
        receive.await.unwrap_or(Err(RequestError::NodeFenced))
    }

    /// Hand a finished `webSocketMessage`'s frames to the cell's output gate.
    /// Awaited so the request stays pinned until the actor has registered the
    /// gate (the core reads the still-active request when it opens); frames flush
    /// or fail asynchronously as durability resolves, not here.
    async fn ws_output(
        &self,
        request: u64,
        scope: String,
        frames: Vec<(u64, WsOut)>,
        write_position: Option<u64>,
    ) {
        let (reply, receive) = oneshot::channel();
        if self
            .tx
            .send(Message::WsOutput {
                request,
                scope,
                frames,
                write_position,
                reply,
            })
            .is_ok()
        {
            let _ = receive.await;
        }
    }

    async fn websocket_opened(
        &self,
        cell: String,
        websocket: u64,
        kind: WebSocketKind,
    ) -> anyhow::Result<()> {
        let (reply, receive) = oneshot::channel();
        self.tx
            .send(Message::WebSocketOpened {
                cell,
                websocket,
                kind,
                reply,
            })
            .map_err(|_| anyhow::anyhow!("core stopped before WebSocket opened"))?;
        let held = receive
            .await
            .context("core stopped while opening WebSocket")?;
        anyhow::ensure!(
            held,
            "outbound WebSocket refused: a cell may hold at most {}, and a node \
             may pin at most {}% of its residency ceiling",
            self.max_outbound_websockets,
            celld_logic::pressure::MAX_OUTBOUND_PIN_PERCENT,
        );
        Ok(())
    }

    fn websocket_closed(&self, cell: String, websocket: u64) {
        let _ = self.tx.send(Message::WebSocketClosed { cell, websocket });
    }

    async fn evict(&self, cell: String) {
        let (reply, receive) = oneshot::channel();
        if self.tx.send(Message::Evict { cell, reply }).is_ok() {
            let _ = receive.await;
        }
    }

    async fn invalidate_remote(&self, cell: String, node: String, epoch: u64) {
        let (reply, receive) = oneshot::channel();
        if self
            .tx
            .send(Message::InvalidateRemote {
                cell,
                node,
                epoch,
                reply,
            })
            .is_ok()
        {
            let _ = receive.await;
        }
    }

    async fn snapshot(&self) -> String {
        let (reply, receive) = oneshot::channel();
        if self.tx.send(Message::Snapshot { reply }).is_err() {
            return "{\"error\":\"actor_stopped\"}".into();
        }
        receive
            .await
            .unwrap_or_else(|_| "{\"error\":\"actor_stopped\"}".into())
    }

    async fn healthy(&self) -> bool {
        let (reply, receive) = oneshot::channel();
        if self.tx.send(Message::Health { reply }).is_err() {
            return false;
        }
        receive.await.unwrap_or(false)
    }

    async fn presence(&self) -> Option<celld_logic::PresenceSnapshot> {
        let (reply, receive) = oneshot::channel();
        self.tx.send(Message::Presence { reply }).ok()?;
        receive.await.ok()
    }
}

/// One cell's WebSocket output gate: an ordered queue of write barriers.
#[derive(Default)]
struct WsGate {
    barriers: VecDeque<WsBarrier>,
}

/// A gated write and the outbound frames held behind it. `settled` is `None`
/// while the write's durability is unproven, `Some(true)` once proven (its
/// frames may flush when it reaches the front), `Some(false)` on failure (the
/// gate breaks). Frames from later non-writing messages accrete onto the
/// newest barrier's `frames`, so they never overtake the write they trail.
struct WsBarrier {
    request: u64,
    settled: Option<bool>,
    frames: Vec<(u64, WsOut)>,
}

struct Actor {
    state: State,
    ownership: Ownership,
    runtime: Option<RuntimeManager>,
    region: String,
    pending: BTreeMap<u64, oneshot::Sender<Result<Routed, RequestError>>>,
    request_cells: BTreeMap<u64, String>,
    route_timings: BTreeMap<String, CellRouteTiming>,
    pending_workers: BTreeMap<u64, oneshot::Sender<WorkerRouted>>,
    eviction_waiters: BTreeMap<String, Vec<oneshot::Sender<()>>>,
    durability_waiters: BTreeMap<u64, (String, Vec<oneshot::Sender<()>>)>,
    eviction_stops: BTreeMap<u64, Vec<oneshot::Sender<()>>>,
    /// Local write responses held open by the output gate, keyed by request,
    /// released when the core emits `ReleaseResponse`.
    gated_responses: BTreeMap<u64, oneshot::Sender<Result<(), RequestError>>>,
    /// Per-cell WebSocket output gate: a FIFO of write barriers, each holding
    /// the outbound frames produced up to it. The front drains in write order as
    /// durability proves, so no client ever sees a frame that trails an
    /// unproven write (the Cloudflare per-object output gate).
    ws_gates: BTreeMap<String, WsGate>,
    /// Gated `webSocketMessage` requests, mapping each to its cell so a
    /// `ReleaseResponse` routes to the WebSocket gate rather than an HTTP reply.
    ws_gated: BTreeMap<u64, String>,
    published: BTreeSet<String>,
    next_request: u64,
    fail_publish_once: bool,
    publishes: u64,
    stops: u64,
    lease_spec: NodeLeaseSpec,
    /// Whether to re-check every core invariant after every event.
    ///
    /// The check is a full scan of the cell table, so its cost grows with
    /// residency: roughly 800us per event at ten thousand resident cells,
    /// which would cap a busy node at about a thousand events a second in
    /// assertion code alone. It is a model-checking assertion, and the place
    /// it earns its keep is simulation, which can afford to run it after
    /// every event over far more schedules than production will ever see.
    /// Debug builds keep it; release builds opt in with CELLD_VALIDATE=1.
    validate_invariants: bool,
    /// Carries the previous CPU tick reading; a rate needs two samples.
    load_sampler: ProcessLoadSampler,
    /// The counters peers rank this node by, when there is a bucket to
    /// publish them to.
    live_load: Option<Arc<celld::ownership_store::LiveLoad>>,
    started_at: Instant,
    fence: mpsc::UnboundedSender<i32>,
    timers: DelayQueue<Timer>,
    timer_keys: BTreeMap<TimerSlot, delay_queue::Key>,
}

struct AdmissionLimits {
    resident: usize,
    activations: usize,
    hibernations: usize,
}

struct ActorIdentity {
    node: String,
    advertise: String,
    region: String,
    managed: bool,
}

impl Actor {
    async fn from_environment(
        limits: AdmissionLimits,
        fail_publish_once: bool,
        fence: mpsc::UnboundedSender<i32>,
        runtime: Option<RuntimeManager>,
        ownership: Option<Ownership>,
        identity: ActorIdentity,
    ) -> anyhow::Result<Self> {
        let ActorIdentity {
            node,
            advertise,
            region,
            managed,
        } = identity;
        let ownership = if let Some(ownership) = ownership {
            ownership
        } else {
            Ownership::Memory(Arc::new(Mutex::new(MemoryOwnership {
                node: node.clone(),
                owners: BTreeMap::new(),
                leases: BTreeMap::new(),
                next_etag: 1,
            })))
        };
        let live_load = match &ownership {
            Ownership::S3(s3) => Some(s3.live()),
            Ownership::Memory(_) => None,
        };
        let process_generation = match &ownership {
            Ownership::S3(s3) => s3
                .process_generation()
                .map(str::to_owned)
                .unwrap_or_else(random_process_generation),
            Ownership::Memory(_) => random_process_generation(),
        };
        let ttl_ms = std::env::var("CELLD_TTL_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(10_000);
        let lease_mode = match std::env::var("CELLD_LAZY_NODE_LEASE").as_deref() {
            Ok("on") => NodeLeaseMode::Lazy,
            Ok("shadow") => NodeLeaseMode::Shadow,
            _ => NodeLeaseMode::Continuous,
        };
        let lease_linger_ms = std::env::var("CELLD_LEASE_LINGER_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(if managed { 0 } else { ttl_ms });
        let lease_spec = NodeLeaseSpec {
            addr: std::env::var("CELLD_ADVERTISE").unwrap_or(advertise),
            peer_protocol: peer_auth::PROTOCOL_VERSION,
            generation: std::env::var("CELLD_TEST_GENERATION").unwrap_or(process_generation),
            ttl_ms,
            mode: lease_mode,
            linger_ms: lease_linger_ms,
        };
        Ok(Self {
            state: State::new(
                node,
                Config {
                    max_resident: limits.resident,
                    max_activations: limits.activations,
                    max_hibernations: limits.hibernations,
                    alarm_resident_ms: celld::wake::resident_ms().max(0) as u64,
                    require_node_lease: true,
                    peer_protocol: celld::peer_auth::PROTOCOL_VERSION,
                    operation_deadline_ms: Some(
                        std::env::var("CELLD_OPERATION_DEADLINE_MS")
                            .ok()
                            .and_then(|value| value.parse().ok())
                            .unwrap_or(DEFAULT_OPERATION_DEADLINE_MS),
                    ),
                    idle_evict_ms: positive_environment::<u64>("CELLD_IDLE_EVICT_S")?
                        .map(|seconds| seconds.saturating_mul(1_000)),
                    pressure: pressure_config_from_environment()?,
                    max_outbound_websockets: std::env::var("CELLD_MAX_OUTBOUND_WEBSOCKETS")
                        .ok()
                        .and_then(|value| value.parse().ok())
                        .unwrap_or(DEFAULT_MAX_OUTBOUND_WEBSOCKETS),
                    ownership_on_evict: match &ownership {
                        // Releasing a cell says "any node may take this
                        // one", which is only true if some other node could
                        // restore it. Without a bucket the sole copy is this
                        // node's disk, keyed to an epoch a re-acquire will
                        // step past, so releasing would lose the cell.
                        Ownership::Memory(_) => OwnershipOnEvict::Sticky,
                        Ownership::S3(_) => ownership_on_evict_from_environment()?,
                    },
                },
            ),
            ownership,
            runtime,
            region,
            pending: BTreeMap::new(),
            request_cells: BTreeMap::new(),
            route_timings: BTreeMap::new(),
            pending_workers: BTreeMap::new(),
            eviction_waiters: BTreeMap::new(),
            durability_waiters: BTreeMap::new(),
            gated_responses: BTreeMap::new(),
            ws_gates: BTreeMap::new(),
            ws_gated: BTreeMap::new(),
            eviction_stops: BTreeMap::new(),
            published: BTreeSet::new(),
            next_request: 1,
            fail_publish_once,
            publishes: 0,
            stops: 0,
            lease_spec,
            live_load,
            load_sampler: ProcessLoadSampler::default(),
            validate_invariants: cfg!(debug_assertions)
                || std::env::var("CELLD_VALIDATE").is_ok_and(|value| value != "0"),
            started_at: Instant::now(),
            fence,
            timers: DelayQueue::new(),
            timer_keys: BTreeMap::new(),
        })
    }

    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<Message>) {
        let mut in_flight = FuturesUnordered::new();
        self.drive(
            Event::StartNodeLease {
                now_ms: now_ms(),
                spec: self.lease_spec.clone(),
            },
            &mut in_flight,
        );
        loop {
            tokio::select! {
                message = rx.recv() => {
                    let Some(message) = message else {
                        break;
                    };
                    self.handle_message(message, &mut in_flight);
                }
                Some(completed) = in_flight.next(), if !in_flight.is_empty() => {
                    if let Some(timing) = completed.timing {
                        self.record_effect_timing(timing);
                    }
                    self.drive(completed.event, &mut in_flight);
                }
                Some(expired) = self.timers.next(), if !self.timers.is_empty() => {
                    let timer = expired.into_inner();
                    self.timer_keys.remove(&TimerSlot::of(&timer));
                    self.drive(Event::TimerFired {
                        timer,
                        now_ms: now_ms(),
                        now_mono_ms: self.started_at.elapsed().as_millis() as u64,
                    }, &mut in_flight);
                }
            }
        }
    }

    fn schedule_timer(&mut self, timer: Timer, at_mono_ms: u64) {
        let slot = TimerSlot::of(&timer);
        let displaced = self.timer_keys.remove(&slot);
        // Displacing is the point for the coalescing slots and a bug for the
        // per-operation ones: two live deadlines sharing a slot means one
        // operation is no longer watched, which is invisible until something
        // hangs forever in production.
        debug_assert!(
            displaced.is_none() || !matches!(slot, TimerSlot::OperationDeadline(_)),
            "an operation deadline displaced another: {slot:?}"
        );
        if let Some(key) = displaced {
            self.timers.remove(&key);
        }
        let delay = std::time::Duration::from_millis(
            at_mono_ms.saturating_sub(self.started_at.elapsed().as_millis() as u64),
        );
        let key = self.timers.insert(timer, delay);
        self.timer_keys.insert(slot, key);
    }

    fn handle_message(&mut self, message: Message, in_flight: &mut FuturesUnordered<EffectFuture>) {
        match message {
            Message::Request {
                cell,
                capacity_handoff,
                reply,
            } => {
                let retired_durability = match self.state.phase(&cell) {
                    Some(Phase::EnsuringDurability { op, .. }) => Some(*op),
                    _ => None,
                };
                let request = self.next_request;
                self.next_request += 1;
                self.pending.insert(request, reply);
                self.begin_route_if_cold(&cell);
                self.request_cells.insert(request, cell.clone());
                self.drive(
                    if capacity_handoff {
                        Event::CapacityRequestAt {
                            request,
                            cell,
                            now_ms: now_ms(),
                            now_mono_ms: self.started_at.elapsed().as_millis() as u64,
                        }
                    } else {
                        Event::RequestAt {
                            request,
                            cell,
                            now_ms: now_ms(),
                            now_mono_ms: self.started_at.elapsed().as_millis() as u64,
                        }
                    },
                    in_flight,
                );
                if let Some(op) = retired_durability {
                    if let Some((_, waiters)) = self.durability_waiters.remove(&op) {
                        for waiter in waiters {
                            let _ = waiter.send(());
                        }
                    }
                }
            }
            Message::WorkerRequest { reply } => {
                let request = self.next_request;
                self.next_request += 1;
                self.pending_workers.insert(request, reply);
                self.drive(Event::WorkerRequest { request }, in_flight);
            }
            Message::SampleLoad => {
                let load = celld_logic::pressure::Load {
                    resident_cells: self.state.occupied(),
                    rss_bytes: resident_memory_bytes(),
                    cpu_percent_x100: self.load_sampler.sample_cpu_percent_x100(),
                };
                let cpu = load.cpu_percent_x100;
                let now_mono_ms = self.started_at.elapsed().as_millis() as u64;
                self.drive(Event::LoadSampled { load, now_mono_ms }, in_flight);
                // Republish what peers rank this node by. The same numbers the
                // latch just saw: a node that reports last tick's residency
                // attracts work it has already refused.
                if let Some(live) = &self.live_load {
                    live.resident_cells
                        .store(self.state.occupied(), Ordering::Relaxed);
                    live.host_websockets
                        .store(self.state.host_websockets(), Ordering::Relaxed);
                    live.pressured
                        .store(self.state.shedding(), Ordering::Relaxed);
                    live.cpu_percent_x100.store(cpu, Ordering::Relaxed);
                }
            }
            Message::GateWrite {
                request,
                position,
                reply,
            } => {
                // The dispatch path only asks to gate a request whose handler
                // advanced the cell's committed position, so this is always a
                // write: hold the response until the core proves it durable.
                self.gated_responses.insert(request, reply);
                self.drive(Event::Wrote { request, position }, in_flight);
            }
            Message::WsOutput {
                request,
                scope,
                frames,
                write_position,
                reply,
            } => {
                match write_position {
                    // The handler wrote: open a barrier holding these frames and
                    // gate on durability. A later `ReleaseResponse` settles it.
                    Some(position) => {
                        self.ws_gates
                            .entry(scope.clone())
                            .or_default()
                            .barriers
                            .push_back(WsBarrier {
                                request,
                                settled: None,
                                frames,
                            });
                        self.ws_gated.insert(request, scope);
                        self.drive(Event::Wrote { request, position }, in_flight);
                    }
                    // No write: trail the newest pending write if one is open,
                    // else the gate is open — flush now.
                    None => match self.ws_gates.get_mut(&scope) {
                        Some(gate) if !gate.barriers.is_empty() => {
                            gate.barriers.back_mut().unwrap().frames.extend(frames);
                        }
                        _ => celld::js::ws_emit_batch(frames),
                    },
                }
                let _ = reply.send(());
            }
            Message::ActivityFinished {
                request,
                cell,
                alarm_at_ms,
                alarm_covered,
            } => {
                // Folded from the old AlarmObserved-then-ActivityFinished pair the
                // activity drop sent: observe the alarm and release any retired
                // durability waiters, then finish the activity — same events, same
                // order, one message instead of two.
                let retired_durability = match self.state.phase(&cell) {
                    Some(Phase::EnsuringDurability { op, .. }) => Some(*op),
                    _ => None,
                };
                self.drive(
                    Event::AlarmObserved {
                        cell,
                        at_ms: alarm_at_ms,
                        covered: alarm_covered,
                        now_ms: now_ms(),
                        now_mono_ms: self.started_at.elapsed().as_millis() as u64,
                    },
                    in_flight,
                );
                if let Some(op) = retired_durability {
                    if let Some((_, waiters)) = self.durability_waiters.remove(&op) {
                        for waiter in waiters {
                            let _ = waiter.send(());
                        }
                    }
                }
                self.drive(Event::ActivityFinished { request }, in_flight);
            }
            Message::WebSocketOpened {
                cell,
                websocket,
                kind,
                reply,
            } => {
                self.drive(
                    Event::WebSocketOpened {
                        cell: cell.clone(),
                        websocket,
                        kind,
                    },
                    in_flight,
                );
                // The core may decline to hold the transport. Say so, rather
                // than acknowledging an open and closing the socket a moment
                // later: an application that hit a ceiling deserves to be
                // told which one, not left watching a socket disappear.
                // Only an outbound socket can be declined, and only for a
                // cell the core knows: an inbound transport is never refused,
                // and reporting one as refused fails opens that succeeded.
                let refused = kind == WebSocketKind::Outbound
                    && !self.state.holds_websocket(&cell, websocket);
                let _ = reply.send(!refused);
            }
            Message::WebSocketClosed { cell, websocket } => {
                self.drive(Event::WebSocketClosed { cell, websocket }, in_flight);
            }
            Message::AlarmObserved {
                cell,
                at_ms,
                covered,
            } => {
                let retired_durability = match self.state.phase(&cell) {
                    Some(Phase::EnsuringDurability { op, .. }) => Some(*op),
                    _ => None,
                };
                self.drive(
                    Event::AlarmObserved {
                        cell,
                        at_ms,
                        covered,
                        now_ms: now_ms(),
                        now_mono_ms: self.started_at.elapsed().as_millis() as u64,
                    },
                    in_flight,
                );
                if let Some(op) = retired_durability {
                    if let Some((_, waiters)) = self.durability_waiters.remove(&op) {
                        for waiter in waiters {
                            let _ = waiter.send(());
                        }
                    }
                }
            }
            Message::WakeHint { cell } => {
                self.drive(
                    Event::WakeHintAt {
                        cell,
                        now_ms: now_ms(),
                        now_mono_ms: self.started_at.elapsed().as_millis() as u64,
                    },
                    in_flight,
                );
            }
            Message::Evict { cell, reply } if self.state.is_active(&cell) => {
                let _ = reply.send(());
            }
            Message::Evict { cell, reply } => match self.state.phase(&cell) {
                Some(Phase::Resident { .. }) => {
                    self.eviction_waiters
                        .entry(cell.clone())
                        .or_default()
                        .push(reply);
                    self.drive(Event::Evict { cell }, in_flight);
                }
                Some(Phase::EnsuringDurability { op, .. }) => {
                    self.durability_waiters
                        .entry(*op)
                        .or_insert_with(|| (cell, Vec::new()))
                        .1
                        .push(reply);
                }
                Some(Phase::Cleaning {
                    op,
                    cause: StopCause::Evict { .. },
                    ..
                }) => {
                    self.eviction_stops.entry(*op).or_default().push(reply);
                }
                _ => {
                    let _ = reply.send(());
                }
            },
            Message::InvalidateRemote {
                cell,
                node,
                epoch,
                reply,
            } => {
                self.drive(Event::InvalidateRemote { cell, node, epoch }, in_flight);
                let _ = reply.send(());
            }
            Message::Snapshot { reply } => {
                let _ = reply.send(self.state_json());
            }
            Message::Health { reply } => {
                let _ = reply.send(self.state.ready_to_serve());
            }
            Message::Presence { reply } => {
                let _ = reply.send(self.state.presence_snapshot());
            }
        }
    }

    /// Settle a gated `webSocketMessage`'s durability. On success mark its
    /// barrier durable and flush the durable prefix in write order; on failure
    /// break the whole cell gate — drop every held frame and reset its sockets,
    /// since an unproven write must never leave an acknowledged trace.
    fn ws_release(&mut self, request: u64, ok: bool) {
        let Some(scope) = self.ws_gated.remove(&request) else {
            return;
        };
        let mut flush = Vec::new();
        let broke = {
            let Some(gate) = self.ws_gates.get_mut(&scope) else {
                return;
            };
            if let Some(barrier) = gate.barriers.iter_mut().find(|b| b.request == request) {
                barrier.settled = Some(ok);
            }
            if gate.barriers.iter().any(|b| b.settled == Some(false)) {
                true
            } else {
                while gate
                    .barriers
                    .front()
                    .is_some_and(|b| b.settled == Some(true))
                {
                    flush.push(gate.barriers.pop_front().unwrap().frames);
                }
                false
            }
        };
        if broke {
            self.ws_gates.remove(&scope);
            self.ws_gated.retain(|_, s| *s != scope);
            celld::js::ws_close_scope(&scope, 1011, "durability unproven");
            return;
        }
        for frames in flush {
            celld::js::ws_emit_batch(frames);
        }
        if self
            .ws_gates
            .get(&scope)
            .is_some_and(|g| g.barriers.is_empty())
        {
            self.ws_gates.remove(&scope);
        }
    }

    fn drive(&mut self, first: Event, in_flight: &mut FuturesUnordered<EffectFuture>) {
        let mut events = VecDeque::from([first]);
        while let Some(event) = events.pop_front() {
            let durability = match &event {
                Event::DurabilityChecked { op, .. } => Some(*op),
                // A deadline resolves the same operation the proof would
                // have, so it has to release the same waiters. Without this
                // the core abandons the eviction and the caller that asked
                // for it stays blocked on a proof that is no longer coming.
                Event::TimerFired {
                    timer: Timer::OperationDeadline { op },
                    ..
                } => Some(*op),
                _ => None,
            };
            let stopped = match &event {
                Event::RuntimeStopped { op } => Some(*op),
                _ => None,
            };
            let effects = on_event(&mut self.state, event);
            if let Some(op) = durability {
                if let Some((_, waiters)) = self.durability_waiters.remove(&op) {
                    let stop = effects.iter().find_map(|effect| match effect {
                        Effect::StopRuntime {
                            op,
                            cause: StopCause::Evict { .. },
                            ..
                        } => Some(*op),
                        _ => None,
                    });
                    if let Some(stop) = stop {
                        self.eviction_stops.entry(stop).or_default().extend(waiters);
                    } else {
                        for waiter in waiters {
                            let _ = waiter.send(());
                        }
                    }
                }
            }
            for effect in effects {
                self.execute(effect, &mut events, in_flight);
            }
            for (cell, timing) in &mut self.route_timings {
                if matches!(self.state.phase(cell), Some(Phase::WaitingCapacity)) {
                    timing
                        .capacity_wait_started
                        .get_or_insert_with(Instant::now);
                }
            }
            if let Some(op) = stopped {
                if let Some(waiters) = self.eviction_stops.remove(&op) {
                    for waiter in waiters {
                        let _ = waiter.send(());
                    }
                }
            }
            if self.validate_invariants {
                self.state.validate().expect("celld core invariant");
            }
        }
    }

    fn execute(
        &mut self,
        effect: Effect,
        immediate: &mut VecDeque<Event>,
        in_flight: &mut FuturesUnordered<EffectFuture>,
    ) {
        match effect {
            Effect::ScheduleTimer { timer, at_mono_ms } => {
                self.schedule_timer(timer, at_mono_ms);
            }
            Effect::ReadSelfNodeLease { op } => {
                let ownership = self.ownership.clone();
                let node = self.state.node().to_string();
                let started_at = self.started_at;
                in_flight.push(Box::pin(async move {
                    let result = ownership.read_self_node_lease(&node).await;
                    CompletedEffect::plain(Event::SelfNodeLeaseRead {
                        op,
                        now_ms: now_ms(),
                        now_mono_ms: started_at.elapsed().as_millis() as u64,
                        result,
                    })
                }));
            }
            Effect::CasNodeLease { op, guard, record } => {
                let ownership = self.ownership.clone();
                let started_at = self.started_at;
                in_flight.push(Box::pin(async move {
                    let result = ownership.cas_node_lease(guard, record).await;
                    CompletedEffect::plain(Event::NodeLeaseCasCompleted {
                        op,
                        now_mono_ms: started_at.elapsed().as_millis() as u64,
                        result,
                    })
                }));
            }
            Effect::ObserveNodeLeaseShadowRelease { sequence } => {
                tracing::info!(
                    event = "node_lease_shadow_release",
                    node = %self.state.node(),
                    sequence,
                    "lazy lease shadow mode would stop renewal"
                );
            }
            Effect::ObserveNodeLeaseReleased => {
                tracing::info!(
                    event = "node_lease_released",
                    node = %self.state.node(),
                    "no locally dependent cells; stopping node lease renewal"
                );
            }
            Effect::ReadOwner { op, cell } => {
                self.route_effect_started(&cell);
                let ownership = self.ownership.clone();
                let timing_cell = cell.clone();
                in_flight.push(Box::pin(async move {
                    let started = Instant::now();
                    let result = ownership.read_owner(&cell).await;
                    CompletedEffect::timed(
                        Event::OwnerRead {
                            op,
                            now_ms: now_ms(),
                            result,
                        },
                        timing_cell,
                        RouteStage::OwnershipRead,
                        started,
                    )
                }));
            }
            Effect::ReadNodeLease { op, cell, owner } => {
                self.route_effect_started(&cell);
                let ownership = self.ownership.clone();
                let timing_cell = cell;
                in_flight.push(Box::pin(async move {
                    let started = Instant::now();
                    let result = ownership.read_node_lease(&owner).await;
                    CompletedEffect::timed(
                        Event::NodeLeaseRead {
                            op,
                            now_ms: now_ms(),
                            result,
                        },
                        timing_cell,
                        RouteStage::NodeLeaseLookup,
                        started,
                    )
                }));
            }
            Effect::ReadCapacityPeers { op, cell } => {
                self.route_effect_started(&cell);
                let ownership = self.ownership.clone();
                in_flight.push(Box::pin(async move {
                    let result = ownership.read_capacity_peers().await;
                    CompletedEffect::plain(Event::CapacityPeersRead {
                        op,
                        now_ms: now_ms(),
                        result,
                    })
                }));
            }
            Effect::CasOwner {
                op,
                cell,
                guard,
                epoch,
                takeover,
            } => {
                self.route_effect_started(&cell);
                if let Some(timing) = self.route_timings.get_mut(&cell) {
                    timing
                        .fresh
                        .get_or_insert(!takeover && matches!(guard, CasGuard::Absent));
                }
                let ownership = self.ownership.clone();
                let timing_cell = cell.clone();
                in_flight.push(Box::pin(async move {
                    let started = Instant::now();
                    let result = ownership.cas_owner(&cell, guard, epoch).await;
                    CompletedEffect::timed(
                        Event::OwnerCasCompleted { op, result },
                        timing_cell,
                        RouteStage::OwnershipAcquire,
                        started,
                    )
                }));
            }
            Effect::ReconcileWakeEntry {
                cell,
                next_alarm_ms,
            } => {
                if let Some(runtime) = self.runtime.clone() {
                    // An ARMED alarm must always be reconcilable, tracked or
                    // not: belief can be lost while the alarm stands (a
                    // failed arm-time PUT, an entry retired under a racing
                    // arm), and skipping here would mean it is never
                    // re-asserted — entryless until the alarm fires or the
                    // cell moves. Gating the CONSUME side on tracking is
                    // what keeps alarm-less cells op-quiescent: untracked
                    // with no alarm, there is nothing to delete.
                    if next_alarm_ms >= 0 || celld::js::wake_entry_tracked(&cell) {
                        let epoch = runtime.published_epoch(&cell).unwrap_or(1);
                        tokio::spawn(async move {
                            // Only a consumed alarm needs the replication
                            // gate; an armed one is just re-stating its entry.
                            // Removing the hint while the commit that consumed
                            // the alarm is still local would lose both the
                            // alarm and the record that could revive it.
                            let consume_durable =
                                next_alarm_ms >= 0 || !runtime.sync_refused(&cell, epoch).await;
                            celld::js::reconcile_wake_entry(&cell, next_alarm_ms, consume_durable)
                                .await;
                        });
                    }
                }
            }
            Effect::ReleaseOwner { op, cell, epoch } => {
                let ownership = self.ownership.clone();
                in_flight.push(Box::pin(async move {
                    let result = ownership.release_owner(&cell, epoch).await;
                    CompletedEffect::plain(Event::OwnerReleased { op, result })
                }));
            }
            Effect::Restore { op, cell, spec } => {
                self.route_effect_started(&cell);
                if let Some(runtime) = self.runtime.clone() {
                    let timing_cell = cell.clone();
                    in_flight.push(Box::pin(async move {
                        let started = Instant::now();
                        let result = runtime.restore_cell(&cell, &spec).await.map_err(|error| {
                            eprintln!("celld restore failed for {cell}: {error:#}");
                            Failure::Definite
                        });
                        CompletedEffect::timed(
                            Event::RestoreCompleted { op, result },
                            timing_cell,
                            RouteStage::Restore,
                            started,
                        )
                    }));
                } else {
                    self.record_effect_timing(EffectTiming {
                        cell,
                        stage: RouteStage::Restore,
                        elapsed_us: 0,
                    });
                    immediate.push_back(Event::RestoreCompleted {
                        op,
                        result: Ok(celld_logic::RestoreOutcome {
                            restored: false,
                            alarm: None,
                        }),
                    });
                }
            }
            Effect::StartRuntime { op, cell, epoch } => {
                self.route_effect_started(&cell);
                if let Some(runtime) = self.runtime.clone() {
                    let fresh = self
                        .route_timings
                        .get(&cell)
                        .and_then(|timing| timing.fresh)
                        .unwrap_or(false);
                    let timing_cell = cell.clone();
                    in_flight.push(Box::pin(async move {
                        let started = Instant::now();
                        let result = runtime
                            .start_cell(cell.clone(), epoch, fresh)
                            .await
                            .map_err(|error| {
                                eprintln!("celld runtime start failed for {cell}: {error:#}");
                                Failure::Definite
                            });
                        CompletedEffect::timed(
                            Event::RuntimeStarted { op, result },
                            timing_cell,
                            RouteStage::IsolateStartup,
                            started,
                        )
                    }));
                } else {
                    self.record_effect_timing(EffectTiming {
                        cell,
                        stage: RouteStage::IsolateStartup,
                        elapsed_us: 0,
                    });
                    immediate.push_back(Event::RuntimeStarted { op, result: Ok(()) });
                }
            }
            Effect::Publish { op, cell, epoch } => {
                self.route_effect_started(&cell);
                let publish_started = Instant::now();
                self.publishes += 1;
                let result = if self.fail_publish_once {
                    self.fail_publish_once = false;
                    Err(Failure::Ambiguous)
                } else {
                    self.runtime
                        .as_ref()
                        .map_or(Ok(()), |runtime| runtime.publish_cell(&cell, epoch))
                        .map_err(|error| {
                            eprintln!("celld runtime publication failed for {cell}: {error:#}");
                            Failure::Definite
                        })
                };
                if result.is_ok() {
                    self.published.insert(cell.clone());
                }
                self.record_effect_timing(EffectTiming {
                    cell: cell.clone(),
                    stage: RouteStage::RegistryInsert,
                    elapsed_us: publish_started.elapsed().as_micros() as u64,
                });
                if result.is_ok() {
                    let node = self.state.node().to_string();
                    self.finish_route(&cell, "activated", "", Some((&node, epoch)));
                }
                immediate.push_back(Event::Published { op, result });
            }
            Effect::EnsureDurable { op, cell, epoch } => {
                let waiters = self.eviction_waiters.remove(&cell).unwrap_or_default();
                self.durability_waiters.insert(op, (cell.clone(), waiters));
                if let Some(runtime) = self.runtime.clone() {
                    in_flight.push(Box::pin(async move {
                        let result = runtime.ensure_durable(&cell, epoch).await.map_err(|error| {
                            eprintln!(
                                "celld durability proof failed for {cell} epoch {epoch}: {error:#}"
                            );
                            Failure::Ambiguous
                        });
                        CompletedEffect::plain(Event::DurabilityChecked { op, result })
                    }));
                } else {
                    immediate.push_back(Event::DurabilityChecked { op, result: Ok(()) });
                }
            }
            // The output gate: prove the cell durable, then let the core release
            // the held write response. Reuses the same recency-proving primitive
            // as EnsureDurable — a proof issued after the write covers it.
            Effect::AwaitDurable {
                op,
                cell,
                epoch,
                position,
            } => {
                if let Some(runtime) = self.runtime.clone() {
                    in_flight.push(Box::pin(async move {
                        // The replicator reports the position it actually proved
                        // durable; the core acks only if it covers this write.
                        let result = match runtime.await_durable(&cell, epoch, position).await {
                            Ok(durable) => Ok(durable),
                            Err(error) => {
                                eprintln!(
                                    "celld output-gate durability proof failed for {cell} \
                                     epoch {epoch}: {error:#}"
                                );
                                Err(Failure::Ambiguous)
                            }
                        };
                        CompletedEffect::plain(Event::DurableReached { op, result })
                    }));
                } else {
                    immediate.push_back(Event::DurableReached {
                        op,
                        result: Ok(position),
                    });
                }
            }
            Effect::ReleaseResponse { request, result } => {
                if self.ws_gated.contains_key(&request) {
                    self.ws_release(request, result.is_ok());
                } else if let Some(reply) = self.gated_responses.remove(&request) {
                    let _ = reply.send(result);
                }
            }
            Effect::StopRuntime {
                op,
                cell,
                epoch,
                cause,
            } => {
                self.stops += 1;
                let evicting = matches!(cause, StopCause::Evict { .. });
                // Handing the cell away makes the local snapshot dead weight;
                // keeping it is the whole point of an idle hibernation.
                let preserve_local = !matches!(cause, StopCause::Evict { rebalance: true });
                if evicting {
                    if let Some(live) = &self.live_load {
                        live.shed_cells.fetch_add(1, Ordering::Relaxed);
                    }
                }
                self.published.remove(&cell);
                if let Some(runtime) = self.runtime.clone() {
                    in_flight.push(Box::pin(async move {
                        runtime
                            .stop_cell(&cell, epoch, evicting, preserve_local)
                            .await;
                        CompletedEffect::plain(Event::RuntimeStopped { op })
                    }));
                } else {
                    immediate.push_back(Event::RuntimeStopped { op });
                }
            }
            Effect::FireAlarm {
                op,
                cell,
                scheduled_ms,
                ..
            } => {
                let started_at = self.started_at;
                if let Some(runtime) = self.runtime.clone() {
                    in_flight.push(Box::pin(async move {
                        let result =
                            runtime
                                .fire_alarm(cell, scheduled_ms)
                                .await
                                .map_err(|error| {
                                    eprintln!("celld alarm dispatch failed: {error:#}");
                                    Failure::Definite
                                });
                        CompletedEffect::plain(Event::AlarmFinished {
                            op,
                            now_ms: now_ms(),
                            now_mono_ms: started_at.elapsed().as_millis() as u64,
                            result,
                        })
                    }));
                } else {
                    immediate.push_back(Event::AlarmFinished {
                        op,
                        now_ms: now_ms(),
                        now_mono_ms: started_at.elapsed().as_millis() as u64,
                        result: Ok((None, true)),
                    });
                }
            }
            Effect::Complete { request, result } => {
                if let Some(cell) = self.request_cells.remove(&request) {
                    match &result {
                        Ok(Route::Remote { node, epoch, .. }) => {
                            self.finish_route(&cell, "remote_owner", "", Some((node, *epoch)));
                        }
                        Ok(Route::Local) => {
                            self.finish_route(&cell, "resident_after_wait", "", None);
                        }
                        Err(error) => {
                            self.finish_route(
                                &cell,
                                "route_error",
                                request_error_phase(*error),
                                None,
                            );
                        }
                    }
                }
                if let Some(reply) = self.pending.remove(&request) {
                    let local = result == Ok(Route::Local);
                    if reply
                        .send(result.map(|route| Routed { request, route }))
                        .is_err()
                        && local
                    {
                        immediate.push_back(Event::ActivityFinished { request });
                    }
                }
            }
            Effect::CompleteWorker { request, route } => {
                if let Some(op) = route.as_ref().and_then(|route| route.retired_durability) {
                    if let Some((_, waiters)) = self.durability_waiters.remove(&op) {
                        for waiter in waiters {
                            let _ = waiter.send(());
                        }
                    }
                }
                if let Some(reply) = self.pending_workers.remove(&request) {
                    let reserved = route.is_some();
                    if reply.send(WorkerRouted { request, route }).is_err() && reserved {
                        immediate.push_back(Event::ActivityFinished { request });
                    }
                }
            }
            Effect::CloseWebSocket { cell, websocket } => {
                // The core declined to hold this transport. Drop it and tell
                // the core it is gone, so the cell is not left believing it
                // has a socket the node already closed.
                eprintln!(
                    "celld refused an outbound WebSocket for {cell}: the node's \
                     outbound pin budget is spent"
                );
                celld::js::ws_unregister(websocket);
                immediate.push_back(Event::WebSocketClosed { cell, websocket });
            }
            Effect::Halt { code, reason } => {
                // Say why before going. Self-fencing is the most drastic thing
                // this process does, and an exit code on its own leaves an
                // operator to guess between a lease it could not renew, a
                // replicator that died, and a crash.
                match reason {
                    celld_logic::HaltReason::NodeLeaseExpired => tracing::warn!(
                        event = "node_lease_watchdog_fence",
                        code,
                        "SELF-FENCE: node lease not renewed within TTL — halting"
                    ),
                }
                let _ = self.fence.send(code);
            }
        }
    }

    fn state_json(&self) -> String {
        let residents = self
            .state
            .residents()
            .into_iter()
            .map(|cell| format!("{cell:?}"))
            .collect::<Vec<_>>()
            .join(",");
        let published = self
            .published
            .iter()
            .map(|cell| format!("{cell:?}"))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "{{\"ownership\":{:?},\"occupied\":{},\"shedding\":{},\"residents\":[{}],\"published\":[{}],\"publishes\":{},\"stops\":{}}}",
            self.ownership.name(),
            self.state.occupied(),
            self.state
                .shed_reason()
                .map_or_else(|| "null".to_string(), |reason| format!("{reason:?}")),
            residents,
            published,
            self.publishes,
            self.stops
        )
    }

    fn begin_route_if_cold(&mut self, cell: &str) {
        if !matches!(
            self.state.phase(cell),
            Some(Phase::Resident { .. } | Phase::EnsuringDurability { .. } | Phase::Remote { .. })
        ) {
            self.route_timings
                .entry(cell.to_string())
                .or_insert_with(CellRouteTiming::new);
        }
    }

    fn route_effect_started(&mut self, cell: &str) {
        if let Some(timing) = self.route_timings.get_mut(cell) {
            timing.effect_started();
        }
    }

    fn record_effect_timing(&mut self, completed: EffectTiming) {
        if let Some(timing) = self.route_timings.get_mut(&completed.cell) {
            timing.record(completed.stage, completed.elapsed_us);
        }
    }

    fn finish_route(
        &mut self,
        cell: &str,
        outcome: &str,
        failure_phase: &str,
        owner: Option<(&str, u64)>,
    ) {
        let Some(mut timing) = self.route_timings.remove(cell) else {
            return;
        };
        if let Some(started) = timing.capacity_wait_started.take() {
            timing.capacity_wait_us = timing
                .capacity_wait_us
                .saturating_add(started.elapsed().as_micros() as u64);
        }
        let (owner_node, epoch) = owner.unwrap_or(("", 0));
        tracing::debug!(
            target: "timing",
            event = "cell_route_timing",
            outcome,
            failure_phase,
            scope = %cell,
            node = %self.state.node(),
            region = %self.region,
            runtime_version = env!("CARGO_PKG_VERSION"),
            owner_node,
            epoch,
            fresh = timing.fresh.unwrap_or(false),
            total_us = timing.started.elapsed().as_micros() as u64,
            latch_wait_us = timing.latch_wait_us,
            ownership_read_us = timing.ownership_read_us,
            node_lease_lookup_us = timing.node_lease_lookup_us,
            capacity_lookup_us = timing.capacity_lookup_us,
            capacity_wait_us = timing.capacity_wait_us,
            activation_slot_wait_us = timing.activation_slot_wait_us,
            lease_permit_us = timing.lease_permit_us,
            ownership_acquire_us = timing.ownership_acquire_us,
            replica_discovery_us = timing.replica_discovery_us,
            restore_us = timing.restore_us,
            isolate_startup_us = timing.isolate_startup_us,
            registry_insert_us = timing.registry_insert_us,
            "cell route resolved"
        );
    }
}

fn request_error_phase(error: RequestError) -> &'static str {
    match error {
        RequestError::NodeUnavailable | RequestError::NodeFenced => "node_authority",
        RequestError::ResolveFailed | RequestError::PeerIncompatible => "ownership_lookup",
        RequestError::CapacityExhausted => "capacity_wait",
        RequestError::AcquireFailed => "ownership_acquire",
        RequestError::RestoreFailed => "restore",
        RequestError::RuntimeFailed => "isolate_startup",
        RequestError::PublishFailed => "registry_insert",
        RequestError::DurabilityUnproven => "output_gate",
    }
}

type HttpReply = Response<UnsyncBoxBody<Bytes, std::io::Error>>;

const STALE_ROUTE_HEADER: &str = "x-cells-route-error";
const STALE_ROUTE_VALUE: &str = "stale-owner";
const DURABLE_OBJECT_ROUTING_ERROR_MARKER: &str = "__CELLD_DO_ROUTING_ERROR__:";

fn owner_unreachable(scope: &str, owner: &str, source: anyhow::Error) -> anyhow::Error {
    // Record how the attempt failed, not just that it did. `connect` is the
    // one that decides whether a retry is safe -- a request that never left
    // this node may be re-sent, a truncated read may not, because the owner
    // already ran it. Without these an operator cannot tell an unreachable
    // peer from one that answered badly, and neither can a bug report.
    let transport = source.downcast_ref::<reqwest::Error>();
    let cause = source
        .source()
        .map(ToString::to_string)
        .unwrap_or_else(|| source.to_string());
    tracing::warn!(
        %scope,
        %owner,
        error = %source,
        %cause,
        connect = transport.is_some_and(reqwest::Error::is_connect),
        timeout = transport.is_some_and(reqwest::Error::is_timeout),
        request = transport.is_some_and(reqwest::Error::is_request),
        body = transport.is_some_and(reqwest::Error::is_body),
        decode = transport.is_some_and(reqwest::Error::is_decode),
        "peer owner unreachable"
    );
    let detail = serde_json::json!({
        "scope": scope,
        "owner": owner,
    });
    source.context(format!("{DURABLE_OBJECT_ROUTING_ERROR_MARKER}{detail}"))
}

fn response(status: StatusCode, body: impl Into<Bytes>) -> HttpReply {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(
            Full::new(body.into())
                .map_err(|never| match never {})
                .boxed_unsync(),
        )
        .expect("static HTTP response")
}

fn asset_response(response: axum::response::Response) -> HttpReply {
    response.map(|body| body.map_err(std::io::Error::other).boxed_unsync())
}

fn peer_response(mut response: HttpReply) -> HttpReply {
    response.headers_mut().insert(
        hyper::header::HeaderName::from_static(peer_auth::RESPONSE_VERSION_HEADER),
        hyper::header::HeaderValue::from_static(peer_auth::PROTOCOL_VERSION_TEXT),
    );
    response
}

fn runtime_response(worker_response: celld::js::HttpResponse) -> HttpReply {
    let Ok(status) = StatusCode::from_u16(worker_response.status) else {
        return response(StatusCode::INTERNAL_SERVER_ERROR, "invalid Worker status");
    };
    let mut builder = Response::builder().status(status);
    for (name, value) in worker_response.headers {
        if matches!(
            name.to_ascii_lowercase().as_str(),
            "connection" | "content-length" | "transfer-encoding"
        ) {
            continue;
        }
        builder = builder.header(name, value);
    }
    let body = match worker_response.stream {
        Some(stream) => {
            let chunks = stream.map(|chunk| {
                chunk
                    .map(|bytes| Frame::data(Bytes::from(bytes)))
                    .map_err(std::io::Error::other)
            });
            StreamBody::new(chunks).boxed_unsync()
        }
        None => Full::new(Bytes::from(worker_response.body))
            .map_err(|never| match never {})
            .boxed_unsync(),
    };
    builder
        .body(body)
        .unwrap_or_else(|_| response(StatusCode::INTERNAL_SERVER_ERROR, "invalid Worker headers"))
}

fn peer_runtime_response(worker_response: celld::js::HttpResponse) -> HttpReply {
    let wire_status = if worker_response.status == 101 && worker_response.ws.is_some() {
        StatusCode::OK
    } else {
        let Ok(status) = StatusCode::from_u16(worker_response.status) else {
            return peer_response(response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "invalid Worker status",
            ));
        };
        status
    };
    let mut builder = Response::builder().status(wire_status);
    for (name, value) in worker_response.headers {
        if matches!(
            name.to_ascii_lowercase().as_str(),
            "connection" | "content-length" | "transfer-encoding"
        ) {
            continue;
        }
        builder = builder.header(name, value);
    }
    if let Some(target) = worker_response.ws {
        if let Ok(value) = serde_json::to_string(&target) {
            builder = builder.header("x-celld-ws-target", value);
        }
    }
    // Say whether this body is streamed. The peer reads an unmarked body
    // rather than handing it on, so without this every response looked
    // buffered and streaming was off across the hop.
    if worker_response.stream.is_some() {
        builder = builder.header("x-celld-body-stream", "1");
    }
    let body = match worker_response.stream {
        Some(stream) => {
            let stream = stream.map(|chunk| {
                chunk
                    .map(|bytes| Frame::data(Bytes::from(bytes)))
                    .map_err(std::io::Error::other)
            });
            StreamBody::new(stream).boxed_unsync()
        }
        None => Full::new(Bytes::from(worker_response.body))
            .map_err(|never| match never {})
            .boxed_unsync(),
    };
    peer_response(builder.body(body).expect("Worker peer response"))
}

#[derive(Debug)]
struct StalePeerRoute {
    scope: String,
}

impl std::fmt::Display for StalePeerRoute {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "peer no longer owns {}", self.scope)
    }
}

impl std::error::Error for StalePeerRoute {}

#[derive(Debug)]
struct RoutedRequestError(RequestError);

impl std::fmt::Display for RoutedRequestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "route failed: {:?}", self.0)
    }
}

impl std::error::Error for RoutedRequestError {}

fn classify_remote_attempt(error: &anyhow::Error) -> celld_logic::routing::Attempt {
    if error.downcast_ref::<StalePeerRoute>().is_some() {
        celld_logic::routing::Attempt::NotOwner
    } else if error
        .downcast_ref::<reqwest::Error>()
        .is_some_and(reqwest::Error::is_connect)
    {
        celld_logic::routing::Attempt::NeverConnected
    } else {
        celld_logic::routing::Attempt::Ambiguous
    }
}

struct WebSocketRouteTiming {
    started: Instant,
    route_resolution_us: u64,
    dispatch_us: u64,
    attempts: u8,
}

impl WebSocketRouteTiming {
    fn emit(
        &self,
        app: &AppHandle,
        scope: &str,
        request_id: Option<celld::js::RequestId>,
        outcome: &str,
        route: &str,
        peer_node: &str,
    ) {
        let request_id = request_id
            .map(celld::js::request_id_string)
            .unwrap_or_default();
        let (node, region) = app
            .runtime
            .as_ref()
            .map_or(("", ""), |runtime| (runtime.node(), runtime.region()));
        tracing::debug!(
            target: "timing",
            event = "websocket_route_timing",
            outcome,
            route,
            peer_node,
            scope,
            request_id,
            node,
            region,
            runtime_version = env!("CARGO_PKG_VERSION"),
            attempts = self.attempts,
            total_us = self.started.elapsed().as_micros() as u64,
            route_resolution_us = self.route_resolution_us,
            dispatch_us = self.dispatch_us,
            "WebSocket cell request resolved"
        );
    }
}

/// The output gate for the co-hosted (same-isolate) Durable Object fast path.
/// That path runs a resident DO's `fetch` inline, bypassing `dispatch_do_call`;
/// when it writes, the inline handler calls here to hold its response until the
/// cell is durable. Reuses the routed machinery: `request` pins the cell (no
/// eviction mid-wait) and `gate_write` drives the core gate. `Ok` releases the
/// inline response; `Err` breaks the call, as a routed gate failure would.
async fn dispatch_gate(app: AppHandle, req: celld::js::GateReq) {
    if !app.output_gate {
        let _ = req.reply.send(Ok(()));
        return;
    }
    let routed = match app.request(req.scope.clone()).await {
        Ok(routed) => routed,
        Err(error) => {
            let _ = req.reply.send(Err(error));
            return;
        }
    };
    // The guard pins the cell and releases the request on drop, so the else
    // branch does not leak the just-acquired request.
    let _activity = app.activity(routed.request, req.scope.clone());
    let result = if routed.route == Route::Local {
        app.gate_write(routed.request, req.position).await
    } else {
        // The owning isolate should route the cell locally; if it moved off the
        // node mid-call, fail closed rather than acknowledge an unproven write.
        Err(RequestError::NodeFenced)
    };
    let _ = req.reply.send(result);
}

async fn dispatch_do_call(app: AppHandle, call: DoCallReq) {
    let DoCallReq {
        request_id,
        cancel,
        scope,
        name,
        url,
        method,
        body,
        headers,
        reply,
    } = call;
    let mut cancel = cancel;
    let mut websocket_timing = headers
        .iter()
        .any(|(name, value)| {
            name.eq_ignore_ascii_case("upgrade") && value.eq_ignore_ascii_case("websocket")
        })
        .then(|| WebSocketRouteTiming {
            started: Instant::now(),
            route_resolution_us: 0,
            dispatch_us: 0,
            attempts: 0,
        });
    let operation = async {
        let mut dispatcher = celld_logic::routing::Dispatcher::default();
        loop {
            if let Some(timing) = websocket_timing.as_mut() {
                timing.attempts = timing.attempts.saturating_add(1);
            }
            // Resolve ownership even if a cancellable caller has already
            // disconnected. Once a local route is definite, queue Fetch then
            // AbortFetch in order so a cold actor observes an incoming Request
            // whose signal is already aborted instead of never running at all.
            let route_started = Instant::now();
            let routed = match app.request(scope.clone()).await {
                Ok(routed) => routed,
                Err(error) => {
                    if let Some(timing) = websocket_timing.as_mut() {
                        timing.route_resolution_us = timing
                            .route_resolution_us
                            .saturating_add(route_started.elapsed().as_micros() as u64);
                        timing.emit(&app, &scope, request_id, "route_error", "", "");
                    }
                    break Err(anyhow::Error::new(RoutedRequestError(error)));
                }
            };
            if let Some(timing) = websocket_timing.as_mut() {
                timing.route_resolution_us = timing
                    .route_resolution_us
                    .saturating_add(route_started.elapsed().as_micros() as u64);
            }
            let Routed { request, route } = routed;
            let (node, addr, epoch, peer_protocol) = match route {
                Route::Local => {
                    let dispatch_started = Instant::now();
                    let _activity = app.activity(request, scope.clone());
                    let result = async {
                        let runtime = app.runtime.as_ref().context("no cell runtime")?;
                        let response = runtime
                            .fetch_cell(
                                scope.clone(),
                                name,
                                RuntimeFetch {
                                    url,
                                    method,
                                    body,
                                    headers,
                                    request_id,
                                },
                                cancel.take(),
                            )
                            .await?;
                        if let Some(target) = &response.ws {
                            let kind = if celld::js::ws_hibernatable(target.id).unwrap_or(false) {
                                WebSocketKind::Hibernatable
                            } else {
                                WebSocketKind::Regular
                            };
                            app.websocket_opened(target.scope.clone(), target.id, kind)
                                .await?;
                        }
                        Ok(response)
                    }
                    .await;
                    // Output gate (RPO=0): a handler that advanced the cell's
                    // write position has its response held until the core proves
                    // the cell durable. The request is still pinned (the
                    // activity guard has not dropped), so it fails rather than
                    // acknowledges a write the node cannot prove durable.
                    let result = match result {
                        Ok(response) => match response.write_position.filter(|_| app.output_gate) {
                            Some(position) => match app.gate_write(request, position).await {
                                Ok(()) => Ok(response),
                                Err(error) => Err(anyhow::Error::new(RoutedRequestError(error))),
                            },
                            None => Ok(response),
                        },
                        Err(error) => Err(error),
                    };
                    if let Some(timing) = websocket_timing.as_mut() {
                        timing.dispatch_us = timing
                            .dispatch_us
                            .saturating_add(dispatch_started.elapsed().as_micros() as u64);
                        timing.emit(
                            &app,
                            &scope,
                            request_id,
                            if result.is_ok() { "ok" } else { "error" },
                            "local",
                            app.runtime.as_ref().map_or("", RuntimeManager::node),
                        );
                    }
                    break result;
                }
                Route::Remote {
                    node,
                    addr,
                    epoch,
                    peer_protocol,
                } => (node, addr, epoch, peer_protocol),
            };
            let dispatch_started = Instant::now();
            let remote_call = async {
                anyhow::ensure!(
                    peer_protocol == peer_auth::PROTOCOL_VERSION,
                    "peer {node} speaks incompatible protocol {peer_protocol}"
                );
                let encoded = serde_json::json!({
                    "name": &name,
                    "url": &url,
                    "method": &method,
                    "bodyBase64": base64::engine::general_purpose::STANDARD.encode(&body),
                    "headers": &headers,
                    "requestId": request_id.map(celld::js::request_id_string),
                    "capacityHandoff": epoch == 0,
                });
                let encoded = serde_json::to_vec(&encoded)?;
                let path = format!("/__do/{scope}");
                let request = app.peer_auth.sign(
                    app.peer_http.post(format!("http://{addr}{path}")),
                    "POST",
                    &path,
                    &encoded,
                    &node,
                )?;
                let response =
                    request.body(encoded).send().await.map_err(|error| {
                        owner_unreachable(&scope, &addr, anyhow::Error::new(error))
                    })?;
                peer_auth::validate_response(response.headers())?;
                if response
                    .headers()
                    .get(STALE_ROUTE_HEADER)
                    .is_some_and(|value| value == STALE_ROUTE_VALUE)
                {
                    return Err(owner_unreachable(
                        &scope,
                        &addr,
                        anyhow::Error::new(StalePeerRoute {
                            scope: scope.clone(),
                        }),
                    ));
                }
                let mut websocket: Option<celld::js::WsTarget> = response
                    .headers()
                    .get("x-celld-ws-target")
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| serde_json::from_str(value).ok());
                if let Some(target) = websocket.as_mut() {
                    target.peer_node = Some(node.clone());
                    target.peer_addr = Some(addr.clone());
                    target.peer_epoch = Some(epoch);
                }
                let status = if websocket.is_some() && response.status() == StatusCode::OK {
                    101
                } else {
                    response.status().as_u16()
                };
                let headers = response
                    .headers()
                    .iter()
                    .filter(|(name, _)| {
                        name.as_str() != "x-celld-ws-target"
                            && name.as_str() != "x-celld-body-stream"
                    })
                    .filter_map(|(name, value)| {
                        value
                            .to_str()
                            .ok()
                            .map(|value| (name.to_string(), value.to_string()))
                    })
                    .collect();
                let (body, stream) = if websocket.is_some() {
                    (Vec::new(), None)
                } else {
                    // Every proxied body streams through, marked or not. The
                    // owner already ran the request, so a failure mid-body is
                    // ambiguous and must surface without a redispatch -- and
                    // streaming makes that structural: the 200 head has gone
                    // out before the failure is known, a chunk error aborts
                    // the client's read instead of ending it cleanly, and
                    // nothing upstream of a sent head can re-send. Buffering
                    // here was the old shape; it turned a truncated body into
                    // a whole-response failure after the owner had committed.
                    (
                        Vec::new(),
                        Some(celld::js::reqwest_response_stream(response)),
                    )
                };
                Ok(HttpResponse {
                    status,
                    headers,
                    body,
                    ws: websocket,
                    stream,
                    // A proxied remote response wrote on the owner, not here.
                    write_position: None,
                })
            };
            let remote = match cancel.as_mut() {
                Some(cancel) => tokio::select! {
                    remote = remote_call => remote,
                    _ = cancel => break Err(anyhow::anyhow!("Durable Object call cancelled")),
                },
                None => remote_call.await,
            };
            if let Some(timing) = websocket_timing.as_mut() {
                timing.dispatch_us = timing
                    .dispatch_us
                    .saturating_add(dispatch_started.elapsed().as_micros() as u64);
            }
            match remote {
                Ok(response) => {
                    if let Some(timing) = websocket_timing.as_ref() {
                        timing.emit(&app, &scope, request_id, "ok", "remote", &node);
                    }
                    break Ok(response);
                }
                Err(error) => {
                    // Epoch zero is a candidate, not an owner. A signed peer
                    // refusal proves this attempt did not execute and should
                    // not consume the ordinary one-owner stale-route budget;
                    // the core excludes that exact load sample before the
                    // next deterministic placement decision.
                    let capacity_refused = epoch == 0
                        && error
                            .chain()
                            .any(|cause| cause.downcast_ref::<StalePeerRoute>().is_some());
                    if capacity_refused {
                        app.invalidate_remote(scope.clone(), node, epoch).await;
                        continue;
                    }
                    let attempt = classify_remote_attempt(&error);
                    if matches!(
                        dispatcher.on_failure(attempt),
                        celld_logic::routing::Next::Redispatch { .. }
                    ) {
                        app.invalidate_remote(scope.clone(), node, epoch).await;
                        continue;
                    }
                    if let Some(timing) = websocket_timing.as_ref() {
                        timing.emit(&app, &scope, request_id, "error", "remote", &node);
                    }
                    break Err(error);
                }
            }
        }
    };
    let result = operation.await;
    let _ = reply.send(result);
}

async fn dispatch_rpc_call(app: AppHandle, call: RpcCallReq) {
    let RpcCallReq {
        scope,
        name,
        method,
        args,
        reply,
    } = call;
    let result = async {
        let mut dispatcher = celld_logic::routing::Dispatcher::default();
        loop {
            let Routed { request, route } = app
                .request(scope.clone())
                .await
                .map_err(|error| anyhow::anyhow!("route RPC {scope}: {error:?}"))?;
            let (node, addr, epoch, peer_protocol) = match route {
                Route::Local => {
                    let _activity = app.activity(request, scope.clone());
                    return app
                        .runtime
                        .as_ref()
                        .context("no cell runtime")?
                        .rpc(scope, name, method, args)
                        .await;
                }
                Route::Remote {
                    node,
                    addr,
                    epoch,
                    peer_protocol,
                } => (node, addr, epoch, peer_protocol),
            };
            anyhow::ensure!(
                peer_protocol == peer_auth::PROTOCOL_VERSION,
                "peer {node} speaks incompatible protocol {peer_protocol}"
            );
            let structured = matches!(args, celld::js::RpcData::V8(_));
            let envelope = match &args {
                celld::js::RpcData::Json(json) => serde_json::json!({
                    "name": &name,
                    "method": &method,
                    "args": serde_json::from_str::<serde_json::Value>(json)
                        .unwrap_or_else(|_| serde_json::json!([])),
                }),
                celld::js::RpcData::V8(bytes) => serde_json::json!({
                    "name": &name,
                    "method": &method,
                    "sc": base64::engine::general_purpose::STANDARD.encode(bytes),
                }),
            };
            let encoded = serde_json::to_vec(&envelope)?;
            let path = format!("/__rpc/{scope}");
            let request = app.peer_auth.sign(
                app.peer_http.post(format!("http://{addr}{path}")),
                "POST",
                &path,
                &encoded,
                &node,
            )?;
            let response = request.body(encoded).send().await;
            let response = match response {
                Ok(response) => response,
                Err(error) => {
                    let attempt = classify_remote_attempt(&anyhow::Error::new(error));
                    if matches!(
                        dispatcher.on_failure(attempt),
                        celld_logic::routing::Next::Redispatch { .. }
                    ) {
                        app.invalidate_remote(scope.clone(), node, epoch).await;
                        continue;
                    }
                    return Err(anyhow::anyhow!("remote RPC transport failed"));
                }
            };
            peer_auth::validate_response(response.headers())?;
            if response
                .headers()
                .get(STALE_ROUTE_HEADER)
                .is_some_and(|value| value == STALE_ROUTE_VALUE)
            {
                if matches!(
                    dispatcher.on_failure(celld_logic::routing::Attempt::NotOwner),
                    celld_logic::routing::Next::Redispatch { .. }
                ) {
                    app.invalidate_remote(scope.clone(), node, epoch).await;
                    continue;
                }
                anyhow::bail!("remote RPC owner was stale");
            }
            anyhow::ensure!(
                response.status().is_success(),
                "remote RPC failed with {}",
                response.status()
            );
            return Ok(if structured {
                celld::js::RpcData::V8(response.bytes().await?.to_vec())
            } else {
                celld::js::RpcData::Json(response.text().await?)
            });
        }
    }
    .await;
    let _ = reply.send(result);
}

async fn request_payload(
    request: Request<Incoming>,
) -> Result<(String, String, Vec<u8>, Vec<(String, String)>), HttpReply> {
    let (parts, body) = request.into_parts();
    let body = body.collect().await.map_err(|error| {
        response(
            StatusCode::BAD_REQUEST,
            format!("request body failed: {error}"),
        )
    })?;
    let headers = parts
        .headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.to_string(), value.to_string()))
        })
        .collect();
    Ok((
        request_url(&parts),
        parts.method.to_string(),
        body.to_bytes().to_vec(),
        headers,
    ))
}

/// `request.url` must name the URL the client asked for, because applications
/// route on its hostname and build absolute redirects from it. Cloudflare
/// reports the real scheme and host, so celld reads the same sources: the
/// absolute-form URI, then the pair an ingress proxy forwards, then Host.
/// `celld.local` remains the fallback for a request that carries none of them.
fn request_url(parts: &hyper::http::request::Parts) -> String {
    if parts.uri.authority().is_some() {
        return parts.uri.to_string();
    }
    let first = |name| {
        parts
            .headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(',').next())
            .map(str::trim)
            .filter(|value| !value.is_empty())
    };
    let host = first("x-forwarded-host")
        .or_else(|| first("host"))
        .unwrap_or("celld.local");
    let scheme = first("x-forwarded-proto").unwrap_or("http");
    format!("{scheme}://{host}{}", parts.uri)
}

async fn internal_do(request: Request<Incoming>, app: AppHandle, scope: String) -> HttpReply {
    let method = request.method().clone();
    let path_and_query = request
        .uri()
        .path_and_query()
        .map_or_else(|| request.uri().path().to_string(), ToString::to_string);
    let request_headers = request.headers().clone();
    let body = match request.into_body().collect().await {
        Ok(body) => body.to_bytes(),
        Err(error) => {
            return peer_response(response(
                StatusCode::BAD_REQUEST,
                format!("invalid body: {error}"),
            ));
        }
    };
    if let Err(error) = app.peer_auth.verify(
        &method,
        &path_and_query,
        &request_headers,
        &body,
        app.peer_auth.source(),
    ) {
        let mut denied = response(error.status(), error.message());
        if matches!(error, peer_auth::VerifyError::WrongTarget) {
            denied.headers_mut().insert(
                hyper::header::HeaderName::from_static(STALE_ROUTE_HEADER),
                hyper::header::HeaderValue::from_static(STALE_ROUTE_VALUE),
            );
        }
        return peer_response(denied);
    }
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(error) => {
            return peer_response(response(
                StatusCode::BAD_REQUEST,
                format!("invalid JSON: {error}"),
            ));
        }
    };
    let url = value
        .get("url")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("http://cell/")
        .to_string();
    let method = value
        .get("method")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("GET")
        .to_string();
    let name = value
        .get("name")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    let request_body = value
        .get("bodyBase64")
        .and_then(serde_json::Value::as_str)
        .and_then(|body| base64::engine::general_purpose::STANDARD.decode(body).ok())
        .unwrap_or_default();
    let headers = serde_json::from_value(
        value
            .get("headers")
            .cloned()
            .unwrap_or_else(|| serde_json::json!([])),
    )
    .unwrap_or_default();
    let request_id = value
        .get("requestId")
        .and_then(serde_json::Value::as_str)
        .and_then(celld::js::parse_request_id);
    let capacity_handoff = value
        .get("capacityHandoff")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let routed = if capacity_handoff {
        app.capacity_request(scope.clone()).await
    } else {
        app.request(scope.clone()).await
    };
    let result = match routed {
        Ok(Routed {
            request,
            route: Route::Local,
        }) => {
            let _activity = app.activity(request, scope.clone());
            let Some(runtime) = &app.runtime else {
                return response(StatusCode::SERVICE_UNAVAILABLE, "no cell runtime");
            };
            let abort_scope = scope.clone();
            // The forwarding node hangs up when its own client does, so this
            // connection going away is the cancellation signal reaching the
            // owner -- and it arrives as a drop, which is why it is a guard
            // rather than the channel `fetch_cell` takes. Without it a handler
            // keeps running on the owner for a client that left the node it
            // dialled.
            let mut abort = AbortPeerFetchOnHangUp {
                runtime: runtime.clone(),
                scope: abort_scope,
                request_id,
            };
            match runtime
                .fetch_cell(
                    scope,
                    name,
                    RuntimeFetch {
                        url,
                        method,
                        body: request_body,
                        headers,
                        request_id,
                    },
                    None,
                )
                .await
            {
                Ok(worker_response) => {
                    abort.request_id = None;
                    // Output gate (RPO=0): a peer-served handler that advanced
                    // the cell's committed position holds its reply until the
                    // cell is proven durable, exactly as the local dispatch
                    // path does. This path used to acknowledge unproven writes
                    // — the loss the takeover tests catch. The activity guard
                    // is still alive, so the request stays pinned across the
                    // wait.
                    if let Some(position) =
                        worker_response.write_position.filter(|_| app.output_gate)
                    {
                        if let Err(error) = app.gate_write(request, position).await {
                            return peer_response(response(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                format!("durability unproven: {error:?}"),
                            ));
                        }
                    }
                    if let Some(target) = &worker_response.ws {
                        let kind = if celld::js::ws_hibernatable(target.id).unwrap_or(false) {
                            WebSocketKind::Hibernatable
                        } else {
                            WebSocketKind::Regular
                        };
                        if let Err(error) = app
                            .websocket_opened(target.scope.clone(), target.id, kind)
                            .await
                        {
                            return peer_response(response(
                                StatusCode::SERVICE_UNAVAILABLE,
                                format!("WebSocket core registration failed: {error:#}"),
                            ));
                        }
                    }
                    peer_runtime_response(worker_response)
                }
                Err(error) => response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("cell Worker failed: {error:#}"),
                ),
            }
        }
        Ok(Routed {
            route: Route::Remote { .. },
            ..
        }) => {
            let mut stale = response(StatusCode::CONFLICT, "stale route");
            stale.headers_mut().insert(
                hyper::header::HeaderName::from_static(STALE_ROUTE_HEADER),
                hyper::header::HeaderValue::from_static(STALE_ROUTE_VALUE),
            );
            stale
        }
        Err(RequestError::CapacityExhausted) => {
            let mut stale = response(StatusCode::CONFLICT, "capacity exhausted");
            stale.headers_mut().insert(
                hyper::header::HeaderName::from_static(STALE_ROUTE_HEADER),
                hyper::header::HeaderValue::from_static(STALE_ROUTE_VALUE),
            );
            stale
        }
        Err(error) => response(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("route failed: {error:?}"),
        ),
    };
    peer_response(result)
}

async fn internal_abort(request: Request<Incoming>, app: AppHandle, path: String) -> HttpReply {
    let (parts, body) = request.into_parts();
    let body = match body.collect().await {
        Ok(body) => body.to_bytes(),
        Err(error) => {
            return peer_response(response(
                StatusCode::BAD_REQUEST,
                format!("invalid abort body: {error}"),
            ));
        }
    };
    let path_and_query = parts
        .uri
        .path_and_query()
        .map_or_else(|| parts.uri.path().to_string(), ToString::to_string);
    if let Err(error) = app.peer_auth.verify(
        &parts.method,
        &path_and_query,
        &parts.headers,
        &body,
        app.peer_auth.source(),
    ) {
        let mut denied = response(error.status(), error.message());
        if matches!(error, peer_auth::VerifyError::WrongTarget) {
            denied.headers_mut().insert(
                hyper::header::HeaderName::from_static(STALE_ROUTE_HEADER),
                hyper::header::HeaderValue::from_static(STALE_ROUTE_VALUE),
            );
        }
        return peer_response(denied);
    }
    if parts.method != hyper::Method::POST {
        return peer_response(response(
            StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed",
        ));
    }
    let Some((encoded_scope, encoded_request)) = path
        .strip_prefix("/__abort/")
        .and_then(|rest| rest.rsplit_once('/'))
    else {
        return peer_response(response(StatusCode::BAD_REQUEST, "invalid abort target"));
    };
    let scope = match percent_encoding::percent_decode_str(encoded_scope).decode_utf8() {
        Ok(scope) => scope.into_owned(),
        Err(_) => return peer_response(response(StatusCode::BAD_REQUEST, "invalid abort scope")),
    };
    let Some(request_id) = celld::js::parse_request_id(encoded_request) else {
        return peer_response(response(StatusCode::BAD_REQUEST, "invalid request id"));
    };
    let result = match app.request(scope.clone()).await {
        Ok(Routed {
            request,
            route: Route::Local,
        }) => {
            let _activity = app.activity(request, scope.clone());
            match &app.runtime {
                Some(runtime) => {
                    runtime.abort_fetch(&scope, request_id);
                    response(StatusCode::NO_CONTENT, Bytes::new())
                }
                None => response(StatusCode::SERVICE_UNAVAILABLE, "no cell runtime"),
            }
        }
        Ok(Routed {
            route: Route::Remote { .. },
            ..
        }) => {
            let mut stale = response(StatusCode::CONFLICT, "stale route");
            stale.headers_mut().insert(
                hyper::header::HeaderName::from_static(STALE_ROUTE_HEADER),
                hyper::header::HeaderValue::from_static(STALE_ROUTE_VALUE),
            );
            stale
        }
        Err(error) => response(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("abort route failed: {error:?}"),
        ),
    };
    peer_response(result)
}

async fn internal_rpc(request: Request<Incoming>, app: AppHandle, scope: String) -> HttpReply {
    let method = request.method().clone();
    let path_and_query = request
        .uri()
        .path_and_query()
        .map_or_else(|| request.uri().path().to_string(), ToString::to_string);
    let request_headers = request.headers().clone();
    let body = match request.into_body().collect().await {
        Ok(body) => body.to_bytes(),
        Err(error) => {
            return peer_response(response(
                StatusCode::BAD_REQUEST,
                format!("invalid body: {error}"),
            ));
        }
    };
    if let Err(error) = app.peer_auth.verify(
        &method,
        &path_and_query,
        &request_headers,
        &body,
        app.peer_auth.source(),
    ) {
        let mut denied = response(error.status(), error.message());
        if matches!(error, peer_auth::VerifyError::WrongTarget) {
            denied.headers_mut().insert(
                hyper::header::HeaderName::from_static(STALE_ROUTE_HEADER),
                hyper::header::HeaderValue::from_static(STALE_ROUTE_VALUE),
            );
        }
        return peer_response(denied);
    }
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(error) => {
            return peer_response(response(
                StatusCode::BAD_REQUEST,
                format!("invalid JSON: {error}"),
            ));
        }
    };
    let name = value
        .get("name")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    let rpc_method = value
        .get("method")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();
    let args = match value.get("sc").and_then(serde_json::Value::as_str) {
        Some(bytes) => celld::js::RpcData::V8(
            base64::engine::general_purpose::STANDARD
                .decode(bytes)
                .unwrap_or_default(),
        ),
        None => celld::js::RpcData::Json(
            value
                .get("args")
                .cloned()
                .unwrap_or_else(|| serde_json::json!([]))
                .to_string(),
        ),
    };
    let result = match app.request(scope.clone()).await {
        Ok(Routed {
            request,
            route: Route::Local,
        }) => {
            let _activity = app.activity(request, scope.clone());
            let Some(runtime) = &app.runtime else {
                return peer_response(response(StatusCode::SERVICE_UNAVAILABLE, "no cell runtime"));
            };
            runtime.rpc(scope, name, rpc_method, args).await
        }
        Ok(Routed {
            route: Route::Remote { .. },
            ..
        }) => {
            let mut stale = response(StatusCode::CONFLICT, "stale route");
            stale.headers_mut().insert(
                hyper::header::HeaderName::from_static(STALE_ROUTE_HEADER),
                hyper::header::HeaderValue::from_static(STALE_ROUTE_VALUE),
            );
            return peer_response(stale);
        }
        Err(error) => Err(anyhow::anyhow!("route failed: {error:?}")),
    };
    match result {
        Ok(celld::js::RpcData::Json(json)) => peer_response(
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(
                    Full::new(Bytes::from(json))
                        .map_err(|never| match never {})
                        .boxed_unsync(),
                )
                .expect("RPC JSON response"),
        ),
        Ok(celld::js::RpcData::V8(bytes)) => peer_response(
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/octet-stream")
                .body(
                    Full::new(Bytes::from(bytes))
                        .map_err(|never| match never {})
                        .boxed_unsync(),
                )
                .expect("RPC clone response"),
        ),
        Err(error) => peer_response(response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("RPC failed: {error:#}"),
        )),
    }
}

async fn dispatch_ws_message(
    app: &AppHandle,
    scope: &str,
    ws_id: u64,
    data: celld::js::WsIn,
) -> anyhow::Result<()> {
    let Routed { request, route } = app
        .request(scope.to_string())
        .await
        .map_err(|error| anyhow::anyhow!("route WebSocket {scope}: {error:?}"))?;
    anyhow::ensure!(route == Route::Local, "WebSocket owner moved off node");
    let activity = app.activity(request, scope.to_string());
    let dispatch = app
        .runtime
        .as_ref()
        .context("no cell runtime")?
        .ws_message(scope.to_string(), ws_id, data)
        .await?;
    // The gate captured the handler's outbound frames. With the gate armed, hand
    // them to the cell's barrier queue; else flush them as the handler produced
    // them. Either way the frames only reach a socket from here.
    if !app.output_gate {
        celld::js::ws_emit_batch(dispatch.frames);
    } else if !dispatch.frames.is_empty() || dispatch.write_position.is_some() {
        app.ws_output(
            request,
            scope.to_string(),
            dispatch.frames,
            dispatch.write_position,
        )
        .await;
    }
    drop(activity);
    Ok(())
}

async fn dispatch_ws_closed(
    app: &AppHandle,
    scope: &str,
    ws_id: u64,
    code: u16,
    reason: String,
    was_clean: bool,
) -> anyhow::Result<()> {
    let Routed { request, route } = app
        .request(scope.to_string())
        .await
        .map_err(|error| anyhow::anyhow!("route WebSocket close {scope}: {error:?}"))?;
    anyhow::ensure!(route == Route::Local, "WebSocket owner moved off node");
    let _activity = app.activity(request, scope.to_string());
    app.runtime
        .as_ref()
        .context("no cell runtime")?
        .ws_closed(scope.to_string(), ws_id, code, reason, was_clean)
        .await
}

async fn finish_websocket(
    app: &AppHandle,
    target: &celld::js::WsTarget,
    code: u16,
    reason: String,
    was_clean: bool,
) {
    let _ = dispatch_ws_closed(app, &target.scope, target.id, code, reason, was_clean).await;
    app.websocket_closed(target.scope.clone(), target.id);
    celld::js::ws_unregister(target.id);
}

enum OutboundWebSocketSink {
    Cell { app: Box<AppHandle>, scope: String },
    Isolate(celld::js::WsPullSender),
}

impl OutboundWebSocketSink {
    async fn open(&self, websocket: u64, protocol: String) -> anyhow::Result<()> {
        match self {
            Self::Cell { app, scope } => {
                let Routed { request, route } = app
                    .request(scope.clone())
                    .await
                    .map_err(|error| anyhow::anyhow!("route outbound WebSocket: {error:?}"))?;
                anyhow::ensure!(
                    route == Route::Local,
                    "outbound WebSocket cell moved off node"
                );
                let _activity = app.activity(request, scope.clone());
                app.websocket_opened(scope.clone(), websocket, WebSocketKind::Outbound)
                    .await?;
                let result = app
                    .runtime
                    .as_ref()
                    .context("no cell runtime")?
                    .ws_open(scope.clone(), websocket, protocol)
                    .await;
                if result.is_err() {
                    app.websocket_closed(scope.clone(), websocket);
                }
                result
            }
            Self::Isolate(tx) => tx
                .send(celld::js::WsPull::Open(protocol))
                .map_err(|_| anyhow::anyhow!("isolate stopped reading WebSocket")),
        }
    }

    async fn message(&self, websocket: u64, data: celld::js::WsIn) -> anyhow::Result<()> {
        match self {
            Self::Cell { app, scope } => dispatch_ws_message(app, scope, websocket, data).await,
            Self::Isolate(tx) => tx
                .send(match data {
                    celld::js::WsIn::Text(text) => celld::js::WsPull::Text(text),
                    celld::js::WsIn::Binary(bytes) => celld::js::WsPull::Binary(bytes),
                })
                .map_err(|_| anyhow::anyhow!("isolate stopped reading WebSocket")),
        }
    }

    async fn closed(
        &self,
        websocket: u64,
        code: u16,
        reason: String,
        was_clean: bool,
    ) -> anyhow::Result<()> {
        match self {
            Self::Cell { app, scope } => {
                let result =
                    dispatch_ws_closed(app, scope, websocket, code, reason, was_clean).await;
                app.websocket_closed(scope.clone(), websocket);
                result
            }
            Self::Isolate(tx) => tx
                .send(celld::js::WsPull::Close(code, reason, was_clean))
                .map_err(|_| anyhow::anyhow!("isolate stopped reading WebSocket")),
        }
    }

    fn scope(&self) -> &str {
        match self {
            Self::Cell { scope, .. } => scope,
            Self::Isolate(_) => "",
        }
    }
}

async fn outbound_websocket_task(
    app: AppHandle,
    request: celld::js::OutboundWsReq,
) -> anyhow::Result<()> {
    use fastwebsockets::OpCode;
    use hyper::header::{HeaderMap, HeaderName, HeaderValue, SEC_WEBSOCKET_PROTOCOL};

    let celld::js::OutboundWsReq {
        scope,
        id,
        url,
        protocols,
        pull,
        headers,
        want_response,
        reply,
    } = request;
    let sink = match pull {
        Some(pull) => OutboundWebSocketSink::Isolate(pull),
        None => OutboundWebSocketSink::Cell {
            app: Box::new(app),
            scope: scope.clone(),
        },
    };
    let mut handshake = HeaderMap::new();
    for (name, value) in &headers {
        if matches!(
            name.to_ascii_lowercase().as_str(),
            "upgrade"
                | "connection"
                | "sec-websocket-key"
                | "sec-websocket-version"
                | "sec-websocket-protocol"
                | "host"
        ) {
            continue;
        }
        let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) else {
            continue;
        };
        handshake.insert(name, value);
    }
    if !protocols.is_empty() {
        handshake.insert(
            SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_str(&protocols.join(", "))
                .context("invalid WebSocket subprotocol")?,
        );
    }
    let timeout = std::time::Duration::from_secs(10);
    let connected = tokio::time::timeout(timeout, celld::ws_client::connect(&url, handshake)).await;
    let connection = match connected {
        Ok(Ok(connection)) => connection,
        Ok(Err(celld::ws_client::Error::Declined(declined))) if want_response => {
            let declined = celld::js::DeclinedUpgrade {
                status: declined.status.as_u16(),
                headers: declined
                    .headers
                    .iter()
                    .map(|(name, value)| {
                        (
                            name.as_str().to_string(),
                            value.to_str().unwrap_or_default().to_string(),
                        )
                    })
                    .collect(),
                body: declined.body,
            };
            let _ = reply.send(Ok(celld::js::OutboundWsOpen {
                protocol: None,
                declined: Some(declined),
            }));
            return Ok(());
        }
        Ok(Err(error)) => {
            let _ = reply.send(Err(anyhow::anyhow!("{error}")));
            return Ok(());
        }
        Err(_) => {
            let _ = reply.send(Err(anyhow::anyhow!(
                "outbound WebSocket handshake timed out after {}ms",
                timeout.as_millis()
            )));
            return Ok(());
        }
    };
    let mut socket = fastwebsockets::FragmentCollector::new(connection.socket);
    let protocol = connection
        .headers
        .get(SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    if protocol
        .as_ref()
        .is_some_and(|selected| !protocols.iter().any(|offered| offered == selected))
    {
        let _ = reply.send(Err(anyhow::anyhow!(
            "server selected an unrequested WebSocket subprotocol"
        )));
        return Ok(());
    }

    let (outbound, mut outputs) = mpsc::unbounded_channel();
    if matches!(sink, OutboundWebSocketSink::Cell { .. }) {
        celld::js::ws_register_outbound(id, sink.scope());
    }
    celld::js::ws_register(id, outbound);
    if let Err(error) = sink
        .open(id, protocol.as_deref().unwrap_or_default().to_string())
        .await
    {
        celld::js::ws_unregister(id);
        let _ = reply.send(Err(error));
        return Ok(());
    }
    if reply
        .send(Ok(celld::js::OutboundWsOpen {
            protocol,
            declined: None,
        }))
        .is_err()
    {
        let _ = sink
            .closed(id, 1006, "opening event was cancelled".into(), false)
            .await;
        celld::js::ws_unregister(id);
        return Ok(());
    }

    let mut close = (1006, String::new(), false);
    loop {
        tokio::select! {
            // Ping is answered by the collector's auto-pong, as the previous
            // client library also did unprompted.
            incoming = socket.read_frame() => {
                let Ok(frame) = incoming else { break };
                let delivered = match frame.opcode {
                    OpCode::Text => sink.message(
                        id,
                        celld::js::WsIn::Text(
                            String::from_utf8_lossy(&frame.payload).into_owned(),
                        ),
                    ).await,
                    OpCode::Binary => sink.message(
                        id,
                        celld::js::WsIn::Binary(frame.payload.to_vec()),
                    ).await,
                    OpCode::Close => {
                        close = websocket_close_details(&frame.payload);
                        break;
                    }
                    _ => Ok(()),
                };
                if delivered.is_err() { break; }
            }
            output = outputs.recv() => {
                let Some(output) = output else { break };
                if let celld::js::WsOut::Close(code, reason) = &output {
                    close = (*code, reason.clone(), true);
                }
                if !write_ws_out(&mut socket, output).await { break; }
            }
        }
    }
    let _ = sink.closed(id, close.0, close.1, close.2).await;
    celld::js::ws_unregister(id);
    Ok(())
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
    out: celld::js::WsOut,
) -> bool
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use fastwebsockets::Frame;
    let keep_open = matches!(out, celld::js::WsOut::Text(_) | celld::js::WsOut::Binary(_));
    let frame = match out {
        celld::js::WsOut::Text(text) => Frame::text(text.into_bytes().into()),
        celld::js::WsOut::Binary(data) => Frame::binary(data.into()),
        celld::js::WsOut::Close(code, reason) => Frame::close(code, reason.as_bytes()),
    };
    ws.write_frame(frame).await.is_ok() && keep_open
}

async fn websocket_task<S>(
    app: AppHandle,
    target: celld::js::WsTarget,
    mut socket: fastwebsockets::WebSocket<S>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use fastwebsockets::OpCode;
    socket.set_auto_close(false);
    let mut socket = fastwebsockets::FragmentCollector::new(socket);
    let (outbound, mut outputs) = mpsc::unbounded_channel();
    celld::js::ws_register(target.id, outbound);
    let mut close = (1006, String::new(), false);
    loop {
        tokio::select! {
            frame = socket.read_frame() => {
                let Ok(frame) = frame else { break };
                let result = match frame.opcode {
                    OpCode::Text => dispatch_ws_message(
                        &app,
                        &target.scope,
                        target.id,
                        celld::js::WsIn::Text(String::from_utf8_lossy(&frame.payload).into_owned()),
                    ).await,
                    OpCode::Binary => dispatch_ws_message(
                        &app,
                        &target.scope,
                        target.id,
                        celld::js::WsIn::Binary(frame.payload.to_vec()),
                    ).await,
                    OpCode::Close => {
                        close = websocket_close_details(&frame.payload);
                        break;
                    }
                    _ => Ok(()),
                };
                if result.is_err() { break; }
            }
            output = outputs.recv() => {
                let Some(output) = output else { break };
                if !write_ws_out(&mut socket, output).await { break; }
            }
        }
    }
    if let Err(error) = dispatch_ws_closed(
        &app,
        &target.scope,
        target.id,
        close.0,
        close.1.clone(),
        close.2,
    )
    .await
    {
        tracing::warn!(
            %error,
            scope = %target.scope,
            websocket = target.id,
            "WebSocket close dispatch failed"
        );
    }

    // The close handler is allowed to choose the response code and reason.
    // Its output is queued while dispatch_ws_closed drives V8, so flush it
    // before unregistering the socket or considering a protocol-level echo.
    let mut handler_sent_close = false;
    while let Ok(output) = outputs.try_recv() {
        handler_sent_close |= matches!(output, celld::js::WsOut::Close(_, _));
        if !write_ws_out(&mut socket, output).await {
            break;
        }
    }
    if let Some(code) =
        celld_logic::schedule::websocket_echo_close(close.0, close.2, handler_sent_close)
    {
        let _ = write_ws_out(&mut socket, celld::js::WsOut::Close(code, close.1)).await;
    }
    app.websocket_closed(target.scope.clone(), target.id);
    celld::js::ws_unregister(target.id);
}

async fn remote_websocket_task<S>(
    app: AppHandle,
    target: celld::js::WsTarget,
    mut client: fastwebsockets::WebSocket<S>,
) -> anyhow::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use fastwebsockets::{FragmentCollector, Frame as WsFrame, OpCode};

    let mut node = target
        .peer_node
        .clone()
        .context("remote WebSocket target has no node")?;
    let mut addr = target
        .peer_addr
        .clone()
        .context("remote WebSocket target has no address")?;
    let mut epoch = target
        .peer_epoch
        .context("remote WebSocket target has no owner epoch")?;
    let mut dispatcher = celld_logic::routing::Dispatcher::default();
    let mut peer = loop {
        let path = format!("/__ws/{}?id={}&epoch={epoch}", target.scope, target.id);
        let signed = app.peer_auth.signed_headers("GET", &path, &[], &node)?;
        match celld::ws_client::connect(&format!("ws://{addr}{path}"), signed).await {
            Ok(peer) => {
                peer_auth::validate_response(&peer.headers)?;
                break peer.socket;
            }
            Err(celld::ws_client::Error::Declined(declined))
                if declined
                    .headers
                    .get(STALE_ROUTE_HEADER)
                    .is_some_and(|value| value == STALE_ROUTE_VALUE) =>
            {
                let celld_logic::routing::Next::Redispatch { .. } =
                    dispatcher.on_failure(celld_logic::routing::Attempt::NotOwner)
                else {
                    anyhow::bail!("stale WebSocket route retry exhausted for {}", target.scope);
                };
                app.invalidate_remote(target.scope.clone(), node.clone(), epoch)
                    .await;
                let routed = app
                    .request(target.scope.clone())
                    .await
                    .map_err(|error| anyhow::anyhow!("refresh WebSocket route: {error:?}"))?;
                match routed.route {
                    Route::Remote {
                        node: fresh_node,
                        addr: fresh_addr,
                        epoch: fresh_epoch,
                        peer_protocol,
                    } => {
                        anyhow::ensure!(
                            peer_protocol == peer_auth::PROTOCOL_VERSION,
                            "peer {fresh_node} speaks incompatible protocol {peer_protocol}"
                        );
                        node = fresh_node;
                        addr = fresh_addr;
                        epoch = fresh_epoch;
                    }
                    Route::Local => {
                        drop(app.activity(routed.request, target.scope.clone()));
                        anyhow::bail!(
                            "WebSocket ownership moved local after remote target was created"
                        );
                    }
                }
            }
            Err(error) => {
                return Err(anyhow::anyhow!("{error}"))
                    .with_context(|| format!("connect WebSocket tunnel to peer {node} at {addr}"));
            }
        }
    };

    // Both halves speak the same implementation, so a frame crosses the hop
    // as itself. The one rewrite left is the close code: 1005 means "no code
    // was sent" and is not transmissible, so it becomes a plain 1000.
    client.set_auto_close(false);
    peer.set_auto_close(false);
    let mut client = FragmentCollector::new(client);
    let mut peer = FragmentCollector::new(peer);
    let mut client_open = true;
    loop {
        tokio::select! {
            frame = client.read_frame(), if client_open => {
                let frame = frame.context("read client WebSocket frame")?;
                let frame = match frame.opcode {
                    OpCode::Text | OpCode::Binary | OpCode::Ping | OpCode::Pong => frame,
                    OpCode::Close => {
                        client_open = false;
                        let (code, reason, _) = websocket_close_details(&frame.payload);
                        WsFrame::close(if code == 1005 { 1000 } else { code }, reason.as_bytes())
                    }
                    _ => continue,
                };
                if peer.write_frame(frame).await.is_err() {
                    if client_open {
                        let _ = client
                            .write_frame(WsFrame::close(1012, b"owner unavailable"))
                            .await;
                    }
                    break;
                }
            }
            frame = peer.read_frame() => {
                let frame = match frame {
                    Ok(frame) => frame,
                    Err(error) => {
                        if client_open {
                            let _ = client
                                .write_frame(WsFrame::close(1012, b"owner unavailable"))
                                .await;
                        }
                        return Err(anyhow::anyhow!("{error}"))
                            .context("read owner WebSocket frame");
                    }
                };
                let closing = frame.opcode == OpCode::Close;
                let frame = if closing {
                    let (code, reason, _) = websocket_close_details(&frame.payload);
                    WsFrame::close(if code == 1005 { 1000 } else { code }, reason.as_bytes())
                } else {
                    frame
                };
                client
                    .write_frame(frame)
                    .await
                    .context("write client WebSocket frame")?;
                if closing {
                    break;
                }
            }
        }
    }
    Ok(())
}

async fn handle_peer_websocket(
    mut request: Request<Incoming>,
    app: AppHandle,
    path: &str,
) -> HttpReply {
    let path_and_query = request
        .uri()
        .path_and_query()
        .map_or_else(|| path.to_string(), ToString::to_string);
    if let Err(error) = app.peer_auth.verify(
        request.method(),
        &path_and_query,
        request.headers(),
        &[],
        app.peer_auth.source(),
    ) {
        return peer_response(response(error.status(), error.message()));
    }
    let Some(encoded_scope) = path.strip_prefix("/__ws/") else {
        return peer_response(response(StatusCode::NOT_FOUND, "missing WebSocket scope"));
    };
    let scope = match percent_encoding::percent_decode_str(encoded_scope).decode_utf8() {
        Ok(scope) => scope.into_owned(),
        Err(_) => return peer_response(response(StatusCode::BAD_REQUEST, "invalid scope")),
    };
    let query: BTreeMap<String, String> = request
        .uri()
        .query()
        .map(|query| {
            url::form_urlencoded::parse(query.as_bytes())
                .into_owned()
                .collect()
        })
        .unwrap_or_default();
    let websocket = query.get("id").and_then(|value| value.parse::<u64>().ok());
    let epoch = query
        .get("epoch")
        .and_then(|value| value.parse::<u64>().ok());
    let (Some(websocket), Some(epoch)) = (websocket, epoch) else {
        return peer_response(response(
            StatusCode::BAD_REQUEST,
            "invalid WebSocket target",
        ));
    };
    let Some(runtime) = &app.runtime else {
        return peer_response(response(StatusCode::SERVICE_UNAVAILABLE, "no cell runtime"));
    };
    if runtime.published_epoch(&scope) != Some(epoch) {
        let mut stale = peer_response(response(StatusCode::CONFLICT, "stale route"));
        stale.headers_mut().insert(
            hyper::header::HeaderName::from_static(STALE_ROUTE_HEADER),
            hyper::header::HeaderValue::from_static(STALE_ROUTE_VALUE),
        );
        return stale;
    }
    let (upgrade_response, upgrade) = match fastwebsockets::upgrade::upgrade(&mut request) {
        Ok(upgrade) => upgrade,
        Err(error) => {
            return peer_response(response(
                StatusCode::BAD_REQUEST,
                format!("ws upgrade: {error}"),
            ));
        }
    };
    let target = celld::js::WsTarget {
        id: websocket,
        scope,
        peer_node: None,
        peer_addr: None,
        peer_epoch: None,
    };
    let task_app = app.clone();
    let task = Box::pin(async move {
        match upgrade.await {
            Ok(socket) => websocket_task(task_app, target, socket).await,
            Err(error) => eprintln!("celld peer WebSocket upgrade failed: {error}"),
        }
    });
    if app.websockets.send(task).is_err() {
        return peer_response(response(
            StatusCode::SERVICE_UNAVAILABLE,
            "WebSocket executor stopped",
        ));
    }
    peer_response(upgrade_response.map(|body| body.map_err(|never| match never {}).boxed_unsync()))
}

async fn handle_websocket(mut request: Request<Incoming>, app: AppHandle) -> HttpReply {
    let started = Instant::now();
    let request_id = celld::js::next_request_id();
    let (upgrade_response, upgrade) = match fastwebsockets::upgrade::upgrade(&mut request) {
        Ok(upgrade) => upgrade,
        Err(error) => return response(StatusCode::BAD_REQUEST, format!("ws upgrade: {error}")),
    };
    let runtime = app.runtime.as_ref().expect("WebSocket runtime checked");
    let body_started = Instant::now();
    let (url, method, body, headers) = match request_payload(request).await {
        Ok(payload) => payload,
        Err(response) => return response,
    };
    let body_read_us = body_started.elapsed().as_micros() as u64;
    let worker_started = Instant::now();
    let worker_response = match runtime
        .fetch_worker_pool(url, method, body, headers, request_id)
        .await
    {
        Ok(response) => response,
        Err(error) => {
            emit_websocket_connection_timing(
                runtime,
                request_id,
                started,
                body_read_us,
                worker_started.elapsed().as_micros() as u64,
                WebSocketConnectionOutcome {
                    outcome: "worker_error",
                    route: "",
                    scope: "",
                    status: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                },
            );
            return response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Worker failed: {error:#}"),
            );
        }
    };
    let worker_dispatch_us = worker_started.elapsed().as_micros() as u64;
    let Some(target) = worker_response.ws else {
        emit_websocket_connection_timing(
            runtime,
            request_id,
            started,
            body_read_us,
            worker_dispatch_us,
            WebSocketConnectionOutcome {
                outcome: "rejected",
                route: "",
                scope: "",
                status: worker_response.status,
            },
        );
        return runtime_response(worker_response);
    };
    if worker_response.status != 101 {
        emit_websocket_connection_timing(
            runtime,
            request_id,
            started,
            body_read_us,
            worker_dispatch_us,
            WebSocketConnectionOutcome {
                outcome: "rejected",
                route: "",
                scope: &target.scope,
                status: worker_response.status,
            },
        );
        return response(StatusCode::BAD_GATEWAY, "unsupported WebSocket route");
    }
    emit_websocket_connection_timing(
        runtime,
        request_id,
        started,
        body_read_us,
        worker_dispatch_us,
        WebSocketConnectionOutcome {
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
    let task_app = app.clone();
    let task = Box::pin(async move {
        match upgrade.await {
            Ok(socket) if target.peer_node.is_some() => {
                if let Err(error) = remote_websocket_task(task_app, target, socket).await {
                    eprintln!("celld remote WebSocket tunnel failed: {error:#}");
                }
            }
            Ok(socket) => websocket_task(task_app, target, socket).await,
            Err(error) => {
                eprintln!("celld WebSocket upgrade failed: {error}");
                if target.peer_node.is_none() {
                    finish_websocket(&task_app, &target, 1006, String::new(), false).await;
                }
            }
        }
    });
    if app.websockets.send(task).is_err() {
        return response(
            StatusCode::SERVICE_UNAVAILABLE,
            "WebSocket executor stopped",
        );
    }
    upgrade_response.map(|body| body.map_err(|never| match never {}).boxed_unsync())
}

struct WebSocketConnectionOutcome<'a> {
    outcome: &'a str,
    route: &'a str,
    scope: &'a str,
    status: u16,
}

fn emit_websocket_connection_timing(
    runtime: &RuntimeManager,
    request_id: celld::js::RequestId,
    started: Instant,
    body_read_us: u64,
    worker_dispatch_us: u64,
    event_outcome: WebSocketConnectionOutcome<'_>,
) {
    let WebSocketConnectionOutcome {
        outcome,
        route,
        scope,
        status,
    } = event_outcome;
    tracing::debug!(
        target: "timing",
        event = "websocket_connection_timing",
        outcome,
        route,
        scope,
        request_id = %celld::js::request_id_string(request_id),
        node = runtime.node(),
        region = runtime.region(),
        runtime_version = env!("CARGO_PKG_VERSION"),
        status,
        total_us = started.elapsed().as_micros() as u64,
        body_read_us,
        worker_dispatch_us,
        "WebSocket connection resolved"
    );
}

async fn handle_ingress(request: Request<Incoming>, app: AppHandle) -> HttpReply {
    if matches!(*request.method(), hyper::Method::GET | hyper::Method::HEAD) {
        if let Some(resolver) = app
            .asset_script
            .as_deref()
            .and_then(|script| app.assets.get(script))
        {
            let path = request.uri().path();
            if !resolver.should_run_worker_first(path) {
                let head = request.method() == hyper::Method::HEAD;
                match resolver
                    .ingress_response(path, request.uri().query(), head, request.headers())
                    .await
                {
                    Ok(Some(response)) => return asset_response(response),
                    Ok(None) if resolver.asset_only() => {
                        return response(StatusCode::NOT_FOUND, "Not found");
                    }
                    Ok(None) => {}
                    Err(error) => {
                        eprintln!("celld asset response failed for {path}: {error:#}");
                        return response(
                            StatusCode::BAD_GATEWAY,
                            "Active deployment asset is unavailable",
                        );
                    }
                }
            }
        }
    }

    let (url, method, body, headers) = match request_payload(request).await {
        Ok(payload) => payload,
        Err(response) => return response,
    };
    match app.fetch_worker(url, method, body, headers).await {
        Ok(worker_response) => runtime_response(worker_response),
        Err(error) => response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Worker failed: {error:#}"),
        ),
    }
}

async fn dispatch_asset_call(app: AppHandle, call: AssetCallReq) {
    let response = match app.assets.get(&call.script) {
        Some(resolver) => {
            resolver
                .binding_response(&call.url, &call.method, &call.headers)
                .await
        }
        None => Err(anyhow::anyhow!(
            "no asset resolver for script {}",
            call.script
        )),
    };
    let _ = call.reply.send(response);
}

async fn dispatch_service_call(app: AppHandle, call: SvcCallReq) {
    let response = match &app.runtime {
        Some(runtime) => {
            runtime
                .fetch_service(
                    &call.script,
                    call.url,
                    call.method,
                    call.body,
                    call.headers,
                    call.cancel,
                )
                .await
        }
        None => Err(anyhow::anyhow!("no Worker runtime")),
    };
    let _ = call.reply.send(response);
}

async fn dispatch_service_rpc(app: AppHandle, call: SvcRpcReq) {
    let response = match &app.runtime {
        Some(runtime) => {
            runtime
                .rpc_service(&call.script, call.entrypoint, call.method, call.args)
                .await
        }
        None => Err(anyhow::anyhow!("no Worker runtime")),
    };
    let _ = call.reply.send(response);
}

async fn internal_probe(request: Request<Incoming>, app: AppHandle) -> HttpReply {
    let (parts, body) = request.into_parts();
    let body = match body.collect().await {
        Ok(body) => body.to_bytes(),
        Err(error) => {
            return peer_response(response(
                StatusCode::BAD_REQUEST,
                format!("invalid probe body: {error}"),
            ));
        }
    };
    let path_and_query = parts
        .uri
        .path_and_query()
        .map_or_else(|| parts.uri.path().to_string(), ToString::to_string);
    if let Err(error) = app.peer_auth.verify(
        &parts.method,
        &path_and_query,
        &parts.headers,
        &body,
        app.peer_auth.source(),
    ) {
        return peer_response(response(error.status(), error.message()));
    }
    if parts.method != hyper::Method::GET {
        return peer_response(response(
            StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed",
        ));
    }
    let Some(challenge) = parts
        .headers
        .get("x-cells-probe-challenge")
        .and_then(|value| value.to_str().ok())
    else {
        return peer_response(response(StatusCode::BAD_REQUEST, "missing probe challenge"));
    };
    match celld::peer_probe::respond(app.peer_auth.source(), &app.advertise, challenge) {
        Ok(probe) => match serde_json::to_vec(&probe) {
            Ok(body) => peer_response(response(StatusCode::OK, body)),
            Err(_) => peer_response(response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "encode probe response",
            )),
        },
        Err(_) => peer_response(response(StatusCode::BAD_REQUEST, "invalid probe challenge")),
    }
}

async fn handle(
    request: Request<Incoming>,
    app: AppHandle,
    shutdown: mpsc::UnboundedSender<()>,
) -> Result<HttpReply, Infallible> {
    let path = request.uri().path().to_string();
    if path.starts_with("/__ws/") && fastwebsockets::upgrade::is_upgrade_request(&request) {
        return Ok(handle_peer_websocket(request, app, &path).await);
    }
    if app.runtime.is_some() && fastwebsockets::upgrade::is_upgrade_request(&request) {
        return Ok(handle_websocket(request, app).await);
    }
    let result = match path.as_str() {
        "/js" if app.runtime.is_some() => {
            let runtime = app.runtime.as_ref().expect("checked runtime");
            let (url, method, body, headers) = match request_payload(request).await {
                Ok(payload) => payload,
                Err(response) => return Ok(response),
            };
            match runtime.fetch_worker(url, method, body, headers).await {
                Ok(worker_response) => runtime_response(worker_response),
                Err(error) => response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Worker failed: {error:#}"),
                ),
            }
        }
        "/__celld/health" if app.healthy().await => response(StatusCode::OK, "{\"ok\":true}"),
        "/__celld/health" => response(StatusCode::SERVICE_UNAVAILABLE, "{\"ok\":false}"),
        "/__celld/probe" => internal_probe(request, app).await,
        "/health" if app.runtime.is_none() && app.healthy().await => {
            response(StatusCode::OK, "{\"ok\":true}")
        }
        "/health" if app.runtime.is_none() => {
            response(StatusCode::SERVICE_UNAVAILABLE, "{\"ok\":false}")
        }
        "/state" => response(StatusCode::OK, app.snapshot().await),
        "/shutdown" => {
            let _ = shutdown.send(());
            response(StatusCode::OK, "{\"ok\":true}")
        }
        _ if path.starts_with("/__abort/") && app.runtime.is_some() => {
            internal_abort(request, app, path).await
        }
        _ if path.starts_with("/__do/") && app.runtime.is_some() => {
            internal_do(request, app, path[6..].to_string()).await
        }
        _ if path.starts_with("/__rpc/") && app.runtime.is_some() => {
            internal_rpc(request, app, path[7..].to_string()).await
        }
        _ if path.starts_with("/do/") && app.runtime.is_some() => {
            let runtime = app.runtime.as_ref().expect("checked runtime");
            let cell = match runtime.cell_scope(&path[4..]) {
                Ok(cell) => cell,
                Err(error) => {
                    return Ok(response(StatusCode::BAD_REQUEST, format!("{error:#}")));
                }
            };
            let (url, method, body, headers) = match request_payload(request).await {
                Ok(payload) => payload,
                Err(response) => return Ok(response),
            };
            // The same dispatcher a Durable Object call from inside a Worker
            // goes through. Public ingress used to resolve the route itself
            // and, on finding another owner, answer with a 307 and a JSON
            // description of where the cell lived -- with no Location header,
            // so nothing could follow it. A fleet behind a load balancer
            // serves a cell only from the node that happens to own it, which
            // is to say it does not serve a fleet at all.
            //
            // Going through `dispatch_do_call` also inherits the redispatch
            // policy and the cancellation channel, so a client that hangs up
            // reaches the owner rather than only the node it connected to.
            let (reply, receive) = oneshot::channel();
            let (cancel_tx, cancel) = oneshot::channel();
            let accepted = celld::js::submit_do_call(celld::js::DoCallReq {
                // Named, and named here: the abort fires only for a call that
                // carries both an id and a cancel signal, so leaving this None
                // silently costs the cancellation rather than failing.
                request_id: Some(celld::js::next_request_id()),
                cancel: Some(cancel),
                scope: cell,
                name: None,
                url,
                method,
                body,
                headers,
                reply,
            });
            if !accepted {
                return Ok(response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "{\"error\":\"dispatcher unavailable\"}",
                ));
            }
            let _hangup = HangUp(Some(cancel_tx));
            match receive.await {
                Ok(Ok(worker_response)) => runtime_response(worker_response),
                Ok(Err(error)) => match error.downcast_ref::<RoutedRequestError>() {
                    Some(error) => response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        format!("{{\"error\":\"{:?}\"}}", error.0),
                    ),
                    None => response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        format!("cell Worker failed: {error:#}"),
                    ),
                },
                Err(_) => response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "{\"error\":\"dispatcher dropped the call\"}",
                ),
            }
        }
        _ if path.starts_with("/cell/") => {
            let cell = path[6..].to_string();
            match app.request(cell.clone()).await {
                Ok(Routed {
                    request,
                    route: Route::Local,
                }) => {
                    let _activity = app.activity(request, cell.clone());
                    response(
                        StatusCode::OK,
                        format!("{{\"route\":\"local\",\"cell\":{cell:?}}}"),
                    )
                }
                Ok(Routed {
                    route:
                        Route::Remote {
                            node,
                            addr,
                            epoch,
                            peer_protocol,
                        },
                    ..
                }) => response(
                    StatusCode::TEMPORARY_REDIRECT,
                    format!(
                        "{{\"route\":\"remote\",\"node\":{node:?},\"addr\":{addr:?},\"epoch\":{epoch},\"peer_protocol\":{peer_protocol}}}"
                    ),
                ),
                Err(error) => response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("{{\"error\":\"{error:?}\"}}"),
                ),
            }
        }
        _ if path.starts_with("/evict/") => {
            app.evict(path[7..].to_string()).await;
            response(StatusCode::OK, "{\"ok\":true}")
        }
        _ if app.runtime.is_some() => handle_ingress(request, app).await,
        _ => response(StatusCode::NOT_FOUND, "{\"error\":\"not_found\"}"),
    };
    Ok(result)
}

struct Settings {
    control_plane: bool,
    bucket: Option<String>,
    load_deployment: bool,
    endpoint: Option<String>,
    region: String,
    listen: celld::startup::Listen,
    advertise: Option<String>,
    unsafe_public_advertise: bool,
}

enum Action {
    Run(Settings),
    Diagnose {
        settings: Settings,
        peers: Vec<String>,
    },
    Deploy(Vec<String>),
    Connect(Vec<String>),
    Credentials(Vec<String>),
    Token(Vec<String>),
    Disconnect(Vec<String>),
    Help,
    Version,
}

fn action_from_process() -> anyhow::Result<Action> {
    let mut arguments = std::env::args().skip(1).collect::<Vec<_>>();
    if let Some(action) = arguments.first().map(String::as_str) {
        let arguments = arguments[1..].to_vec();
        match action {
            "deploy" => return Ok(Action::Deploy(arguments)),
            "connect" => return Ok(Action::Connect(arguments)),
            "credentials" => return Ok(Action::Credentials(arguments)),
            "token" => return Ok(Action::Token(arguments)),
            "disconnect" => return Ok(Action::Disconnect(arguments)),
            _ => {}
        }
    }
    let diagnose = arguments.first().is_some_and(|value| value == "diagnose");
    if diagnose {
        arguments.remove(0);
    }
    if matches!(
        arguments.first().map(String::as_str),
        Some("--help" | "-h" | "help")
    ) {
        return Ok(Action::Help);
    }
    if matches!(
        arguments.first().map(String::as_str),
        Some("--version" | "-V" | "version")
    ) {
        return Ok(Action::Version);
    }
    let env = |name: &str| {
        std::env::var(name)
            .ok()
            .filter(|value| !value.trim().is_empty())
    };
    let celld_bucket = env("CELLD_BUCKET");
    let fixture_bucket = env("CELLD_TEST_BUCKET");
    let control_plane = match env("CELLD_CLOUD").as_deref() {
        Some(value) if value.eq_ignore_ascii_case("on") => true,
        Some(value) if value.eq_ignore_ascii_case("off") => false,
        Some(value) => anyhow::bail!("CELLD_CLOUD must be `on` or `off`, not {value:?}"),
        None => false,
    };
    let configured_listen = env("CELLD_ADDR");
    if !diagnose
        && arguments.is_empty()
        && !control_plane
        && celld_bucket.is_none()
        && fixture_bucket.is_none()
        && configured_listen.is_none()
    {
        return Ok(Action::Help);
    }
    let mut listen_configured = configured_listen.is_some();
    let mut peers = Vec::new();
    let mut settings = Settings {
        control_plane,
        bucket: fixture_bucket
            .or_else(|| celld_bucket.clone())
            .map(|value| value.trim_start_matches("s3://").to_string()),
        load_deployment: celld_bucket.is_some(),
        endpoint: env("S3_ENDPOINT"),
        region: env("AWS_REGION")
            .or_else(|| env("AWS_DEFAULT_REGION"))
            .unwrap_or_else(|| "us-east-1".to_string()),
        // This placeholder is resolved after command-line mode flags. An
        // explicit environment or CLI address always wins; otherwise managed
        // mode selects the first free demo port and standalone uses 8080,
        // matching celld's listener contract.
        listen: configured_listen
            .map(celld::startup::Listen::Explicit)
            .unwrap_or(celld::startup::Listen::AutoLoopback),
        advertise: env("CELLD_ADVERTISE"),
        unsafe_public_advertise: match env("CELLD_UNSAFE_PUBLIC_ADVERTISE").as_deref() {
            Some(value) if value.eq_ignore_ascii_case("on") => true,
            Some(value) if value.eq_ignore_ascii_case("off") => false,
            Some(value) => {
                anyhow::bail!("CELLD_UNSAFE_PUBLIC_ADVERTISE must be `on` or `off`, not {value:?}")
            }
            None => false,
        },
    };
    let mut args = arguments.into_iter();
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--control-plane" => settings.control_plane = true,
            "--bucket" => {
                let bucket = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--bucket requires a value"))?;
                settings.bucket = Some(bucket.trim_start_matches("s3://").to_string());
                settings.load_deployment = true;
            }
            "--endpoint" => {
                settings.endpoint = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--endpoint requires a value"))?,
                );
            }
            "--region" => {
                settings.region = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--region requires a value"))?;
            }
            "--listen" => {
                settings.listen = celld::startup::Listen::Explicit(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--listen requires a value"))?,
                );
                listen_configured = true;
            }
            "--advertise" => {
                settings.advertise = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--advertise requires a value"))?,
                );
            }
            "--unsafe-public-advertise" => settings.unsafe_public_advertise = true,
            "--peer" if diagnose => {
                let peer = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--peer requires a node-session ID"))?;
                celld::startup::validate_peer_id(&peer)?;
                peers.push(peer);
            }
            "--no-control-plane" => settings.control_plane = false,
            other => {
                anyhow::bail!("unknown command or option: {other}; run `celld --help` for usage")
            }
        }
    }
    if !listen_configured {
        settings.listen = if settings.control_plane {
            celld::startup::Listen::AutoLoopback
        } else {
            celld::startup::Listen::Explicit("127.0.0.1:8080".to_string())
        };
    }
    Ok(if diagnose {
        Action::Diagnose { settings, peers }
    } else {
        Action::Run(settings)
    })
}

fn print_help() {
    println!(
        r#"celld — self-hosted, distributed Durable Objects

USAGE:
  celld --bucket s3://NAME [OPTIONS]
  celld deploy [PROJECT] --bucket s3://NAME [OPTIONS]
  celld diagnose --bucket s3://NAME [OPTIONS] [--peer NODE_ID]...

Production install: celld --bucket s3://NAME [OPTIONS]

OPTIONS:
  --bucket s3://NAME     Fleet bucket; uses the standard AWS credential chain
  --endpoint URL         Optional S3-compatible endpoint
  --region REGION        Storage region (default: AWS_REGION or us-east-1)
  --listen IP:PORT       Listener; explicit conflicts fail (default: 127.0.0.1:8080)
  --advertise ADDR:PORT  Address peers can reach: IP:PORT or HOST:PORT
                         (required when --listen is 0.0.0.0 or ::)
  --peer NODE_ID         Diagnose one node with a signed direct probe; repeatable
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
  CELLD_WATCH                     Local SQLite/replication working directory
  CELLD_ESBUILD                   Override esbuild executable path
  CELLD_WORKERS                   Stateless Worker pool size
  CELLD_ACTIVATIONS               Concurrent cold activations
  CELLD_HIBERNATIONS              Concurrent evictions (default: 4)
  CELLD_MAX_COHOSTED              Service-binding isolate limit (default: 8)
  CELLD_VARS_FILE, CELLD_VAR_*    Worker variable overrides
  CELLD_ASSET_CACHE_DIR           Downloaded static-asset cache directory
  CELLD_ASSET_CACHE_BYTES         Asset cache limit

TUNING:
  CELLD_TTL_MS                    Node lease lifetime (default: 10000)
  CELLD_IDLE_EVICT_S              Idle-cell eviction age (default: 300)
  CELLD_LOCAL_CACHE_MAX_BYTES     Hibernated SQLite cache limit (default: 2 GiB; 0 disables)
  CELLD_MAX_RESIDENT_CELLS        Resident-cell hard cap, enforced at admission
  CELLD_RESIDENT_LOW_WATER        Deprecated and ignored
  CELLD_PRESSURE_OWNERSHIP        release to rebalance, sticky to cache locally
  CELLD_MAX_RSS_MB                RSS shed threshold (default: 80% of memory; 0 disables)
  CELLD_MAX_CPU_PERCENT           CPU pressure-shedding threshold
  CELLD_ALARM_RESIDENT_MS         Near-alarm residency window
  CELLD_WAKER_TICK_MS             Orphan-alarm scan interval
  CELLD_V8_HEAP_LIMIT_MB          Per-isolate V8 heap limit
  CELLD_FETCH_TIMEOUT_S           Outbound fetch timeout
  CELLD_HANDLER_BUDGET_S          JavaScript handler budget
  CELLD_TOKIO_THREADS             Tokio runtime worker threads
  RUST_LOG                        Runtime log filter (default: info)

EXPERIMENTAL:
  CELLD_WORKER_LOADER             Worker Loader binding name for Code Mode
  CELLD_AI_BINDING, CELLD_AI_URL  AI binding name and endpoint

Documentation: https://celld.dev/docs"#
    );
}

fn worker_loader_binding() -> Option<String> {
    std::env::var("CELLD_WORKER_LOADER")
        .ok()
        .filter(|name| !name.is_empty())
}

/// An activation effect that has not answered by now is not going to help the
/// request that is waiting on it. celld had no such bound and parked requests
/// past ninety seconds.
const DEFAULT_OPERATION_DEADLINE_MS: u64 = 15_000;
/// Cold routes are I/O concurrency, but must stay below the point where they
/// can starve the node lease heartbeat or object store.
const DEFAULT_MAX_CONCURRENT_ACTIVATIONS: usize = 128;
/// celld's own default. Evictions are bounded far tighter than activations
/// because each one carries a durability proof, and a node that lets its whole
/// working set prove durability at once turns a walk down into a thundering
/// herd against the bucket.
const DEFAULT_MAX_CONCURRENT_HIBERNATIONS: usize = 4;
/// Preserved SQLite snapshots make a same-node wake a rename instead of a
/// remote restore, but must not grow with the lifetime population of a node.
const DEFAULT_LOCAL_CACHE_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;
/// The walk is O(cached cells), so keep it off the hot maintenance cadence.
const LOCAL_CACHE_PRUNE_PERIOD: std::time::Duration = std::time::Duration::from_secs(60);

fn main() -> anyhow::Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if let Some(workers) = std::env::var("CELLD_TOKIO_THREADS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|workers| *workers > 0)
    {
        builder.worker_threads(workers);
    }
    builder.build()?.block_on(async_main())
}

async fn async_main() -> anyhow::Result<()> {
    celld::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let mut settings = match action_from_process()? {
        Action::Deploy(arguments) => return fleet::run_deploy(arguments).await,
        Action::Connect(arguments) => {
            return celld::control_plane::handle_connect_command(arguments).await
        }
        Action::Credentials(arguments) => {
            return celld::control_plane::handle_credentials_command(arguments).await
        }
        Action::Token(arguments) => {
            return celld::control_plane::handle_token_command(arguments).await
        }
        Action::Disconnect(arguments) => {
            return celld::control_plane::handle_disconnect_command(arguments).await
        }
        Action::Help => {
            print_help();
            return Ok(());
        }
        Action::Version => {
            let profile = if cfg!(debug_assertions) {
                " (debug)"
            } else {
                ""
            };
            println!("celld {}{profile}", env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
        Action::Diagnose {
            mut settings,
            peers,
        } => {
            let bound = celld::startup::bind_listener(&celld::startup::RuntimeSettings {
                listen: settings.listen.clone(),
                advertise: settings.advertise.clone(),
                unsafe_public_advertise: settings.unsafe_public_advertise,
            })
            .await?;
            // The bind proves the address is free; diagnose never serves on it,
            // and an operator should not read this line as a running listener.
            println!(
                "ok listen {} (bind check; diagnose does not serve)",
                bound.listen
            );
            println!(
                "ok advertise {} ({}; direct reachability is not inferred)",
                bound.advertise,
                bound.advertise.scope()
            );
            let managed_storage = if settings.control_plane {
                match celld::control_plane::installation_storage().context(
                    "managed diagnostics require an existing enrollment; run `celld --control-plane` first",
                )? {
                    celld::control_plane::InstallationStorageConfig::Managed(storage) => {
                        settings.bucket = Some(storage.bucket.clone());
                        settings.endpoint = Some(storage.endpoint.clone());
                        settings.region = storage.region.clone();
                        Some(storage)
                    }
                    celld::control_plane::InstallationStorageConfig::Byo(storage) => {
                        settings.bucket = Some(storage.bucket);
                        settings.endpoint = storage.endpoint;
                        settings.region = storage.region;
                        None
                    }
                }
            } else {
                None
            };
            let bucket = settings
                .bucket
                .ok_or_else(|| anyhow::anyhow!("celld diagnose requires --bucket"))?;
            let client = fleet::s3_client_with_credentials(
                &bucket,
                settings.endpoint.as_deref(),
                &settings.region,
                managed_storage.as_ref(),
            )?;
            return fleet::diagnose(&client, peers, settings.unsafe_public_advertise).await;
        }
        Action::Run(settings) => settings,
    };
    celld::startup::raise_file_limit();
    let max_resident = std::env::var("CELLD_MAX_RESIDENT_CELLS")
        .ok()
        .and_then(|value| value.parse().ok())
        // celld has no resident ceiling unless the operator configures one.
        // The clean-sheet prototype originally defaulted to eight, which
        // introduced eviction churn in otherwise unconstrained workloads and
        // made cancellation semantics depend on cold-reactivation latency.
        .unwrap_or(usize::MAX);
    let local_cache_max_bytes = local_cache_max_bytes_from_environment()?;
    let fail_publish_once = std::env::var_os("CELLD_TEST_FAIL_PUBLISH_ONCE").is_some();
    let bound = celld::startup::bind_listener(&celld::startup::RuntimeSettings {
        listen: settings.listen.clone(),
        advertise: settings.advertise.clone(),
        unsafe_public_advertise: settings.unsafe_public_advertise,
    })
    .await?;
    let advertise = bound.advertise.to_string();
    let listen = bound.listen.to_string();
    let listener = bound.listener;
    let mut adapter_credential_version = None;
    let managed_storage = if settings.control_plane {
        let requested_byo =
            settings
                .bucket
                .as_ref()
                .map(|bucket| celld::control_plane::ByoStorageConfig {
                    bucket: bucket.clone(),
                    endpoint: settings.endpoint.clone(),
                    region: settings.region.clone(),
                });
        celld::control_plane::connect_on_startup_with_storage(requested_byo).await?;
        settings.load_deployment = true;
        let (storage, credential_version) =
            celld::control_plane::installation_storage_with_version()?;
        adapter_credential_version = Some(credential_version);
        match storage {
            celld::control_plane::InstallationStorageConfig::Managed(storage) => {
                settings.bucket = Some(storage.bucket.clone());
                settings.endpoint = Some(storage.endpoint.clone());
                settings.region = storage.region.clone();
                Some(storage)
            }
            celld::control_plane::InstallationStorageConfig::Byo(storage) => {
                settings.bucket = Some(storage.bucket);
                settings.endpoint = storage.endpoint;
                settings.region = storage.region;
                None
            }
        }
    } else {
        None
    };
    let storage_credentials =
        managed_storage
            .as_ref()
            .map(|storage| celld::replication::StorageCredentials {
                access_key_id: storage.access_key_id.clone(),
                secret_access_key: storage.secret_access_key.clone(),
                session_token: storage.session_token.clone(),
            });
    let (tx, rx) = mpsc::unbounded_channel();
    let sample_tx = tx.clone();
    let alarm_tx = tx.clone();
    let alarm_observer: celld::runtime::AlarmObserver = Arc::new(move |cell, at_ms| {
        let _ = alarm_tx.send(Message::AlarmObserved {
            cell,
            at_ms,
            covered: false,
        });
    });
    let (fence_tx, mut fence_rx) = mpsc::unbounded_channel();
    let node = std::env::var("CELLD_NODE").unwrap_or_else(|_| random_node_session_id());
    celld::control_plane::install_reexec_node_session_id(&node)?;
    let probe_public_key = celld::peer_probe::install_signer()?;
    let workers = std::env::var("CELLD_WORKERS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|workers| *workers > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1)
        });
    let max_activations = positive_environment::<usize>("CELLD_ACTIVATIONS")?
        .unwrap_or_else(|| workers.min(DEFAULT_MAX_CONCURRENT_ACTIVATIONS));
    let max_hibernations = positive_environment::<usize>("CELLD_HIBERNATIONS")?
        .unwrap_or(DEFAULT_MAX_CONCURRENT_HIBERNATIONS);
    let data_dir = std::env::var_os("CELLD_TEST_DATA_DIR")
        .or_else(|| std::env::var_os("CELLD_WATCH"))
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join(format!("celld-{}", std::process::id())));
    let mut deploy_agent = None;
    let (runtime, ownership, peer_key, wake_scan, assets, asset_script) =
        if let Some(bucket) = settings.bucket.clone().filter(|_| settings.load_deployment) {
            let client = fleet::s3_client_with_credentials(
                &bucket,
                settings.endpoint.as_deref(),
                &settings.region,
                managed_storage.as_ref(),
            )?;
            if settings.control_plane {
                fleet::validate_managed_bucket(&client).await?;
            } else {
                fleet::validate_bucket(&client).await?;
            }
            let lease_client = fleet::s3_lease_client_with_credentials(
                &bucket,
                settings.endpoint.as_deref(),
                &settings.region,
                managed_storage.as_ref(),
            )?;
            if settings.control_plane {
                celld::control_plane::wait_for_initial_deployment(&client).await?;
                deploy_agent = Some(client.clone());
            }
            let peer_key = peer_auth::load_or_create(&client).await?;
            let mut deployment = fleet::load_current_worker(&client, node.clone()).await?;
            let primary_script = deployment.script_name.clone();
            let mut asset_resolvers = HashMap::new();
            if let Some(resolver) = deployment.assets.take() {
                asset_resolvers.insert(primary_script.clone(), resolver);
            }
            let max_cohosted = std::env::var("CELLD_MAX_COHOSTED")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(8);
            let mut visited = BTreeSet::from([primary_script.clone()]);
            let mut queue = deployment
                .services
                .iter()
                .map(|(_, script, _)| script.clone())
                .collect::<VecDeque<_>>();
            let mut cohosted = Vec::new();
            while let Some(target) = queue.pop_front() {
                if target == primary_script || !visited.insert(target.clone()) {
                    continue;
                }
                if cohosted.len() >= max_cohosted {
                    anyhow::bail!(
                    "service binding graph exceeds CELLD_MAX_COHOSTED={max_cohosted} at {target}"
                );
                }
                let mut loaded = fleet::load_named_worker(&client, &target, node.clone())
                    .await
                    .with_context(|| format!("load service binding target {target}"))?;
                if loaded.script_name != target {
                    anyhow::bail!(
                        "service pointer {target} resolved script {}",
                        loaded.script_name
                    );
                }
                queue.extend(loaded.services.iter().map(|(_, script, _)| script.clone()));
                if let Some(resolver) = loaded.assets.take() {
                    asset_resolvers.insert(target, resolver);
                }
                cohosted.push(CohostedWorker {
                    options: loaded.options,
                    services: loaded.services,
                    asset_binding: loaded.asset_binding,
                    workers: 2,
                });
            }
            let wake = Arc::new(celld::wake::WakeFlusher::new());
            celld::js::set_arm_gate(ArmGate {
                bucket: client.clone(),
                flusher: wake.clone(),
            });
            // celld treats replication as a node service, not as a property
            // of today's manifest. Start it even for a stateless deployment
            // so a later deployment can introduce cells without changing the
            // durability contract underneath the node.
            let replication = Some(Replication::start(
                client.clone(),
                &data_dir,
                settings.endpoint.clone(),
                settings.region.clone(),
                storage_credentials.clone(),
            )?);
            let asset_script = Some(Arc::<str>::from(primary_script));
            let assets = Arc::new(asset_resolvers);
            let runtime = RuntimeManager::start(RuntimeOptions {
                worker: deployment.options,
                services: deployment.services,
                asset_binding: deployment.asset_binding,
                loader_binding: worker_loader_binding(),
                cohosted,
                workers,
                data_dir,
                replication,
                wake: Some(wake.clone()),
                alarm_observer: alarm_observer.clone(),
                node: node.clone(),
                region: settings.region.clone(),
            })?;
            let wake_scan = Some((client.clone(), wake.clone()));
            let ownership = Ownership::S3(Arc::new(
                S3Ownership::with_probe_public_key(
                    client,
                    lease_client,
                    node.clone(),
                    probe_public_key.clone(),
                )
                .with_lease_ttl_ms(lease_ttl_ms_from_environment()),
            ));
            (
                Some(runtime),
                Some(ownership),
                peer_key,
                wake_scan,
                assets,
                asset_script,
            )
        } else if let Ok(script_path) = std::env::var("CELLD_TEST_SCRIPT_PATH") {
            let source = std::fs::read_to_string(&script_path)?;
            let do_classes = std::env::var("CELLD_TEST_DO_CLASSES")
                .unwrap_or_default()
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .collect();
            let bindings = std::env::var("CELLD_TEST_DO_BINDINGS")
                .unwrap_or_default()
                .split(',')
                .filter_map(|value| value.split_once('='))
                .map(|(name, class)| (name.trim().to_string(), class.trim().to_string()))
                .filter(|(name, class)| !name.is_empty() && !class.is_empty())
                .collect();
            let options = WorkerConfigOptions {
                src: source,
                script_name: "celld-local".to_string(),
                do_classes,
                bindings,
                r2_bindings: Vec::new(),
                ai_binding: fleet::configured_ai_binding(None),
                vars: Vec::new(),
                node: node.clone(),
                text: Vec::new(),
                compat: Compat::default(),
            };
            let (ownership, peer_key, wake, wake_scan) = match settings.bucket.clone() {
                Some(bucket) => {
                    let client = fleet::s3_client_with_credentials(
                        &bucket,
                        settings.endpoint.as_deref(),
                        &settings.region,
                        managed_storage.as_ref(),
                    )?;
                    let lease_client = fleet::s3_lease_client_with_credentials(
                        &bucket,
                        settings.endpoint.as_deref(),
                        &settings.region,
                        managed_storage.as_ref(),
                    )?;
                    let peer_key = peer_auth::load_or_create(&client).await?;
                    let wake = Arc::new(celld::wake::WakeFlusher::new());
                    celld::js::set_arm_gate(ArmGate {
                        bucket: client.clone(),
                        flusher: wake.clone(),
                    });
                    let wake_scan = Some((client.clone(), wake.clone()));
                    (
                        Some(Ownership::S3(Arc::new(
                            S3Ownership::with_probe_public_key(
                                client,
                                lease_client,
                                node.clone(),
                                probe_public_key.clone(),
                            )
                            .with_lease_ttl_ms(lease_ttl_ms_from_environment()),
                        ))),
                        peer_key,
                        Some(wake),
                        wake_scan,
                    )
                }
                None => (None, random_peer_key(), None, None),
            };
            (
                Some(RuntimeManager::start(RuntimeOptions {
                    worker: options,
                    services: Vec::new(),
                    asset_binding: None,
                    loader_binding: worker_loader_binding(),
                    cohosted: Vec::new(),
                    workers,
                    data_dir,
                    replication: None,
                    wake: wake.clone(),
                    alarm_observer: alarm_observer.clone(),
                    node: node.clone(),
                    region: settings.region.clone(),
                })?),
                ownership,
                peer_key,
                wake_scan,
                Arc::new(HashMap::new()),
                None,
            )
        } else {
            let (ownership, peer_key) = match settings.bucket.clone() {
                Some(bucket) => {
                    let client = fleet::s3_client_with_credentials(
                        &bucket,
                        settings.endpoint.as_deref(),
                        &settings.region,
                        managed_storage.as_ref(),
                    )?;
                    let lease_client = fleet::s3_lease_client_with_credentials(
                        &bucket,
                        settings.endpoint.as_deref(),
                        &settings.region,
                        managed_storage.as_ref(),
                    )?;
                    let peer_key = peer_auth::load_or_create(&client).await?;
                    (
                        Some(Ownership::S3(Arc::new(
                            S3Ownership::with_probe_public_key(
                                client,
                                lease_client,
                                node.clone(),
                                probe_public_key.clone(),
                            )
                            .with_lease_ttl_ms(lease_ttl_ms_from_environment()),
                        ))),
                        peer_key,
                    )
                }
                None => (None, random_peer_key()),
            };
            (
                None,
                ownership,
                peer_key,
                None,
                Arc::new(HashMap::new()),
                None,
            )
        };
    let peer_auth = Arc::new(PeerAuth::new(peer_key, node.clone())?);
    let actor = Actor::from_environment(
        AdmissionLimits {
            resident: max_resident,
            activations: max_activations,
            hibernations: max_hibernations,
        },
        fail_publish_once,
        fence_tx,
        runtime.clone(),
        ownership,
        ActorIdentity {
            node: node.clone(),
            advertise: advertise.clone(),
            region: settings.region.clone(),
            managed: settings.control_plane,
        },
    )
    .await?;
    let ownership_name = actor.ownership.name();
    let explorer_replication = runtime.as_ref().and_then(RuntimeManager::replication);
    let local_cache_replication = explorer_replication.clone();
    let (websocket_tx, mut websocket_rx) = mpsc::unbounded_channel();
    let app = AppHandle {
        tx,
        runtime,
        assets,
        asset_script,
        // Connect-only timeout: a peer request may legitimately run long, but
        // a handshake that never completes provably ran nothing, so failing it
        // fast lets the caller re-resolve the owner and redispatch.
        peer_http: reqwest::Client::builder()
            .connect_timeout(PEER_CONNECT_TIMEOUT)
            .build()
            .unwrap(),
        peer_auth,
        advertise: advertise.clone(),
        websockets: websocket_tx,
        // Opt-in while the write-detection signal is corrected. A fleet run
        // (2026-08-04) showed `write_position` over-reports: it is
        // `sqlite3_total_changes` on the connection, which counts celld's own
        // internal writes (the DO actor name in `kv`, alarm bookkeeping) on
        // activation, so the first request after a takeover trips the gate and
        // fails on the not-yet-replicated fresh epoch. The signal must become
        // the per-request delta of committed app writes before default-on.
        output_gate: std::env::var("CELLD_OUTPUT_GATE").as_deref() != Ok("0"),
        max_outbound_websockets: std::env::var("CELLD_MAX_OUTBOUND_WEBSOCKETS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_MAX_OUTBOUND_WEBSOCKETS),
    };
    let (do_call_tx, mut do_call_rx) = mpsc::unbounded_channel();
    celld::js::set_do_call_tx(do_call_tx);
    let (gate_tx, mut gate_rx) = mpsc::unbounded_channel();
    celld::js::set_gate_tx(gate_tx);
    let (rpc_call_tx, mut rpc_call_rx) = mpsc::unbounded_channel();
    celld::js::set_rpc_call_tx(rpc_call_tx);
    let (service_call_tx, mut service_call_rx) = mpsc::unbounded_channel();
    celld::js::set_svc_call_tx(service_call_tx);
    let (service_rpc_tx, mut service_rpc_rx) = mpsc::unbounded_channel();
    celld::js::set_svc_rpc_tx(service_rpc_tx);
    let (asset_call_tx, mut asset_call_rx) = mpsc::unbounded_channel();
    celld::js::set_asset_call_tx(asset_call_tx);
    let (outbound_ws_tx, mut outbound_ws_rx) = mpsc::unbounded_channel();
    celld::js::set_outbound_ws_tx(outbound_ws_tx);
    tokio::spawn(actor.run(rx));

    // The sampler is a plain ticker: it measures and posts, and decides
    // nothing. Everything downstream of the numbers -- the latch, the target,
    // which cell goes -- is in the core, so a sample sequence replays.
    {
        let period = std::time::Duration::from_millis(
            std::env::var("CELLD_LOAD_SAMPLE_MS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(1_000),
        );
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(period);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if sample_tx.send(Message::SampleLoad).is_err() {
                    return;
                }
            }
        });
    }

    if let Some((client, wake)) = wake_scan {
        let deadline = Instant::now() + std::time::Duration::from_secs(10);
        while !app.healthy().await {
            if Instant::now() >= deadline {
                anyhow::bail!("node authority was not established before wake scan");
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        for (cell, due_ms) in celld::wake::due_scan(&client, now_ms() as i64).await {
            wake.adopt(&cell, due_ms);
            let _ = app.tx.send(Message::WakeHint { cell });
        }

        // And again, on a timer, for the rest of the process's life.
        //
        // The boot scan alone only covers alarms that came due while nothing
        // was watching *before this node started*. A node that dies while
        // this one is already running leaves its cells with armed alarms and
        // no owner, and nothing would look at them again until this process
        // restarted -- an alarm silently not firing, which is the one thing a
        // Durable Object is not allowed to do.
        //
        // Every decision about a due entry stays in the core: a hint for a
        // cell this node already serves is ignored, and one for a cell with a
        // live owner elsewhere resolves to that owner rather than stealing
        // it. The scan only reports what the bucket says is due.
        let scan_app = app.clone();
        let waker_node = node.clone();
        let tick_ms = positive_environment::<u64>("CELLD_WAKER_TICK_MS")?.unwrap_or(60_000);
        let period = std::time::Duration::from_millis(tick_ms);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(period);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await;
            let mut dead_node_gc = celld::dead_node_gc::DeadNodeGc::default();
            loop {
                tick.tick().await;
                dead_node_gc
                    .run_elected_pass(&client, &waker_node, tick_ms)
                    .await;
                for (cell, due_ms) in celld::wake::due_scan(&client, now_ms() as i64).await {
                    wake.adopt(&cell, due_ms);
                    if scan_app.tx.send(Message::WakeHint { cell }).is_err() {
                        return;
                    }
                }
            }
        });
    }

    if let Some(client) = deploy_agent {
        celld::control_plane::start_deploy_agent(client.clone(), Arc::new(AtomicBool::new(true)));
        let presence_app = app.clone();
        celld::control_plane::start_presence_agent(celld::control_plane::PresenceRuntime {
            s3: client,
            replication: explorer_replication,
            node_session_id: node,
            advertise,
            listen,
            credential_version: adapter_credential_version
                .expect("managed adapters have a credential version"),
            snapshot: Arc::new(move || {
                let app = presence_app.clone();
                Box::pin(async move { app.presence().await })
            }),
        });
    }

    println!(
        "celld listening on {} (ownership={ownership_name})",
        listener.local_addr()?
    );
    let (shutdown_tx, mut shutdown_rx) = mpsc::unbounded_channel();
    let mut connections: FuturesUnordered<ConnectionFuture> = FuturesUnordered::new();
    let mut do_calls: FuturesUnordered<DoCallFuture> = FuturesUnordered::new();
    let mut gate_calls: FuturesUnordered<DoCallFuture> = FuturesUnordered::new();
    let mut service_calls: FuturesUnordered<DoCallFuture> = FuturesUnordered::new();
    let mut asset_calls: FuturesUnordered<AssetCallFuture> = FuturesUnordered::new();
    let mut websockets: FuturesUnordered<WebSocketFuture> = FuturesUnordered::new();
    let mut cache_prunes: FuturesUnordered<CachePruneFuture> = FuturesUnordered::new();
    let mut replication_health = tokio::time::interval(std::time::Duration::from_millis(250));
    let mut local_cache_prune = tokio::time::interval_at(
        tokio::time::Instant::now() + LOCAL_CACHE_PRUNE_PERIOD,
        LOCAL_CACHE_PRUNE_PERIOD,
    );
    local_cache_prune.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            connection = listener.accept() => {
                let (stream, _) = connection?;
                let app = app.clone();
                let shutdown = shutdown_tx.clone();
                connections.push(Box::pin(async move {
                    let service = service_fn(move |request| {
                        handle(request, app.clone(), shutdown.clone())
                    });
                    if let Err(error) = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .with_upgrades()
                        .await
                    {
                        eprintln!("celld connection failed: {error}");
                    }
                }));
            }
            Some(()) = connections.next(), if !connections.is_empty() => {}
            call = do_call_rx.recv() => {
                let Some(call) = call else {
                    anyhow::bail!("Durable Object call channel closed");
                };
                do_calls.push(Box::pin(dispatch_do_call(app.clone(), call)));
            }
            Some(()) = do_calls.next(), if !do_calls.is_empty() => {}
            req = gate_rx.recv() => {
                let Some(req) = req else {
                    anyhow::bail!("output-gate channel closed");
                };
                gate_calls.push(Box::pin(dispatch_gate(app.clone(), req)));
            }
            Some(()) = gate_calls.next(), if !gate_calls.is_empty() => {}
            call = service_call_rx.recv() => {
                let Some(call) = call else {
                    anyhow::bail!("service call channel closed");
                };
                service_calls.push(Box::pin(dispatch_service_call(app.clone(), call)));
            }
            call = service_rpc_rx.recv() => {
                let Some(call) = call else {
                    anyhow::bail!("service RPC channel closed");
                };
                service_calls.push(Box::pin(dispatch_service_rpc(app.clone(), call)));
            }
            Some(()) = service_calls.next(), if !service_calls.is_empty() => {}
            call = rpc_call_rx.recv() => {
                let Some(call) = call else {
                    anyhow::bail!("Durable Object RPC channel closed");
                };
                do_calls.push(Box::pin(dispatch_rpc_call(app.clone(), call)));
            }
            call = asset_call_rx.recv() => {
                let Some(call) = call else {
                    anyhow::bail!("asset call channel closed");
                };
                asset_calls.push(Box::pin(dispatch_asset_call(app.clone(), call)));
            }
            Some(()) = asset_calls.next(), if !asset_calls.is_empty() => {}
            socket = websocket_rx.recv() => {
                let Some(socket) = socket else {
                    anyhow::bail!("WebSocket channel closed");
                };
                websockets.push(socket);
            }
            Some(()) = websockets.next(), if !websockets.is_empty() => {}
            _ = local_cache_prune.tick(), if local_cache_replication.is_some()
                && local_cache_max_bytes.is_some() && cache_prunes.is_empty() => {
                let replication = local_cache_replication.clone().unwrap();
                let max_bytes = local_cache_max_bytes.unwrap();
                cache_prunes.push(Box::pin(async move {
                    let result = tokio::task::spawn_blocking(move || {
                        replication.prune_local_cache(max_bytes)
                    }).await;
                    (max_bytes, result)
                }));
            }
            Some((max_bytes, result)) = cache_prunes.next(), if !cache_prunes.is_empty() => {
                match result {
                    Ok((kept, evicted, bytes)) if evicted > 0 => {
                        tracing::info!(
                            event = "local_cache_pruned",
                            kept,
                            evicted,
                            bytes,
                            max_bytes,
                            "pruned least-recently-used hibernation caches"
                        );
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(%error, "local cache pruning task failed");
                    }
                }
            }
            outbound = outbound_ws_rx.recv() => {
                let Some(outbound) = outbound else {
                    anyhow::bail!("outbound WebSocket channel closed");
                };
                let app = app.clone();
                websockets.push(Box::pin(async move {
                    if let Err(error) = outbound_websocket_task(app, outbound).await {
                        eprintln!("celld outbound WebSocket failed: {error:#}");
                    }
                }));
            }
            _ = shutdown_rx.recv() => break,
            code = fence_rx.recv() => {
                std::process::exit(code.unwrap_or(3));
            }
            _ = replication_health.tick() => {
                if let Some(runtime) = &app.runtime {
                    match runtime.replication_status() {
                        Ok(None) => {}
                        Ok(Some(status)) => {
                            eprintln!("SELF-FENCE: replication process exited unexpectedly: {status}");
                            std::process::exit(3);
                        }
                        Err(error) => {
                            eprintln!("SELF-FENCE: replication process health check failed: {error}");
                            std::process::exit(3);
                        }
                    }
                }
            }
        }
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

fn random_peer_key() -> [u8; 32] {
    let mut key = [0_u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut key);
    key
}

fn random_process_generation() -> String {
    random_peer_key()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Host resource sampling, ported from celld unchanged.
///
/// These are the only measurements the pressure decision reads, and they stay
/// here rather than in the core for the obvious reason: `/proc` is I/O, and a
/// core that reads it cannot be replayed.
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
                    (((ticks.saturating_sub(previous_ticks)) as f64 / ticks_per_second / elapsed)
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
    let fields = stat
        .get(stat.rfind(')')? + 2..)?
        .split_whitespace()
        .collect::<Vec<_>>();
    Some(fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?)
}

#[cfg(not(target_os = "linux"))]
fn process_cpu_ticks() -> Option<u64> {
    None
}

#[cfg(unix)]
fn clock_ticks_per_second() -> u64 {
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    u64::try_from(ticks)
        .ok()
        .filter(|ticks| *ticks > 0)
        .unwrap_or(100)
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

#[cfg(target_os = "macos")]
fn resident_memory_bytes() -> u64 {
    let mut info: libc::proc_taskinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::proc_taskinfo>() as libc::c_int;
    let got = unsafe {
        libc::proc_pidinfo(
            std::process::id() as libc::c_int,
            libc::PROC_PIDTASKINFO,
            0,
            (&raw mut info).cast(),
            size,
        )
    };
    if got == size { info.pti_resident_size } else { 0 }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn resident_memory_bytes() -> u64 {
    0
}

#[cfg(target_os = "linux")]
fn page_size() -> u64 {
    let bytes = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    u64::try_from(bytes)
        .ok()
        .filter(|bytes| *bytes > 0)
        .unwrap_or(4096)
}

/// Watermarks from celld's public environment contract.
/// `CELLD_PRESSURE_OWNERSHIP`: `release` (the default) or `sticky`.
/// The lease lifetime, as the capacity listing needs it to decide which node
/// records are stale enough to skip reading. Same variable and default the
/// lease itself uses.
/// Concurrent outbound WebSockets one cell may hold
/// (`CELLD_MAX_OUTBOUND_WEBSOCKETS`).
const DEFAULT_MAX_OUTBOUND_WEBSOCKETS: usize = 32;

/// Matches the fleet and ownership-store clients. Long enough to survive a
/// slow but live peer, short enough that a stale address does not hold a
/// request for the kernel's own connect timeout.
const PEER_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

fn lease_ttl_ms_from_environment() -> u64 {
    std::env::var("CELLD_TTL_MS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10_000)
}

fn ownership_on_evict_from_environment() -> anyhow::Result<OwnershipOnEvict> {
    match std::env::var("CELLD_PRESSURE_OWNERSHIP") {
        Err(_) => Ok(OwnershipOnEvict::Release),
        Ok(value) => match value.trim() {
            "release" => Ok(OwnershipOnEvict::Release),
            "sticky" => Ok(OwnershipOnEvict::Sticky),
            other => Err(anyhow::anyhow!(
                "CELLD_PRESSURE_OWNERSHIP must be `release` or `sticky`, not `{other}`"
            )),
        },
    }
}

/// Total memory this process may use: the cgroup limit when one applies
/// (containers), the machine otherwise.
#[cfg(target_os = "linux")]
fn total_memory_bytes() -> Option<u64> {
    for path in [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ] {
        if let Ok(raw) = std::fs::read_to_string(path) {
            if let Ok(limit) = raw.trim().parse::<u64>() {
                // cgroup v1 reports "no limit" as a huge page-rounded
                // number; anything at or above 1 PiB means unlimited.
                if limit < (1 << 50) {
                    return Some(limit);
                }
            }
        }
    }
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let kb = meminfo
        .lines()
        .find(|line| line.starts_with("MemTotal:"))?
        .split_whitespace()
        .nth(1)?
        .parse::<u64>()
        .ok()?;
    Some(kb.saturating_mul(1024))
}

#[cfg(target_os = "macos")]
fn total_memory_bytes() -> Option<u64> {
    let mut size: u64 = 0;
    let mut len = std::mem::size_of::<u64>();
    let ok = unsafe {
        libc::sysctlbyname(
            c"hw.memsize".as_ptr(),
            (&raw mut size).cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    (ok == 0 && size > 0).then_some(size)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn total_memory_bytes() -> Option<u64> {
    None
}

fn pressure_config_from_environment() -> anyhow::Result<celld_logic::pressure::PressureConfig> {
    // Residency is a hard cap (`CELLD_MAX_RESIDENT_CELLS` -> `Config::max_resident`),
    // enforced at admission -- not a pressure watermark, so it is not built here.
    // `CELLD_RESIDENT_LOW_WATER` is accepted for compatibility but no longer
    // drives a proactive residency walk down; idle cells are reclaimed on demand
    // and by idle-eviction instead.
    //
    // RSS shedding is on by default: a node that runs into its memory ceiling
    // must give cells back, not OOM. The default ceiling is 80% of what the
    // process may use; an explicit CELLD_MAX_RSS_MB overrides it, and 0
    // disables shedding entirely.
    let rss_high_bytes = match std::env::var("CELLD_MAX_RSS_MB") {
        Err(_) => total_memory_bytes().map(|total| total / 5 * 4),
        Ok(value) => match value.trim().parse::<u64>() {
            Ok(0) => None,
            Ok(megabytes) => Some(megabytes.saturating_mul(1024 * 1024)),
            Err(error) => {
                return Err(anyhow::anyhow!("CELLD_MAX_RSS_MB must be a number: {error}"));
            }
        },
    };
    Ok(celld_logic::pressure::PressureConfig {
        rss_high_bytes,
        cpu_high_x100: positive_environment::<u64>("CELLD_MAX_CPU_PERCENT")?
            .map(|percent| percent.saturating_mul(100)),
    })
}

fn positive_environment<T>(name: &str) -> anyhow::Result<Option<T>>
where
    T: std::str::FromStr + Default + PartialOrd + std::fmt::Display,
    T::Err: std::fmt::Display,
{
    let Ok(value) = std::env::var(name) else {
        return Ok(None);
    };
    let parsed = value
        .trim()
        .parse::<T>()
        .map_err(|error| anyhow::anyhow!("{name} must be a positive number: {error}"))?;
    if parsed <= T::default() {
        anyhow::bail!("{name} must be greater than zero, not {parsed}");
    }
    Ok(Some(parsed))
}

fn local_cache_max_bytes_from_environment() -> anyhow::Result<Option<u64>> {
    let Ok(value) = std::env::var("CELLD_LOCAL_CACHE_MAX_BYTES") else {
        return Ok(Some(DEFAULT_LOCAL_CACHE_MAX_BYTES));
    };
    let bytes = value.trim().parse::<u64>().map_err(|error| {
        anyhow::anyhow!("CELLD_LOCAL_CACHE_MAX_BYTES must be a byte count: {error}")
    })?;
    Ok((bytes > 0).then_some(bytes))
}

/// Signals cancellation when the connection handling this request goes away.
struct HangUp(Option<oneshot::Sender<()>>);

impl Drop for HangUp {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}

/// Abandons a forwarded fetch on the owner when the peer connection carrying
/// it goes away. Disarmed by clearing the id once the fetch has answered.
struct AbortPeerFetchOnHangUp {
    runtime: RuntimeManager,
    scope: String,
    request_id: Option<celld::js::RequestId>,
}

impl Drop for AbortPeerFetchOnHangUp {
    fn drop(&mut self) {
        if let Some(request_id) = self.request_id {
            self.runtime.abort_fetch(&self.scope, request_id);
        }
    }
}
