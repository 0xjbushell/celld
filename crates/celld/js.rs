// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The JS engine: rusty_v8 directly. One isolate per cell.
//!
//! This slice runs actual Durable Objects: the worker's default-export `fetch`
//! receives an `env` whose bindings are DO namespaces; `env.NS.get(id).fetch()`
//! instantiates the exported DO class (once per id) with a `state` whose
//! `storage` is backed by the cell's SQLite (crate::storage). DO storage is
//! async in JS, synchronous underneath — the ops are sync Rust, wrapped in
//! `async` by the JS harness.
use crate::asyncrt;
use crate::storage;
use anyhow::{anyhow, Result};
use futures_util::StreamExt as _;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use v8::{ValueDeserializerHelper, ValueSerializerHelper};

/// The pending exception text from a TryCatch scope (a macro so it needs no
/// bound on the crate-private scope traits).
macro_rules! exc {
    ($tc:expr) => {
        $tc.exception()
            .map(|e| e.to_rust_string_lossy(&*$tc))
            .unwrap_or_else(|| "<none>".into())
    };
}

/// A cross-node dispatch: the isolate hit `env.NS.get(id)` for a cell this node
/// does not own. The op hands this to the tokio runtime, which resolves the
/// owner and HTTP-proxies the fetch, replying on `reply` (an async oneshot the
/// async-op future awaits — the JS thread is never blocked).
pub struct DoCallReq {
    pub request_id: Option<RequestId>,
    pub cancel: Option<tokio::sync::oneshot::Receiver<()>>,
    pub scope: String,
    pub name: Option<String>,
    pub url: String,
    pub method: String,
    pub body: Vec<u8>,
    pub headers: Vec<(String, String)>,
    pub reply: tokio::sync::oneshot::Sender<Result<HttpResponse>>,
}
static DO_CALL_TX: OnceLock<tokio::sync::mpsc::UnboundedSender<DoCallReq>> = OnceLock::new();

/// An output-gate request for the co-hosted (same-isolate) Durable Object fast
/// path. That path runs a resident DO's `fetch` inline, bypassing
/// `dispatch_do_call` where the gate lives; the inline handler samples the
/// cell's committed-write position and, on a write, asks the actor to hold the
/// inline response here until the cell is durable. `position` is the sampled
/// committed-write position (the same signal the routed gate uses).
pub struct GateReq {
    pub scope: String,
    pub position: u64,
    pub reply: tokio::sync::oneshot::Sender<Result<(), celld_logic::RequestError>>,
}
static GATE_TX: OnceLock<tokio::sync::mpsc::UnboundedSender<GateReq>> = OnceLock::new();
pub fn set_gate_tx(tx: tokio::sync::mpsc::UnboundedSender<GateReq>) {
    let _ = GATE_TX.set(tx);
}

/// A service-binding call: `env.NAME.fetch()`. Unlike a Durable Object call
/// there is no identity to resolve — any isolate running `script` will do — so
/// the runtime hands this straight to that script's worker pool.
pub struct SvcCallReq {
    /// Fires when the caller's request signal aborts, so the router stops
    /// waiting on the target instead of leaving the call outstanding.
    pub cancel: Option<tokio::sync::oneshot::Receiver<()>>,
    pub script: String,
    pub url: String,
    pub method: String,
    pub body: Vec<u8>,
    pub headers: Vec<(String, String)>,
    pub reply: tokio::sync::oneshot::Sender<Result<HttpResponse>>,
}
static SVC_CALL_TX: OnceLock<tokio::sync::mpsc::UnboundedSender<SvcCallReq>> = OnceLock::new();

/// A Worker assets-binding call. The script name selects the immutable asset
/// index loaded for that Worker; unlike ingress this never falls back into the
/// Worker and therefore cannot recurse.
pub struct AssetCallReq {
    pub script: String,
    pub url: String,
    pub method: String,
    pub headers: Vec<(String, String)>,
    pub reply: tokio::sync::oneshot::Sender<Result<HttpResponse>>,
}
static ASSET_CALL_TX: OnceLock<tokio::sync::mpsc::UnboundedSender<AssetCallReq>> = OnceLock::new();

/// An RPC call on a named `WorkerEntrypoint` of another script. Arguments and
/// the result cross as V8 structured-clone bytes.
pub struct SvcRpcReq {
    pub script: String,
    pub entrypoint: String,
    pub method: String,
    pub args: Vec<u8>,
    pub reply: tokio::sync::oneshot::Sender<Result<Vec<u8>>>,
}
static SVC_RPC_TX: OnceLock<tokio::sync::mpsc::UnboundedSender<SvcRpcReq>> = OnceLock::new();
pub fn set_svc_rpc_tx(tx: tokio::sync::mpsc::UnboundedSender<SvcRpcReq>) {
    let _ = SVC_RPC_TX.set(tx);
}
pub fn set_svc_call_tx(tx: tokio::sync::mpsc::UnboundedSender<SvcCallReq>) {
    let _ = SVC_CALL_TX.set(tx);
}
pub fn set_asset_call_tx(tx: tokio::sync::mpsc::UnboundedSender<AssetCallReq>) {
    let _ = ASSET_CALL_TX.set(tx);
}
pub fn set_do_call_tx(tx: tokio::sync::mpsc::UnboundedSender<DoCallReq>) {
    let _ = DO_CALL_TX.set(tx);
}

/// Hand a Durable Object call to the dispatcher. Public HTTP ingress uses the
/// same queue a Worker's `env.NAME.get(id).fetch()` does, so both resolve
/// ownership, forward, and redispatch by one policy rather than two.
#[must_use]
pub fn submit_do_call(call: DoCallReq) -> bool {
    DO_CALL_TX.get().is_some_and(|tx| tx.send(call).is_ok())
}

/// The arm-time wake-entry gate, output-gate style (matching Durable
/// Objects): `setAlarm()` resolves optimistically on the committed local
/// write — it never yields the cell's event scheduling to remote I/O — and
/// the cell's response edge is withheld until every pending wake-entry PUT
/// has landed. Invariant: an arm the caller has OBSERVED acknowledged
/// (received the response) is covered by a durable entry.
pub struct ArmGate {
    pub bucket: crate::bucket::Bucket,
    pub flusher: Arc<crate::wake::WakeFlusher>,
}
static ARM_GATE: OnceLock<ArmGate> = OnceLock::new();
pub fn set_arm_gate(gate: ArmGate) {
    let _ = ARM_GATE.set(gate);
}

/// Whether this node still holds a wake entry for `cell`.
pub fn wake_entry_tracked(cell: &str) -> bool {
    ARM_GATE.get().is_some_and(|gate| gate.flusher.tracks(cell))
}

/// Adopt the wake entry a restored alarm implies, so consuming that alarm
/// deletes it rather than orphaning it.
pub fn adopt_wake_entry(cell: &str, at_ms: i64) {
    if let Some(gate) = ARM_GATE.get() {
        gate.flusher.adopt(cell, at_ms);
    }
}

/// Bring the bucket's wake entry for `cell` into line with its alarm.
///
/// Arming writes an entry; something has to take it away again once the alarm
/// has been consumed, or the entry outlives its alarm and every later due scan
/// finds it and wakes a cell with nothing to do. `consume_durable` gates that
/// final delete on the consuming commit being replicated -- removing the hint
/// while the commit that consumed the alarm is still only local would lose
/// both the alarm and the record that could have revived it.
pub async fn reconcile_wake_entry(cell: &str, next_alarm_ms: i64, consume_durable: bool) {
    let Some(gate) = ARM_GATE.get() else {
        return;
    };
    gate.flusher
        .reconcile(&gate.bucket, cell, next_alarm_ms, consume_durable)
        .await;
}

type ArmGateRx = tokio::sync::oneshot::Receiver<Result<(), String>>;
static PENDING_ARM_GATES: OnceLock<Mutex<HashMap<String, Vec<ArmGateRx>>>> = OnceLock::new();
fn pending_arm_gates() -> &'static Mutex<HashMap<String, Vec<ArmGateRx>>> {
    PENDING_ARM_GATES.get_or_init(Default::default)
}

/// A committed alarm tightened the durable wake bound: launch the entry PUT
/// and register it against the cell's output gate. No-op when the bound
/// already covers it or no gate is configured (unit tests).
fn spawn_arm_gate(cell: &str, at_ms: i64) {
    let Some(gate) = ARM_GATE.get() else { return };
    let Some(celld_logic::wake::Op::Put { key, due_ms }) = gate.flusher.arm_op(cell, at_ms) else {
        return;
    };
    let cell_ = cell.to_string();
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let mut pending = pending_arm_gates().lock().unwrap();
        let gates = pending.entry(cell_.clone()).or_default();
        // Arms with no response edge to drain them (alarm handlers,
        // WebSocket events) must not accumulate: drop already-settled gates.
        gates.retain_mut(|gate| {
            matches!(
                gate.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            )
        });
        gates.push(rx);
    }
    asyncrt::op_handle().spawn(async move {
        let gate = ARM_GATE.get().unwrap();
        // The core may have demanded this PUT because the tracked entry's
        // consume-delete is already on the wire; the PUT must land after
        // that delete, not race it (S3 orders concurrent same-key writes
        // arbitrarily).
        gate.flusher.await_no_inflight_delete(&cell_).await;
        let body = format!("{{\"cell\":{cell_:?},\"due_ms\":{due_ms}}}");
        let result = gate
            .bucket
            .put(&key, body.into_bytes())
            .await
            .map(|_| gate.flusher.confirm_arm(&cell_, due_ms, key))
            .map_err(|e| format!("setAlarm wake entry: {e}"));
        let _ = tx.send(result);
    });
}

/// Await the cell's pending wake-entry PUTs — the output-gate drain a
/// response edge performs before releasing its reply. An error means an arm
/// the response would acknowledge is not durably covered; the caller must
/// fail the response rather than lie (the flusher retries the entry).
pub async fn drain_arm_gates(cell: &str) -> Result<(), String> {
    let gates = pending_arm_gates().lock().unwrap().remove(cell);
    let Some(gates) = gates else { return Ok(()) };
    for gate in gates {
        match gate.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(error),
            Err(_) => return Err("wake-entry gate task dropped".into()),
        }
    }
    Ok(())
}

pub type RequestId = u128;

#[derive(Clone, Copy)]
pub struct FetchRequest<'a> {
    pub url: &'a str,
    pub method: &'a str,
    pub body: &'a [u8],
    pub headers: &'a [(String, String)],
    pub request_id: Option<RequestId>,
}

/// Allocate an id for an ingress or service-binding request so it can be
/// aborted mid-flight.
pub fn next_request_id() -> RequestId {
    next_do_request_id()
}

static NEXT_DO_REQUEST_ID: AtomicU64 = AtomicU64::new(1);
static DO_REQUEST_PROCESS_PREFIX: OnceLock<u64> = OnceLock::new();
static DO_CALL_CANCELS: OnceLock<
    std::sync::Mutex<HashMap<RequestId, tokio::sync::oneshot::Sender<()>>>,
> = OnceLock::new();
fn do_call_cancels(
) -> &'static std::sync::Mutex<HashMap<RequestId, tokio::sync::oneshot::Sender<()>>> {
    DO_CALL_CANCELS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

fn next_do_request_id() -> RequestId {
    let prefix = *DO_REQUEST_PROCESS_PREFIX.get_or_init(|| {
        let mut bytes = [0; 8];
        getrandom::fill(&mut bytes).expect("OS random source unavailable");
        u64::from_ne_bytes(bytes)
    });
    let sequence = NEXT_DO_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    (u128::from(prefix) << 64) | u128::from(sequence)
}

pub fn request_id_string(request_id: RequestId) -> String {
    format!("{request_id:032x}")
}

pub fn parse_request_id(value: &str) -> Option<RequestId> {
    u128::from_str_radix(value, 16).ok()
}

struct DoCallCancelGuard(RequestId);

impl Drop for DoCallCancelGuard {
    fn drop(&mut self) {
        do_call_cancels().lock().unwrap().remove(&self.0);
    }
}

/// An RPC payload crossing the host boundary. JS stubs marshal by V8
/// structured clone (`V8`); the legacy cross-node JSON envelope and the test
/// harness use `Json`. `__dispatchRpc` answers in the flavor it was asked in.
pub enum RpcData {
    Json(String),
    V8(Vec<u8>),
}

/// A native Durable Object RPC call (`stub.someMethod(...args)`).
/// Routing/activation is identical to fetch calls.
pub struct RpcCallReq {
    pub scope: String,
    pub name: Option<String>,
    pub method: String,
    pub args: RpcData,
    pub reply: tokio::sync::oneshot::Sender<Result<RpcData>>,
}
static RPC_CALL_TX: OnceLock<tokio::sync::mpsc::UnboundedSender<RpcCallReq>> = OnceLock::new();
pub fn set_rpc_call_tx(tx: tokio::sync::mpsc::UnboundedSender<RpcCallReq>) {
    let _ = RPC_CALL_TX.set(tx);
}

pub struct OutboundWsReq {
    pub scope: String,
    pub id: u64,
    pub url: String,
    pub protocols: Vec<String>,
    /// Present for an isolate-polled (Worker) socket. Created and registered
    /// on the JS thread before the request is sent, so `__ws_next` can never
    /// run ahead of its own queue.
    pub pull: Option<WsPullSender>,
    /// Extra request headers, for the `fetch()` upgrade form.
    pub headers: Vec<(String, String)>,
    /// A `fetch()` upgrade wants the whole handshake outcome, including the
    /// ordinary response a server that declines to upgrade sent instead.
    pub want_response: bool,
    pub reply: tokio::sync::oneshot::Sender<Result<OutboundWsOpen>>,
}

/// The ordinary HTTP response a server sent instead of upgrading. `fetch()`
/// returns it verbatim rather than turning it into a connection error.
pub struct DeclinedUpgrade {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// What an outbound handshake produced.
pub struct OutboundWsOpen {
    pub protocol: Option<String>,
    pub declined: Option<DeclinedUpgrade>,
}
static OUTBOUND_WS_TX: OnceLock<tokio::sync::mpsc::UnboundedSender<OutboundWsReq>> =
    OnceLock::new();
pub fn set_outbound_ws_tx(tx: tokio::sync::mpsc::UnboundedSender<OutboundWsReq>) {
    let _ = OUTBOUND_WS_TX.set(tx);
}

static NEXT_TIMER_ID: AtomicU64 = AtomicU64::new(1);
static TIMER_CANCELS: OnceLock<std::sync::Mutex<HashMap<u64, tokio::sync::oneshot::Sender<()>>>> =
    OnceLock::new();
fn timer_cancels() -> &'static std::sync::Mutex<HashMap<u64, tokio::sync::oneshot::Sender<()>>> {
    TIMER_CANCELS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}
static NEXT_HTTP_STREAM_ID: AtomicU64 = AtomicU64::new(1);
enum HttpStreamSource {
    Response(reqwest::Response),
    Receiver(tokio::sync::mpsc::Receiver<Result<Vec<u8>, String>>),
    Stream(HttpChunkStream),
}
struct HttpStreamEntry {
    created: Instant,
    source: Option<HttpStreamSource>,
    cancelled: tokio::sync::watch::Sender<bool>,
}
static HTTP_STREAMS: OnceLock<std::sync::Mutex<HashMap<u64, HttpStreamEntry>>> = OnceLock::new();
fn http_streams() -> &'static std::sync::Mutex<HashMap<u64, HttpStreamEntry>> {
    HTTP_STREAMS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}
fn register_http_stream(stream_id: u64, source: HttpStreamSource) {
    let (cancelled, _) = tokio::sync::watch::channel(false);
    let mut streams = http_streams().lock().unwrap();
    streams.retain(|_, stream| stream.created.elapsed() < Duration::from_secs(60));
    streams.insert(
        stream_id,
        HttpStreamEntry {
            created: Instant::now(),
            source: Some(source),
            cancelled,
        },
    );
}
struct ResponseStreamWriter {
    created: Instant,
    writer: tokio::sync::mpsc::Sender<Result<Vec<u8>, String>>,
    finished: tokio::sync::watch::Sender<bool>,
}
static RESPONSE_STREAM_WRITERS: OnceLock<std::sync::Mutex<HashMap<u64, ResponseStreamWriter>>> =
    OnceLock::new();
fn response_stream_writers() -> &'static std::sync::Mutex<HashMap<u64, ResponseStreamWriter>> {
    RESPONSE_STREAM_WRITERS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

pub type HttpChunkStream =
    Pin<Box<dyn futures_util::Stream<Item = Result<Vec<u8>, String>> + Send>>;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WsTarget {
    pub id: u64,
    pub scope: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_node: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_addr: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_epoch: Option<u64>,
}

/// Encode a cell/service response for the JS side. A text body crosses as a
/// JS string (cheap, lossless), binary as a byte array, and a streaming body
/// by id — serializing a Vec<u8> as a JSON number array is the dominant cost
/// for real DO responses. `ws_target` is carried only by the DO path.
fn encode_http_response(mut response: HttpResponse, ws_target: bool) -> String {
    let mut obj = serde_json::json!({
        "status": response.status,
        "headers": response.headers,
    });
    if ws_target {
        obj["wsTarget"] = serde_json::json!(response.ws);
    }
    if let Some(stream) = response.stream.take() {
        let stream_id = NEXT_HTTP_STREAM_ID.fetch_add(1, Ordering::Relaxed);
        register_http_stream(stream_id, HttpStreamSource::Stream(stream));
        obj["streamId"] = serde_json::json!(stream_id);
    } else {
        match std::str::from_utf8(&response.body) {
            Ok(text) => obj["body"] = serde_json::Value::String(text.into()),
            Err(_) => obj["bodyBytes"] = serde_json::json!(response.body),
        }
    }
    obj.to_string()
}

pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
    /// A final Worker response that directly forwards an outbound fetch body.
    /// Main hands this reqwest stream to Axum without materializing it.
    pub stream: Option<HttpChunkStream>,
    pub headers: Vec<(String, String)>,
    pub ws: Option<WsTarget>,
    /// The cell's committed-write position after the handler ran, for a local
    /// Durable Object request. The shell gates the response on durability when
    /// this advanced past the cell's last seen position. `None` for responses
    /// with no cell storage (Worker, asset, or proxied remote).
    pub write_position: Option<u64>,
}

/// Jobs accepted by one resident cell isolate. While a handler awaits an
/// outbound operation, the isolate may service another event, matching the
/// Durable Objects event-loop semantics and allowing RPC call-backs (A→B→A).
pub enum CellJob {
    Fetch {
        request_id: Option<RequestId>,
        scope: String,
        name: Option<String>,
        url: String,
        method: String,
        body: Vec<u8>,
        headers: Vec<(String, String)>,
        reply: tokio::sync::oneshot::Sender<Result<HttpResponse>>,
    },
    /// Run the worker's `export default fetch` INSIDE this cell isolate. When
    /// the worker routes `env.NS.get(id)` to a scope this isolate owns, the DO
    /// call resolves in-isolate, avoiding the `__do_call` host round trip.
    WorkerFetch {
        request_id: RequestId,
        url: String,
        method: String,
        body: Vec<u8>,
        headers: Vec<(String, String)>,
        /// Keeps the landing cell unavailable to another top-level Worker
        /// request until this event, including post-response work, is done.
        inline_activity: crate::CellActivityGuard,
        /// If this job is observed while another event is already driving the
        /// landing isolate, run it on the ordinary stateless Worker pool. A
        /// Worker fetch cannot be nested in the same V8 isolate, but its DO
        /// calls may safely re-enter this cell through the normal host path.
        fallback_workers: Arc<crate::WorkerPool>,
        reply: tokio::sync::oneshot::Sender<Result<HttpResponse>>,
    },
    Rpc {
        scope: String,
        name: Option<String>,
        method: String,
        args: RpcData,
        reply: tokio::sync::oneshot::Sender<Result<RpcData>>,
    },
    WsOpen {
        scope: String,
        ws_id: u64,
        protocol: String,
        reply: tokio::sync::oneshot::Sender<Result<()>>,
    },
    WsMessage {
        scope: String,
        ws_id: u64,
        data: WsIn,
        reply: tokio::sync::oneshot::Sender<Result<WsDispatch>>,
    },
    WsClosed {
        scope: String,
        ws_id: u64,
        code: u16,
        reason: String,
        was_clean: bool,
        reply: tokio::sync::oneshot::Sender<Result<()>>,
    },
    AbortFetch {
        request_id: RequestId,
    },
    Alarm {
        scope: String,
        scheduled_ms: i64,
        reply: tokio::sync::oneshot::Sender<Result<Option<i64>>>,
    },
    Shutdown,
}

/// Outbound WebSocket traffic from a DO's `ws.send`/`ws.close`. The host holds
/// the socket in a task decoupled from the isolate (so the cell can hibernate
/// while the socket lives); `ws.send` routes here by wsId.
pub enum WsOut {
    Text(String),
    Binary(Vec<u8>),
    Close(u16, String),
}

/// The result of delivering one `webSocketMessage`. `frames` are the outbound
/// frames the handler produced, captured by the output gate; `write_position`
/// is the cell's committed-write count after the handler when it advanced past
/// where it stood before — i.e. the handler wrote, and its frames must be held
/// until that position is durable. `None` means no write: flush the frames.
pub struct WsDispatch {
    pub frames: Vec<(u64, WsOut)>,
    pub write_position: Option<u64>,
}

/// One inbound event on a socket the ISOLATE polls, rather than one the host
/// pushes into a cell.
///
/// A Durable Object socket must survive between events and wake a hibernated
/// cell, so its frames arrive as `CellJob`s. A Worker socket cannot work that
/// way: the stateless pool has no addressable isolate, and `asyncrt`'s region
/// aborts every pending op when the request ends. That is not a limitation to
/// route around — it is exactly the lifetime Cloudflare gives a Worker socket,
/// which lives and dies with its `IoContext`. So the isolate pulls, the same
/// way it already pulls a streamed response body.
pub enum WsPull {
    Open(String),
    Text(String),
    Binary(Vec<u8>),
    Close(u16, String, bool),
}

/// Tags for the byte frame `__ws_next` resolves with. A tagged buffer keeps a
/// binary message on its fast path instead of base64 through a JSON envelope.
const WS_PULL_TAG_TEXT: u8 = 0;
const WS_PULL_TAG_BINARY: u8 = 1;
const WS_PULL_TAG_OPEN: u8 = 2;
const WS_PULL_TAG_CLOSE: u8 = 3;

impl WsPull {
    fn encode(self) -> Vec<u8> {
        let (tag, mut body) = match self {
            WsPull::Text(text) => (WS_PULL_TAG_TEXT, text.into_bytes()),
            WsPull::Binary(bytes) => (WS_PULL_TAG_BINARY, bytes),
            WsPull::Open(protocol) => (WS_PULL_TAG_OPEN, protocol.into_bytes()),
            WsPull::Close(code, reason, was_clean) => (
                WS_PULL_TAG_CLOSE,
                serde_json::json!({
                    "code": code,
                    "reason": reason,
                    "wasClean": was_clean,
                })
                .to_string()
                .into_bytes(),
            ),
        };
        let mut framed = Vec::with_capacity(body.len() + 1);
        framed.push(tag);
        framed.append(&mut body);
        framed
    }
}

pub type WsPullReceiver = tokio::sync::mpsc::UnboundedReceiver<WsPull>;
pub type WsPullSender = tokio::sync::mpsc::UnboundedSender<WsPull>;

/// One socket's inbound queue. Shared so an op can await it without holding
/// the registry lock; one isolate polls a given socket serially.
type WsPullQueue = Arc<tokio::sync::Mutex<WsPullReceiver>>;
type WsPullRegistry = std::sync::Mutex<HashMap<u64, WsPullQueue>>;

/// Inbound queues for isolate-polled sockets, keyed by wsId.
static WS_PULL: OnceLock<WsPullRegistry> = OnceLock::new();

fn ws_pull() -> &'static WsPullRegistry {
    WS_PULL.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

pub fn ws_pull_register(id: u64, rx: WsPullReceiver) {
    ws_pull()
        .lock()
        .unwrap()
        .insert(id, Arc::new(tokio::sync::Mutex::new(rx)));
}

pub fn ws_pull_unregister(id: u64) {
    ws_pull().lock().unwrap().remove(&id);
}

thread_local! {
    /// Sockets opened by the region currently running on this thread, one
    /// frame per nested region. `asyncrt` aborts a region's pending ops on
    /// exit; these are the host-side resources that abort cannot reach, so
    /// the region closes them itself.
    static WS_PULL_REGION: RefCell<Vec<Vec<u64>>> = const { RefCell::new(Vec::new()) };
    /// Sockets opened before the region exists. A handler runs synchronously
    /// up to its first await BEFORE its run loop is entered, so a socket
    /// constructed at the top of a handler is created with no frame to belong
    /// to. The loop adopts these when it starts.
    static WS_PULL_PENDING: RefCell<Vec<u64>> = const { RefCell::new(Vec::new()) };
}

pub fn ws_region_enter() {
    let adopted =
        WS_PULL_PENDING.with(|pending| pending.borrow_mut().drain(..).collect::<Vec<_>>());
    WS_PULL_REGION.with(|regions| regions.borrow_mut().push(adopted));
}

/// Close every isolate-polled socket the closing region opened. A Worker
/// socket lives and dies with its request, exactly as it does on Cloudflare.
pub fn ws_region_exit() {
    let opened = WS_PULL_REGION.with(|regions| regions.borrow_mut().pop().unwrap_or_default());
    for id in opened {
        ws_emit(id, WsOut::Close(1001, "request ended".into()));
        ws_pull_unregister(id);
        ws_unregister(id);
    }
}

fn ws_region_track(id: u64) {
    WS_PULL_REGION.with(|regions| {
        if let Some(current) = regions.borrow_mut().last_mut() {
            current.push(id);
            return;
        }
        WS_PULL_PENDING.with(|pending| pending.borrow_mut().push(id));
    });
}
pub enum WsIn {
    Text(String),
    Binary(Vec<u8>),
}
struct WsMeta {
    scope: String,
    hibernatable: bool,
    tags: Vec<String>,
    attachment: Option<String>,
    pending: Vec<WsOut>,
}
#[derive(Default)]
struct WsRegistry {
    outputs: HashMap<u64, tokio::sync::mpsc::UnboundedSender<WsOut>>,
    metadata: HashMap<u64, WsMeta>,
}
impl WsRegistry {
    fn register(&mut self, id: u64, tx: tokio::sync::mpsc::UnboundedSender<WsOut>) {
        if let Some(meta) = self.metadata.get_mut(&id) {
            for pending in meta.pending.drain(..) {
                let _ = tx.send(pending);
            }
        }
        self.outputs.insert(id, tx);
    }

    fn unregister(&mut self, id: u64) -> Option<WsMeta> {
        self.outputs.remove(&id);
        self.metadata.remove(&id)
    }

    fn emit(&mut self, id: u64, out: WsOut) {
        if let Some(tx) = self.outputs.get(&id) {
            tracing::debug!(ws_id = id, "queued outbound WebSocket frame");
            let _ = tx.send(out);
        } else if let Some(meta) = self.metadata.get_mut(&id) {
            tracing::debug!(ws_id = id, "buffered pre-upgrade WebSocket frame");
            meta.pending.push(out);
        } else {
            // The socket is gone and the frame has nowhere to go. Silence here
            // is what made a held frame indistinguishable from a sent one.
            tracing::warn!(ws_id = id, "dropped a frame for a closed WebSocket");
        }
    }
}
static WS_REGISTRY: OnceLock<std::sync::Mutex<WsRegistry>> = OnceLock::new();
static REGULAR_WS_COUNTS: OnceLock<std::sync::Mutex<HashMap<String, usize>>> = OnceLock::new();
static NEXT_WS_ID: AtomicU64 = AtomicU64::new(1);
fn ws_registry() -> &'static std::sync::Mutex<WsRegistry> {
    WS_REGISTRY.get_or_init(|| std::sync::Mutex::new(WsRegistry::default()))
}
fn regular_ws_counts() -> &'static std::sync::Mutex<HashMap<String, usize>> {
    REGULAR_WS_COUNTS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}
fn increment_regular_ws(scope: &str) {
    *regular_ws_counts()
        .lock()
        .unwrap()
        .entry(scope.to_string())
        .or_default() += 1;
}
fn decrement_regular_ws(scope: &str) {
    let mut counts = regular_ws_counts().lock().unwrap();
    let Some(count) = counts.get_mut(scope) else {
        return;
    };
    *count = count.saturating_sub(1);
    if *count == 0 {
        counts.remove(scope);
    }
}
pub fn has_regular_websocket(scope: &str) -> bool {
    regular_ws_counts()
        .lock()
        .unwrap()
        .get(scope)
        .is_some_and(|count| *count > 0)
}
pub fn ws_hibernatable(id: u64) -> Option<bool> {
    ws_registry()
        .lock()
        .unwrap()
        .metadata
        .get(&id)
        .map(|meta| meta.hibernatable)
}
pub fn ws_next_id() -> u64 {
    NEXT_WS_ID.fetch_add(1, Ordering::Relaxed)
}
pub fn ws_register(id: u64, tx: tokio::sync::mpsc::UnboundedSender<WsOut>) {
    ws_registry().lock().unwrap().register(id, tx);
}
pub fn ws_register_outbound(id: u64, scope: &str) {
    let inserted = {
        let mut registry = ws_registry().lock().unwrap();
        if let std::collections::hash_map::Entry::Vacant(entry) = registry.metadata.entry(id) {
            entry.insert(WsMeta {
                scope: scope.to_string(),
                hibernatable: false,
                tags: Vec::new(),
                attachment: None,
                pending: Vec::new(),
            });
            true
        } else {
            false
        }
    };
    if inserted {
        increment_regular_ws(scope);
    }
}
pub fn ws_unregister(id: u64) {
    let meta = ws_registry().lock().unwrap().unregister(id);
    if let Some(meta) = meta.filter(|meta| !meta.hibernatable) {
        decrement_regular_ws(&meta.scope);
    }
}
thread_local! {
    // Output-gate capture. While a `webSocketMessage` runs, its outbound frames
    // are collected here instead of hitting the wire, so the shell can hold them
    // until the message's write is durable. A stack: a reentrant event pushes
    // its own buffer, and frames nested inside a capturing event land on top.
    //
    // What may be captured is decided per socket, by `ws_gate_may_hold`, and
    // that check is what keeps this from deadlocking. Delivering a message runs
    // the isolate's event loop, which drives every ready task on this thread,
    // not just this handler -- so anything captured here is withheld for as
    // long as that loop runs.
    static WS_CAPTURE: RefCell<Vec<Vec<(u64, WsOut)>>> = const { RefCell::new(Vec::new()) };
}

fn ws_capture_begin() {
    WS_CAPTURE.with(|c| c.borrow_mut().push(Vec::new()));
}

fn ws_capture_take() -> Vec<(u64, WsOut)> {
    WS_CAPTURE.with(|c| c.borrow_mut().pop().unwrap_or_default())
}

/// Whether a socket is one the output gate may hold frames for: a hibernatable
/// transport, whose messages the host pushes into the cell.
///
/// A socket the isolate opened and polls itself is not. Its handler runs inside
/// the isolate's event loop, and that loop is what a held frame would be
/// waiting on: the reply to a frame the gate is withholding never arrives, so
/// the loop never finishes, so the frame is never released. Frames from those
/// sockets go straight out, as they did before the gate existed.
fn ws_gate_may_hold(id: u64) -> bool {
    let registry = ws_registry().lock().unwrap();
    registry
        .metadata
        .get(&id)
        .is_some_and(|meta| meta.hibernatable)
}

fn ws_emit(id: u64, out: WsOut) {
    let capturing = WS_CAPTURE.with(|c| !c.borrow().is_empty());
    if capturing && ws_gate_may_hold(id) {
        WS_CAPTURE.with(|c| c.borrow_mut().last_mut().unwrap().push((id, out)));
        return;
    }
    ws_registry().lock().unwrap().emit(id, out);
}

/// Flush frames the output gate held: send each to its socket's task. Called
/// from the actor thread once the gating write is proved durable.
pub fn ws_emit_batch(frames: Vec<(u64, WsOut)>) {
    let mut registry = ws_registry().lock().unwrap();
    for (id, out) in frames {
        registry.emit(id, out);
    }
}

/// Break a cell's sockets: the output gate could not prove a write durable, so
/// close every socket the cell owns rather than let a client keep a connection
/// whose acknowledged effects may not have persisted (a reset DO).
pub fn ws_close_scope(scope: &str, code: u16, reason: &str) {
    tracing::warn!(scope, code, "PROBE close_scope");
    let mut registry = ws_registry().lock().unwrap();
    let ids: Vec<u64> = registry
        .metadata
        .iter()
        .filter(|(_, meta)| meta.scope == scope)
        .map(|(id, _)| *id)
        .collect();
    for id in ids {
        registry.emit(id, WsOut::Close(code, reason.to_string()));
    }
}

thread_local! {
    // JS promise resolvers awaiting an async op, keyed by the op's id.
    static PROMISES: RefCell<HashMap<u64, v8::Global<v8::PromiseResolver>>> =
        RefCell::new(HashMap::new());
    // One outbound HTTP client per JS thread — building it per fetch rebuilds
    // the TLS stack every call (async-op-hazards.md).
    static HTTP: reqwest::Client = reqwest::Client::new();
    static HTTP_MANUAL: reqwest::Client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none()).build().unwrap();
    static DO_ID_KEYS: RefCell<HashMap<String, [u8; 32]>> = RefCell::new(HashMap::new());
    static CANCELLED_REQUESTS: RefCell<HashSet<RequestId>> = RefCell::new(HashSet::new());
}
fn promise_store(id: u64, r: v8::Global<v8::PromiseResolver>) {
    PROMISES.with(|p| {
        p.borrow_mut().insert(id, r);
    });
}

/// Resolve or reject op `id`'s promise with its outcome.
fn resolve_res(tc: &mut v8::PinScope, id: u64, res: Result<asyncrt::OpOut, String>) {
    let g = match PROMISES.with(|p| p.borrow_mut().remove(&id)) {
        Some(g) => g,
        None => return,
    };
    let r = v8::Local::new(tc, g);
    match res {
        Ok(asyncrt::OpOut::Str(v)) => {
            let s = v8::String::new(tc, &v).unwrap();
            r.resolve(tc, s.into());
        }
        Ok(asyncrt::OpOut::Bytes(b)) => {
            let v = bytes_value(tc, b);
            r.resolve(tc, v);
        }
        Err(e) => {
            let s = v8::String::new(tc, &e).unwrap();
            let ex = v8::Exception::error(tc, s);
            r.reject(tc, ex);
        }
    }
}

/// Move `bytes` into a `Uint8Array` without copying.
fn bytes_value<'s>(scope: &mut v8::PinScope<'s, '_>, bytes: Vec<u8>) -> v8::Local<'s, v8::Value> {
    let len = bytes.len();
    let store = v8::ArrayBuffer::new_backing_store_from_vec(bytes).make_shared();
    let buffer = v8::ArrayBuffer::with_backing_store(scope, &store);
    v8::Uint8Array::new(scope, buffer, 0, len).unwrap().into()
}

fn handler_budget() -> Duration {
    static BUDGET: OnceLock<Duration> = OnceLock::new();
    *BUDGET.get_or_init(|| {
        Duration::from_secs(
            std::env::var("CELLD_HANDLER_BUDGET_S")
                .ok()
                .and_then(|value| value.parse().ok())
                .filter(|seconds| *seconds > 0)
                .unwrap_or(300),
        )
    })
}

/// Aborts raised on another thread — an HTTP or service-binding caller
/// disconnecting while the target runs in a worker pool. Durable Object aborts
/// also arrive as a reentrant `CellJob::AbortFetch` when a cell thread can
/// service its own channel; a pool worker is blocked inside its handler, so
/// the abort has to be visible from wherever the loop happens to be.
///
/// The counter keeps the common path to one relaxed atomic load per loop turn:
/// with nothing pending, the mutex is never taken.
static PENDING_ABORTS: OnceLock<std::sync::Mutex<std::collections::HashSet<RequestId>>> =
    OnceLock::new();
static PENDING_ABORT_COUNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

fn pending_aborts() -> &'static std::sync::Mutex<std::collections::HashSet<RequestId>> {
    PENDING_ABORTS.get_or_init(Default::default)
}

/// Mark an in-flight request cancelled from any thread.
pub fn abort_request(request_id: RequestId) {
    if pending_aborts().lock().unwrap().insert(request_id) {
        PENDING_ABORT_COUNT.fetch_add(1, Ordering::Relaxed);
    }
}

fn clear_request_cancellation(request_id: RequestId) {
    let _ = take_pending_abort(request_id);
}

fn take_pending_abort(request_id: RequestId) -> bool {
    if PENDING_ABORT_COUNT.load(Ordering::Relaxed) == 0 {
        return false;
    }
    if pending_aborts().lock().unwrap().remove(&request_id) {
        PENDING_ABORT_COUNT.fetch_sub(1, Ordering::Relaxed);
        return true;
    }
    false
}

fn take_request_cancellation(request_id: Option<RequestId>) -> bool {
    request_id.is_some_and(|request_id| {
        CANCELLED_REQUESTS.with(|requests| requests.borrow_mut().remove(&request_id))
            || take_pending_abort(request_id)
    })
}

enum RunLoopOutcome<T> {
    Complete(T),
    RequestCancelled,
}

/// Drive a promise and apply `on_fulfilled` as soon as it settles. The callback
/// may return a composite waitUntil promise after publishing its response.
/// Top-level fetches retain only registered waitUntil work; Durable Object
/// events additionally drain I/O started by the handler because libraries
/// commonly launch that work from synchronous callbacks.
///
/// Every exit path drops the region and purges unresolved host promises. A
/// panicked/aborted op is rejected into JS rather than hanging the isolate.
fn run_loop_then<'s, T, F>(
    tc: &mut v8::PinScope<'s, '_>,
    promise: v8::Local<v8::Promise>,
    cell_rx: Option<&std::sync::mpsc::Receiver<CellJob>>,
    actor_scope: Option<&str>,
    request_id: Option<RequestId>,
    drain_spawned_ops: bool,
    on_fulfilled: F,
) -> Result<RunLoopOutcome<T>>
where
    F: FnOnce(
        &mut v8::PinScope<'s, '_>,
        v8::Local<'s, v8::Value>,
    ) -> Result<(T, Option<v8::Local<'s, v8::Promise>>)>,
{
    let mut region: JoinSet<(u64, Result<asyncrt::OpOut, String>)> = JoinSet::new();
    ws_region_enter();
    // Paired with `ws_region_exit` on the close path below, which every exit
    // from this function reaches.
    struct WsRegionGuard;
    impl Drop for WsRegionGuard {
        fn drop(&mut self) {
            ws_region_exit();
        }
    }
    let _ws_region = WsRegionGuard;
    let mut idmap: HashMap<tokio::task::Id, u64> = HashMap::new();
    let started = Instant::now();
    let budget = handler_budget();
    let mut on_fulfilled = Some(on_fulfilled);
    let mut output = None;
    let mut background = None;
    let mut request_cancelled = false;
    // Unknown until the handler first suspends. Cache the deadline so an actor
    // with pending async I/O does not query SQLite on every 10 ms input-gate
    // wakeup. Reentrant events invalidate it because they may change the alarm.
    let mut next_alarm_at: Option<Option<i64>> = None;
    let result = loop {
        let mut published_this_turn = false;
        if !request_cancelled && take_request_cancellation(request_id) {
            request_cancelled = true;
            if let Some(id) = request_id {
                let _ = abort_incoming_request(tc, id);
            }
            background = end_event_context(tc)?;
        }
        for (id, fut) in asyncrt::drain_spawns() {
            let h = region.spawn_on(async move { (id, fut.await) }, &asyncrt::op_handle());
            idmap.insert(h.id(), id);
        }
        tc.perform_microtask_checkpoint(); // fire .then chains; they may enqueue ops
        if let Some(error) = take_execution_termination(tc) {
            break Err(error);
        }
        for (id, fut) in asyncrt::drain_spawns() {
            let h = region.spawn_on(async move { (id, fut.await) }, &asyncrt::op_handle());
            idmap.insert(h.id(), id);
        }
        if !request_cancelled && output.is_none() {
            match promise.state() {
                v8::PromiseState::Fulfilled => {
                    let callback = on_fulfilled.take().expect("handler callback used once");
                    let (value, wait_until) = callback(tc, promise.result(tc))?;
                    published_this_turn = wait_until.is_some();
                    output = Some(value);
                    background = wait_until;
                }
                v8::PromiseState::Rejected => {
                    break Err(anyhow!("rejected: {}", reject_reason(tc, promise)))
                }
                v8::PromiseState::Pending => {}
            }
        }
        // Reentrant jobs are serviced before AND after the response publishes:
        // post-response waitUntil work may call back into this cell (A→A), and
        // a pending job would otherwise stall until the handler budget.
        if let Some(rx) = cell_rx {
            let mut serviced_event = false;
            while service_reentrant(tc, rx)? {
                serviced_event = true;
                // Adopt ops the job just enqueued (an AbortFetch's abort
                // listener registering waitUntil work, say) BEFORE the next
                // job runs: a reentrant dispatch drains the shared spawn
                // queue into its own region and aborts the survivors on
                // exit, so an op left queued here would be silently killed
                // and its promise purged — this event then waits on it
                // forever.
                for (id, fut) in asyncrt::drain_spawns() {
                    let h = region.spawn_on(async move { (id, fut.await) }, &asyncrt::op_handle());
                    idmap.insert(h.id(), id);
                }
            }
            if output.is_none() && !request_cancelled && take_request_cancellation(request_id) {
                request_cancelled = true;
                if let Some(id) = request_id {
                    let _ = abort_incoming_request(tc, id);
                }
                background = end_event_context(tc)?;
            }
            if serviced_event {
                next_alarm_at = None;
            }
            tc.perform_microtask_checkpoint();
            if let Some(error) = take_execution_termination(tc) {
                break Err(error);
            }
            if !request_cancelled && output.is_none() {
                match promise.state() {
                    v8::PromiseState::Fulfilled => {
                        let callback = on_fulfilled.take().expect("handler callback used once");
                        let (value, wait_until) = callback(tc, promise.result(tc))?;
                        published_this_turn = wait_until.is_some();
                        output = Some(value);
                        background = wait_until;
                    }
                    v8::PromiseState::Rejected => {
                        break Err(anyhow!("rejected: {}", reject_reason(tc, promise)))
                    }
                    v8::PromiseState::Pending => {}
                }
            }
        }
        // Response marshalling can start a bounded JS-to-host body pump and
        // register it with waitUntil. Drive the pump once and move its native
        // ops into this event region before checking whether the region is
        // empty. Pending handlers pay only the branch above.
        if published_this_turn {
            tc.perform_microtask_checkpoint();
            if let Some(error) = take_execution_termination(tc) {
                break Err(error);
            }
            for (id, fut) in asyncrt::drain_spawns() {
                let h = region.spawn_on(async move { (id, fut.await) }, &asyncrt::op_handle());
                idmap.insert(h.id(), id);
            }
        }
        if request_cancelled || output.is_some() {
            let event_done = match background {
                None => true,
                Some(wait_until) => match wait_until.state() {
                    v8::PromiseState::Fulfilled => true,
                    v8::PromiseState::Rejected => {
                        tracing::warn!(
                            error = %reject_reason(tc, wait_until),
                            "waitUntil aggregate rejected",
                        );
                        true
                    }
                    v8::PromiseState::Pending => false,
                },
            };
            if event_done && (request_cancelled || !drain_spawned_ops || region.is_empty()) {
                if request_cancelled {
                    break Ok(RunLoopOutcome::RequestCancelled);
                }
                break Ok(RunLoopOutcome::Complete(output.take().unwrap()));
            }
        }
        if !request_cancelled && output.is_none() {
            if let Some(scope) = actor_scope {
                let alarm_at = *next_alarm_at.get_or_insert_with(|| storage::get_alarm(scope));
                if alarm_at.is_some_and(|alarm_at| alarm_at <= unix_now_ms()) {
                    service_due_alarm(tc, scope, cell_rx.expect("actor events have a receiver"))?;
                    next_alarm_at = None;
                    continue;
                }
            }
        }
        if region.is_empty() {
            if request_cancelled {
                tracing::warn!("request-cancellation waitUntil stalled with no pending op");
                break Ok(RunLoopOutcome::RequestCancelled);
            }
            if output.is_none() {
                if let (Some(rx), Some(Some(alarm_at))) = (cell_rx, next_alarm_at) {
                    let elapsed = started.elapsed();
                    if elapsed >= budget {
                        break Err(anyhow!("handler exceeded {}s budget", budget.as_secs()));
                    }
                    let until_alarm =
                        Duration::from_millis(alarm_at.saturating_sub(unix_now_ms()).max(0) as u64);
                    let wait = (budget - elapsed).min(until_alarm);
                    match rx.recv_timeout(wait) {
                        Ok(job) => {
                            if !handle_reentrant_job(tc, rx, job)? {
                                break Err(anyhow!("cell shutting down"));
                            }
                            next_alarm_at = None;
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                            break Err(anyhow!("cell channel disconnected"));
                        }
                    }
                    continue;
                }
            }
            break match output.take() {
                Some(_) => Err(anyhow!("waitUntil stalled with no pending op")),
                None => Err(anyhow!("handler stalled: awaited work with no pending op")),
            };
        }
        let elapsed = started.elapsed();
        if elapsed >= budget {
            if request_cancelled {
                tracing::warn!("request-cancellation waitUntil exceeded handler budget");
                break Ok(RunLoopOutcome::RequestCancelled);
            }
            break Err(anyhow!("handler exceeded {}s budget", budget.as_secs()));
        }
        // block until one op completes (or the remaining budget elapses). The
        // timeout must be built INSIDE the runtime context, hence the async {}.
        let remaining = budget - elapsed;
        // An abortable ingress or service-binding request has to notice a
        // cancellation raised on another thread, so its wait is bounded the
        // same way an actor's is. Non-abortable internal fetches retain the
        // uncapped wait.
        let mut wait = if cell_rx.is_some() || request_id.is_some() {
            remaining.min(Duration::from_millis(10))
        } else {
            remaining
        };
        if let Some(Some(alarm_at)) = next_alarm_at {
            wait = wait.min(Duration::from_millis(
                alarm_at.saturating_sub(unix_now_ms()).max(0) as u64,
            ));
        }
        let joined = asyncrt::block_on(async {
            tokio::time::timeout(wait, region.join_next_with_id()).await
        });
        match joined {
            Ok(Some(Ok((tid, (id, res))))) => {
                idmap.remove(&tid);
                resolve_res(tc, id, res);
                next_alarm_at = None;
            }
            Ok(Some(Err(je))) => {
                // op panicked/aborted → reject its promise
                if let Some(id) = idmap.remove(&je.id()) {
                    resolve_res(tc, id, Err("op aborted".into()));
                    next_alarm_at = None;
                }
            }
            Ok(None) => {} // region drained concurrently; loop re-checks
            Err(_) if cell_rx.is_some() || request_id.is_some() => continue,
            Err(_) => break Err(anyhow!("handler exceeded {}s budget", budget.as_secs())),
        }
    };
    // region close: `region` drops here (aborts survivors); purge resolvers for
    // anything still pending or enqueued-but-unspawned so PROMISES can't leak.
    for id in idmap.values() {
        PROMISES.with(|p| {
            p.borrow_mut().remove(id);
        });
    }
    for (id, _) in asyncrt::drain_spawns() {
        PROMISES.with(|p| {
            p.borrow_mut().remove(&id);
        });
    }
    result
}

fn run_loop<'s>(
    tc: &mut v8::PinScope<'s, '_>,
    promise: v8::Local<v8::Promise>,
    cell_rx: Option<&std::sync::mpsc::Receiver<CellJob>>,
) -> Result<v8::Local<'s, v8::Value>> {
    match run_loop_then(tc, promise, cell_rx, None, None, false, |_tc, value| {
        Ok((value, None))
    })? {
        RunLoopOutcome::Complete(value) => Ok(value),
        RunLoopOutcome::RequestCancelled => Err(anyhow!("The client has disconnected")),
    }
}

fn resolved_promise<'s>(
    tc: &mut v8::PinScope<'s, '_>,
    value: v8::Local<'s, v8::Value>,
) -> Result<v8::Local<'s, v8::Promise>> {
    let resolver =
        v8::PromiseResolver::new(tc).ok_or_else(|| anyhow!("could not create event promise"))?;
    resolver.resolve(tc, value);
    Ok(resolver.get_promise(tc))
}

/// Publish a fetch response when the handler settles, then keep driving the
/// event region so waitUntil work can complete without delaying headers or a
/// streaming body.
fn complete_fetch_event<'s>(
    tc: &mut v8::PinScope<'s, '_>,
    ret: v8::Local<'s, v8::Value>,
    cell_rx: Option<&std::sync::mpsc::Receiver<CellJob>>,
    critical_scope: Option<&str>,
    // The cell's committed-write position sampled before the handler ran. The
    // response's `write_position` is set only if the handler advanced it, so
    // the output gate sees a per-request write and ignores celld's own
    // activation-time writes (the DO actor name, alarm bookkeeping).
    writes_before: Option<u64>,
    request_id: Option<RequestId>,
    reply: tokio::sync::oneshot::Sender<Result<HttpResponse>>,
) {
    let promise = match ret.try_cast::<v8::Promise>() {
        Ok(promise) => Ok(promise),
        Err(_) => resolved_promise(tc, ret),
    };
    let mut reply = Some(reply);
    let result = promise.and_then(|promise| {
        run_loop_then(
            tc,
            promise,
            cell_rx,
            critical_scope,
            request_id,
            false,
            |tc, value| {
                if let Some(error) = critical_scope.and_then(storage::sql_critical_error) {
                    return Err(anyhow!(error));
                }
                let mut response = read_response(tc, value, cell_rx)?;
                // Gate on durability only if this handler advanced the cell's
                // committed-write position past where it was before the handler
                // ran -- a per-request write, not celld's activation writes.
                response.write_position = match (
                    writes_before,
                    critical_scope.and_then(storage::write_position),
                ) {
                    (Some(before), Some(after)) if after > before => Some(after),
                    _ => None,
                };
                let wait_until = end_event_context(tc)?;
                if let Some(reply) = reply.take() {
                    let _ = reply.send(Ok(response));
                }
                Ok(((), wait_until))
            },
        )
    });
    let result = match critical_scope.and_then(storage::sql_critical_error) {
        Some(error) => Err(anyhow!(error)),
        None => result,
    };
    match result {
        Ok(RunLoopOutcome::Complete(())) => {}
        Ok(RunLoopOutcome::RequestCancelled) => {
            if let Some(reply) = reply.take() {
                let _ = reply.send(Err(anyhow!("The client has disconnected")));
            }
        }
        Err(error) => {
            let _ = end_event_context(tc);
            if let Some(reply) = reply.take() {
                let _ = reply.send(Err(error));
            } else {
                tracing::warn!(%error, "post-response event work failed");
            }
        }
    }
}

fn run_event_value<'s>(
    tc: &mut v8::PinScope<'s, '_>,
    ret: v8::Local<'s, v8::Value>,
    cell_rx: Option<&std::sync::mpsc::Receiver<CellJob>>,
    actor_scope: Option<&str>,
    request_id: Option<RequestId>,
) -> Result<v8::Local<'s, v8::Value>> {
    let promise = match ret.try_cast::<v8::Promise>() {
        Ok(promise) => promise,
        Err(_) => resolved_promise(tc, ret)?,
    };
    let result = run_loop_then(
        tc,
        promise,
        cell_rx,
        actor_scope,
        request_id,
        true,
        |tc, value| Ok((value, end_event_context(tc)?)),
    );
    match result {
        Ok(RunLoopOutcome::Complete(value)) => Ok(value),
        Ok(RunLoopOutcome::RequestCancelled) => Err(anyhow!("The client has disconnected")),
        Err(error) => {
            let _ = end_event_context(tc);
            Err(error)
        }
    }
}

fn handle_reentrant_job(
    tc: &mut v8::PinScope,
    rx: &std::sync::mpsc::Receiver<CellJob>,
    job: CellJob,
) -> Result<bool> {
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
            let result = register_actor_name(tc, &scope, name.as_deref()).and_then(|()| {
                call_dispatch_to(
                    tc,
                    &scope,
                    FetchRequest {
                        url: &url,
                        method: &method,
                        body: &body,
                        headers: &headers,
                        request_id,
                    },
                    rx,
                )
            });
            let _ = reply.send(result);
        }
        CellJob::Rpc {
            scope,
            name,
            method,
            args,
            reply,
        } => {
            let result = register_actor_name(tc, &scope, name.as_deref())
                .and_then(|()| call_dispatch_rpc(tc, &scope, &method, args, rx));
            let _ = reply.send(result);
        }
        CellJob::WsOpen {
            scope,
            ws_id,
            protocol,
            reply,
        } => {
            let result = call_dispatch_ws_open(tc, &scope, ws_id, &protocol, rx);
            let _ = reply.send(result);
        }
        CellJob::WsMessage {
            scope,
            ws_id,
            data,
            reply,
        } => {
            let result = call_dispatch_ws(tc, &scope, ws_id, data, rx);
            let _ = reply.send(result);
        }
        CellJob::WsClosed {
            scope,
            ws_id,
            code,
            reason,
            was_clean,
            reply,
        } => {
            let result = call_dispatch_ws_closed(tc, &scope, ws_id, code, &reason, was_clean, rx);
            let _ = reply.send(result);
        }
        // Two top-level Worker events cannot run concurrently in one isolate.
        // The idle reservation normally prevents this path, but a queued job
        // can be observed by a nested actor event under enough overlap. Move
        // it to the stateless pool; any call back to this cell then arrives as
        // an ordinary reentrant CellJob::Fetch, which is supported above.
        CellJob::WorkerFetch {
            request_id,
            url,
            method,
            body,
            headers,
            inline_activity: _inline_activity,
            fallback_workers,
            reply,
        } => {
            // The reentrant pump implies this isolate is running an actor event,
            // so the sans-IO routing is fixed: a Worker event never runs nested
            // — reschedule to the pool carrying the request identity.
            debug_assert_eq!(
                celld_logic::schedule::route_worker_fetch(true),
                celld_logic::schedule::Route::RescheduleToPool,
            );
            let _ = fallback_workers.send(crate::WorkerJob::Fetch {
                queued_at: Instant::now(),
                url,
                method,
                body,
                headers,
                request_id: Some(request_id),
                reply,
            });
        }
        CellJob::AbortFetch { request_id } => match abort_incoming_request(tc, request_id) {
            Ok(true) => {
                CANCELLED_REQUESTS.with(|requests| {
                    requests.borrow_mut().insert(request_id);
                });
            }
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(%error, request_id, "incoming request abort listener failed");
            }
        },
        CellJob::Alarm {
            scope,
            scheduled_ms,
            reply,
        } => {
            let now = unix_now_ms();
            let result = if now < scheduled_ms {
                Err(anyhow!("alarm dispatched before its deadline"))
            } else {
                service_due_alarm(tc, &scope, rx).map(|()| storage::get_alarm(&scope))
            };
            let _ = reply.send(result);
        }
        CellJob::Shutdown => return Ok(false),
    }
    Ok(true)
}

fn abort_incoming_request(tc: &mut v8::PinScope, request_id: RequestId) -> Result<bool> {
    let global = tc.get_current_context().global(tc);
    let key = v8::String::new(tc, "__abortIncomingRequest").unwrap();
    let function: v8::Local<v8::Function> = global
        .get(tc, key.into())
        .ok_or_else(|| anyhow!("no __abortIncomingRequest"))?
        .try_into()
        .map_err(|_| anyhow!("__abortIncomingRequest is not a function"))?;
    let request_id = v8::String::new(tc, &request_id_string(request_id)).unwrap();
    let recv = v8::undefined(tc).into();
    let result = function
        .call(tc, recv, &[request_id.into()])
        .ok_or_else(|| anyhow!("__abortIncomingRequest threw"))?;
    Ok(result.boolean_value(tc))
}

fn service_reentrant(
    tc: &mut v8::PinScope,
    rx: &std::sync::mpsc::Receiver<CellJob>,
) -> Result<bool> {
    let job = match rx.try_recv() {
        Ok(job) => job,
        Err(std::sync::mpsc::TryRecvError::Empty) => return Ok(false),
        Err(std::sync::mpsc::TryRecvError::Disconnected) => return Ok(false),
    };
    handle_reentrant_job(tc, rx, job)
}

fn unix_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn call_fire_alarm(
    tc: &mut v8::PinScope,
    scope: &str,
    scheduled_time: i64,
    retry: i64,
    rx: &std::sync::mpsc::Receiver<CellJob>,
) -> std::result::Result<(), AlarmRunError> {
    let context = tc.get_current_context();
    let global = context.global(tc);
    let key = v8::String::new(tc, "__fireAlarm").unwrap();
    let function: v8::Local<v8::Function> = global
        .get(tc, key.into())
        .ok_or_else(|| AlarmRunError::infrastructure(anyhow!("no __fireAlarm")))?
        .try_into()
        .map_err(|_| AlarmRunError::infrastructure(anyhow!("__fireAlarm is not a function")))?;
    let scope = v8::String::new(tc, scope).unwrap();
    let scheduled_time = v8::Number::new(tc, scheduled_time as f64);
    let retry = v8::Number::new(tc, retry as f64);
    let recv = v8::undefined(tc).into();
    begin_event_context(tc).map_err(AlarmRunError::infrastructure)?;
    let Some(ret) = function.call(
        tc,
        recv,
        &[scope.into(), scheduled_time.into(), retry.into()],
    ) else {
        let _ = end_event_context(tc);
        return Err(AlarmRunError::user(anyhow!("alarm threw")));
    };
    run_event_value(tc, ret, Some(rx), None, None).map_err(AlarmRunError::user)?;
    Ok(())
}

fn service_due_alarm(
    tc: &mut v8::PinScope,
    scope: &str,
    rx: &std::sync::mpsc::Receiver<CellJob>,
) -> Result<()> {
    let now = unix_now_ms();
    let Some((scheduled_time, retry)) = storage::due_alarm_entry(scope, now) else {
        return Ok(());
    };
    storage::begin_alarm_handler(scope, scheduled_time);
    match call_fire_alarm(tc, scope, scheduled_time, retry, rx) {
        Ok(()) => {
            storage::finish_alarm_handler(scope, true, now);
            tracing::info!(cell = %scope, scheduled_time, "alarm fired reentrantly");
        }
        Err(error) => {
            let counts_against_limit = error.counts_against_limit();
            storage::finish_alarm_handler_with_retry_policy(
                scope,
                false,
                now,
                counts_against_limit,
            );
            tracing::warn!(
                %error,
                cell = %scope,
                scheduled_time,
                counts_against_limit,
                "reentrant alarm failed; backing off",
            );
        }
    }
    Ok(())
}

fn call_dispatch_to(
    tc: &mut v8::PinScope,
    scope: &str,
    request: FetchRequest<'_>,
    rx: &std::sync::mpsc::Receiver<CellJob>,
) -> Result<HttpResponse> {
    let context = tc.get_current_context();
    let global = context.global(tc);
    let key = v8::String::new(tc, "__dispatchTo").unwrap();
    let f: v8::Local<v8::Function> = global
        .get(tc, key.into())
        .ok_or_else(|| anyhow!("no __dispatchTo"))?
        .try_into()
        .map_err(|_| anyhow!("not fn"))?;
    let sc = v8::String::new(tc, scope).unwrap();
    let u = v8::String::new(tc, request.url).unwrap();
    let m = v8::String::new(tc, request.method).unwrap();
    let b = bytes_value(tc, request.body.to_vec());
    let h = v8::String::new(tc, &serde_json::to_string(request.headers)?).unwrap();
    let request_id_value: v8::Local<v8::Value> = match request.request_id {
        Some(request_id) => v8::String::new(tc, &request_id_string(request_id))
            .unwrap()
            .into(),
        None => v8::null(tc).into(),
    };
    let recv = v8::undefined(tc).into();
    begin_event_context(tc)?;
    let Some(ret) = f.call(
        tc,
        recv,
        &[sc.into(), u.into(), m.into(), b, h.into(), request_id_value],
    ) else {
        let _ = end_event_context(tc);
        return Err(anyhow!("dispatchTo threw"));
    };
    let ret = run_event_value(tc, ret, Some(rx), Some(scope), request.request_id);
    if let Some(error) = storage::sql_critical_error(scope) {
        return Err(anyhow!(error));
    }
    let ret = ret?;
    read_response(tc, ret, Some(rx))
}

/// An `RpcData` payload as the JS argument `__dispatchRpc` expects: a string
/// for the JSON flavor, a `Uint8Array` for structured clone.
fn rpc_data_value<'s>(scope: &mut v8::PinScope<'s, '_>, data: RpcData) -> v8::Local<'s, v8::Value> {
    match data {
        RpcData::Json(json) => v8::String::new(scope, &json).unwrap().into(),
        RpcData::V8(bytes) => bytes_value(scope, bytes),
    }
}

/// The inverse: `__dispatchRpc` answers in the flavor it was asked in.
fn rpc_data_ret(scope: &mut v8::PinScope, ret: v8::Local<v8::Value>) -> RpcData {
    match view_bytes(ret) {
        Some(bytes) => RpcData::V8(bytes),
        None => RpcData::Json(ret.to_rust_string_lossy(scope)),
    }
}

fn call_dispatch_rpc(
    tc: &mut v8::PinScope,
    scope: &str,
    method: &str,
    args: RpcData,
    rx: &std::sync::mpsc::Receiver<CellJob>,
) -> Result<RpcData> {
    let context = tc.get_current_context();
    let global = context.global(tc);
    let key = v8::String::new(tc, "__dispatchRpc").unwrap();
    let f: v8::Local<v8::Function> = global
        .get(tc, key.into())
        .ok_or_else(|| anyhow!("no __dispatchRpc"))?
        .try_into()
        .map_err(|_| anyhow!("not fn"))?;
    let sc = v8::String::new(tc, scope).unwrap();
    let method = v8::String::new(tc, method).unwrap();
    let args = rpc_data_value(tc, args);
    let recv = v8::undefined(tc).into();
    begin_event_context(tc)?;
    let Some(ret) = f.call(tc, recv, &[sc.into(), method.into(), args]) else {
        let _ = end_event_context(tc);
        return Err(anyhow!("dispatchRpc threw"));
    };
    let ret = run_event_value(tc, ret, Some(rx), Some(scope), None);
    if let Some(error) = storage::sql_critical_error(scope) {
        return Err(anyhow!(error));
    }
    let ret = ret?;
    Ok(rpc_data_ret(tc, ret))
}

fn call_dispatch_ws(
    tc: &mut v8::PinScope,
    scope: &str,
    ws_id: u64,
    data: WsIn,
    rx: &std::sync::mpsc::Receiver<CellJob>,
) -> Result<WsDispatch> {
    let context = tc.get_current_context();
    let global = context.global(tc);
    let (key, data) = match data {
        WsIn::Text(text) => ("__wsMessage", v8::String::new(tc, &text).unwrap().into()),
        WsIn::Binary(bytes) => ("__wsBinary", bytes_value(tc, bytes)),
    };
    let key = v8::String::new(tc, key).unwrap();
    let f: v8::Local<v8::Function> = global
        .get(tc, key.into())
        .ok_or_else(|| anyhow!("no WebSocket message dispatcher"))?
        .try_into()
        .map_err(|_| anyhow!("not fn"))?;
    let sc = v8::String::new(tc, scope).unwrap();
    let id = v8::Number::new(tc, ws_id as f64);
    let recv = v8::undefined(tc).into();
    // Sample the committed-write position and capture outbound frames across the
    // handler: the output gate holds a message's frames until its write proves
    // durable (the same delta discipline as the fetch path).
    let before = storage::write_position(scope);
    ws_capture_begin();
    begin_event_context(tc)?;
    let Some(ret) = f.call(tc, recv, &[sc.into(), id.into(), data]) else {
        let _ = end_event_context(tc);
        let _ = ws_capture_take();
        return Err(anyhow!("WebSocket message dispatch threw"));
    };
    let result = run_event_value(tc, ret, Some(rx), Some(scope), None);
    let frames = ws_capture_take();
    if let Some(error) = storage::sql_critical_error(scope) {
        return Err(anyhow!(error));
    }
    result?;
    let write_position = match (before, storage::write_position(scope)) {
        (Some(before), Some(after)) if after > before => Some(after),
        _ => None,
    };
    // A handler that did not write has nothing to hold its frames behind, so
    // send them here rather than shipping them to the actor and back. That
    // detour is not free: it races the socket's own teardown, and a frame that
    // arrives after `ws_unregister` is discarded -- the handler's send silently
    // never happens. Only a write's frames take the barrier path.
    if write_position.is_none() {
        ws_emit_batch(frames);
        return Ok(WsDispatch {
            frames: Vec::new(),
            write_position,
        });
    }
    Ok(WsDispatch {
        frames,
        write_position,
    })
}

fn call_dispatch_ws_open(
    tc: &mut v8::PinScope,
    scope: &str,
    ws_id: u64,
    protocol: &str,
    rx: &std::sync::mpsc::Receiver<CellJob>,
) -> Result<()> {
    let context = tc.get_current_context();
    let global = context.global(tc);
    let key = v8::String::new(tc, "__wsOpen").unwrap();
    let f: v8::Local<v8::Function> = global
        .get(tc, key.into())
        .ok_or_else(|| anyhow!("no __wsOpen"))?
        .try_into()
        .map_err(|_| anyhow!("not fn"))?;
    let sc = v8::String::new(tc, scope).unwrap();
    let id = v8::Number::new(tc, ws_id as f64);
    let protocol = v8::String::new(tc, protocol).unwrap();
    let recv = v8::undefined(tc).into();
    begin_event_context(tc)?;
    let Some(ret) = f.call(tc, recv, &[sc.into(), id.into(), protocol.into()]) else {
        let _ = end_event_context(tc);
        return Err(anyhow!("wsOpen threw"));
    };
    run_event_value(tc, ret, Some(rx), Some(scope), None)?;
    Ok(())
}

fn call_dispatch_ws_closed(
    tc: &mut v8::PinScope,
    scope: &str,
    ws_id: u64,
    code: u16,
    reason: &str,
    was_clean: bool,
    rx: &std::sync::mpsc::Receiver<CellJob>,
) -> Result<()> {
    let context = tc.get_current_context();
    let global = context.global(tc);
    let key = v8::String::new(tc, "__wsClosed").unwrap();
    let f: v8::Local<v8::Function> = global
        .get(tc, key.into())
        .ok_or_else(|| anyhow!("no __wsClosed"))?
        .try_into()
        .map_err(|_| anyhow!("not fn"))?;
    let sc = v8::String::new(tc, scope).unwrap();
    let id = v8::Number::new(tc, ws_id as f64);
    let code = v8::Number::new(tc, f64::from(code));
    let reason = v8::String::new(tc, reason).unwrap();
    let was_clean = v8::Boolean::new(tc, was_clean);
    let recv = v8::undefined(tc).into();
    begin_event_context(tc)?;
    let Some(ret) = f.call(
        tc,
        recv,
        &[
            sc.into(),
            id.into(),
            code.into(),
            reason.into(),
            was_clean.into(),
        ],
    ) else {
        let _ = end_event_context(tc);
        return Err(anyhow!("wsClosed threw"));
    };
    let result = run_event_value(tc, ret, Some(rx), Some(scope), None);
    if let Some(error) = storage::sql_critical_error(scope) {
        return Err(anyhow!(error));
    }
    result?;
    Ok(())
}

/// The rejection reason of a settled-rejected promise, with a little stack.
fn reject_reason(tc: &mut v8::PinScope, p: v8::Local<v8::Promise>) -> String {
    let r = p.result(tc);
    let msg = r.to_rust_string_lossy(tc);
    let stk = r
        .to_object(tc)
        .and_then(|o| {
            let k = v8::String::new(tc, "stack")?;
            o.get(tc, k.into())
        })
        .map(|s| s.to_rust_string_lossy(tc))
        .unwrap_or_default();
    let tail = stk
        .lines()
        .skip(1)
        .take(3)
        .map(|l| l.trim())
        .collect::<Vec<_>>()
        .join(" <- ");
    if tail.is_empty() {
        msg
    } else {
        format!("{msg} [{tail}]")
    }
}

pub struct Engine;
impl Engine {
    pub fn init() {
        // No `v8::icu::set_common_data_78` call: the rusty_v8 152 prebuilt
        // statically links the complete ICU data (full locale tables and
        // regex property-of-strings, e.g. /\p{RGI_Emoji}/v — verified
        // empirically), so overriding it with an embedded icudtl.dat only
        // duplicated 10.8 MB in the binary. A test pins that coverage, so a
        // future v8 bump that reduces the builtin data fails rather than
        // silently narrowing it; the fix would be to re-embed rusty_v8's
        // `third_party/icu/common/icudtl.dat`, 16-byte-aligned.
        let platform = v8::new_default_platform(0, false).make_shared();
        v8::V8::initialize_platform(platform);
        v8::V8::initialize();
    }
}

/// Per-worker compatibility switches, derived from the manifest's
/// compatibility date and flags (Workerd compatibility-date.capnp). `Default`
/// is every switch off; production derives real values in `main`.
#[derive(Clone, Copy, Default)]
pub struct Compat {
    pub delete_all_deletes_alarm: bool,
    /// `js_rpc`: RPC on a Durable Object class that does not extend
    /// `DurableObject` (Workerd worker-rpc.c++ getTargetInfo()).
    pub js_rpc: bool,
    /// `fetcher_has_get_put_delete` (off = `fetcher_no_get_put_delete`,
    /// default on for dates >= 2024-03-26): the deprecated `get()`/`put()`/
    /// `delete()` HTTP helpers on stubs (Workerd http.c++ Fetcher).
    pub fetcher_get_put_delete: bool,
    /// `websocket_standard_binary_type`: `binaryType` defaults to `"blob"` and
    /// a binary message arrives as a `Blob`, per the WHATWG default. Without
    /// it celld keeps the historical `"arraybuffer"`.
    pub websocket_standard_binary_type: bool,
}

pub struct WorkerConfig {
    src: String,
    script_name: String,
    do_classes: Vec<String>,
    bindings: Vec<(String, String)>,
    r2_bindings: Vec<String>,
    ai_binding: Option<String>,
    vars: Vec<(String, String)>,
    node: String,
    text: Vec<(String, String)>,
    compat: Compat,
    /// `[[services]]`: (binding name, target script, optional entrypoint).
    /// The target runs in this process; see [[service-bindings]].
    services: Vec<(String, String, Option<String>)>,
    asset_binding: Option<String>,
    /// `env` name of the Worker Loader binding, if this Worker may spawn
    /// dynamic isolates.
    loader_binding: Option<String>,
    /// Ambient outbound authority. Loaded workers may be denied.
    egress: EgressPolicy,
    /// Extra `env` values a loaded worker was handed, as a JSON object string
    /// merged onto its `env`. Loader-only; empty for normal workers.
    loader_env: Option<String>,
    /// A loaded worker's non-main modules as (name, source), so the main module
    /// can import siblings. Loader-only; empty for normal single-file bundles.
    loader_modules: Vec<(String, String)>,
}

pub struct WorkerConfigOptions {
    pub src: String,
    pub script_name: String,
    pub do_classes: Vec<String>,
    pub bindings: Vec<(String, String)>,
    pub r2_bindings: Vec<String>,
    pub ai_binding: Option<String>,
    pub vars: Vec<(String, String)>,
    pub node: String,
    pub text: Vec<(String, String)>,
    pub compat: Compat,
}

impl WorkerConfig {
    pub fn new(options: WorkerConfigOptions) -> Self {
        let WorkerConfigOptions {
            src,
            script_name,
            do_classes,
            bindings,
            r2_bindings,
            ai_binding,
            vars,
            node,
            text,
            compat,
        } = options;
        Self {
            src,
            script_name,
            do_classes,
            bindings,
            r2_bindings,
            ai_binding,
            vars,
            node,
            text,
            compat,
            services: Vec::new(),
            asset_binding: None,
            loader_binding: None,
            egress: EgressPolicy::Allow,
            loader_env: None,
            loader_modules: Vec::new(),
        }
    }

    /// Grant this Worker a Worker Loader binding at `env` name `binding`.
    pub fn with_loader(mut self, binding: Option<String>) -> Self {
        self.loader_binding = binding;
        self
    }

    /// Set this Worker's ambient outbound authority (loaded workers only).
    fn with_egress(mut self, egress: EgressPolicy) -> Self {
        self.egress = egress;
        self
    }

    /// Merge `env` (a JSON object string) onto a loaded worker's `env`.
    fn with_loader_env(mut self, env: Option<String>) -> Self {
        self.loader_env = env;
        self
    }

    /// Non-main modules a loaded worker's main module may import.
    fn with_loader_modules(mut self, modules: Vec<(String, String)>) -> Self {
        self.loader_modules = modules;
        self
    }

    /// Declare the service bindings this Worker may call.
    pub fn with_services(mut self, services: Vec<(String, String, Option<String>)>) -> Self {
        self.services = services;
        self
    }

    pub fn with_asset_binding(mut self, binding: Option<String>) -> Self {
        self.asset_binding = binding;
        self
    }
}

pub struct Worker {
    inner: Option<WorkerIsolate>,
    config: Arc<WorkerConfig>,
    owned: Arc<[String]>,
}

pub struct WorkerIsolate {
    isolate: v8::OwnedIsolate,
    context: v8::Global<v8::Context>,
    fetch: v8::Global<v8::Function>,
    original_heap_limit: usize,
    runtime_state: Arc<ActorRuntimeState>,
}

impl std::ops::Deref for Worker {
    type Target = WorkerIsolate;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().expect("Worker isolate unavailable")
    }
}

impl std::ops::DerefMut for Worker {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut().expect("Worker isolate unavailable")
    }
}

const DEFAULT_V8_HEAP_LIMIT_BYTES: usize = 128 * 1024 * 1024;
const V8_HEAP_EMERGENCY_BYTES: usize = 16 * 1024 * 1024;

#[derive(Default)]
struct HeapLimitState {
    excessively_exceeded: AtomicBool,
}

type SerializedPut = (String, Vec<u8>);
type PendingPuts = HashMap<String, Vec<SerializedPut>>;

/// Ambient outbound authority for an isolate. Normal workers keep `Allow`; a
/// Worker Loader can hand a loaded worker `Deny` (globalOutbound: null) so its
/// global `fetch()` throws and it must reach the world through `env`
/// capabilities.
#[derive(Clone, Copy, Default, PartialEq)]
enum EgressPolicy {
    #[default]
    Allow,
    Deny,
}

#[derive(Default)]
struct ActorRuntimeState {
    termination: std::sync::Mutex<Option<ExecutionTermination>>,
    pending_puts: std::sync::Mutex<PendingPuts>,
    worker_exit_requested: AtomicBool,
    egress: EgressPolicy,
}

struct ExecutionTermination {
    error: String,
    actor_scope: Option<String>,
}

fn finish_terminated_actor_event(scope: &mut v8::PinScope, actor_scope: &str) {
    let global = scope.get_current_context().global(scope);
    let key = v8::String::new(scope, "__endTerminatedActorEvent").unwrap();
    let Some(value) = global.get(scope, key.into()) else {
        return;
    };
    let Ok(function) = v8::Local::<v8::Function>::try_from(value) else {
        return;
    };
    let actor_scope = v8::String::new(scope, actor_scope).unwrap();
    let recv = v8::undefined(scope).into();
    let _ = function.call(scope, recv, &[actor_scope.into()]);
}

fn take_execution_termination(scope: &mut v8::PinScope) -> Option<anyhow::Error> {
    let state = scope.get_slot::<Arc<ActorRuntimeState>>().cloned();
    let termination = state.and_then(|state| state.termination.lock().ok()?.take());
    let is_terminating = scope.is_execution_terminating();
    if termination.is_some() || is_terminating {
        scope.cancel_terminate_execution();
    }
    if let Some(termination) = termination {
        if let Some(actor_scope) = termination.actor_scope {
            finish_terminated_actor_event(scope, &actor_scope);
        }
        return Some(anyhow!(termination.error));
    }
    is_terminating.then(|| anyhow!("JavaScript execution was terminated"))
}

extern "C" fn near_heap_limit(
    data: *mut std::ffi::c_void,
    current_heap_limit: usize,
    _initial_heap_limit: usize,
) -> usize {
    // SAFETY: `data` points into the Arc stored in the isolate slot. Worker::drop
    // removes this callback before the isolate (and its slots) are destroyed.
    let state = unsafe { &*(data as *const HeapLimitState) };
    state.excessively_exceeded.store(true, Ordering::Relaxed);
    // V8 fatally aborts the process if a near-limit callback does not extend
    // the limit. This reserve lets JS observe condemnation and unwind.
    current_heap_limit.saturating_add(V8_HEAP_EMERGENCY_BYTES)
}

fn v8_heap_limit_bytes() -> usize {
    std::env::var("CELLD_V8_HEAP_LIMIT_MB")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .and_then(|megabytes| megabytes.checked_mul(1024 * 1024))
        .filter(|bytes| *bytes > 0)
        .unwrap_or(DEFAULT_V8_HEAP_LIMIT_BYTES)
}

impl Drop for WorkerIsolate {
    fn drop(&mut self) {
        self.isolate
            .remove_near_heap_limit_callback(near_heap_limit, self.original_heap_limit);
    }
}

#[derive(Debug)]
pub struct AlarmRunError {
    source: anyhow::Error,
    counts_against_limit: bool,
}

impl AlarmRunError {
    fn infrastructure(source: anyhow::Error) -> Self {
        Self {
            source,
            counts_against_limit: false,
        }
    }

    fn user(source: anyhow::Error) -> Self {
        Self {
            source,
            counts_against_limit: true,
        }
    }

    pub fn counts_against_limit(&self) -> bool {
        self.counts_against_limit
    }
}

impl std::fmt::Display for AlarmRunError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(formatter)
    }
}

impl std::error::Error for AlarmRunError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.source()
    }
}

impl Worker {
    /// Compile the worker module, wire the DO harness, and extract the entry
    /// `fetch`. `do_classes` come from the manifest; `bindings` maps a binding
    /// name to a DO class name (from wrangler metadata).
    ///
    /// The runtime builds a `WorkerConfig` directly; this helper is
    /// test-only.
    #[cfg(test)]
    pub fn load(options: WorkerConfigOptions, owned: &[String]) -> Result<Worker> {
        let config = Arc::new(WorkerConfig::new(options));
        Self::load_config(config, owned)
    }

    pub fn load_config(config: Arc<WorkerConfig>, owned: &[String]) -> Result<Worker> {
        Self::load_config_owned(config, Arc::from(owned.to_vec()))
    }

    fn load_config_owned(config: Arc<WorkerConfig>, owned: Arc<[String]>) -> Result<Worker> {
        let src = config.src.as_str();
        let script_name = config.script_name.as_str();
        let do_classes = config.do_classes.as_slice();
        let node = config.node.as_str();
        let text = config.text.as_slice();
        let compat = config.compat;
        let params = v8::CreateParams::default().heap_limits(0, v8_heap_limit_bytes());
        let mut isolate = v8::Isolate::new(params);
        // Dynamic `import()` of builtin specifiers; per-import()-call only.
        isolate.set_host_import_module_dynamically_callback(host_import_module_dynamically);
        let original_heap_limit = isolate.get_heap_statistics().heap_size_limit();
        let heap_limit_state = Arc::new(HeapLimitState::default());
        let runtime_state = Arc::new(ActorRuntimeState {
            egress: config.egress,
            ..Default::default()
        });
        let heap_limit_state_ptr =
            Arc::as_ptr(&heap_limit_state) as *mut HeapLimitState as *mut std::ffi::c_void;
        isolate.set_slot(heap_limit_state);
        isolate.set_slot(runtime_state.clone());
        isolate.add_near_heap_limit_callback(near_heap_limit, heap_limit_state_ptr);
        let (context, fetch) = {
            v8::scope!(let hs, &mut isolate);
            let context = v8::Context::new(hs, Default::default());
            let cs = &mut v8::ContextScope::new(hs, context);
            let tc = std::pin::pin!(v8::TryCatch::new(cs));
            let scope = &mut tc.init();

            install_ops(scope, context);
            install_prelude(scope)?; // Web Platform APIs
            install_harness(scope)?; // DO object model + minimal Response
            install_lazy_globals(scope)?;
            // A global, so it must exist before the module evaluates: bundles
            // read Cloudflare.compatibilityFlags at module scope.
            inject_compatibility_flags(scope, compat)?;
            inject_storage_compatibility(scope, compat)?;

            let module = match compile_module(scope, "worker.js", src) {
                Some(m) => m,
                None => return Err(anyhow!("compile: {}", exc!(scope))),
            };
            register_stubs(scope, src, text); // cloudflare:*/node:* + text modules
            if !config.loader_modules.is_empty() {
                register_loader_modules(scope, &config.loader_modules);
            }
            module
                .instantiate_module(scope, resolve_external)
                .ok_or_else(|| anyhow!("instantiate: {}", exc!(scope)))?;
            let ev = match module.evaluate(scope) {
                Some(value) => value,
                None => {
                    if let Some(error) = take_execution_termination(scope) {
                        return Err(error);
                    }
                    return Err(anyhow!("evaluate: {}", exc!(scope)));
                }
            };
            if let Some(error) = take_execution_termination(scope) {
                return Err(error);
            }
            if let Ok(p) = ev.try_cast::<v8::Promise>() {
                if p.state() == v8::PromiseState::Rejected {
                    let r = p.result(scope);
                    let stk = r
                        .to_object(scope)
                        .and_then(|o| {
                            let k = v8::String::new(scope, "stack")?;
                            o.get(scope, k.into())
                        })
                        .map(|s| s.to_rust_string_lossy(scope))
                        .unwrap_or_default();
                    return Err(anyhow!(
                        "top-level rejected: {} | {}",
                        r.to_rust_string_lossy(scope),
                        stk.lines().take(4).collect::<Vec<_>>().join(" <- ")
                    ));
                }
            }

            let ns = module
                .get_module_namespace()
                .to_object(scope)
                .ok_or_else(|| anyhow!("ns"))?;

            // register each exported DO class into the harness registry
            for cn in do_classes {
                let key = v8::String::new(scope, cn).unwrap();
                let cls = ns
                    .get(scope, key.into())
                    .ok_or_else(|| anyhow!("DO class {cn} not exported"))?;
                register_class(scope, cn, cls)?;
            }
            inject_namespace_keys(scope, script_name, do_classes)?;
            populate_cf_exports(scope, ns, do_classes)?;
            register_entrypoints(scope, ns)?;
            // build env from bindings and stash it in the harness
            build_env(scope, &config)?;
            // tell the harness which cells are local (route the rest cross-node)
            inject_routing(scope, owned.as_ref(), node)?;

            // entry fetch
            let dk = v8::String::new(scope, "default").unwrap();
            let default = ns
                .get(scope, dk.into())
                .ok_or_else(|| anyhow!("no default export"))?
                .to_object(scope)
                .ok_or_else(|| anyhow!("default not object"))?;
            let fk = v8::String::new(scope, "fetch").unwrap();
            let f: v8::Local<v8::Function> = default
                .get(scope, fk.into())
                .ok_or_else(|| anyhow!("no fetch"))?
                .try_into()
                .map_err(|_| anyhow!("fetch not fn"))?;

            // Lets a self-targeted service binding invoke the handler in
            // this isolate instead of crossing to a pool thread.
            {
                let cell_key = v8::String::new(scope, "__cell").unwrap();
                if let Some(cell) = context
                    .global(scope)
                    .get(scope, cell_key.into())
                    .and_then(|value| value.to_object(scope))
                {
                    let key = v8::String::new(scope, "selfFetch").unwrap();
                    cell.set(scope, key.into(), f.into());
                    // Optional scheduled handler, reached by a self-targeted
                    // service binding's scheduled().
                    let sk = v8::String::new(scope, "scheduled").unwrap();
                    if let Some(handler) = default.get(scope, sk.into()) {
                        if handler.is_function() {
                            let key_ = v8::String::new(scope, "selfScheduled").unwrap();
                            cell.set(scope, key_.into(), handler);
                        }
                    }
                }
            }
            (v8::Global::new(scope, context), v8::Global::new(scope, f))
        };
        Ok(Worker {
            inner: Some(WorkerIsolate {
                isolate,
                context,
                fetch,
                original_heap_limit,
                runtime_state,
            }),
            config,
            owned,
        })
    }

    fn reload_after_process_exit(&mut self) -> Result<()> {
        if !self
            .runtime_state
            .worker_exit_requested
            .load(Ordering::Relaxed)
        {
            return Ok(());
        }
        let config = self.config.clone();
        let owned = self.owned.clone();
        drop(self.inner.take());
        let replacement = Self::load_config_owned(config, owned)
            .map_err(|error| anyhow!("reload after process.exit: {error:#}"))?;
        self.inner = replacement.inner;
        Ok(())
    }

    /// Restore an idFromName() actor's human-readable identity before its
    /// constructor runs. The host calls this on activation and before a named
    /// request is dispatched.
    pub fn set_id_name(&mut self, scope: &str, name: &str) -> Result<()> {
        let context_g = self.context.clone();
        v8::scope!(let hs, &mut self.isolate);
        let context = v8::Local::new(hs, context_g);
        let cs = &mut v8::ContextScope::new(hs, context);
        let tc = std::pin::pin!(v8::TryCatch::new(cs));
        register_actor_name(&mut tc.init(), scope, Some(name))
    }

    /// Fire the DO's alarm() handler for `scope`. Returns Ok on success, Err if
    /// the handler threw (caller retries per the at-least-once contract).
    pub fn fire_alarm(
        &mut self,
        scope: &str,
        retry: i64,
        rx: &std::sync::mpsc::Receiver<CellJob>,
    ) -> std::result::Result<(), AlarmRunError> {
        let scheduled_time = storage::active_alarm_scheduled_time(scope).ok_or_else(|| {
            AlarmRunError::infrastructure(anyhow!(
                "alarm handler started without a scheduled alarm",
            ))
        })?;
        let context_g = self.context.clone();
        v8::scope!(let hs, &mut self.isolate);
        let context = v8::Local::new(hs, context_g);
        let cs = &mut v8::ContextScope::new(hs, context);
        let tc = std::pin::pin!(v8::TryCatch::new(cs));
        let result = call_fire_alarm(&mut tc.init(), scope, scheduled_time, retry, rx);
        if let Some(error) = storage::sql_critical_error(scope) {
            return Err(AlarmRunError::user(anyhow!(error)));
        }
        result
    }

    /// Run a specific DO instance's fetch, addressed by its scope — the target
    /// of a cross-node proxy hop. Bypasses the Worker's default export (which
    /// would re-route). Called on the owner node via the `/__do/<scope>` endpoint.
    pub fn dispatch_to_and_reply(
        &mut self,
        scope: &str,
        request: FetchRequest<'_>,
        rx: &std::sync::mpsc::Receiver<CellJob>,
        reply: tokio::sync::oneshot::Sender<Result<HttpResponse>>,
    ) {
        let context_g = self.context.clone();
        v8::scope!(let hs, &mut self.isolate);
        let context = v8::Local::new(hs, context_g);
        let cs = &mut v8::ContextScope::new(hs, context);
        let tc = std::pin::pin!(v8::TryCatch::new(cs));
        let tc = &mut tc.init();
        let global = context.global(tc);
        let key = v8::String::new(tc, "__dispatchTo").unwrap();
        let f: v8::Local<v8::Function> = match global
            .get(tc, key.into())
            .ok_or_else(|| anyhow!("no __dispatchTo"))
            .and_then(|value| value.try_into().map_err(|_| anyhow!("not fn")))
        {
            Ok(f) => f,
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        };
        let sc = v8::String::new(tc, scope).unwrap();
        let u = v8::String::new(tc, request.url).unwrap();
        let m = v8::String::new(tc, request.method).unwrap();
        let b = bytes_value(tc, request.body.to_vec());
        let headers = match serde_json::to_string(request.headers) {
            Ok(headers) => v8::String::new(tc, &headers).unwrap(),
            Err(error) => {
                let _ = reply.send(Err(error.into()));
                return;
            }
        };
        let request_id_value: v8::Local<v8::Value> = match request.request_id {
            Some(request_id) => v8::String::new(tc, &request_id_string(request_id))
                .unwrap()
                .into(),
            None => v8::null(tc).into(),
        };
        let recv = v8::undefined(tc).into();
        // Sample the committed-write position before the handler runs, so the
        // output gate can tell an app write from celld's own activation writes.
        let writes_before = storage::write_position(scope);
        if let Err(error) = begin_event_context(tc) {
            let _ = reply.send(Err(error));
            return;
        }
        let Some(ret) = f.call(
            tc,
            recv,
            &[
                sc.into(),
                u.into(),
                m.into(),
                b,
                headers.into(),
                request_id_value,
            ],
        ) else {
            let _ = end_event_context(tc);
            let _ = reply.send(Err(anyhow!("dispatchTo threw: {}", exc!(tc))));
            return;
        };
        complete_fetch_event(
            tc,
            ret,
            Some(rx),
            Some(scope),
            writes_before,
            request.request_id,
            reply,
        );
    }

    /// Invoke one public method on a DO instance (Cloudflare native RPC),
    /// with arguments and result in either `RpcData` flavor.
    pub fn dispatch_rpc_data(
        &mut self,
        scope: &str,
        method: &str,
        args: RpcData,
        rx: &std::sync::mpsc::Receiver<CellJob>,
    ) -> Result<RpcData> {
        let context_g = self.context.clone();
        v8::scope!(let hs, &mut self.isolate);
        let context = v8::Local::new(hs, context_g);
        let cs = &mut v8::ContextScope::new(hs, context);
        let tc = std::pin::pin!(v8::TryCatch::new(cs));
        let tc = &mut tc.init();
        let global = context.global(tc);
        let key = v8::String::new(tc, "__dispatchRpc").unwrap();
        let f: v8::Local<v8::Function> = global
            .get(tc, key.into())
            .ok_or_else(|| anyhow!("no __dispatchRpc"))?
            .try_into()
            .map_err(|_| anyhow!("not fn"))?;
        let sc = v8::String::new(tc, scope).unwrap();
        let method = v8::String::new(tc, method).unwrap();
        let args = rpc_data_value(tc, args);
        let recv = v8::undefined(tc).into();
        begin_event_context(tc)?;
        let Some(ret) = f.call(tc, recv, &[sc.into(), method.into(), args]) else {
            let _ = end_event_context(tc);
            return Err(anyhow!("dispatchRpc threw: {}", exc!(tc)));
        };
        let ret = run_event_value(tc, ret, Some(rx), Some(scope), None);
        if let Some(error) = storage::sql_critical_error(scope) {
            return Err(anyhow!(error));
        }
        let ret = ret?;
        Ok(rpc_data_ret(tc, ret))
    }

    /// JSON-flavored `dispatch_rpc_data`, the test harness's entry point.
    #[cfg(test)]
    pub fn dispatch_rpc(
        &mut self,
        scope: &str,
        method: &str,
        args: &str,
        rx: &std::sync::mpsc::Receiver<CellJob>,
    ) -> Result<String> {
        let args = RpcData::Json(args.into());
        match self.dispatch_rpc_data(scope, method, args, rx)? {
            RpcData::Json(json) => Ok(json),
            RpcData::V8(_) => Err(anyhow!("JSON RPC answered bytes")),
        }
    }

    /// Invoke `method` on the named `WorkerEntrypoint`. The service-binding
    /// counterpart of `dispatch_rpc_data`: no actor scope, so no cell
    /// receiver and no SQL critical-error check. Arguments and result are V8
    /// structured-clone bytes.
    pub fn dispatch_entrypoint_rpc(
        &mut self,
        entrypoint: &str,
        method: &str,
        args: Vec<u8>,
    ) -> Result<Vec<u8>> {
        let context_g = self.context.clone();
        v8::scope!(let hs, &mut self.isolate);
        let context = v8::Local::new(hs, context_g);
        let cs = &mut v8::ContextScope::new(hs, context);
        let tc = std::pin::pin!(v8::TryCatch::new(cs));
        let tc = &mut tc.init();
        let global = context.global(tc);
        let key = v8::String::new(tc, "__dispatchEntrypointRpc").unwrap();
        let f: v8::Local<v8::Function> = global
            .get(tc, key.into())
            .ok_or_else(|| anyhow!("no __dispatchEntrypointRpc"))?
            .try_into()
            .map_err(|_| anyhow!("not fn"))?;
        let entrypoint = v8::String::new(tc, entrypoint).unwrap();
        let method = v8::String::new(tc, method).unwrap();
        let args = bytes_value(tc, args);
        let recv = v8::undefined(tc).into();
        begin_event_context(tc)?;
        let Some(ret) = f.call(tc, recv, &[entrypoint.into(), method.into(), args]) else {
            let _ = end_event_context(tc);
            return Err(anyhow!("entrypoint RPC threw: {}", exc!(tc)));
        };
        let ret = run_event_value(tc, ret, None, None, None)?;
        view_bytes(ret).ok_or_else(|| anyhow!("entrypoint RPC answered non-bytes"))
    }

    pub fn dispatch_ws_open(
        &mut self,
        scope: &str,
        ws_id: u64,
        protocol: &str,
        rx: &std::sync::mpsc::Receiver<CellJob>,
    ) -> Result<()> {
        let context_g = self.context.clone();
        v8::scope!(let hs, &mut self.isolate);
        let context = v8::Local::new(hs, context_g);
        let cs = &mut v8::ContextScope::new(hs, context);
        let tc = std::pin::pin!(v8::TryCatch::new(cs));
        let tc = &mut tc.init();
        call_dispatch_ws_open(tc, scope, ws_id, protocol, rx)
    }

    /// Deliver a WebSocket message to the cell's DO (`webSocketMessage`). The
    /// socket lives in the host; `ws.send` inside the handler routes back out by
    /// wsId. The isolate may have been hibernated between messages — the host
    /// re-activates before calling this.
    pub fn dispatch_ws(
        &mut self,
        scope: &str,
        ws_id: u64,
        data: WsIn,
        rx: &std::sync::mpsc::Receiver<CellJob>,
    ) -> Result<WsDispatch> {
        let context_g = self.context.clone();
        v8::scope!(let hs, &mut self.isolate);
        let context = v8::Local::new(hs, context_g);
        let cs = &mut v8::ContextScope::new(hs, context);
        let tc = std::pin::pin!(v8::TryCatch::new(cs));
        let tc = &mut tc.init();
        call_dispatch_ws(tc, scope, ws_id, data, rx)
    }

    pub fn dispatch_ws_closed(
        &mut self,
        scope: &str,
        ws_id: u64,
        code: u16,
        reason: &str,
        was_clean: bool,
        rx: &std::sync::mpsc::Receiver<CellJob>,
    ) -> Result<()> {
        let context_g = self.context.clone();
        v8::scope!(let hs, &mut self.isolate);
        let context = v8::Local::new(hs, context_g);
        let cs = &mut v8::ContextScope::new(hs, context);
        let tc = std::pin::pin!(v8::TryCatch::new(cs));
        let tc = &mut tc.init();
        let global = context.global(tc);
        let key = v8::String::new(tc, "__wsClosed").unwrap();
        let f: v8::Local<v8::Function> = global
            .get(tc, key.into())
            .ok_or_else(|| anyhow!("no __wsClosed"))?
            .try_into()
            .map_err(|_| anyhow!("not fn"))?;
        let sc = v8::String::new(tc, scope).unwrap();
        let id = v8::Number::new(tc, ws_id as f64);
        let code = v8::Number::new(tc, f64::from(code));
        let reason = v8::String::new(tc, reason).unwrap();
        let was_clean = v8::Boolean::new(tc, was_clean);
        let recv = v8::undefined(tc).into();
        begin_event_context(tc)?;
        let Some(ret) = f.call(
            tc,
            recv,
            &[
                sc.into(),
                id.into(),
                code.into(),
                reason.into(),
                was_clean.into(),
            ],
        ) else {
            let _ = end_event_context(tc);
            return Err(anyhow!("wsClosed threw: {}", exc!(tc)));
        };
        let result = run_event_value(tc, ret, Some(rx), Some(scope), None);
        if let Some(error) = storage::sql_critical_error(scope) {
            return Err(anyhow!(error));
        }
        result?;
        Ok(())
    }

    /// Run the worker `fetch` and reply on the oneshot. Test-only: it drives
    /// one request at a time and registers no signal, so the shipped binary
    /// never needs it.
    #[cfg(all(test, celld_internal_tests))]
    pub fn fetch_and_reply(
        &mut self,
        url: &str,
        method: &str,
        body: &[u8],
        headers: &[(String, String)],
        reply: tokio::sync::oneshot::Sender<Result<HttpResponse>>,
    ) -> Result<()> {
        self.fetch_and_reply_id(url, method, body, headers, None, reply)
    }

    /// As `fetch_and_reply`, but the request's signal is registered under
    /// `request_id` so an HTTP or service-binding caller that disconnects
    /// reaches the target's `request.signal` mid-flight.
    pub fn fetch_and_reply_id(
        &mut self,
        url: &str,
        method: &str,
        body: &[u8],
        headers: &[(String, String)],
        request_id: Option<RequestId>,
        reply: tokio::sync::oneshot::Sender<Result<HttpResponse>>,
    ) -> Result<()> {
        self.fetch_and_reply_inner(url, method, body, headers, request_id, reply);
        self.reload_after_process_exit()
    }

    fn fetch_and_reply_inner(
        &mut self,
        url: &str,
        method: &str,
        body: &[u8],
        headers: &[(String, String)],
        request_id: Option<RequestId>,
        reply: tokio::sync::oneshot::Sender<Result<HttpResponse>>,
    ) {
        let context_g = self.context.clone();
        let fetch = self.fetch.clone();
        v8::scope!(let hs, &mut self.isolate);
        let context = v8::Local::new(hs, context_g);
        let cs = &mut v8::ContextScope::new(hs, context);
        let tc = std::pin::pin!(v8::TryCatch::new(cs));
        let tc = &mut tc.init();

        let built = match request_id {
            Some(_) => make_incoming_request(tc, url, method, body, headers),
            None => make_request(tc, url, method, body, headers),
        };
        let req = match built {
            Ok(req) => req,
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        };
        // env comes from the harness (globalThis.__cell.env)
        let env = match harness_env(tc) {
            Ok(env) => env,
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        };
        let recv = v8::undefined(tc).into();
        let f = v8::Local::new(tc, fetch);
        let execution_ctx = match begin_event_context(tc) {
            Ok(execution_ctx) => execution_ctx,
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        };
        let Some(ret) = f.call(tc, recv, &[req, env, execution_ctx]) else {
            let error = take_execution_termination(tc)
                .unwrap_or_else(|| anyhow!("fetch threw: {}", exc!(tc)));
            let _ = end_event_context(tc);
            let _ = reply.send(Err(error));
            return;
        };
        let active_request_id = match request_id {
            Some(request_id)
                if ret
                    .try_cast::<v8::Promise>()
                    .is_ok_and(|promise| promise.state() == v8::PromiseState::Pending) =>
            {
                if let Err(error) = register_incoming_request(tc, request_id, req) {
                    clear_request_cancellation(request_id);
                    let _ = end_event_context(tc);
                    let _ = reply.send(Err(error));
                    return;
                }
                Some(request_id)
            }
            Some(request_id) => {
                clear_request_cancellation(request_id);
                None
            }
            None => None,
        };
        complete_fetch_event(tc, ret, None, None, None, active_request_id, reply);
        if let Some(request_id) = active_request_id {
            finish_incoming_request(tc, request_id);
        }
    }

    /// Run the worker `fetch` in a CELL isolate, pumping this cell's job
    /// receiver so an in-isolate `env.NS.get(ownScope).fetch()` resolves without
    /// a host hop. Identical to `fetch_and_reply_id` except the event loop is
    /// driven with `Some(rx)`.
    pub fn worker_fetch_and_reply(
        &mut self,
        request: FetchRequest<'_>,
        rx: &std::sync::mpsc::Receiver<CellJob>,
        reply: tokio::sync::oneshot::Sender<Result<HttpResponse>>,
    ) -> Result<()> {
        self.worker_fetch_and_reply_inner(request, rx, reply);
        self.reload_after_process_exit()
    }

    fn worker_fetch_and_reply_inner(
        &mut self,
        request: FetchRequest<'_>,
        rx: &std::sync::mpsc::Receiver<CellJob>,
        reply: tokio::sync::oneshot::Sender<Result<HttpResponse>>,
    ) {
        let context_g = self.context.clone();
        let fetch = self.fetch.clone();
        v8::scope!(let hs, &mut self.isolate);
        let context = v8::Local::new(hs, context_g);
        let cs = &mut v8::ContextScope::new(hs, context);
        let tc = std::pin::pin!(v8::TryCatch::new(cs));
        let tc = &mut tc.init();
        let built = match request.request_id {
            Some(_) => make_incoming_request(
                tc,
                request.url,
                request.method,
                request.body,
                request.headers,
            ),
            None => make_request(
                tc,
                request.url,
                request.method,
                request.body,
                request.headers,
            ),
        };
        let req = match built {
            Ok(req) => req,
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        };
        let env = match harness_env(tc) {
            Ok(env) => env,
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        };
        let recv = v8::undefined(tc).into();
        let f = v8::Local::new(tc, fetch);
        let execution_ctx = match begin_event_context(tc) {
            Ok(execution_ctx) => execution_ctx,
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        };
        let Some(ret) = f.call(tc, recv, &[req, env, execution_ctx]) else {
            let error = take_execution_termination(tc)
                .unwrap_or_else(|| anyhow!("fetch threw: {}", exc!(tc)));
            let _ = end_event_context(tc);
            let _ = reply.send(Err(error));
            return;
        };
        let active_request_id = match request.request_id {
            Some(request_id)
                if ret
                    .try_cast::<v8::Promise>()
                    .is_ok_and(|promise| promise.state() == v8::PromiseState::Pending) =>
            {
                if let Err(error) = register_incoming_request(tc, request_id, req) {
                    clear_request_cancellation(request_id);
                    let _ = end_event_context(tc);
                    let _ = reply.send(Err(error));
                    return;
                }
                Some(request_id)
            }
            Some(request_id) => {
                clear_request_cancellation(request_id);
                None
            }
            None => None,
        };
        complete_fetch_event(tc, ret, Some(rx), None, None, active_request_id, reply);
        if let Some(request_id) = active_request_id {
            finish_incoming_request(tc, request_id);
        }
    }
}

// ---- native ops exposed to JS ----

/// Host ops, defined non-enumerable. They are runtime internals: a bundle
/// walking `globalThis` must not find them, let alone `new` one — `for (const
/// k in globalThis) new globalThis[k]()` used to reach `__actor_abort` and
/// kill the actor.
macro_rules! ops {
    ($scope:expr, $global:expr, $($name:literal => $op:path),* $(,)?) => {
        $({
            let f = v8::Function::new($scope, $op).unwrap();
            let k = v8::String::new($scope, $name).unwrap();
            $global.define_own_property(
                $scope, k.into(), f.into(), v8::PropertyAttribute::DONT_ENUM);
        })*
    };
}

fn install_ops(scope: &mut v8::PinScope, context: v8::Local<v8::Context>) {
    let global = context.global(scope);
    ops! { scope, global,
        "__heap_limit_excessively_exceeded" =>
            op_heap_limit_excessively_exceeded,
        "__ws_send" => op_ws_send,
        "__ws_send_binary" => op_ws_send_binary,
        "__ws_close" => op_ws_close,
        "__ws_alloc" => op_ws_alloc,
        "__ws_accept" => op_ws_accept,
        "__ws_accept_regular" => op_ws_accept_regular,
        "__ws_list" => op_ws_list,
        "__ws_attachment_set" => op_ws_attachment_set,
        "__ws_connect" => op_ws_connect,
        "__ws_next" => op_ws_next,
        "__ws_upgrade" => op_ws_upgrade,
        "__storage_get" => op_storage_get,
        "__storage_get_many" => op_storage_get_many,
        "__sql_exec" => op_sql_exec,
        "__sql_ingest" => op_sql_ingest,
        "__sql_cursor_start" => op_sql_cursor_start,
        "__sql_cursor_next" => op_sql_cursor_next,
        "__sql_cursor_close" => op_sql_cursor_close,
        "__sql_database_size" => op_sql_database_size,
        "__storage_transaction_control" => op_storage_transaction_control,
        "__log" => op_log,
        "__storage_put" => op_storage_put,
        "__storage_put_many" => op_storage_put_many,
        "__storage_queue_put" => op_storage_queue_put,
        "__storage_queue_put_many" => op_storage_queue_put_many,
        "__storage_put_serialized" => op_storage_put_serialized,
        "__storage_queue_put_serialized" => op_storage_queue_put_serialized,
        "__storage_flush_pending_puts" => op_storage_flush_pending_puts,
        "__storage_cancel_pending_puts" => op_storage_cancel_pending_puts,
        "__actor_abort" => op_actor_abort,
        "__process_exit" => op_process_exit,
        "__storage_delete" => op_storage_delete,
        "__storage_delete_many" => op_storage_delete_many,
        "__storage_list" => op_storage_list,
        "__storage_sync_list_start" => op_storage_sync_list_start,
        "__storage_sync_list_next" => op_storage_sync_list_next,
        "__storage_delete_all" => op_storage_delete_all,
        "$$urlParse" => op_url_parse,
        "$$urlPatternParse" => op_urlpattern_parse,
        "$$urlPatternMatchInput" => op_urlpattern_match_input,
        "$$atob" => op_atob,
        "$$btoa" => op_btoa,
        "$$textDecoderLabel" => op_text_decoder_label,
        "$$textDecoderNew" => op_text_decoder_new,
        "$$textDecoderDecode" => op_text_decoder_decode,
        "$$textDecoderFree" => op_text_decoder_free,
        "__alarm_set" => op_alarm_set,
        "__alarm_get" => op_alarm_get,
        "__alarm_delete" => op_alarm_delete,
        "__loader_load" => op_loader_load,
        "__loader_fetch" => op_loader_fetch,
        "__loader_rpc" => op_loader_rpc,
        "__loader_drop" => op_loader_drop,
        "__do_call" => op_do_call,
        "__writePosition" => op_write_position,
        "__gateWrite" => op_gate_write,
        "__svc_call" => op_svc_call,
        "__svc_call_cancellable" => op_svc_call_cancellable,
        "__svc_rpc" => op_svc_rpc,
        "__do_call_cancellable" => op_do_call_cancellable,
        "__do_call_cancel" => op_do_call_cancel,
        "__do_id" => op_do_id,
        "__rpc_call" => op_rpc_call,
        "__sc_encode" => op_sc_encode,
        "__sc_decode" => op_sc_decode,
        "__op_fetch" => op_fetch,
        "__asset_fetch" => op_asset_fetch,
        "__http_stream_read" => op_http_stream_read,
        "__http_stream_cancel" => op_http_stream_cancel,
        "__http_stream_tee" => op_http_stream_tee,
        "__response_stream_create" => op_response_stream_create,
        "__response_stream_write" => op_response_stream_write,
        "__response_stream_closed" => op_response_stream_closed,
        "__response_stream_close" => op_response_stream_close,
        "__op_timer" => op_timer,
        "__timer_alloc" => op_timer_alloc,
        "__timer_cancel" => op_timer_cancel,
        "__crypto_operation" => op_crypto_operation,
        "$$randomValues" => op_webcrypto_random,
        "$$digest" => op_webcrypto_digest,
        "$$hmacSign" => op_webcrypto_hmac_sign,
        "$$hmacVerify" => op_webcrypto_hmac_verify,
        "$$aesEncrypt" => op_webcrypto_aes_encrypt,
        "$$aesDecrypt" => op_webcrypto_aes_decrypt,
        "$$pbkdf2" => op_node_pbkdf2,
        "$$hkdf" => op_node_hkdf,
        "__als_get" => op_als_get,
        "__als_set" => op_als_set,
        "__util_type_flags" => op_util_type_flags,
        "__util_constructor_name" => op_util_constructor_name,
        "__util_proxy_details" => op_util_proxy_details,
        "__util_promise_details" => op_util_promise_details,
        "__util_preview_entries" => op_util_preview_entries,
        "__builtin_module" => op_builtin_module,
        "__zlib" => op_zlib,
        "__zlib_stream_new" => op_zlib_stream_new,
        "__zlib_stream_push" => op_zlib_stream_push,
        "__zlib_stream_end" => op_zlib_stream_end,
        "__zlib_stream_drop" => op_zlib_stream_drop,
    }
    #[cfg(all(test, celld_internal_tests))]
    ops! { scope, global,
        "__test_gc" => op_test_gc,
        "__loader_count" => op_loader_count,
        "__test_set_heap_limit_excessively_exceeded" =>
            op_test_set_heap_limit_excessively_exceeded,
        "__sql_set_max_page_count_for_test" =>
            op_sql_set_max_page_count_for_test,
        "__sql_set_write_fault_for_test" => op_sql_set_write_fault_for_test,
        "__sql_set_cache_size_for_test" => op_sql_set_cache_size_for_test,
        "__sql_set_interrupt_fault_for_test" =>
            op_sql_set_interrupt_fault_for_test,
        "__sql_register_nomem_function_for_test" =>
            op_sql_register_nomem_function_for_test,
    }
}

/// Create a JS promise for an async op `id` and return it to JS.
fn promise_for<'s>(scope: &mut v8::PinScope<'s, '_>, id: u64) -> v8::Local<'s, v8::Value> {
    let resolver = v8::PromiseResolver::new(scope).unwrap();
    let promise = resolver.get_promise(scope);
    promise_store(id, v8::Global::new(scope, resolver));
    promise.into()
}

/// Async cross-node dispatch: hand the fetch to the tokio proxy task and await
/// its reply off-thread — the JS thread is never blocked. Resolves to a JSON
/// `{status, body, headers}` string the harness turns back into a Response.
/// `__svc_rpc(script, entrypoint, method, argsSc)` -> Promise<Uint8Array>;
/// arguments and result are V8 structured-clone bytes.
fn op_svc_rpc(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let script = args.get(0).to_rust_string_lossy(scope);
    let entrypoint = args.get(1).to_rust_string_lossy(scope);
    let method = args.get(2).to_rust_string_lossy(scope);
    let call_args = view_bytes(args.get(3)).unwrap_or_default();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let sent = SVC_RPC_TX
        .get()
        .map(|t| {
            t.send(SvcRpcReq {
                script,
                entrypoint,
                method,
                args: call_args,
                reply: tx,
            })
            .is_ok()
        })
        .unwrap_or(false);
    let id = asyncrt::enqueue(async move {
        if !sent {
            return Err("no service binding channel".into());
        }
        match rx.await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(error)) => Err(format!("{error}")),
            Err(error) => Err(format!("service dropped: {error}")),
        }
    });
    rv.set(promise_for(scope, id));
}

/// `__svc_call(script, url, method, body, headersJson)` -> Promise<json>.
/// The service-binding equivalent of `__do_call`: no scope to resolve and no
/// cancellation token, just a handoff to the target script's pool.
fn op_svc_call(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    op_svc_call_impl(scope, args, &mut rv, false);
}

fn op_svc_call_cancellable(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    op_svc_call_impl(scope, args, &mut rv, true);
}

fn op_svc_call_impl(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    rv: &mut v8::ReturnValue<v8::Value>,
    cancellable: bool,
) {
    let script = args.get(0).to_rust_string_lossy(scope);
    let url = args.get(1).to_rust_string_lossy(scope);
    let method = args.get(2).to_rust_string_lossy(scope);
    let body = view_bytes(args.get(3)).unwrap_or_default();
    let headers =
        serde_json::from_str(&args.get(4).to_rust_string_lossy(scope)).unwrap_or_default();
    let (tx, rx) = tokio::sync::oneshot::channel();
    // Shares the Durable Object cancel registry, so `__do_call_cancel` works
    // unchanged for a service call.
    let (request_id, cancel, cancel_guard) = if cancellable {
        let request_id = next_do_request_id();
        let (cancel_sender, cancel_receiver) = tokio::sync::oneshot::channel();
        do_call_cancels()
            .lock()
            .unwrap()
            .insert(request_id, cancel_sender);
        (
            Some(request_id),
            Some(cancel_receiver),
            Some(DoCallCancelGuard(request_id)),
        )
    } else {
        (None, None, None)
    };
    let sent = SVC_CALL_TX
        .get()
        .map(|t| {
            t.send(SvcCallReq {
                cancel,
                script,
                url,
                method,
                body,
                headers,
                reply: tx,
            })
            .is_ok()
        })
        .unwrap_or(false);
    let id = asyncrt::enqueue(async move {
        let _cancel_guard = cancel_guard;
        if !sent {
            return Err("no service binding channel".into());
        }
        match rx.await {
            Ok(Ok(response)) => Ok(encode_http_response(response, false)),
            Ok(Err(error)) => Err(format!("{error}")),
            Err(error) => Err(format!("service dropped: {error}")),
        }
    });
    let promise = promise_for(scope, id);
    if let Some(request_id) = request_id {
        if let Some(object) = promise.to_object(scope) {
            let key = v8::String::new(scope, "__celldCancelId").unwrap();
            let value = v8::String::new(scope, &request_id.to_string()).unwrap();
            object.set(scope, key.into(), value.into());
        }
    }
    rv.set(promise);
}

// --- Worker Loader: dynamic isolates for Code Mode ---
// A running Worker spawns a fresh isolate from code it supplies at runtime and
// invokes it. Walking skeleton: `env.LOADER.load(code)` -> stub;
// `stub.getEntrypoint().fetch(req)` runs the loaded worker's default fetch,
// reusing the co-hosted-worker thread model and `__svc_call` response
// marshalling. Capability-scoped env, controlled outbound, named-entrypoint
// RPC, and memoized `get(name, getCode)` are not yet wired.

// Mirror the workerd dynamic-worker limits (worker-loader.c++): 64 MiB total
// module bytes, 1 MiB env. Messages match so the conformance cases pass.
const MAX_DYNAMIC_WORKER_CODE_SIZE: usize = 64 * 1024 * 1024;
const MAX_DYNAMIC_WORKER_ENV_SIZE: usize = 1024 * 1024;

enum LoaderJob {
    Fetch {
        url: String,
        method: String,
        body: Vec<u8>,
        headers: Vec<(String, String)>,
        reply: tokio::sync::oneshot::Sender<Result<HttpResponse>>,
    },
    // Named-entrypoint RPC: args and result are V8 structured-clone bytes,
    // like the service-binding `__svc_rpc` path.
    Rpc {
        entrypoint: String,
        method: String,
        args: Vec<u8>,
        reply: tokio::sync::oneshot::Sender<Result<Vec<u8>>>,
    },
}

type LoaderRegistry = HashMap<u64, std::sync::mpsc::Sender<LoaderJob>>;
static LOADER_REGISTRY: OnceLock<std::sync::Mutex<LoaderRegistry>> = OnceLock::new();
static LOADER_NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn loader_registry() -> &'static std::sync::Mutex<LoaderRegistry> {
    LOADER_REGISTRY.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

fn loader_throw(scope: &mut v8::PinScope, message: &str) {
    let message = v8::String::new(scope, message).unwrap();
    let exception = v8::Exception::error(scope, message);
    scope.throw_exception(exception);
}

/// `__loader_load(codeJson)` -> stub id. Builds a WorkerConfig from the
/// supplied modules, spawns a fresh isolate on its own thread (isolates are
/// pinned per thread), and registers its job channel. The isolate compiles
/// asynchronously; jobs queue until it is ready, and a load failure drains
/// queued jobs with the error rather than dropping their reply channels.
fn op_loader_load(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let code_json = args.get(0).to_rust_string_lossy(scope);
    let code: serde_json::Value = match serde_json::from_str(&code_json) {
        Ok(code) => code,
        Err(e) => return loader_throw(scope, &format!("worker loader: {e}")),
    };
    let Some(main) = code.get("mainModule").and_then(|v| v.as_str()) else {
        return loader_throw(scope, "worker loader: missing mainModule");
    };
    let Some(src) = code
        .get("modules")
        .and_then(|m| m.get(main))
        .and_then(|v| v.as_str())
        .map(str::to_string)
    else {
        return loader_throw(
            scope,
            &format!("worker loader: module {main:?} missing or not a string"),
        );
    };
    // Every module other than the main one is a sibling the main module may
    // import.
    let loader_modules: Vec<(String, String)> = code
        .get("modules")
        .and_then(|m| m.as_object())
        .map(|m| {
            m.iter()
                .filter(|(name, _)| name.as_str() != main)
                .filter_map(|(name, v)| v.as_str().map(|s| (name.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();
    // Total module bytes, checked before compiling anything (the oversized
    // module is never parsed) — the extra modules are not yet loaded but do
    // count against the ceiling, as upstream.
    let code_size: usize = code
        .get("modules")
        .and_then(|m| m.as_object())
        .map(|m| m.values().filter_map(|v| v.as_str()).map(str::len).sum())
        .unwrap_or(0);
    if code_size > MAX_DYNAMIC_WORKER_CODE_SIZE {
        return loader_throw(
            scope,
            &format!(
                "Dynamic Worker code size ({code_size} bytes) exceeds the \
                 maximum allowed size of {MAX_DYNAMIC_WORKER_CODE_SIZE} bytes."
            ),
        );
    }
    // Plain JSON `env` values merge onto the loaded worker's env; capability
    // stubs are not yet supported and would fail to serialize upstream in JS.
    let loader_env = code
        .get("env")
        .filter(|v| !v.is_null())
        .map(|v| v.to_string());
    if let Some(env) = &loader_env {
        if env.len() > MAX_DYNAMIC_WORKER_ENV_SIZE {
            return loader_throw(
                scope,
                &format!(
                    "Dynamic Worker env size ({} bytes) exceeds the maximum \
                     allowed size of {MAX_DYNAMIC_WORKER_ENV_SIZE} bytes.",
                    env.len()
                ),
            );
        }
    }
    // globalOutbound: absent inherits the caller's authority, null denies
    // ambient egress, a Fetcher (broker) is not implemented yet.
    let egress = match code.get("globalOutbound") {
        None => actor_runtime_state(scope).egress,
        Some(v) if v.is_null() => EgressPolicy::Deny,
        Some(_) => {
            return loader_throw(
                scope,
                "worker loader: globalOutbound broker is not implemented yet",
            );
        }
    };
    // Bound live loaded workers so a runaway agent loop cannot exhaust
    // threads and isolates. Evicted workers (dropped stubs) free their slot.
    let max = std::env::var("CELLD_MAX_LOADED_WORKERS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(256);
    if loader_registry().lock().unwrap().len() >= max {
        return loader_throw(
            scope,
            &format!("worker loader: too many loaded workers (limit {max})"),
        );
    }
    // Honor the WorkerCode's declared compatibility (workerd worker_compat
    // reads snake_case keys); Code Mode workers keep RPC on regardless.
    let mut compat = crate::worker_compat(&serde_json::json!({
        "compatibility_date": code.get("compatibilityDate"),
        "compatibility_flags": code.get("compatibilityFlags"),
    }));
    compat.js_rpc = true;
    let id = LOADER_NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let config = Arc::new(
        WorkerConfig::new(WorkerConfigOptions {
            src,
            script_name: format!("__loader:{id}"),
            do_classes: Vec::new(),
            bindings: Vec::new(),
            r2_bindings: Vec::new(),
            ai_binding: None,
            vars: Vec::new(),
            node: String::new(),
            text: Vec::new(),
            compat,
        })
        .with_egress(egress)
        .with_loader_env(loader_env)
        .with_loader_modules(loader_modules),
    );
    let (tx, rx) = std::sync::mpsc::channel::<LoaderJob>();
    loader_registry().lock().unwrap().insert(id, tx);
    std::thread::spawn(move || {
        asyncrt::init();
        let mut w = match Worker::load_config(config, &[]) {
            Ok(w) => w,
            Err(e) => return drain_loader_load_error(rx, &format!("{e}")),
        };
        for job in rx {
            match job {
                LoaderJob::Fetch {
                    url,
                    method,
                    body,
                    headers,
                    reply,
                } => {
                    if w.fetch_and_reply_id(&url, &method, &body, &headers, None, reply)
                        .is_err()
                    {
                        break;
                    }
                }
                LoaderJob::Rpc {
                    entrypoint,
                    method,
                    args,
                    reply,
                } => {
                    let _ = reply.send(w.dispatch_entrypoint_rpc(&entrypoint, &method, args));
                }
            }
        }
    });
    rv.set(v8::Number::new(scope, id as f64).into());
}

/// Reply to every queued job with the load error, so a worker that failed to
/// compile rejects its callers cleanly instead of dropping their channels.
fn drain_loader_load_error(rx: std::sync::mpsc::Receiver<LoaderJob>, error: &str) {
    for job in rx {
        match job {
            LoaderJob::Fetch { reply, .. } => {
                let _ = reply.send(Err(anyhow!("{error}")));
            }
            LoaderJob::Rpc { reply, .. } => {
                let _ = reply.send(Err(anyhow!("{error}")));
            }
        }
    }
}

/// `__loader_fetch(id, url, method, body, headersJson)` -> Promise<json>. The
/// loaded-worker analog of `__svc_call`: encodes the response the same way.
fn op_loader_fetch(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    let url = args.get(1).to_rust_string_lossy(scope);
    let method = args.get(2).to_rust_string_lossy(scope);
    let body = view_bytes(args.get(3)).unwrap_or_default();
    let headers =
        serde_json::from_str(&args.get(4).to_rust_string_lossy(scope)).unwrap_or_default();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let sent = loader_registry()
        .lock()
        .unwrap()
        .get(&id)
        .map(|s| {
            s.send(LoaderJob::Fetch {
                url,
                method,
                body,
                headers,
                reply: tx,
            })
            .is_ok()
        })
        .unwrap_or(false);
    let async_id = asyncrt::enqueue(async move {
        if !sent {
            return Err("worker loader: unknown worker".into());
        }
        match rx.await {
            Ok(Ok(response)) => Ok(encode_http_response(response, false)),
            Ok(Err(error)) => Err(format!("{error}")),
            Err(error) => Err(format!("loaded worker dropped: {error}")),
        }
    });
    rv.set(promise_for(scope, async_id));
}

/// `__loader_rpc(id, entrypoint, method, argsSc)` -> Promise<Uint8Array>. The
/// loaded-worker analog of `__svc_rpc`: a named-entrypoint method call whose
/// args and result are V8 structured-clone bytes.
fn op_loader_rpc(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    let entrypoint = args.get(1).to_rust_string_lossy(scope);
    let method = args.get(2).to_rust_string_lossy(scope);
    let call_args = view_bytes(args.get(3)).unwrap_or_default();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let sent = loader_registry()
        .lock()
        .unwrap()
        .get(&id)
        .map(|s| {
            s.send(LoaderJob::Rpc {
                entrypoint,
                method,
                args: call_args,
                reply: tx,
            })
            .is_ok()
        })
        .unwrap_or(false);
    let async_id = asyncrt::enqueue(async move {
        if !sent {
            return Err("worker loader: unknown worker".into());
        }
        match rx.await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(error)) => Err(format!("{error}")),
            Err(error) => Err(format!("loaded worker dropped: {error}")),
        }
    });
    rv.set(promise_for(scope, async_id));
}

/// `__loader_drop(id)` — evict a loaded worker. Called from a
/// FinalizationRegistry when its stub is GC'd: removing the registry entry
/// drops the job-channel Sender, so the worker thread's receive loop ends and
/// the isolate is dropped on that thread. Without this every load() would leak
/// a thread and isolate.
fn op_loader_drop(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    loader_registry().lock().unwrap().remove(&id);
}

/// `__loader_count()` -> live loaded-worker count, for eviction tests.
#[cfg(all(test, celld_internal_tests))]
fn op_loader_count(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let n = loader_registry().lock().unwrap().len();
    rv.set(v8::Number::new(scope, n as f64).into());
}

/// `__writePosition(scope)` -> the cell's committed-write position (a number),
/// or `null` when the scope has no open connection. The co-hosted DO fast path
/// samples this around an inline `fetch` to tell a write from a read.
fn op_write_position(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    match storage::write_position(&cell) {
        Some(position) => rv.set(v8::Number::new(scope, position as f64).into()),
        None => rv.set(v8::null(scope).into()),
    }
}

/// `__gateWrite(scope, position)` -> a promise that resolves once the cell is
/// durable and rejects if durability cannot be proved. The co-hosted DO fast
/// path awaits this before returning an inline write's response, so the caller
/// never observes a write the node cannot prove durable — the routed output
/// gate, applied where the inline write actually happens.
fn op_gate_write(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let position = args
        .get(1)
        .to_integer(scope)
        .map(|n| n.value() as u64)
        .unwrap_or(0);
    let (tx, receive) = tokio::sync::oneshot::channel();
    let sent = GATE_TX
        .get()
        .map(|gate| {
            gate.send(GateReq {
                scope: cell,
                position,
                reply: tx,
            })
            .is_ok()
        })
        .unwrap_or(false);
    let id = asyncrt::enqueue(async move {
        if !sent {
            return Err("no output-gate channel".into());
        }
        match receive.await {
            Ok(Ok(())) => Ok(String::new()),
            Ok(Err(error)) => Err(format!("{error:?}")),
            Err(_) => Err("output gate dropped".into()),
        }
    });
    rv.set(promise_for(scope, id));
}

fn op_do_call(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    op_do_call_impl(scope, args, &mut rv, false);
}

fn op_do_call_cancellable(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    op_do_call_impl(scope, args, &mut rv, true);
}

fn op_do_call_impl(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    rv: &mut v8::ReturnValue<v8::Value>,
    cancellable: bool,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let name_value = args.get(1);
    let name = (!name_value.is_null_or_undefined()).then(|| name_value.to_rust_string_lossy(scope));
    let url = args.get(2).to_rust_string_lossy(scope);
    let method = args.get(3).to_rust_string_lossy(scope);
    let body = view_bytes(args.get(4)).unwrap_or_default();
    let headers =
        serde_json::from_str(&args.get(5).to_rust_string_lossy(scope)).unwrap_or_default();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let (request_id, cancel, cancel_guard) = if cancellable {
        let request_id = next_do_request_id();
        let (cancel_sender, cancel_receiver) = tokio::sync::oneshot::channel();
        do_call_cancels()
            .lock()
            .unwrap()
            .insert(request_id, cancel_sender);
        (
            Some(request_id),
            Some(cancel_receiver),
            Some(DoCallCancelGuard(request_id)),
        )
    } else {
        (None, None, None)
    };
    let sent = DO_CALL_TX
        .get()
        .map(|t| {
            t.send(DoCallReq {
                request_id,
                cancel,
                scope: cell,
                name,
                url,
                method,
                body,
                headers,
                reply: tx,
            })
            .is_ok()
        })
        .unwrap_or(false);
    let id = asyncrt::enqueue(async move {
        let _cancel_guard = cancel_guard;
        if !sent {
            return Err("no proxy channel".into());
        }
        match rx.await {
            Ok(Ok(response)) => Ok(encode_http_response(response, true)),
            Ok(Err(e)) => Err(format!("{e}")),
            Err(e) => Err(format!("proxy dropped: {e}")),
        }
    });
    let p = promise_for(scope, id);
    if let Some(request_id) = request_id {
        if let Some(promise) = p.to_object(scope) {
            let key = v8::String::new(scope, "__celldCancelId").unwrap();
            let request_id = v8::String::new(scope, &request_id_string(request_id)).unwrap();
            promise.set(scope, key.into(), request_id.into());
        }
    }
    rv.set(p);
}

fn op_do_call_cancel(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let request_id = args.get(0).to_rust_string_lossy(scope);
    let Some(request_id) = parse_request_id(&request_id) else {
        return;
    };
    if let Some(cancel) = do_call_cancels().lock().unwrap().remove(&request_id) {
        let _ = cancel.send(());
    }
}

/// `__rpc_call(scope, name, method, argsSc)` -> Promise<Uint8Array>;
/// arguments and result are V8 structured-clone bytes.
fn op_rpc_call(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let name_value = args.get(1);
    let name = (!name_value.is_null_or_undefined()).then(|| name_value.to_rust_string_lossy(scope));
    let method = args.get(2).to_rust_string_lossy(scope);
    let args = RpcData::V8(view_bytes(args.get(3)).unwrap_or_default());
    let (tx, rx) = tokio::sync::oneshot::channel();
    let sent = RPC_CALL_TX
        .get()
        .map(|t| {
            t.send(RpcCallReq {
                scope: cell,
                name,
                method,
                args,
                reply: tx,
            })
            .is_ok()
        })
        .unwrap_or(false);
    let id = asyncrt::enqueue(async move {
        if !sent {
            return Err("no RPC channel".into());
        }
        match rx.await {
            Ok(Ok(RpcData::V8(bytes))) => Ok(bytes),
            Ok(Ok(RpcData::Json(_))) => Err("RPC answered JSON to a structured-clone call".into()),
            Ok(Err(e)) => Err(format!("{e}")),
            Err(e) => Err(format!("RPC proxy dropped: {e}")),
        }
    });
    let p = promise_for(scope, id);
    rv.set(p);
}

/// Outbound `fetch` — the op behind the harness's `fetch()`. Resolves to a JSON
/// `{status, body, headers}` string. Streaming is a later layer.
fn op_fetch(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    // A loaded worker with globalOutbound: null has no ambient egress: it must
    // reach the world through `env` capabilities. Message matches workerd.
    if actor_runtime_state(scope).egress == EgressPolicy::Deny {
        return loader_throw(
            scope,
            "This worker is not permitted to access the internet via global \
             functions like fetch(). It must use capabilities (such as \
             bindings in 'env') to talk to the outside world.",
        );
    }
    let method = args.get(0).to_rust_string_lossy(scope);
    let url = args.get(1).to_rust_string_lossy(scope);
    let b = args.get(2);
    let body: Option<Vec<u8>> = if b.is_undefined() || b.is_null() {
        None
    } else {
        serde_json::from_str(&b.to_rust_string_lossy(scope)).ok()
    };
    let headers: Vec<(String, String)> =
        serde_json::from_str(&args.get(3).to_rust_string_lossy(scope)).unwrap_or_default();
    let redirect = args.get(4).to_rust_string_lossy(scope);
    let client = if redirect == "manual" {
        HTTP_MANUAL.with(|client| client.clone())
    } else {
        HTTP.with(|client| client.clone())
    };
    let id = asyncrt::enqueue(async move {
        let m = reqwest::Method::from_bytes(method.as_bytes()).unwrap_or(reqwest::Method::GET);
        // a timeout bounds a black-hole host: the future settles (Err) instead
        // of parking run_loop forever — the hang the Drop guard can't catch.
        let fetch_timeout = std::env::var("CELLD_FETCH_TIMEOUT_S")
            .ok()
            .and_then(|value| value.parse().ok())
            .filter(|seconds| *seconds > 0)
            .unwrap_or(120);
        let mut rb = client
            .request(m, &url)
            .timeout(std::time::Duration::from_secs(fetch_timeout));
        for (name, value) in headers {
            rb = rb.header(name, value);
        }
        if let Some(b) = body {
            rb = rb.body(b);
        }
        match rb.send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let headers = resp
                    .headers()
                    .iter()
                    .map(|(name, value)| {
                        (
                            name.as_str().to_string(),
                            value.to_str().unwrap_or_default().to_string(),
                        )
                    })
                    .collect::<Vec<_>>();
                let stream_id = NEXT_HTTP_STREAM_ID.fetch_add(1, Ordering::Relaxed);
                register_http_stream(stream_id, HttpStreamSource::Response(resp));
                Ok(serde_json::json!({
                    "status": status, "streamId": stream_id, "headers": headers,
                })
                .to_string())
            }
            Err(e) => Err(format!("fetch: {e}")),
        }
    });
    let p = promise_for(scope, id);
    rv.set(p);
}

fn op_asset_fetch(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let script = args.get(0).to_rust_string_lossy(scope);
    let method = args.get(1).to_rust_string_lossy(scope);
    let url = args.get(2).to_rust_string_lossy(scope);
    let headers: Vec<(String, String)> =
        serde_json::from_str(&args.get(3).to_rust_string_lossy(scope)).unwrap_or_default();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let sent = ASSET_CALL_TX.get().is_some_and(|sender| {
        sender
            .send(AssetCallReq {
                script,
                url,
                method,
                headers,
                reply: tx,
            })
            .is_ok()
    });
    let id = asyncrt::enqueue(async move {
        if !sent {
            return Err("no asset resolver channel".to_string());
        }
        match rx.await {
            Ok(Ok(response)) => {
                let stream_id = NEXT_HTTP_STREAM_ID.fetch_add(1, Ordering::Relaxed);
                let (body_tx, body_rx) = tokio::sync::mpsc::channel(1);
                if !response.body.is_empty() {
                    let _ = body_tx.send(Ok(response.body)).await;
                }
                drop(body_tx);
                register_http_stream(stream_id, HttpStreamSource::Receiver(body_rx));
                Ok(serde_json::json!({
                    "status": response.status,
                    "streamId": stream_id,
                    "headers": response.headers,
                })
                .to_string())
            }
            Ok(Err(error)) => Err(format!("asset fetch: {error}")),
            Err(error) => Err(format!("asset resolver dropped: {error}")),
        }
    });
    rv.set(promise_for(scope, id));
}

async fn next_http_stream_chunk(source: &mut HttpStreamSource) -> Result<Option<Vec<u8>>, String> {
    match source {
        HttpStreamSource::Response(response) => response
            .chunk()
            .await
            .map(|chunk| chunk.map(|bytes| bytes.to_vec()))
            .map_err(|error| format!("response stream: {error}")),
        HttpStreamSource::Receiver(receiver) => match receiver.recv().await {
            Some(Ok(bytes)) => Ok(Some(bytes)),
            Some(Err(error)) => Err(error),
            None => Ok(None),
        },
        HttpStreamSource::Stream(stream) => stream.next().await.transpose(),
    }
}

/// Move a host response source into a directly-polled stream. No pump task is
/// needed: the eventual HTTP or JS consumer supplies the backpressure.
fn http_chunk_stream(source: HttpStreamSource) -> HttpChunkStream {
    match source {
        HttpStreamSource::Response(response) => Box::pin(response.bytes_stream().map(|chunk| {
            chunk
                .map(|bytes| bytes.to_vec())
                .map_err(|error| format!("response stream: {error}"))
        })),
        HttpStreamSource::Receiver(receiver) => {
            Box::pin(tokio_stream::wrappers::ReceiverStream::new(receiver))
        }
        HttpStreamSource::Stream(stream) => stream,
    }
}

/// Host-native tee for an outbound response. Both branches are represented by
/// stream IDs, so one can be returned through Axum while JS independently
/// scans the other for observability or usage accounting.
fn tee_http_stream(mut source: HttpStreamSource) -> (u64, u64) {
    let (tx1, rx1) = tokio::sync::mpsc::channel(16);
    let (tx2, rx2) = tokio::sync::mpsc::channel(16);
    let id1 = NEXT_HTTP_STREAM_ID.fetch_add(1, Ordering::Relaxed);
    let id2 = NEXT_HTTP_STREAM_ID.fetch_add(1, Ordering::Relaxed);
    register_http_stream(id1, HttpStreamSource::Receiver(rx1));
    register_http_stream(id2, HttpStreamSource::Receiver(rx2));
    asyncrt::op_handle().spawn(async move {
        loop {
            match next_http_stream_chunk(&mut source).await {
                Ok(Some(bytes)) => {
                    let first = tx1.send(Ok(bytes.clone())).await.is_ok();
                    let second = tx2.send(Ok(bytes)).await.is_ok();
                    if !first && !second {
                        break;
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    let _ = tx1.send(Err(error.clone())).await;
                    let _ = tx2.send(Err(error)).await;
                    break;
                }
            }
        }
    });
    (id1, id2)
}

fn op_http_stream_read(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let stream_id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    let source = http_streams()
        .lock()
        .unwrap()
        .get_mut(&stream_id)
        .and_then(|stream| {
            stream
                .source
                .take()
                .map(|source| (source, stream.cancelled.subscribe()))
        });
    let id = asyncrt::enqueue(async move {
        let Some((mut source, mut cancelled)) = source else {
            return Ok(serde_json::json!({ "done": true }).to_string());
        };
        let next = tokio::select! {
            result = next_http_stream_chunk(&mut source) => Some(result),
            _ = cancelled.changed() => None,
        };
        match next {
            None => Ok(serde_json::json!({ "done": true }).to_string()),
            Some(Ok(Some(bytes))) => {
                if let Some(stream) = http_streams().lock().unwrap().get_mut(&stream_id) {
                    stream.created = Instant::now();
                    stream.source = Some(source);
                }
                Ok(serde_json::json!({ "done": false, "bytes": bytes }).to_string())
            }
            Some(Ok(None)) => {
                http_streams().lock().unwrap().remove(&stream_id);
                Ok(serde_json::json!({ "done": true }).to_string())
            }
            Some(Err(error)) => {
                http_streams().lock().unwrap().remove(&stream_id);
                Err(error)
            }
        }
    });
    rv.set(promise_for(scope, id));
}

fn op_http_stream_tee(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let stream_id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    let source = http_streams()
        .lock()
        .unwrap()
        .remove(&stream_id)
        .and_then(|stream| stream.source);
    let Some(source) = source else {
        let message = v8::String::new(scope, "response stream is no longer available").unwrap();
        let exception = v8::Exception::type_error(scope, message);
        scope.throw_exception(exception);
        return;
    };
    let ids = tee_http_stream(source);
    let json = serde_json::to_string(&ids).unwrap();
    rv.set(v8::String::new(scope, &json).unwrap().into());
}

fn op_http_stream_cancel(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let stream_id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    if let Some(stream) = http_streams().lock().unwrap().remove(&stream_id) {
        let _ = stream.cancelled.send(true);
    }
}

fn op_response_stream_create(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let stream_id = NEXT_HTTP_STREAM_ID.fetch_add(1, Ordering::Relaxed);
    let (writer, receiver) = tokio::sync::mpsc::channel(1);
    let (finished, _) = tokio::sync::watch::channel(false);
    let now = Instant::now();
    register_http_stream(stream_id, HttpStreamSource::Receiver(receiver));
    let mut writers = response_stream_writers().lock().unwrap();
    writers.retain(|_, stream| stream.created.elapsed() < Duration::from_secs(60));
    writers.insert(
        stream_id,
        ResponseStreamWriter {
            created: now,
            writer,
            finished,
        },
    );
    rv.set(v8::Number::new(scope, stream_id as f64).into());
}

fn op_response_stream_write(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let stream_id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    let bytes = args
        .get(1)
        .try_cast::<v8::ArrayBufferView>()
        .ok()
        .map(|view| {
            let mut bytes = vec![0; view.byte_length()];
            view.copy_contents(&mut bytes);
            bytes
        });
    let writer = response_stream_writers()
        .lock()
        .unwrap()
        .get_mut(&stream_id)
        .map(|stream| {
            stream.created = Instant::now();
            stream.writer.clone()
        });
    let id = asyncrt::enqueue(async move {
        let Some(bytes) = bytes else {
            return Err("response stream chunks must be ArrayBuffer views".into());
        };
        let Some(writer) = writer else {
            return Err("response stream consumer canceled".into());
        };
        if writer.send(Ok(bytes)).await.is_err() {
            response_stream_writers().lock().unwrap().remove(&stream_id);
            return Err("response stream consumer canceled".into());
        }
        Ok(String::new())
    });
    rv.set(promise_for(scope, id));
}

fn op_response_stream_closed(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let stream_id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    let (writer, mut finished) = response_stream_writers()
        .lock()
        .unwrap()
        .get(&stream_id)
        .map(|stream| (stream.writer.clone(), stream.finished.subscribe()))
        .unzip();
    let id = asyncrt::enqueue(async move {
        let cancelled = match (writer, finished.as_mut()) {
            (Some(writer), Some(finished)) => tokio::select! {
                biased;
                result = finished.changed() => result.is_err(),
                _ = writer.closed() => true,
            },
            _ => false,
        };
        Ok(String::from(if cancelled {
            "cancelled"
        } else {
            "finished"
        }))
    });
    rv.set(promise_for(scope, id));
}

fn op_response_stream_close(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let stream_id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    let error = args.get(1).to_rust_string_lossy(scope);
    let stream = response_stream_writers().lock().unwrap().remove(&stream_id);
    let id = asyncrt::enqueue(async move {
        if let Some(stream) = stream {
            let _ = stream.finished.send(true);
            if !error.is_empty() {
                let _ = stream.writer.send(Err(error)).await;
            }
        }
        Ok(String::new())
    });
    rv.set(promise_for(scope, id));
}

pub fn reqwest_response_stream(response: reqwest::Response) -> HttpChunkStream {
    http_chunk_stream(HttpStreamSource::Response(response))
}

/// Timer op behind `setTimeout`: a promise resolving after `ms`.
fn op_timer(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let timer_id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    let ms = args.get(1).number_value(scope).unwrap_or(0.0).max(0.0) as u64;
    let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel();
    timer_cancels().lock().unwrap().insert(timer_id, cancel_tx);
    let id = asyncrt::enqueue(async move {
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_millis(ms)) => {}
            _ = &mut cancel_rx => {}
        }
        timer_cancels().lock().unwrap().remove(&timer_id);
        Ok(String::new())
    });
    let p = promise_for(scope, id);
    rv.set(p);
}

fn op_timer_alloc(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let id = v8::Number::new(scope, NEXT_TIMER_ID.fetch_add(1, Ordering::Relaxed) as f64);
    rv.set(id.into());
}

fn op_timer_cancel(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let timer_id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    if let Some(cancel) = timer_cancels().lock().unwrap().remove(&timer_id) {
        let _ = cancel.send(());
    }
}

fn durable_object_id_key(namespace_key: &str) -> [u8; 32] {
    DO_ID_KEYS.with(|keys| {
        if let Some(key) = keys.borrow().get(namespace_key) {
            return *key;
        }
        use sha2::Digest;
        let key: [u8; 32] = sha2::Sha256::digest(namespace_key.as_bytes()).into();
        keys.borrow_mut().insert(namespace_key.to_string(), key);
        key
    })
}

fn durable_object_id_hmac(key: &[u8; 32], input: &[u8]) -> hmac::Hmac<sha2::Sha256> {
    use hmac::Mac;
    let mut mac = <hmac::Hmac<sha2::Sha256> as hmac::Mac>::new_from_slice(key)
        .expect("SHA-256 accepts any HMAC key");
    mac.update(input);
    mac
}

fn durable_object_id_for_name(namespace_key: &str, name: &str) -> [u8; 32] {
    use hmac::Mac;

    let key = durable_object_id_key(namespace_key);
    let mut id = [0_u8; 32];
    let digest = durable_object_id_hmac(&key, name.as_bytes())
        .finalize()
        .into_bytes();
    id[..16].copy_from_slice(&digest[..16]);
    let digest = durable_object_id_hmac(&key, &id[..16])
        .finalize()
        .into_bytes();
    id[16..].copy_from_slice(&digest[..16]);
    id
}

fn durable_object_id_hex(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn decode_durable_object_id(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 {
        return None;
    }
    let mut output = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let nibble = |byte| match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        };
        output[index] = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    Some(output)
}

fn throw_durable_object_id_error(scope: &mut v8::PinScope, message: &str) {
    let message = v8::String::new(scope, message).unwrap();
    let exception = v8::Exception::type_error(scope, message);
    scope.throw_exception(exception);
}

fn register_actor_name(
    scope: &mut v8::PinScope,
    actor_scope: &str,
    name: Option<&str>,
) -> Result<()> {
    let Some(name) = name else {
        return Ok(());
    };
    let context = scope.get_current_context();
    let global = context.global(scope);
    let cell_key = v8::String::new(scope, "__cell").unwrap();
    let cell = global
        .get(scope, cell_key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing __cell runtime state"))?;
    let names_key = v8::String::new(scope, "idNames").unwrap();
    let names = cell
        .get(scope, names_key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing Durable Object name registry"))?;
    let actor_scope_key = v8::String::new(scope, actor_scope).unwrap();
    if let Some(existing) = names.get(scope, actor_scope_key.into()) {
        if !existing.is_undefined() {
            if existing.to_rust_string_lossy(scope) == name {
                return Ok(());
            }
            anyhow::bail!("actor name conflicts with active identity for {actor_scope}");
        }
    }

    let (class_name, id) = actor_scope
        .split_once(':')
        .ok_or_else(|| anyhow!("named Durable Object scope has no class separator"))?;
    let namespace_keys_key = v8::String::new(scope, "namespaceKeys").unwrap();
    let namespace_keys = cell
        .get(scope, namespace_keys_key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing Durable Object namespace registry"))?;
    let class_name_key = v8::String::new(scope, class_name).unwrap();
    let namespace_key = namespace_keys
        .get(scope, class_name_key.into())
        .filter(|value| value.is_string())
        .ok_or_else(|| anyhow!("missing namespace key for Durable Object class {class_name}"))?
        .to_rust_string_lossy(scope);
    let expected = durable_object_id_hex(&durable_object_id_for_name(&namespace_key, name));
    if id != expected {
        anyhow::bail!("actor name does not match Durable Object ID for {actor_scope}");
    }
    storage::set_actor_name(actor_scope, name)?;

    let name_value = v8::String::new(scope, name).unwrap();
    if !names
        .set(scope, actor_scope_key.into(), name_value.into())
        .unwrap_or(false)
    {
        anyhow::bail!("could not register Durable Object name");
    }
    Ok(())
}

fn op_do_id(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    use hmac::Mac;

    let namespace_key = args.get(0).to_rust_string_lossy(scope);
    let operation = args.get(1).to_rust_string_lossy(scope);
    let input = args.get(2).to_rust_string_lossy(scope);
    let key = durable_object_id_key(&namespace_key);
    let mut id = [0_u8; 32];

    match operation.as_str() {
        "name" => {
            id = durable_object_id_for_name(&namespace_key, &input);
            rv.set(
                v8::String::new(scope, &durable_object_id_hex(&id))
                    .unwrap()
                    .into(),
            );
            return;
        }
        "unique" => {
            if getrandom::fill(&mut id[..16]).is_err() {
                throw_durable_object_id_error(scope, "secure random generation failed");
                return;
            }
        }
        "validate" => {
            let Some(decoded) = decode_durable_object_id(&input) else {
                throw_durable_object_id_error(
                    scope,
                    "Invalid Durable Object ID: must be 64 hex digits",
                );
                return;
            };
            if durable_object_id_hmac(&key, &decoded[..16])
                .verify_truncated_left(&decoded[16..])
                .is_err()
            {
                throw_durable_object_id_error(
                    scope,
                    "Durable Object ID is not valid for this namespace",
                );
                return;
            }
            rv.set(
                v8::String::new(scope, &input.to_ascii_lowercase())
                    .unwrap()
                    .into(),
            );
            return;
        }
        _ => {
            throw_durable_object_id_error(scope, "unknown Durable Object ID operation");
            return;
        }
    }

    let digest = durable_object_id_hmac(&key, &id[..16])
        .finalize()
        .into_bytes();
    id[16..].copy_from_slice(&digest[..16]);
    rv.set(
        v8::String::new(scope, &durable_object_id_hex(&id))
            .unwrap()
            .into(),
    );
}

fn crypto_bytes(value: &serde_json::Value, name: &str) -> Result<Vec<u8>> {
    serde_json::from_value(value.get(name).cloned().unwrap_or_default())
        .map_err(|_| anyhow!("invalid {name} bytes"))
}

fn rsa_jwk_uint(value: &serde_json::Value, name: &str) -> Result<rsa::BigUint> {
    use base64::Engine;
    let encoded = value
        .get(name)
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("RSA JWK missing {name}"))?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| anyhow!("invalid RSA JWK {name}"))?;
    Ok(rsa::BigUint::from_bytes_be(&bytes))
}

fn rsa_jwk_encode(value: &rsa::BigUint) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value.to_bytes_be())
}

fn crypto_operation(operation: &str, args: &serde_json::Value) -> Result<serde_json::Value> {
    match operation {
        "hmac-sign" => {
            use hmac::Mac;
            let key = crypto_bytes(args, "key")?;
            let data = crypto_bytes(args, "data")?;
            let hash = args
                .get("hash")
                .and_then(|value| value.as_str())
                .unwrap_or("SHA-256");
            if !hash.eq_ignore_ascii_case("SHA-256") && !hash.eq_ignore_ascii_case("SHA256") {
                return Err(anyhow!("unsupported HMAC hash"));
            }
            let mut mac = <hmac::Hmac<sha2::Sha256> as hmac::Mac>::new_from_slice(&key)
                .map_err(|_| anyhow!("invalid HMAC key"))?;
            mac.update(&data);
            Ok(serde_json::json!({ "bytes": mac.finalize().into_bytes().to_vec() }))
        }
        "aes-gcm-encrypt" | "aes-gcm-decrypt" => {
            use aes_gcm::aead::{Aead, KeyInit};
            let key = crypto_bytes(args, "key")?;
            let iv = crypto_bytes(args, "iv")?;
            let data = crypto_bytes(args, "data")?;
            let cipher = aes_gcm::Aes256Gcm::new_from_slice(&key)
                .map_err(|_| anyhow!("AES-GCM requires a 256-bit key"))?;
            let nonce = aes_gcm::Nonce::from_slice(&iv);
            let bytes = if operation == "aes-gcm-encrypt" {
                cipher.encrypt(nonce, data.as_ref())
            } else {
                cipher.decrypt(nonce, data.as_ref())
            }
            .map_err(|_| anyhow!("AES-GCM operation failed"))?;
            Ok(serde_json::json!({ "bytes": bytes }))
        }
        "ed25519-sign" => {
            use ed25519_dalek::pkcs8::DecodePrivateKey;
            use ed25519_dalek::Signer;
            let key = crypto_bytes(args, "key")?;
            let data = crypto_bytes(args, "data")?;
            let key = ed25519_dalek::SigningKey::from_pkcs8_der(&key)
                .map_err(|_| anyhow!("invalid Ed25519 PKCS#8 key"))?;
            Ok(serde_json::json!({ "bytes": key.sign(&data).to_bytes().to_vec() }))
        }
        "p256-sign" => {
            use p256::ecdsa::signature::Signer;
            use p256::pkcs8::DecodePrivateKey;
            let key = crypto_bytes(args, "key")?;
            let data = crypto_bytes(args, "data")?;
            let key = p256::ecdsa::SigningKey::from_pkcs8_der(&key)
                .map_err(|_| anyhow!("invalid P-256 PKCS#8 key"))?;
            let signature: p256::ecdsa::Signature = key.sign(&data);
            Ok(serde_json::json!({ "bytes": signature.to_bytes().to_vec() }))
        }
        "rsa-generate" => {
            use rsa::traits::{PrivateKeyParts, PublicKeyParts};
            let mut rng = rand::rngs::OsRng;
            let mut private = rsa::RsaPrivateKey::new(&mut rng, 2048)?;
            private.precompute()?;
            let public = rsa::RsaPublicKey::from(&private);
            let public_jwk = serde_json::json!({
                "kty": "RSA",
                "n": rsa_jwk_encode(public.n()),
                "e": rsa_jwk_encode(public.e()),
                "alg": "RSA-OAEP-256",
                "ext": true,
                "key_ops": ["encrypt"],
            });
            let private_jwk = serde_json::json!({
                "kty": "RSA",
                "n": rsa_jwk_encode(private.n()),
                "e": rsa_jwk_encode(private.e()),
                "d": rsa_jwk_encode(private.d()),
                "p": rsa_jwk_encode(&private.primes()[0]),
                "q": rsa_jwk_encode(&private.primes()[1]),
                "alg": "RSA-OAEP-256",
                "ext": true,
                "key_ops": ["decrypt"],
            });
            Ok(serde_json::json!({ "publicKey": public_jwk, "privateKey": private_jwk }))
        }
        "rsa-oaep-decrypt" => {
            let jwk = args.get("jwk").ok_or_else(|| anyhow!("missing RSA JWK"))?;
            let n = rsa_jwk_uint(jwk, "n")?;
            let e = rsa_jwk_uint(jwk, "e")?;
            let d = rsa_jwk_uint(jwk, "d")?;
            let p = rsa_jwk_uint(jwk, "p")?;
            let q = rsa_jwk_uint(jwk, "q")?;
            let mut key = rsa::RsaPrivateKey::from_components(n, e, d, vec![p, q])?;
            key.precompute()?;
            let data = crypto_bytes(args, "data")?;
            let bytes = key.decrypt(rsa::Oaep::new::<sha2::Sha256>(), &data)?;
            Ok(serde_json::json!({ "bytes": bytes }))
        }
        _ => Err(anyhow!("unsupported crypto operation")),
    }
}

fn op_crypto_operation(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let operation = args.get(0).to_rust_string_lossy(scope);
    let input: serde_json::Value =
        serde_json::from_str(&args.get(1).to_rust_string_lossy(scope)).unwrap_or_default();
    match crypto_operation(&operation, &input) {
        Ok(value) => {
            let json = value.to_string();
            rv.set(v8::String::new(scope, &json).unwrap().into());
        }
        Err(error) => {
            let message = v8::String::new(scope, &format!("crypto: {error}")).unwrap();
            let exception = v8::Exception::error(scope, message);
            scope.throw_exception(exception);
        }
    }
}

fn view_bytes(value: v8::Local<v8::Value>) -> Option<Vec<u8>> {
    let view = v8::Local::<v8::ArrayBufferView>::try_from(value).ok()?;
    let mut bytes = vec![0_u8; view.byte_length()];
    view.copy_contents(&mut bytes);
    Some(bytes)
}

fn webcrypto_return_bytes(
    scope: &mut v8::PinScope,
    mut rv: v8::ReturnValue<v8::Value>,
    bytes: &[u8],
) {
    let buffer = v8::ArrayBuffer::new(scope, bytes.len());
    if !bytes.is_empty() {
        let store = buffer.get_backing_store();
        let destination = unsafe {
            std::slice::from_raw_parts_mut(store.data().unwrap().as_ptr() as *mut u8, bytes.len())
        };
        destination.copy_from_slice(bytes);
    }
    let view = v8::Uint8Array::new(scope, buffer, 0, bytes.len()).unwrap();
    rv.set(view.into());
}

/// Fill an existing integer typed array in-place. JS performs the WebCrypto
/// type and 65,536-byte quota checks before entering this host op.
fn op_webcrypto_random(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let Ok(view) = v8::Local::<v8::ArrayBufferView>::try_from(args.get(0)) else {
        return;
    };
    let Some(buffer) = view.buffer(scope) else {
        return;
    };
    let store = buffer.get_backing_store();
    let offset = view.byte_offset();
    let len = view.byte_length();
    let bytes = unsafe {
        std::slice::from_raw_parts_mut(store[offset..offset + len].as_ptr() as *mut u8, len)
    };
    if getrandom::fill(bytes).is_err() {
        let message = v8::String::new(scope, "secure random generation failed").unwrap();
        let exception = v8::Exception::error(scope, message);
        scope.throw_exception(exception);
    }
}

/// WebCrypto digest: (algorithm, ArrayBufferView) -> Uint8Array.
fn op_webcrypto_digest(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    rv: v8::ReturnValue<v8::Value>,
) {
    use sha2::Digest;
    let algorithm = args.get(0).to_rust_string_lossy(scope).to_ascii_uppercase();
    let Some(bytes) = view_bytes(args.get(1)) else {
        return;
    };
    let digest = match algorithm.as_str() {
        "MD5" => md5::Md5::digest(&bytes).to_vec(),
        "SHA-1" => sha1::Sha1::digest(&bytes).to_vec(),
        "SHA-224" => sha2::Sha224::digest(&bytes).to_vec(),
        "SHA-256" => sha2::Sha256::digest(&bytes).to_vec(),
        "SHA-384" => sha2::Sha384::digest(&bytes).to_vec(),
        "SHA-512" => sha2::Sha512::digest(&bytes).to_vec(),
        _ => return,
    };
    webcrypto_return_bytes(scope, rv, &digest);
}

fn op_webcrypto_hmac_sign(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    rv: v8::ReturnValue<v8::Value>,
) {
    use hmac::Mac;
    let algorithm = args.get(0).to_rust_string_lossy(scope).to_ascii_uppercase();
    let Some(key) = view_bytes(args.get(1)) else {
        return;
    };
    let Some(data) = view_bytes(args.get(2)) else {
        return;
    };
    macro_rules! sign {
        ($digest:ty) => {{
            let Ok(mut mac) = <hmac::Hmac<$digest> as hmac::Mac>::new_from_slice(&key) else {
                return;
            };
            mac.update(&data);
            mac.finalize().into_bytes().to_vec()
        }};
    }
    let signature = match algorithm.as_str() {
        "MD5" => sign!(md5::Md5),
        "SHA-1" => sign!(sha1::Sha1),
        "SHA-224" => sign!(sha2::Sha224),
        "SHA-256" => sign!(sha2::Sha256),
        "SHA-384" => sign!(sha2::Sha384),
        "SHA-512" => sign!(sha2::Sha512),
        _ => return,
    };
    webcrypto_return_bytes(scope, rv, &signature);
}

/// HMAC-based KDF cores for node:crypto. Manual PBKDF2 (RFC 2898) and HKDF
/// (RFC 5869) over the hmac crate — no extra dependencies. The JS layer has
/// already validated the digest name and the 255×hashLen output cap.
fn hmac_once<D>(key: &[u8], parts: &[&[u8]]) -> Vec<u8>
where
    D: hmac::digest::Digest + hmac::digest::core_api::BlockSizeUser,
{
    use hmac::Mac;
    let mut mac = <hmac::SimpleHmac<D> as hmac::Mac>::new_from_slice(key)
        .expect("HMAC accepts any key length");
    for part in parts {
        mac.update(part);
    }
    mac.finalize().into_bytes().to_vec()
}

fn pbkdf2_kdf<D>(password: &[u8], salt: &[u8], iterations: u32, keylen: usize) -> Vec<u8>
where
    D: hmac::digest::Digest + hmac::digest::core_api::BlockSizeUser,
{
    let mut out = Vec::with_capacity(keylen);
    let mut block: u32 = 1;
    while out.len() < keylen {
        let mut u = hmac_once::<D>(password, &[salt, &block.to_be_bytes()]);
        let mut t = u.clone();
        for _ in 1..iterations {
            u = hmac_once::<D>(password, &[&u]);
            for (t_, u_) in t.iter_mut().zip(&u) {
                *t_ ^= u_;
            }
        }
        out.extend_from_slice(&t);
        block += 1;
    }
    out.truncate(keylen);
    out
}

fn hkdf_kdf<D>(ikm: &[u8], salt: &[u8], info: &[u8], length: usize) -> Vec<u8>
where
    D: hmac::digest::Digest + hmac::digest::core_api::BlockSizeUser,
{
    let prk = hmac_once::<D>(salt, &[ikm]);
    let mut out = Vec::with_capacity(length);
    let mut t: Vec<u8> = Vec::new();
    let mut counter: u8 = 1;
    while out.len() < length {
        t = hmac_once::<D>(&prk, &[&t, info, &[counter]]);
        out.extend_from_slice(&t);
        counter += 1;
    }
    out.truncate(length);
    out
}

macro_rules! dispatch_digest {
    ($name:expr, $fn:ident, ($($arg:expr),*)) => {
        match $name {
            "MD5" => Some($fn::<md5::Md5>($($arg),*)),
            "SHA-1" => Some($fn::<sha1::Sha1>($($arg),*)),
            "SHA-224" => Some($fn::<sha2::Sha224>($($arg),*)),
            "SHA-256" => Some($fn::<sha2::Sha256>($($arg),*)),
            "SHA-384" => Some($fn::<sha2::Sha384>($($arg),*)),
            "SHA-512" => Some($fn::<sha2::Sha512>($($arg),*)),
            _ => None,
        }
    };
}

/// `$$pbkdf2(algorithm, password, salt, iterations, keylen)` -> Uint8Array
fn op_node_pbkdf2(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    rv: v8::ReturnValue<v8::Value>,
) {
    let algorithm = args.get(0).to_rust_string_lossy(scope);
    let Some(password) = view_bytes(args.get(1)) else {
        return;
    };
    let Some(salt) = view_bytes(args.get(2)) else {
        return;
    };
    let iterations = args.get(3).uint32_value(scope).unwrap_or(0).max(1);
    let keylen = args.get(4).uint32_value(scope).unwrap_or(0) as usize;
    let out = dispatch_digest!(
        algorithm.as_str(),
        pbkdf2_kdf,
        (&password, &salt, iterations, keylen)
    );
    if let Some(out) = out {
        webcrypto_return_bytes(scope, rv, &out);
    }
}

/// `$$hkdf(algorithm, ikm, salt, info, length)` -> Uint8Array
fn op_node_hkdf(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    rv: v8::ReturnValue<v8::Value>,
) {
    let algorithm = args.get(0).to_rust_string_lossy(scope);
    let Some(ikm) = view_bytes(args.get(1)) else {
        return;
    };
    let Some(salt) = view_bytes(args.get(2)) else {
        return;
    };
    let Some(info) = view_bytes(args.get(3)) else {
        return;
    };
    let length = args.get(4).uint32_value(scope).unwrap_or(0) as usize;
    let out = dispatch_digest!(algorithm.as_str(), hkdf_kdf, (&ikm, &salt, &info, length));
    if let Some(out) = out {
        webcrypto_return_bytes(scope, rv, &out);
    }
}

fn op_webcrypto_hmac_verify(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    use hmac::Mac;
    let algorithm = args.get(0).to_rust_string_lossy(scope).to_ascii_uppercase();
    let Some(key) = view_bytes(args.get(1)) else {
        return;
    };
    let Some(signature) = view_bytes(args.get(2)) else {
        return;
    };
    let Some(data) = view_bytes(args.get(3)) else {
        return;
    };
    macro_rules! verify {
        ($digest:ty) => {{
            let Ok(mut mac) = <hmac::Hmac<$digest> as hmac::Mac>::new_from_slice(&key) else {
                return;
            };
            mac.update(&data);
            mac.verify_slice(&signature).is_ok()
        }};
    }
    let valid = match algorithm.as_str() {
        "SHA-1" => verify!(sha1::Sha1),
        "SHA-256" => verify!(sha2::Sha256),
        "SHA-384" => verify!(sha2::Sha384),
        "SHA-512" => verify!(sha2::Sha512),
        _ => return,
    };
    rv.set(v8::Boolean::new(scope, valid).into());
}

fn op_webcrypto_aes_encrypt(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    rv: v8::ReturnValue<v8::Value>,
) {
    use aes_gcm::aead::{Aead, KeyInit};
    let Some(key) = view_bytes(args.get(0)) else {
        return;
    };
    let Some(iv) = view_bytes(args.get(1)) else {
        return;
    };
    let Some(data) = view_bytes(args.get(2)) else {
        return;
    };
    if iv.len() != 12 {
        return;
    }
    let nonce = aes_gcm::Nonce::from_slice(&iv);
    let encrypted = match key.len() {
        16 => {
            let Ok(cipher) = aes_gcm::Aes128Gcm::new_from_slice(&key) else {
                return;
            };
            cipher.encrypt(nonce, data.as_ref())
        }
        32 => {
            let Ok(cipher) = aes_gcm::Aes256Gcm::new_from_slice(&key) else {
                return;
            };
            cipher.encrypt(nonce, data.as_ref())
        }
        _ => return,
    };
    let Ok(encrypted) = encrypted else { return };
    webcrypto_return_bytes(scope, rv, &encrypted);
}

fn op_webcrypto_aes_decrypt(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    rv: v8::ReturnValue<v8::Value>,
) {
    use aes_gcm::aead::{Aead, KeyInit};
    let Some(key) = view_bytes(args.get(0)) else {
        return;
    };
    let Some(iv) = view_bytes(args.get(1)) else {
        return;
    };
    let Some(data) = view_bytes(args.get(2)) else {
        return;
    };
    if iv.len() != 12 {
        return;
    }
    let nonce = aes_gcm::Nonce::from_slice(&iv);
    let decrypted = match key.len() {
        16 => {
            let Ok(cipher) = aes_gcm::Aes128Gcm::new_from_slice(&key) else {
                return;
            };
            cipher.decrypt(nonce, data.as_ref())
        }
        32 => {
            let Ok(cipher) = aes_gcm::Aes256Gcm::new_from_slice(&key) else {
                return;
            };
            cipher.decrypt(nonce, data.as_ref())
        }
        _ => return,
    };
    let Ok(decrypted) = decrypted else { return };
    webcrypto_return_bytes(scope, rv, &decrypted);
}

fn op_zlib(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    use std::io::{Read, Write};
    let mode = args.get(0).to_rust_string_lossy(scope);
    let bytes: Vec<u8> =
        serde_json::from_str(&args.get(1).to_rust_string_lossy(scope)).unwrap_or_default();
    let result = (|| -> Result<Vec<u8>> {
        match mode.as_str() {
            "gzip" => {
                let mut encoder =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                encoder.write_all(&bytes)?;
                Ok(encoder.finish()?)
            }
            "gunzip" => {
                let mut decoder = flate2::read::GzDecoder::new(bytes.as_slice());
                let mut out = Vec::new();
                decoder.read_to_end(&mut out)?;
                Ok(out)
            }
            "deflate" => {
                let mut encoder =
                    flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
                encoder.write_all(&bytes)?;
                Ok(encoder.finish()?)
            }
            "inflate" => {
                let mut decoder = flate2::read::ZlibDecoder::new(bytes.as_slice());
                let mut out = Vec::new();
                decoder.read_to_end(&mut out)?;
                Ok(out)
            }
            "deflateRaw" => {
                let mut encoder =
                    flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
                encoder.write_all(&bytes)?;
                Ok(encoder.finish()?)
            }
            "inflateRaw" => {
                let mut decoder = flate2::read::DeflateDecoder::new(bytes.as_slice());
                let mut out = Vec::new();
                decoder.read_to_end(&mut out)?;
                Ok(out)
            }
            _ => Err(anyhow!("unsupported zlib operation")),
        }
    })();
    match result {
        Ok(bytes) => {
            let json = serde_json::to_string(&bytes).unwrap();
            rv.set(v8::String::new(scope, &json).unwrap().into());
        }
        Err(error) => {
            let message = v8::String::new(scope, &format!("zlib: {error}")).unwrap();
            let exception = v8::Exception::error(scope, message);
            scope.throw_exception(exception);
        }
    }
}

/// One live `CompressionStream`/`DecompressionStream`: a stateful flate2
/// writer whose `Vec` sink is drained after every push, so output streams
/// chunk by chunk instead of arriving once at close.
enum ZlibStream {
    GzipEncode(flate2::write::GzEncoder<Vec<u8>>),
    GzipDecode(flate2::write::GzDecoder<Vec<u8>>),
    ZlibEncode(flate2::write::ZlibEncoder<Vec<u8>>),
    ZlibDecode(flate2::write::ZlibDecoder<Vec<u8>>),
    RawEncode(flate2::write::DeflateEncoder<Vec<u8>>),
    RawDecode(flate2::write::DeflateDecoder<Vec<u8>>),
}

/// The six writer types share `write`/`flush`/`get_mut`/`finish` but no
/// trait, so dispatch once here.
macro_rules! zlib_each {
    ($stream:expr, $w:ident => $body:expr) => {
        match $stream {
            ZlibStream::GzipEncode($w) => $body,
            ZlibStream::GzipDecode($w) => $body,
            ZlibStream::ZlibEncode($w) => $body,
            ZlibStream::ZlibDecode($w) => $body,
            ZlibStream::RawEncode($w) => $body,
            ZlibStream::RawDecode($w) => $body,
        }
    };
}

impl ZlibStream {
    fn new(format: &str, decompress: bool) -> Option<ZlibStream> {
        use flate2::write as z;
        let level = flate2::Compression::default();
        let sink = Vec::new;
        Some(match (format, decompress) {
            ("gzip", false) => Self::GzipEncode(z::GzEncoder::new(sink(), level)),
            ("gzip", true) => Self::GzipDecode(z::GzDecoder::new(sink())),
            ("deflate", false) => Self::ZlibEncode(z::ZlibEncoder::new(sink(), level)),
            ("deflate", true) => Self::ZlibDecode(z::ZlibDecoder::new(sink())),
            ("deflate-raw", false) => Self::RawEncode(z::DeflateEncoder::new(sink(), level)),
            ("deflate-raw", true) => Self::RawDecode(z::DeflateDecoder::new(sink())),
            _ => return None,
        })
    }

    fn decompress(&self) -> bool {
        matches!(
            self,
            Self::GzipDecode(_) | Self::ZlibDecode(_) | Self::RawDecode(_)
        )
    }

    /// Workerd's canonical message, matched by its conformance suite.
    fn failure(&self) -> &'static str {
        if self.decompress() {
            "Decompression failed."
        } else {
            "Compression failed."
        }
    }

    /// Write one chunk and return whatever output it produced. Decoded
    /// output flows per chunk; encoders hold theirs until a full buffer
    /// accumulates (matching Workerd, whose suite reads a small message
    /// back as one chunk), with the rest arriving from `finish`. A
    /// decoder that stops consuming input has hit the end of the
    /// compressed stream, so leftover bytes are trailing garbage —
    /// Workerd's strict_compression_checks.
    fn push(&mut self, mut input: &[u8]) -> Result<Vec<u8>> {
        use std::io::Write;
        const ENCODER_CHUNK: usize = 32 * 1024;
        let failure = self.failure();
        let held = if self.decompress() { 0 } else { ENCODER_CHUNK };
        zlib_each!(self, w => {
            while !input.is_empty() {
                let n = w.write(input).map_err(|_| anyhow!(failure))?;
                if n == 0 { return Err(anyhow!(failure)); }
                input = &input[n..];
            }
            if w.get_ref().len() < held { return Ok(Vec::new()); }
            Ok(std::mem::take(w.get_mut()))
        })
    }

    /// Terminate the stream and return the final output. For gzip this
    /// also validates the trailer, so a truncated stream errors here.
    fn finish(self) -> Result<Vec<u8>> {
        let failure = self.failure();
        zlib_each!(self, w => w.finish().map_err(|_| anyhow!(failure)))
    }
}

thread_local! {
    static ZLIB_STREAMS: std::cell::RefCell<
        std::collections::HashMap<u64, ZlibStream>> = Default::default();
    static ZLIB_STREAM_NEXT: std::cell::Cell<u64> =
        const { std::cell::Cell::new(1) };
}

fn zlib_stream_return(
    scope: &mut v8::PinScope,
    rv: v8::ReturnValue<v8::Value>,
    result: Result<Vec<u8>>,
) {
    match result {
        Ok(bytes) => webcrypto_return_bytes(scope, rv, &bytes),
        Err(error) => {
            let message = v8::String::new(scope, &error.to_string()).unwrap();
            let exception = v8::Exception::type_error(scope, message);
            scope.throw_exception(exception);
        }
    }
}

/// `__zlib_stream_new(format, decompress)` -> id. JS validates the format
/// and throws the spec TypeError, so an unknown one here is a bug.
fn op_zlib_stream_new(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let format = args.get(0).to_rust_string_lossy(scope);
    let decompress = args.get(1).boolean_value(scope);
    let stream = ZlibStream::new(&format, decompress).expect("JS validated the compression format");
    let id = ZLIB_STREAM_NEXT.with(|next| {
        let id = next.get();
        next.set(id + 1);
        id
    });
    ZLIB_STREAMS.with(|streams| streams.borrow_mut().insert(id, stream));
    rv.set(v8::Number::new(scope, id as f64).into());
}

/// `__zlib_stream_push(id, view)` -> Uint8Array (possibly empty). An
/// error poisons the stream: its native state is dropped and the
/// TypeError errors both sides of the transform.
fn op_zlib_stream_push(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0) as u64;
    let bytes = view_bytes(args.get(1)).unwrap_or_default();
    let result = ZLIB_STREAMS.with(|streams| {
        let mut streams = streams.borrow_mut();
        let stream = streams
            .get_mut(&id)
            .ok_or_else(|| anyhow!("zlib stream already closed"))?;
        let out = stream.push(&bytes);
        if out.is_err() {
            streams.remove(&id);
        }
        out
    });
    zlib_stream_return(scope, rv, result);
}

/// `__zlib_stream_end(id)` -> the final Uint8Array. Consumes the stream.
fn op_zlib_stream_end(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0) as u64;
    let result = ZLIB_STREAMS.with(|streams| {
        streams
            .borrow_mut()
            .remove(&id)
            .ok_or_else(|| anyhow!("zlib stream already closed"))
    });
    zlib_stream_return(scope, rv, result.and_then(ZlibStream::finish));
}

/// `__zlib_stream_drop(id)` — cancelled/aborted transform cleanup.
fn op_zlib_stream_drop(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0) as u64;
    ZLIB_STREAMS.with(|streams| streams.borrow_mut().remove(&id));
}

fn op_ws_send(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let id = args
        .get(0)
        .to_integer(scope)
        .map(|n| n.value() as u64)
        .unwrap_or(0);
    let data = args.get(1).to_rust_string_lossy(scope);
    ws_emit(id, WsOut::Text(data));
}
fn op_ws_send_binary(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let id = args
        .get(0)
        .to_integer(scope)
        .map(|n| n.value() as u64)
        .unwrap_or(0);
    let data = view_bytes(args.get(1)).unwrap_or_default();
    ws_emit(id, WsOut::Binary(data));
}
fn op_ws_close(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let id = args
        .get(0)
        .to_integer(scope)
        .map(|n| n.value() as u64)
        .unwrap_or(0);
    let code = args.get(1).uint32_value(scope).unwrap_or(1000) as u16;
    let reason = args.get(2).to_rust_string_lossy(scope);
    ws_emit(id, WsOut::Close(code, reason));
}
fn op_ws_alloc(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    rv.set(v8::Number::new(scope, ws_next_id() as f64).into());
}
/// `fetch(url, { headers: { Upgrade: "websocket" } })`. Returns a JSON
/// envelope: either an upgraded socket, or the ordinary response a server sent
/// instead, which the caller returns unchanged.
fn op_ws_upgrade(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    if actor_runtime_state(scope).egress == EgressPolicy::Deny {
        return loader_throw(
            scope,
            "This worker is not permitted to access the internet via global functions.",
        );
    }
    let id = args
        .get(0)
        .to_integer(scope)
        .map(|n| n.value() as u64)
        .unwrap_or(0);
    let cell = args.get(1).to_rust_string_lossy(scope);
    let url = args.get(2).to_rust_string_lossy(scope);
    let headers: Vec<(String, String)> =
        serde_json::from_str(&args.get(3).to_rust_string_lossy(scope)).unwrap_or_default();
    let protocols: Vec<String> = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("sec-websocket-protocol"))
        .map(|(_, value)| value.split(',').map(|p| p.trim().to_string()).collect())
        .unwrap_or_default();
    let pull = cell.is_empty().then(|| {
        let (pull_tx, pull_rx) = tokio::sync::mpsc::unbounded_channel();
        ws_pull_register(id, pull_rx);
        ws_region_track(id);
        pull_tx
    });
    let (tx, rx) = tokio::sync::oneshot::channel();
    let sent = OUTBOUND_WS_TX.get().is_some_and(|sender| {
        sender
            .send(OutboundWsReq {
                scope: cell,
                id,
                url,
                protocols,
                pull,
                headers,
                want_response: true,
                reply: tx,
            })
            .is_ok()
    });
    let async_id = asyncrt::enqueue(async move {
        if !sent {
            return Err("no outbound WebSocket channel".into());
        }
        let open = match rx.await {
            Ok(Ok(open)) => open,
            Ok(Err(error)) => return Err(format!("WebSocket upgrade failed: {error}")),
            Err(error) => return Err(format!("WebSocket connector dropped: {error}")),
        };
        Ok(match open.declined {
            Some(declined) => serde_json::json!({
                "upgraded": false,
                "status": declined.status,
                "headers": declined.headers,
                "body": declined.body,
            })
            .to_string(),
            None => serde_json::json!({
                "upgraded": true,
                "protocol": open.protocol.unwrap_or_default(),
            })
            .to_string(),
        })
    });
    rv.set(promise_for(scope, async_id));
}

/// Await the next inbound event on an isolate-polled socket. Resolves with a
/// tagged buffer; a closed queue resolves as a 1006 close so the JS pump always
/// terminates rather than hanging on a dropped sender.
fn op_ws_next(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let id = args
        .get(0)
        .to_integer(scope)
        .map(|n| n.value() as u64)
        .unwrap_or(0);
    let queue = ws_pull().lock().unwrap().get(&id).cloned();
    let async_id = asyncrt::enqueue(async move {
        let Some(queue) = queue else {
            return Ok(WsPull::Close(1006, "socket is not registered".into(), false).encode());
        };
        let mut queue = queue.lock().await;
        Ok(queue
            .recv()
            .await
            .unwrap_or_else(|| WsPull::Close(1006, String::new(), false))
            .encode())
    });
    rv.set(promise_for(scope, async_id));
}

fn op_ws_connect(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    if actor_runtime_state(scope).egress == EgressPolicy::Deny {
        return loader_throw(
            scope,
            "This worker is not permitted to access the internet via global functions.",
        );
    }
    let id = args
        .get(0)
        .to_integer(scope)
        .map(|n| n.value() as u64)
        .unwrap_or(0);
    let cell = args.get(1).to_rust_string_lossy(scope);
    let url = args.get(2).to_rust_string_lossy(scope);
    let protocols: Vec<String> =
        serde_json::from_str(&args.get(3).to_rust_string_lossy(scope)).unwrap_or_default();
    // No cell means a Worker socket: the isolate polls it, so register the
    // queue here on the JS thread and track it against the running region.
    let pull = cell.is_empty().then(|| {
        let (pull_tx, pull_rx) = tokio::sync::mpsc::unbounded_channel();
        ws_pull_register(id, pull_rx);
        ws_region_track(id);
        pull_tx
    });
    let (tx, rx) = tokio::sync::oneshot::channel();
    let sent = OUTBOUND_WS_TX.get().is_some_and(|sender| {
        sender
            .send(OutboundWsReq {
                scope: cell,
                id,
                url,
                protocols,
                pull,
                headers: Vec::new(),
                want_response: false,
                reply: tx,
            })
            .is_ok()
    });
    let async_id = asyncrt::enqueue(async move {
        if !sent {
            return Err("no outbound WebSocket channel".into());
        }
        match rx.await {
            Ok(Ok(open)) => Ok(open.protocol.unwrap_or_default()),
            Ok(Err(error)) => Err(format!("WebSocket connection failed: {error}")),
            Err(error) => Err(format!("WebSocket connector dropped: {error}")),
        }
    });
    rv.set(promise_for(scope, async_id));
}
fn op_ws_accept(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let id = args
        .get(0)
        .to_integer(scope)
        .map(|n| n.value() as u64)
        .unwrap_or(0);
    let cell = args.get(1).to_rust_string_lossy(scope);
    let tags: Vec<String> =
        serde_json::from_str(&args.get(2).to_rust_string_lossy(scope)).unwrap_or_default();
    let replaced_regular_scope = {
        let mut registry = ws_registry().lock().unwrap();
        let sockets = &mut registry.metadata;
        let replaced_regular_scope = sockets
            .get(&id)
            .filter(|meta| !meta.hibernatable)
            .map(|meta| meta.scope.clone());
        sockets
            .entry(id)
            .and_modify(|meta| {
                meta.scope = cell.clone();
                meta.hibernatable = true;
                meta.tags = tags.clone();
            })
            .or_insert(WsMeta {
                scope: cell.clone(),
                hibernatable: true,
                tags,
                attachment: None,
                pending: Vec::new(),
            });
        replaced_regular_scope
    };
    if let Some(scope) = replaced_regular_scope {
        decrement_regular_ws(&scope);
    }
    tracing::info!(ws_id = id, scope = %cell, "accepted hibernatable WebSocket");
}
fn op_ws_accept_regular(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let id = args
        .get(0)
        .to_integer(scope)
        .map(|n| n.value() as u64)
        .unwrap_or(0);
    let cell = args.get(1).to_rust_string_lossy(scope);
    let inserted = {
        let mut registry = ws_registry().lock().unwrap();
        let sockets = &mut registry.metadata;
        if let std::collections::hash_map::Entry::Vacant(entry) = sockets.entry(id) {
            entry.insert(WsMeta {
                scope: cell.clone(),
                hibernatable: false,
                tags: Vec::new(),
                attachment: None,
                pending: Vec::new(),
            });
            true
        } else {
            false
        }
    };
    if inserted {
        increment_regular_ws(&cell);
    }
    tracing::info!(ws_id = id, scope = %cell, "accepted regular WebSocket");
}
fn op_ws_list(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let tag = args.get(1);
    let tag = if tag.is_undefined() || tag.is_null() {
        None
    } else {
        Some(tag.to_rust_string_lossy(scope))
    };
    let rows = ws_registry()
        .lock()
        .unwrap()
        .metadata
        .iter()
        .filter(|(_, meta)| {
            meta.hibernatable
                && meta.scope == cell
                && tag.as_ref().is_none_or(|tag| meta.tags.contains(tag))
        })
        .map(|(id, meta)| {
            serde_json::json!({
                "id": id,
                "tags": meta.tags,
                "attachment": meta.attachment,
            })
        })
        .collect::<Vec<_>>();
    let json = serde_json::to_string(&rows).unwrap();
    rv.set(v8::String::new(scope, &json).unwrap().into());
}
fn op_ws_attachment_set(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let id = args
        .get(0)
        .to_integer(scope)
        .map(|n| n.value() as u64)
        .unwrap_or(0);
    let attachment = args.get(1).to_rust_string_lossy(scope);
    if let Some(meta) = ws_registry().lock().unwrap().metadata.get_mut(&id) {
        meta.attachment = Some(attachment);
    }
}

#[cfg(all(test, celld_internal_tests))]
mod websocket_registry_private {
    include!(env!("CELLD_CONFORMANCE_WEBSOCKET_REGISTRY_TESTS"));
}
fn op_storage_get(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let scope_s = args.get(0).to_rust_string_lossy(scope);
    let key = args.get(1).to_rust_string_lossy(scope);
    match storage::get_stored(&scope_s, &key) {
        Ok(Some(value)) => {
            if let Some((value, _)) = deserialize_stored(scope, value, args.get(2)) {
                rv.set(value);
            }
        }
        Ok(None) => rv.set(v8::undefined(scope).into()),
        Err(error) => throw_storage_error(scope, "get", error),
    }
}
fn op_storage_get_many(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let keys = match storage_string_array(scope, args.get(1)) {
        Ok(keys) => keys,
        Err(error) => {
            throw_storage_error(scope, "get", error);
            return;
        }
    };
    match storage::get_many_stored(&cell, &keys) {
        Ok(entries) => {
            let sentinel = args.get(2);
            if let Some((map, tagged)) = storage_entries_map(scope, entries, sentinel) {
                rv.set(if tagged {
                    wrap_stored(scope, sentinel, map.into())
                } else {
                    map.into()
                });
            }
        }
        Err(error) => throw_storage_error(scope, "get", error),
    }
}
fn op_storage_put(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let s = args.get(0).to_rust_string_lossy(scope);
    let k = args.get(1).to_rust_string_lossy(scope);
    let Some(value) = serialize_storage_value(scope, args.get(2)) else {
        return;
    };
    if let Err(error) = storage::put_serialized(&s, &k, &value) {
        throw_storage_error(scope, "put", error);
    }
}
fn op_storage_put_many(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let Ok(entries) = v8::Local::<v8::Array>::try_from(args.get(1)) else {
        throw_storage_error(scope, "put", "entries must be an array");
        return;
    };
    let mut serialized = Vec::with_capacity(entries.length() as usize);
    for index in 0..entries.length() {
        let Some(entry) = entries.get_index(scope, index) else {
            return;
        };
        let Ok(entry) = v8::Local::<v8::Array>::try_from(entry) else {
            throw_storage_error(scope, "put", "entry must be a key/value pair");
            return;
        };
        let Some(key) = entry.get_index(scope, 0) else {
            return;
        };
        let Some(value) = entry.get_index(scope, 1) else {
            return;
        };
        let Some(value) = serialize_storage_value(scope, value) else {
            return;
        };
        serialized.push((key.to_rust_string_lossy(scope), value));
    }
    if let Err(error) = storage::put_many_serialized(&cell, &serialized) {
        throw_storage_error(scope, "put", error);
    }
}

/// `__storage_put_serialized(cell, key, bytes)`: write pre-encoded row
/// bytes verbatim — the stored-stub envelope (or a plain clone) built by
/// the JS storage wrapper after a plain clone failed. Never on the fast
/// path.
fn op_storage_put_serialized(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let key = args.get(1).to_rust_string_lossy(scope);
    let Some(bytes) = view_bytes(args.get(2)) else {
        throw_storage_error(scope, "put", "serialized row must be bytes");
        return;
    };
    if let Err(error) = storage::put_serialized(&cell, &key, &bytes) {
        throw_storage_error(scope, "put", error);
    }
}

/// The queued flavor of [`op_storage_put_serialized`], joining the same
/// pending-puts batch the plain queue ops feed.
fn op_storage_queue_put_serialized(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let key = args.get(1).to_rust_string_lossy(scope);
    let Some(bytes) = view_bytes(args.get(2)) else {
        throw_storage_error(scope, "put", "serialized row must be bytes");
        return;
    };
    queue_serialized_puts(scope, cell, vec![(key, bytes)]);
}

fn actor_runtime_state(scope: &mut v8::PinScope) -> Arc<ActorRuntimeState> {
    scope
        .get_slot::<Arc<ActorRuntimeState>>()
        .expect("actor runtime state slot")
        .clone()
}

fn queue_serialized_puts(scope: &mut v8::PinScope, cell: String, entries: Vec<(String, Vec<u8>)>) {
    actor_runtime_state(scope)
        .pending_puts
        .lock()
        .expect("pending puts lock poisoned")
        .entry(cell)
        .or_default()
        .extend(entries);
}

fn op_storage_queue_put(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let key = args.get(1).to_rust_string_lossy(scope);
    let Some(value) = serialize_storage_value(scope, args.get(2)) else {
        return;
    };
    queue_serialized_puts(scope, cell, vec![(key, value)]);
}

fn op_storage_queue_put_many(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let Ok(entries) = v8::Local::<v8::Array>::try_from(args.get(1)) else {
        throw_storage_error(scope, "put", "entries must be an array");
        return;
    };
    let mut serialized = Vec::with_capacity(entries.length() as usize);
    for index in 0..entries.length() {
        let Some(entry) = entries.get_index(scope, index) else {
            return;
        };
        let Ok(entry) = v8::Local::<v8::Array>::try_from(entry) else {
            throw_storage_error(scope, "put", "entry must be a key/value pair");
            return;
        };
        let Some(key) = entry.get_index(scope, 0) else {
            return;
        };
        let Some(value) = entry.get_index(scope, 1) else {
            return;
        };
        let Some(value) = serialize_storage_value(scope, value) else {
            return;
        };
        serialized.push((key.to_rust_string_lossy(scope), value));
    }
    queue_serialized_puts(scope, cell, serialized);
}

fn op_storage_flush_pending_puts(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let entries = actor_runtime_state(scope)
        .pending_puts
        .lock()
        .expect("pending puts lock poisoned")
        .remove(&cell)
        .unwrap_or_default();
    if entries.is_empty() {
        return;
    }
    if let Err(error) = storage::put_many_serialized(&cell, &entries) {
        throw_storage_error(scope, "put", error);
    }
}

fn op_storage_cancel_pending_puts(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    actor_runtime_state(scope)
        .pending_puts
        .lock()
        .expect("pending puts lock poisoned")
        .remove(&cell);
}

fn op_actor_abort(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let reason = args.get(1).to_rust_string_lossy(scope);
    let state = actor_runtime_state(scope);
    state
        .pending_puts
        .lock()
        .expect("pending puts lock poisoned")
        .remove(&cell);
    *state.termination.lock().expect("termination lock poisoned") = Some(ExecutionTermination {
        error: format!("__CELLD_ACTOR_ABORT__:{reason}"),
        actor_scope: Some(cell),
    });
    scope.terminate_execution();
}

fn op_process_exit(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let code = args.get(0).integer_value(scope).unwrap_or(0);
    let actor_scope = args.get(1).to_rust_string_lossy(scope);
    let actor_scope = (!actor_scope.is_empty()).then_some(actor_scope);
    let state = actor_runtime_state(scope);
    if let Some(actor_scope) = actor_scope.as_deref() {
        state
            .pending_puts
            .lock()
            .expect("pending puts lock poisoned")
            .remove(actor_scope);
    } else {
        state.worker_exit_requested.store(true, Ordering::Relaxed);
    }
    *state.termination.lock().expect("termination lock poisoned") = Some(ExecutionTermination {
        error: format!("__CELLD_PROCESS_EXIT__:The Node.js process.exit({code}) API was called."),
        actor_scope,
    });
    scope.terminate_execution();
}

fn op_log(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let msg: Vec<String> = (0..args.length())
        .map(|i| args.get(i).to_rust_string_lossy(scope))
        .collect();
    tracing::info!(target: "cell_console", "{}", msg.join(" "));
}
fn op_heap_limit_excessively_exceeded(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let exceeded = scope
        .get_slot::<Arc<HeapLimitState>>()
        .is_some_and(|state| state.excessively_exceeded.load(Ordering::Relaxed));
    rv.set(v8::Boolean::new(scope, exceeded).into());
}
#[cfg(all(test, celld_internal_tests))]
fn op_test_set_heap_limit_excessively_exceeded(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    if let Some(state) = scope.get_slot::<Arc<HeapLimitState>>() {
        state
            .excessively_exceeded
            .store(args.get(0).boolean_value(scope), Ordering::Relaxed);
    }
}
#[cfg(all(test, celld_internal_tests))]
fn op_test_gc(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    scope.request_garbage_collection_for_testing(v8::GarbageCollectionType::Full);
}
/// `ctx.storage.sql.exec(query, ...binds)` — args: scope, query, binds(JSON).
/// Returns a JSON `{columns, rows, rowsWritten}` (or `{error}`).
fn op_sql_exec(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let sc = args.get(0).to_rust_string_lossy(scope);
    let query = args.get(1).to_rust_string_lossy(scope);
    let binds: Vec<serde_json::Value> =
        serde_json::from_str(&args.get(2).to_rust_string_lossy(scope)).unwrap_or_default();
    let out = match storage::sql_exec(&sc, &query, &binds) {
        Ok((columns, rows, rows_written)) => serde_json::json!({
            "columns": columns, "rows": rows, "rowsWritten": rows_written,
        }),
        Err(e) => serde_json::json!({ "error": e }),
    };
    rv.set(v8::String::new(scope, &out.to_string()).unwrap().into());
}
fn op_sql_ingest(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let input = args.get(1).to_rust_string_lossy(scope);
    let out = match storage::sql_ingest(&cell, &input) {
        Ok((remainder, rows_written, statement_count)) => serde_json::json!({
            "remainder": remainder,
            "rowsWritten": rows_written,
            "statementCount": statement_count,
        }),
        Err(error) => serde_json::json!({ "error": error }),
    };
    rv.set(v8::String::new(scope, &out.to_string()).unwrap().into());
}
fn op_sql_cursor_start(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let query = args.get(1).to_rust_string_lossy(scope);
    let binds: Vec<serde_json::Value> =
        serde_json::from_str(&args.get(2).to_rust_string_lossy(scope)).unwrap_or_default();
    let out = match storage::sql_cursor_start(&cell, &query, &binds) {
        Ok((cursor, columns, row, rows_written, reused_cached_query)) => serde_json::json!({
            "native": true,
            "cursorId": cursor,
            "columns": columns,
            "row": row,
            "rowsWritten": rows_written,
            "reusedCachedQuery": reused_cached_query,
        }),
        Err(error) => serde_json::json!({ "error": error }),
    };
    rv.set(v8::String::new(scope, &out.to_string()).unwrap().into());
}
fn op_sql_cursor_next(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cursor = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    let out = match storage::sql_cursor_next(cursor) {
        Ok((row, rows_written)) => serde_json::json!({
            "row": row,
            "rowsWritten": rows_written,
        }),
        Err(error) => serde_json::json!({ "error": error }),
    };
    rv.set(v8::String::new(scope, &out.to_string()).unwrap().into());
}
fn op_sql_cursor_close(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cursor = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    storage::sql_cursor_close(cursor);
}
fn op_sql_database_size(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    match storage::sql_database_size(&cell) {
        Ok(bytes) => rv.set(v8::Number::new(scope, bytes as f64).into()),
        Err(error) => throw_storage_error(scope, "sql.databaseSize", error),
    }
}
#[cfg(all(test, celld_internal_tests))]
fn op_sql_set_max_page_count_for_test(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let pages = args.get(1).uint32_value(scope).unwrap_or(0);
    if let Err(error) = storage::set_max_page_count_for_test(&cell, pages) {
        throw_storage_error(scope, "sql.setMaxPageCountForTest", error);
    }
}
#[cfg(all(test, celld_internal_tests))]
fn op_sql_set_write_fault_for_test(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    storage::set_write_fault_for_test(args.get(0).boolean_value(scope));
}
#[cfg(all(test, celld_internal_tests))]
fn op_sql_set_cache_size_for_test(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let pages = args.get(1).int32_value(scope).unwrap_or(0);
    if let Err(error) = storage::set_cache_size_for_test(&cell, pages) {
        throw_storage_error(scope, "sql.setCacheSizeForTest", error);
    }
}
#[cfg(all(test, celld_internal_tests))]
fn op_sql_set_interrupt_fault_for_test(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let enabled = args.get(1).boolean_value(scope);
    if let Err(error) = storage::set_interrupt_fault_for_test(&cell, enabled) {
        throw_storage_error(scope, "sql.setInterruptFaultForTest", error);
    }
}
#[cfg(all(test, celld_internal_tests))]
fn op_sql_register_nomem_function_for_test(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    if let Err(error) = storage::register_nomem_function_for_test(&cell) {
        throw_storage_error(scope, "sql.registerNomemFunctionForTest", error);
    }
}
fn op_storage_transaction_control(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let action = args.get(1).to_rust_string_lossy(scope);
    let nested = args.get(2).boolean_value(scope);
    let savepoint = args.get(3).to_rust_string_lossy(scope);
    match storage::transaction_control(&cell, &action, nested, &savepoint) {
        Err(error) => throw_storage_error(scope, "transaction", error),
        // An outermost commit published a dirty alarm: register its
        // wake-entry PUT against the cell's output gate.
        Ok(Some(at)) if at >= 0 => spawn_arm_gate(&cell, at),
        Ok(_) => {}
    }
}
fn op_storage_delete(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let s = args.get(0).to_rust_string_lossy(scope);
    let k = args.get(1).to_rust_string_lossy(scope);
    match storage::delete(&s, &k) {
        Ok(deleted) => rv.set(v8::Boolean::new(scope, deleted).into()),
        Err(error) => throw_storage_error(scope, "delete", error),
    }
}
fn op_storage_delete_many(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let keys = match storage_string_array(scope, args.get(1)) {
        Ok(keys) => keys,
        Err(error) => {
            throw_storage_error(scope, "delete", error);
            return;
        }
    };
    match storage::delete_many(&cell, &keys) {
        Ok(deleted) => rv.set(v8::Number::new(scope, deleted as f64).into()),
        Err(error) => throw_storage_error(scope, "delete", error),
    }
}

#[derive(Default, serde::Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct StorageListOptions {
    start: Option<String>,
    end: Option<String>,
    start_after: Option<String>,
    prefix: Option<String>,
    limit: Option<usize>,
    reverse: bool,
}

fn op_storage_list(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let options = match serde_json::from_str::<StorageListOptions>(
        &args.get(1).to_rust_string_lossy(scope),
    ) {
        Ok(options) => options,
        Err(error) => {
            throw_storage_error(scope, "list", error);
            return;
        }
    };
    match storage::list_stored_with_options(
        &cell,
        options.start.as_deref(),
        options.end.as_deref(),
        options.start_after.as_deref(),
        options.prefix.as_deref(),
        options.limit,
        options.reverse,
    ) {
        Ok(entries) => {
            let sentinel = args.get(2);
            if let Some((map, tagged)) = storage_entries_map(scope, entries, sentinel) {
                rv.set(if tagged {
                    wrap_stored(scope, sentinel, map.into())
                } else {
                    map.into()
                });
            }
        }
        Err(error) => throw_storage_error(scope, "list", error),
    }
}

fn op_storage_sync_list_start(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let options = match serde_json::from_str::<StorageListOptions>(
        &args.get(1).to_rust_string_lossy(scope),
    ) {
        Ok(options) => options,
        Err(error) => {
            throw_storage_error(scope, "kv.list", error);
            return;
        }
    };
    match storage::sync_list_start(
        &cell,
        options.start.as_deref(),
        options.end.as_deref(),
        options.start_after.as_deref(),
        options.prefix.as_deref(),
        options.limit,
        options.reverse,
    ) {
        Ok(cursor) => rv.set(v8::Number::new(scope, cursor as f64).into()),
        Err(error) => throw_storage_error(scope, "kv.list", error),
    }
}

fn op_storage_sync_list_next(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cursor = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    match storage::sync_list_next(cursor) {
        Ok(Some((key, value))) => {
            let Some((decoded, _)) = deserialize_stored(scope, value, args.get(1)) else {
                throw_storage_error(
                    scope,
                    "kv.list",
                    format!("deserialization failed for key {key}"),
                );
                return;
            };
            let pair = v8::Array::new(scope, 2);
            let key = v8::String::new(scope, &key).unwrap();
            pair.set_index(scope, 0, key.into());
            pair.set_index(scope, 1, decoded);
            rv.set(pair.into());
        }
        Ok(None) => rv.set(v8::null(scope).into()),
        Err(error) => throw_storage_error(scope, "kv.list", error),
    }
}

struct StorageValueDelegate;

impl v8::ValueSerializerImpl for StorageValueDelegate {
    fn throw_data_clone_error(&self, scope: &mut v8::PinScope, message: v8::Local<v8::String>) {
        let exception = v8::Exception::type_error(scope, message);
        scope.throw_exception(exception);
    }
}

impl v8::ValueDeserializerImpl for StorageValueDelegate {}

fn serialize_storage_value(
    scope: &mut v8::PinScope,
    value: v8::Local<v8::Value>,
) -> Option<Vec<u8>> {
    let context = scope.get_current_context();
    let serializer = v8::ValueSerializer::new(scope, Box::new(StorageValueDelegate));
    serializer.write_header();
    if !serializer.write_value(context, value).unwrap_or(false) {
        return None;
    }
    let mut bytes = serializer.release();
    // Workerd pins persisted actor values to V8 wire version 15 so rolling
    // upgrades remain readable by older processes. V8 150 writes version 16;
    // for Cells' supported (sub-4GiB) ArrayBuffers its varint encoding is
    // byte-compatible, so retain the Workerd on-disk version tag.
    if bytes.starts_with(&[0xff, 0x10]) {
        bytes[1] = 0x0f;
    }
    Some(bytes)
}

fn deserialize_storage_value<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    value: storage::StoredValue,
) -> Option<v8::Local<'s, v8::Value>> {
    match value {
        storage::StoredValue::LegacyJson(json) => {
            let json = v8::String::new(scope, &json)?;
            v8::json::parse(scope, json)
        }
        storage::StoredValue::V8(bytes) => {
            let context = scope.get_current_context();
            let deserializer =
                v8::ValueDeserializer::new(scope, Box::new(StorageValueDelegate), &bytes);
            deserializer.set_supports_legacy_wire_format(true);
            if !deserializer.read_header(context).unwrap_or(false) {
                return None;
            }
            deserializer.read_value(context)
        }
    }
}

/// First byte of a stored-stub row: a durable stub marker tree follows
/// as an ordinary V8 clone. Plain rows keep V8's own 0xff prefix, so
/// the tag branch never runs for them.
const STORED_STUB_TAG: u8 = 0x01;

fn wrap_stored<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    sentinel: v8::Local<v8::Value>,
    value: v8::Local<v8::Value>,
) -> v8::Local<'s, v8::Value> {
    let pair = v8::Array::new(scope, 2);
    pair.set_index(scope, 0, sentinel);
    pair.set_index(scope, 1, value);
    pair.into()
}

/// Decode one stored row for JS. A stub-tagged row comes back as
/// `[sentinel, tree]` (plus `true`), which only the storage wrapper —
/// holder of the closure-private sentinel — turns back into live stubs.
fn deserialize_stored<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    value: storage::StoredValue,
    sentinel: v8::Local<v8::Value>,
) -> Option<(v8::Local<'s, v8::Value>, bool)> {
    match value {
        storage::StoredValue::V8(bytes) if bytes.first() == Some(&STORED_STUB_TAG) => {
            let tree =
                deserialize_storage_value(scope, storage::StoredValue::V8(bytes[1..].to_vec()))?;
            Some((wrap_stored(scope, sentinel, tree), true))
        }
        value => deserialize_storage_value(scope, value).map(|value| (value, false)),
    }
}

/// `__sc_encode(value)` -> Uint8Array: V8 structured clone, sharing the
/// storage serializer (same delegate, same pinned wire version). The RPC
/// marshalling seam — a failed clone leaves the delegate's throw pending.
fn op_sc_encode(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    if let Some(bytes) = serialize_storage_value(scope, args.get(0)) {
        rv.set(bytes_value(scope, bytes));
    }
}

/// `__sc_decode(bytes)` -> value: inverse of `__sc_encode`.
fn op_sc_decode(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let Some(bytes) = view_bytes(args.get(0)) else {
        let message = v8::String::new(scope, "__sc_decode expects bytes").unwrap();
        let exception = v8::Exception::type_error(scope, message);
        scope.throw_exception(exception);
        return;
    };
    match deserialize_storage_value(scope, storage::StoredValue::V8(bytes)) {
        Some(value) => rv.set(value),
        None => throw_storage_error(scope, "rpc", "corrupt clone payload"),
    }
}

fn storage_string_array(
    scope: &mut v8::PinScope,
    value: v8::Local<v8::Value>,
) -> std::result::Result<Vec<String>, &'static str> {
    let array = v8::Local::<v8::Array>::try_from(value).map_err(|_| "keys must be an array")?;
    let mut keys = Vec::with_capacity(array.length() as usize);
    for index in 0..array.length() {
        let value = array.get_index(scope, index).ok_or("missing key")?;
        keys.push(value.to_rust_string_lossy(scope));
    }
    Ok(keys)
}

/// The `bool` reports whether any entry was a stored-stub row, so the
/// caller wraps the whole map only then — plain maps cross unwrapped and
/// the JS side never walks them.
fn storage_entries_map<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    entries: Vec<(String, storage::StoredValue)>,
    sentinel: v8::Local<v8::Value>,
) -> Option<(v8::Local<'s, v8::Map>, bool)> {
    let map = v8::Map::new(scope);
    let mut any_tagged = false;
    for (key, value) in entries {
        let key = v8::String::new(scope, &key)?;
        let (value, tagged) = deserialize_stored(scope, value, sentinel)?;
        any_tagged |= tagged;
        map.set(scope, key.into(), value)?;
    }
    Some((map, any_tagged))
}

fn throw_storage_error(scope: &mut v8::PinScope, operation: &str, error: impl std::fmt::Display) {
    let message = v8::String::new(scope, &format!("storage.{operation}: {error}")).unwrap();
    let exception = v8::Exception::error(scope, message);
    scope.throw_exception(exception);
}
fn op_storage_delete_all(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let delete_alarm = args.get(1).boolean_value(scope);
    if let Err(error) = storage::delete_all_with_alarm(&cell, delete_alarm) {
        let message = v8::String::new(scope, &format!("deleteAll: {error}")).unwrap();
        let exception = v8::Exception::error(scope, message);
        scope.throw_exception(exception);
    }
}

/// $$urlParse(input, base?) -> {protocol,username,password,host,port,pathname,search,hash,href}
/// Backed by the WHATWG-conformant `url` crate.
fn op_url_parse(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let input = args.get(0).to_rust_string_lossy(scope);
    let base = args.get(1);
    let parsed = if base.is_undefined() {
        url::Url::parse(&input)
    } else {
        url::Url::options()
            .base_url(
                url::Url::parse(&base.to_rust_string_lossy(scope))
                    .ok()
                    .as_ref(),
            )
            .parse(&input)
    };
    let u = match parsed {
        Ok(u) => u,
        Err(e) => {
            let msg = v8::String::new(scope, &format!("Invalid URL: {e}")).unwrap();
            let exc = v8::Exception::type_error(scope, msg);
            scope.throw_exception(exc);
            return;
        }
    };
    let o = v8::Object::new(scope);
    let set = |k: &str, v: &str| {
        let key = v8::String::new(scope, k).unwrap();
        let val = v8::String::new(scope, v).unwrap();
        o.set(scope, key.into(), val.into());
    };
    set("protocol", u.scheme());
    set("username", u.username());
    set("password", u.password().unwrap_or(""));
    set("host", u.host_str().unwrap_or(""));
    set("port", &u.port().map(|p| p.to_string()).unwrap_or_default());
    set("pathname", u.path());
    set("search", u.query().unwrap_or(""));
    set("hash", u.fragment().unwrap_or(""));
    set("href", u.as_str());
    rv.set(o.into());
}

// URLPattern host seam, split like Deno's: pattern parsing and match-input
// canonicalization run in Rust via the `urlpattern` crate; per-match regex
// execution stays in JS `RegExp` (src/js/url_pattern.js). JSON is the
// boundary — both ops run at construct/match-canonicalize time only.

/// `$$urlPatternParse(inputJson, baseURL?, ignoreCase)` -> json. Input is a
/// pattern string or an init object of string components. Returns
/// per-component `{ patternString, regexpString, groupNameList }` plus
/// `hasRegexpGroups`. Throws TypeError on an invalid pattern.
fn op_urlpattern_parse(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    use urlpattern::quirks;
    let input = args.get(0).to_rust_string_lossy(scope);
    let input: quirks::StringOrInit = serde_json::from_str(&input).unwrap();
    let base = args.get(1);
    let base = (!base.is_undefined()).then(|| base.to_rust_string_lossy(scope));
    let options = urlpattern::UrlPatternOptions {
        ignore_case: args.get(2).boolean_value(scope),
    };
    let pattern = quirks::process_construct_pattern_input(input, base.as_deref())
        .and_then(|init| quirks::parse_pattern(init, options));
    let p = match pattern {
        Ok(p) => p,
        Err(e) => {
            let msg = v8::String::new(scope, &e.to_string()).unwrap();
            let exc = v8::Exception::type_error(scope, msg);
            scope.throw_exception(exc);
            return;
        }
    };
    let component = |c: &quirks::UrlPatternComponent| {
        serde_json::json!({
            "patternString": c.pattern_string,
            "regexpString": c.regexp_string,
            "groupNameList": c.group_name_list,
        })
    };
    let out = serde_json::json!({
        "protocol": component(&p.protocol),
        "username": component(&p.username),
        "password": component(&p.password),
        "hostname": component(&p.hostname),
        "port": component(&p.port),
        "pathname": component(&p.pathname),
        "search": component(&p.search),
        "hash": component(&p.hash),
        "hasRegexpGroups": p.has_regexp_groups,
    });
    rv.set(v8::String::new(scope, &out.to_string()).unwrap().into());
}

/// `$$urlPatternMatchInput(inputJson, baseURL?)` -> json `[8 strings]` in
/// component order (protocol..hash), or `null` when the input does not parse
/// as a URL. Throws TypeError for an init combined with a baseURL argument.
fn op_urlpattern_match_input(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    use urlpattern::quirks;
    let input = args.get(0).to_rust_string_lossy(scope);
    let input: quirks::StringOrInit = serde_json::from_str(&input).unwrap();
    let base = args.get(1);
    let base = (!base.is_undefined()).then(|| base.to_rust_string_lossy(scope));
    let null = v8::null(scope).into();
    let m = match quirks::process_match_input(input, base.as_deref()) {
        Ok(Some((input_, _inputs))) => match quirks::parse_match_input(input_) {
            Some(m) => m,
            None => return rv.set(null),
        },
        Ok(None) => return rv.set(null),
        Err(e) => {
            let msg = v8::String::new(scope, &e.to_string()).unwrap();
            let exc = v8::Exception::type_error(scope, msg);
            scope.throw_exception(exc);
            return;
        }
    };
    let out = serde_json::json!([
        m.protocol, m.username, m.password, m.hostname, m.port, m.pathname, m.search, m.hash,
    ]);
    rv.set(v8::String::new(scope, &out.to_string()).unwrap().into());
}

fn op_atob(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    use base64::Engine;
    let input = args.get(0).to_rust_string_lossy(scope);
    let cleaned: String = input.chars().filter(|c| !c.is_whitespace()).collect();
    match base64::engine::general_purpose::STANDARD.decode(cleaned) {
        Ok(bytes) => {
            // atob returns a binary string (latin1)
            let s: String = bytes.iter().map(|&b| b as char).collect();
            rv.set(v8::String::new(scope, &s).unwrap().into());
        }
        Err(_) => {
            let msg = v8::String::new(scope, "Invalid base64").unwrap();
            let exc = v8::Exception::error(scope, msg);
            scope.throw_exception(exc);
        }
    }
}

// node:util introspection seam: V8-level queries that JS cannot perform,
// mirroring Workerd's C++ `node-internal:util` builtin. Only the lazy
// node:async_hooks prelude calls these. The value is V8's
// continuation-preserved embedder data — the primitive Workerd's
// AsyncContextFrame rides — which V8 itself captures and restores
// around every promise reaction. No promise hooks are installed, and an
// isolate that never touches AsyncLocalStorage never sets the value, so
// a bundle that does not import async_hooks pays nothing per promise.

fn op_als_get(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    rv.set(scope.get_continuation_preserved_embedder_data());
}

fn op_als_set(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    scope.set_continuation_preserved_embedder_data(args.get(0));
}

// node:util prelude calls these; registration is the sole per-isolate cost.

/// Type-check bitmask. Bit order must match `T` in src/js/node_util.js.
fn op_util_type_flags(
    _scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let v = args.get(0);
    let checks: [bool; 27] = [
        v.is_external(),
        v.is_date(),
        v.is_arguments_object(),
        v.is_big_int_object(),
        v.is_boolean_object(),
        v.is_number_object(),
        v.is_string_object(),
        v.is_symbol_object(),
        v.is_native_error(),
        v.is_reg_exp(),
        v.is_async_function(),
        v.is_generator_function(),
        v.is_generator_object(),
        v.is_promise(),
        v.is_map(),
        v.is_set(),
        v.is_map_iterator(),
        v.is_set_iterator(),
        v.is_weak_map(),
        v.is_weak_set(),
        v.is_array_buffer(),
        v.is_data_view(),
        v.is_shared_array_buffer(),
        v.is_proxy(),
        v.is_module_namespace_object(),
        v.is_typed_array(),
        v.is_array_buffer_view(),
    ];
    let mut flags: u32 = 0;
    for (i, hit) in checks.iter().enumerate() {
        if *hit {
            flags |= 1 << i;
        }
    }
    rv.set_uint32(flags);
}

fn op_util_constructor_name(
    _scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let Ok(obj) = v8::Local::<v8::Object>::try_from(args.get(0)) else {
        return; // undefined
    };
    rv.set(obj.get_constructor_name().into());
}

/// `[target, handler]` for a proxy, undefined otherwise.
fn op_util_proxy_details(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let Ok(proxy) = v8::Local::<v8::Proxy>::try_from(args.get(0)) else {
        return;
    };
    let target = proxy.get_target(scope);
    let handler = proxy.get_handler(scope);
    rv.set(v8::Array::new_with_elements(scope, &[target, handler]).into());
}

/// `[state]` for a pending promise, `[state, result]` otherwise,
/// undefined for a non-promise. States: 0 pending, 1 fulfilled, 2 rejected.
fn op_util_promise_details(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let Ok(promise) = v8::Local::<v8::Promise>::try_from(args.get(0)) else {
        return;
    };
    let state = match promise.state() {
        v8::PromiseState::Pending => 0,
        v8::PromiseState::Fulfilled => 1,
        v8::PromiseState::Rejected => 2,
    };
    let pending = state == 0;
    let state: v8::Local<v8::Value> = v8::Integer::new(scope, state).into();
    let elements = if pending {
        vec![state]
    } else {
        vec![state, promise.result(scope)]
    };
    rv.set(v8::Array::new_with_elements(scope, &elements).into());
}

/// `[entries, isKeyValue]` for collections and their iterators (V8's
/// PreviewEntries), undefined when V8 has no preview.
fn op_util_preview_entries(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let Ok(obj) = v8::Local::<v8::Object>::try_from(args.get(0)) else {
        return;
    };
    let (entries, is_key_value) = obj.preview_entries(scope);
    let Some(entries) = entries else { return };
    let flag: v8::Local<v8::Value> = v8::Boolean::new(scope, is_key_value).into();
    rv.set(v8::Array::new_with_elements(scope, &[entries.into(), flag]).into());
}

fn op_alarm_set(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let s = args.get(0).to_rust_string_lossy(scope);
    let at = args.get(1).number_value(scope).unwrap_or(0.0) as i64;
    match storage::set_alarm(&s, at) {
        Err(error) => throw_storage_error(scope, "setAlarm", error),
        // Committed immediately: register the wake-entry PUT against the
        // cell's output gate. Inside an explicit transaction (`Ok(None)`)
        // the gate registers at that transaction's commit instead.
        Ok(Some(committed)) if committed >= 0 => spawn_arm_gate(&s, committed),
        Ok(_) => {}
    }
}
fn op_alarm_get(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let s = args.get(0).to_rust_string_lossy(scope);
    match storage::get_alarm(&s) {
        Some(at) => rv.set(v8::Number::new(scope, at as f64).into()),
        None => rv.set(v8::null(scope).into()),
    }
}
fn op_alarm_delete(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let s = args.get(0).to_rust_string_lossy(scope);
    if let Err(error) = storage::delete_alarm(&s) {
        throw_storage_error(scope, "deleteAlarm", error);
    }
}

fn op_btoa(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    use base64::Engine;
    let input = args.get(0).to_rust_string_lossy(scope);
    let bytes: Vec<u8> = input.chars().map(|c| c as u8).collect();
    let s = base64::engine::general_purpose::STANDARD.encode(bytes);
    rv.set(v8::String::new(scope, &s).unwrap().into());
}

// ---- WHATWG legacy text decoders (encoding_rs) ----
//
// TextDecoder's utf-8/utf-16 hot path stays pure JS in
// src/js/text_encoding.js; these ops back every other WHATWG label
// (windows-1252, Big5, GBK/GB18030, ISO-2022-JP, x-user-defined, …).
// Streaming decoders live in a thread-local table keyed by id; JS frees
// one on the final decode (last=true), on a fatal error (encoding_rs
// poisons an errored decoder), or via FinalizationRegistry when a
// mid-stream decoder is abandoned. Ids are never reused, so a late
// finalizer free of an already-closed id is a no-op.

thread_local! {
    static TEXT_DECODERS: std::cell::RefCell<
        std::collections::HashMap<u64, encoding_rs::Decoder>> =
            Default::default();
    static TEXT_DECODER_NEXT: std::cell::Cell<u64> =
        const { std::cell::Cell::new(1) };
}

/// `$$textDecoderLabel(label)` -> canonical lowercase name, or undefined
/// for unknown labels and the replacement encoding (RangeError in JS).
fn op_text_decoder_label(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let label = args.get(0).to_rust_string_lossy(scope);
    if let Some(enc) = encoding_rs::Encoding::for_label_no_replacement(label.as_bytes()) {
        let name = enc.name().to_ascii_lowercase();
        rv.set(v8::String::new(scope, &name).unwrap().into());
    }
}

/// `$$textDecoderNew(name, ignoreBOM)` -> id.
fn op_text_decoder_new(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let name = args.get(0).to_rust_string_lossy(scope);
    let ignore_bom = args.get(1).boolean_value(scope);
    let enc = encoding_rs::Encoding::for_label(name.as_bytes()).expect("JS resolved the label");
    let dec = if ignore_bom {
        enc.new_decoder_without_bom_handling()
    } else {
        enc.new_decoder_with_bom_removal()
    };
    let id = TEXT_DECODER_NEXT.with(|next| {
        let id = next.get();
        next.set(id + 1);
        id
    });
    TEXT_DECODERS.with(|table| table.borrow_mut().insert(id, dec));
    rv.set(v8::Number::new(scope, id as f64).into());
}

/// `$$textDecoderDecode(id, view, fatal, last)` -> string. Frees the
/// decoder when `last` is true or on a fatal malformed sequence
/// (TypeError).
fn op_text_decoder_decode(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0) as u64;
    let bytes = view_bytes(args.get(1)).unwrap_or_default();
    let fatal = args.get(2).boolean_value(scope);
    let last = args.get(3).boolean_value(scope);
    // Per spec an empty streaming chunk is a no-op, but encoding_rs
    // 0.8 flushes a pending multibyte lead when fed an empty non-last
    // slice (e.g. Shift_JIS 0x82, "", 0xA0 would yield U+FFFD instead
    // of あ). Skip the decoder entirely.
    if bytes.is_empty() && !last {
        rv.set(v8::String::empty(scope).into());
        return;
    }
    let mut dec = TEXT_DECODERS
        .with(|table| table.borrow_mut().remove(&id))
        .expect("JS holds the only live id");
    let mut out = String::new();
    let ok = if fatal {
        let cap = dec
            .max_utf8_buffer_length_without_replacement(bytes.len())
            .unwrap();
        out.reserve(cap);
        matches!(
            dec.decode_to_string_without_replacement(&bytes, &mut out, last),
            (encoding_rs::DecoderResult::InputEmpty, _),
        )
    } else {
        let cap = dec.max_utf8_buffer_length(bytes.len()).unwrap();
        out.reserve(cap);
        let _ = dec.decode_to_string(&bytes, &mut out, last);
        true
    };
    if !ok {
        let msg = v8::String::new(scope, "The encoded data was not valid.").unwrap();
        let exc = v8::Exception::type_error(scope, msg);
        scope.throw_exception(exc);
        return;
    }
    if !last {
        TEXT_DECODERS.with(|table| table.borrow_mut().insert(id, dec));
    }
    rv.set(v8::String::new(scope, &out).unwrap().into());
}

/// `$$textDecoderFree(id)` — FinalizationRegistry cleanup for a decoder
/// abandoned mid-stream.
fn op_text_decoder_free(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0) as u64;
    TEXT_DECODERS.with(|table| table.borrow_mut().remove(&id));
}

// ---- the JS harness: Web API + the DO object model ----

/// Per-process compiled-bytecode cache for the fixed bootstrap scripts.
/// Every cell wake pays a fresh isolate, and without this each one re-parses
/// and re-compiles the same ~23k lines of prelude + harness — the single
/// largest slice of hibernated-wake latency. The first isolate compiles
/// eagerly and publishes the cache; every later isolate consumes it and only
/// executes.
type BootstrapCache = std::collections::HashMap<&'static str, std::sync::Arc<Vec<u8>>>;

fn bootstrap_code_cache() -> &'static std::sync::Mutex<BootstrapCache> {
    static CACHE: std::sync::OnceLock<std::sync::Mutex<BootstrapCache>> =
        std::sync::OnceLock::new();
    CACHE.get_or_init(Default::default)
}

fn run_bootstrap_script(scope: &mut v8::PinScope, name: &'static str, src: &str) -> Result<()> {
    use v8::script_compiler::CompileOptions;
    use v8::script_compiler::NoCacheReason;
    let code = v8::String::new(scope, src).unwrap();
    let cached = bootstrap_code_cache().lock().unwrap().get(name).cloned();
    let unbound = match cached {
        // `CachedData` borrows `bytes`; the Arc binding outlives the Source.
        Some(bytes) => {
            let data = v8::script_compiler::CachedData::new(&bytes);
            let mut source = v8::script_compiler::Source::new_with_cached_data(code, None, data);
            let unbound = v8::script_compiler::compile_unbound_script(
                scope,
                &mut source,
                CompileOptions::ConsumeCodeCache,
                NoCacheReason::NoReason,
            )
            .ok_or_else(|| anyhow!("bootstrap compile {name}"))?;
            debug_assert!(
                !source.get_cached_data().is_some_and(|data| data.rejected()),
                "code cache rejected for {name}"
            );
            unbound
        }
        None => {
            let mut source = v8::script_compiler::Source::new(code, None);
            // Eager: a lazily-compiled cache would push function-body compile
            // cost back into every consumer's first request.
            let unbound = v8::script_compiler::compile_unbound_script(
                scope,
                &mut source,
                CompileOptions::EagerCompile,
                NoCacheReason::NoReason,
            )
            .ok_or_else(|| anyhow!("bootstrap compile {name}"))?;
            if let Some(data) = unbound.create_code_cache() {
                bootstrap_code_cache()
                    .lock()
                    .unwrap()
                    .insert(name, std::sync::Arc::new(data.to_vec()));
            }
            unbound
        }
    };
    unbound
        .bind_to_current_context(scope)
        .run(scope)
        .ok_or_else(|| anyhow!("bootstrap run {name}"))?;
    Ok(())
}

/// WPT-conformant Web APIs: TextEncoder, URL/URLSearchParams, atob/btoa, and
/// Headers. Run once per isolate before the Cells/Cloudflare-specific harness.
fn install_prelude(scope: &mut v8::PinScope) -> Result<()> {
    const PRELUDE: &[(&str, &str)] = &[
        ("text_encoding.js", include_str!("js/text_encoding.js")),
        (
            "url_search_params.js",
            include_str!("js/url_search_params.js"),
        ),
        ("url.js", include_str!("js/url.js")),
        ("atob_btoa.js", include_str!("js/atob_btoa.js")),
        ("headers.js", include_str!("js/headers.js")),
        ("streams.js", include_str!("js/streams.js")),
        ("writable_stream.js", include_str!("js/writable_stream.js")),
        (
            "text_encoding_streams.js",
            include_str!("js/text_encoding_streams.js"),
        ),
    ];
    for (name, src) in PRELUDE {
        run_bootstrap_script(scope, name, src)?;
    }
    Ok(())
}

fn install_harness(scope: &mut v8::PinScope) -> Result<()> {
    run_bootstrap_script(scope, "harness.js", include_str!("js/harness.js"))?;
    run_bootstrap_script(scope, "crypto.js", include_str!("js/crypto.js"))
}

fn register_class(scope: &mut v8::PinScope, name: &str, cls: v8::Local<v8::Value>) -> Result<()> {
    let ctx = scope.get_current_context();
    let global = ctx.global(scope);
    let ck = v8::String::new(scope, "__cell").unwrap();
    let cell = global
        .get(scope, ck.into())
        .unwrap()
        .to_object(scope)
        .unwrap();
    let clk = v8::String::new(scope, "classes").unwrap();
    let classes = cell
        .get(scope, clk.into())
        .unwrap()
        .to_object(scope)
        .unwrap();
    let nk = v8::String::new(scope, name).unwrap();
    classes.set(scope, nk.into(), cls);
    Ok(())
}

/// Populate `cloudflare:workers` `exports` with the worker module's own exports:
/// a Durable Object class is exposed as its namespace (idFromName/get/RPC), any
/// other export by value. `default` is omitted (it is the fetch entrypoint, not
/// an importable export). Mirrors Workerd's `import { exports }` surface.
fn populate_cf_exports(
    scope: &mut v8::PinScope,
    ns: v8::Local<v8::Object>,
    do_classes: &[String],
) -> Result<()> {
    let context = scope.get_current_context();
    let global = context.global(scope);
    let cf = global
        .get(scope, v8::String::new(scope, "__cf").unwrap().into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing __cf runtime state"))?;
    let exports = cf
        .get(scope, v8::String::new(scope, "exports").unwrap().into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing __cf.exports"))?;
    let cell = global
        .get(scope, v8::String::new(scope, "__cell").unwrap().into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing __cell runtime state"))?;
    let make_ns: v8::Local<v8::Function> = cell
        .get(
            scope,
            v8::String::new(scope, "makeNamespace").unwrap().into(),
        )
        .and_then(|value| value.try_into().ok())
        .ok_or_else(|| anyhow!("missing __cell.makeNamespace"))?;
    let do_set: std::collections::HashSet<&str> = do_classes.iter().map(String::as_str).collect();
    let names = ns
        .get_own_property_names(scope, Default::default())
        .ok_or_else(|| anyhow!("no module export names"))?;
    for index in 0..names.length() {
        let name_value = names.get_index(scope, index).unwrap();
        let name = name_value.to_rust_string_lossy(scope);
        if name == "default" {
            continue;
        }
        let export_value = if do_set.contains(name.as_str()) {
            let arg = v8::String::new(scope, &name).unwrap().into();
            make_ns
                .call(scope, cell.into(), &[arg])
                .ok_or_else(|| anyhow!("makeNamespace({name}) failed"))?
        } else {
            ns.get(scope, name_value)
                .ok_or_else(|| anyhow!("export {name}"))?
        };
        let key = v8::String::new(scope, &name).unwrap();
        exports.set(scope, key.into(), export_value);
    }
    Ok(())
}

fn inject_namespace_keys(
    scope: &mut v8::PinScope,
    script_name: &str,
    do_classes: &[String],
) -> Result<()> {
    let context = scope.get_current_context();
    let global = context.global(scope);
    let cell_key = v8::String::new(scope, "__cell").unwrap();
    let cell = global
        .get(scope, cell_key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing __cell runtime state"))?;
    let keys_key = v8::String::new(scope, "namespaceKeys").unwrap();
    let keys = cell
        .get(scope, keys_key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing Durable Object namespace registry"))?;
    let script_key = v8::String::new(scope, "script").unwrap();
    let script_value = v8::String::new(scope, script_name).unwrap();
    cell.set(scope, script_key.into(), script_value.into());
    for class_name in do_classes {
        let class_key = v8::String::new(scope, class_name).unwrap();
        let namespace_key = format!("cells:v1:{}:{script_name}:{class_name}", script_name.len(),);
        let namespace_key = v8::String::new(scope, &namespace_key).unwrap();
        if !keys
            .set(scope, class_key.into(), namespace_key.into())
            .unwrap_or(false)
        {
            anyhow::bail!("could not register namespace key for {class_name}");
        }
    }
    Ok(())
}

/// `globalThis.Cloudflare.compatibilityFlags`. Only flags Cells actually
/// honours are listed; a flag Cells does not model is absent (falsy) rather
/// than reported as enabled.
///
/// Built through the V8 object API rather than by compiling a snippet: this
/// runs on every isolate load, and a separate `Script::compile` there cost
/// ~100us per worker.
fn inject_compatibility_flags(scope: &mut v8::PinScope, compat: Compat) -> Result<()> {
    let context = scope.get_current_context();
    let global = context.global(scope);
    let flags = v8::Object::new(scope);
    let set_flag = |scope: &mut v8::PinScope, name: &str, value: bool| {
        let key = v8::String::new(scope, name).unwrap();
        let value = v8::Boolean::new(scope, value);
        flags.set(scope, key.into(), value.into());
    };
    set_flag(scope, "upper_case_all_http_methods", true);
    // Cells' async stream/pipe methods return rejected promises
    // rather than throwing synchronously, which is exactly the
    // behavior this flag names.
    set_flag(scope, "capture_async_api_throws", true);
    set_flag(
        scope,
        "delete_all_deletes_alarm",
        compat.delete_all_deletes_alarm,
    );
    set_flag(scope, "js_rpc", compat.js_rpc);
    set_flag(
        scope,
        "fetcher_no_get_put_delete",
        !compat.fetcher_get_put_delete,
    );
    let cloudflare = v8::Object::new(scope);
    let key = v8::String::new(scope, "compatibilityFlags").unwrap();
    cloudflare.set(scope, key.into(), flags.into());
    let key = v8::String::new(scope, "Cloudflare").unwrap();
    global.set(scope, key.into(), cloudflare.into());
    Ok(())
}

fn build_env(scope: &mut v8::PinScope, config: &WorkerConfig) -> Result<()> {
    let script_name = config.script_name.as_str();
    let bindings = config.bindings.as_slice();
    let r2_bindings = config.r2_bindings.as_slice();
    let ai_binding = config.ai_binding.as_deref();
    let vars = config.vars.as_slice();
    let services = config.services.as_slice();
    let asset_binding = config.asset_binding.as_deref();
    // call the harness: for each binding, env[bindingName] = makeNamespace(className)
    let src = {
        let mut lines = String::from("(() => { const e = __cell.env;\n");
        for (bname, cname) in bindings {
            lines.push_str(&format!(
                "e[{:?}] = __cell.makeNamespace({:?});\n",
                bname, cname
            ));
        }
        for name in r2_bindings {
            lines.push_str(&format!("e[{:?}] = __makeR2Bucket({:?});\n", name, name));
        }
        if let (Some(name), Ok(url)) = (ai_binding, std::env::var("CELLD_AI_URL")) {
            lines.push_str(&format!("e[{:?}] = __makeAiBinding({:?});\n", name, url));
        }
        for (binding, script, entrypoint) in services {
            let entrypoint = match entrypoint {
                Some(name) => format!("{name:?}"),
                None => "null".to_string(),
            };
            lines.push_str(&format!(
                "e[{:?}] = __makeServiceBinding({:?}, {});\n",
                binding, script, entrypoint
            ));
        }
        for (name, value) in vars {
            lines.push_str(&format!("e[{:?}] = {:?};\n", name, value));
        }
        if let Some(name) = asset_binding {
            lines.push_str(&format!(
                "e[{:?}] = __makeAssetsBinding({:?});\n",
                name, script_name
            ));
        }
        if let Some(name) = config.loader_binding.as_deref() {
            lines.push_str(&format!("e[{:?}] = __makeLoader();\n", name));
        }
        // A loaded worker's caller-supplied `env` (plain JSON values only in
        // the walking skeleton) merges last, over the declared bindings.
        if let Some(env) = config.loader_env.as_deref() {
            lines.push_str(&format!("Object.assign(e, {});\n", env));
        }
        lines.push_str("})();");
        lines
    };
    let code = v8::String::new(scope, &src).unwrap();
    let s = v8::Script::compile(scope, code, None).ok_or_else(|| anyhow!("env compile"))?;
    s.run(scope).ok_or_else(|| anyhow!("env run"))?;
    Ok(())
}

/// Record every exported class deriving from `WorkerEntrypoint` in
/// `__cell.entrypoints`, so a service binding with `entrypoint = "Name"` can
/// resolve it, and every export deriving from `DurableObject` in
/// `__cell.doExports`, so misrouting a DO class as a stateless entrypoint is
/// reported as such (Workerd getExportedHandler()). Runs once per load; the
/// check is a prototype walk, not a call into user code.
fn register_entrypoints(scope: &mut v8::PinScope, ns: v8::Local<v8::Object>) -> Result<()> {
    let context = scope.get_current_context();
    let global = context.global(scope);
    let cell_key = v8::String::new(scope, "__cell").unwrap();
    let cell = global
        .get(scope, cell_key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing __cell runtime state"))?;
    let key = v8::String::new(scope, "entrypoints").unwrap();
    let entrypoints = cell
        .get(scope, key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing entrypoint registry"))?;
    let key = v8::String::new(scope, "objectEntrypoints").unwrap();
    let object_entrypoints = cell
        .get(scope, key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing object-entrypoint registry"))?;
    let key = v8::String::new(scope, "doExports").unwrap();
    let do_exports = cell
        .get(scope, key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing doExports registry"))?;
    let cf_key = v8::String::new(scope, "__cf").unwrap();
    let cf = global
        .get(scope, cf_key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing __cf"))?;
    let key = v8::String::new(scope, "WorkerEntrypoint").unwrap();
    let entrypoint_base = cf
        .get(scope, key.into())
        .ok_or_else(|| anyhow!("missing WorkerEntrypoint base"))?;
    let key = v8::String::new(scope, "DurableObject").unwrap();
    let durable_base = cf
        .get(scope, key.into())
        .ok_or_else(|| anyhow!("missing DurableObject base"))?;
    let Some(names) = ns.get_own_property_names(scope, Default::default()) else {
        return Ok(());
    };
    let yes = v8::Boolean::new(scope, true);
    for index in 0..names.length() {
        let Some(name) = names.get_index(scope, index) else {
            continue;
        };
        let Some(value) = ns.get(scope, name) else {
            continue;
        };
        if !value.is_function() {
            // A plain-object export is Workerd's non-class entrypoint; its
            // handler functions dispatch as fn(arg, env, ctx).
            if value.is_object() {
                object_entrypoints.set(scope, name, value);
            }
            continue;
        }
        // Walk the prototype chain rather than calling anything.
        let mut proto = value.to_object(scope).and_then(|o| o.get_prototype(scope));
        while let Some(current) = proto {
            if current == entrypoint_base {
                entrypoints.set(scope, name, value);
                break;
            }
            if current == durable_base {
                do_exports.set(scope, name, yes.into());
                break;
            }
            proto = current
                .to_object(scope)
                .and_then(|o| o.get_prototype(scope));
        }
    }
    Ok(())
}

/// Mark which cell scopes are local to this node and record the node id. Every
/// other scope routes cross-node via `__do_call`.
fn inject_routing(scope: &mut v8::PinScope, owned: &[String], node: &str) -> Result<()> {
    let mut src = format!("(() => {{ __cell.node = {node:?};\n");
    for s in owned {
        src.push_str(&format!("__cell.owned[{s:?}] = true;\n"));
    }
    src.push_str("})();");
    let code = v8::String::new(scope, &src).unwrap();
    let s = v8::Script::compile(scope, code, None).ok_or_else(|| anyhow!("routing compile"))?;
    s.run(scope).ok_or_else(|| anyhow!("routing run"))?;
    Ok(())
}

fn inject_storage_compatibility(scope: &mut v8::PinScope, compat: Compat) -> Result<()> {
    let source = format!(
        "__cell.deleteAllDeletesAlarm = {};\n\
         __cell.compat.jsRpc = {};\n\
         __cell.compat.fetcherGetPutDelete = {};\n\
         __cell.compat.websocketStandardBinaryType = {};",
        compat.delete_all_deletes_alarm,
        compat.js_rpc,
        compat.fetcher_get_put_delete,
        compat.websocket_standard_binary_type,
    );
    let code = v8::String::new(scope, &source).unwrap();
    let script =
        v8::Script::compile(scope, code, None).ok_or_else(|| anyhow!("compatibility compile"))?;
    script
        .run(scope)
        .ok_or_else(|| anyhow!("compatibility run"))?;
    Ok(())
}

fn harness_env<'s>(scope: &mut v8::PinScope<'s, '_>) -> Result<v8::Local<'s, v8::Value>> {
    let ctx = scope.get_current_context();
    let global = ctx.global(scope);
    let ck = v8::String::new(scope, "__cell").unwrap();
    let cell = global
        .get(scope, ck.into())
        .unwrap()
        .to_object(scope)
        .unwrap();
    let ek = v8::String::new(scope, "env").unwrap();
    Ok(cell.get(scope, ek.into()).unwrap())
}

fn begin_event_context<'s>(scope: &mut v8::PinScope<'s, '_>) -> Result<v8::Local<'s, v8::Value>> {
    let global = scope.get_current_context().global(scope);
    let key = v8::String::new(scope, "__beginEvent").unwrap();
    let function: v8::Local<v8::Function> = global
        .get(scope, key.into())
        .ok_or_else(|| anyhow!("no __beginEvent"))?
        .try_into()
        .map_err(|_| anyhow!("__beginEvent is not a function"))?;
    let recv = v8::undefined(scope).into();
    function
        .call(scope, recv, &[])
        .ok_or_else(|| anyhow!("__beginEvent threw"))
}

fn end_event_context<'s>(
    scope: &mut v8::PinScope<'s, '_>,
) -> Result<Option<v8::Local<'s, v8::Promise>>> {
    let global = scope.get_current_context().global(scope);
    let key = v8::String::new(scope, "__endEvent").unwrap();
    let function: v8::Local<v8::Function> = global
        .get(scope, key.into())
        .ok_or_else(|| anyhow!("no __endEvent"))?
        .try_into()
        .map_err(|_| anyhow!("__endEvent is not a function"))?;
    let recv = v8::undefined(scope).into();
    let value = function
        .call(scope, recv, &[])
        .ok_or_else(|| anyhow!("__endEvent threw"))?;
    if value.is_null_or_undefined() {
        Ok(None)
    } else {
        Ok(Some(value.try_into().map_err(|_| {
            anyhow!("__endEvent did not return a promise")
        })?))
    }
}

// ---- module compilation + request/response marshalling ----

fn compile_module<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    name: &str,
    src: &str,
) -> Option<v8::Local<'s, v8::Module>> {
    let name = v8::String::new(scope, name)?;
    let source = v8::String::new(scope, src)?;
    let origin = v8::ScriptOrigin::new(
        scope,
        name.into(),
        0,
        0,
        false,
        0,
        None,
        false,
        false,
        true,
        None,
    );
    let mut src = v8::script_compiler::Source::new(source, Some(&origin));
    v8::script_compiler::compile_module(scope, &mut src)
}

thread_local! {
    // specifier -> pre-compiled stub module, consulted by `resolve_external`
    // while V8 statically links a real worker bundle's imports.
    static MODREG: RefCell<HashMap<String, v8::Global<v8::Module>>> =
        RefCell::new(HashMap::new());
}

/// Scan a bundle for its external (`cloudflare:*` / `node:*`) imports and the
/// named/default bindings pulled from each. V8 static-links these, so a stub
/// must provide exactly these names; namespace imports (`* as x`) need none.
/// Is `spec` an external builtin (`node:*`, `cloudflare:*`, or a bare node
/// builtin)? esbuild bundles every npm dep inline and leaves only builtins
/// external, so a remaining bare specifier is a node builtin.
fn is_external(spec: &str) -> bool {
    const BARE: &[&str] = &[
        "path",
        "os",
        "fs",
        "fs/promises",
        "crypto",
        "stream",
        "util",
        "util/types",
        "assert",
        "module",
        "process",
        "tty",
        "url",
        "v8",
        "constants",
        "buffer",
        "events",
        "zlib",
        "child_process",
        "http",
        "https",
        "http2",
        "net",
        "tls",
        "dns",
        "readline",
        "string_decoder",
        "querystring",
        "async_hooks",
        "perf_hooks",
        "worker_threads",
        "vm",
        "diagnostics_channel",
        "timers",
        "inspector",
        "dgram",
        "cluster",
        "punycode",
        "sqlite",
    ];
    if spec.starts_with("node:") || spec.starts_with("cloudflare:") {
        return true;
    }
    // bare builtin, or a builtin submodule like `stream/promises`
    BARE.contains(&spec) || BARE.contains(&spec.split('/').next().unwrap_or(""))
}

fn scan_external_imports(
    src: &str,
) -> std::collections::BTreeMap<String, std::collections::BTreeSet<String>> {
    let re = regex::Regex::new(r#"import\s+([^;]*?)\s*from\s*["']([^"']+)["']"#).unwrap();
    let braces = regex::Regex::new(r"\{([^}]*)\}").unwrap();
    let mut map: std::collections::BTreeMap<String, std::collections::BTreeSet<String>> =
        Default::default();
    for cap in re.captures_iter(src) {
        if !is_external(&cap[2]) {
            continue;
        }
        let clause = cap[1].trim();
        let set = map.entry(cap[2].to_string()).or_default();
        if let Some(b) = braces.captures(clause) {
            for part in b[1].split(',') {
                let name = part.split_whitespace().next().unwrap_or("");
                // A commented-out binding inside the braces is not a name.
                if !name.is_empty() && !name.starts_with("//") {
                    set.insert(name.to_string());
                }
            }
        }
        let head = clause
            .split(['{', '*'])
            .next()
            .unwrap_or("")
            .trim()
            .trim_end_matches(',')
            .trim();
        if !head.is_empty() {
            set.insert("default".into());
        } // default import
          // `import * as x` consumes the whole namespace; the stub must then
          // export the module's full surface, not just the scanned names.
        if clause.contains('*') {
            set.insert("*".into());
        }
    }
    map
}

/// A stub ES module for `spec`. `cloudflare:workers` binds the real DO base
/// class from the harness; every other builtin routes to a pass-through proxy
/// so evaluation never crashes on an unsupported node/cf API.
/// A module Cells really implements in JS. Its source is injected into the
/// generated stub module, and `register_stubs` only builds stubs for
/// specifiers a bundle actually imports — so an isolate that never imports it
/// pays nothing at load. `global` is what the source defines and doubles as
/// the redefinition guard when a bundle imports several aliases.
struct LazyModule {
    specs: &'static [&'static str],
    global: &'static str,
    source: &'static str,
}

const LAZY_MODULES: &[LazyModule] = &[
    LazyModule {
        specs: &["assert", "node:assert"],
        global: "__assertModule",
        source: include_str!("js/node_assert.js"),
    },
    LazyModule {
        specs: &["assert/strict", "node:assert/strict"],
        global: "__assertStrictModule",
        source: include_str!("js/node_assert.js"),
    },
    LazyModule {
        specs: &["timers/promises", "node:timers/promises"],
        global: "__timersPromises",
        source: include_str!("js/node_timers.js"),
    },
    LazyModule {
        specs: &["test", "node:test", "node:test/reporters"],
        global: "__nodeTest",
        source: include_str!("js/node_test.js"),
    },
    LazyModule {
        specs: &["util", "node:util"],
        global: "__utilModule",
        source: include_str!("js/node_util.js"),
    },
    // Same source, different namespace: node:util/types has the type
    // predicates as its named exports. The shared source defines both
    // globals, so either entry's guard covers the other.
    LazyModule {
        specs: &["util/types", "node:util/types"],
        global: "__utilTypesModule",
        source: include_str!("js/node_util.js"),
    },
    LazyModule {
        specs: &["events", "node:events"],
        global: "__eventsModule",
        source: include_str!("js/node_events.js"),
    },
    LazyModule {
        specs: &["path", "node:path", "path/posix", "node:path/posix"],
        global: "__pathModule",
        source: include_str!("js/node_path.js"),
    },
    // Same source; either entry's guard covers the other.
    LazyModule {
        specs: &["path/win32", "node:path/win32"],
        global: "__pathWin32Module",
        source: include_str!("js/node_path.js"),
    },
    // Shares its source with the `Buffer` LAZY_GLOBALS entry; the script
    // is self-guarded, so whichever seam runs first wins and the other
    // reuses the same classes.
    LazyModule {
        specs: &["buffer", "node:buffer"],
        global: "__buffer",
        source: include_str!("js/node_buffer.js"),
    },
    LazyModule {
        specs: &["crypto", "node:crypto"],
        global: "__cryptoModule",
        source: include_str!("js/node_crypto.js"),
    },
    LazyModule {
        specs: &["async_hooks", "node:async_hooks"],
        global: "__asyncHooksModule",
        source: include_str!("js/node_async_hooks.js"),
    },
    // One source defines the whole stream family; whichever entry runs
    // first, its guard covers the others.
    LazyModule {
        specs: &["stream", "node:stream"],
        global: "__streamModule",
        source: include_str!("js/node_stream.js"),
    },
    LazyModule {
        specs: &["stream/promises", "node:stream/promises"],
        global: "__streamPromises",
        source: include_str!("js/node_stream.js"),
    },
    LazyModule {
        specs: &["stream/web", "node:stream/web"],
        global: "__streamWeb",
        source: include_str!("js/node_stream.js"),
    },
    LazyModule {
        specs: &["stream/consumers", "node:stream/consumers"],
        global: "__streamConsumers",
        source: include_str!("js/node_stream.js"),
    },
];

/// A global whose definition only compiles when something reads the name.
/// One script may define several names; touching any of them installs them
/// all. A bundle that names none pays only the `SetLazyDataProperty` calls
/// at boot, so this is where a new global belongs unless the prelude has to
/// see it.
struct LazyGlobal {
    names: &'static [&'static str],
    source: &'static str,
}

const LAZY_GLOBALS: &[LazyGlobal] = &[
    LazyGlobal {
        names: &["IdentityTransformStream", "FixedLengthStream"],
        source: include_str!("js/identity_transform_stream.js"),
    },
    LazyGlobal {
        names: &["CompressionStream", "DecompressionStream"],
        source: include_str!("js/compression_streams.js"),
    },
    LazyGlobal {
        names: &[
            "ReadableByteStreamController",
            "ReadableStreamBYOBReader",
            "ReadableStreamBYOBRequest",
        ],
        source: include_str!("js/byte_streams.js"),
    },
    LazyGlobal {
        names: &["Buffer"],
        source: include_str!("js/node_buffer.js"),
    },
    LazyGlobal {
        names: &["setImmediate", "clearImmediate"],
        source: include_str!("js/set_immediate.js"),
    },
    LazyGlobal {
        names: &["URLPattern"],
        source: include_str!("js/url_pattern.js"),
    },
];

/// Runs once per group, the first time one of its names is read. V8 then
/// replaces the accessor with the returned value, so the script never
/// compiles twice.
fn lazy_global_getter(
    scope: &mut v8::PinScope,
    name: v8::Local<v8::Name>,
    args: v8::PropertyCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let index = args.data().integer_value(scope).unwrap();
    let group = &LAZY_GLOBALS[index as usize];
    let code = v8::String::new(scope, group.source).unwrap();
    let script = v8::Script::compile(scope, code, None).unwrap();
    let exports = script.run(scope).unwrap();
    let exports: v8::Local<v8::Object> = exports.try_into().unwrap();
    // The group's other names are still lazy; define them now or reading one
    // would compile the same script again. Not this one — V8 asserts the
    // property is still an accessor when the getter returns.
    let global = scope.get_current_context().global(scope);
    for other in group.names {
        let key = v8::String::new(scope, other).unwrap();
        if name.strict_equals(key.into()) {
            continue;
        }
        let value = exports.get(scope, key.into()).unwrap();
        global.create_data_property(scope, key.into(), value);
    }
    rv.set(exports.get(scope, name.into()).unwrap());
}

fn install_lazy_globals(scope: &mut v8::PinScope) -> Result<()> {
    let global = scope.get_current_context().global(scope);
    for (i, group) in LAZY_GLOBALS.iter().enumerate() {
        let data = v8::Integer::new(scope, i as i32);
        for name in group.names {
            let key = v8::String::new(scope, name).unwrap();
            global.set_lazy_data_property_with_data(
                scope,
                key.into(),
                lazy_global_getter,
                data.into(),
                v8::PropertyAttribute::NONE,
                v8::SideEffectType::HasSideEffect,
                v8::SideEffectType::HasSideEffect,
            );
        }
    }
    Ok(())
}

/// Globals already installed by the src/js/harness for partially supported
/// builtins. These cost every isolate, so new modules belong in
/// `LAZY_MODULES` instead.
fn eager_module_global(spec: &str) -> Option<&'static str> {
    Some(match spec {
        "fs" | "node:fs" | "fs/promises" | "node:fs/promises" => "globalThis.__fs",
        "zlib" | "node:zlib" => "globalThis.__zlibModule",
        _ => return None,
    })
}

fn stub_source(spec: &str, names: &std::collections::BTreeSet<String>) -> String {
    let cf = spec == "cloudflare:workers";
    let lazy = LAZY_MODULES.iter().find(|m| m.specs.contains(&spec));
    let eager = eager_module_global(spec);
    let base = if cf {
        "globalThis.__cf".to_string()
    } else if let Some(m) = lazy {
        format!("globalThis.{}", m.global)
    } else if let Some(global) = eager {
        global.to_string()
    } else {
        "globalThis.__nodeStub".to_string()
    };
    let real = cf || lazy.is_some() || eager.is_some();
    let mut out = String::new();
    if let Some(m) = lazy {
        out.push_str(&format!("if (!globalThis.{}) {{\n", m.global));
        out.push_str(m.source);
        out.push_str("}\n");
    }
    for n in names {
        if n == "*" {
            continue; // namespace imports take the full-surface path
        } else if n == "default" {
            out.push_str(&format!("export default {base};\n"));
        } else if cf
            && matches!(
                n.as_str(),
                "DurableObject" | "RpcTarget" | "WorkerEntrypoint" | "env" | "exports"
            )
        {
            out.push_str(&format!("export const {n} = globalThis.__cf.{n};\n"));
        } else if real {
            out.push_str(&format!("export const {n} = {base}.{n};\n"));
        } else {
            out.push_str(&format!("export const {n} = globalThis.__nodeStub;\n"));
        }
    }
    out
}

/// Compile one stub module per external specifier into MODREG. Run before
/// instantiating a real bundle; `resolve_external` then serves them.
fn register_stubs(scope: &mut v8::PinScope, src: &str, text: &[(String, String)]) {
    MODREG.with(|r| r.borrow_mut().clear());
    let reg = |spec: String, source: String, scope: &mut v8::PinScope| {
        if let Some(m) = compile_module(scope, &spec, &source) {
            MODREG.with(|r| r.borrow_mut().insert(spec, v8::Global::new(scope, m)));
        } else {
            tracing::warn!(%spec, "module stub failed to compile");
        }
    };
    for (spec, names) in scan_external_imports(src) {
        // `import * as x` binds the whole namespace, so the stub's exports
        // must be the module's full surface — probed from the backing
        // object, like dynamic import() — not just the scanned names.
        let s = if names.contains("*") {
            full_surface_source(scope, &spec, &names).unwrap_or_else(|| stub_source(&spec, &names))
        } else {
            stub_source(&spec, &names)
        };
        reg(spec, s, scope);
    }
    // sibling text modules (wrangler Text rule: `import md from './x.md'`)
    for (spec, content) in text {
        let s = format!(
            "export default {};",
            serde_json::to_string(content).unwrap()
        );
        reg(spec.clone(), s, scope);
    }
    tracing::debug!(
        mods = MODREG.with(|r| r.borrow().len()),
        "registered import stubs + text modules"
    );
}

/// Register a loaded worker's sibling JS modules (Worker Loader multi-module
/// bundles), in addition to the builtins `register_stubs` already added for the
/// main module. Each module is registered under its own name and `./name` so
/// both bare and relative sibling imports resolve; the whole graph links when
/// the main module instantiates. Any builtin a sibling imports (and the main
/// module did not) is stubbed too.
fn register_loader_modules(scope: &mut v8::PinScope, modules: &[(String, String)]) {
    for (_name, source) in modules {
        for (spec, names) in scan_external_imports(source) {
            if MODREG.with(|r| r.borrow().contains_key(&spec)) {
                continue;
            }
            let s = if names.contains("*") {
                full_surface_source(scope, &spec, &names)
                    .unwrap_or_else(|| stub_source(&spec, &names))
            } else {
                stub_source(&spec, &names)
            };
            if let Some(m) = compile_module(scope, &spec, &s) {
                MODREG.with(|r| r.borrow_mut().insert(spec, v8::Global::new(scope, m)));
            }
        }
    }
    for (name, source) in modules {
        let Some(m) = compile_module(scope, name, source) else {
            tracing::warn!(%name, "loader module failed to compile");
            continue;
        };
        let g = v8::Global::new(scope, m);
        MODREG.with(|r| {
            let mut reg = r.borrow_mut();
            reg.insert(name.clone(), g.clone());
            reg.insert(format!("./{name}"), g);
        });
    }
}

/// Module resolve callback: serve a registered stub for `cloudflare:*`/`node:*`.
/// celld bundles are single-file, so anything else is genuinely unresolvable.
fn resolve_external<'s>(
    context: v8::Local<'s, v8::Context>,
    specifier: v8::Local<'s, v8::String>,
    _a: v8::Local<'s, v8::FixedArray>,
    _r: v8::Local<'s, v8::Module>,
) -> Option<v8::Local<'s, v8::Module>> {
    v8::callback_scope!(unsafe scope, context);
    let spec = specifier.to_rust_string_lossy(scope);
    let m = MODREG.with(|r| r.borrow().get(&spec).map(|g| v8::Local::new(scope, g)));
    if m.is_none() {
        tracing::warn!(%spec, "resolve: no stub for specifier");
    }
    m
}

/// The (guarded setup script, backing-object expression) pair for a builtin
/// specifier, or None if `spec` is not one. The setup materializes a lazy
/// module's source at most once; the expression then names its global.
fn builtin_source(spec: &str) -> Option<(String, String)> {
    if spec == "cloudflare:workers" {
        return Some((String::new(), "globalThis.__cf".into()));
    }
    if let Some(m) = LAZY_MODULES.iter().find(|m| m.specs.contains(&spec)) {
        return Some((
            format!("if (!globalThis.{}) {{\n{}}}\n", m.global, m.source),
            format!("globalThis.{}", m.global),
        ));
    }
    if let Some(global) = eager_module_global(spec) {
        return Some((String::new(), global.to_string()));
    }
    is_external(spec).then(|| (String::new(), "globalThis.__nodeStub".to_string()))
}

/// Full-surface module source for a builtin: run the (guarded) setup, probe
/// the backing object's enumerable keys, and re-export each one. Serves both
/// `import * as x` stubs and dynamic import(). `extra` adds statically
/// scanned names the probe may have missed, so mixed named + namespace
/// imports still link. String export names sidestep identifier restrictions.
fn full_surface_source(
    scope: &mut v8::PinScope,
    spec: &str,
    extra: &std::collections::BTreeSet<String>,
) -> Option<String> {
    let (setup, expr) = builtin_source(spec)?;
    let probe = format!("{setup}JSON.stringify(Object.keys(Object({expr})))");
    let code = v8::String::new(scope, &probe)?;
    let script = v8::Script::compile(scope, code, None)?;
    let keys = script.run(scope)?.to_rust_string_lossy(scope);
    let mut names: Vec<String> = serde_json::from_str(&keys).unwrap_or_default();
    for name in extra {
        if name != "*" && !names.contains(name) {
            names.push(name.clone());
        }
    }
    let mut src = format!("const __b = {expr};\nexport default __b;\n");
    for (i, name) in names.iter().enumerate() {
        if name == "default" {
            continue;
        }
        let name = serde_json::to_string(name).unwrap();
        src.push_str(&format!(
            "const __e{i} = __b[{name}]; export {{ __e{i} as {name} }};\n"
        ));
    }
    Some(src)
}

/// `process.getBuiltinModule(id)`: the builtin's backing object, or
/// undefined for anything that is not a node builtin.
fn op_builtin_module(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let spec = args.get(0).to_rust_string_lossy(scope);
    if spec.starts_with("cloudflare:") {
        return; // not a node builtin
    }
    let Some((setup, expr)) = builtin_source(&spec) else {
        return;
    };
    let code = v8::String::new(scope, &format!("{setup}{expr}")).unwrap();
    if let Some(script) = v8::Script::compile(scope, code, None) {
        if let Some(value) = script.run(scope) {
            rv.set(value);
        }
    }
}

/// Evaluate (once per isolate) a full-namespace module for a builtin
/// specifier. Unlike the static stubs, whose exports are the statically
/// scanned import names, this re-exports every enumerable key of the
/// backing object — a dynamic import's consumer can reach for any of them.
/// Cached in MODREG under a `dyn:` key so later import() calls are a
/// table lookup.
fn dynamic_namespace<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    spec: &str,
) -> Result<v8::Local<'s, v8::Value>> {
    let key = format!("dyn:{spec}");
    let cached = MODREG.with(|r| r.borrow().get(&key).map(|g| v8::Local::new(scope, g)));
    if let Some(module) = cached {
        return Ok(module.get_module_namespace());
    }
    let src = full_surface_source(scope, spec, &Default::default())
        .ok_or_else(|| anyhow!("dynamic import of \"{spec}\" is not supported"))?;
    let module = compile_module(scope, spec, &src)
        .ok_or_else(|| anyhow!("dynamic module for {spec} did not compile"))?;
    module
        .instantiate_module(scope, resolve_external)
        .ok_or_else(|| anyhow!("dynamic module for {spec} did not link"))?;
    module
        .evaluate(scope)
        .ok_or_else(|| anyhow!("dynamic module for {spec} threw"))?;
    if module.get_status() != v8::ModuleStatus::Evaluated {
        return Err(anyhow!("dynamic module for {spec} failed to evaluate"));
    }
    MODREG.with(|r| r.borrow_mut().insert(key, v8::Global::new(scope, module)));
    Ok(module.get_module_namespace())
}

/// Dynamic `import()` host hook. Builtin specifiers resolve through the same
/// table static imports use; anything else rejects — celld bundles are
/// single-file, so there is nothing else to load.
fn host_import_module_dynamically<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    _host_defined_options: v8::Local<'s, v8::Data>,
    _resource_name: v8::Local<'s, v8::Value>,
    specifier: v8::Local<'s, v8::String>,
    _import_attributes: v8::Local<'s, v8::FixedArray>,
) -> Option<v8::Local<'s, v8::Promise>> {
    let resolver = v8::PromiseResolver::new(scope)?;
    let promise = resolver.get_promise(scope);
    let spec = specifier.to_rust_string_lossy(scope);
    let tc = std::pin::pin!(v8::TryCatch::new(scope));
    let tc = &mut tc.init();
    match dynamic_namespace(tc, &spec) {
        Ok(namespace) => {
            resolver.resolve(tc, namespace);
        }
        Err(error) => {
            let caught = tc.exception();
            tc.reset();
            let exception = caught.unwrap_or_else(|| {
                let message = v8::String::new(tc, &error.to_string()).unwrap();
                v8::Exception::error(tc, message)
            });
            resolver.reject(tc, exception);
        }
    }
    Some(promise)
}

/// Like `make_request`, but marks the request as incoming. Its signal is
/// registered only if the handler actually suspends, preserving the
/// synchronous request path.
fn make_incoming_request<'s>(
    tc: &mut v8::PinScope<'s, '_>,
    url: &str,
    method: &str,
    body: &[u8],
    headers: &[(String, String)],
) -> Result<v8::Local<'s, v8::Value>> {
    let global = tc.get_current_context().global(tc);
    let key = v8::String::new(tc, "__makeIncomingRequest").unwrap();
    let f: v8::Local<v8::Function> = global
        .get(tc, key.into())
        .ok_or_else(|| anyhow!("no __makeIncomingRequest"))?
        .try_into()
        .map_err(|_| anyhow!("not fn"))?;
    let url = v8::String::new(tc, url).unwrap();
    let method = v8::String::new(tc, method).unwrap();
    let body = bytes_value(tc, body.to_vec());
    let headers = v8::String::new(
        tc,
        &serde_json::to_string(headers).unwrap_or_else(|_| "[]".into()),
    )
    .unwrap();
    let recv = v8::undefined(tc).into();
    f.call(tc, recv, &[url.into(), method.into(), body, headers.into()])
        .ok_or_else(|| anyhow!("__makeIncomingRequest threw"))
}

fn register_incoming_request(
    tc: &mut v8::PinScope,
    request_id: RequestId,
    request: v8::Local<v8::Value>,
) -> Result<()> {
    let global = tc.get_current_context().global(tc);
    let key = v8::String::new(tc, "__registerIncomingRequest").unwrap();
    let function: v8::Local<v8::Function> = global
        .get(tc, key.into())
        .ok_or_else(|| anyhow!("no __registerIncomingRequest"))?
        .try_into()
        .map_err(|_| anyhow!("__registerIncomingRequest is not a function"))?;
    let id = v8::String::new(tc, &request_id_string(request_id)).unwrap();
    let recv = v8::undefined(tc).into();
    function
        .call(tc, recv, &[id.into(), request])
        .ok_or_else(|| anyhow!("__registerIncomingRequest threw"))?;
    Ok(())
}

fn finish_incoming_request(tc: &mut v8::PinScope, request_id: RequestId) {
    let global = tc.get_current_context().global(tc);
    let key = v8::String::new(tc, "__finishIncomingRequest").unwrap();
    let Some(value) = global.get(tc, key.into()) else {
        return;
    };
    let Ok(function) = value.try_cast::<v8::Function>() else {
        return;
    };
    let id = v8::String::new(tc, &request_id_string(request_id)).unwrap();
    let recv = v8::undefined(tc).into();
    let _ = function.call(tc, recv, &[id.into()]);
}

fn make_request<'s>(
    tc: &mut v8::PinScope<'s, '_>,
    url: &str,
    method: &str,
    body: &[u8],
    headers: &[(String, String)],
) -> Result<v8::Local<'s, v8::Value>> {
    let g = tc.get_current_context().global(tc);
    let k = v8::String::new(tc, "__makeRequest").unwrap();
    let f: v8::Local<v8::Function> = g.get(tc, k.into()).unwrap().try_into().unwrap();
    let u = v8::String::new(tc, url).unwrap();
    let m = v8::String::new(tc, method).unwrap();
    let b = bytes_value(tc, body.to_vec());
    let h = v8::String::new(tc, &serde_json::to_string(headers)?).unwrap();
    let recv = v8::undefined(tc).into();
    f.call(tc, recv, &[u.into(), m.into(), b, h.into()])
        .ok_or_else(|| anyhow!("makeRequest threw"))
}

fn read_response(
    scope: &mut v8::PinScope,
    ret: v8::Local<v8::Value>,
    cell_rx: Option<&std::sync::mpsc::Receiver<CellJob>>,
) -> Result<HttpResponse> {
    let ctx = scope.get_current_context();
    let global = ctx.global(scope);
    let key = v8::String::new(scope, "__readResponse").unwrap();
    let f: v8::Local<v8::Function> = global
        .get(scope, key.into())
        .ok_or_else(|| anyhow!("no __readResponse"))?
        .try_into()
        .map_err(|_| anyhow!("not fn"))?;
    let recv = v8::undefined(scope).into();
    let out = f
        .call(scope, recv, &[ret])
        .ok_or_else(|| anyhow!("readResponse threw"))?;
    let out = if let Ok(promise) = out.try_cast::<v8::Promise>() {
        run_loop(scope, promise, cell_rx)?
    } else {
        out
    };
    let out = out.to_object(scope).ok_or_else(|| anyhow!("not object"))?;
    let sk = v8::String::new(scope, "status").unwrap();
    let bk = v8::String::new(scope, "bodyBytes").unwrap();
    let tk = v8::String::new(scope, "bodyStreamId").unwrap();
    let hk = v8::String::new(scope, "headersJson").unwrap();
    let wk = v8::String::new(scope, "wsTargetJson").unwrap();
    let status = out
        .get(scope, sk.into())
        .and_then(|v| v.uint32_value(scope))
        .unwrap_or(200) as u16;
    // Body bytes cross as a Uint8Array copied directly out of V8 — no JSON
    // number array (which was ~4x the bytes to serialize + parse per response).
    let body = out
        .get(scope, bk.into())
        .and_then(|v| v8::Local::<v8::ArrayBufferView>::try_from(v).ok())
        .map(|view| {
            let mut buf = vec![0u8; view.byte_length()];
            view.copy_contents(&mut buf);
            buf
        })
        .unwrap_or_default();
    let stream_id = out
        .get(scope, tk.into())
        .and_then(|value| value.integer_value(scope))
        .unwrap_or(0)
        .max(0) as u64;
    let stream = if stream_id == 0 {
        None
    } else {
        http_streams()
            .lock()
            .unwrap()
            .remove(&stream_id)
            .and_then(|stream| stream.source)
            .map(http_chunk_stream)
    };
    if stream_id != 0 && stream.is_none() {
        return Err(anyhow!(
            "response stream {stream_id} is no longer available"
        ));
    }
    let headers = out
        .get(scope, hk.into())
        .and_then(|v| serde_json::from_str(&v.to_rust_string_lossy(scope)).ok())
        .unwrap_or_default();
    let ws_json = out
        .get(scope, wk.into())
        .map(|v| v.to_rust_string_lossy(scope))
        .unwrap_or_else(|| "null".into());
    let ws = serde_json::from_str(&ws_json).ok();
    if status == 101 {
        tracing::info!(%ws_json, has_target = ws.is_some(), "JS WebSocket response");
    }
    Ok(HttpResponse {
        status,
        body,
        stream,
        headers,
        ws,
        write_position: None,
    })
}

#[cfg(all(test, celld_internal_tests))]
fn init_engine_for_tests() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        v8::V8::set_flags_from_string("--expose-gc");
        Engine::init();
    });
}

/// Externally injected Web-platform tests.
#[cfg(all(test, celld_internal_tests))]
mod conformance_runtime_tests {
    include!(env!("CELLD_CONFORMANCE_JS_RUNTIME_TESTS"));
}

#[cfg(all(test, celld_internal_tests))]
mod conformance_web_platform_tests {
    include!(env!("CELLD_CONFORMANCE_WEB_PLATFORM_TESTS"));
}
