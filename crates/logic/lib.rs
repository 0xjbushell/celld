//! Clean-sheet celld decision core.
//!
//! [`on_event`] is the only way behavioral state advances. The production
//! executor and deterministic simulator both feed it events and perform the
//! returned effects. No adapter may mutate [`State`] directly.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

pub mod alarm;
pub mod cache;
pub mod dead_node_reconciliation;
pub mod peer;
pub mod pressure;
pub mod restore;
pub mod routing;
pub mod schedule;
pub mod sqlite;
pub mod wake;

pub type Ms = i64;

pub type CellId = String;
pub type NodeId = String;
pub type RequestId = u64;
pub type WebSocketId = u64;
pub type OpId = u64;
pub type Epoch = u64;

/// One runtime that this node can currently serve locally.
///
/// Presence is a read-only projection of the decision core, not a second
/// inventory maintained by an adapter. Keeping the epoch beside the cell ID
/// lets management and inspection traffic identify the exact fenced runtime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PresenceCell {
    pub id: CellId,
    pub epoch: Epoch,
}

/// Cumulative lifecycle decisions exposed to management from the same state
/// machine that made them. These are advisory counters, but they are not a
/// second shell-owned model and replay to the same values for the same events.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ActivitySnapshot {
    pub acquired: u64,
    pub proxied: u64,
    pub expired_owner_leases: u64,
    pub restored: u64,
    pub advanced_epochs: u64,
}

/// Management-facing lifecycle state derived atomically from [`State`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PresenceSnapshot {
    pub serving: bool,
    pub cells: Vec<PresenceCell>,
    pub activity: ActivitySnapshot,
    pub lazy_lease_shadow: LeaseLifecycleShadowBatch,
}

impl PresenceSnapshot {
    pub fn owned_cells(&self) -> usize {
        self.cells.len()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    /// Resident cells plus activation reservations may never exceed this.
    pub max_resident: usize,
    /// Complete nonresident routes which may be in flight at once.
    ///
    /// This is deliberately independent of the stateless Worker pool. A warm
    /// request consumes no activation slot, while a cold request holds one
    /// across ownership resolution, capacity waiting, restore, and publish.
    pub max_activations: usize,
    /// Evictions that may hold a durability proof in flight at once.
    ///
    /// A proof is a round trip to the bucket, so draining a node one cell at a
    /// time takes the number of cells times that latency -- a node shedding
    /// five hundred cells against a two hundred millisecond proof refuses
    /// admission for a minute and a half while it walks down. Bounded
    /// concurrency is what makes the walk down finish in a time anyone can
    /// reason about.
    pub max_hibernations: usize,
    /// Concurrent outbound WebSockets one cell may hold.
    ///
    /// Distinct from the node-wide pin budget, which counts *cells* held
    /// resident: one socket is enough to pin a cell, so that budget says
    /// nothing about how many a single cell may open. This bounds what one
    /// application can consume on its own behalf.
    pub max_outbound_websockets: usize,
    /// What an evicted cell's ownership record should say.
    ///
    /// Releasing it lets any node take the cell next, which is what makes a
    /// loaded node shed load rather than merely stop hosting it: keeping the
    /// record means every later request for that cell still routes here, to a
    /// node that already decided it has no room. Keeping it is right when the
    /// local hibernation snapshot is the point, because a same-node wake is a
    /// rename instead of a restore.
    pub ownership_on_evict: OwnershipOnEvict,
    /// Production executors require a live self-node lease before serving.
    /// Deterministic unit slices which do not exercise node authority can
    /// disable this explicitly.
    pub require_node_lease: bool,
    /// Exact cross-node request protocol this process can authenticate and
    /// understand. A live owner speaking another version is unavailable, not
    /// stale: incompatibility never authorizes takeover.
    pub peer_protocol: u16,
    /// How long an activation effect may remain outstanding before the core
    /// stops waiting for it.
    ///
    /// Without this a swallowed effect is invisible: no event ever arrives, no
    /// timer is watching, and the request waits forever while every piece of
    /// state remains perfectly consistent. celld shipped that and parked
    /// requests past ninety seconds. `None` restores the old behaviour, which
    /// only a test that wants to observe an indefinite wait should ask for.
    pub operation_deadline_ms: Option<u64>,
    /// How close an armed alarm may be before the cell stops being worth
    /// hibernating. Inside this window the wake would cost more than the
    /// residency it saves, so the cell is held.
    pub alarm_resident_ms: u64,
    /// How long a cell may sit unused before the node gives it back, with no
    /// pressure involved. `None` keeps every cell resident until something
    /// needs the room.
    pub idle_evict_ms: Option<u64>,
    /// Ceilings and low watermarks for load shedding.
    ///
    /// `max_resident` is a hard cap on reservations; this is the softer,
    /// resource-aware question of whether the node is overloaded at all.
    /// Without it a node has only a cell count to reason about, so it meets
    /// memory or CPU exhaustion by running into it rather than shedding.
    pub pressure: pressure::PressureConfig,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnerRecord {
    /// `None` is a deliberately released, fenced record. Epochs never reset.
    pub node: Option<NodeId>,
    pub epoch: Epoch,
    pub etag: String,
}

/// The routing and authority fields read from `nodes/<node>.json`.
///
/// The executor samples wall time and returns the verbatim record. The core,
/// not the storage adapter, decides whether it is live and routable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeLeaseRecord {
    pub node: NodeId,
    pub addr: String,
    pub expires_ms: u64,
    pub peer_protocol: u16,
    /// Per-process generation; production stores this in
    /// `ownership_index_generation`.
    pub generation: String,
    /// Object version observed by the read. Empty only in synthetic events.
    pub etag: String,
}

/// One advisory fleet-capacity observation returned by the storage shell.
///
/// Membership and expiry remain authoritative node-lease facts. Load is only
/// used to choose where an unowned cell should try to land; the chosen peer
/// must still atomically admit the handoff before it acquires ownership.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapacityPeer {
    pub node: NodeId,
    pub addr: String,
    pub expires_ms: u64,
    pub peer_protocol: u16,
    pub sampled_ms: u64,
    pub resident_cells: usize,
    pub host_websockets: usize,
    pub rss_bytes: u64,
    pub pressured: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeLeaseSpec {
    pub addr: String,
    pub peer_protocol: u16,
    pub generation: String,
    pub ttl_ms: u64,
    pub mode: NodeLeaseMode,
    pub linger_ms: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum NodeLeaseMode {
    #[default]
    Continuous,
    Shadow,
    Lazy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeLeaseAuthorityAction {
    Hold,
    Renew,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseLifecycleShadowSnapshot {
    pub mode: NodeLeaseMode,
    pub active_cells: usize,
    pub serving_cells: usize,
    pub idle_ms: u64,
    pub linger_ms: u64,
    pub lease_active: bool,
    pub elapsed_since_ok_ms: u64,
    pub elapsed_since_renew_ms: u64,
    pub ttl_ms: u64,
    pub shadow_release_reported: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeaseLifecycleShadowExpected {
    pub shadow_release: bool,
    pub authority_action: NodeLeaseAuthorityAction,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseLifecycleShadowDecision {
    pub sequence: u64,
    pub observed_at_ms: u64,
    pub snapshot: LeaseLifecycleShadowSnapshot,
    pub expected: LeaseLifecycleShadowExpected,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LeaseLifecycleShadowBatch {
    pub dropped: u64,
    pub decisions: Vec<LeaseLifecycleShadowDecision>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CasGuard {
    Absent,
    Match(String),
}

/// See [`Config::ownership_on_evict`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OwnershipOnEvict {
    /// Publish the cell as unowned so any node may take it next.
    #[default]
    Release,
    /// Keep the record, so a same-node wake can reuse the local snapshot.
    Sticky,
}

/// Why the node is halting. The shell writes this down: a process that
/// self-fences and exits without saying why leaves an operator with an exit
/// code and nothing else.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HaltReason {
    /// The node lease was not renewed inside its TTL, so this node can no
    /// longer prove it owns anything it is serving.
    NodeLeaseExpired,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CasOutcome {
    Applied,
    Rejected,
}

/// Observable result of a successful restore effect. A non-fresh activation
/// may still discover that no local or replicated database exists; the
/// adapter reports what happened instead of making the core infer I/O truth.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestoreOutcome {
    pub restored: bool,
    /// The alarm the restored database already had armed.
    ///
    /// A cell carries its alarm in its own SQLite, so a cell that arrives
    /// here from another node — or wakes cold — has one the isolate has not
    /// re-armed yet. Nothing else tells this node about it: the observer
    /// fires when a *running* isolate calls `setAlarm`, which is exactly the
    /// case this is not. Without it the mirror reads "no alarm" and every
    /// residency decision that consults it is wrong in the direction of
    /// shedding a cell that is about to fire.
    pub alarm: Option<RestoredAlarm>,
}

/// See [`RestoreOutcome::alarm`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestoredAlarm {
    pub at_ms: i64,
    /// Whether a durable wake entry already covers it. Read from the same
    /// flusher the observer consults, not assumed: claiming coverage this
    /// node cannot prove is how an alarm gets hibernated away.
    pub covered: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaseCasOutcome {
    Applied { etag: String },
    Rejected,
}

/// Deterministic timers are versioned effects, not implicit clock reads. A
/// stale firing from a replaced lease generation is harmless.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Timer {
    NodeLeaseRenew {
        generation: u64,
    },
    NodeLeaseFence {
        generation: u64,
    },
    CellAlarm {
        cell: CellId,
        generation: u64,
    },
    /// Fires if `op` is still outstanding. Identified by operation rather than
    /// by cell, so a completion that lands first simply leaves a stale timer
    /// that finds nothing to expire.
    OperationDeadline {
        op: OpId,
    },
}

/// Whether a failed operation definitely did not commit or may have committed
/// before its caller lost the response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failure {
    Definite,
    Ambiguous,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WebSocketKind {
    Hibernatable,
    Regular,
    /// A transport the cell opened itself with `new WebSocket(url)`. It pins
    /// its cell exactly as a regular one does -- a live transport cannot move
    /// with ownership -- but unlike an inbound client socket it is created by
    /// application code at a rate the application chooses, so how much of the
    /// node it may hold is budgeted.
    Outbound,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    StartNodeLease {
        now_ms: u64,
        spec: NodeLeaseSpec,
    },
    SelfNodeLeaseRead {
        op: OpId,
        now_ms: u64,
        now_mono_ms: u64,
        result: Result<Option<NodeLeaseRecord>, Failure>,
    },
    NodeLeaseCasCompleted {
        op: OpId,
        now_mono_ms: u64,
        result: Result<LeaseCasOutcome, Failure>,
    },
    TimerFired {
        timer: Timer,
        now_ms: u64,
        now_mono_ms: u64,
    },
    Request {
        request: RequestId,
        cell: CellId,
    },
    /// Production form of [`Event::Request`] with the sampled clocks needed
    /// to acquire an idle lazy node lease. Untimed model slices retain the
    /// compact variant above and use the state's last observed clocks.
    RequestAt {
        request: RequestId,
        cell: CellId,
        now_ms: u64,
        now_mono_ms: u64,
    },
    /// A peer selected this node as a possible landing place for an unowned
    /// cell. Unlike ordinary ingress, this request must refuse immediately if
    /// its advertised capacity has gone stale; waiting here would strand the
    /// forwarding node instead of letting it traverse another candidate.
    CapacityRequestAt {
        request: RequestId,
        cell: CellId,
        now_ms: u64,
        now_mono_ms: u64,
    },
    /// Reserve an idle resident isolate for a top-level Worker request. The
    /// shell falls back to the stateless pool when no resident is available;
    /// choosing and pinning a resident is lifecycle policy and therefore
    /// belongs in the replayable core.
    WorkerRequest {
        request: RequestId,
    },
    Cancel {
        request: RequestId,
    },
    ActivityFinished {
        request: RequestId,
    },
    /// A request that ran locally advanced its cell's committed WAL to
    /// `position`. Its response is withheld — the output gate — until that
    /// position is replicated, so celld never acknowledges a write it could
    /// still lose. Read-only requests emit [`Event::ActivityFinished`] instead
    /// and pay no durability latency. The epoch is not carried: the core reads
    /// it from the pinned cell's resident phase, which cannot change while the
    /// request holds it.
    Wrote {
        request: RequestId,
        position: u64,
    },
    /// The shell finished proving a gated write durable. `Ok(position)` reports
    /// the committed-write position the replica has *actually* proved durable;
    /// the core acknowledges only when it covers the gated write's position, so
    /// a replicator that proves less than it was asked to cannot force an early
    /// ack. `Err` failed the proof outright.
    DurableReached {
        op: OpId,
        result: Result<u64, Failure>,
    },
    WebSocketOpened {
        cell: CellId,
        websocket: WebSocketId,
        kind: WebSocketKind,
    },
    WebSocketClosed {
        cell: CellId,
        websocket: WebSocketId,
    },
    AlarmObserved {
        cell: CellId,
        at_ms: Option<i64>,
        covered: bool,
        now_ms: u64,
        now_mono_ms: u64,
    },
    AlarmFinished {
        op: OpId,
        now_ms: u64,
        now_mono_ms: u64,
        result: Result<(Option<i64>, bool), Failure>,
    },
    WakeHint {
        cell: CellId,
    },
    WakeHintAt {
        cell: CellId,
        now_ms: u64,
        now_mono_ms: u64,
    },
    OwnerRead {
        op: OpId,
        /// Wall-clock observation made when the ownership read completed. It
        /// bounds reuse of a shared owner-node lease without letting the core
        /// read a clock itself.
        now_ms: u64,
        result: Result<Option<OwnerRecord>, Failure>,
    },
    NodeLeaseRead {
        op: OpId,
        now_ms: u64,
        result: Result<Option<NodeLeaseRecord>, Failure>,
    },
    CapacityPeersRead {
        op: OpId,
        now_ms: u64,
        result: Result<Vec<CapacityPeer>, Failure>,
    },
    OwnerCasCompleted {
        op: OpId,
        result: Result<CasOutcome, Failure>,
    },
    OwnerReleased {
        op: OpId,
        result: Result<CasOutcome, Failure>,
    },
    RestoreCompleted {
        op: OpId,
        result: Result<RestoreOutcome, Failure>,
    },
    RuntimeStarted {
        op: OpId,
        result: Result<(), Failure>,
    },
    Published {
        op: OpId,
        result: Result<(), Failure>,
    },
    DurabilityChecked {
        op: OpId,
        result: Result<(), Failure>,
    },
    RuntimeStopped {
        op: OpId,
    },
    /// Policy input for this first slice. Later eviction selection emits this
    /// from the same core rather than an external caller choosing a victim.
    Evict {
        cell: CellId,
    },
    /// A periodic resource sample from the edge.
    ///
    /// The core never reads a clock or a proc file; the shell measures and
    /// hands the numbers over, and every decision that follows -- whether the
    /// node is overloaded, whether the latch stays hot, how far to shed --
    /// happens here where a schedule can replay it.
    LoadSampled {
        load: pressure::Load,
        now_mono_ms: u64,
    },
    /// Retire an exact cached remote route after a dispatch failure which is
    /// known not to have executed the request. Newer route generations are
    /// unaffected by delayed failure reports.
    InvalidateRemote {
        cell: CellId,
        node: NodeId,
        epoch: Epoch,
    },
    NodeFenced,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Route {
    Local,
    Remote {
        node: NodeId,
        addr: String,
        epoch: Epoch,
        peer_protocol: u16,
    },
}

/// Exact resident isolate reserved for a top-level Worker request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerRoute {
    pub cell: CellId,
    pub epoch: Epoch,
    /// A pending durability proof made stale by selecting this still-routable
    /// resident. The executor uses the ID only to release its effect waiter;
    /// a late completion is ignored by the core's phase check.
    pub retired_durability: Option<OpId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestError {
    NodeUnavailable,
    ResolveFailed,
    AcquireFailed,
    RestoreFailed,
    RuntimeFailed,
    PublishFailed,
    NodeFenced,
    PeerIncompatible,
    CapacityExhausted,
    /// A local write ran but its durability could not be proven, so the
    /// response must fail rather than falsely acknowledge the write.
    DurabilityUnproven,
}

/// Work performed outside the core. Every asynchronous effect is versioned;
/// completion events with an obsolete `op` are ignored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    ScheduleTimer {
        timer: Timer,
        at_mono_ms: u64,
    },
    ReadSelfNodeLease {
        op: OpId,
    },
    CasNodeLease {
        op: OpId,
        guard: CasGuard,
        record: NodeLeaseRecord,
    },
    /// Shadow mode proposed the lazy release but deliberately retained
    /// authority. The shell only records this decision; it performs no I/O.
    ObserveNodeLeaseShadowRelease {
        sequence: u64,
    },
    /// Lazy mode stopped renewing its idle node lease. The bucket object is
    /// intentionally left to expire; this effect is observability only.
    ObserveNodeLeaseReleased,
    ReadOwner {
        op: OpId,
        cell: CellId,
    },
    ReadNodeLease {
        op: OpId,
        cell: CellId,
        owner: NodeId,
    },
    /// Enumerate recent node leases and return their advisory load records.
    /// Listing and bounded parallel reads are adapter mechanics; selection,
    /// reservations, and exclusions are deterministic core policy.
    ReadCapacityPeers {
        op: OpId,
        cell: CellId,
    },
    CasOwner {
        op: OpId,
        cell: CellId,
        guard: CasGuard,
        epoch: Epoch,
        takeover: bool,
    },
    /// Bring the bucket's wake entry for this cell into line with its alarm.
    ///
    /// Emitted wherever the alarm settles, which is the only place that
    /// knows. An arm needs an entry; a consumed alarm needs its entry gone,
    /// or every later due scan finds a hint for an alarm that already fired
    /// and wakes a cell with nothing to do. `next_alarm_ms` is -1 when no
    /// alarm remains.
    ReconcileWakeEntry {
        cell: CellId,
        next_alarm_ms: i64,
    },
    /// Publish an evicted cell as unowned, keeping its epoch, so the next
    /// node to want it can take it without waiting for this one to notice.
    ReleaseOwner {
        op: OpId,
        cell: CellId,
        epoch: Epoch,
    },
    Restore {
        op: OpId,
        cell: CellId,
        spec: RestoreSpec,
    },
    StartRuntime {
        op: OpId,
        cell: CellId,
        epoch: Epoch,
    },
    Publish {
        op: OpId,
        cell: CellId,
        epoch: Epoch,
    },
    /// Prove that every commit made before this effect is recoverable from
    /// replica authority. Voluntary eviction cannot begin until this succeeds.
    EnsureDurable {
        op: OpId,
        cell: CellId,
        epoch: Epoch,
    },
    /// The output gate: prove the cell's committed `position` is replicated so
    /// a withheld local write response can be released. Unlike
    /// [`Effect::EnsureDurable`] this is per-request and changes no cell phase,
    /// so the cell keeps serving co-resident requests while one response waits.
    AwaitDurable {
        op: OpId,
        cell: CellId,
        epoch: Epoch,
        position: u64,
    },
    StopRuntime {
        op: OpId,
        cell: CellId,
        epoch: Epoch,
        cause: StopCause,
    },
    FireAlarm {
        op: OpId,
        cell: CellId,
        epoch: Epoch,
        scheduled_ms: i64,
    },
    Complete {
        request: RequestId,
        result: Result<Route, RequestError>,
    },
    /// Release a withheld local write response now that its durability is
    /// decided: `Ok` acknowledges the write, `Err` fails it. Emitted only for a
    /// request the shell held open via [`Event::Wrote`].
    ReleaseResponse {
        request: RequestId,
        result: Result<(), RequestError>,
    },
    /// Complete the synchronous resident-selection decision. `None` means
    /// the executor must use the ordinary stateless Worker pool.
    CompleteWorker {
        request: RequestId,
        route: Option<WorkerRoute>,
    },
    /// Refuse a transport the node cannot afford to hold. The shell closes it;
    /// the cell carries on without it.
    CloseWebSocket {
        cell: CellId,
        websocket: WebSocketId,
    },
    Halt {
        code: i32,
        reason: HaltReason,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopCause {
    Cleanup,
    /// `rebalance` means this eviction hands the cell to the fleet: its
    /// ownership record is released and the local replica is not worth
    /// keeping. An idle hibernation is the opposite on both counts -- it
    /// keeps the record so the next activation here renames the file into
    /// place instead of paying a full remote restore.
    Evict {
        rebalance: bool,
    },
    Fence,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Activation {
    Claim(Claim),
    Restore(RestoreSpec),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ColdStart {
    ReadOwner,
    Restore(RestoreSpec),
}

/// Facts already decided by ownership resolution that select a safe restore
/// source. The effect adapter must not rediscover or guess these.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreSpec {
    pub epoch: Epoch,
    /// This activation conditionally created epoch one, so no replica can
    /// precede it.
    pub fresh: bool,
    /// Ownership was seized from a different node (or a released record), so
    /// a previous local hibernation cache is not authoritative.
    pub took_over: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Claim {
    pub guard: CasGuard,
    pub epoch: Epoch,
    pub takeover: bool,
    /// Ambiguous acquires already reconciled for this claim.
    ///
    /// An ambiguous compare-and-swap is re-read rather than retried blindly,
    /// which is correct — it may have applied. But the re-read leads to
    /// another acquire, and if that is ambiguous too the cycle repeats. With
    /// no bound a persistently unanswered store turns a request that used to
    /// hang into one that spins, which is worse: it burns a slot and an
    /// object-store budget forever instead of merely waiting.
    pub reconciles: u32,
}

/// How many times one claim may reconcile an ambiguous acquire before the
/// request is failed and the caller left to decide. Small: each pass is a full
/// read plus a write, and a store answering ambiguously three times running is
/// not about to start answering.
pub const MAX_ACQUIRE_RECONCILES: u32 = 3;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Phase {
    Dormant,
    /// Cold demand queued behind `max_activations`, before any I/O begins.
    WaitingActivation,
    ReadingOwner {
        op: OpId,
    },
    ReadingNodeLease {
        op: OpId,
        owner: OwnerRecord,
    },
    ReadingCapacity {
        op: OpId,
        claim: Claim,
    },
    WaitingCapacity,
    Acquiring {
        op: OpId,
        claim: Claim,
    },
    ReconcilingAcquire {
        op: OpId,
        claim: Claim,
    },
    Restoring {
        op: OpId,
        spec: RestoreSpec,
    },
    Starting {
        op: OpId,
        epoch: Epoch,
    },
    Publishing {
        op: OpId,
        epoch: Epoch,
    },
    /// The runtime remains published and routable while replica durability is
    /// being proved. A failed or ambiguous proof returns to `Resident`.
    EnsuringDurability {
        op: OpId,
        epoch: Epoch,
    },
    Cleaning {
        op: OpId,
        epoch: Epoch,
        cause: StopCause,
    },
    OwnedDormant {
        epoch: Epoch,
    },
    Resident {
        epoch: Epoch,
    },
    Remote {
        node: NodeId,
        addr: String,
        epoch: Epoch,
        peer_protocol: u16,
        /// Present only for epoch-zero capacity candidates. It identifies the
        /// exact advisory sample a refusal disproved.
        capacity_sampled_ms: Option<u64>,
    },
    Fenced,
}

/// One local write response held open by the output gate.
#[derive(Clone, Debug, PartialEq, Eq)]
struct GatedWrite {
    request: RequestId,
    cell: CellId,
    epoch: Epoch,
    position: u64,
}

#[derive(Clone, Debug)]
struct Cell {
    phase: Phase,
    requests: BTreeSet<RequestId>,
    websockets: BTreeMap<WebSocketId, WebSocketKind>,
    waiting_for: Option<Activation>,
    waiting_activation: Option<ColdStart>,
    /// The in-flight release of this cell's ownership record, if any.
    releasing: Option<OpId>,
    /// Whether the eviction now under way hands this cell to the fleet.
    evict_rebalance: bool,
    alarm: Option<AlarmState>,
    alarm_wake: bool,
    /// When this cell's hibernation was last refused, if it has been.
    ///
    /// A cell that cannot prove its replica durable goes back to residency,
    /// and being cold is exactly why nobody registered it -- so it settles at
    /// the head of the eviction order and is chosen again on the next pass,
    /// and the one after. Enough of those and the node has no shed candidate
    /// it will ever succeed with. Recording the refusal lets the order prefer
    /// cells that have not just failed, while still coming back to them when
    /// they are all that is left.
    hibernation_refused_mono_ms: Option<u64>,
    /// When this cell last did something, on the remembered monotonic clock.
    ///
    /// Eviction order is the whole reason this exists. Without it the shed
    /// candidate is whichever cell sorts first by id, so a node under
    /// sustained pressure evicts its alphabetically-first cell over and over
    /// -- restoring and shedding the busiest cell on the node while an idle
    /// one further down the alphabet is never touched.
    last_used_mono_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum AlarmState {
    Armed {
        at_ms: i64,
        generation: u64,
        covered: bool,
    },
    Firing {
        op: OpId,
        at_ms: i64,
        generation: u64,
        covered: bool,
    },
}

#[derive(Clone, Debug)]
struct HeldNodeLease {
    spec: NodeLeaseSpec,
    record: NodeLeaseRecord,
    last_ok_mono_ms: u64,
    last_attempt_mono_ms: u64,
    timer_generation: u64,
    inactive_since_mono_ms: Option<u64>,
    shadow_release_reported: bool,
}

#[derive(Clone, Debug)]
struct PendingNodeLease {
    spec: NodeLeaseSpec,
    desired: NodeLeaseRecord,
    prior: Option<HeldNodeLease>,
    /// This read follows a write whose result was ambiguous. It may prove the
    /// desired record landed, but must not turn a failed create into an
    /// unbounded read/write loop.
    readback_only: bool,
}

#[derive(Clone, Debug)]
enum NodeAuthority {
    Unstarted,
    /// Lazy lifecycle with no live lease. The spec is retained so the next
    /// local dependency can acquire before touching cell ownership.
    Inactive(NodeLeaseSpec),
    Reading {
        op: OpId,
        pending: PendingNodeLease,
    },
    Writing {
        op: OpId,
        pending: PendingNodeLease,
    },
    Held(HeldNodeLease),
    Failed,
    Fenced,
}

impl Default for Cell {
    fn default() -> Self {
        Self {
            phase: Phase::Dormant,
            requests: BTreeSet::new(),
            websockets: BTreeMap::new(),
            waiting_for: None,
            waiting_activation: None,
            releasing: None,
            evict_rebalance: false,
            alarm: None,
            alarm_wake: false,
            hibernation_refused_mono_ms: None,
            last_used_mono_ms: 0,
        }
    }
}

/// All authoritative coordination state for the first vertical slice.
pub struct State {
    node: NodeId,
    config: Config,
    fenced: bool,
    next_op: OpId,
    next_timer_generation: u64,
    cells: BTreeMap<CellId, Cell>,
    request_cells: BTreeMap<RequestId, CellId>,
    /// Local routes handed to the executor but not yet reported complete.
    /// These are eviction pins: the adapter may still be running user code.
    active_requests: BTreeMap<RequestId, CellId>,
    /// Local write responses withheld by the output gate until their cell is
    /// proven durable to the written position, keyed by the durability op. An
    /// open gate makes its cell active, so the cell cannot be evicted
    /// underneath it; a fence drains these to a failed response so a write is
    /// never acknowledged after the node loses authority.
    gated_writes: BTreeMap<OpId, GatedWrite>,
    /// Requests whose activity ended while a write of theirs was still on the
    /// output gate. The pin outlives the activity: it keeps the cell from being
    /// evicted under the gate, and keeps a later write of the same request able
    /// to open one. See `activity_finished`.
    gate_pinned: BTreeSet<RequestId>,
    /// Last resident selected for top-level Worker execution. Cell IDs are
    /// ordered, so this is enough to replay a fair round-robin without shell
    /// atomics or registry iteration order leaking into behavior.
    worker_cursor: Option<CellId>,
    activity: ActivitySnapshot,
    activation_waiters: VecDeque<CellId>,
    /// Cells holding a complete cold-route admission. Keeping this explicit,
    /// instead of inferring it from executor tasks, makes the concurrency bound
    /// part of the replayable state machine.
    activation_permits: BTreeSet<CellId>,
    /// Cells with an eviction in flight -- from the durability proof through
    /// the runtime stop. Explicit for the same reason as the activation
    /// permits: the bound belongs in the replayable state machine rather than
    /// being implied by how many executor tasks happen to exist.
    hibernation_permits: BTreeSet<CellId>,
    /// Cells waiting for a residency slot, in arrival order. FIFO is the
    /// whole admission policy: waking every waiter on a release and letting
    /// them race is unfair by construction — under sustained eviction a
    /// waiter can time out while thousands of slots are freed around it.
    /// The queue converts "eventually, probably" into a bound: a waiter with
    /// `k` waiters ahead of it is admitted within `k` releases, so no
    /// arrival pattern can starve it.
    capacity_waiters: VecDeque<CellId>,
    /// Requests received as epoch-zero capacity handoffs. Keeping the mode on
    /// the request rather than in the HTTP adapter makes the admit/refuse race
    /// atomic with every other lifecycle transition.
    capacity_requests: BTreeSet<RequestId>,
    /// Reservations made against each unchanged advisory sample. Concurrent
    /// lookups are applied one event at a time, so projected load cannot pick
    /// the same advertised final slot twice by accident.
    capacity_reservations: BTreeMap<NodeId, usize>,
    capacity_samples: BTreeMap<NodeId, u64>,
    /// A refusal disproves exactly one load sample. The node becomes eligible
    /// again only after its lease advertises a newer sample.
    capacity_rejections: BTreeMap<NodeId, u64>,
    /// Live routing authority is shared by every cell owned by the same node.
    /// Keeping that cache here makes expiry and invalidation deterministic,
    /// rather than an invisible executor optimization.
    node_lease_cache: BTreeMap<NodeId, NodeLeaseRecord>,
    /// Start/publish effects invalidated by fencing may still commit. Their
    /// late completion must trigger compensating cleanup, not be ignored.
    retired_runtime_ops: BTreeMap<OpId, (CellId, Epoch)>,
    node_authority: NodeAuthority,
    /// Requests admitted while a lazy node is inactive. They are not entered
    /// into per-cell routing state until the node lease is authoritative, so
    /// a failed lease acquisition cannot create cell ownership.
    node_lease_waiters: BTreeMap<RequestId, CellId>,
    /// Alarm/wake demand has no request to complete, but it obeys the same
    /// acquire-before-ownership rule.
    node_wake_waiters: BTreeSet<CellId>,
    lazy_lease_shadow: LeaseLifecycleShadowBatch,
    next_shadow_sequence: u64,
    /// The most recent wall-clock instant any event carried. Alarms are wall
    /// clock, so judging whether one is imminent needs this rather than the
    /// monotonic reading next to it.
    now_ms: u64,
    /// The most recent monotonic instant any event carried. Not a clock read:
    /// the core never asks what time it is, it remembers what it was told, so
    /// a handler that was not handed a timestamp can still arm a deadline
    /// relative to the event being processed.
    now_mono_ms: u64,
    /// How far down the current sample asked the node to shed.
    ///
    /// The walk down runs at eviction speed rather than sample speed: each
    /// completed eviction starts the next while residency is still above this
    /// floor. Without one it is a cell per sample, so a node fifteen over its
    /// watermark refuses work for fifteen sampling periods. Recomputed every
    /// sample, so a resource trigger comes down by a proportion of what was
    /// last measured rather than aiming at a cell count that means nothing to
    /// it.
    shed_floor: usize,
    /// The shedding latch. celld kept this in the executor, which meant the
    /// hysteresis -- the part with actual behaviour -- was the one piece the
    /// simulation could not reach. It is carried here so a sample
    /// sequence is replayable.
    shedding: bool,
    /// Which resource is holding the latch, for the effect the shell logs.
    shed_reason: Option<&'static str>,
}

impl State {
    pub fn new(node: impl Into<NodeId>, config: Config) -> Self {
        assert!(
            config.max_hibernations > 0,
            "max_hibernations must be positive"
        );
        assert!(
            config.max_activations > 0,
            "max_activations must be positive"
        );
        Self {
            node: node.into(),
            config,
            fenced: false,
            next_op: 1,
            next_timer_generation: 1,
            cells: BTreeMap::new(),
            request_cells: BTreeMap::new(),
            active_requests: BTreeMap::new(),
            gated_writes: BTreeMap::new(),
            gate_pinned: BTreeSet::new(),
            worker_cursor: None,
            activity: ActivitySnapshot::default(),
            activation_waiters: VecDeque::new(),
            activation_permits: BTreeSet::new(),
            hibernation_permits: BTreeSet::new(),
            capacity_waiters: VecDeque::new(),
            capacity_requests: BTreeSet::new(),
            capacity_reservations: BTreeMap::new(),
            capacity_samples: BTreeMap::new(),
            capacity_rejections: BTreeMap::new(),
            node_lease_cache: BTreeMap::new(),
            retired_runtime_ops: BTreeMap::new(),
            node_authority: NodeAuthority::Unstarted,
            node_lease_waiters: BTreeMap::new(),
            node_wake_waiters: BTreeSet::new(),
            lazy_lease_shadow: LeaseLifecycleShadowBatch::default(),
            next_shadow_sequence: 1,
            now_ms: 0,
            now_mono_ms: 0,
            shed_floor: 0,
            shedding: false,
            shed_reason: None,
        }
    }

    pub fn node(&self) -> &str {
        &self.node
    }

    pub fn is_fenced(&self) -> bool {
        self.fenced
    }

    pub fn node_authoritative(&self) -> bool {
        if !self.config.require_node_lease {
            return !self.fenced;
        }
        match &self.node_authority {
            NodeAuthority::Held(_) => true,
            NodeAuthority::Reading { pending, .. } | NodeAuthority::Writing { pending, .. } => {
                pending.prior.is_some()
            }
            _ => false,
        }
    }

    /// Whether the process can accept new traffic. An idle lazy node is ready
    /// even though it intentionally holds no authority: a DO request first
    /// acquires the lease, while stateless Worker traffic needs none.
    pub fn ready_to_serve(&self) -> bool {
        if self.fenced {
            return false;
        }
        self.node_authoritative()
            || matches!(
                &self.node_authority,
                NodeAuthority::Inactive(NodeLeaseSpec {
                    mode: NodeLeaseMode::Lazy,
                    ..
                }) | NodeAuthority::Reading {
                    pending: PendingNodeLease {
                        spec: NodeLeaseSpec {
                            mode: NodeLeaseMode::Lazy,
                            ..
                        },
                        prior: None,
                        ..
                    },
                    ..
                } | NodeAuthority::Writing {
                    pending: PendingNodeLease {
                        spec: NodeLeaseSpec {
                            mode: NodeLeaseMode::Lazy,
                            ..
                        },
                        prior: None,
                        ..
                    },
                    ..
                }
            )
    }

    pub fn phase(&self, cell: &str) -> Option<&Phase> {
        self.cells.get(cell).map(|cell| &cell.phase)
    }

    /// Which resource is currently holding the shedding latch, if any.
    ///
    /// A node that is refusing work should be able to say why; without this
    /// the operator sees only that admissions stopped.
    pub fn shed_reason(&self) -> Option<&'static str> {
        self.shed_reason
    }

    /// Whether this node is currently shedding, for peers ranking it as a
    /// placement target. A node that says no while walking down invites the
    /// work it is trying to get rid of.
    pub fn shedding(&self) -> bool {
        self.shedding
    }

    /// Host-side sockets open across every cell. Each one pins its cell
    /// against hibernation, so a node holding many is a poor landing place
    /// even when its residency looks unremarkable.
    pub fn host_websockets(&self) -> usize {
        self.cells.values().map(|cell| cell.websockets.len()).sum()
    }

    /// Does this cell still hold that socket? False when the core declined
    /// it, so the shell can answer the opener instead of closing underneath.
    pub fn holds_websocket(&self, id: &str, websocket: WebSocketId) -> bool {
        self.cells
            .get(id)
            .is_some_and(|cell| cell.websockets.contains_key(&websocket))
    }

    pub fn occupied(&self) -> usize {
        self.cells
            .values()
            .filter(|cell| phase_occupies_capacity(&cell.phase))
            .count()
    }

    pub fn residents(&self) -> Vec<CellId> {
        self.cells
            .iter()
            .filter(|(_, cell)| {
                matches!(
                    cell.phase,
                    Phase::Resident { .. } | Phase::EnsuringDurability { .. }
                )
            })
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Return the exact management projection for the current event boundary.
    /// Activations are not visible until publication; a runtime being held for
    /// durability remains visible because it is still published and routable.
    pub fn presence_snapshot(&self) -> PresenceSnapshot {
        let cells = self
            .cells
            .iter()
            .filter_map(|(id, cell)| match cell.phase {
                Phase::Resident { epoch } | Phase::EnsuringDurability { epoch, .. } => {
                    Some(PresenceCell {
                        id: id.clone(),
                        epoch,
                    })
                }
                _ => None,
            })
            .collect();
        PresenceSnapshot {
            serving: self.ready_to_serve(),
            cells,
            activity: self.activity,
            lazy_lease_shadow: self.lazy_lease_shadow.clone(),
        }
    }

    pub fn waiting(&self) -> Vec<CellId> {
        self.capacity_waiters.iter().cloned().collect()
    }

    pub fn activation_waiting(&self) -> Vec<CellId> {
        self.activation_waiters.iter().cloned().collect()
    }

    pub fn activating(&self) -> usize {
        self.activation_permits.len()
    }

    /// Cheap internal consistency gate run by both executors after every event.
    pub fn validate(&self) -> Result<(), String> {
        if self.occupied() > self.config.max_resident {
            return Err(format!(
                "occupied {} exceeds ceiling {}",
                self.occupied(),
                self.config.max_resident
            ));
        }
        if self.hibernation_permits.len() > self.config.max_hibernations {
            return Err(format!(
                "hibernating {} exceeds ceiling {}",
                self.hibernation_permits.len(),
                self.config.max_hibernations
            ));
        }
        if self.activation_permits.len() > self.config.max_activations {
            return Err(format!(
                "activating {} exceeds ceiling {}",
                self.activation_permits.len(),
                self.config.max_activations
            ));
        }

        let mut activation_queued = BTreeSet::new();
        for id in &self.activation_waiters {
            if !activation_queued.insert(id) {
                return Err(format!("activation waiter {id:?} is queued twice"));
            }
            let Some(cell) = self.cells.get(id) else {
                return Err(format!("activation waiter {id:?} has no cell state"));
            };
            if cell.phase != Phase::WaitingActivation || cell.waiting_activation.is_none() {
                return Err(format!("activation waiter {id:?} is not waiting"));
            }
            if self.activation_permits.contains(id) {
                return Err(format!("activation waiter {id:?} also holds a permit"));
            }
        }
        for id in &self.activation_permits {
            let Some(cell) = self.cells.get(id) else {
                return Err(format!("activation permit {id:?} has no cell state"));
            };
            if !phase_holds_activation(&cell.phase) {
                return Err(format!(
                    "activation permit {id:?} is held in terminal phase {:?}",
                    cell.phase
                ));
            }
        }

        let mut queued = BTreeSet::new();
        for id in &self.capacity_waiters {
            if !queued.insert(id) {
                return Err(format!("capacity waiter {id:?} is queued twice"));
            }
            let Some(cell) = self.cells.get(id) else {
                return Err(format!("capacity waiter {id:?} has no cell state"));
            };
            if cell.phase != Phase::WaitingCapacity || cell.waiting_for.is_none() {
                return Err(format!("capacity waiter {id:?} is not waiting"));
            }
        }
        for (id, cell) in &self.cells {
            let is_activation_queued = activation_queued.contains(id);
            if (cell.phase == Phase::WaitingActivation) != is_activation_queued {
                return Err(format!("cell {id:?} activation queue and phase disagree"));
            }
            let is_queued = queued.contains(id);
            if (cell.phase == Phase::WaitingCapacity) != is_queued {
                return Err(format!("cell {id:?} queue and phase disagree"));
            }
            for request in &cell.requests {
                if self.request_cells.get(request) != Some(id) {
                    return Err(format!(
                        "request {request} index disagrees with cell {id:?}"
                    ));
                }
            }
        }
        for (request, id) in &self.request_cells {
            if !self
                .cells
                .get(id)
                .is_some_and(|cell| cell.requests.contains(request))
            {
                return Err(format!("request index {request} has no matching waiter"));
            }
        }
        for (request, id) in &self.node_lease_waiters {
            if self.request_cells.contains_key(request)
                || self.active_requests.contains_key(request)
            {
                return Err(format!(
                    "request {request} waits for node authority and is already routed"
                ));
            }
            if id.is_empty() {
                return Err(format!("request {request} has an empty node-lease target"));
            }
        }
        for (request, id) in &self.active_requests {
            if self.request_cells.contains_key(request) {
                return Err(format!("request {request} is both pending and active"));
            }
            if !self.cells.get(id).is_some_and(|cell| {
                matches!(
                    cell.phase,
                    Phase::Resident { .. } | Phase::EnsuringDurability { .. }
                )
            }) {
                return Err(format!(
                    "active request {request} has no resident cell {id:?}"
                ));
            }
        }
        for (op, gate) in &self.gated_writes {
            // A held write keeps its request pinned, so the cell cannot be
            // evicted underneath the gate and a later write of the same
            // request can still find it.
            if self.active_requests.get(&gate.request) != Some(&gate.cell) {
                return Err(format!(
                    "gated write op {op} for request {} is not pinned on {:?}",
                    gate.request, gate.cell
                ));
            }
        }
        for (id, cell) in &self.cells {
            if matches!(cell.alarm, Some(AlarmState::Firing { .. }))
                && !matches!(cell.phase, Phase::Resident { .. })
            {
                return Err(format!("cell {id:?} is firing an alarm while not resident"));
            }
            if cell
                .websockets
                .values()
                .any(|kind| matches!(kind, WebSocketKind::Regular | WebSocketKind::Outbound))
                && !matches!(
                    cell.phase,
                    Phase::Resident { .. } | Phase::EnsuringDurability { .. }
                )
            {
                return Err(format!(
                    "cell {id:?} has a live transport while not resident"
                ));
            }
        }
        Ok(())
    }

    fn op(&mut self) -> OpId {
        let op = self.next_op;
        self.next_op = self.next_op.checked_add(1).expect("operation id exhausted");
        op
    }

    fn has_capacity(&self) -> bool {
        self.occupied() < self.config.max_resident
            && pressure::may_admit(
                self.occupied(),
                0,
                self.config.pressure.resident_high,
                self.shedding,
            )
    }

    pub fn is_active(&self, id: &str) -> bool {
        self.active_requests.values().any(|cell| cell == id)
            || self.cells.get(id).is_some_and(|cell| {
                cell.websockets
                    .values()
                    .any(|kind| matches!(kind, WebSocketKind::Regular | WebSocketKind::Outbound))
            })
    }

    pub fn websocket_count(&self, id: &str) -> usize {
        self.cells.get(id).map_or(0, |cell| cell.websockets.len())
    }

    /// Distinct local cell lifecycles which require this process's node
    /// authority. Hibernated cells do not pin a lazy lease by themselves, but
    /// a host-held WebSocket does even after its isolate has hibernated.
    fn node_lease_dependency_count(&self) -> usize {
        let mut cells = BTreeSet::new();
        cells.extend(self.node_lease_waiters.values().cloned());
        cells.extend(self.node_wake_waiters.iter().cloned());
        for (id, cell) in &self.cells {
            if phase_depends_on_node_lease(&cell.phase) || !cell.websockets.is_empty() {
                cells.insert(id.clone());
            }
        }
        cells.len()
    }

    /// Update idle/liveness bookkeeping after every state transition. This is
    /// deliberately derived from core state rather than an executor-owned
    /// active-cell counter, which used to let the model and production drift.
    fn update_node_lease_dependencies(&mut self, now_mono_ms: u64) {
        let active = self.node_lease_dependency_count();
        let held = match &mut self.node_authority {
            NodeAuthority::Held(held) => Some(held),
            NodeAuthority::Reading { pending, .. } | NodeAuthority::Writing { pending, .. } => {
                pending.prior.as_mut()
            }
            _ => None,
        };
        let Some(held) = held else {
            return;
        };
        if active == 0 {
            held.inactive_since_mono_ms.get_or_insert(now_mono_ms);
        } else {
            held.inactive_since_mono_ms = None;
            held.shadow_release_reported = false;
        }
    }

    fn record_shadow_release(
        &mut self,
        held: &HeldNodeLease,
        now_ms: u64,
        now_mono_ms: u64,
        effects: &mut Vec<Effect>,
    ) {
        const MAX_SHADOW_DECISIONS: usize = 8;
        let sequence = self.next_shadow_sequence;
        self.next_shadow_sequence = self.next_shadow_sequence.saturating_add(1);
        if self.lazy_lease_shadow.decisions.len() == MAX_SHADOW_DECISIONS {
            self.lazy_lease_shadow.decisions.remove(0);
            self.lazy_lease_shadow.dropped = self.lazy_lease_shadow.dropped.saturating_add(1);
        }
        let idle_ms = held
            .inactive_since_mono_ms
            .map_or(0, |inactive| now_mono_ms.saturating_sub(inactive));
        self.lazy_lease_shadow
            .decisions
            .push(LeaseLifecycleShadowDecision {
                sequence,
                observed_at_ms: now_ms,
                snapshot: LeaseLifecycleShadowSnapshot {
                    mode: NodeLeaseMode::Shadow,
                    active_cells: self.node_lease_dependency_count(),
                    serving_cells: self.residents().len(),
                    idle_ms,
                    linger_ms: held.spec.linger_ms,
                    lease_active: true,
                    elapsed_since_ok_ms: now_mono_ms.saturating_sub(held.last_ok_mono_ms),
                    elapsed_since_renew_ms: now_mono_ms.saturating_sub(held.last_attempt_mono_ms),
                    ttl_ms: held.spec.ttl_ms,
                    shadow_release_reported: false,
                },
                expected: LeaseLifecycleShadowExpected {
                    shadow_release: true,
                    authority_action: NodeLeaseAuthorityAction::Renew,
                },
            });
        effects.push(Effect::ObserveNodeLeaseShadowRelease { sequence });
    }

    fn complete_request(
        &mut self,
        id: &str,
        request: RequestId,
        result: Result<Route, RequestError>,
        effects: &mut Vec<Effect>,
    ) {
        self.capacity_requests.remove(&request);
        match &result {
            Ok(Route::Local) => {
                self.active_requests.insert(request, id.to_string());
                let now = self.now_mono_ms;
                if let Some(cell) = self.cells.get_mut(id) {
                    cell.last_used_mono_ms = now;
                }
            }
            Ok(Route::Remote { .. }) => {
                self.activity.proxied = self.activity.proxied.saturating_add(1);
            }
            Err(_) => {}
        }
        effects.push(Effect::Complete { request, result });
    }

    fn record_acquisition(&mut self, spec: &RestoreSpec) {
        self.activity.acquired = self.activity.acquired.saturating_add(1);
        if spec.took_over {
            self.activity.expired_owner_leases =
                self.activity.expired_owner_leases.saturating_add(1);
        }
        if spec.epoch > 1 {
            self.activity.advanced_epochs = self.activity.advanced_epochs.saturating_add(1);
        }
    }

    fn worker_request(&mut self, request: RequestId, effects: &mut Vec<Effect>) {
        let route = self
            .cells
            .iter()
            .filter_map(|(id, cell)| {
                let (epoch, retired_durability) = match cell.phase {
                    Phase::Resident { epoch } => (epoch, None),
                    Phase::EnsuringDurability { op, epoch } => (epoch, Some(op)),
                    _ => return None,
                };
                if self.is_active(id) || matches!(cell.alarm, Some(AlarmState::Firing { .. })) {
                    return None;
                }
                Some((id.clone(), epoch, retired_durability))
            })
            .find(|(id, _, _)| self.worker_cursor.as_ref().is_none_or(|cursor| id > cursor))
            .or_else(|| {
                self.cells.iter().find_map(|(id, cell)| {
                    let (epoch, retired_durability) = match cell.phase {
                        Phase::Resident { epoch } => (epoch, None),
                        Phase::EnsuringDurability { op, epoch } => (epoch, Some(op)),
                        _ => return None,
                    };
                    (!self.is_active(id) && !matches!(cell.alarm, Some(AlarmState::Firing { .. })))
                        .then(|| (id.clone(), epoch, retired_durability))
                })
            })
            .map(|(cell, epoch, retired_durability)| WorkerRoute {
                cell,
                epoch,
                retired_durability,
            });

        if let Some(route) = &route {
            self.worker_cursor = Some(route.cell.clone());
            self.active_requests.insert(request, route.cell.clone());
            if let Some(cell) = self.cells.get_mut(&route.cell) {
                if matches!(cell.phase, Phase::EnsuringDurability { .. }) {
                    cell.phase = Phase::Resident { epoch: route.epoch };
                }
                cell.last_used_mono_ms = self.now_mono_ms;
            }
        }
        effects.push(Effect::CompleteWorker { request, route });
    }

    fn finish_requests(
        &mut self,
        id: &str,
        cell: &mut Cell,
        result: Result<Route, RequestError>,
        effects: &mut Vec<Effect>,
    ) {
        for request in std::mem::take(&mut cell.requests) {
            self.request_cells.remove(&request);
            self.complete_request(id, request, result.clone(), effects);
        }
    }

    fn begin_cold_route(
        &mut self,
        id: &str,
        cell: &mut Cell,
        start: ColdStart,
        effects: &mut Vec<Effect>,
    ) {
        debug_assert!(self.activation_permits.contains(id));
        cell.waiting_activation = None;
        match start {
            ColdStart::ReadOwner => {
                let op = self.op();
                cell.phase = Phase::ReadingOwner { op };
                effects.push(Effect::ReadOwner {
                    op,
                    cell: id.to_string(),
                });
            }
            ColdStart::Restore(spec) => {
                self.activate_or_wait(id, cell, Activation::Restore(spec), effects)
            }
        }
    }

    fn admit_or_queue_activation(
        &mut self,
        id: &str,
        cell: &mut Cell,
        start: ColdStart,
        effects: &mut Vec<Effect>,
    ) {
        if self.activation_permits.contains(id) {
            self.begin_cold_route(id, cell, start, effects);
        } else if self.activation_permits.len() < self.config.max_activations {
            self.activation_permits.insert(id.to_string());
            self.begin_cold_route(id, cell, start, effects);
        } else {
            cell.phase = Phase::WaitingActivation;
            cell.waiting_activation = Some(start);
            self.activation_waiters.push_back(id.to_string());
        }
    }

    fn pump_activations(&mut self, effects: &mut Vec<Effect>) {
        self.activation_permits.retain(|id| {
            self.cells
                .get(id)
                .is_some_and(|cell| phase_holds_activation(&cell.phase))
        });

        while self.activation_permits.len() < self.config.max_activations {
            let Some(id) = self.activation_waiters.pop_front() else {
                break;
            };
            let Some(mut cell) = self.cells.remove(&id) else {
                continue;
            };
            if cell.phase != Phase::WaitingActivation {
                self.cells.insert(id, cell);
                continue;
            }
            let Some(start) = cell.waiting_activation.take() else {
                self.cells.insert(id, cell);
                continue;
            };
            if cell.requests.is_empty() && !cell.alarm_wake {
                cell.phase = match start {
                    ColdStart::ReadOwner => Phase::Dormant,
                    ColdStart::Restore(spec) => Phase::OwnedDormant { epoch: spec.epoch },
                };
            } else {
                self.activation_permits.insert(id.clone());
                self.begin_cold_route(&id, &mut cell, start, effects);
            }
            self.cells.insert(id, cell);
        }
    }

    fn begin_activation(
        &mut self,
        id: &str,
        cell: &mut Cell,
        activation: Activation,
        effects: &mut Vec<Effect>,
    ) {
        debug_assert!(self.has_capacity());
        cell.waiting_for = None;
        let op = self.op();
        match activation {
            Activation::Claim(claim) => {
                cell.phase = Phase::Acquiring {
                    op,
                    claim: claim.clone(),
                };
                effects.push(Effect::CasOwner {
                    op,
                    cell: id.to_string(),
                    guard: claim.guard,
                    epoch: claim.epoch,
                    takeover: claim.takeover,
                });
            }
            Activation::Restore(spec) => {
                cell.phase = Phase::Restoring {
                    op,
                    spec: spec.clone(),
                };
                effects.push(Effect::Restore {
                    op,
                    cell: id.to_string(),
                    spec,
                });
            }
        }
    }

    fn activate_or_wait(
        &mut self,
        id: &str,
        cell: &mut Cell,
        activation: Activation,
        effects: &mut Vec<Effect>,
    ) {
        if cell.requests.is_empty() && !cell.alarm_wake {
            cell.phase = match activation {
                Activation::Claim(_) => Phase::Dormant,
                Activation::Restore(spec) => Phase::OwnedDormant { epoch: spec.epoch },
            };
        } else if self.has_capacity() {
            self.begin_activation(id, cell, activation, effects);
        } else {
            cell.phase = Phase::WaitingCapacity;
            cell.waiting_for = Some(activation);
            self.capacity_waiters.push_back(id.to_string());
            self.shed_one(effects);
        }
    }

    fn pump_capacity(&mut self, effects: &mut Vec<Effect>) {
        while self.has_capacity() {
            let Some(id) = self.capacity_waiters.pop_front() else {
                break;
            };
            let Some(mut cell) = self.cells.remove(&id) else {
                continue;
            };
            if cell.phase != Phase::WaitingCapacity {
                self.cells.insert(id, cell);
                continue;
            }
            let Some(activation) = cell.waiting_for.take() else {
                self.cells.insert(id, cell);
                continue;
            };
            if cell.requests.is_empty() && !cell.alarm_wake {
                cell.phase = match activation {
                    Activation::Claim(_) => Phase::Dormant,
                    Activation::Restore(spec) => Phase::OwnedDormant { epoch: spec.epoch },
                };
            } else {
                self.begin_activation(&id, &mut cell, activation, effects);
            }
            self.cells.insert(id, cell);
        }
        self.shed_one(effects);
    }

    fn start_node_lease(&mut self, now_ms: u64, spec: NodeLeaseSpec, effects: &mut Vec<Effect>) {
        if !matches!(
            self.node_authority,
            NodeAuthority::Unstarted | NodeAuthority::Failed | NodeAuthority::Inactive(_)
        ) {
            return;
        }
        if spec.mode == NodeLeaseMode::Lazy {
            self.node_authority = NodeAuthority::Inactive(spec);
            return;
        }
        self.begin_node_lease_acquisition(now_ms, spec, effects);
    }

    fn begin_node_lease_acquisition(
        &mut self,
        now_ms: u64,
        spec: NodeLeaseSpec,
        effects: &mut Vec<Effect>,
    ) {
        let desired = NodeLeaseRecord {
            node: self.node.clone(),
            addr: spec.addr.clone(),
            expires_ms: now_ms.saturating_add(spec.ttl_ms),
            peer_protocol: spec.peer_protocol,
            generation: spec.generation.clone(),
            etag: String::new(),
        };
        let pending = PendingNodeLease {
            spec,
            desired,
            prior: None,
            readback_only: false,
        };
        let op = self.op();
        self.node_authority = NodeAuthority::Reading { op, pending };
        effects.push(Effect::ReadSelfNodeLease { op });
    }

    fn fail_initial_node_lease(&mut self, spec: NodeLeaseSpec, effects: &mut Vec<Effect>) {
        self.node_authority = if spec.mode == NodeLeaseMode::Lazy {
            NodeAuthority::Inactive(spec)
        } else {
            NodeAuthority::Failed
        };
        for (request, _) in std::mem::take(&mut self.node_lease_waiters) {
            effects.push(Effect::Complete {
                request,
                result: Err(RequestError::NodeUnavailable),
            });
        }
        self.node_wake_waiters.clear();
    }

    fn hold_node_lease(
        &mut self,
        spec: NodeLeaseSpec,
        record: NodeLeaseRecord,
        prior: Option<HeldNodeLease>,
        now_mono_ms: u64,
        effects: &mut Vec<Effect>,
    ) {
        let generation = self.next_timer_generation;
        self.next_timer_generation = self
            .next_timer_generation
            .checked_add(1)
            .expect("timer generation exhausted");
        let renew_after = (spec.ttl_ms / 3).max(1);
        effects.push(Effect::ScheduleTimer {
            timer: Timer::NodeLeaseRenew { generation },
            at_mono_ms: now_mono_ms.saturating_add(renew_after),
        });
        effects.push(Effect::ScheduleTimer {
            timer: Timer::NodeLeaseFence { generation },
            at_mono_ms: now_mono_ms.saturating_add(spec.ttl_ms).saturating_add(1),
        });
        let inactive_since_mono_ms = prior
            .as_ref()
            .and_then(|held| held.inactive_since_mono_ms)
            .or_else(|| (self.node_lease_dependency_count() == 0).then_some(now_mono_ms));
        let shadow_release_reported = prior
            .as_ref()
            .is_some_and(|held| held.shadow_release_reported);
        self.node_authority = NodeAuthority::Held(HeldNodeLease {
            spec,
            record,
            last_ok_mono_ms: now_mono_ms,
            last_attempt_mono_ms: now_mono_ms,
            timer_generation: generation,
            inactive_since_mono_ms,
            shadow_release_reported,
        });
        self.drain_node_lease_waiters(effects);
    }

    fn resume_node_lease_after_failure(
        &mut self,
        prior: HeldNodeLease,
        now_mono_ms: u64,
        effects: &mut Vec<Effect>,
    ) {
        let retry_after = (prior.spec.ttl_ms / 3).max(1);
        effects.push(Effect::ScheduleTimer {
            timer: Timer::NodeLeaseRenew {
                generation: prior.timer_generation,
            },
            at_mono_ms: now_mono_ms.saturating_add(retry_after),
        });
        self.node_authority = NodeAuthority::Held(prior);
    }

    fn release_or_fence_node_lease(&mut self, held: HeldNodeLease, effects: &mut Vec<Effect>) {
        if held.spec.mode == NodeLeaseMode::Lazy && self.node_lease_dependency_count() == 0 {
            self.node_authority = NodeAuthority::Inactive(held.spec);
            effects.push(Effect::ObserveNodeLeaseReleased);
        } else {
            self.fence_node(effects);
        }
    }

    fn begin_node_lease_write(
        &mut self,
        pending: PendingNodeLease,
        guard: CasGuard,
        effects: &mut Vec<Effect>,
    ) {
        let op = self.op();
        let record = pending.desired.clone();
        self.node_authority = NodeAuthority::Writing { op, pending };
        effects.push(Effect::CasNodeLease { op, guard, record });
    }

    fn read_self_node_lease(
        &mut self,
        op: OpId,
        _now_ms: u64,
        now_mono_ms: u64,
        result: Result<Option<NodeLeaseRecord>, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let authority = std::mem::replace(&mut self.node_authority, NodeAuthority::Failed);
        let NodeAuthority::Reading {
            op: current,
            pending,
        } = authority
        else {
            self.node_authority = authority;
            return;
        };
        if current != op {
            self.node_authority = NodeAuthority::Reading {
                op: current,
                pending,
            };
            return;
        }
        match result {
            Err(_) => {
                if let Some(prior) = pending.prior {
                    self.resume_node_lease_after_failure(prior, now_mono_ms, effects);
                } else {
                    self.fail_initial_node_lease(pending.spec, effects);
                }
            }
            Ok(Some(record)) if same_node_lease(&record, &pending.desired) => {
                self.hold_node_lease(pending.spec, record, pending.prior, now_mono_ms, effects);
            }
            Ok(Some(record))
                if pending
                    .prior
                    .as_ref()
                    .is_some_and(|prior| same_node_lease(&record, &prior.record)) =>
            {
                let mut prior = pending.prior.expect("checked above");
                prior.record.etag = record.etag;
                self.resume_node_lease_after_failure(prior, now_mono_ms, effects);
            }
            Ok(record) if pending.prior.is_some() => {
                // A renewal was ambiguous and read-back no longer names our
                // exact generation, or the record vanished. Authority is lost.
                let _ = record;
                self.release_or_fence_node_lease(pending.prior.expect("checked above"), effects);
            }
            Ok(_) if pending.readback_only => {
                // The ambiguous initial write did not publish the exact
                // desired generation. Fail this activation; a later request
                // may retry from a fresh read, but this one cannot spin.
                self.fail_initial_node_lease(pending.spec, effects);
            }
            Ok(Some(record)) => {
                // A configured node id is a singleton key: restarting that
                // node replaces its prior process generation immediately. The
                // ETag still serializes competing replacements, and a process
                // which loses that CAS never becomes authoritative.
                self.begin_node_lease_write(pending, CasGuard::Match(record.etag), effects);
            }
            Ok(None) => self.begin_node_lease_write(pending, CasGuard::Absent, effects),
        }
    }

    fn node_lease_cas_completed(
        &mut self,
        op: OpId,
        now_mono_ms: u64,
        result: Result<LeaseCasOutcome, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let authority = std::mem::replace(&mut self.node_authority, NodeAuthority::Failed);
        let NodeAuthority::Writing {
            op: current,
            pending,
        } = authority
        else {
            self.node_authority = authority;
            return;
        };
        if current != op {
            self.node_authority = NodeAuthority::Writing {
                op: current,
                pending,
            };
            return;
        }
        match result {
            Ok(LeaseCasOutcome::Applied { etag }) => {
                let mut record = pending.desired;
                record.etag = etag;
                self.hold_node_lease(pending.spec, record, pending.prior, now_mono_ms, effects);
            }
            Ok(LeaseCasOutcome::Rejected) if pending.prior.is_some() => {
                self.release_or_fence_node_lease(pending.prior.expect("checked above"), effects);
            }
            Ok(LeaseCasOutcome::Rejected) => {
                let read = self.op();
                self.node_authority = NodeAuthority::Reading { op: read, pending };
                effects.push(Effect::ReadSelfNodeLease { op: read });
            }
            Err(Failure::Ambiguous) => {
                let read = self.op();
                let mut pending = pending;
                pending.readback_only = true;
                self.node_authority = NodeAuthority::Reading { op: read, pending };
                effects.push(Effect::ReadSelfNodeLease { op: read });
            }
            Err(Failure::Definite) => {
                if let Some(prior) = pending.prior {
                    self.resume_node_lease_after_failure(prior, now_mono_ms, effects);
                } else {
                    self.fail_initial_node_lease(pending.spec, effects);
                }
            }
        }
    }

    /// Arm a deadline for every activation effect this event produced.
    ///
    /// Done once, centrally, rather than at each of the eleven sites that emit
    /// one: those are eleven chances to forget, and a forgotten deadline is
    /// invisible until something hangs. The effect list already names every
    /// operation the executor is about to start, so it is the natural place to
    /// decide which of them are worth watching.
    fn arm_operation_deadlines(&mut self, effects: &mut Vec<Effect>) {
        let Some(deadline_ms) = self.config.operation_deadline_ms else {
            return;
        };
        let watched: Vec<OpId> = effects
            .iter()
            .filter_map(|effect| match effect {
                // Every operation that holds something back. The activation
                // stages hold a caller; a durability proof holds an eviction;
                // a firing alarm holds the cell out of dormancy. All three
                // keep the ownership record claimed while they wait.
                //
                // `StopRuntime` is the deliberate exception. It has no failure
                // handling to reuse -- a stop cannot fail, it can only not
                // finish -- so abandoning one would mean declaring a runtime
                // gone while it may still be running. That needs a decision,
                // not a timer.
                Effect::ReadOwner { op, .. }
                | Effect::ReadNodeLease { op, .. }
                | Effect::ReadCapacityPeers { op, .. }
                | Effect::CasOwner { op, .. }
                | Effect::Restore { op, .. }
                | Effect::StartRuntime { op, .. }
                | Effect::Publish { op, .. }
                | Effect::EnsureDurable { op, .. }
                // A held write response: a swallowed durability proof must not
                // hang a client forever.
                | Effect::AwaitDurable { op, .. }
                | Effect::FireAlarm { op, .. } => Some(*op),
                _ => None,
            })
            .collect();
        let at_mono_ms = self.now_mono_ms.saturating_add(deadline_ms);
        for op in watched {
            effects.push(Effect::ScheduleTimer {
                timer: Timer::OperationDeadline { op },
                at_mono_ms,
            });
        }
    }

    /// An activation effect outlived its deadline.
    ///
    /// Expiry deliberately reuses each stage's own failure handling rather
    /// than introducing a second way to abandon work: the core already knows
    /// how to reconcile an ambiguous acquire and how to fail a read, and a
    /// deadline is only a different reason for reaching those paths.
    ///
    /// The classification is the whole substance. A read cannot have committed
    /// anything, so it is definite. Everything past it may have taken effect
    /// on the far side while the answer was lost, so it is ambiguous — the
    /// same distinction that decides whether a retry is safe. Calling a
    /// timed-out compare-and-swap definite would let a second attempt
    /// overwrite an epoch that had in fact been applied.
    fn expire_operation(&mut self, op: OpId, now_ms: u64, effects: &mut Vec<Effect>) {
        // A firing alarm is tracked on the cell rather than in its phase, so
        // it is looked for first. Expiry re-arms it exactly as a failed
        // handler would, which keeps alarms at-least-once instead of turning
        // a stuck handler into a lost one.
        if self.cells.values().any(|cell| {
            matches!(cell.alarm, Some(AlarmState::Firing { op: current, .. }) if current == op)
        }) {
            let now_mono_ms = self.now_mono_ms;
            self.alarm_finished(op, now_ms, now_mono_ms, Err(Failure::Ambiguous), effects);
            return;
        }
        // A gated write is tracked in `gated_writes`, not a cell phase, so it
        // is resolved here before the phase scan. Ambiguous is the only safe
        // class: the write may or may not be durable, so the client must not be
        // told it succeeded.
        if self.gated_writes.contains_key(&op) {
            self.durable_reached(op, Err(Failure::Ambiguous), effects);
            return;
        }
        let Some(id) = self.find_cell(|phase| match phase {
            Phase::ReadingOwner { op: current }
            | Phase::ReadingNodeLease { op: current, .. }
            | Phase::ReadingCapacity { op: current, .. }
            | Phase::Acquiring { op: current, .. }
            | Phase::ReconcilingAcquire { op: current, .. }
            | Phase::Restoring { op: current, .. }
            | Phase::Starting { op: current, .. }
            | Phase::Publishing { op: current, .. }
            | Phase::EnsuringDurability { op: current, .. } => *current == op,
            _ => false,
        }) else {
            // Already answered, superseded, or fenced. A stale deadline has
            // nothing to expire, which is the ordinary case.
            return;
        };
        let phase = self.cells.get(&id).map(|cell| cell.phase.clone());
        match phase {
            Some(Phase::ReadingOwner { .. }) => {
                self.owner_read(op, 0, Err(Failure::Definite), effects)
            }
            Some(Phase::ReadingNodeLease { .. }) => {
                self.node_lease_read(op, now_ms, Err(Failure::Definite), effects)
            }
            Some(Phase::ReadingCapacity { .. }) => {
                self.capacity_peers_read(op, now_ms, Err(Failure::Definite), effects)
            }
            Some(Phase::Acquiring { .. }) | Some(Phase::ReconcilingAcquire { .. }) => {
                self.owner_cas_completed(op, Err(Failure::Ambiguous), effects)
            }
            Some(Phase::Restoring { .. }) => {
                self.restore_completed(op, Err(Failure::Ambiguous), effects)
            }
            Some(Phase::Starting { .. }) => {
                self.runtime_started(op, Err(Failure::Ambiguous), effects)
            }
            Some(Phase::Publishing { .. }) => self.published(op, Err(Failure::Ambiguous), effects),
            // An unprovable snapshot leaves the cell resident. Evicting on a
            // proof that never arrived is the one outcome that loses data.
            Some(Phase::EnsuringDurability { .. }) => {
                self.durability_checked(op, Err(Failure::Ambiguous), effects)
            }
            _ => {}
        }
    }

    fn timer_fired(
        &mut self,
        timer: Timer,
        now_ms: u64,
        now_mono_ms: u64,
        effects: &mut Vec<Effect>,
    ) {
        self.update_node_lease_dependencies(now_mono_ms);
        let timer = match timer {
            Timer::CellAlarm { cell, generation } => {
                self.cell_alarm_timer(&cell, generation, now_ms, now_mono_ms, effects);
                return;
            }
            // An activation deadline is not conditional on node authority: a
            // request stalled behind a swallowed effect must be released
            // whatever the lease is doing.
            Timer::OperationDeadline { op } => {
                self.expire_operation(op, now_ms, effects);
                return;
            }
            timer => timer,
        };
        let active = match &self.node_authority {
            NodeAuthority::Held(held) => Some(held.clone()),
            NodeAuthority::Reading { pending, .. } | NodeAuthority::Writing { pending, .. } => {
                pending.prior.clone()
            }
            _ => None,
        };
        let Some(held) = active else {
            return;
        };
        match timer {
            Timer::NodeLeaseFence { generation }
                if generation == held.timer_generation
                    && now_mono_ms.saturating_sub(held.last_ok_mono_ms) > held.spec.ttl_ms =>
            {
                self.release_or_fence_node_lease(held, effects);
            }
            Timer::NodeLeaseRenew { generation }
                if generation == held.timer_generation
                    && matches!(self.node_authority, NodeAuthority::Held(_)) =>
            {
                let NodeAuthority::Held(mut prior) =
                    std::mem::replace(&mut self.node_authority, NodeAuthority::Failed)
                else {
                    unreachable!("held checked above")
                };
                let idle_ms = prior
                    .inactive_since_mono_ms
                    .map_or(0, |inactive| now_mono_ms.saturating_sub(inactive));
                let idle_long_enough =
                    prior.inactive_since_mono_ms.is_some() && idle_ms >= prior.spec.linger_ms;
                if prior.spec.mode == NodeLeaseMode::Shadow
                    && idle_long_enough
                    && !prior.shadow_release_reported
                {
                    self.record_shadow_release(&prior, now_ms, now_mono_ms, effects);
                    prior.shadow_release_reported = true;
                }
                if prior.spec.mode == NodeLeaseMode::Lazy && idle_long_enough {
                    self.node_authority = NodeAuthority::Inactive(prior.spec);
                    effects.push(Effect::ObserveNodeLeaseReleased);
                    return;
                }
                prior.last_attempt_mono_ms = now_mono_ms;
                let spec = prior.spec.clone();
                let desired = NodeLeaseRecord {
                    node: self.node.clone(),
                    addr: spec.addr.clone(),
                    expires_ms: now_ms.saturating_add(spec.ttl_ms),
                    peer_protocol: spec.peer_protocol,
                    generation: spec.generation.clone(),
                    etag: String::new(),
                };
                let guard = CasGuard::Match(prior.record.etag.clone());
                self.begin_node_lease_write(
                    PendingNodeLease {
                        spec,
                        desired,
                        prior: Some(prior),
                        readback_only: false,
                    },
                    guard,
                    effects,
                );
            }
            Timer::CellAlarm { .. } => unreachable!("cell timers returned above"),
            _ => {}
        }
    }

    fn schedule_alarm_timer(
        &self,
        cell: &str,
        generation: u64,
        at_ms: i64,
        now_ms: u64,
        now_mono_ms: u64,
        effects: &mut Vec<Effect>,
    ) {
        let at_ms = u64::try_from(at_ms).unwrap_or(0);
        effects.push(Effect::ScheduleTimer {
            timer: Timer::CellAlarm {
                cell: cell.to_string(),
                generation,
            },
            at_mono_ms: now_mono_ms.saturating_add(at_ms.saturating_sub(now_ms)),
        });
    }

    fn alarm_observed(
        &mut self,
        id: &str,
        at_ms: Option<i64>,
        covered: bool,
        now_ms: u64,
        now_mono_ms: u64,
        effects: &mut Vec<Effect>,
    ) {
        let Some(mut cell) = self.cells.remove(id) else {
            return;
        };
        if matches!(cell.phase, Phase::Fenced) {
            self.cells.insert(id.to_string(), cell);
            return;
        }
        if let Phase::EnsuringDurability { epoch, .. } = cell.phase {
            cell.phase = Phase::Resident { epoch };
        }
        let observed_before = match cell.alarm {
            Some(AlarmState::Armed { at_ms, .. }) | Some(AlarmState::Firing { at_ms, .. }) => at_ms,
            None => -1,
        };
        cell.alarm = at_ms.filter(|at_ms| *at_ms >= 0).map(|at_ms| {
            let generation = self.next_timer_generation;
            self.next_timer_generation = self
                .next_timer_generation
                .checked_add(1)
                .expect("timer generation exhausted");
            self.schedule_alarm_timer(id, generation, at_ms, now_ms, now_mono_ms, effects);
            AlarmState::Armed {
                at_ms,
                generation,
                covered,
            }
        });
        if cell.alarm.is_none() {
            cell.alarm_wake = false;
        }
        // Both ways an alarm settles arrive here -- a request that changed it
        // and a firing that consumed it (`alarm_finished` routes through this
        // function). Saying so once, here, is what keeps the bucket entry
        // from depending on the shell noticing each path separately. Only on
        // a change: an activity that left the alarm alone has nothing to
        // mirror, and re-stating it would put a bucket round trip on the end
        // of every request.
        let settled = at_ms.filter(|at_ms| *at_ms >= 0).unwrap_or(-1);
        if settled != observed_before {
            effects.push(Effect::ReconcileWakeEntry {
                cell: id.to_string(),
                next_alarm_ms: settled,
            });
        }
        self.cells.insert(id.to_string(), cell);
    }

    fn cell_alarm_timer(
        &mut self,
        id: &str,
        generation: u64,
        now_ms: u64,
        now_mono_ms: u64,
        effects: &mut Vec<Effect>,
    ) {
        if !self.node_authoritative() {
            if let Some(spec) = self.lazy_node_lease_spec() {
                self.node_wake_waiters.insert(id.to_string());
                if matches!(self.node_authority, NodeAuthority::Inactive(_)) {
                    self.begin_node_lease_acquisition(now_ms, spec, effects);
                }
                effects.push(Effect::ScheduleTimer {
                    timer: Timer::CellAlarm {
                        cell: id.to_string(),
                        generation,
                    },
                    at_mono_ms: now_mono_ms.saturating_add(100),
                });
            }
            return;
        }
        let Some(mut cell) = self.cells.remove(id) else {
            return;
        };
        let Some(AlarmState::Armed {
            at_ms,
            generation: current,
            covered,
        }) = cell.alarm
        else {
            self.cells.insert(id.to_string(), cell);
            return;
        };
        if current != generation {
            self.cells.insert(id.to_string(), cell);
            return;
        }
        if i64::try_from(now_ms).unwrap_or(i64::MAX) < at_ms {
            self.schedule_alarm_timer(id, generation, at_ms, now_ms, now_mono_ms, effects);
            self.cells.insert(id.to_string(), cell);
            return;
        }
        let epoch = match cell.phase {
            Phase::Resident { epoch } => epoch,
            Phase::EnsuringDurability { epoch, .. } => {
                cell.phase = Phase::Resident { epoch };
                epoch
            }
            Phase::Fenced => {
                cell.alarm = None;
                self.cells.insert(id.to_string(), cell);
                return;
            }
            Phase::OwnedDormant { epoch } => {
                cell.alarm_wake = true;
                self.admit_or_queue_activation(
                    id,
                    &mut cell,
                    ColdStart::Restore(RestoreSpec {
                        epoch,
                        fresh: false,
                        took_over: false,
                    }),
                    effects,
                );
                effects.push(Effect::ScheduleTimer {
                    timer: Timer::CellAlarm {
                        cell: id.to_string(),
                        generation,
                    },
                    at_mono_ms: now_mono_ms.saturating_add(100),
                });
                self.cells.insert(id.to_string(), cell);
                return;
            }
            Phase::Dormant => {
                cell.alarm_wake = true;
                self.admit_or_queue_activation(id, &mut cell, ColdStart::ReadOwner, effects);
                effects.push(Effect::ScheduleTimer {
                    timer: Timer::CellAlarm {
                        cell: id.to_string(),
                        generation,
                    },
                    at_mono_ms: now_mono_ms.saturating_add(100),
                });
                self.cells.insert(id.to_string(), cell);
                return;
            }
            _ => {
                effects.push(Effect::ScheduleTimer {
                    timer: Timer::CellAlarm {
                        cell: id.to_string(),
                        generation,
                    },
                    at_mono_ms: now_mono_ms.saturating_add(100),
                });
                self.cells.insert(id.to_string(), cell);
                return;
            }
        };
        let op = self.op();
        cell.alarm = Some(AlarmState::Firing {
            op,
            at_ms,
            generation,
            covered,
        });
        cell.alarm_wake = false;
        self.cells.insert(id.to_string(), cell);
        effects.push(Effect::FireAlarm {
            op,
            cell: id.to_string(),
            epoch,
            scheduled_ms: at_ms,
        });
    }

    fn alarm_finished(
        &mut self,
        op: OpId,
        now_ms: u64,
        now_mono_ms: u64,
        result: Result<(Option<i64>, bool), Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let Some(id) = self.cells.iter().find_map(|(id, cell)| {
            matches!(cell.alarm, Some(AlarmState::Firing { op: current, .. }) if current == op)
                .then(|| id.clone())
        }) else {
            return;
        };
        let Some(mut cell) = self.cells.remove(&id) else {
            return;
        };
        let Some(AlarmState::Firing {
            at_ms,
            generation,
            covered,
            ..
        }) = cell.alarm
        else {
            self.cells.insert(id, cell);
            return;
        };
        match result {
            Ok((at_ms, covered)) => {
                // Leave the fired alarm in place for `alarm_observed` to
                // replace: it assigns unconditionally, and it compares against
                // what was there to decide whether the bucket entry has to
                // follow. Clearing it here first makes a consumed alarm look
                // like it was never armed, and the entry is never deleted.
                self.cells.insert(id.clone(), cell);
                self.alarm_observed(&id, at_ms, covered, now_ms, now_mono_ms, effects);
            }
            Err(_) => {
                cell.alarm = Some(AlarmState::Armed {
                    at_ms,
                    generation,
                    covered,
                });
                self.cells.insert(id.clone(), cell);
                effects.push(Effect::ScheduleTimer {
                    timer: Timer::CellAlarm {
                        cell: id,
                        generation,
                    },
                    at_mono_ms: now_mono_ms.saturating_add(500),
                });
            }
        }
    }

    fn fence_node(&mut self, effects: &mut Vec<Effect>) {
        self.node_authority = NodeAuthority::Fenced;
        self.fence(effects);
        effects.push(Effect::Halt {
            code: 3,
            reason: HaltReason::NodeLeaseExpired,
        });
    }

    fn lazy_node_lease_spec(&self) -> Option<NodeLeaseSpec> {
        match &self.node_authority {
            NodeAuthority::Inactive(spec) if spec.mode == NodeLeaseMode::Lazy => Some(spec.clone()),
            NodeAuthority::Reading { pending, .. } | NodeAuthority::Writing { pending, .. }
                if pending.prior.is_none() && pending.spec.mode == NodeLeaseMode::Lazy =>
            {
                Some(pending.spec.clone())
            }
            _ => None,
        }
    }

    fn request(
        &mut self,
        request: RequestId,
        id: CellId,
        now_ms: u64,
        capacity_handoff: bool,
        effects: &mut Vec<Effect>,
    ) {
        if self.request_cells.contains_key(&request)
            || self.node_lease_waiters.contains_key(&request)
        {
            return;
        }
        if self.fenced {
            effects.push(Effect::Complete {
                request,
                result: Err(RequestError::NodeFenced),
            });
            return;
        }
        if capacity_handoff {
            self.capacity_requests.insert(request);
        }
        if self.node_authoritative() {
            self.request_authorized(request, id, effects);
            return;
        }
        if let Some(spec) = self.lazy_node_lease_spec() {
            self.node_lease_waiters.insert(request, id);
            if matches!(self.node_authority, NodeAuthority::Inactive(_)) {
                self.begin_node_lease_acquisition(now_ms, spec, effects);
            }
        } else {
            effects.push(Effect::Complete {
                request,
                result: Err(RequestError::NodeUnavailable),
            });
        }
    }

    fn drain_node_lease_waiters(&mut self, effects: &mut Vec<Effect>) {
        let requests = std::mem::take(&mut self.node_lease_waiters);
        for (request, cell) in requests {
            self.request_authorized(request, cell, effects);
        }
        let wakes = std::mem::take(&mut self.node_wake_waiters);
        for cell in wakes {
            self.wake_hint_authorized(cell, effects);
        }
    }

    fn request_authorized(&mut self, request: RequestId, id: CellId, effects: &mut Vec<Effect>) {
        let mut cell = self.cells.remove(&id).unwrap_or_default();
        match &cell.phase {
            Phase::Resident { .. } => {
                self.complete_request(&id, request, Ok(Route::Local), effects)
            }
            Phase::EnsuringDurability { epoch, .. } => {
                // The runtime is still published, so a new request wins the
                // race with voluntary eviction. Retiring the operation makes
                // its eventual durability completion harmless.
                let epoch = *epoch;
                cell.phase = Phase::Resident { epoch };
                self.complete_request(&id, request, Ok(Route::Local), effects);
            }
            Phase::Remote {
                node,
                addr,
                epoch,
                peer_protocol,
                ..
            } => effects.push(Effect::Complete {
                request,
                result: Ok(Route::Remote {
                    node: node.clone(),
                    addr: addr.clone(),
                    epoch: *epoch,
                    peer_protocol: *peer_protocol,
                }),
            }),
            Phase::Fenced => effects.push(Effect::Complete {
                request,
                result: Err(RequestError::NodeFenced),
            }),
            phase => {
                cell.requests.insert(request);
                self.request_cells.insert(request, id.clone());
                match phase {
                    Phase::Dormant => {
                        self.admit_or_queue_activation(
                            &id,
                            &mut cell,
                            ColdStart::ReadOwner,
                            effects,
                        );
                    }
                    Phase::OwnedDormant { epoch } => {
                        let epoch = *epoch;
                        self.admit_or_queue_activation(
                            &id,
                            &mut cell,
                            ColdStart::Restore(RestoreSpec {
                                epoch,
                                fresh: false,
                                took_over: false,
                            }),
                            effects,
                        );
                    }
                    _ => {}
                }
            }
        }
        self.cells.insert(id, cell);
    }

    fn wake_hint(&mut self, id: CellId, now_ms: u64, effects: &mut Vec<Effect>) {
        if self.fenced {
            return;
        }
        if self.node_authoritative() {
            self.wake_hint_authorized(id, effects);
            return;
        }
        if let Some(spec) = self.lazy_node_lease_spec() {
            self.node_wake_waiters.insert(id);
            if matches!(self.node_authority, NodeAuthority::Inactive(_)) {
                self.begin_node_lease_acquisition(now_ms, spec, effects);
            }
        }
    }

    fn wake_hint_authorized(&mut self, id: CellId, effects: &mut Vec<Effect>) {
        let mut cell = self.cells.remove(&id).unwrap_or_default();
        cell.alarm_wake = true;
        match cell.phase {
            Phase::Dormant | Phase::Remote { .. } => {
                self.admit_or_queue_activation(&id, &mut cell, ColdStart::ReadOwner, effects);
            }
            Phase::OwnedDormant { epoch } => self.admit_or_queue_activation(
                &id,
                &mut cell,
                ColdStart::Restore(RestoreSpec {
                    epoch,
                    fresh: false,
                    took_over: false,
                }),
                effects,
            ),
            _ => {}
        }
        self.cells.insert(id, cell);
    }

    fn cancel(&mut self, request: RequestId) {
        self.capacity_requests.remove(&request);
        if self.node_lease_waiters.remove(&request).is_some() {
            return;
        }
        let Some(id) = self.request_cells.remove(&request) else {
            return;
        };
        let Some(cell) = self.cells.get_mut(&id) else {
            return;
        };
        cell.requests.remove(&request);
        if cell.requests.is_empty() && !cell.alarm_wake && cell.phase == Phase::WaitingActivation {
            let start = cell.waiting_activation.take();
            cell.phase = match start {
                Some(ColdStart::Restore(spec)) => Phase::OwnedDormant { epoch: spec.epoch },
                _ => Phase::Dormant,
            };
            self.activation_waiters.retain(|queued| queued != &id);
        } else if cell.requests.is_empty()
            && !cell.alarm_wake
            && cell.phase == Phase::WaitingCapacity
        {
            let activation = cell.waiting_for.take();
            cell.phase = match activation {
                Some(Activation::Restore(spec)) => Phase::OwnedDormant { epoch: spec.epoch },
                _ => Phase::Dormant,
            };
            self.capacity_waiters.retain(|queued| queued != &id);
        }
    }

    fn begin_capacity_lookup(
        &mut self,
        id: &str,
        cell: &mut Cell,
        claim: Claim,
        effects: &mut Vec<Effect>,
    ) {
        let op = self.op();
        cell.phase = Phase::ReadingCapacity {
            op,
            claim: claim.clone(),
        };
        effects.push(Effect::ReadCapacityPeers {
            op,
            cell: id.to_string(),
        });
    }

    /// Place a genuinely unowned cell. Ordinary ingress may look for fleet
    /// capacity before waiting locally; a capacity handoff must either reserve
    /// a real local slot now or explicitly refuse so its caller can traverse.
    fn place_unowned(
        &mut self,
        id: &str,
        cell: &mut Cell,
        claim: Claim,
        effects: &mut Vec<Effect>,
    ) {
        if self.has_capacity() {
            self.activate_or_wait(id, cell, Activation::Claim(claim), effects);
            return;
        }

        let handoffs: Vec<RequestId> = cell
            .requests
            .iter()
            .copied()
            .filter(|request| self.capacity_requests.contains(request))
            .collect();
        for request in handoffs {
            cell.requests.remove(&request);
            self.request_cells.remove(&request);
            self.complete_request(
                id,
                request,
                Err(RequestError::CapacityExhausted),
                effects,
            );
        }

        if cell.requests.is_empty() || !self.config.require_node_lease {
            // Alarm wakes cannot be proxied as an HTTP capacity handoff. They
            // retain the old local wait semantics until alarm dispatch itself
            // has a fleet transport effect. Lease-disabled mode likewise has
            // no authoritative fleet membership to enumerate.
            self.activate_or_wait(id, cell, Activation::Claim(claim), effects);
        } else {
            self.begin_capacity_lookup(id, cell, claim, effects);
        }
    }

    fn owner_read(
        &mut self,
        op: OpId,
        now_ms: u64,
        result: Result<Option<OwnerRecord>, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let Some(id) = self.cells.iter().find_map(|(id, cell)| {
            matches!(
                cell.phase,
                Phase::ReadingOwner { op: current }
                    | Phase::ReconcilingAcquire { op: current, .. }
                    if current == op
            )
            .then(|| id.clone())
        }) else {
            return;
        };
        let mut cell = self.cells.remove(&id).expect("cell found above");
        let reconciling_claim = match &cell.phase {
            Phase::ReconcilingAcquire { claim, .. } => Some(claim.clone()),
            _ => None,
        };
        // A reconcile re-reads and then acquires again: one attempt
        // continuing, not a new one. The count has to survive the round trip
        // or the bound below can never be reached.
        let reconciles = reconciling_claim
            .as_ref()
            .map_or(0, |claim| claim.reconciles);
        match result {
            Ok(Some(record))
                if reconciling_claim.as_ref().is_some_and(|claim| {
                    record.node.as_deref() == Some(self.node.as_str())
                        && record.epoch == claim.epoch
                }) =>
            {
                let spec = RestoreSpec {
                    epoch: record.epoch,
                    fresh: matches!(
                        reconciling_claim.as_ref().map(|claim| &claim.guard),
                        Some(CasGuard::Absent)
                    ),
                    took_over: reconciling_claim
                        .as_ref()
                        .is_some_and(|claim| claim.takeover),
                };
                self.record_acquisition(&spec);
                self.activate_or_wait(&id, &mut cell, Activation::Restore(spec), effects);
            }
            Ok(Some(record)) if record.node.as_deref() == Some(self.node.as_str()) => {
                let epoch = record.epoch.saturating_add(1);
                self.activate_or_wait(
                    &id,
                    &mut cell,
                    Activation::Claim(Claim {
                        guard: CasGuard::Match(record.etag),
                        epoch,
                        takeover: false,
                        reconciles,
                    }),
                    effects,
                );
            }
            Ok(Some(record)) if record.node.is_none() => {
                let epoch = record.epoch.saturating_add(1);
                self.place_unowned(
                    &id,
                    &mut cell,
                    Claim {
                        guard: CasGuard::Match(record.etag),
                        epoch,
                        takeover: true,
                        reconciles,
                    },
                    effects,
                );
            }
            Ok(Some(record)) => {
                let owner = record.node.clone().expect("foreign owner checked above");
                let cached = self
                    .node_lease_cache
                    .get(&owner)
                    .filter(|lease| lease.expires_ms > now_ms && !lease.addr.is_empty())
                    .cloned();
                if let Some(lease) = cached {
                    self.apply_node_lease_result(
                        &id,
                        &mut cell,
                        record,
                        now_ms,
                        Ok(Some(lease)),
                        effects,
                    );
                } else {
                    self.node_lease_cache.remove(&owner);
                    let next = self.op();
                    cell.phase = Phase::ReadingNodeLease {
                        op: next,
                        owner: record,
                    };
                    effects.push(Effect::ReadNodeLease {
                        op: next,
                        cell: id.clone(),
                        owner,
                    });
                }
            }
            Ok(None) => {
                self.place_unowned(
                    &id,
                    &mut cell,
                    Claim {
                        guard: CasGuard::Absent,
                        epoch: 1,
                        takeover: false,
                        reconciles,
                    },
                    effects,
                );
            }
            Err(_) => {
                cell.phase = Phase::Dormant;
                self.finish_requests(&id, &mut cell, Err(RequestError::ResolveFailed), effects);
            }
        }
        self.cells.insert(id, cell);
        if reconciling_claim.is_some() {
            self.pump_capacity(effects);
        }
    }

    fn node_lease_read(
        &mut self,
        op: OpId,
        now_ms: u64,
        result: Result<Option<NodeLeaseRecord>, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let Some(id) = self.find_cell(
            |phase| matches!(phase, Phase::ReadingNodeLease { op: current, .. } if *current == op),
        ) else {
            return;
        };
        let mut cell = self.cells.remove(&id).expect("cell found above");
        let Phase::ReadingNodeLease { owner: record, .. } = &cell.phase else {
            unreachable!()
        };
        let record = record.clone();
        if let Ok(Some(lease)) = &result {
            if record.node.as_deref() == Some(lease.node.as_str())
                && lease.expires_ms > now_ms
                && !lease.addr.is_empty()
            {
                self.node_lease_cache
                    .insert(lease.node.clone(), lease.clone());
            }
        }
        self.apply_node_lease_result(&id, &mut cell, record, now_ms, result, effects);
        self.cells.insert(id, cell);
    }

    fn capacity_peers_read(
        &mut self,
        op: OpId,
        now_ms: u64,
        result: Result<Vec<CapacityPeer>, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let Some(id) = self.find_cell(
            |phase| matches!(phase, Phase::ReadingCapacity { op: current, .. } if *current == op),
        ) else {
            return;
        };
        let mut cell = self.cells.remove(&id).expect("cell found above");
        let Phase::ReadingCapacity { claim, .. } = &cell.phase else {
            unreachable!()
        };
        let claim = claim.clone();
        let peers = match result {
            Ok(peers) => peers,
            Err(_) => {
                self.activate_or_wait(&id, &mut cell, Activation::Claim(claim), effects);
                self.cells.insert(id, cell);
                return;
            }
        };

        // A newer load sample supersedes both reservations made against and
        // refusals of the prior sample. Equal samples deliberately retain
        // both, which is what makes concurrent read completions compose.
        for peer in &peers {
            let prior = self.capacity_samples.get(&peer.node).copied().unwrap_or(0);
            if peer.sampled_ms > prior {
                self.capacity_samples
                    .insert(peer.node.clone(), peer.sampled_ms);
                self.capacity_reservations.remove(&peer.node);
                self.capacity_rejections.remove(&peer.node);
            }
        }

        let selected = peers
            .into_iter()
            .filter(|peer| {
                peer.node != self.node
                    && peer.expires_ms > now_ms
                    && !peer.addr.is_empty()
                    && peer.peer_protocol == self.config.peer_protocol
                    && peer.sampled_ms != 0
                    && !peer.pressured
                    && self
                        .capacity_rejections
                        .get(&peer.node)
                        .is_none_or(|sample| peer.sampled_ms > *sample)
            })
            .map(|peer| {
                let projected = peer.resident_cells.saturating_add(
                    self.capacity_reservations
                        .get(&peer.node)
                        .copied()
                        .unwrap_or(0),
                );
                (peer, projected)
            })
            .min_by_key(|(peer, projected)| {
                (
                    *projected,
                    peer.host_websockets,
                    peer.rss_bytes,
                    peer.node.clone(),
                )
            });

        if let Some((peer, _)) = selected {
            *self
                .capacity_reservations
                .entry(peer.node.clone())
                .or_default() += 1;
            cell.phase = Phase::Remote {
                node: peer.node.clone(),
                addr: peer.addr.clone(),
                epoch: 0,
                peer_protocol: peer.peer_protocol,
                capacity_sampled_ms: Some(peer.sampled_ms),
            };
            self.finish_requests(
                &id,
                &mut cell,
                Ok(Route::Remote {
                    node: peer.node,
                    addr: peer.addr,
                    epoch: 0,
                    peer_protocol: peer.peer_protocol,
                }),
                effects,
            );
        } else {
            self.activate_or_wait(&id, &mut cell, Activation::Claim(claim), effects);
        }
        self.cells.insert(id, cell);
    }

    fn apply_node_lease_result(
        &mut self,
        id: &str,
        cell: &mut Cell,
        record: OwnerRecord,
        now_ms: u64,
        result: Result<Option<NodeLeaseRecord>, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        match result {
            Ok(Some(lease))
                if lease.expires_ms > now_ms
                    && !lease.addr.is_empty()
                    && record.node.as_deref() == Some(lease.node.as_str())
                    && lease.peer_protocol != self.config.peer_protocol =>
            {
                cell.phase = Phase::Dormant;
                self.finish_requests(id, cell, Err(RequestError::PeerIncompatible), effects);
            }
            Ok(Some(lease))
                if lease.expires_ms > now_ms
                    && !lease.addr.is_empty()
                    && record.node.as_deref() == Some(lease.node.as_str())
                    && lease.peer_protocol == self.config.peer_protocol =>
            {
                let node = lease.node;
                let addr = lease.addr;
                let epoch = record.epoch;
                cell.phase = Phase::Remote {
                    node: node.clone(),
                    addr: addr.clone(),
                    epoch,
                    peer_protocol: lease.peer_protocol,
                    capacity_sampled_ms: None,
                };
                self.finish_requests(
                    id,
                    cell,
                    Ok(Route::Remote {
                        node,
                        addr,
                        epoch,
                        peer_protocol: lease.peer_protocol,
                    }),
                    effects,
                );
            }
            Ok(_) => {
                let epoch = record.epoch.saturating_add(1);
                self.activate_or_wait(
                    id,
                    cell,
                    Activation::Claim(Claim {
                        guard: CasGuard::Match(record.etag),
                        epoch,
                        takeover: true,
                        reconciles: 0,
                    }),
                    effects,
                );
            }
            Err(_) => {
                cell.phase = Phase::Dormant;
                self.finish_requests(id, cell, Err(RequestError::ResolveFailed), effects);
            }
        }
    }

    fn owner_cas_completed(
        &mut self,
        op: OpId,
        result: Result<CasOutcome, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let Some(id) = self.find_cell(
            |phase| matches!(phase, Phase::Acquiring { op: current, .. } if *current == op),
        ) else {
            return;
        };
        let mut cell = self.cells.remove(&id).expect("cell found above");
        let Phase::Acquiring { claim, .. } = &cell.phase else {
            unreachable!()
        };
        let claim = claim.clone();
        match result {
            Ok(CasOutcome::Applied) => {
                let next = self.op();
                let spec = RestoreSpec {
                    epoch: claim.epoch,
                    fresh: matches!(claim.guard, CasGuard::Absent),
                    took_over: claim.takeover,
                };
                self.record_acquisition(&spec);
                cell.phase = Phase::Restoring {
                    op: next,
                    spec: spec.clone(),
                };
                effects.push(Effect::Restore {
                    op: next,
                    cell: id.clone(),
                    spec,
                });
            }
            Ok(CasOutcome::Rejected) => {
                let next = self.op();
                cell.phase = Phase::ReadingOwner { op: next };
                effects.push(Effect::ReadOwner {
                    op: next,
                    cell: id.clone(),
                });
            }
            Err(Failure::Ambiguous) if claim.reconciles < MAX_ACQUIRE_RECONCILES => {
                let next = self.op();
                let claim = Claim {
                    reconciles: claim.reconciles + 1,
                    ..claim
                };
                cell.phase = Phase::ReconcilingAcquire { op: next, claim };
                effects.push(Effect::ReadOwner {
                    op: next,
                    cell: id.clone(),
                });
            }
            Err(Failure::Ambiguous) => {
                // Out of reconciles. The claim may or may not have applied, so
                // this cell is left dormant for a later request to resolve
                // from the record rather than guessed at here.
                cell.phase = Phase::Dormant;
                self.finish_requests(&id, &mut cell, Err(RequestError::AcquireFailed), effects);
            }
            Err(Failure::Definite) => {
                cell.phase = Phase::Dormant;
                self.finish_requests(&id, &mut cell, Err(RequestError::AcquireFailed), effects);
            }
        }
        self.cells.insert(id, cell);
        self.pump_capacity(effects);
    }

    fn restore_completed(
        &mut self,
        op: OpId,
        result: Result<RestoreOutcome, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let Some(id) = self.find_cell(
            |phase| matches!(phase, Phase::Restoring { op: current, .. } if *current == op),
        ) else {
            return;
        };
        let mut cell = self.cells.remove(&id).expect("cell found above");
        let Phase::Restoring { spec, .. } = cell.phase else {
            unreachable!()
        };
        let epoch = spec.epoch;
        match result {
            Ok(outcome) => {
                if outcome.restored {
                    self.activity.restored = self.activity.restored.saturating_add(1);
                }
                // Seed the mirror from the durable truth the restore loaded,
                // before the isolate opens the same database and long before
                // it would re-arm anything.
                if let Some(alarm) = outcome.alarm.filter(|alarm| alarm.at_ms >= 0) {
                    let generation = self.next_timer_generation;
                    self.next_timer_generation = self
                        .next_timer_generation
                        .checked_add(1)
                        .expect("timer generation exhausted");
                    let (now_ms, now_mono_ms) = (self.now_ms, self.now_mono_ms);
                    self.schedule_alarm_timer(
                        &id,
                        generation,
                        alarm.at_ms,
                        now_ms,
                        now_mono_ms,
                        effects,
                    );
                    cell.alarm = Some(AlarmState::Armed {
                        at_ms: alarm.at_ms,
                        generation,
                        covered: alarm.covered,
                    });
                }
                let next = self.op();
                cell.phase = Phase::Starting { op: next, epoch };
                effects.push(Effect::StartRuntime {
                    op: next,
                    cell: id.clone(),
                    epoch,
                });
            }
            Err(_) => {
                cell.phase = Phase::OwnedDormant { epoch };
                self.finish_requests(&id, &mut cell, Err(RequestError::RestoreFailed), effects);
            }
        }
        self.cells.insert(id, cell);
        self.pump_capacity(effects);
    }

    fn runtime_started(
        &mut self,
        op: OpId,
        result: Result<(), Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let Some(id) = self.find_cell(
            |phase| matches!(phase, Phase::Starting { op: current, .. } if *current == op),
        ) else {
            self.compensate_retired_runtime(op, result, effects);
            return;
        };
        let mut cell = self.cells.remove(&id).expect("cell found above");
        let Phase::Starting { epoch, .. } = cell.phase else {
            unreachable!()
        };
        match result {
            Ok(()) => {
                let next = self.op();
                cell.phase = Phase::Publishing { op: next, epoch };
                effects.push(Effect::Publish {
                    op: next,
                    cell: id.clone(),
                    epoch,
                });
            }
            Err(_) => {
                cell.phase = Phase::OwnedDormant { epoch };
                self.finish_requests(&id, &mut cell, Err(RequestError::RuntimeFailed), effects);
            }
        }
        self.cells.insert(id, cell);
        self.pump_capacity(effects);
    }

    fn published(&mut self, op: OpId, result: Result<(), Failure>, effects: &mut Vec<Effect>) {
        let Some(id) = self.find_cell(
            |phase| matches!(phase, Phase::Publishing { op: current, .. } if *current == op),
        ) else {
            self.compensate_retired_runtime(op, result, effects);
            return;
        };
        let mut cell = self.cells.remove(&id).expect("cell found above");
        let Phase::Publishing { epoch, .. } = cell.phase else {
            unreachable!()
        };
        match result {
            Ok(()) => {
                cell.phase = Phase::Resident { epoch };
                self.finish_requests(&id, &mut cell, Ok(Route::Local), effects);
            }
            Err(_) => {
                let next = self.op();
                cell.phase = Phase::Cleaning {
                    op: next,
                    epoch,
                    cause: StopCause::Cleanup,
                };
                effects.push(Effect::StopRuntime {
                    op: next,
                    cell: id.clone(),
                    epoch,
                    cause: StopCause::Cleanup,
                });
            }
        }
        self.cells.insert(id, cell);
    }

    /// A release either published the cell as unowned or did not. Ownership
    /// is the bucket's answer, not this node's, so a rejected or failed write
    /// simply leaves the record naming this node -- correct, if less useful,
    /// and the next eviction gets another chance.
    fn owner_released(&mut self, op: OpId, result: Result<CasOutcome, Failure>) {
        let Some(id) = self
            .cells
            .iter()
            .find(|(_, cell)| cell.releasing == Some(op))
            .map(|(id, _)| id.clone())
        else {
            return;
        };
        let cell = self.cells.get_mut(&id).expect("cell found above");
        cell.releasing = None;
        // Only a cell still sitting where the eviction left it may be
        // forgotten. Anything else means it was wanted again while the write
        // was in flight, and that claim outranks a release decided earlier.
        if matches!(result, Ok(CasOutcome::Applied))
            && matches!(cell.phase, Phase::OwnedDormant { .. })
        {
            cell.phase = Phase::Dormant;
        }
    }

    fn runtime_stopped(&mut self, op: OpId, effects: &mut Vec<Effect>) {
        let Some(id) = self.find_cell(
            |phase| matches!(phase, Phase::Cleaning { op: current, .. } if *current == op),
        ) else {
            return;
        };
        let mut cell = self.cells.remove(&id).expect("cell found above");
        let Phase::Cleaning { epoch, cause, .. } = cell.phase else {
            unreachable!()
        };
        match cause {
            StopCause::Cleanup => {
                cell.phase = Phase::OwnedDormant { epoch };
                self.finish_requests(&id, &mut cell, Err(RequestError::PublishFailed), effects);
            }
            StopCause::Evict { rebalance } if cell.requests.is_empty() => {
                cell.phase = Phase::OwnedDormant { epoch };
                self.hibernation_permits.remove(&id);
                // The record still names this node, which is the whole cost of
                // stopping here: every later request for the cell routes to a
                // node that has already decided it has no room. Publishing it
                // as unowned is what turns an eviction into shed load.
                if rebalance {
                    let op = self.op();
                    cell.releasing = Some(op);
                    effects.push(Effect::ReleaseOwner {
                        op,
                        cell: id.clone(),
                        epoch,
                    });
                }
            }
            StopCause::Evict { .. } => {
                // A request arrived mid-eviction, so the cell turns straight
                // back around. The eviction is over either way.
                self.hibernation_permits.remove(&id);
                self.admit_or_queue_activation(
                    &id,
                    &mut cell,
                    ColdStart::Restore(RestoreSpec {
                        epoch,
                        fresh: false,
                        took_over: false,
                    }),
                    effects,
                );
            }
            StopCause::Fence => unreachable!("fenced cells do not wait for runtime shutdown"),
        }
        self.cells.insert(id, cell);
        self.pump_capacity(effects);
        self.shed_toward_floor(effects);
    }

    fn durability_checked(
        &mut self,
        op: OpId,
        result: Result<(), Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let now = self.now_mono_ms;
        let Some(id) = self.find_cell(|phase| {
            matches!(phase, Phase::EnsuringDurability { op: current, .. } if *current == op)
        }) else {
            return;
        };
        let cell = self.cells.get_mut(&id).expect("cell found above");
        let Phase::EnsuringDurability { epoch, .. } = cell.phase else {
            unreachable!()
        };
        match result {
            Ok(()) => {
                let stop = self.next_op;
                self.next_op = self.next_op.checked_add(1).expect("operation id exhausted");
                let rebalance = cell.evict_rebalance;
                cell.phase = Phase::Cleaning {
                    op: stop,
                    epoch,
                    cause: StopCause::Evict { rebalance },
                };
                effects.push(Effect::StopRuntime {
                    op: stop,
                    cell: id,
                    epoch,
                    cause: StopCause::Evict { rebalance },
                });
            }
            Err(_) => {
                cell.phase = Phase::Resident { epoch };
                cell.hibernation_refused_mono_ms = Some(now);
                self.hibernation_permits.remove(&id);
            }
        }
    }

    /// Evict on demand. Local, like an idle hibernation: the caller asked
    /// this node to drop the cell, not to give it away.
    fn evict(&mut self, id: &str, effects: &mut Vec<Effect>) {
        self.begin_eviction(id, false, effects);
    }

    /// Does an eviction made for room hand the cell away?
    fn rebalances(&self) -> bool {
        self.config.ownership_on_evict == OwnershipOnEvict::Release
    }

    fn begin_eviction(&mut self, id: &str, rebalance: bool, effects: &mut Vec<Effect>) -> bool {
        // An alarm about to fire is not worth a hibernation: the wake costs
        // more than the residency it would save, so the cell is held even
        // though its entry is perfectly durable. Coverage says the alarm can
        // survive the eviction; this says it is not worth surviving it.
        let alarm_is_imminent = self.cells.get(id).is_some_and(|cell| match cell.alarm {
            Some(AlarmState::Armed { at_ms, .. }) | Some(AlarmState::Firing { at_ms, .. }) => {
                at_ms >= 0
                    && (at_ms as u64).saturating_sub(self.now_ms) <= self.config.alarm_resident_ms
            }
            None => false,
        });
        // Only while there is room to spare. Holding a cell to save a wake is
        // worth it on an idle node and indefensible on a full one: the window
        // defaults to an hour, so on an alarm-driven workload this would hold
        // most of the node and pin it at its ceiling -- trading a real
        // admission failure for a saved activation. Under pressure the node
        // takes the wake.
        if alarm_is_imminent && !self.shedding {
            return false;
        }
        let alarm_is_safe = self.cells.get(id).is_some_and(|cell| {
            cell.alarm
                .as_ref()
                .is_none_or(|alarm| matches!(alarm, AlarmState::Armed { covered: true, .. }))
        });
        if self.is_active(id) || !alarm_is_safe {
            return false;
        }
        let Some(Phase::Resident { epoch }) = self.cells.get(id).map(|cell| &cell.phase) else {
            return false;
        };
        let epoch = *epoch;
        let op = self.op();
        let Some(cell) = self.cells.get_mut(id) else {
            return false;
        };
        cell.evict_rebalance = rebalance;
        cell.phase = Phase::EnsuringDurability { op, epoch };
        self.hibernation_permits.insert(id.to_string());
        effects.push(Effect::EnsureDurable {
            op,
            cell: id.to_string(),
            epoch,
        });
        true
    }

    fn shed_one(&mut self, effects: &mut Vec<Effect>) {
        // Only when the blocker is headroom. A latched node is already walking
        // down on its own schedule, and admission stays closed until it
        // reaches the low watermark -- so shedding to make room for a waiter
        // that cannot be admitted has no stopping condition, and every
        // completed eviction re-enters here through `pump_capacity` and starts
        // another. That empties the node.
        if self.shedding
            || self.capacity_waiters.is_empty()
            || self.hibernation_permits.len() >= self.config.max_hibernations
        {
            return;
        }
        if let Some(victim) = self.shed_candidate() {
            self.begin_eviction(&victim, self.rebalances(), effects);
        }
    }

    /// The cell to shed right now: resident, idle, not holding an alarm the
    /// node still owes, and the least recently used of those. Demand shedding
    /// and pressure shedding both come through here, so they cannot disagree
    /// about what is safe to take or which one to take first.
    fn shed_candidate(&self) -> Option<CellId> {
        self.cells
            .iter()
            .filter(|(id, cell)| {
                matches!(cell.phase, Phase::Resident { .. })
                    && cell.alarm.as_ref().is_none_or(|alarm| {
                        matches!(alarm, AlarmState::Armed { covered: true, .. })
                    })
                    && !self.is_active(id)
            })
            // Cells that have never been refused first, then by how long ago
            // the refusal was, then least recently used, with the id as a
            // tiebreak so the choice stays a function of the state and not of
            // map iteration order. A refused cell is still reachable -- it
            // just stops being the answer every time.
            .min_by_key(|(id, cell)| {
                (
                    cell.hibernation_refused_mono_ms,
                    cell.last_used_mono_ms,
                    (*id).clone(),
                )
            })
            .map(|(id, _)| id.clone())
    }

    /// Release one cell that has gone cold, with nothing asking for the room.
    ///
    /// Shedding answers "this node is in trouble"; this answers the ordinary
    /// question of a cell nobody has touched in a long time. Without it a node
    /// only ever gives a cell back under pressure or when another cell wants
    /// the slot, so a quiet node holds every cell it ever served until it
    /// reaches a watermark -- paying to keep runtimes alive for traffic that
    /// stopped hours ago.
    fn evict_idle(&mut self, now_mono_ms: u64, effects: &mut Vec<Effect>) {
        let Some(idle_ms) = self.config.idle_evict_ms else {
            return;
        };
        let Some(candidate) = self.shed_candidate() else {
            return;
        };
        let cold = self
            .cells
            .get(&candidate)
            .is_some_and(|cell| now_mono_ms.saturating_sub(cell.last_used_mono_ms) >= idle_ms);
        if cold {
            // Idle hibernation is a local residency decision, not a handoff.
            self.begin_eviction(&candidate, false, effects);
        }
    }

    /// Fold a resource sample into the shedding latch, then act on it.
    ///
    /// The latch is the whole point. Shedding on the instantaneous crossing
    /// alone flaps: the eviction relieves the pressure, admission resumes, the
    /// ceiling is crossed again. `PressureConfig` holds the node in shedding
    /// until every configured low watermark clears, so the node walks down to
    /// its target instead of oscillating around its ceiling.
    fn load_sampled(&mut self, load: pressure::Load, now_mono_ms: u64, effects: &mut Vec<Effect>) {
        let state = self.config.pressure.state(load, self.shedding);
        self.shedding = state.shedding;
        self.shed_reason = self
            .config
            .pressure
            .shedding_trigger(load, state.shedding)
            .or(state.trigger);
        if !state.shedding {
            // Relieved. Whatever was queued for capacity may proceed.
            self.pump_capacity(effects);
            self.evict_idle(now_mono_ms, effects);
            return;
        }
        // How far this sample asks the node to come down. A residency trigger
        // aims at the low watermark; a resource trigger takes a proportion of
        // what was just measured, because the effect of an eviction on RSS or
        // CPU is not visible until the next sample.
        let Some(reason) = self.shed_reason else {
            return;
        };
        self.shed_floor = self
            .config
            .pressure
            .release_target(load.resident_cells, reason);
        self.shed_toward_floor(effects);
    }

    /// Continue a latched walk down as each eviction lands.
    ///
    /// `shed_one` stands down while the latch is hot, because its stopping
    /// condition -- the waiter got in -- cannot be met until the node is
    /// relieved. This is the other half: a stopping condition that can be met,
    /// so the node reaches its floor at the speed evictions complete rather
    /// than one per sampling period. Serialized like every other eviction
    /// path, so a walk down never puts the whole working set in flight.
    fn shed_toward_floor(&mut self, effects: &mut Vec<Effect>) {
        if !self.shedding || self.occupied() <= self.shed_floor {
            return;
        }
        // Fill the permits rather than starting one and waiting for it. Each
        // proof is a round trip, so a serialized drain costs the number of
        // cells times that latency; running the bound's worth at once is the
        // difference between a walk down measured in seconds and one measured
        // in minutes.
        // Count what is already leaving against the target. `occupied` still
        // includes a cell whose proof is in flight, so comparing it directly
        // nominates cells the evictions already under way will account for,
        // and the node settles below its floor by up to the whole bound --
        // the mistake celld's eviction budget documents: spend the cut on
        // commit, never on nomination.
        while self.hibernation_permits.len() < self.config.max_hibernations
            && self
                .occupied()
                .saturating_sub(self.hibernation_permits.len())
                > self.shed_floor
        {
            let Some(victim) = self.shed_candidate() else {
                return;
            };
            if !self.begin_eviction(&victim, self.rebalances(), effects) {
                return;
            }
        }
    }

    fn activity_finished(&mut self, request: RequestId, effects: &mut Vec<Effect>) {
        // A write still on the output gate keeps its request pinned, so the
        // cell cannot be evicted before the write is proven durable. The unpin
        // moves to whichever path drains the last gate for this request.
        if self.gated_writes.values().any(|gate| gate.request == request) {
            self.gate_pinned.insert(request);
            return;
        }
        if let Some(id) = self.active_requests.remove(&request) {
            let now = self.now_mono_ms;
            if let Some(cell) = self.cells.get_mut(&id) {
                cell.last_used_mono_ms = now;
            }
        }
        self.shed_one(effects);
    }

    /// Open the output gate for a local write: hold its response until the
    /// cell's committed `position` is proven replicated. The request must still
    /// be a live local activity on its cell, resident at the epoch that
    /// committed the write; otherwise durability cannot be proven for it and
    /// the response fails rather than falsely acknowledging the write.
    fn wrote(&mut self, request: RequestId, position: u64, effects: &mut Vec<Effect>) {
        let held = self.active_requests.get(&request).and_then(|id| {
            match self.cells.get(id).map(|cell| &cell.phase) {
                Some(Phase::Resident { epoch }) => Some((id.clone(), *epoch)),
                _ => None,
            }
        });
        let Some((cell, epoch)) = held else {
            effects.push(Effect::ReleaseResponse {
                request,
                result: Err(RequestError::DurabilityUnproven),
            });
            return;
        };
        let op = self.op();
        self.gated_writes.insert(
            op,
            GatedWrite {
                request,
                cell: cell.clone(),
                epoch,
                position,
            },
        );
        effects.push(Effect::AwaitDurable {
            op,
            cell,
            epoch,
            position,
        });
    }

    /// A gated write's durability proof completed. Acknowledge the write only
    /// when the replica proved a position that *covers* it — a shorter proof
    /// (a lagging or lying replicator) fails it rather than acknowledging a
    /// write the node cannot actually restore. Any error fails it. A completion
    /// for a gate already drained (fence or deadline) is ignored — the
    /// versioned-op discipline used throughout the core.
    fn durable_reached(
        &mut self,
        op: OpId,
        result: Result<u64, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let Some(gate) = self.gated_writes.remove(&op) else {
            return;
        };
        let result = match result {
            Ok(durable) if durable >= gate.position => Ok(()),
            _ => Err(RequestError::DurabilityUnproven),
        };
        // Unpin before releasing: the cleanup this ends -- shedding, eviction --
        // is queued ahead of the response, so a caller that sees its write
        // acknowledged sees the residency it released too. `activity_finished`
        // re-checks the gate map, so a request holding a second gated write
        // simply re-pins itself here.
        if self.gate_pinned.remove(&gate.request) {
            self.activity_finished(gate.request, effects);
        }
        effects.push(Effect::ReleaseResponse {
            request: gate.request,
            result,
        });
    }

    /// How many cells are currently held resident by a non-hibernatable
    /// transport.
    fn outbound_pinned(&self) -> usize {
        self.cells
            .values()
            .filter(|cell| {
                cell.websockets
                    .values()
                    .any(|kind| *kind == WebSocketKind::Outbound)
            })
            .count()
    }

    fn websocket_opened(
        &mut self,
        id: &str,
        websocket: WebSocketId,
        kind: WebSocketKind,
        effects: &mut Vec<Effect>,
    ) {
        if self.fenced {
            return;
        }
        // A non-hibernatable transport holds its cell resident for as long as
        // it is open, so every one of them is a cell the node can never shed.
        // Pin the whole ceiling and there is nothing left to nominate:
        // residency cannot fall, admission waits on capacity that will never
        // be freed, and the node is wedged by its own applications. Cells that
        // already hold one are not counted again -- the budget is on how much
        // of the node is held, not on how many sockets exist.
        let already_pinned = self.cells.get(id).is_some_and(|cell| {
            cell.websockets
                .values()
                .any(|kind| *kind == WebSocketKind::Outbound)
        });
        let cell_outbound = self.cells.get(id).map_or(0, |cell| {
            cell.websockets
                .values()
                .filter(|kind| **kind == WebSocketKind::Outbound)
                .count()
        });
        if kind == WebSocketKind::Outbound
            && (cell_outbound >= self.config.max_outbound_websockets
                || (!already_pinned
                    && !pressure::may_pin_outbound(
                        self.outbound_pinned(),
                        self.config.pressure.resident_high,
                    )))
        {
            effects.push(Effect::CloseWebSocket {
                cell: id.to_string(),
                websocket,
            });
            return;
        }
        let Some(cell) = self.cells.get_mut(id) else {
            return;
        };
        cell.websockets.insert(websocket, kind);
    }

    fn websocket_closed(&mut self, id: &str, websocket: WebSocketId, effects: &mut Vec<Effect>) {
        if let Some(cell) = self.cells.get_mut(id) {
            cell.websockets.remove(&websocket);
        }
        self.shed_one(effects);
    }

    fn invalidate_remote(&mut self, id: &str, node: &str, epoch: Epoch) {
        self.node_lease_cache.remove(node);
        let rejected_sample = self.cells.get(id).and_then(|cell| match &cell.phase {
            Phase::Remote {
                node: current,
                epoch: current_epoch,
                capacity_sampled_ms,
                ..
            } if current == node && *current_epoch == epoch => *capacity_sampled_ms,
            _ => None,
        });
        if let Some(sample) = rejected_sample {
            self.capacity_rejections.insert(node.to_string(), sample);
            self.capacity_reservations.remove(node);
        }
        let Some(cell) = self.cells.get_mut(id) else {
            return;
        };
        if matches!(
            &cell.phase,
            Phase::Remote {
                node: current,
                epoch: current_epoch,
                ..
            } if current == node && *current_epoch == epoch
        ) {
            cell.phase = Phase::Dormant;
        }
    }

    fn fence(&mut self, effects: &mut Vec<Effect>) {
        if self.fenced {
            return;
        }
        self.fenced = true;
        self.activation_waiters.clear();
        self.activation_permits.clear();
        self.capacity_waiters.clear();
        self.active_requests.clear();
        // Any write still waiting on the output gate loses its cell here, so it
        // must fail rather than be acknowledged — the fence and the fail are
        // atomic. A late DurableReached for a drained op is ignored.
        self.gate_pinned.clear();
        for (_, gate) in std::mem::take(&mut self.gated_writes) {
            effects.push(Effect::ReleaseResponse {
                request: gate.request,
                result: Err(RequestError::NodeFenced),
            });
        }
        for (request, _) in std::mem::take(&mut self.node_lease_waiters) {
            effects.push(Effect::Complete {
                request,
                result: Err(RequestError::NodeFenced),
            });
        }
        self.node_wake_waiters.clear();
        let ids: Vec<CellId> = self.cells.keys().cloned().collect();
        for id in ids {
            let mut cell = self.cells.remove(&id).expect("id came from map");
            match &cell.phase {
                Phase::Starting { op, epoch } | Phase::Publishing { op, epoch } => {
                    self.retired_runtime_ops.insert(*op, (id.clone(), *epoch));
                }
                _ => {}
            }
            if let Some(epoch) = runtime_epoch(&cell.phase) {
                let op = self.op();
                effects.push(Effect::StopRuntime {
                    op,
                    cell: id.clone(),
                    epoch,
                    cause: StopCause::Fence,
                });
            }
            self.finish_requests(&id, &mut cell, Err(RequestError::NodeFenced), effects);
            cell.phase = Phase::Fenced;
            cell.waiting_for = None;
            cell.waiting_activation = None;
            cell.alarm = None;
            cell.alarm_wake = false;
            cell.websockets.clear();
            self.cells.insert(id, cell);
        }
    }

    fn find_cell(&self, predicate: impl Fn(&Phase) -> bool) -> Option<CellId> {
        self.cells
            .iter()
            .find_map(|(id, cell)| predicate(&cell.phase).then(|| id.clone()))
    }

    fn compensate_retired_runtime(
        &mut self,
        op: OpId,
        result: Result<(), Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let Some((cell, epoch)) = self.retired_runtime_ops.remove(&op) else {
            return;
        };
        // Definite failure created nothing. Success or ambiguity may have
        // created/published a runtime after authority was revoked, so cleanup
        // is mandatory and idempotent.
        if result != Err(Failure::Definite) {
            let cleanup = self.op();
            effects.push(Effect::StopRuntime {
                op: cleanup,
                cell,
                epoch,
                cause: StopCause::Cleanup,
            });
        }
    }
}

/// The sole state transition entry point.
/// The monotonic instant an event carries, if it carries one. Events without
/// a timestamp leave the remembered instant alone rather than resetting it.
/// The wall-clock reading an event carried, if it carried one.
fn event_now_ms(event: &Event) -> Option<u64> {
    match event {
        Event::StartNodeLease { now_ms, .. }
        | Event::SelfNodeLeaseRead { now_ms, .. }
        | Event::OwnerRead { now_ms, .. }
        | Event::NodeLeaseRead { now_ms, .. }
        | Event::TimerFired { now_ms, .. }
        | Event::AlarmObserved { now_ms, .. }
        | Event::AlarmFinished { now_ms, .. } => Some(*now_ms),
        _ => None,
    }
}

fn event_mono_ms(event: &Event) -> Option<u64> {
    match event {
        Event::SelfNodeLeaseRead { now_mono_ms, .. }
        | Event::NodeLeaseCasCompleted { now_mono_ms, .. }
        | Event::RequestAt { now_mono_ms, .. }
        | Event::CapacityRequestAt { now_mono_ms, .. }
        | Event::WakeHintAt { now_mono_ms, .. }
        | Event::TimerFired { now_mono_ms, .. }
        | Event::AlarmObserved { now_mono_ms, .. }
        | Event::AlarmFinished { now_mono_ms, .. }
        | Event::LoadSampled { now_mono_ms, .. } => Some(*now_mono_ms),
        _ => None,
    }
}

pub fn on_event(state: &mut State, event: Event) -> Vec<Effect> {
    let mut effects = Vec::new();
    if let Some(now_mono_ms) = event_mono_ms(&event) {
        state.now_mono_ms = state.now_mono_ms.max(now_mono_ms);
    }
    if let Some(now_ms) = event_now_ms(&event) {
        state.now_ms = state.now_ms.max(now_ms);
    }
    match event {
        Event::StartNodeLease { now_ms, spec } => {
            state.start_node_lease(now_ms, spec, &mut effects)
        }
        Event::SelfNodeLeaseRead {
            op,
            now_ms,
            now_mono_ms,
            result,
        } => state.read_self_node_lease(op, now_ms, now_mono_ms, result, &mut effects),
        Event::NodeLeaseCasCompleted {
            op,
            now_mono_ms,
            result,
        } => state.node_lease_cas_completed(op, now_mono_ms, result, &mut effects),
        Event::TimerFired {
            timer,
            now_ms,
            now_mono_ms,
        } => state.timer_fired(timer, now_ms, now_mono_ms, &mut effects),
        Event::Request { request, cell } => state.request(request, cell, 0, false, &mut effects),
        Event::RequestAt {
            request,
            cell,
            now_ms,
            ..
        } => state.request(request, cell, now_ms, false, &mut effects),
        Event::CapacityRequestAt {
            request,
            cell,
            now_ms,
            ..
        } => state.request(request, cell, now_ms, true, &mut effects),
        Event::WorkerRequest { request } => state.worker_request(request, &mut effects),
        Event::Cancel { request } => state.cancel(request),
        Event::ActivityFinished { request } => state.activity_finished(request, &mut effects),
        Event::Wrote { request, position } => state.wrote(request, position, &mut effects),
        Event::DurableReached { op, result } => state.durable_reached(op, result, &mut effects),
        Event::WebSocketOpened {
            cell,
            websocket,
            kind,
        } => state.websocket_opened(&cell, websocket, kind, &mut effects),
        Event::WebSocketClosed { cell, websocket } => {
            state.websocket_closed(&cell, websocket, &mut effects)
        }
        Event::AlarmObserved {
            cell,
            at_ms,
            covered,
            now_ms,
            now_mono_ms,
        } => state.alarm_observed(&cell, at_ms, covered, now_ms, now_mono_ms, &mut effects),
        Event::AlarmFinished {
            op,
            now_ms,
            now_mono_ms,
            result,
        } => state.alarm_finished(op, now_ms, now_mono_ms, result, &mut effects),
        Event::WakeHint { cell } => state.wake_hint(cell, 0, &mut effects),
        Event::WakeHintAt { cell, now_ms, .. } => state.wake_hint(cell, now_ms, &mut effects),
        Event::OwnerRead { op, now_ms, result } => {
            state.owner_read(op, now_ms, result, &mut effects)
        }
        Event::NodeLeaseRead { op, now_ms, result } => {
            state.node_lease_read(op, now_ms, result, &mut effects)
        }
        Event::CapacityPeersRead { op, now_ms, result } => {
            state.capacity_peers_read(op, now_ms, result, &mut effects)
        }
        Event::OwnerCasCompleted { op, result } => {
            state.owner_cas_completed(op, result, &mut effects)
        }
        Event::OwnerReleased { op, result } => state.owner_released(op, result),
        Event::RestoreCompleted { op, result } => state.restore_completed(op, result, &mut effects),
        Event::RuntimeStarted { op, result } => state.runtime_started(op, result, &mut effects),
        Event::Published { op, result } => state.published(op, result, &mut effects),
        Event::DurabilityChecked { op, result } => {
            state.durability_checked(op, result, &mut effects)
        }
        Event::RuntimeStopped { op } => state.runtime_stopped(op, &mut effects),
        Event::Evict { cell } => state.evict(&cell, &mut effects),
        Event::LoadSampled { load, now_mono_ms } => {
            state.load_sampled(load, now_mono_ms, &mut effects)
        }
        Event::InvalidateRemote { cell, node, epoch } => {
            state.invalidate_remote(&cell, &node, epoch)
        }
        Event::NodeFenced => state.fence_node(&mut effects),
    }
    state.pump_activations(&mut effects);
    state.update_node_lease_dependencies(state.now_mono_ms);
    if cfg!(debug_assertions) {
        state.validate().expect("state invariant");
    }
    state.arm_operation_deadlines(&mut effects);
    effects
}

fn same_node_lease(left: &NodeLeaseRecord, right: &NodeLeaseRecord) -> bool {
    left.node == right.node
        && left.addr == right.addr
        && left.expires_ms == right.expires_ms
        && left.peer_protocol == right.peer_protocol
        && left.generation == right.generation
}

fn phase_occupies_capacity(phase: &Phase) -> bool {
    matches!(
        phase,
        Phase::Acquiring { .. }
            | Phase::ReconcilingAcquire { .. }
            | Phase::Restoring { .. }
            | Phase::Starting { .. }
            | Phase::Publishing { .. }
            | Phase::EnsuringDurability { .. }
            | Phase::Cleaning { .. }
            | Phase::Resident { .. }
    )
}

fn phase_holds_activation(phase: &Phase) -> bool {
    matches!(
        phase,
        Phase::ReadingOwner { .. }
            | Phase::ReadingNodeLease { .. }
            | Phase::ReadingCapacity { .. }
            | Phase::WaitingCapacity
            | Phase::Acquiring { .. }
            | Phase::ReconcilingAcquire { .. }
            | Phase::Restoring { .. }
            | Phase::Starting { .. }
            | Phase::Publishing { .. }
            | Phase::Cleaning { .. }
    )
}

fn phase_depends_on_node_lease(phase: &Phase) -> bool {
    matches!(
        phase,
        Phase::WaitingActivation
            | Phase::ReadingOwner { .. }
            | Phase::ReadingNodeLease { .. }
            | Phase::ReadingCapacity { .. }
            | Phase::WaitingCapacity
            | Phase::Acquiring { .. }
            | Phase::ReconcilingAcquire { .. }
            | Phase::Restoring { .. }
            | Phase::Starting { .. }
            | Phase::Publishing { .. }
            | Phase::EnsuringDurability { .. }
            | Phase::Cleaning { .. }
            | Phase::Resident { .. }
    )
}

fn runtime_epoch(phase: &Phase) -> Option<Epoch> {
    match phase {
        Phase::Publishing { epoch, .. }
        | Phase::EnsuringDurability { epoch, .. }
        | Phase::Cleaning { epoch, .. }
        | Phase::Resident { epoch } => Some(*epoch),
        _ => None,
    }
}
