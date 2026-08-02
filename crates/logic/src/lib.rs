//! `celld-logic`: the sans-IO coordination core — a pure library the celld
//! binary drives in production.
//!
//! Coordination logic is a pure function of `(state, event) -> effects`: no
//! async, no I/O, no clock reads, no rng, no locks. All nondeterminism is
//! confined to the PORTS below — `Clock`, `Rand`, `ObjectStore`, `Replicator`,
//! `CellRuntime`. Production supplies tokio + real S3 + a Litestream subprocess
//! behind them; because the core reads only the ports, an executor that
//! supplies seeded, deterministic models can replay it exactly. That
//! replayability is the whole point.
//!
//! Grown one subsystem at a time. Landed: the pure `wake`/`pressure` transitions,
//! the node-lease `lease` decision, and the per-cell `cell` acquisition. The
//! full ownership lifecycle (`on_event`) reifies `ownership.rs` as the single
//! source of the coordination decision — see the design pass in the doc.
#![allow(dead_code)] // interface module: ports and variants wire in per subsystem

pub mod alarm;
pub mod cache;
pub mod cell;
pub mod dead_node_reconciliation;
pub mod evict;
pub mod lease;
pub mod lifecycle;
pub mod peer;
pub mod pressure;
pub mod restore;
pub mod routing;
pub mod schedule;
pub mod sqlite;
pub mod wake;

/// S3 entity tag. Opaque to the core; equality is the only operation it needs.
pub type Etag = String;

/// Milliseconds. Signed to carry wake's `-1 = no alarm` sentinel and to make
/// monotonic deltas subtractable without underflow ceremony.
pub type Ms = i64;

/// Why a conditional store op did not take effect — the core branches on this
/// exactly as `ownership.rs` branches on a 412 versus a network error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreError {
    /// 412: an `if-none-match` / `if-match` precondition failed — someone else
    /// moved first. A *correct* refusal, not a fault.
    Precondition,
    /// Network / timeout / 5xx: the outcome is unknown. Retry; never assume it
    /// failed (an assumed-failed PUT that actually landed is a lost fence).
    Transient,
}

pub type StoreResult<T> = Result<T, StoreError>;

/// Compare-and-swap object store — the widest port. Every `aws_sdk_s3` call in
/// `ownership.rs` / `wake.rs` is one of these, with etag/412 semantics any
/// executor must reproduce exactly. Sync because the core is single-threaded;
/// production's async executor performs the same operations off-core.
pub trait ObjectStore {
    /// Body + etag, or `None` if absent.
    fn get(&mut self, key: &str) -> StoreResult<Option<(Vec<u8>, Etag)>>;
    /// `if-none-match: *` — create iff absent. `Err(Precondition)` if present.
    fn put_if_absent(&mut self, key: &str, body: &[u8]) -> StoreResult<Etag>;
    /// `if-match: etag` — replace iff unchanged. `Err(Precondition)` on drift.
    fn put_if_match(
        &mut self,
        key: &str,
        etag: &Etag,
        body: &[u8],
    ) -> StoreResult<Etag>;
    /// Unconditional PUT (wake entries need no CAS).
    fn put(&mut self, key: &str, body: &[u8]) -> StoreResult<Etag>;
    /// Keys under `prefix`, lexicographically ascending — S3 list order, which
    /// the wake scan relies on to stop at the first future bucket.
    fn list(&mut self, prefix: &str) -> StoreResult<Vec<(String, Etag)>>;
    /// Unconditional DELETE; deleting an absent key is not an error.
    fn delete(&mut self, key: &str) -> StoreResult<()>;
}

/// Time, split by trust. Wall is `SystemTime`: advisory, skewable, used only
/// for lease-expiry hints. Mono is `Instant`: the fence is monotonic by design
/// (`ownership.rs`), so a backward wall step can never suppress it.
pub trait Clock {
    fn wall_ms(&self) -> Ms;
    fn mono_ms(&self) -> Ms;
}

/// Seeded randomness. The load-bearing draw is the node-session id
/// (`main.rs`, `OsRng` in production); alarm-generation ids route here too so a
/// seeded executor can replay them.
pub trait Rand {
    fn next_u64(&mut self) -> u64;
}

/// Durability, kept a subprocess in production (Litestream). Pinned here; wired
/// in step 4. Behind this port an executor can apply replication lag, torn-write,
/// and stale-restore faults — proving coordination correctness given a
/// durability oracle.
pub trait Replicator {
    /// Begin replicating this cell at `epoch` (a new owner restores then serves).
    fn activate(&mut self, cell: &str, epoch: u64);
    /// Stop replicating (hibernate or release).
    fn hibernate(&mut self, cell: &str);
    /// Highest epoch whose writes are durably in the bucket — the lag oracle a
    /// stale-restore fault perturbs.
    fn epoch_replicated(&self, cell: &str) -> Option<u64>;
}

/// The V8 isolate, kept OUTSIDE the core (cells become scripted event sources).
/// The boundary already exists as the `CellJob` channel plus the
/// `next_alarm_ms` atomic, so this is minimal surgery; wired in step 5.
pub trait CellRuntime {
    fn dispatch(&mut self, cell: &str, job: CellJob);
}

/// Placeholder for the cell-runtime job vocabulary (fleshed out in step 5).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CellJob {
    Fetch,
    Alarm { gen: u64 },
}
