//! The node-lease lifecycle decision, reified sans-IO.
//!
//! `ownership.rs`'s `renewal_loop` is a tokio task that reads the monotonic
//! clock, CAS-renews an S3 object, and calls `std::process::exit(3)` inline
//! when the lease lapses. The DECISION buried in it — hold / renew / fence /
//! release — is a pure function of a few scalars; only the clock read and the
//! effects are I/O. Here that decision is extracted so it runs identically in
//! production (executor: tokio renew, real `process::exit`) and under a
//! deterministic executor (a state transition, no process dies).
//!
//! Authority is modeled at the NODE level, as production ships it: one lease
//! per node, and a lapse fences every cell the node owns at once. This is the
//! topology a per-cell CAS model idealizes away.

/// The monotonic, already-sampled inputs to one renewal tick. The clock reads
/// (`Instant::elapsed`) happen at the edge; the core sees only deltas, so it is
/// pure and replayable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeaseTick {
    /// Is a lease currently held? (`lease.as_ref().is_some()`)
    pub lease_held: bool,
    /// Cells this node is actively serving — the fence blast radius.
    pub active_cells: usize,
    /// `active_cells == 0 && idle >= linger`: a lazy node may shed here.
    pub idle_long_enough: bool,
    /// Monotonic ms since the last SUCCESSFUL renewal — the fence is measured
    /// on this, never the wall clock (a backward wall step must not suppress it).
    pub elapsed_since_ok_ms: u64,
    /// Monotonic ms since the last renewal ATTEMPT (renewal cadence is ttl/3).
    pub elapsed_since_renew_ms: u64,
    pub ttl_ms: u64,
    /// Lazy mode releases an idle lease instead of holding it; Continuous and
    /// Shadow both fence on lapse and never lazily release.
    pub lazy: bool,
}

/// What the executor must perform this tick. `Fence` is the effect that was
/// `std::process::exit(3)` inline; the core names it instead of causing it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseAction {
    /// No lease held — nothing to do.
    NoLease,
    /// Nothing this tick: lease is live and not yet due for renewal.
    Hold,
    /// CAS-renew the lease now.
    Renew,
    /// Drop the lease gracefully (idle lazy node, or a lapsed lease on a lazy
    /// node with nothing to protect). No halt.
    Release,
    /// Halt: authority was lost while cells still depend on it. In production
    /// the executor turns this into `process::exit(3)`.
    Fence,
}

/// The pre-renewal decision — a total function of one tick.
pub fn decide(t: &LeaseTick) -> LeaseAction {
    if !t.lease_held {
        return LeaseAction::NoLease;
    }
    // A lazy node with no active cells past its linger sheds proactively.
    if t.lazy && t.idle_long_enough {
        return LeaseAction::Release;
    }
    // Monotonic self-fence: too long since our own last successful renewal.
    if t.elapsed_since_ok_ms > t.ttl_ms {
        return fence_or_release(t.active_cells, t.lazy);
    }
    if t.elapsed_since_renew_ms >= t.ttl_ms / 3 {
        return LeaseAction::Renew;
    }
    LeaseAction::Hold
}

/// A renewal CAS returned 412 — another process holds our node id. Same choice
/// as a fence: halt if anything depends on us, else release quietly. Called by
/// the executor after it performs a `Renew` and sees the rejection.
pub fn on_renew_rejected(active_cells: usize, lazy: bool) -> LeaseAction {
    fence_or_release(active_cells, lazy)
}

/// Lost authority: fence when cells depend on it (any active cell, or a
/// non-lazy node that must stay authoritative), otherwise release.
fn fence_or_release(active_cells: usize, lazy: bool) -> LeaseAction {
    if active_cells > 0 || !lazy {
        LeaseAction::Fence
    } else {
        LeaseAction::Release
    }
}

/// Whether a node-session lease is still live at `now_ms` — the one comparison
/// the whole fleet shares (`ownership.rs`): a lease is live strictly while its
/// expiry is in the future.
pub fn lease_live(expires_ms: u64, now_ms: u64) -> bool {
    expires_ms > now_ms
}

/// A peer node-session record as dead-node reconciliation reads it. Built
/// from the bucket `NodeRec` at the edge; the core sees only these fields.
pub struct NodeRecord<'a> {
    pub expires_ms: u64,
    /// The node id the record claims — may differ from the key's node after a
    /// session-id reuse, in which case its ownership generation is not this
    /// key's to inherit.
    pub node: &'a str,
    pub ownership_index_generation: &'a str,
}

/// Classification of one `nodes/<key_node>.json` record during dead-node
/// reconciliation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeLiveness {
    /// The lease is still in the future — leave it alone.
    Live,
    /// Lapsed or absent: its cells may hold an armed alarm with no wake entry.
    /// The generation is the dead node's own, or empty when a different (reused)
    /// session wrote the record.
    Dead { ownership_index_generation: String },
}

/// `record` is the read of the node key (`None` if absent — also dead).
pub fn node_liveness(
    record: Option<NodeRecord>,
    key_node: &str,
    now_ms: u64,
) -> NodeLiveness {
    match record {
        Some(r) if lease_live(r.expires_ms, now_ms) => NodeLiveness::Live,
        Some(r) => NodeLiveness::Dead {
            ownership_index_generation: if r.node == key_node {
                r.ownership_index_generation.to_string()
            } else {
                String::new()
            },
        },
        None => NodeLiveness::Dead { ownership_index_generation: String::new() },
    }
}

/// Storage-timeout budgets a node must satisfy for its self-fence to mean
/// what it claims. Reified here because the relationship between celld's
/// SDK timeouts and its lease TTL is a correctness property, not a tuning
/// preference, and nothing asserted it before 2026-08-01.
#[derive(Clone, Copy, Debug)]
pub struct FenceBudget {
    /// Longest a single storage attempt may run.
    pub attempt_timeout_ms: u64,
    /// Longest a storage call may run including retries.
    pub operation_timeout_ms: u64,
    /// The node lease TTL — the deadline the self-fence enforces.
    pub ttl_ms: u64,
    /// Does the fence run independently of the renewal's I/O (its own
    /// watchdog), or only after the renewal call returns?
    pub fence_is_independent: bool,
}

/// Can this configuration fence within its TTL, whatever storage does?
///
/// The 2026-08-01 lab partition is the case: a black-holed endpoint made a
/// renewal wait unboundedly, and because the fence ran only after that call
/// returned, a 10-second TTL was exceeded by 124 seconds. Either the fence
/// is independent of the call (a watchdog), or every storage call must
/// finish well inside the TTL. celld now does BOTH; this states why either
/// alone is insufficient.
pub fn fence_is_timely(b: &FenceBudget) -> bool {
    if b.fence_is_independent {
        return true;
    }
    // Without independence the fence inherits the call's worst case, and it
    // must still leave room to act before the TTL expires.
    b.operation_timeout_ms < b.ttl_ms && b.attempt_timeout_ms < b.ttl_ms
}
