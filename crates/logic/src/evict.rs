//! Maintenance-sweep decisions, reified sans-IO. The sweep in `main.rs` is the
//! densest remaining cluster of imperative coordination — idle/pressure
//! eviction, the alarm-pin residency gate, owed-alarm retry, death detection.
//! Each is a pure predicate over scalars the executor samples; the executor
//! keeps the locks, the registry, and the interleaving. This module grows one
//! decision at a time as the sweep is lifted.

use crate::Ms;

/// Whether an armed cell must stay resident, or may hibernate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AlarmPin {
    /// Keep the cell resident: the alarm fires too soon to bother, or its wake
    /// entry is not yet durable to catch it after hibernation.
    Hold,
    /// The alarm is safely covered; the cell may hibernate.
    Lift,
}

/// The fail-closed hibernation gate for an armed cell. A cell hibernates only
/// when its alarm is far enough out to beat the stay-resident break-even AND
/// its wake entry is DURABLE in the bucket — no durable entry, no eviction, so
/// a lost index can never strand an armed alarm.
///
/// `next_alarm_ms < 0` means no alarm is armed: nothing pins the cell.
pub fn alarm_pin(
    next_alarm_ms: Ms,
    now_ms: Ms,
    resident_ms: Ms,
    wake_covered: bool,
) -> AlarmPin {
    if next_alarm_ms < 0 {
        return AlarmPin::Lift;
    }
    if next_alarm_ms - now_ms > resident_ms && wake_covered {
        AlarmPin::Lift
    } else {
        AlarmPin::Hold
    }
}

/// Whether the sweep evicts a cell on this pass, or keeps it resident.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    Evict,
    Keep,
}

/// Ordinary recency eviction, with a pressure override. Under pressure the
/// sweep evicts regardless of how recently the cell was touched — shedding load
/// is the whole point; otherwise a cell is evicted only once it has been idle
/// for at least `idle_evict_s`. The armed-alarm pin is a separate, earlier gate
/// (`alarm_pin`); this decides an unpinned cell's fate.
pub fn idle(pressure_evict: bool, idle_s: u64, idle_evict_s: u64) -> Verdict {
    if pressure_evict || idle_s >= idle_evict_s {
        Verdict::Evict
    } else {
        Verdict::Keep
    }
}

/// The durability gate before hibernating a cell: never evict local state the
/// bucket cannot restore at the current epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplicaGate {
    /// The current epoch is verified durable in the bucket; hibernation may
    /// proceed.
    Durable,
    /// The epoch advanced since the replica check was sampled — the check is
    /// stale; retry next sweep, quietly.
    StaleCheck,
    /// The checked epoch has no bucket replica — replication is wedged; hold
    /// resident (the executor warns, deduplicated per epoch) rather than let the
    /// cell become unrestorable.
    Unreplicated,
}

/// `checked_epoch`/`replicated` come from a replica LIST taken earlier in the
/// sweep; `epoch` is the cell's epoch now. The epoch-staleness check wins first
/// — a moved epoch means the durability verdict is about the wrong lineage.
pub fn replica_gate(epoch: u64, checked_epoch: u64, replicated: bool) -> ReplicaGate {
    if epoch != checked_epoch {
        ReplicaGate::StaleCheck
    } else if !replicated {
        ReplicaGate::Unreplicated
    } else {
        ReplicaGate::Durable
    }
}

/// The pressure-eviction budget for one sweep pass — two counters that keep a
/// shed from overshooting or stalling. `to_evict` is the real cut needed to
/// reach the target; `to_nominate` is a small reserve beyond it so a pinned,
/// busy, or stale candidate cannot starve the pass. The load-bearing rule: the
/// real budget is spent ONLY on `commit`, called after a cell is actually
/// removed — spending it at nomination let candidates that never evicted eat the
/// cut and the controller settled below its floor.
#[derive(Clone, Copy, Debug)]
pub struct PressureBudget {
    to_evict: usize,
    to_nominate: usize,
}

impl PressureBudget {
    /// `reserve` (production: the hibernation concurrency) bounds how far past
    /// the real cut the pass may nominate candidates.
    pub fn new(resident_cells: usize, target: usize, reserve: usize) -> Self {
        let to_evict = resident_cells.saturating_sub(target);
        Self { to_evict, to_nominate: to_evict.saturating_add(reserve) }
    }

    /// Room to nominate another eviction candidate this pass.
    pub fn may_nominate(&self) -> bool {
        self.to_nominate > 0
    }

    /// Nominate a candidate — spends the reserve, not the real cut.
    pub fn nominate(&mut self) {
        self.to_nominate = self.to_nominate.saturating_sub(1);
    }

    /// Room to actually evict — the real cut, so the sweep never dips below the
    /// target watermark.
    pub fn may_evict(&self) -> bool {
        self.to_evict > 0
    }

    /// Commit a real eviction. Call ONLY after the cell is removed.
    pub fn commit(&mut self) {
        self.to_evict = self.to_evict.saturating_sub(1);
    }
}

/// One cell as the sweep's candidate selection reads it — a snapshot the
/// executor gathers (under its locks) so the selection itself is pure. No I/O,
/// no locks here.
pub struct CellState {
    pub scope: String,
    pub idle_s: u64,
    /// A regular (non-hibernatable) WebSocket pins the actor.
    pub has_regular_websocket: bool,
    /// Live hibernatable-socket count — a handoff cannot move a live transport.
    pub websocket_count: usize,
    pub epoch: u64,
}

/// The sweep's Phase-1 output: cells to replica-verify, and the subset selected
/// for pressure eviction.
#[derive(Default)]
pub struct CandidatePlan {
    /// `(scope, epoch)` to check for a durable replica (the executor's I/O step).
    pub replica_candidates: Vec<(String, u64)>,
    /// Scopes nominated for pressure eviction (spent the reserve budget).
    pub pressure_candidates: std::collections::HashSet<String>,
}

/// Phase 1 of the maintenance sweep, reified: pick eviction candidates from
/// `cells` (given in eviction-priority — idle descending — order), nominating
/// pressure candidates against `budget`. Pure; the executor performs the
/// replica I/O and the eviction against this plan.
pub fn plan_candidates(
    cells: &[CellState],
    idle_evict_s: u64,
    lazy_leases: bool,
    pressure_release: bool,
    budget: &mut Option<PressureBudget>,
) -> CandidatePlan {
    let mut plan = CandidatePlan::default();
    for c in cells {
        let pressure_evict = budget.as_ref().is_some_and(|b| b.may_nominate());
        // Idle-young cells are only touched under pressure.
        if c.idle_s < idle_evict_s && !pressure_evict {
            continue;
        }
        if c.has_regular_websocket {
            continue;
        }
        // A live host transport cannot move with ownership: skip when a lazy
        // lease or a release-policy pressure handoff would migrate it.
        let pressure_handoff = pressure_evict && pressure_release;
        if (lazy_leases || pressure_handoff) && c.websocket_count > 0 {
            continue;
        }
        plan.replica_candidates.push((c.scope.clone(), c.epoch));
        if pressure_evict {
            plan.pressure_candidates.insert(c.scope.clone());
            if let Some(b) = budget.as_mut() {
                b.nominate();
            }
        }
    }
    plan
}

/// The eviction verdict after the executor has won the close exclusion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PostClose {
    /// Remove the cell and hibernate it.
    Evict,
    /// Put it back — it became active, or its alarm changed, during the replica
    /// I/O window.
    Abort,
}

/// Phase 2's post-close decision: after `begin_close` succeeds, the sweep
/// re-reads the cell's idle time and alarm (both may have moved during the
/// replica verification I/O) and decides evict-or-abort. `alarm_unchanged` is
/// the fresh alarm compared against the value the pin decision saw — a change
/// means the coverage check is stale and a refire could be lost.
pub fn decide_post_close(
    current_idle: u64,
    idle_evict_s: u64,
    pressure_evict: bool,
    alarm_unchanged: bool,
) -> PostClose {
    if idle(pressure_evict, current_idle, idle_evict_s) == Verdict::Keep {
        return PostClose::Abort;
    }
    if !alarm_unchanged {
        return PostClose::Abort;
    }
    PostClose::Evict
}

/// Share of the residency ceiling that may be pinned by outbound sockets.
/// Half: enough headroom that the sweep always has something it is allowed to
/// evict, without making a legitimate socket workload useless.
pub const MAX_OUTBOUND_PIN_PERCENT: usize = 50;

/// May another cell be pinned resident by an outbound WebSocket?
///
/// An outbound socket is not hibernatable, so `plan_candidates` refuses to
/// evict its cell for as long as it is open. That is correct — a live host
/// transport cannot survive hibernation — but it means every pinned cell is
/// permanently removed from the eviction pool. Pin the whole ceiling and the
/// sweep has nothing left to nominate: residency can never fall, admission
/// waits for capacity that cannot be freed, and the node serves
/// `capacity_exhausted` until an application happens to close a socket.
///
/// A per-cell ceiling does not bound this. One socket each across a thousand
/// cells pins a thousand cells. The budget has to be node-wide, counted in
/// pinned *cells* rather than sockets, because one socket is enough to pin.
pub fn may_pin_outbound(pinned_cells: usize, resident_high: Option<usize>) -> bool {
    resident_high.is_none_or(|high| {
        pinned_cells < high.saturating_mul(MAX_OUTBOUND_PIN_PERCENT) / 100
    })
}
