//! The per-cell coordination lifecycle, reified sans-IO:
//! `on_event(&mut Cell, now_mono, wall_ms, Event) -> Vec<Effect>`. This is the
//! target production state machine that `ownership.rs` / the alarm scheduler
//! execute, and that a deterministic executor drives.
//!
//! Topology: authority is a NODE concern. There is no
//! per-cell renewal here — the node lease (`crate::lease`) is the single
//! heartbeat, and a cell's cell/own.json record is write-once-then-CAS. The
//! monotonic self-fence lives at the node level; a cell learns it lost
//! authority two ways: `NodeFenced` (the node lease lapsed) or `CasRejected`
//! (a competitor advanced this one cell's epoch).
//!
//! Alarms are celld's REAL mechanism, not an idealization: RUN-FIRST, not
//! claim-then-run (`service_due_alarm` is cell-local — `begin_alarm_handler` is
//! in-memory, the handler runs, then `finish_alarm_handler` deletes the row).
//! At-most-once across owners is NOT a durable claim; it is the epoch-prefix
//! restore (a superseded owner's writes land in a stale prefix, and restore
//! takes the highest epoch). So alarms are at-least-once — a rerun is possible
//! before the consume is durable — and I4' ("no rerun after DURABLE success")
//! holds through the store, not the decision.
//!
//! Wake tiers + fail-closed hibernation (I6/I7) fold in next.
use crate::alarm::alarm_retry;
use crate::alarm::AlarmRetry;
use crate::cell::acquire;
use crate::cell::Acquire;
use crate::cell::Owner;
use crate::Etag;

pub type Epoch = u64;
pub type Gen = u64;
pub type Ms = u64;

/// The node's view of the cell's alarm row. `retry`/`counted_retry` mirror the
/// persisted columns production keys its backoff and ceiling on
/// (`storage.rs`): `retry` drives the exponential backoff, `counted_retry` is
/// the subset counting toward abandonment. The policy itself is `alarm_retry`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AlarmLocal {
    pub gen: Gen,
    pub due_wall_ms: Ms,
    pub retry: u32,
    pub counted_retry: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Phase {
    Idle,
    Probing,
    Claiming { epoch: Epoch },
    Owned { epoch: Epoch, etag: Etag },
    Fenced,
}

/// What the executor feeds back after performing an `Effect`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// Demand: a request needs this cell resident on this node.
    Activate,
    /// Periodic tick (the clocks ride in `on_event`'s params).
    Tick,
    GetOwner { owner: Owner, etag: Etag },
    GetMissing,
    GetFailed,
    OwnerLive { live: bool },
    OwnerLeaseFailed,
    CasOk { epoch: Epoch, etag: Etag },
    CasRejected,
    CasFailed,
    /// Activation restore completed — the durable alarm truth at restore time
    /// (None = no alarm, or not yet durable).
    RestoreOk { alarm: Option<AlarmLocal> },
    RestoreFailed,
    /// The restored cell's isolate has completed startup. Startup includes
    /// loading the deployment and restoring actor identity; neither ownership
    /// nor a successful restore makes a cell routable.
    RuntimeReady,
    RuntimeFailed,
    /// Publishing the live runtime in the local registry completed. This is
    /// the activation commit point: only `PublishOk` makes the cell resident
    /// and serving.
    PublishOk,
    PublishFailed,
    /// User code on a resident cell called `setAlarm` (gen durably unique).
    ArmAlarm { gen: Gen, due_wall_ms: Ms },
    /// The handler finished (the executor sampled its outcome).
    /// `counts_against_limit` is false for failures the caller excuses (e.g. a
    /// shed under pressure), matching `finish_alarm_handler_with_retry_policy`.
    AlarmOutcome { gen: Gen, ok: bool, counts_against_limit: bool },
    /// The eviction sweep decided to hibernate this cell.
    BeginHibernate,
    /// The fail-closed hibernate hook's wake-entry PUT result.
    WakeEntryOk,
    WakeEntryFailed,
    /// The node lease lapsed: drop this cell.
    NodeFenced,
}

/// What the executor performs. No `CasRenew`: the node lease renews, not cells.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    Get,
    GetOwnerLease { node: String },
    CasCreate { epoch: Epoch },
    CasWrite { etag: Etag, epoch: Epoch, takeover: bool },
    RestoreTruth { epoch: Epoch },
    StartRuntime { epoch: Epoch },
    PublishResident { epoch: Epoch },
    StartServing { epoch: Epoch },
    StopServing,
    /// Write the alarm into the cell's replicated storage (lagged).
    ArmDurable { gen: Gen, due_wall_ms: Ms },
    /// Run the handler — the external effect. RUN-FIRST: no durable claim
    /// precedes it; the epoch prefix is what makes it safe across owners.
    RunAlarm { gen: Gen, retry: u32, epoch: Epoch },
    /// Durable delete of a completed/abandoned alarm.
    ConsumeAlarm { gen: Gen },
    /// The fail-closed hibernate hook: publish a durable wake entry so a
    /// hibernated alarm-bearing cell can be revived. Eviction waits on its OK.
    PutWakeEntry { gen: Gen, due_wall_ms: Ms },
    /// Activation consumed the hints — delete this cell's wake entries.
    DeleteWakeEntries,
}

pub struct Cell {
    pub node: String,
    pub phase: Phase,
    pub serving: bool,
    /// Isolate live, truth loaded, and published in the local registry.
    /// `Owned && !resident` is either hibernated-owned or activation debt.
    pub resident: bool,
    pub alarm: Option<AlarmLocal>,
    /// A fail-closed wake-entry PUT is in flight (mid-hibernation).
    pub hibernating: bool,
    inflight: bool,
    wake_pending: bool,
    restore_inflight: bool,
    runtime_start_inflight: bool,
    publish_inflight: bool,
    /// Handler in flight: no re-fire until the outcome lands.
    running: Option<Gen>,
    pending_owner: Option<(Owner, Etag)>,
}

impl Cell {
    pub fn new(node: impl Into<String>) -> Self {
        Cell {
            node: node.into(),
            phase: Phase::Idle,
            serving: false,
            resident: false,
            alarm: None,
            hibernating: false,
            inflight: false,
            wake_pending: false,
            restore_inflight: false,
            runtime_start_inflight: false,
            publish_inflight: false,
            running: None,
            pending_owner: None,
        }
    }

    /// A cell whose ownership is already held at `epoch` — the acquisition CAS
    /// won, or a sticky tier-2 restore of retained ownership — positioned
    /// exactly as the post-`CasOk` state (`Owned`, restore pending) so the
    /// executor can drive it restore→resident without re-running the CAS.
    /// Ownership is not in question here; that is the node lease's concern.
    pub fn owned(node: impl Into<String>, epoch: Epoch, etag: impl Into<Etag>) -> Self {
        let mut c = Cell::new(node);
        c.phase = Phase::Owned { epoch, etag: etag.into() };
        c.restore_inflight = true;
        c
    }

    pub fn ready(&self) -> bool {
        matches!(self.phase, Phase::Owned { .. }) && self.resident
    }

    fn epoch(&self) -> Option<Epoch> {
        match self.phase {
            Phase::Owned { epoch, .. } => Some(epoch),
            _ => None,
        }
    }

    /// The whole per-cell protocol — pure: no I/O, no awaits, no clock reads
    /// (the clocks are injected). Both clocks are supplied for symmetry with the
    /// executor's `Clock` port; `_now_mono` is currently unread (alarm backoff
    /// is wall-scheduled since B1) and returns when the fence folds in.
    pub fn on_event(&mut self, _now_mono: Ms, wall_ms: Ms, ev: Event) -> Vec<Effect> {
        let mut out = Vec::new();

        if ev == Event::NodeFenced {
            return self.fence();
        }

        match (&self.phase, ev) {
            (Phase::Idle, Event::Activate) => {
                self.wake_pending = true;
                if !self.inflight {
                    self.phase = Phase::Probing;
                    self.inflight = true;
                    out.push(Effect::Get);
                }
            }
            (Phase::Probing, Event::GetMissing) => {
                self.inflight = true;
                self.phase = Phase::Claiming { epoch: 1 };
                out.push(Effect::CasCreate { epoch: 1 });
            }
            (Phase::Probing, Event::GetOwner { owner, etag }) => {
                self.inflight = false;
                match acquire(Some(&owner), &self.node, None) {
                    Acquire::Take { epoch, takeover } => {
                        self.begin_claim(epoch, etag, takeover, &mut out);
                    }
                    Acquire::NeedLiveness => {
                        self.inflight = true;
                        let node = owner.node.clone();
                        self.pending_owner = Some((owner, etag));
                        out.push(Effect::GetOwnerLease { node });
                    }
                    Acquire::GiveUp | Acquire::Defer => self.give_up(),
                }
            }
            (Phase::Probing, Event::OwnerLive { live }) => {
                self.inflight = false;
                if let Some((owner, etag)) = self.pending_owner.take() {
                    match acquire(Some(&owner), &self.node, Some(live)) {
                        Acquire::Take { epoch, takeover } => {
                            self.begin_claim(epoch, etag, takeover, &mut out);
                        }
                        Acquire::Defer | Acquire::GiveUp | Acquire::NeedLiveness => {
                            self.give_up();
                        }
                    }
                }
            }
            (Phase::Probing, Event::GetFailed | Event::OwnerLeaseFailed) => {
                self.pending_owner = None;
                self.give_up();
            }
            (Phase::Claiming { epoch }, Event::CasOk { epoch: got, etag }) if *epoch == got => {
                self.inflight = false;
                self.phase = Phase::Owned { epoch: got, etag };
                self.restore_inflight = true;
                out.push(Effect::RestoreTruth { epoch: got });
            }
            (Phase::Claiming { .. }, Event::CasRejected | Event::CasFailed) => {
                self.give_up();
            }
            (Phase::Owned { epoch, .. }, Event::RestoreOk { alarm }) if self.restore_inflight => {
                self.restore_inflight = false;
                self.alarm = alarm; // durable truth wins over any heap value
                self.runtime_start_inflight = true;
                out.push(Effect::StartRuntime { epoch: *epoch });
            }
            (Phase::Owned { epoch, .. }, Event::RuntimeReady)
                if self.runtime_start_inflight =>
            {
                self.runtime_start_inflight = false;
                self.publish_inflight = true;
                out.push(Effect::PublishResident { epoch: *epoch });
            }
            (Phase::Owned { epoch, .. }, Event::PublishOk) if self.publish_inflight => {
                self.publish_inflight = false;
                self.resident = true;
                self.serving = true;
                self.wake_pending = false;
                out.push(Effect::StartServing { epoch: *epoch });
                out.push(Effect::DeleteWakeEntries);
            }
            // ---- fail-closed hibernation ----
            (Phase::Owned { .. }, Event::BeginHibernate)
                if self.resident && !self.hibernating && self.running.is_none() =>
            {
                match self.alarm {
                    // FAIL CLOSED: no eviction until the wake entry is durable.
                    Some(a) => {
                        self.hibernating = true;
                        out.push(Effect::PutWakeEntry { gen: a.gen, due_wall_ms: a.due_wall_ms });
                    }
                    // No alarm to wake for: hibernate immediately.
                    None => {
                        self.resident = false;
                        self.serving = false;
                        out.push(Effect::StopServing);
                    }
                }
            }
            (Phase::Owned { .. }, Event::WakeEntryOk) if self.hibernating => {
                self.hibernating = false;
                // Re-check activity after the round trip; the alarm may have
                // begun firing inside the window. The heap keeps `alarm` (tier-2).
                if self.running.is_none() {
                    self.resident = false;
                    self.serving = false;
                    out.push(Effect::StopServing);
                }
            }
            (Phase::Owned { .. }, Event::WakeEntryFailed) if self.hibernating => {
                // The entry PUT failed: stay resident (the pin is the fallback).
                // An executor that skipped this gate would evict and lose the wake.
                self.hibernating = false;
            }
            (Phase::Owned { .. }, Event::RestoreFailed) => {
                self.restore_inflight = false;
            }
            (Phase::Owned { .. }, Event::RuntimeFailed) => {
                self.runtime_start_inflight = false;
            }
            (Phase::Owned { .. }, Event::PublishFailed) => {
                self.publish_inflight = false;
            }
            (Phase::Owned { .. }, Event::CasRejected) => {
                out = self.fence();
            }
            // ---- alarm: arm / run-first fire / consume ----
            (Phase::Owned { .. }, Event::ArmAlarm { gen, due_wall_ms }) if self.resident => {
                self.alarm = Some(AlarmLocal { gen, due_wall_ms, retry: 0, counted_retry: 0 });
                out.push(Effect::ArmDurable { gen, due_wall_ms });
            }
            (Phase::Owned { epoch, .. }, Event::Tick) => {
                let epoch = *epoch;
                // tier-1 resident scheduler: RUN-FIRST when due (celld's real
                // cell-local fire — no durable claim precedes the handler).
                // Backoff is encoded in `due_wall_ms` (rescheduled forward on
                // failure, as production reschedules `at_ms`), so the due gate
                // is the only gate — no separate monotonic timer.
                if self.resident && self.serving && self.running.is_none() {
                    if let Some(a) = self.alarm {
                        if wall_ms >= a.due_wall_ms {
                            self.running = Some(a.gen);
                            out.push(Effect::RunAlarm { gen: a.gen, retry: a.retry, epoch });
                        }
                    }
                }
                // tier-2: hibernated-owned with a due alarm re-activates. Sticky
                // (same epoch): the node kept its lease and fenced epoch, so this
                // is a local restore, not a re-acquisition.
                let alarm_due = self.alarm.is_some_and(|a| wall_ms >= a.due_wall_ms);
                if !self.resident
                    && !self.hibernating
                    && !self.restore_inflight
                    && !self.runtime_start_inflight
                    && !self.publish_inflight
                    && (self.wake_pending || alarm_due)
                {
                    self.restore_inflight = true;
                    out.push(Effect::RestoreTruth { epoch });
                }
            }
            (Phase::Owned { epoch, .. }, Event::Activate) => {
                self.wake_pending = true;
                if !self.resident
                    && !self.hibernating
                    && !self.restore_inflight
                    && !self.runtime_start_inflight
                    && !self.publish_inflight
                {
                    self.restore_inflight = true;
                    out.push(Effect::RestoreTruth { epoch: *epoch });
                }
            }
            (Phase::Owned { .. }, Event::AlarmOutcome { gen, ok, counts_against_limit }) => {
                if self.running == Some(gen) {
                    self.running = None;
                }
                if let Some(mut a) = self.alarm {
                    if a.gen == gen {
                        // A failure reschedules by `alarm::alarm_retry` — the
                        // SAME schedule production persists (storage.rs) —
                        // bumping the counters, or abandons at the ceiling.
                        // Success consumes durably.
                        let retry = (!ok).then(|| {
                            alarm_retry(
                                wall_ms as i64,
                                a.retry as i64,
                                a.counted_retry as i64,
                                counts_against_limit,
                            )
                        });
                        match retry {
                            Some(AlarmRetry::Retry { at_ms }) => {
                                a.due_wall_ms = at_ms as Ms;
                                a.retry += 1;
                                a.counted_retry += u32::from(counts_against_limit);
                                self.alarm = Some(a);
                            }
                            // success, or the retry ceiling: abandon durably.
                            None | Some(AlarmRetry::GiveUp) => {
                                self.alarm = None;
                                out.push(Effect::ConsumeAlarm { gen });
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        out
    }

    fn begin_claim(&mut self, epoch: Epoch, etag: Etag, takeover: bool, out: &mut Vec<Effect>) {
        self.phase = Phase::Claiming { epoch };
        self.inflight = true;
        out.push(Effect::CasWrite { etag, epoch, takeover });
    }

    fn give_up(&mut self) {
        self.phase = Phase::Idle;
        self.inflight = false;
        self.wake_pending = false;
    }

    fn fence(&mut self) -> Vec<Effect> {
        let was_serving = self.serving;
        self.phase = Phase::Fenced;
        self.serving = false;
        self.resident = false;
        self.hibernating = false;
        self.restore_inflight = false;
        self.runtime_start_inflight = false;
        self.publish_inflight = false;
        self.inflight = false;
        self.wake_pending = false;
        self.alarm = None;
        self.running = None;
        self.pending_owner = None;
        if was_serving {
            vec![Effect::StopServing]
        } else {
            vec![]
        }
    }
}
