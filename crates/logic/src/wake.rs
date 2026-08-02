//! Alarm-wake entry reconciliation, sans-IO — plus the wake key scheme.
//!
//! `WakeCore`'s `decide` is a pure transition; `reconcile` drives it against an
//! `ObjectStore` port, so the identical logic runs against real S3 in production
//! (`crate::wake::WakeFlusher` wraps it) or any other `ObjectStore`. The key
//! scheme (`entry_key` / `parse_entry_key`) lives here so both sides share one
//! definition.
use super::Ms;
use super::ObjectStore;
use super::StoreError;
use std::collections::HashMap;

/// Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

/// The time bucket a due timestamp falls in: minute precision, UTC,
/// lexicographically ordered so the waker LISTs due buckets in order.
fn minute_bucket(due_ms: i64) -> String {
    let mins = due_ms.div_euclid(60_000);
    let (y, mo, d) = civil_from_days(mins.div_euclid(1440));
    let m = mins.rem_euclid(1440);
    format!("{y:04}-{mo:02}-{d:02}T{:02}:{:02}", m / 60, m % 60)
}

pub fn entry_key(due_ms: i64, cell: &str) -> String {
    format!("wake/{}/{}", minute_bucket(due_ms), cell)
}

fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = y - i64::from(m <= 2);
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = i64::from(if m > 2 { m - 3 } else { m + 9 });
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

/// Inverse of `entry_key`: (due minute floor in ms, cell scope).
pub fn parse_entry_key(key: &str) -> Option<(i64, String)> {
    let rest = key.strip_prefix("wake/")?;
    let (minute, cell) = rest.split_at(rest.char_indices().nth(16)?.0);
    let cell = cell.strip_prefix('/')?;
    let y: i64 = minute.get(0..4)?.parse().ok()?;
    let mo: u32 = minute.get(5..7)?.parse().ok()?;
    let d: u32 = minute.get(8..10)?.parse().ok()?;
    let h: i64 = minute.get(11..13)?.parse().ok()?;
    let mi: i64 = minute.get(14..16)?.parse().ok()?;
    if minute.get(4..5)? != "-" || minute.get(10..11)? != "T" {
        return None;
    }
    let mins = days_from_civil(y, mo, d) * 1440 + h * 60 + mi;
    Some((mins * 60_000, cell.to_string()))
}

/// What the bucket needs for one cell given its committed alarm — the pure
/// transition's output, performed by `reconcile`.
pub enum Op {
    Put { key: String, due_ms: Ms },
    Delete { key: String },
}

/// What the bucket is believed to hold for one cell.
struct Entry {
    due_ms: Ms,
    key: String,
    /// True once this core PUT the object and saw it succeed. An adopted entry
    /// is believed-present but unproven: it may be deleted when its alarm is
    /// consumed, but may never satisfy the fail-closed eviction gate.
    verified: bool,
    /// A consume-delete for this exact key has been DECIDED but not yet
    /// performed. The executor's delete is asynchronous, and in that window
    /// an arm must not ride on an entry that is about to vanish — that race
    /// acked alarms with no durable entry (the SIGKILL deep-gate failure).
    /// An arm during the window cancels the delete instead.
    delete_pending: bool,
}

/// The reified `WakeFlusher`: owned state, no lock, no I/O.
#[derive(Default)]
pub struct WakeCore {
    flushed: HashMap<String, Entry>,
}

impl WakeCore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Arm-time decision: the PUT that must land before this arm is acked to
    /// the application, or `None` when the durable bound already covers it. A
    /// proven entry at the same or an EARLIER minute suffices — a stale-early
    /// entry costs one spurious wake, never a lost one — so recurring re-arms
    /// within a covered minute and postponements are free; only tightening
    /// pays the synchronous PUT.
    pub fn arm(&mut self, cell: &str, next_alarm_ms: Ms) -> Option<Op> {
        if next_alarm_ms < 0 {
            return None; // deletes are never synchronous
        }
        let want = entry_key(next_alarm_ms, cell);
        match self.flushed.get_mut(cell) {
            // Riding on an entry a consume is about to delete would ack an
            // alarm that ends up entryless. Cancel that delete instead: this
            // arm now owns the entry, and `take_delete` will refuse it.
            Some(e) if e.verified && e.key <= want && e.delete_pending => {
                e.delete_pending = false;
                if e.key == want {
                    None
                } else {
                    // The entry is stale-early: keep it (it still covers) but
                    // re-assert at the wanted key so coverage is exact and the
                    // cancelled delete cannot strand a later postponement.
                    Some(Op::Put { key: want, due_ms: next_alarm_ms })
                }
            }
            Some(e) if e.verified && e.key <= want => None,
            _ => Some(Op::Put { key: want, due_ms: next_alarm_ms }),
        }
    }

    /// Pure transition — byte-for-byte `WakeFlusher::ops`. The entry key, not
    /// the exact due time, is an entry's identity: two alarms in one minute
    /// share it.
    ///
    /// Ordering: a moved entry PUTs the new key before deleting the old one,
    /// so no schedule point leaves an armed alarm entryless — a crash between
    /// the two ops strands one extra entry (one spurious wake), never zero.
    ///
    /// `consume_durable` gates the final delete of a consumed alarm: false
    /// while the consuming commit is not yet replicated, in which case the
    /// entry must outlive the local commit — deleting it and then losing the
    /// commit to the replication lag would leave replicated truth armed with
    /// no entry, the one unrecoverable state.
    pub fn decide(
        &mut self,
        cell: &str,
        next_alarm_ms: Ms,
        consume_durable: bool,
    ) -> Vec<Op> {
        if next_alarm_ms < 0 {
            return match self.flushed.get_mut(cell) {
                Some(e) if consume_durable => {
                    // Arm the guard: an arm before this delete lands cancels
                    // it, and `take_delete` re-checks at execution time.
                    e.delete_pending = true;
                    vec![Op::Delete { key: e.key.clone() }]
                }
                _ => vec![],
            };
        }
        let held = self.flushed.get(cell);
        let want = entry_key(next_alarm_ms, cell);
        match held {
            None => vec![Op::Put { key: want, due_ms: next_alarm_ms }],
            Some(e) if e.key != want => vec![
                Op::Put { key: want.clone(), due_ms: next_alarm_ms },
                Op::Delete { key: e.key.clone() },
            ],
            // adopted but never proven: assert it so the cell can hibernate
            Some(e) if !e.verified => {
                vec![Op::Put { key: want, due_ms: next_alarm_ms }]
            }
            Some(_) => vec![],
        }
    }

    /// Take responsibility for the entry a restored alarm implies. Without
    /// this, a cell revived anywhere but the process that hibernated it has no
    /// record to delete from: its entry outlives the alarm and re-wakes the
    /// cell on every lease lapse.
    pub fn adopt(&mut self, cell: &str, due_ms: Ms) {
        if due_ms < 0 {
            return;
        }
        self.flushed.entry(cell.to_string()).or_insert_with(|| Entry {
            due_ms,
            key: entry_key(due_ms, cell),
            verified: false,
            delete_pending: false,
        });
    }

    /// Is this exact committed alarm durably covered by a proven entry? The
    /// fail-closed gate: eviction of an alarm-bearing cell requires it.
    pub fn covered(&self, cell: &str, next_alarm_ms: Ms) -> bool {
        self.flushed
            .get(cell)
            .is_some_and(|e| e.verified && e.key == entry_key(next_alarm_ms, cell))
    }

    /// Entries whose cells this node hibernated and whose due time has arrived.
    /// Sorted, unlike production's `HashMap` walk, so the schedule is
    /// reproducible — determinism the sans-IO core owes its executor.
    pub fn due_cells(&self, now_ms: Ms) -> Vec<String> {
        let mut due: Vec<String> = self
            .flushed
            .iter()
            .filter(|(_, e)| e.due_ms <= now_ms)
            .map(|(cell, _)| cell.clone())
            .collect();
        due.sort();
        due
    }

    pub fn tracks(&self, cell: &str) -> bool {
        self.flushed.contains_key(cell)
    }

    /// A PUT landed: the entry is now proven present. Called by the celld
    /// binary's async executor (`WakeFlusher`) after a successful S3 PUT — a
    /// separate crate, so this is `pub`.
    pub fn confirm_put(&mut self, cell: &str, due_ms: Ms, key: String) {
        self.flushed.insert(
            cell.to_string(),
            Entry { due_ms, key, verified: true, delete_pending: false },
        );
    }

    /// Immediately before performing a consume-delete: is it still wanted?
    /// False when an arm rode in during the async window and cancelled it —
    /// performing the delete then would strand that acked alarm.
    pub fn take_delete(&mut self, cell: &str, key: &str) -> bool {
        match self.flushed.get_mut(cell) {
            Some(e) if e.key == key && e.delete_pending => {
                e.delete_pending = false;
                true
            }
            // No entry (already retired) or a different key: the move-delete
            // path, which is always safe — a new entry was PUT first.
            None => true,
            Some(e) => e.key != key,
        }
    }

    /// Stop tracking this cell — after a delete, or when a wake resolved to a
    /// remote owner and its alarm is no longer ours.
    pub fn forget(&mut self, cell: &str) {
        self.flushed.remove(cell);
    }

    /// A delete of `key` was performed. Forget the cell only if that key is
    /// still the tracked entry: with put-before-delete ordering, a moved
    /// entry's confirm already replaced the tracked key, and deleting the old
    /// key must not drop belief in the new one.
    pub fn retire(&mut self, cell: &str, key: &str) {
        if self.flushed.get(cell).is_some_and(|e| e.key == key) {
            self.flushed.remove(cell);
        }
    }
}

/// The sweep executor's per-cell hint decisions, shared with production.
///
/// A cell revived by another process carries a wake entry this core never
/// PUT; the activation hint (the alarm it restored with) is adopted so a
/// consume deletes that entry instead of orphaning it. The hint is consumed
/// EXACTLY ONCE: once the consumed cell is no longer tracked, a still-latched
/// hint re-adopts a phantom entry every cycle and re-deletes it forever —
/// the op-quiescence violation `sweep_cell`'s test pins.
pub fn should_adopt_hint(tracks: bool, hint_ms: Ms) -> bool {
    !tracks && hint_ms >= 0
}

/// After the cycle's ops ran: is the activation hint spent? True exactly when
/// the committed alarm is consumed and nothing is tracked anymore.
pub fn hint_consumed(mirror_ms: Ms, tracks_after: bool) -> bool {
    mirror_ms < 0 && !tracks_after
}

/// May this node take the singleton waker-role lease — because it already holds
/// it, or the current holder's lease has expired? An exactly-expired lease MUST
/// be claimable (`<=`, not `<`): the waker is a SINGLE role, so a claim that
/// stalls on the boundary leaves every hibernated cell's alarm unwoken until
/// some other node happens to reclaim.
pub fn waker_may_claim(held_by_us: bool, expires_ms: Ms, now_ms: Ms) -> bool {
    held_by_us || expires_ms <= now_ms
}

/// One maintenance cycle for one cell — the sans-IO twin of the production
/// flush loop. Returns the updated activation hint (`-1` once consumed).
pub fn sweep_cell<S: ObjectStore>(
    core: &mut WakeCore,
    store: &mut S,
    cell: &str,
    mirror_ms: Ms,
    hint_ms: Ms,
    consume_durable: bool,
) -> Ms {
    if should_adopt_hint(core.tracks(cell), hint_ms) {
        core.adopt(cell, hint_ms);
    }
    reconcile(core, store, cell, mirror_ms, consume_durable);
    if hint_consumed(mirror_ms, core.tracks(cell)) {
        -1
    } else {
        hint_ms
    }
}

/// Reconcile one cell against the store — the sans-IO executor for wake. This
/// is `WakeFlusher::reconcile` with `aws_sdk_s3::Client` replaced by an
/// `ObjectStore` port: identical control flow, two backends. Failed PUTs keep
/// local state unchanged so the next tick retries (an entry may be late, never
/// silently absent); failed deletes drop matching state anyway — a stale
/// entry is one spurious wake, the cheaper error.
pub fn reconcile<S: ObjectStore>(
    core: &mut WakeCore,
    store: &mut S,
    cell: &str,
    next_alarm_ms: Ms,
    consume_durable: bool,
) {
    for op in core.decide(cell, next_alarm_ms, consume_durable) {
        match op {
            Op::Put { key, due_ms } => {
                let body = format!("{{\"cell\":{cell:?},\"due_ms\":{due_ms}}}");
                match store.put(&key, body.as_bytes()) {
                    Ok(_) => core.confirm_put(cell, due_ms, key),
                    // A later Delete in this batch depends on this PUT: with
                    // the new entry absent, deleting the old one would leave
                    // the armed alarm entryless. Abort; the next tick retries.
                    Err(StoreError::Precondition | StoreError::Transient) => return,
                }
            }
            Op::Delete { key } => {
                // Re-check under the core's state: an arm may have cancelled
                // this delete while it was queued.
                if core.take_delete(cell, &key) {
                    let _ = store.delete(&key);
                    core.retire(cell, &key);
                }
            }
        }
    }
}
