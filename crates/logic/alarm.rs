//! Alarm retry policy, reified sans-IO. After a handler fails, `celld` persists
//! a backoff in SQLite (`storage.rs`) and abandons the alarm once a bounded
//! number of limit-counting failures accrue. That schedule is a pure function
//! of the row's retry counters; storage is its executor.
//!
//! NOTE: `lifecycle::on_event` models a SEPARATE in-memory attempt backoff. The
//! two converge when `on_event` is wired into production (the
//! `ensure_cell → on_event` slice).

use crate::Ms;

/// Failures that count toward the ceiling before the alarm is abandoned.
const RETRY_CEILING: i64 = 6;
/// Backoff base; the nth retry waits `BACKOFF_BASE_MS << min(retry, MAX_SHIFT)`.
const BACKOFF_BASE_MS: Ms = 2_000;
const MAX_BACKOFF_SHIFT: i64 = 6;

/// What to do with an alarm whose handler just failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AlarmRetry {
    /// The ceiling is reached; abandon the alarm.
    GiveUp,
    /// Reschedule for `at_ms`; the executor also bumps the retry counters.
    Retry { at_ms: Ms },
}

/// `retry` is the total failure count (drives the exponential backoff);
/// `counted_retry` is how many counted against the limit. `counts_against_limit`
/// is false for failures the caller excuses (e.g. a shed under pressure) — those
/// back off but never reach the ceiling.
pub fn alarm_retry(
    now_ms: Ms,
    retry: i64,
    counted_retry: i64,
    counts_against_limit: bool,
) -> AlarmRetry {
    if counts_against_limit && counted_retry >= RETRY_CEILING {
        AlarmRetry::GiveUp
    } else {
        let shift = retry.clamp(0, MAX_BACKOFF_SHIFT) as u32;
        let at_ms = now_ms.saturating_add(BACKOFF_BASE_MS * (1_i64 << shift));
        AlarmRetry::Retry { at_ms }
    }
}
