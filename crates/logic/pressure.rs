//! `PressureConfig::state` reified sans-IO: load shedding is a pure classifier
//! of a resource sample plus prior shedding state (the hysteresis latch). No
//! I/O, no clock — the env read that builds the config stays at the edge
//! (`main.rs`). This is the degenerate case of the pattern: a pure decision
//! with no ports and no effects, included to prove the seam is uniform. It is
//! the extraction target for `main.rs`'s private `PressureConfig`.

/// Watermarks and ceilings. Built once from the environment by the caller;
/// the core never reads the environment itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct PressureConfig {
    pub resident_high: Option<usize>,
    pub resident_low: usize,
    pub rss_high_bytes: Option<u64>,
    pub cpu_high_x100: Option<u64>,
}

/// A resource sample — the only input the classifier reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Load {
    pub resident_cells: usize,
    pub rss_bytes: u64,
    pub cpu_percent_x100: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PressureState {
    pub admission_blocked: bool,
    pub shedding: bool,
    pub trigger: Option<&'static str>,
}

impl PressureConfig {
    /// The first ceiling the sample crosses, or `None`.
    pub fn trigger(self, s: Load) -> Option<&'static str> {
        if self.resident_high.is_some_and(|h| s.resident_cells >= h) {
            return Some("resident_cells");
        }
        if self.rss_high_bytes.is_some_and(|h| s.rss_bytes >= h) {
            return Some("rss");
        }
        if self.cpu_high_x100.is_some_and(|h| s.cpu_percent_x100 >= h) {
            return Some("cpu");
        }
        None
    }

    /// Back under every low watermark (80% of each high) — the latch release.
    pub fn relieved(self, s: Load) -> bool {
        let resident_ok =
            self.resident_high.is_none_or(|_| s.resident_cells <= self.resident_low);
        let rss_ok = self
            .rss_high_bytes
            .is_none_or(|h| s.rss_bytes <= h.saturating_mul(4) / 5);
        let cpu_ok = self
            .cpu_high_x100
            .is_none_or(|h| s.cpu_percent_x100 <= h.saturating_mul(4) / 5);
        resident_ok && rss_ok && cpu_ok
    }

    /// How far to shed down to for a given trigger.
    pub fn release_target(self, resident_cells: usize, reason: &str) -> usize {
        if reason == "resident_cells" {
            self.resident_low
        } else {
            resident_cells.saturating_sub((resident_cells / 10).max(1))
        }
    }

    /// The resource whose low watermark still holds the shedding latch. An
    /// instantaneous trigger wins; inside a hysteresis band, preserve the
    /// resource-specific reason until every configured low watermark clears.
    pub fn shedding_trigger(
        self,
        s: Load,
        was_shedding: bool,
    ) -> Option<&'static str> {
        if let Some(trigger) = self.trigger(s) {
            return Some(trigger);
        }
        if !was_shedding || self.relieved(s) {
            return None;
        }
        if self.resident_high.is_some() && s.resident_cells > self.resident_low {
            return Some("resident_cells");
        }
        if self
            .rss_high_bytes
            .is_some_and(|h| s.rss_bytes > h.saturating_mul(4) / 5)
        {
            return Some("rss");
        }
        if self
            .cpu_high_x100
            .is_some_and(|h| s.cpu_percent_x100 > h.saturating_mul(4) / 5)
        {
            return Some("cpu");
        }
        None
    }

    /// The transition: `was_shedding` is the only carried state — the latch
    /// that keeps shedding between the high crossing and the low release.
    pub fn state(self, s: Load, was_shedding: bool) -> PressureState {
        let trigger = self.trigger(s);
        let admission_blocked = trigger.is_some();
        let shedding = self.shedding_trigger(s, was_shedding).is_some();
        PressureState { admission_blocked, shedding, trigger }
    }
}


/// May one more cell be admitted to residency right now?
///
/// Two independent facts gate admission. Conflating them turns graceful
/// degradation under load into a cliff.
///
/// `sampled_pressure` is the hysteresis latch from [`PressureConfig::state`]:
/// a periodic view of RSS, CPU, and residency, and the only thing entitled to
/// say "this node is overloaded". The headroom test below is the other, and it
/// is instantaneous.
///
/// A request that merely lost a race for the last slot has learned the second
/// fact, not the first. If such a waiter is allowed to assert node-wide
/// pressure, every other cell on the node is refused until the next sample —
/// including cells that would fit — and the waiters keep the latch hot for one
/// another. Capacity that exists then goes unused for a full sampling period.
pub fn may_admit(
    owned: usize,
    pending: usize,
    resident_high: Option<usize>,
    sampled_pressure: bool,
) -> bool {
    if sampled_pressure {
        return false;
    }
    resident_high.is_none_or(|high| owned.saturating_add(pending) < high)
}

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
        // At least one, always. The share alone rounds to zero below a
        // ceiling of two, which would make an outbound socket impossible on a
        // small node rather than merely budgeted -- celld's ceilings were
        // large enough that the floor never came up.
        pinned_cells < (high.saturating_mul(MAX_OUTBOUND_PIN_PERCENT) / 100).max(1)
    })
}
