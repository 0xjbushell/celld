// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! `PressureConfig::state` reified sans-IO: resource-pressure shedding as a
//! pure classifier of a sample plus prior shedding state (the hysteresis
//! latch). No I/O, no clock — the env read that builds the config stays at the
//! edge (`main.rs`).
//!
//! Residency is deliberately *not* here. A node's cell count is a hard cap
//! enforced at admission ([`crate::State::has_capacity`]), self-limiting and
//! known exactly; it is not a resource that needs a proactive walk down. This
//! classifier answers only the other question — "is this node out of memory or
//! CPU, and must give cells back to recover?" — which a cell count cannot
//! answer. Conflating the two produced the placement churn and the admission
//! wedge; splitting them is what keeps each decision small.

/// Resource watermarks. Built once from the environment by the caller; the
/// core never reads the environment itself. Residency has no watermark here —
/// it is capped at admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct PressureConfig {
    pub rss_high_bytes: Option<u64>,
    pub cpu_high_x100: Option<u64>,
}

/// A resource sample — the only input the classifier reads. `resident_cells`
/// is carried so a resource trigger can size its walk down as a proportion of
/// what is actually resident.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Load {
    pub resident_cells: usize,
    pub rss_bytes: u64,
    pub cpu_percent_x100: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PressureState {
    pub shedding: bool,
    pub trigger: Option<&'static str>,
}

impl PressureConfig {
    /// The first resource ceiling the sample crosses, or `None`.
    pub fn trigger(self, s: Load) -> Option<&'static str> {
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
        let rss_ok = self
            .rss_high_bytes
            .is_none_or(|h| s.rss_bytes <= h.saturating_mul(4) / 5);
        let cpu_ok = self
            .cpu_high_x100
            .is_none_or(|h| s.cpu_percent_x100 <= h.saturating_mul(4) / 5);
        rss_ok && cpu_ok
    }

    /// How far to shed down to. A resource trigger takes a proportion of what
    /// was just measured, because the effect of an eviction on RSS or CPU is
    /// not visible until the next sample.
    pub fn release_target(self, resident_cells: usize) -> usize {
        resident_cells.saturating_sub((resident_cells / 10).max(1))
    }

    /// The resource whose low watermark still holds the latch. An instantaneous
    /// trigger wins; inside a hysteresis band, preserve the resource-specific
    /// reason until every configured low watermark clears.
    pub fn shedding_trigger(self, s: Load, was_shedding: bool) -> Option<&'static str> {
        if let Some(trigger) = self.trigger(s) {
            return Some(trigger);
        }
        if !was_shedding || self.relieved(s) {
            return None;
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
        PressureState {
            shedding: self.shedding_trigger(s, was_shedding).is_some(),
            trigger: self.trigger(s),
        }
    }
}

pub const MAX_OUTBOUND_PIN_PERCENT: usize = 50;

/// May another cell be pinned resident by an outbound WebSocket?
///
/// An outbound socket is not hibernatable, so eviction refuses its cell for as
/// long as it is open. That is correct — a live host transport cannot survive
/// hibernation — but it means every pinned cell is removed from the eviction
/// pool. Pin the whole ceiling and a resource walk down has nothing to
/// nominate. The budget is node-wide, counted in pinned *cells* rather than
/// sockets, because one socket is enough to pin. `ceiling` is the hard resident
/// cap.
pub fn may_pin_outbound(pinned_cells: usize, ceiling: Option<usize>) -> bool {
    ceiling.is_none_or(|cap| {
        // At least one, always. The share alone rounds to zero below a ceiling
        // of two, which would make an outbound socket impossible on a small
        // node rather than merely budgeted.
        pinned_cells < (cap.saturating_mul(MAX_OUTBOUND_PIN_PERCENT) / 100).max(1)
    })
}
