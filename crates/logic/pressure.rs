// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Memory-pressure shedding as a pure classifier of an RSS sample plus the
//! prior shedding state (the hysteresis latch). No I/O, no clock — the env
//! read that builds the config stays at the edge (`main.rs`).
//!
//! Residency is deliberately *not* here. A node's cell count is a hard cap
//! enforced at admission ([`crate::State::has_capacity`]), self-limiting and
//! known exactly; it is not a resource that needs a proactive walk down. This
//! classifier answers only the other question — "is this node out of memory
//! and must give cells back to recover?" — which a cell count cannot answer.
//! Conflating the two produced the placement churn and the admission wedge;
//! splitting them is what keeps each decision small.

/// Resource watermarks. Built once from the environment by the caller; the
/// core never reads the environment itself. Residency has no watermark here —
/// it is capped at admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct PressureConfig {
    pub rss_high_bytes: Option<u64>,
}

/// A resource sample — the only input the classifier reads. `resident_cells`
/// is carried so a resource trigger can size its walk down as a proportion of
/// what is actually resident.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Load {
    pub resident_cells: usize,
    pub rss_bytes: u64,
}

impl PressureConfig {
    /// Is the RSS sample over its ceiling, or is a prior crossing still above
    /// the low watermark (80% of high)?
    pub fn shedding(self, s: Load, was_shedding: bool) -> bool {
        self.rss_high_bytes.is_some_and(|h| {
            s.rss_bytes >= h || (was_shedding && s.rss_bytes > h.saturating_mul(4) / 5)
        })
    }

    /// How far to shed down to. A memory trigger takes a proportion of what was
    /// just measured because the effect of an eviction is not visible until
    /// the next RSS sample.
    pub fn release_target(resident_cells: usize) -> usize {
        resident_cells.saturating_sub((resident_cells / 10).max(1))
    }
}

pub const MAX_OUTBOUND_PIN_PERCENT: usize = 50;

/// May another cell be pinned resident by an outbound WebSocket?
///
/// An outbound socket is not hibernatable, so eviction refuses its cell for as
/// long as it is open. That is correct — a live host transport cannot survive
/// eviction — but it means every pinned cell is removed from the eviction
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
