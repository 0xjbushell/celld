//! Retry policy for incomplete dead-node reconciliation passes.
//!
//! Reconciliation may fail because storage is transiently unavailable or
//! because a historical cell is incompatible with the current deployment.
//! Neither case may turn the elected waker into a tight restore loop. The
//! async executor owns timers and deployment-change wakeups; this module owns
//! only the pure, saturating backoff decision.

const MAX_BACKOFF_SHIFT: u32 = 6;

/// Delay after `failure_count` consecutive incomplete reconciliation passes.
///
/// Counts start at one: the first failure waits one ordinary waker tick, then
/// delays double through 64 ticks. A successful pass removes the caller's
/// retry state.
pub fn retry_delay_ms(tick_ms: u64, failure_count: u32) -> u64 {
    let shift = failure_count.saturating_sub(1).min(MAX_BACKOFF_SHIFT);
    tick_ms.saturating_mul(1_u64 << shift)
}

/// Parse one `node-cells/<node>/<generation>/<cell>` marker key into its GC
/// identity: the owning node and the indexed cell. The generation component
/// is deliberately discarded — ANY generation under a dead node's prefix is
/// debris. Matching only the node record's current generation permanently
/// strands every superseded generation's markers: a same-ID restart rolls
/// the record, and the old markers become unreachable garbage while the
/// record itself is retired.
pub fn parse_marker_key(key: &str) -> Option<(&str, &str)> {
    let rest = key.strip_prefix("node-cells/")?;
    let mut parts = rest.splitn(3, '/');
    let (node, generation, cell) = (parts.next()?, parts.next()?, parts.next()?);
    if node.is_empty() || generation.is_empty() || cell.is_empty() {
        return None;
    }
    Some((node, cell))
}
