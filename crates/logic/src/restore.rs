//! Restore-source durability predicates, reified sans-IO. Activation restores a
//! cell's SQLite from the newest SAFE source — a local hibernation snapshot, a
//! local crash epoch, or the replicated bucket. Which source is a durability
//! decision: pick a stale one over a newer replica and the cell serves lost
//! writes; pick a needlessly-remote one and pay dozens of sequential round trips
//! (measured: 46, 0 local reuses in 910 activations, before the previous-epoch
//! lookup). The AVAILABILITY of each source is I/O (file checks, S3 LIST); these
//! predicates are the pure choices among them, and the safety fences within.

pub type Epoch = u64;

/// May a hibernation snapshot from the PREVIOUS epoch be reused? Ordinary idle
/// hibernation is followed by an epoch advance, so its cache sits under
/// `epoch - 1` — safe to reuse only when we did NOT take the cell over from
/// another node. A takeover means someone else may have written the cell while
/// we slept, so their newer durable state, not our stale snapshot, is
/// authoritative. Epoch 1 has no previous epoch.
pub fn previous_epoch_reusable(epoch: Epoch, took_over: bool) -> bool {
    epoch > 1 && !took_over
}

/// Restore the LOCAL crash epoch instead of the replicated bucket? Yes when
/// there is no replica, or the local epoch is at least as new as the newest
/// replicated one — a local restore is cheap, and a same-or-newer local epoch
/// carries every durable write the replica holds. A replica STRICTLY newer than
/// the local epoch wins: it may hold writes this node never saw.
pub fn local_epoch_wins(local_epoch: Epoch, remote_epoch: Option<Epoch>) -> bool {
    remote_epoch.is_none_or(|remote| local_epoch >= remote)
}

/// May a discovered local epoch be recovered during activation of `activating`?
/// Only when STRICTLY before it. Recovering an epoch `>= activating` would
/// reopen the ownership the activation just advanced past, letting two processes
/// open the same db — the strict `<` is the fence, not an optimization.
pub fn recoverable(candidate: Epoch, activating: Epoch) -> bool {
    candidate < activating
}

