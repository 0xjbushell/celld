//! The per-cell ownership decision, reified sans-IO.
//!
//! A cell's authority lives in one write-once-then-CAS record,
//! `cells/<cell>/own.json` = `{ node, epoch }`. Acquiring it is the
//! Probing→Claiming logic `ownership.rs` performs: read the record, and — for a
//! record owned by someone else — read that owner's node lease to see if it is
//! still alive, then either take over at `epoch+1` or defer. Only the GETs and
//! the CAS are I/O; the branching (epoch math, take-over-vs-advance-vs-defer)
//! is a pure function extracted here, so production's async `acquire_cell` and
//! a deterministic executor drive the identical decision.

/// The core's view of an ownership record. The serde wire type
/// (`ownership::Own`) maps to this at the I/O edge; the core carries no
/// serialization concern. An empty `node` is the relinquished/unowned marker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Owner {
    pub node: String,
    pub epoch: u64,
}

/// One step of acquisition. `NeedLiveness` models the two-GET shape: the caller
/// performs the owner's node-lease GET, then calls `acquire` again with
/// `owner_live: Some(..)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Acquire {
    /// No record (or it vanished) — nothing to acquire here.
    GiveUp,
    /// Owned by another node whose liveness is unknown — go read its lease.
    NeedLiveness,
    /// Owned by a live node — back off.
    Defer,
    /// Write ownership to me at `epoch` via if-match. `takeover` seizes a dead
    /// owner's cell (vs. advancing my own epoch); it only colors the activity
    /// log, but the two must not be conflated.
    Take { epoch: u64, takeover: bool },
}

/// The acquisition decision. `owner` is the observed record (`None` = absent);
/// `owner_live` is `None` until the owner's lease has been read.
pub fn acquire(owner: Option<&Owner>, me: &str, owner_live: Option<bool>) -> Acquire {
    let Some(o) = owner else {
        return Acquire::GiveUp;
    };
    // Reusing a node id (restart) still advances the epoch, so a superseded
    // lineage's late writes can never land in ours.
    if o.node == me {
        return Acquire::Take { epoch: o.epoch + 1, takeover: false };
    }
    match owner_live {
        None => Acquire::NeedLiveness,
        Some(true) => Acquire::Defer,
        Some(false) => Acquire::Take { epoch: o.epoch + 1, takeover: true },
    }
}

/// The record to write when relinquishing: node cleared, epoch retained so the
/// next owner must advance it (deleting would reset the fence sequence to one).
/// `None` if the on-disk owner no longer matches — someone else moved, and it
/// is not ours to release.
pub fn relinquish(owner: Option<&Owner>, me: &str, my_epoch: u64) -> Option<Owner> {
    match owner {
        Some(o) if o.node == me && o.epoch == my_epoch => {
            Some(Owner { node: String::new(), epoch: my_epoch })
        }
        _ => None,
    }
}
