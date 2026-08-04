//! Whether a failed peer dispatch may be re-sent.
//!
//! A cell runs on exactly one node, so every other node forwards to the
//! owner. When that forward fails, celld must decide between two bad
//! options: fail the caller, or re-send and risk running the request twice.
//! The deciding fact is how far the attempt got. A connection that was never
//! established carried no request bytes, so the owner's cell is untouched and
//! a fresh attempt cannot double-apply. Every other failure — a timeout after
//! the request was written, a truncated body, a decode error — leaves the
//! outcome ambiguous: the owner may have run it and lost the reply. Re-sending
//! those would break at-most-once execution.
//!
//! The case this exists for is a burst of cold activations, where TCP
//! handshakes stall past the client's connect timeout. Those forwards never
//! reached the owner, so surfacing them to the caller is a self-inflicted
//! error.

/// How far a dispatch attempt got before it failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Attempt {
    /// The peer answered that it no longer owns the cell. Nothing ran.
    NotOwner,
    /// The connection was never established. No bytes reached the owner.
    NeverConnected,
    /// The request was on the wire when the attempt failed. The owner may or
    /// may not have run it.
    Ambiguous,
}

/// Redispatches allowed per failure class, counted separately so a route that
/// is both stale and unreachable still terminates. The unreachable budget is
/// deliberately one: every attempt of that class costs a full connect timeout,
/// so each retry is also the latency a caller pays when the owner is genuinely
/// down, not just briefly congested.
pub const MAX_NOT_OWNER: u32 = 1;
pub const MAX_NEVER_CONNECTED: u32 = 1;

/// May a request that failed with `attempt` be dispatched again, given how
/// many redispatches of that class it has already spent?
pub fn may_redispatch(attempt: Attempt, spent: u32) -> bool {
    match attempt {
        Attempt::Ambiguous => false,
        Attempt::NotOwner => spent < MAX_NOT_OWNER,
        Attempt::NeverConnected => spent < MAX_NEVER_CONNECTED,
    }
}

/// What the router does after a failed attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Next {
    /// Resolve the owner again and re-send. `forget_node` additionally drops
    /// the cached node record, whose address is otherwise trusted until the
    /// node's lease expires — re-resolving without it returns the identical
    /// failed route, so the redispatch is wasted.
    Redispatch { forget_node: bool },
    /// Give up and answer the caller.
    Fail,
}

/// Per-request redispatch state. Budgets are counted per failure class so a
/// route that is both stale and unreachable still terminates.
#[derive(Clone, Copy, Debug, Default)]
pub struct Dispatcher {
    spent_not_owner: u32,
    spent_never_connected: u32,
}

impl Dispatcher {
    /// Record a failed attempt and decide what happens next.
    pub fn on_failure(&mut self, attempt: Attempt) -> Next {
        let spent = match attempt {
            Attempt::NotOwner => self.spent_not_owner,
            _ => self.spent_never_connected,
        };
        if !may_redispatch(attempt, spent) {
            return Next::Fail;
        }
        match attempt {
            Attempt::NotOwner => {
                self.spent_not_owner += 1;
                // Ownership moved; the address is still good.
                Next::Redispatch { forget_node: false }
            }
            _ => {
                self.spent_never_connected += 1;
                Next::Redispatch { forget_node: true }
            }
        }
    }
}
