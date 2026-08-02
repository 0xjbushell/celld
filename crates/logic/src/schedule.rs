//! Cell-isolate dispatch decisions, reified sans-IO. A cell isolate runs one
//! event at a time. A top-level Worker fetch takes the resident-isolate fast
//! path only when the isolate is idle; if the isolate is already pumping an
//! actor event, the fetch must reschedule to the stateless Worker pool — never
//! run nested — carrying its request identity so the reply still lands
//! (`js.rs`, the fix the 256-ring warm lab forced). The executor and the
//! production run loop hold the isolate channels and the pool; this is the pure
//! routing they consult, and the DST drives it directly (`tests/dst`,
//! `actor_call`). See `wiki/designs/dst-actor-call.md`.

/// Where a top-level Worker fetch runs when it reaches a cell isolate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    /// The isolate is idle: run the fetch on the resident isolate (fast path).
    OnIsolate,
    /// The isolate is pumping an actor event: hand the fetch to the stateless
    /// Worker pool with its request identity preserved. A Worker event never
    /// runs nested in an isolate already running an actor event.
    RescheduleToPool,
}

/// Route a top-level Worker fetch reaching a cell isolate. The load-bearing
/// invariant: a Worker event must never execute nested in an isolate already
/// pumping an actor event, so a busy isolate always reschedules to the pool.
pub fn route_worker_fetch(isolate_active: bool) -> Route {
    if isolate_active {
        Route::RescheduleToPool
    } else {
        Route::OnIsolate
    }
}
