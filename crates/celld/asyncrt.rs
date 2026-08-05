// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Host async runtime for the JS thread, using structured concurrency. Ops
//! never spawn detached: they ENQUEUE a future, and `js::run_loop` owns a
//! per-request tokio `JoinSet` (the "region") that spawns, drives, and — on
//! every exit path — aborts whatever is still pending.
//! Orphan tasks, leaked bookkeeping, and panic-hangs are impossible by
//! construction, not patched. The region is the one asupersync idea we take:
//! a task cannot outlive the scope that owns it.
use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

static NEXT_ID: AtomicU64 = AtomicU64::new(1);
static HOST_RT: OnceLock<tokio::runtime::Handle> = OnceLock::new();

/// What an async op resolves its JS promise with: a string, or raw bytes
/// (delivered as a `Uint8Array` — the structured-clone RPC payloads).
pub enum OpOut {
    Str(String),
    Bytes(Vec<u8>),
}
impl From<String> for OpOut {
    fn from(value: String) -> Self {
        Self::Str(value)
    }
}
impl From<Vec<u8>> for OpOut {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value)
    }
}

pub type OpFuture = Pin<Box<dyn Future<Output = Result<OpOut, String>> + Send>>;

thread_local! {
    // Per-JS-thread current-thread runtime: the Worker and each cell run on
    // their own thread, so each needs its own runtime — a shared current-thread
    // runtime cannot be driven by two threads at once. Held as `Rc` so a serve
    // loop can `block_on` on a clone without keeping the RefCell borrowed (which
    // would deadlock op spawns that clone the handle mid-drive).
    static RT: RefCell<Option<std::rc::Rc<tokio::runtime::Runtime>>> =
        const { RefCell::new(None) };
    // Ops push (id, future); run_loop drains and spawns them into its region.
    static SPAWNS: RefCell<Vec<(u64, OpFuture)>> = const { RefCell::new(Vec::new()) };
}

pub fn init() {
    RT.with(|r| {
        if r.borrow().is_none() {
            *r.borrow_mut() = Some(std::rc::Rc::new(
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap(),
            ));
        }
    });
}

/// The thread's runtime (an `Rc` clone), for a serve loop that drives both hyper
/// and this thread's JS ops on one runtime. `block_on` on the returned clone
/// leaves the thread-local RefCell free for `handle()`/op spawns.
pub fn runtime() -> std::rc::Rc<tokio::runtime::Runtime> {
    RT.with(|r| r.borrow().as_ref().unwrap().clone())
}

/// Install celld's shared Tokio runtime for host I/O. Futures remain owned by
/// each request's JoinSet, but polling network work on the shared runtime lets
/// a reqwest response body move into Axum and continue streaming after the JS
/// handler has returned.
pub fn set_host_handle(handle: tokio::runtime::Handle) {
    let _ = HOST_RT.set(handle);
}

pub fn handle() -> tokio::runtime::Handle {
    RT.with(|r| r.borrow().as_ref().unwrap().handle().clone())
}
pub fn op_handle() -> tokio::runtime::Handle {
    HOST_RT.get().cloned().unwrap_or_else(handle)
}
pub fn block_on<F: Future>(f: F) -> F::Output {
    let rt = runtime();
    rt.block_on(f)
}

/// Register an async op's future; returns its promise id. NOT spawned here — the
/// region in `run_loop` owns spawning, so a task is never detached.
pub fn enqueue<T: Into<OpOut>>(
    fut: impl Future<Output = Result<T, String>> + Send + 'static,
) -> u64 {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let fut: OpFuture = Box::pin(async move { fut.await.map(Into::into) });
    SPAWNS.with(|s| s.borrow_mut().push((id, fut)));
    id
}

pub fn drain_spawns() -> Vec<(u64, OpFuture)> {
    SPAWNS.with(|s| s.borrow_mut().drain(..).collect())
}
