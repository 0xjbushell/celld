// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Async operations started by JavaScript.
//!
//! An op is enqueued during a synchronous isolate turn. The request driver
//! takes those futures when the turn ends, polls them on celld's shared Tokio
//! runtime, and re-enters the isolate only to deliver a completed result.
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
    // Ops push (id, future); the turn driver drains and owns them.
    static SPAWNS: RefCell<Vec<(u64, OpFuture)>> = const { RefCell::new(Vec::new()) };
}

/// Install celld's shared Tokio runtime for host I/O. Futures remain owned by
/// each request driver, but polling network work on the shared runtime lets a
/// reqwest response body move into Axum and continue streaming after the JS
/// handler has returned.
pub fn set_host_handle(handle: tokio::runtime::Handle) {
    let _ = HOST_RT.set(handle);
}

pub fn op_handle() -> tokio::runtime::Handle {
    HOST_RT
        .get()
        .cloned()
        .unwrap_or_else(tokio::runtime::Handle::current)
}

/// Register an async op's future; returns its promise id. NOT spawned here — the
/// request driver owns polling, so a task is never detached.
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
