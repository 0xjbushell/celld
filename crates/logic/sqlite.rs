// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! SQLite failure classification, reified sans-IO. When a storage operation
//! fails, does it POISON the actor — the engine destroyed an active transaction,
//! so every later operation on this scope must fail closed — or is it merely
//! RECOVERABLE, a statement error that left the transaction intact? Getting this
//! wrong is a safety bug in both directions: a poisoned actor kept serving
//! observes corrupt half-rolled-back state, while a healthy actor wrongly
//! poisoned is killed on a benign constraint violation. Production reads SQLite's
//! autocommit flag over FFI; the classification over the resulting scalars is
//! pure, and this is where it lives.

/// SQLite primary result codes that mean the ENGINE failed (not just one
/// statement) — stable values from the SQLite C API. Production asserts these
/// match `rusqlite::ffi::*`, so any drift is caught in debug/test builds.
pub const SQLITE_NOMEM: i32 = 7;
pub const SQLITE_INTERRUPT: i32 = 9;
pub const SQLITE_IOERR: i32 = 10;
pub const SQLITE_FULL: i32 = 13;

/// What a failed SQLite operation does to the actor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Disposition {
    /// A critical engine error destroyed an active transaction. The scope is
    /// poisoned: fail every subsequent operation closed rather than serve state
    /// SQLite half-rolled-back.
    Poison,
    /// A statement error that left the transaction open. The actor recovers.
    Recoverable,
}

/// Classify a failed SQLite operation. Poison ONLY when a critical engine error
/// (`FULL`/`IOERR`/`NOMEM`/`INTERRUPT`) coincides with a destroyed transaction —
/// SQLite rolled an active transaction back, observable as autocommit being
/// re-enabled. Extended result codes carry the primary code in the low byte, so
/// mask before comparing. Anything else — a statement error, a critical error
/// that spared the transaction, or a failure outside a transaction — recovers.
pub fn classify_failure(
    result_code: i32,
    started_in_transaction: bool,
    now_autocommit: bool,
) -> Disposition {
    let primary = result_code & 0xff;
    let critical = matches!(
        primary,
        SQLITE_FULL | SQLITE_IOERR | SQLITE_NOMEM | SQLITE_INTERRUPT
    );
    let transaction_destroyed = started_in_transaction && now_autocommit;
    if critical && transaction_destroyed {
        Disposition::Poison
    } else {
        Disposition::Recoverable
    }
}
