//! Cell storage: SQLite backing for the Durable Object storage API.
//!
//! DO's `ctx.storage` is async in JS but synchronous underneath (local SQLite).
//! We expose synchronous Rust ops to V8 and wrap them in `async` in the JS
//! harness — same contract, no thread-hopping. Each cell is its OWN db file
//! (its own replicated, epoch-fenced bucket prefix), so the JS thread holds a
//! `scope -> Connection` map: `open` on activate, `close` on hibernate. The
//! `scope` column survives from the single-db era and still keys rows, but a
//! db now holds exactly one cell.
use rusqlite::{Connection, OptionalExtension};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::ffi::{CStr, CString};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

thread_local! {
    static DBS: RefCell<HashMap<String, Connection>> =
        RefCell::new(HashMap::new());
    static ACTIVE_ALARMS: RefCell<HashMap<String, ActiveAlarm>> =
        RefCell::new(HashMap::new());
    static ALARM_WATCHERS: RefCell<HashMap<String, Arc<AtomicI64>>> =
        RefCell::new(HashMap::new());
    static ALARM_DIRTY_TRANSACTIONS: RefCell<HashSet<String>> =
        RefCell::new(HashSet::new());
    static SYNC_LIST_CURSORS: RefCell<HashMap<u64, SyncListCursor>> =
        RefCell::new(HashMap::new());
    static SQL_CURSORS: RefCell<HashMap<u64, SqlCursor>> =
        RefCell::new(HashMap::new());
    static SQL_STATEMENT_CACHES: RefCell<HashMap<String, SqlStatementCache>> =
        RefCell::new(HashMap::new());
    static SQL_CRITICAL_ERRORS: RefCell<HashMap<String, String>> =
        RefCell::new(HashMap::new());
}

const SQL_STATEMENT_CACHE_MAX_SIZE: usize = 1024 * 1024;

#[derive(Clone, Copy)]
struct ActiveAlarm {
    fired_at_ms: i64,
    generation: Option<i64>,
}

struct SyncListCursor {
    scope: String,
    database: *mut rusqlite::ffi::sqlite3,
    statement: *mut rusqlite::ffi::sqlite3_stmt,
}

struct SqlCursor {
    scope: String,
    database: *mut rusqlite::ffi::sqlite3,
    statement: *mut rusqlite::ffi::sqlite3_stmt,
    changes_before: u64,
    cache_query: Option<Arc<str>>,
}

#[derive(Default)]
struct SqlStatementCache {
    entries: HashMap<Arc<str>, CachedSqlStatement>,
    total_size: usize,
    clock: u64,
}

struct CachedSqlStatement {
    statement: *mut rusqlite::ffi::sqlite3_stmt,
    use_count: u64,
    size: usize,
    last_used: u64,
}

impl Drop for CachedSqlStatement {
    fn drop(&mut self) {
        if !self.statement.is_null() {
            // SAFETY: an idle cache slot exclusively owns its statement.
            unsafe {
                rusqlite::ffi::sqlite3_finalize(self.statement);
            }
        }
    }
}

impl Drop for SyncListCursor {
    fn drop(&mut self) {
        // SAFETY: the statement is prepared by sync_list_start(), stored in
        // exactly one cursor, and finalized before its owning connection is
        // removed from DBS.
        unsafe {
            rusqlite::ffi::sqlite3_finalize(self.statement);
        }
    }
}

impl Drop for SqlCursor {
    fn drop(&mut self) {
        if self.statement.is_null() {
            return;
        }
        let statement = std::mem::replace(&mut self.statement, std::ptr::null_mut());
        if let Some(query) = self.cache_query.take() {
            recycle_sql_statement(&self.scope, &query, statement);
        } else {
            // SAFETY: this cursor exclusively owns its one-off statement.
            unsafe {
                rusqlite::ffi::sqlite3_finalize(statement);
            }
        }
    }
}

fn schema(c: &Connection) -> anyhow::Result<()> {
    c.pragma_update(None, "journal_mode", "WAL")?;
    // NORMAL, not the FULL default: with WAL, commits then skip the
    // per-commit WAL fsync (measured 1.4ms -> 19us per put on cloud
    // disks; the fsync was the entire single-cell write budget).
    // Process crashes lose nothing. An OS/power crash may lose the
    // last commits locally — celld's durability boundary for node
    // loss is Litestream replication either way, and Litestream's own
    // documentation recommends NORMAL under replication.
    c.pragma_update(None, "synchronous", "NORMAL")?;
    c.execute(
        "CREATE TABLE IF NOT EXISTS kv \
         (scope TEXT, k TEXT, v TEXT, PRIMARY KEY(scope,k))",
        [],
    )?;
    c.execute(
        "CREATE TABLE IF NOT EXISTS alarms \
         (scope TEXT PRIMARY KEY, at_ms INTEGER, retry INTEGER DEFAULT 0, \
          counted_retry INTEGER NOT NULL DEFAULT 0, \
          generation INTEGER NOT NULL DEFAULT 0)",
        [],
    )?;
    c.execute(
        "CREATE TABLE IF NOT EXISTS cell_metadata \
         (scope TEXT PRIMARY KEY, actor_name TEXT)",
        [],
    )?;
    let alarm_columns = {
        let mut statement = c.prepare("PRAGMA table_info(alarms)")?;
        let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
        columns.collect::<rusqlite::Result<Vec<_>>>()?
    };
    if !alarm_columns.iter().any(|column| column == "generation") {
        c.execute(
            "ALTER TABLE alarms ADD COLUMN generation INTEGER NOT NULL DEFAULT 0",
            [],
        )?;
    }
    if !alarm_columns
        .iter()
        .any(|column| column == "counted_retry")
    {
        c.execute(
            "ALTER TABLE alarms ADD COLUMN counted_retry INTEGER NOT NULL DEFAULT 0",
            [],
        )?;
    }
    Ok(())
}

fn is_reserved_sql_name(name: &str) -> bool {
    name.get(..4)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("_cf_"))
}

fn valid_sql_boolean(value: &str) -> bool {
    let value = value.trim();
    let value = if value.len() >= 2
        && ((value.starts_with('\'') && value.ends_with('\''))
            || (value.starts_with('"') && value.ends_with('"')))
    {
        &value[1..value.len() - 1]
    } else {
        value
    };
    matches!(
        value.to_ascii_lowercase().as_str(),
        "true" | "false" | "yes" | "no" | "on" | "off" | "1" | "0"
    )
}

fn valid_sql_i32(value: &str) -> bool {
    value.parse::<i32>().is_ok()
        || value
            .strip_prefix("0x")
            .or_else(|| value.strip_prefix("0X"))
            .and_then(|digits| u32::from_str_radix(digits, 16).ok())
            .is_some_and(|number| number <= i32::MAX as u32)
}

fn allowed_sql_pragma(name: &str, value: Option<&str>) -> bool {
    let name = name.to_ascii_lowercase();
    match name.as_str() {
        "data_version" | "table_list" => value.is_none(),
        "case_sensitive_like"
        | "foreign_keys"
        | "defer_foreign_keys"
        | "ignore_check_constraints"
        | "legacy_alter_table"
        | "recursive_triggers"
        | "reverse_unordered_selects" => value.is_none_or(valid_sql_boolean),
        "table_info" | "table_xinfo" | "foreign_key_list" | "index_info" | "index_list"
        | "index_xinfo" => value.is_some_and(|name| !is_reserved_sql_name(name)),
        "foreign_key_check" => value.is_none_or(|name| !is_reserved_sql_name(name)),
        "quick_check" => value.is_none_or(|value| {
            value.parse::<u32>().is_ok() || !is_reserved_sql_name(value)
        }),
        "optimize" => value.is_none_or(valid_sql_i32),
        _ => false,
    }
}

fn authorize_sql(context: rusqlite::hooks::AuthContext<'_>) -> rusqlite::hooks::Authorization {
    use rusqlite::hooks::{AuthAction, Authorization};

    let prohibited = match context.action {
        AuthAction::Transaction { .. }
        | AuthAction::Savepoint { .. }
        | AuthAction::Attach { .. }
        | AuthAction::Detach { .. }
        | AuthAction::CreateTempIndex { .. }
        | AuthAction::CreateTempTable { .. }
        | AuthAction::CreateTempTrigger { .. }
        | AuthAction::CreateTempView { .. }
        | AuthAction::DropTempIndex { .. }
        | AuthAction::DropTempTable { .. }
        | AuthAction::DropTempTrigger { .. }
        | AuthAction::DropTempView { .. } => true,
        AuthAction::Pragma {
            pragma_name,
            pragma_value,
        } => !allowed_sql_pragma(pragma_name, pragma_value),
        AuthAction::CreateVtable { module_name, .. } => {
            !module_name.eq_ignore_ascii_case("fts5")
                && !module_name.eq_ignore_ascii_case("fts5vocab")
        }
        AuthAction::Function { function_name } => matches!(
            function_name.to_ascii_lowercase().as_str(),
            "load_extension" | "sqlite_version" | "sqlite_source_id"
        ),
        _ => false,
    };
    if prohibited {
        return Authorization::Deny;
    }

    let reserved = match context.action {
        AuthAction::Unknown { arg1, arg2, .. } => {
            arg1.is_some_and(is_reserved_sql_name) || arg2.is_some_and(is_reserved_sql_name)
        }
        AuthAction::CreateIndex {
            index_name,
            table_name,
        }
        | AuthAction::CreateTempIndex {
            index_name,
            table_name,
        }
        | AuthAction::DropIndex {
            index_name,
            table_name,
        }
        | AuthAction::DropTempIndex {
            index_name,
            table_name,
        } => is_reserved_sql_name(index_name) || is_reserved_sql_name(table_name),
        AuthAction::CreateTrigger {
            trigger_name,
            table_name,
        }
        | AuthAction::CreateTempTrigger {
            trigger_name,
            table_name,
        }
        | AuthAction::DropTrigger {
            trigger_name,
            table_name,
        }
        | AuthAction::DropTempTrigger {
            trigger_name,
            table_name,
        } => is_reserved_sql_name(trigger_name) || is_reserved_sql_name(table_name),
        AuthAction::CreateTable { table_name }
        | AuthAction::CreateTempTable { table_name }
        | AuthAction::Delete { table_name }
        | AuthAction::DropTable { table_name }
        | AuthAction::DropTempTable { table_name }
        | AuthAction::Insert { table_name }
        | AuthAction::Analyze { table_name } => is_reserved_sql_name(table_name),
        AuthAction::CreateView { view_name }
        | AuthAction::CreateTempView { view_name }
        | AuthAction::DropView { view_name }
        | AuthAction::DropTempView { view_name } => is_reserved_sql_name(view_name),
        AuthAction::Read {
            table_name,
            column_name,
        }
        | AuthAction::Update {
            table_name,
            column_name,
        } => is_reserved_sql_name(table_name) || is_reserved_sql_name(column_name),
        AuthAction::AlterTable {
            database_name,
            table_name,
        } => is_reserved_sql_name(database_name) || is_reserved_sql_name(table_name),
        AuthAction::CreateVtable {
            table_name,
            module_name: _,
        }
        | AuthAction::DropVtable {
            table_name,
            module_name: _,
        } => is_reserved_sql_name(table_name),
        AuthAction::Reindex { index_name } => is_reserved_sql_name(index_name),
        _ => false,
    } || context
        .database_name
        .is_some_and(is_reserved_sql_name)
        || context.accessor.is_some_and(is_reserved_sql_name);

    if reserved {
        Authorization::Deny
    } else {
        Authorization::Allow
    }
}

fn without_sql_authorizer<T>(connection: &Connection, callback: impl FnOnce() -> T) -> T {
    use rusqlite::hooks::{AuthContext, Authorization};

    connection.authorizer(None::<fn(AuthContext<'_>) -> Authorization>);
    let result = callback();
    connection.authorizer(Some(authorize_sql));
    result
}

fn without_sql_authorizer_mut<T>(
    connection: &mut Connection,
    callback: impl FnOnce(&mut Connection) -> T,
) -> T {
    use rusqlite::hooks::{AuthContext, Authorization};

    connection.authorizer(None::<fn(AuthContext<'_>) -> Authorization>);
    let result = callback(connection);
    connection.authorizer(Some(authorize_sql));
    result
}

/// Open (or replace) the connection for `scope`'s db file.
pub fn open(scope: &str, path: &str) -> anyhow::Result<()> {
    finish_open(scope, Connection::open(path)?)
}

/// Open `scope` through the fault-injection VFS so tests can fail its writes.
#[cfg(all(test, celld_internal_tests))]
pub fn open_with_fault_vfs_for_test(
    scope: &str,
    path: &str,
) -> anyhow::Result<()> {
    finish_open(scope, crate::fault::open_database(path)?)
}

fn finish_open(scope: &str, c: Connection) -> anyhow::Result<()> {
    schema(&c)?;
    // Match Workerd's SQLite security budgets. Applying native connection
    // limits once here avoids request-path parsing and keeps rejected queries
    // from consuming unbounded parser, VDBE, or expression resources.
    // SAFETY: `c` owns a live SQLite connection for the duration of this call.
    unsafe {
        let database = c.handle();
        for (category, limit) in [
            (rusqlite::ffi::SQLITE_LIMIT_LENGTH, 2_200_000),
            (rusqlite::ffi::SQLITE_LIMIT_SQL_LENGTH, 100_000),
            (rusqlite::ffi::SQLITE_LIMIT_COLUMN, 100),
            (rusqlite::ffi::SQLITE_LIMIT_EXPR_DEPTH, 100),
            (rusqlite::ffi::SQLITE_LIMIT_COMPOUND_SELECT, 5),
            (rusqlite::ffi::SQLITE_LIMIT_VDBE_OP, 25_000),
            (rusqlite::ffi::SQLITE_LIMIT_FUNCTION_ARG, 127),
            (rusqlite::ffi::SQLITE_LIMIT_ATTACHED, 0),
            (rusqlite::ffi::SQLITE_LIMIT_LIKE_PATTERN_LENGTH, 50),
            (rusqlite::ffi::SQLITE_LIMIT_VARIABLE_NUMBER, 100),
            (rusqlite::ffi::SQLITE_LIMIT_TRIGGER_DEPTH, 10),
            (rusqlite::ffi::SQLITE_LIMIT_WORKER_THREADS, 0),
        ] {
            rusqlite::ffi::sqlite3_limit(database, category, limit);
        }
    }
    c.authorizer(Some(authorize_sql));
    close_sync_list_cursors(scope);
    close_sql_cursors(scope);
    close_sql_statement_cache(scope);
    SQL_CRITICAL_ERRORS.with(|errors| errors.borrow_mut().remove(scope));
    DBS.with(|d| d.borrow_mut().insert(scope.to_string(), c));
    Ok(())
}

/// Drop `scope`'s connection (hibernate) so litestream can release the file.
pub fn close(scope: &str) {
    close_sync_list_cursors(scope);
    close_sql_cursors(scope);
    close_sql_statement_cache(scope);
    SQL_CRITICAL_ERRORS.with(|errors| errors.borrow_mut().remove(scope));
    DBS.with(|d| d.borrow_mut().remove(scope));
    ACTIVE_ALARMS.with(|alarms| alarms.borrow_mut().remove(scope));
    ALARM_DIRTY_TRANSACTIONS.with(|dirty| {
        dirty.borrow_mut().remove(scope);
    });
}

fn with<T>(scope: &str, f: impl FnOnce(&Connection) -> T) -> Option<T> {
    DBS.with(|d| d.borrow().get(scope).map(f))
}

fn with_mut<T>(scope: &str, f: impl FnOnce(&mut Connection) -> T) -> Option<T> {
    DBS.with(|d| d.borrow_mut().get_mut(scope).map(f))
}

static NEXT_BATCH_SAVEPOINT: AtomicU64 = AtomicU64::new(1);
static NEXT_SYNC_LIST_CURSOR: AtomicU64 = AtomicU64::new(1);
static NEXT_SQL_CURSOR: AtomicU64 = AtomicU64::new(1);

fn with_batch_savepoint<T>(
    connection: &Connection,
    callback: impl FnOnce(&Connection) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    without_sql_authorizer(connection, || {
        let sequence = NEXT_BATCH_SAVEPOINT.fetch_add(1, Ordering::Relaxed);
        let name = format!("cells_batch_{sequence}");
        connection.execute_batch(&format!("SAVEPOINT {name};"))?;
        match callback(connection) {
            Ok(value) => {
                connection.execute_batch(&format!("RELEASE {name};"))?;
                Ok(value)
            }
            Err(error) => {
                let _ =
                    connection.execute_batch(&format!("ROLLBACK TO {name}; RELEASE {name};"));
                Err(error)
            }
        }
    })
}

fn json_to_sql(v: &serde_json::Value) -> rusqlite::types::Value {
    use rusqlite::types::Value;
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Integer(*b as i64),
        serde_json::Value::Number(n) if n.is_i64() => Value::Integer(n.as_i64().unwrap()),
        serde_json::Value::Number(n) => Value::Real(n.as_f64().unwrap_or(0.0)),
        serde_json::Value::String(s) => Value::Text(s.clone()),
        serde_json::Value::Object(o) if o.contains_key("__celld_bytes") => {
            let bytes = o
                .get("__celld_bytes")
                .and_then(|v| v.as_array())
                .map(|values| {
                    values
                        .iter()
                        .filter_map(|v| v.as_u64().map(|n| n as u8))
                        .collect()
                })
                .unwrap_or_default();
            Value::Blob(bytes)
        }
        _ => Value::Null,
    }
}

fn vref_to_json(v: rusqlite::types::ValueRef) -> serde_json::Value {
    use rusqlite::types::ValueRef;
    match v {
        ValueRef::Null => serde_json::Value::Null,
        ValueRef::Integer(i) => serde_json::json!(i),
        ValueRef::Real(f) => serde_json::json!(f),
        ValueRef::Text(t) => serde_json::Value::String(String::from_utf8_lossy(t).into_owned()),
        ValueRef::Blob(b) => serde_json::json!({ "__celld_bytes": b }),
    }
}

fn total_changes(connection: &Connection) -> u64 {
    // SAFETY: `handle()` belongs to this live connection and SQLite only reads
    // its monotonically increasing completed-write counter.
    unsafe { total_changes_for_handle(connection.handle()) }
}

unsafe fn total_changes_for_handle(database: *mut rusqlite::ffi::sqlite3) -> u64 {
    rusqlite::ffi::sqlite3_total_changes64(database) as u64
}

pub enum StoredValue {
    LegacyJson(String),
    V8(Vec<u8>),
}

fn stored_value(value: rusqlite::types::ValueRef<'_>) -> StoredValue {
    match value {
        rusqlite::types::ValueRef::Blob(bytes) => StoredValue::V8(bytes.to_vec()),
        rusqlite::types::ValueRef::Text(bytes) => {
            StoredValue::LegacyJson(String::from_utf8_lossy(bytes).into_owned())
        }
        value => StoredValue::LegacyJson(vref_to_json(value).to_string()),
    }
}

/// (column names, result rows, rows written) from a cell SQL exec.
type SqlExec = (Vec<String>, Vec<Vec<serde_json::Value>>, u64);

/// Run SQL on the cell's own SQLite (the DO `ctx.storage.sql` API). Cloudflare
/// accepts schema batches in one `exec()` call, so execute multi-statement,
/// parameter-free input as a batch. Queries and parameterized statements still
/// use a prepared statement so their cursor rows are returned.
pub fn sql_exec(
    scope: &str,
    query: &str,
    binds: &[serde_json::Value],
) -> Result<SqlExec, String> {
    require_sql_healthy(scope)?;
    let run = with(scope, |c| {
        let changes_before = total_changes(c);
        if binds.is_empty() && query.matches(';').count() > 1 {
            c.execute_batch(query).map_err(|e| e.to_string())?;
            return Ok((
                Vec::new(),
                Vec::new(),
                total_changes(c).saturating_sub(changes_before),
            ));
        }
        let mut stmt = c.prepare(query).map_err(|e| e.to_string())?;
        let cols: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
        let n = cols.len();
        let params = rusqlite::params_from_iter(binds.iter().map(json_to_sql));
        let mut rows = stmt.query(params).map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let mut r = Vec::with_capacity(n);
            for i in 0..n {
                r.push(vref_to_json(row.get_ref(i).map_err(|e| e.to_string())?));
            }
            out.push(r);
        }
        drop(rows);
        drop(stmt);
        Ok((
            cols,
            out,
            total_changes(c).saturating_sub(changes_before),
        ))
    });
    run.unwrap_or_else(|| Err(format!("no db for {scope}")))
}

/// Execute every complete statement at the front of `input`, returning the
/// untouched suffix beginning with the first incomplete statement. SQLite's
/// own parser determines statement boundaries, including trigger bodies and
/// semicolons inside strings. Rows are stepped and discarded without crossing
/// the V8 boundary.
pub fn sql_ingest(scope: &str, input: &str) -> Result<(String, u64, u64), String> {
    require_sql_healthy(scope)?;
    let sql = CString::new(input).map_err(|error| error.to_string())?;
    let result = with(scope, |connection| -> anyhow::Result<_> {
        // SAFETY: `sql` remains alive for the entire loop. Every prepared
        // statement is finalized on all paths, and SQLite's tail points within
        // the same NUL-terminated allocation.
        unsafe {
            let database = connection.handle();
            let started_in_transaction =
                rusqlite::ffi::sqlite3_get_autocommit(database) == 0;
            let start = sql.as_ptr();
            let mut source = start;
            let changes_before = total_changes_for_handle(database);
            let mut statement_count = 0_u64;

            loop {
                let offset = source.offset_from(start) as usize;
                let remainder = input
                    .get(offset..)
                    .ok_or_else(|| anyhow::anyhow!("SQLite returned an invalid SQL tail"))?;
                let mut statement = std::ptr::null_mut();
                let mut tail = std::ptr::null();
                let prepare = rusqlite::ffi::sqlite3_prepare_v2(
                    database,
                    source,
                    -1,
                    &mut statement,
                    &mut tail,
                );
                if prepare != rusqlite::ffi::SQLITE_OK {
                    if !statement.is_null() {
                        rusqlite::ffi::sqlite3_finalize(statement);
                    }
                    if rusqlite::ffi::sqlite3_complete(source) == 0 {
                        return Ok((
                            remainder.to_string(),
                            total_changes_for_handle(database)
                                .saturating_sub(changes_before),
                            statement_count,
                        ));
                    }
                    return Err(sqlite_operation_failure(
                        scope,
                        database,
                        started_in_transaction,
                        prepare,
                        "prepare ingested SQL",
                    ));
                }
                if statement.is_null() {
                    return Ok((
                        remainder.to_string(),
                        total_changes_for_handle(database).saturating_sub(changes_before),
                        statement_count,
                    ));
                }
                if tail.is_null() || tail < source {
                    rusqlite::ffi::sqlite3_finalize(statement);
                    return Err(anyhow::anyhow!("SQLite returned an invalid SQL tail"));
                }

                let consumed_length = tail.offset_from(source) as usize;
                let consumed = CString::new(std::slice::from_raw_parts(
                    source.cast::<u8>(),
                    consumed_length,
                ))?;
                if rusqlite::ffi::sqlite3_complete(consumed.as_ptr()) == 0 {
                    rusqlite::ffi::sqlite3_finalize(statement);
                    return Ok((
                        remainder.to_string(),
                        total_changes_for_handle(database).saturating_sub(changes_before),
                        statement_count,
                    ));
                }

                loop {
                    let step = rusqlite::ffi::sqlite3_step(statement);
                    match step {
                        rusqlite::ffi::SQLITE_ROW => {}
                        rusqlite::ffi::SQLITE_DONE => break,
                        _ => {
                            let error = sqlite_operation_failure(
                                scope,
                                database,
                                started_in_transaction,
                                step,
                                "step ingested SQL",
                            );
                            rusqlite::ffi::sqlite3_finalize(statement);
                            return Err(error);
                        }
                    }
                }
                rusqlite::ffi::sqlite3_finalize(statement);
                statement_count += 1;
                source = tail;
            }
        }
    });
    result
        .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
        .map_err(|error| error.to_string())
}

unsafe fn sql_cursor_columns(
    statement: *mut rusqlite::ffi::sqlite3_stmt,
) -> Vec<String> {
    let count = rusqlite::ffi::sqlite3_column_count(statement);
    (0..count)
        .map(|index| {
            let name = rusqlite::ffi::sqlite3_column_name(statement, index);
            if name.is_null() {
                String::new()
            } else {
                CStr::from_ptr(name).to_string_lossy().into_owned()
            }
        })
        .collect()
}

unsafe fn sql_cursor_row(
    statement: *mut rusqlite::ffi::sqlite3_stmt,
) -> Vec<serde_json::Value> {
    let count = rusqlite::ffi::sqlite3_column_count(statement);
    (0..count)
        .map(|index| match rusqlite::ffi::sqlite3_column_type(statement, index) {
            rusqlite::ffi::SQLITE_INTEGER => {
                serde_json::json!(rusqlite::ffi::sqlite3_column_int64(statement, index))
            }
            rusqlite::ffi::SQLITE_FLOAT => {
                serde_json::json!(rusqlite::ffi::sqlite3_column_double(statement, index))
            }
            rusqlite::ffi::SQLITE_TEXT => {
                let length =
                    rusqlite::ffi::sqlite3_column_bytes(statement, index) as usize;
                let pointer = rusqlite::ffi::sqlite3_column_text(statement, index);
                let value = if length == 0 {
                    String::new()
                } else {
                    String::from_utf8_lossy(std::slice::from_raw_parts(pointer, length))
                        .into_owned()
                };
                serde_json::Value::String(value)
            }
            rusqlite::ffi::SQLITE_BLOB => {
                let length =
                    rusqlite::ffi::sqlite3_column_bytes(statement, index) as usize;
                let pointer: *const u8 =
                    rusqlite::ffi::sqlite3_column_blob(statement, index).cast();
                let bytes = if length == 0 {
                    Vec::new()
                } else {
                    std::slice::from_raw_parts(pointer, length).to_vec()
                };
                serde_json::json!({ "__celld_bytes": bytes })
            }
            _ => serde_json::Value::Null,
        })
        .collect()
}

enum SqlStatementCacheLookup {
    Miss,
    Busy,
    Hit {
        statement: *mut rusqlite::ffi::sqlite3_stmt,
        reused: bool,
        query: Arc<str>,
    },
}

fn take_cached_sql_statement(scope: &str, query: &str) -> SqlStatementCacheLookup {
    SQL_STATEMENT_CACHES.with(|caches| {
        let mut caches = caches.borrow_mut();
        let Some(cache) = caches.get_mut(scope) else {
            return SqlStatementCacheLookup::Miss;
        };
        cache.clock = cache.clock.wrapping_add(1);
        let clock = cache.clock;
        let Some(cache_query) = cache
            .entries
            .get_key_value(query)
            .map(|(query, _)| Arc::clone(query))
        else {
            return SqlStatementCacheLookup::Miss;
        };
        let entry = cache
            .entries
            .get_mut(query)
            .expect("cache key disappeared during lookup");
        entry.last_used = clock;
        if entry.statement.is_null() {
            return SqlStatementCacheLookup::Busy;
        }
        let statement = std::mem::replace(&mut entry.statement, std::ptr::null_mut());
        let reused = entry.use_count > 0;
        entry.use_count = entry.use_count.saturating_add(1);
        SqlStatementCacheLookup::Hit {
            statement,
            reused,
            query: cache_query,
        }
    })
}

fn register_cached_sql_statement(scope: &str, query: &str) -> Option<Arc<str>> {
    SQL_STATEMENT_CACHES.with(|caches| {
        let mut caches = caches.borrow_mut();
        let cache = caches.entry(scope.to_string()).or_default();
        cache.clock = cache.clock.wrapping_add(1);
        let clock = cache.clock;
        let size = query.len();
        let cache_query: Arc<str> = Arc::from(query);
        cache.total_size = cache.total_size.saturating_add(size);
        cache.entries.insert(
            Arc::clone(&cache_query),
            CachedSqlStatement {
                statement: std::ptr::null_mut(),
                use_count: 1,
                size,
                last_used: clock,
            },
        );

        while cache.total_size > SQL_STATEMENT_CACHE_MAX_SIZE {
            let Some(lru) = cache
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(query, _)| Arc::clone(query))
            else {
                break;
            };
            if let Some(removed) = cache.entries.remove(&lru) {
                cache.total_size = cache.total_size.saturating_sub(removed.size);
            }
        }
        cache
            .entries
            .contains_key(query)
            .then_some(cache_query)
    })
}

fn discard_cached_sql_statement(scope: &str, query: &str) {
    SQL_STATEMENT_CACHES.with(|caches| {
        let mut caches = caches.borrow_mut();
        let Some(cache) = caches.get_mut(scope) else {
            return;
        };
        if let Some(removed) = cache.entries.remove(query) {
            cache.total_size = cache.total_size.saturating_sub(removed.size);
        }
        if cache.entries.is_empty() {
            caches.remove(scope);
        }
    });
}

fn recycle_sql_statement(
    scope: &str,
    query: &str,
    statement: *mut rusqlite::ffi::sqlite3_stmt,
) {
    // SAFETY: the caller transfers exclusive ownership of a live statement.
    let reusable = unsafe {
        rusqlite::ffi::sqlite3_reset(statement) == rusqlite::ffi::SQLITE_OK
            && rusqlite::ffi::sqlite3_clear_bindings(statement)
                == rusqlite::ffi::SQLITE_OK
    };
    let mut retained = false;
    if reusable {
        SQL_STATEMENT_CACHES.with(|caches| {
            if let Some(entry) = caches
                .borrow_mut()
                .get_mut(scope)
                .and_then(|cache| cache.entries.get_mut(query))
            {
                if entry.statement.is_null() {
                    entry.statement = statement;
                    retained = true;
                }
            }
        });
    }
    if !retained {
        if !reusable {
            discard_cached_sql_statement(scope, query);
        }
        // SAFETY: no cache slot retained the transferred statement.
        unsafe {
            rusqlite::ffi::sqlite3_finalize(statement);
        }
    }
}

fn close_sql_statement_cache(scope: &str) {
    SQL_STATEMENT_CACHES.with(|caches| {
        caches.borrow_mut().remove(scope);
    });
}

fn discard_in_use_sql_statement(
    scope: &str,
    cache_query: Option<&str>,
    statement: *mut rusqlite::ffi::sqlite3_stmt,
) {
    if let Some(query) = cache_query {
        discard_cached_sql_statement(scope, query);
    }
    if !statement.is_null() {
        // SAFETY: the caller transfers exclusive ownership of the statement.
        unsafe {
            rusqlite::ffi::sqlite3_finalize(statement);
        }
    }
}

/// (cursor id, column names, first row, rows written, reused-cached-query)
/// from starting a SQL cursor.
type SqlCursorStart =
    (u64, Vec<String>, Option<Vec<serde_json::Value>>, u64, bool);

/// Prepare and step a SQL statement once. The returned cursor owns the native
/// statement and yields at most one copied row per subsequent call.
pub fn sql_cursor_start(
    scope: &str,
    query: &str,
    binds: &[serde_json::Value],
) -> Result<SqlCursorStart, String> {
    require_sql_healthy(scope)?;
    let (cached_statement, cached_query, cache_busy, reused_cached_query) =
        match take_cached_sql_statement(scope, query) {
            SqlStatementCacheLookup::Miss => {
                (std::ptr::null_mut(), None, false, false)
            }
            SqlStatementCacheLookup::Busy => {
                (std::ptr::null_mut(), None, true, false)
            }
            SqlStatementCacheLookup::Hit {
                statement,
                reused,
                query,
            } => (statement, Some(query), false, reused),
        };
    let sql = CString::new(query).map_err(|error| error.to_string())?;
    let result = with(scope, |connection| -> anyhow::Result<_> {
        // SAFETY: the connection remains in DBS until close(), which drops all
        // SQL cursors first. SQLite receives transient copies of every bind.
        unsafe {
            let database = connection.handle();
            let started_in_transaction =
                rusqlite::ffi::sqlite3_get_autocommit(database) == 0;
            let changes_before = total_changes_for_handle(database);
            let start = sql.as_ptr();
            let mut source = start;
            let mut saw_prefix = false;
            loop {
                let using_cached =
                    source == start && !cached_statement.is_null();
                let mut cache_query = using_cached
                    .then(|| cached_query.as_ref().expect("cached query key").clone());
                let mut statement = if using_cached {
                    cached_statement
                } else {
                    std::ptr::null_mut()
                };
                let mut tail = std::ptr::null();
                if !using_cached {
                    let prepare = rusqlite::ffi::sqlite3_prepare_v2(
                        database,
                        source,
                        -1,
                        &mut statement,
                        &mut tail,
                    );
                    if prepare != rusqlite::ffi::SQLITE_OK {
                        discard_in_use_sql_statement(
                            scope,
                            cache_query.as_deref(),
                            statement,
                        );
                        return Err(sqlite_operation_failure(
                            scope,
                            database,
                            started_in_transaction,
                            prepare,
                            "prepare SQL cursor",
                        ));
                    }
                }
                if statement.is_null() {
                    return Err(anyhow::anyhow!("SQL code did not contain a statement"));
                }

                let has_more = !using_cached
                    && !tail.is_null()
                    && CStr::from_ptr(tail)
                        .to_bytes()
                        .iter()
                        .any(|byte| !byte.is_ascii_whitespace());
                if has_more {
                    if rusqlite::ffi::sqlite3_bind_parameter_count(statement) != 0 {
                        discard_in_use_sql_statement(scope, None, statement);
                        return Err(anyhow::anyhow!(
                            "Wrong number of parameter bindings for SQL query."
                        ));
                    }
                    loop {
                        let step = rusqlite::ffi::sqlite3_step(statement);
                        match step {
                            rusqlite::ffi::SQLITE_ROW => {}
                            rusqlite::ffi::SQLITE_DONE => break,
                            _ => {
                                let error = sqlite_operation_failure(
                                    scope,
                                    database,
                                    started_in_transaction,
                                    step,
                                    "step SQL prefix",
                                );
                                discard_in_use_sql_statement(scope, None, statement);
                                return Err(error);
                            }
                        }
                    }
                    rusqlite::ffi::sqlite3_finalize(statement);
                    source = tail;
                    saw_prefix = true;
                    continue;
                }

                let expected =
                    rusqlite::ffi::sqlite3_bind_parameter_count(statement) as usize;
                if expected != binds.len() {
                    discard_in_use_sql_statement(
                        scope,
                        cache_query.as_deref(),
                        statement,
                    );
                    return Err(anyhow::anyhow!(
                        "Wrong number of parameter bindings for SQL query."
                    ));
                }
                for (offset, value) in binds.iter().map(json_to_sql).enumerate() {
                    if let Err(error) =
                        bind_cursor_value(database, statement, offset as i32 + 1, &value)
                    {
                        discard_in_use_sql_statement(
                            scope,
                            cache_query.as_deref(),
                            statement,
                        );
                        return Err(error);
                    }
                }
                if cache_query.is_none()
                    && !cache_busy
                    && !saw_prefix
                {
                    cache_query = register_cached_sql_statement(scope, query);
                }
                let step = rusqlite::ffi::sqlite3_step(statement);
                return match step {
                    rusqlite::ffi::SQLITE_ROW => {
                        // Read metadata after the first step so SQLite has
                        // automatically recompiled an expired cached statement.
                        let columns = sql_cursor_columns(statement);
                        let row = sql_cursor_row(statement);
                        Ok((
                            database,
                            statement,
                            changes_before,
                            columns,
                            Some(row),
                            0,
                            cache_query,
                        ))
                    }
                    rusqlite::ffi::SQLITE_DONE => {
                        let columns = sql_cursor_columns(statement);
                        let rows_written =
                            total_changes_for_handle(database).saturating_sub(changes_before);
                        Ok((
                            database,
                            statement,
                            changes_before,
                            columns,
                            None,
                            rows_written,
                            cache_query,
                        ))
                    }
                    _ => {
                        let error = sqlite_operation_failure(
                            scope,
                            database,
                            started_in_transaction,
                            step,
                            "step SQL cursor",
                        );
                        discard_in_use_sql_statement(
                            scope,
                            cache_query.as_deref(),
                            statement,
                        );
                        Err(error)
                    }
                }
            }
        }
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
    .map_err(|error| error.to_string())?;

    let (
        database,
        statement,
        changes_before,
        columns,
        row,
        rows_written,
        cache_query,
    ) = result;
    if row.is_none() {
        if let Some(query) = cache_query.as_deref() {
            recycle_sql_statement(scope, query, statement);
        } else {
            discard_in_use_sql_statement(scope, None, statement);
        }
        return Ok((0, columns, row, rows_written, reused_cached_query));
    }
    let id = NEXT_SQL_CURSOR.fetch_add(1, Ordering::Relaxed);
    SQL_CURSORS.with(|cursors| {
        cursors.borrow_mut().insert(
            id,
            SqlCursor {
                scope: scope.to_string(),
                database,
                statement,
                changes_before,
                cache_query,
            },
        );
    });
    Ok((id, columns, row, rows_written, reused_cached_query))
}

pub fn sql_cursor_next(
    cursor_id: u64,
) -> Result<(Option<Vec<serde_json::Value>>, u64), String> {
    SQL_CURSORS.with(|cursors| {
        let mut cursors = cursors.borrow_mut();
        let cursor = cursors
            .get_mut(&cursor_id)
            .ok_or_else(|| "SQL cursor is no longer active".to_string())?;
        if let Some(error) = sql_critical_error(&cursor.scope) {
            cursors.remove(&cursor_id);
            return Err(error);
        }
        // SAFETY: the cursor owns its statement on a connection that remains
        // live until the cursor is removed.
        let started_in_transaction =
            unsafe { rusqlite::ffi::sqlite3_get_autocommit(cursor.database) == 0 };
        let step = unsafe { rusqlite::ffi::sqlite3_step(cursor.statement) };
        match step {
            rusqlite::ffi::SQLITE_ROW => {
                let row = unsafe { sql_cursor_row(cursor.statement) };
                Ok((Some(row), 0))
            }
            rusqlite::ffi::SQLITE_DONE => {
                let rows_written = unsafe {
                    total_changes_for_handle(cursor.database)
                        .saturating_sub(cursor.changes_before)
                };
                cursors.remove(&cursor_id);
                Ok((None, rows_written))
            }
            _ => {
                let error = sqlite_operation_failure(
                    &cursor.scope,
                    cursor.database,
                    started_in_transaction,
                    step,
                    "step SQL cursor",
                )
                .to_string();
                if let Some(query) = cursor.cache_query.take() {
                    discard_cached_sql_statement(&cursor.scope, &query);
                }
                cursors.remove(&cursor_id);
                Err(error)
            }
        }
    })
}

pub fn sql_cursor_close(cursor_id: u64) {
    SQL_CURSORS.with(|cursors| {
        cursors.borrow_mut().remove(&cursor_id);
    });
}

pub fn sql_database_size(scope: &str) -> Result<u64, String> {
    require_sql_healthy(scope)?;
    with(scope, |connection| {
        without_sql_authorizer(connection, || {
            connection
                .query_row(
                    "SELECT (page_count - freelist_count) * page_size \
                     FROM pragma_page_count(), pragma_freelist_count(), pragma_page_size()",
                    [],
                    |row| row.get::<_, u64>(0),
                )
                .map_err(|error| error.to_string())
        })
    })
    .unwrap_or_else(|| Err(format!("no db for {scope}")))
}

/// `Ok(Some(at_ms))` when an outermost commit published a dirty committed
/// alarm: the caller performs the arm-time wake-entry gate before acking the
/// transaction. Rollbacks restore previously committed (already covered)
/// state and never gate.
pub fn transaction_control(
    scope: &str,
    action: &str,
    nested: bool,
    savepoint: &str,
) -> Result<Option<i64>, String> {
    if let Some(error) = sql_critical_error(scope) {
        let result = match action {
            "rollback" => Ok(None),
            "rollback_explicit" => Err(error),
            "commit" => {
                Err("Cannot commit transaction due to an earlier SQL critical error".to_string())
            }
            _ => Err(error),
        };
        if result.is_ok() && !nested && action == "rollback" {
            publish_alarm_if_transaction_dirty(scope);
        }
        return result;
    }
    if nested
        && !savepoint
            .strip_prefix("cells_tx_")
            .is_some_and(|suffix| suffix.parse::<u64>().is_ok())
    {
        return Err("invalid storage transaction savepoint".to_string());
    }
    let result = with(scope, |connection| {
        without_sql_authorizer(connection, || {
            let query = match (action, nested) {
                ("start", false) => "BEGIN IMMEDIATE".to_string(),
                ("start", true) => format!("SAVEPOINT {savepoint}"),
                ("commit", false) => "COMMIT".to_string(),
                ("commit", true) => format!("RELEASE {savepoint}"),
                ("rollback", false) => "ROLLBACK".to_string(),
                ("rollback_explicit", false) => "ROLLBACK".to_string(),
                ("rollback", true) => {
                    format!("ROLLBACK TO {savepoint}; RELEASE {savepoint}")
                }
                ("rollback_explicit", true) => {
                    format!("ROLLBACK TO {savepoint}; RELEASE {savepoint}")
                }
                _ => return Err("invalid storage transaction action".to_string()),
            };
            connection.execute_batch(&query).map_err(|error| {
                // A critical failure while committing or rolling back can make
                // SQLite destroy the whole transaction; classify it so the
                // actor poisons exactly like a mid-statement critical error.
                if action != "start" {
                    if let rusqlite::Error::SqliteFailure(failure, _) = &error {
                        // SAFETY: the handle belongs to this live connection.
                        let database = unsafe { connection.handle() };
                        return sqlite_operation_failure(
                            scope,
                            database,
                            true,
                            failure.extended_code,
                            "control storage transaction",
                        )
                        .to_string();
                    }
                }
                error.to_string()
            })
        })
    })
    .unwrap_or_else(|| Err(format!("no db for {scope}")));
    let result = result.map(|()| None);
    if result.is_ok()
        && !nested
        && matches!(action, "commit" | "rollback" | "rollback_explicit")
    {
        let published = publish_alarm_if_transaction_dirty(scope);
        if action == "commit" {
            return Ok(published);
        }
    }
    result
}

#[cfg(all(test, celld_internal_tests))]
pub fn set_query_only_for_test(scope: &str, enabled: bool) -> anyhow::Result<()> {
    with(scope, |connection| {
        without_sql_authorizer(connection, || {
            connection
                .pragma_update(None, "query_only", enabled)
                .map_err(Into::into)
        })
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

#[cfg(all(test, celld_internal_tests))]
pub fn set_max_page_count_for_test(scope: &str, pages: u32) -> anyhow::Result<()> {
    with(scope, |connection| {
        without_sql_authorizer(connection, || {
            connection
                .pragma_update(None, "max_page_count", pages)
                .map_err(Into::into)
        })
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

/// Arm or clear the thread-local write fault of the test VFS. Only databases
/// opened by `open_with_fault_vfs_for_test` are affected.
#[cfg(all(test, celld_internal_tests))]
pub fn set_write_fault_for_test(enabled: bool) {
    crate::fault::set_write_fault(enabled);
}

/// Shrink the page cache so a large write spills to disk mid-statement, the
/// same trick Workerd's SQLITE_IOERR test uses.
#[cfg(all(test, celld_internal_tests))]
pub fn set_cache_size_for_test(scope: &str, pages: i32) -> anyhow::Result<()> {
    with(scope, |connection| {
        without_sql_authorizer(connection, || {
            connection
                .pragma_update(None, "cache_size", pages)
                .map_err(Into::into)
        })
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

/// Interrupt the next statement on `scope` through SQLite's real interrupt
/// machinery: a progress handler that aborts on its first callback.
#[cfg(all(test, celld_internal_tests))]
pub fn set_interrupt_fault_for_test(
    scope: &str,
    enabled: bool,
) -> anyhow::Result<()> {
    unsafe extern "C" fn interrupt(
        _: *mut std::ffi::c_void,
    ) -> std::ffi::c_int {
        1
    }
    with(scope, |connection| {
        // SAFETY: the handle belongs to this live connection.
        unsafe {
            let database = connection.handle();
            if enabled {
                rusqlite::ffi::sqlite3_progress_handler(
                    database,
                    1,
                    Some(interrupt),
                    std::ptr::null_mut(),
                );
            } else {
                rusqlite::ffi::sqlite3_progress_handler(
                    database,
                    0,
                    None,
                    std::ptr::null_mut(),
                );
            }
        }
        Ok(())
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

/// Register `cells_nomem_for_test()`, a scalar that reports allocation
/// failure through SQLite's own OOM path (`sqlite3_result_error_nomem`), so a
/// statement fails with SQLITE_NOMEM exactly as a real allocator failure
/// would.
#[cfg(all(test, celld_internal_tests))]
pub fn register_nomem_function_for_test(scope: &str) -> anyhow::Result<()> {
    unsafe extern "C" fn nomem(
        context: *mut rusqlite::ffi::sqlite3_context,
        _argc: std::ffi::c_int,
        _argv: *mut *mut rusqlite::ffi::sqlite3_value,
    ) {
        rusqlite::ffi::sqlite3_result_error_nomem(context);
    }
    with(scope, |connection| {
        // SAFETY: the handle belongs to this live connection; the function is
        // stateless.
        let rc = unsafe {
            rusqlite::ffi::sqlite3_create_function_v2(
                connection.handle(),
                c"cells_nomem_for_test".as_ptr(),
                0,
                rusqlite::ffi::SQLITE_UTF8,
                std::ptr::null_mut(),
                Some(nomem),
                None,
                None,
                None,
            )
        };
        anyhow::ensure!(
            rc == rusqlite::ffi::SQLITE_OK,
            "create nomem function: {rc}",
        );
        Ok(())
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

#[cfg(all(test, celld_internal_tests))]
pub fn sql_limit_for_test(scope: &str, category: i32) -> anyhow::Result<i32> {
    with(scope, |connection| {
        // SAFETY: a negative new value queries the live connection's current
        // limit without changing it.
        Ok(unsafe { rusqlite::ffi::sqlite3_limit(connection.handle(), category, -1) })
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

#[cfg(all(test, celld_internal_tests))]
pub fn sql_statement_cache_stats(scope: &str) -> (usize, usize, usize) {
    SQL_STATEMENT_CACHES.with(|caches| {
        caches.borrow().get(scope).map_or((0, 0, 0), |cache| {
            (
                cache.entries.len(),
                cache.total_size,
                cache
                    .entries
                    .values()
                    .filter(|entry| !entry.statement.is_null())
                    .count(),
            )
        })
    })
}

// Test-only single-key helpers; the runtime goes through the batched forms.
#[cfg(test)]
pub fn get(scope: &str, key: &str) -> Option<String> {
    with(scope, |c| {
        c.query_row(
            "SELECT v FROM kv WHERE scope=?1 AND k=?2",
            [scope, key],
            |r| r.get::<_, String>(0),
        )
        .ok()
    })?
}

pub fn get_stored(scope: &str, key: &str) -> anyhow::Result<Option<StoredValue>> {
    with(scope, |c| {
        c.query_row(
            "SELECT v FROM kv WHERE scope=?1 AND k=?2",
            [scope, key],
            |row| Ok(stored_value(row.get_ref(0)?)),
        )
        .optional()
        .map_err(Into::into)
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

/// Fetch multiple keys in storage order, omitting absent keys. Duplicate input
/// keys are coalesced, matching Durable Object storage's map-like result.
#[cfg(all(test, celld_internal_tests))]
pub fn get_many(scope: &str, keys: &[String]) -> anyhow::Result<Vec<(String, String)>> {
    get_many_stored(scope, keys)?
        .into_iter()
        .map(|(key, value)| match value {
            StoredValue::LegacyJson(value) => Ok((key, value)),
            StoredValue::V8(_) => Err(anyhow::anyhow!(
                "structured-clone value cannot be read as legacy JSON",
            )),
        })
        .collect()
}

pub fn get_many_stored(
    scope: &str,
    keys: &[String],
) -> anyhow::Result<Vec<(String, StoredValue)>> {
    let mut keys = keys.to_vec();
    keys.sort();
    keys.dedup();
    with(scope, |c| {
        let mut statement = c.prepare("SELECT v FROM kv WHERE scope=?1 AND k=?2")?;
        let mut values = Vec::new();
        for key in keys {
            if let Some(value) = statement
                .query_row(rusqlite::params![scope, key], |row| {
                    Ok(stored_value(row.get_ref(0)?))
                })
                .optional()?
            {
                values.push((key, value));
            }
        }
        Ok(values)
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

#[cfg(test)]
pub fn put(scope: &str, key: &str, val: &str) {
    with(scope, |c| {
        c.execute(
            "INSERT INTO kv(scope,k,v) VALUES(?1,?2,?3) \
         ON CONFLICT(scope,k) DO UPDATE SET v=excluded.v",
            [scope, key, val],
        )
    });
}

pub fn put_serialized(scope: &str, key: &str, value: &[u8]) -> anyhow::Result<()> {
    with(scope, |c| {
        c.execute(
            "INSERT INTO kv(scope,k,v) VALUES(?1,?2,?3) \
             ON CONFLICT(scope,k) DO UPDATE SET v=excluded.v",
            rusqlite::params![scope, key, value],
        )
        .map(|_| ())
        .map_err(Into::into)
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

/// Atomically apply a multi-key put.
#[cfg(all(test, celld_internal_tests))]
pub fn put_many(scope: &str, entries: &[(String, String)]) -> anyhow::Result<()> {
    if entries.is_empty() {
        return with(scope, |_| ())
            .ok_or_else(|| anyhow::anyhow!("no db for {scope}"));
    }
    with_mut(scope, |c| without_sql_authorizer_mut(c, |c| {
        if !c.is_autocommit() {
            return with_batch_savepoint(c, |c| {
                let mut statement = c.prepare(
                    "INSERT INTO kv(scope,k,v) VALUES(?1,?2,?3) \
                     ON CONFLICT(scope,k) DO UPDATE SET v=excluded.v",
                )?;
                for (key, value) in entries {
                    statement.execute(rusqlite::params![scope, key, value])?;
                }
                Ok(())
            });
        }
        let transaction = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        {
            let mut statement = transaction.prepare(
                "INSERT INTO kv(scope,k,v) VALUES(?1,?2,?3) \
                 ON CONFLICT(scope,k) DO UPDATE SET v=excluded.v",
            )?;
            for (key, value) in entries {
                statement.execute(rusqlite::params![scope, key, value])?;
            }
        }
        transaction.commit()?;
        Ok(())
    }))
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

pub fn put_many_serialized(
    scope: &str,
    entries: &[(String, Vec<u8>)],
) -> anyhow::Result<()> {
    if entries.is_empty() {
        return with(scope, |_| ())
            .ok_or_else(|| anyhow::anyhow!("no db for {scope}"));
    }
    with_mut(scope, |c| without_sql_authorizer_mut(c, |c| {
        let write = |c: &Connection| -> anyhow::Result<()> {
            let mut statement = c.prepare(
                "INSERT INTO kv(scope,k,v) VALUES(?1,?2,?3) \
                 ON CONFLICT(scope,k) DO UPDATE SET v=excluded.v",
            )?;
            for (key, value) in entries {
                statement.execute(rusqlite::params![scope, key, value])?;
            }
            Ok(())
        };
        if !c.is_autocommit() {
            return with_batch_savepoint(c, write);
        }
        let transaction = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        write(&transaction)?;
        transaction.commit()?;
        Ok(())
    }))
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

pub fn delete(scope: &str, key: &str) -> anyhow::Result<bool> {
    with(scope, |c| {
        c.execute("DELETE FROM kv WHERE scope=?1 AND k=?2", [scope, key])
            .map(|n| n > 0)
            .map_err(Into::into)
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

/// Atomically delete multiple keys and return the number that existed.
pub fn delete_many(scope: &str, keys: &[String]) -> anyhow::Result<usize> {
    if keys.is_empty() {
        return with(scope, |_| 0)
            .ok_or_else(|| anyhow::anyhow!("no db for {scope}"));
    }
    with_mut(scope, |c| without_sql_authorizer_mut(c, |c| {
        if !c.is_autocommit() {
            return with_batch_savepoint(c, |c| {
                let mut statement = c.prepare("DELETE FROM kv WHERE scope=?1 AND k=?2")?;
                let mut deleted = 0;
                for key in keys {
                    deleted += statement.execute(rusqlite::params![scope, key])?;
                }
                Ok(deleted)
            });
        }
        let transaction = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let mut deleted = 0;
        {
            let mut statement = transaction.prepare("DELETE FROM kv WHERE scope=?1 AND k=?2")?;
            for key in keys {
                deleted += statement.execute(rusqlite::params![scope, key])?;
            }
        }
        transaction.commit()?;
        Ok(deleted)
    }))
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

/// List a half-open key range in bytewise SQLite text order.
#[cfg(all(test, celld_internal_tests))]
pub fn list(
    scope: &str,
    begin: Option<&str>,
    end: Option<&str>,
    limit: Option<usize>,
    reverse: bool,
) -> anyhow::Result<Vec<(String, String)>> {
    list_with_options(scope, begin, end, None, None, limit, reverse)
}

/// List storage using the full Durable Object key-filter surface. Predicates
/// stay in SQLite so large objects are not materialized and filtered in V8.
#[cfg(all(test, celld_internal_tests))]
pub fn list_with_options(
    scope: &str,
    begin: Option<&str>,
    end: Option<&str>,
    start_after: Option<&str>,
    prefix: Option<&str>,
    limit: Option<usize>,
    reverse: bool,
) -> anyhow::Result<Vec<(String, String)>> {
    list_stored_with_options(scope, begin, end, start_after, prefix, limit, reverse)?
        .into_iter()
        .map(|(key, value)| match value {
            StoredValue::LegacyJson(value) => Ok((key, value)),
            StoredValue::V8(_) => Err(anyhow::anyhow!(
                "structured-clone value cannot be read as legacy JSON",
            )),
        })
        .collect()
}

pub fn list_stored_with_options(
    scope: &str,
    begin: Option<&str>,
    end: Option<&str>,
    start_after: Option<&str>,
    prefix: Option<&str>,
    limit: Option<usize>,
    reverse: bool,
) -> anyhow::Result<Vec<(String, StoredValue)>> {
    with(scope, |c| {
        let (query, parameters) =
            list_query(scope, begin, end, start_after, prefix, limit, reverse);
        let mut statement = c.prepare(&query)?;
        let rows = statement.query_map(rusqlite::params_from_iter(parameters), |row| {
            Ok((row.get(0)?, stored_value(row.get_ref(1)?)))
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

fn list_query(
    scope: &str,
    begin: Option<&str>,
    end: Option<&str>,
    start_after: Option<&str>,
    prefix: Option<&str>,
    limit: Option<usize>,
    reverse: bool,
) -> (String, Vec<rusqlite::types::Value>) {
    use rusqlite::types::Value;

    let order = if reverse { "DESC" } else { "ASC" };
    let mut query = String::from("SELECT k,v FROM kv WHERE scope=?");
    let mut parameters = vec![Value::Text(scope.to_string())];
    if let Some(begin) = begin {
        query.push_str(" AND k>=?");
        parameters.push(Value::Text(begin.to_string()));
    }
    if let Some(start_after) = start_after {
        query.push_str(" AND k>?");
        parameters.push(Value::Text(start_after.to_string()));
    }
    if let Some(end) = end {
        query.push_str(" AND k<?");
        parameters.push(Value::Text(end.to_string()));
    }
    if let Some(prefix) = prefix {
        query.push_str(" AND k>=?");
        parameters.push(Value::Text(prefix.to_string()));
        if let Some(upper_bound) = prefix_upper_bound(prefix) {
            query.push_str(" AND k<?");
            parameters.push(Value::Text(upper_bound));
        }
    }
    query.push_str(&format!(" ORDER BY k {order} LIMIT ?"));
    parameters.push(Value::Integer(
        limit.unwrap_or(usize::MAX).min(i64::MAX as usize) as i64,
    ));
    (query, parameters)
}

fn sqlite_failure(
    database: *mut rusqlite::ffi::sqlite3,
    operation: &str,
) -> anyhow::Error {
    // SAFETY: database is the live handle owned by the scope's Connection.
    let detail = unsafe {
        CStr::from_ptr(rusqlite::ffi::sqlite3_errmsg(database))
            .to_string_lossy()
            .into_owned()
    };
    anyhow::anyhow!("{operation}: {detail}")
}

pub fn sql_critical_error(scope: &str) -> Option<String> {
    SQL_CRITICAL_ERRORS.with(|errors| errors.borrow().get(scope).cloned())
}

fn require_sql_healthy(scope: &str) -> Result<(), String> {
    match sql_critical_error(scope) {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn sqlite_operation_failure(
    scope: &str,
    database: *mut rusqlite::ffi::sqlite3,
    started_in_transaction: bool,
    result_code: i32,
    operation: &str,
) -> anyhow::Error {
    let error = sqlite_failure(database, operation);
    // The classification is sans-IO (`celld_logic::sqlite`); only the autocommit
    // read is FFI. Pin the reified codes against rusqlite's at COMPILE time, so
    // a drift is a build error, not a debug-only surprise.
    const _: () = assert!(
        celld_logic::sqlite::SQLITE_FULL == rusqlite::ffi::SQLITE_FULL
            && celld_logic::sqlite::SQLITE_IOERR == rusqlite::ffi::SQLITE_IOERR
            && celld_logic::sqlite::SQLITE_NOMEM == rusqlite::ffi::SQLITE_NOMEM
            && celld_logic::sqlite::SQLITE_INTERRUPT == rusqlite::ffi::SQLITE_INTERRUPT,
    );
    let now_autocommit = unsafe { rusqlite::ffi::sqlite3_get_autocommit(database) != 0 };
    let poison = celld_logic::sqlite::classify_failure(
        result_code,
        started_in_transaction,
        now_autocommit,
    ) == celld_logic::sqlite::Disposition::Poison;
    if poison {
        SQL_CRITICAL_ERRORS.with(|errors| {
            errors
                .borrow_mut()
                .entry(scope.to_string())
                .or_insert_with(|| error.to_string());
        });
    }
    error
}

unsafe fn bind_cursor_value(
    database: *mut rusqlite::ffi::sqlite3,
    statement: *mut rusqlite::ffi::sqlite3_stmt,
    index: i32,
    value: &rusqlite::types::Value,
) -> anyhow::Result<()> {
    use rusqlite::types::Value;
    let result = match value {
        Value::Null => rusqlite::ffi::sqlite3_bind_null(statement, index),
        Value::Integer(value) => {
            rusqlite::ffi::sqlite3_bind_int64(statement, index, *value)
        }
        Value::Real(value) => rusqlite::ffi::sqlite3_bind_double(statement, index, *value),
        Value::Text(value) => rusqlite::ffi::sqlite3_bind_text(
            statement,
            index,
            value.as_ptr().cast(),
            value.len().min(i32::MAX as usize) as i32,
            rusqlite::ffi::SQLITE_TRANSIENT(),
        ),
        Value::Blob(value) => rusqlite::ffi::sqlite3_bind_blob(
            statement,
            index,
            value.as_ptr().cast(),
            value.len().min(i32::MAX as usize) as i32,
            rusqlite::ffi::SQLITE_TRANSIENT(),
        ),
    };
    if result == rusqlite::ffi::SQLITE_OK {
        Ok(())
    } else {
        Err(sqlite_failure(database, "bind sync KV cursor"))
    }
}

fn close_sync_list_cursors(scope: &str) {
    SYNC_LIST_CURSORS.with(|cursors| {
        cursors
            .borrow_mut()
            .retain(|_, cursor| cursor.scope != scope);
    });
}

fn close_sql_cursors(scope: &str) {
    SQL_CURSORS.with(|cursors| {
        cursors
            .borrow_mut()
            .retain(|_, cursor| cursor.scope != scope);
    });
}

pub fn sync_list_start(
    scope: &str,
    begin: Option<&str>,
    end: Option<&str>,
    start_after: Option<&str>,
    prefix: Option<&str>,
    limit: Option<usize>,
    reverse: bool,
) -> anyhow::Result<u64> {
    close_sync_list_cursors(scope);
    let (query, parameters) =
        list_query(scope, begin, end, start_after, prefix, limit, reverse);
    let query = CString::new(query)?;
    let cursor = with(scope, |connection| -> anyhow::Result<SyncListCursor> {
        // SAFETY: the connection remains resident in DBS for the cursor's
        // lifetime. close() finalizes all of its cursors before removal.
        unsafe {
            let database = connection.handle();
            let mut statement = std::ptr::null_mut();
            let result = rusqlite::ffi::sqlite3_prepare_v2(
                database,
                query.as_ptr(),
                -1,
                &mut statement,
                std::ptr::null_mut(),
            );
            if result != rusqlite::ffi::SQLITE_OK {
                return Err(sqlite_failure(database, "prepare sync KV cursor"));
            }
            for (offset, value) in parameters.iter().enumerate() {
                if let Err(error) =
                    bind_cursor_value(database, statement, offset as i32 + 1, value)
                {
                    rusqlite::ffi::sqlite3_finalize(statement);
                    return Err(error);
                }
            }
            Ok(SyncListCursor {
                scope: scope.to_string(),
                database,
                statement,
            })
        }
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))?;
    let id = NEXT_SYNC_LIST_CURSOR.fetch_add(1, Ordering::Relaxed);
    SYNC_LIST_CURSORS.with(|cursors| cursors.borrow_mut().insert(id, cursor));
    Ok(id)
}

pub fn sync_list_next(cursor_id: u64) -> anyhow::Result<Option<(String, StoredValue)>> {
    SYNC_LIST_CURSORS.with(|cursors| {
        let mut cursors = cursors.borrow_mut();
        let cursor = cursors
            .get_mut(&cursor_id)
            .ok_or_else(|| anyhow::anyhow!("sync KV cursor was invalidated"))?;
        // SAFETY: the cursor owns a prepared statement on its still-live
        // connection. Column bytes are copied before the next step/finalize.
        let result = unsafe { rusqlite::ffi::sqlite3_step(cursor.statement) };
        match result {
            rusqlite::ffi::SQLITE_ROW => {
                let key = unsafe {
                    let length =
                        rusqlite::ffi::sqlite3_column_bytes(cursor.statement, 0) as usize;
                    let pointer = rusqlite::ffi::sqlite3_column_text(cursor.statement, 0);
                    if length == 0 {
                        String::new()
                    } else {
                        String::from_utf8_lossy(std::slice::from_raw_parts(pointer, length))
                            .into_owned()
                    }
                };
                let value = unsafe {
                    match rusqlite::ffi::sqlite3_column_type(cursor.statement, 1) {
                        rusqlite::ffi::SQLITE_BLOB => {
                            let length =
                                rusqlite::ffi::sqlite3_column_bytes(cursor.statement, 1) as usize;
                            let pointer =
                                rusqlite::ffi::sqlite3_column_blob(cursor.statement, 1).cast();
                            StoredValue::V8(if length == 0 {
                                Vec::new()
                            } else {
                                std::slice::from_raw_parts(pointer, length).to_vec()
                            })
                        }
                        rusqlite::ffi::SQLITE_TEXT => {
                            let length =
                                rusqlite::ffi::sqlite3_column_bytes(cursor.statement, 1) as usize;
                            let pointer =
                                rusqlite::ffi::sqlite3_column_text(cursor.statement, 1);
                            StoredValue::LegacyJson(
                                if length == 0 {
                                    String::new()
                                } else {
                                    String::from_utf8_lossy(std::slice::from_raw_parts(
                                        pointer, length,
                                    ))
                                    .into_owned()
                                },
                            )
                        }
                        rusqlite::ffi::SQLITE_INTEGER => StoredValue::LegacyJson(
                            rusqlite::ffi::sqlite3_column_int64(cursor.statement, 1).to_string(),
                        ),
                        rusqlite::ffi::SQLITE_FLOAT => StoredValue::LegacyJson(
                            rusqlite::ffi::sqlite3_column_double(cursor.statement, 1).to_string(),
                        ),
                        _ => StoredValue::LegacyJson("null".to_string()),
                    }
                };
                Ok(Some((key, value)))
            }
            rusqlite::ffi::SQLITE_DONE => {
                cursors.remove(&cursor_id);
                Ok(None)
            }
            _ => {
                let error = sqlite_failure(cursor.database, "step sync KV cursor");
                cursors.remove(&cursor_id);
                Err(error)
            }
        }
    })
}

/// Smallest string strictly above every string beginning with `prefix` under
/// SQLite's default binary UTF-8 collation.
fn prefix_upper_bound(prefix: &str) -> Option<String> {
    let mut characters = prefix.chars().collect::<Vec<_>>();
    for index in (0..characters.len()).rev() {
        let mut next = characters[index] as u32 + 1;
        while next <= char::MAX as u32 {
            if let Some(character) = char::from_u32(next) {
                characters.truncate(index);
                characters.push(character);
                return Some(characters.into_iter().collect());
            }
            next += 1;
        }
    }
    None
}

/// A local SQLite transaction implementing the portable portion of Workerd's
/// ActorCache transaction contract.
#[cfg(all(test, celld_internal_tests))]
pub struct KvTransaction<'connection> {
    scope: String,
    transaction: rusqlite::Transaction<'connection>,
}

#[cfg(all(test, celld_internal_tests))]
impl KvTransaction<'_> {
    pub fn get(&self, key: &str) -> anyhow::Result<Option<String>> {
        match self.transaction.query_row(
            "SELECT v FROM kv WHERE scope=?1 AND k=?2",
            rusqlite::params![self.scope, key],
            |row| row.get(0),
        ) {
            Ok(value) => Ok(Some(value)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub fn get_many(&self, keys: &[String]) -> anyhow::Result<Vec<(String, String)>> {
        let mut keys = keys.to_vec();
        keys.sort();
        keys.dedup();
        let mut statement = self
            .transaction
            .prepare("SELECT v FROM kv WHERE scope=?1 AND k=?2")?;
        let mut values = Vec::new();
        for key in keys {
            if let Some(value) = statement
                .query_row(rusqlite::params![self.scope, key], |row| {
                    row.get::<_, String>(0)
                })
                .optional()?
            {
                values.push((key, value));
            }
        }
        Ok(values)
    }

    pub fn put(&self, key: &str, value: &str) -> anyhow::Result<()> {
        self.transaction.execute(
            "INSERT INTO kv(scope,k,v) VALUES(?1,?2,?3) \
             ON CONFLICT(scope,k) DO UPDATE SET v=excluded.v",
            rusqlite::params![self.scope, key, value],
        )?;
        Ok(())
    }

    pub fn put_many(&self, entries: &[(String, String)]) -> anyhow::Result<()> {
        let mut statement = self.transaction.prepare(
            "INSERT INTO kv(scope,k,v) VALUES(?1,?2,?3) \
             ON CONFLICT(scope,k) DO UPDATE SET v=excluded.v",
        )?;
        for (key, value) in entries {
            statement.execute(rusqlite::params![self.scope, key, value])?;
        }
        Ok(())
    }

    pub fn delete_many(&self, keys: &[String]) -> anyhow::Result<usize> {
        let mut statement = self
            .transaction
            .prepare("DELETE FROM kv WHERE scope=?1 AND k=?2")?;
        let mut deleted = 0;
        for key in keys {
            deleted += statement.execute(rusqlite::params![self.scope, key])?;
        }
        Ok(deleted)
    }

    pub fn get_alarm(&self) -> anyhow::Result<Option<i64>> {
        Ok(self
            .transaction
            .query_row(
                "SELECT at_ms FROM alarms WHERE scope=?1",
                [self.scope.as_str()],
                |row| row.get(0),
            )
            .optional()?)
    }

    pub fn set_alarm(&self, at_ms: i64) -> anyhow::Result<()> {
        self.transaction.execute(
            "INSERT INTO alarms(scope,at_ms,retry,counted_retry,generation) \
             VALUES(?1,?2,0,0,random()) \
             ON CONFLICT(scope) DO UPDATE SET \
               at_ms=excluded.at_ms, retry=0, counted_retry=0, generation=random()",
            rusqlite::params![self.scope, at_ms],
        )?;
        Ok(())
    }

    pub fn delete_alarm(&self) -> anyhow::Result<()> {
        self.transaction
            .execute("DELETE FROM alarms WHERE scope=?1", [self.scope.as_str()])?;
        Ok(())
    }

    pub fn list(
        &self,
        begin: Option<&str>,
        end: Option<&str>,
        limit: Option<usize>,
        reverse: bool,
    ) -> anyhow::Result<Vec<(String, String)>> {
        let order = if reverse { "DESC" } else { "ASC" };
        let query = format!(
            "SELECT k,v FROM kv \
             WHERE scope=?1 AND (?2 IS NULL OR k>=?2) AND (?3 IS NULL OR k<?3) \
             ORDER BY k {order} LIMIT ?4",
        );
        let mut statement = self.transaction.prepare(&query)?;
        let rows = statement.query_map(
            rusqlite::params![
                self.scope,
                begin,
                end,
                limit.unwrap_or(usize::MAX).min(i64::MAX as usize) as i64,
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }
}

/// Run a KV transaction and commit only if the callback succeeds.
#[cfg(all(test, celld_internal_tests))]
pub fn transaction<T>(
    scope: &str,
    callback: impl FnOnce(&KvTransaction<'_>) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    with_mut(scope, |connection| without_sql_authorizer_mut(connection, |connection| {
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let transaction = KvTransaction {
            scope: scope.to_string(),
            transaction,
        };
        let value = callback(&transaction)?;
        transaction.transaction.commit()?;
        Ok(value)
    }))
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

/// Persist the optional human-readable name associated with an idFromName()
/// actor. This is runtime identity metadata, not user storage, so deleteAll()
/// must not remove it.
pub fn set_actor_name(scope: &str, name: &str) -> anyhow::Result<()> {
    with(scope, |connection| -> anyhow::Result<()> {
        connection.execute(
            "INSERT INTO cell_metadata(scope, actor_name) VALUES(?1, ?2) \
             ON CONFLICT(scope) DO NOTHING",
            rusqlite::params![scope, name],
        )?;
        let stored: Option<String> = connection
            .query_row(
                "SELECT actor_name FROM cell_metadata WHERE scope=?1",
                [scope],
                |row| row.get(0),
            )
            .optional()?;
        if stored.as_deref() != Some(name) {
            anyhow::bail!("actor name conflicts with persisted identity for {scope}");
        }
        Ok(())
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

pub fn get_actor_name(scope: &str) -> anyhow::Result<Option<String>> {
    with(scope, |connection| {
        connection
            .query_row(
                "SELECT actor_name FROM cell_metadata WHERE scope=?1",
                [scope],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

/// Delete all user storage, optionally deleting the alarm. Workerd's
/// ActorCache exposes this distinction internally; compatibility flags decide
/// which form the public JS deleteAll() operation selects.
pub fn delete_all_with_alarm(scope: &str, delete_alarm: bool) -> anyhow::Result<()> {
    let result = with(scope, |c| -> anyhow::Result<()> {
        let mut statement = c.prepare(
            "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'",
        )?;
        let tables = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(statement);
        without_sql_authorizer(c, || {
            c.execute_batch("PRAGMA foreign_keys=OFF; BEGIN IMMEDIATE;")?;
            let result = (|| -> anyhow::Result<()> {
                for table in tables {
                    if table == "cell_metadata" || (!delete_alarm && table == "alarms") {
                        continue;
                    }
                    let quoted = table.replace('"', "\"\"");
                    c.execute_batch(&format!("DROP TABLE IF EXISTS \"{quoted}\";"))?;
                }
                c.execute_batch("COMMIT;")?;
                schema(c)?;
                Ok(())
            })();
            if result.is_err() {
                let _ = c.execute_batch("ROLLBACK;");
            }
            let _ = c.execute_batch("PRAGMA foreign_keys=ON;");
            result
        })
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")));
    if result.is_ok() && delete_alarm {
        publish_alarm(scope);
    }
    result
}

// An alarm write that fails must reach the caller: setAlarm is a durability
// promise, and a swallowed SQLITE_FULL here resolved the JS promise while
// persisting nothing, then republished the stale committed state.
//
// Returns the committed alarm value when the write committed immediately
// (autocommit): the caller performs the arm-time wake-entry gate before
// acking. `None` means the write is deferred inside an explicit transaction;
// the gate runs at that transaction's commit instead.
pub fn set_alarm(scope: &str, at_ms: i64) -> anyhow::Result<Option<i64>> {
    with(scope, |c| {
        c.execute(
            "INSERT INTO alarms(scope,at_ms,retry,counted_retry,generation) \
             VALUES(?1,?2,0,0,random()) \
             ON CONFLICT(scope) DO UPDATE SET \
               at_ms=excluded.at_ms, retry=0, counted_retry=0, generation=random()",
            rusqlite::params![scope, at_ms],
        )
        .map(|_| ())
        .map_err(Into::into)
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))?;
    Ok(publish_alarm(scope))
}

pub fn get_alarm(scope: &str) -> Option<i64> {
    let alarm = alarm_state(scope);
    let hidden_during_handler = ACTIVE_ALARMS.with(|alarms| {
        alarms
            .borrow()
            .get(scope)
            .is_some_and(|active| active.generation == alarm.map(|(_, generation)| generation))
    });
    if hidden_during_handler {
        None
    } else {
        alarm.map(|(at_ms, _)| at_ms)
    }
}

pub fn delete_alarm(scope: &str) -> anyhow::Result<()> {
    with(scope, |c| {
        c.execute("DELETE FROM alarms WHERE scope=?1", [scope])
            .map(|_| ())
            .map_err(Into::into)
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))?;
    let _ = publish_alarm(scope); // a delete loosens: never gated
    Ok(())
}

/// Publish alarm mutations to the owning cell thread's eviction state.
///
/// The observer keeps the ordinary request path free of SQLite reads: alarm
/// state changes only when an exceptional alarm operation mutates the table.
pub fn watch_alarm(scope: &str, watcher: Arc<AtomicI64>) {
    watcher.store(
        alarm_state(scope).map_or(-1, |(at_ms, _)| at_ms),
        Ordering::Release,
    );
    ALARM_WATCHERS.with(|watchers| {
        watchers.borrow_mut().insert(scope.to_string(), watcher);
    });
}

pub fn unwatch_alarm(scope: &str) {
    ALARM_WATCHERS.with(|watchers| {
        watchers.borrow_mut().remove(scope);
    });
}

/// Publish committed alarm state to the watcher. Returns the committed value
/// (`-1` for none) when publication happened, `None` when the mutation is
/// still inside an explicit transaction (published at its boundary) or the
/// db is gone. The returned value drives the arm-time wake-entry gate.
fn publish_alarm(scope: &str) -> Option<i64> {
    // A storage transaction may call setAlarm()/deleteAlarm() several times
    // before committing or may roll them back. Publish only committed state;
    // the outer transaction boundary reconciles the watcher after either
    // commit or rollback.
    let state = with(scope, |connection| {
        if connection.is_autocommit() {
            Ok(connection
                .query_row(
                    "SELECT at_ms FROM alarms WHERE scope=?1",
                    [scope],
                    |row| row.get::<_, i64>(0),
                )
                .ok()
                .unwrap_or(-1))
        } else {
            Err(())
        }
    });
    let at_ms = match state {
        Some(Ok(at_ms)) => at_ms,
        Some(Err(())) => {
            ALARM_DIRTY_TRANSACTIONS.with(|dirty| {
                dirty.borrow_mut().insert(scope.to_string());
            });
            return None;
        }
        None => return None,
    };
    ALARM_DIRTY_TRANSACTIONS.with(|dirty| {
        dirty.borrow_mut().remove(scope);
    });
    ALARM_WATCHERS.with(|watchers| {
        if let Some(watcher) = watchers.borrow().get(scope) {
            watcher.store(at_ms, Ordering::Release);
        }
    });
    Some(at_ms)
}

fn publish_alarm_if_transaction_dirty(scope: &str) -> Option<i64> {
    let dirty = ALARM_DIRTY_TRANSACTIONS.with(|scopes| scopes.borrow().contains(scope));
    if dirty {
        publish_alarm(scope)
    } else {
        None
    }
}

/// Retry count of `scope`'s alarm if it is due at/before `now_ms` (each cell
/// thread self-polls its own db between requests — no central scheduler).
#[cfg(all(test, celld_internal_tests))]
pub fn due_alarm(scope: &str, now_ms: i64) -> Option<i64> {
    due_alarm_entry(scope, now_ms).map(|(_, retry)| retry)
}

/// Scheduled time and retry count for an alarm due at/before `now_ms`.
pub fn due_alarm_entry(scope: &str, now_ms: i64) -> Option<(i64, i64)> {
    with(scope, |c| {
        c.query_row(
            "SELECT at_ms,retry FROM alarms WHERE scope=?1 AND at_ms<=?2",
            rusqlite::params![scope, now_ms],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .ok()
    })
    .flatten()
}

fn alarm_state(scope: &str) -> Option<(i64, i64)> {
    with(scope, |connection| {
        connection
            .query_row(
                "SELECT at_ms,generation FROM alarms WHERE scope=?1",
                [scope],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .ok()
    })?
}

/// The persisted alarm in a restored cell DB, read directly by path — the
/// activation executor seeds the lifecycle Cell's alarm mirror from this at
/// restore time, before the isolate opens the scope's `with` connection. Read
/// only; the connection is dropped before `spawn_cell` opens the same file.
/// Returns (due_wall_ms, generation, retry, counted_retry); `None` if unarmed.
pub fn persisted_alarm(db_path: &str, scope: &str) -> Option<(i64, i64, u32, u32)> {
    let connection = rusqlite::Connection::open_with_flags(
        db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .ok()?;
    connection
        .query_row(
            "SELECT at_ms,generation,retry,counted_retry FROM alarms WHERE scope=?1",
            [scope],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get::<_, i64>(2)? as u32,
                    row.get::<_, i64>(3)? as u32,
                ))
            },
        )
        .ok()
}

/// Enter an alarm handler. Until the handler changes the alarm, `getAlarm()`
/// observes null as required by the Durable Object contract.
pub fn begin_alarm_handler(scope: &str, fired_at_ms: i64) {
    let generation = alarm_state(scope).map(|(_, generation)| generation);
    ACTIVE_ALARMS.with(|alarms| {
        alarms.borrow_mut().insert(
            scope.to_string(),
            ActiveAlarm {
                fired_at_ms,
                generation,
            },
        );
    });
}

pub fn active_alarm_scheduled_time(scope: &str) -> Option<i64> {
    ACTIVE_ALARMS.with(|alarms| {
        alarms
            .borrow()
            .get(scope)
            .map(|active| active.fired_at_ms)
    })
}

/// Finish an alarm handler. A successful handler consumes its original alarm;
/// a failed handler reschedules it. Any alarm explicitly changed by the
/// handler wins over automatic cleanup or retry.
pub fn finish_alarm_handler(scope: &str, succeeded: bool, now_ms: i64) {
    finish_alarm_handler_with_retry_policy(scope, succeeded, now_ms, true);
}

pub fn finish_alarm_handler_with_retry_policy(
    scope: &str,
    succeeded: bool,
    now_ms: i64,
    retry_counts_against_limit: bool,
) {
    let active = ACTIVE_ALARMS.with(|alarms| alarms.borrow_mut().remove(scope));
    let Some(active) = active else {
        return;
    };
    let current_generation = alarm_state(scope).map(|(_, generation)| generation);
    if active.generation != current_generation {
        return;
    }
    if succeeded {
        with(scope, |connection| {
            connection.execute(
                "DELETE FROM alarms WHERE scope=?1 AND at_ms=?2",
                rusqlite::params![scope, active.fired_at_ms],
            )
        });
        publish_alarm(scope);
    } else {
        bump_alarm_with_policy(scope, now_ms, retry_counts_against_limit);
    }
}

/// Delete the fired alarm unless the handler re-armed it to the future.
#[cfg(all(test, celld_internal_tests))]
pub fn clear_alarm_if_due(scope: &str, fired_at_ms: i64) {
    with(scope, |c| {
        c.execute(
            "DELETE FROM alarms WHERE scope=?1 AND at_ms<=?2",
            rusqlite::params![scope, fired_at_ms],
        )
    });
    publish_alarm(scope);
}

pub fn bump_alarm_with_policy(scope: &str, now_ms: i64, counts_against_limit: bool) {
    with(scope, |c| {
        let (retry, counted_retry): (i64, i64) = c
            .query_row(
                "SELECT retry,counted_retry FROM alarms WHERE scope=?1",
                [scope],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap_or((0, 0));
        // The schedule is sans-IO (`celld_logic::alarm::alarm_retry`); this fn
        // is its executor, reading the counters and writing the new row.
        match celld_logic::alarm::alarm_retry(now_ms, retry, counted_retry, counts_against_limit) {
            celld_logic::alarm::AlarmRetry::GiveUp => {
                let _ = c.execute("DELETE FROM alarms WHERE scope=?1", [scope]);
            }
            celld_logic::alarm::AlarmRetry::Retry { at_ms } => {
                let _ = c.execute(
                    "UPDATE alarms SET at_ms=?2, retry=retry+1, \
                       counted_retry=counted_retry+?3 WHERE scope=?1",
                    rusqlite::params![scope, at_ms, i64::from(counts_against_limit)],
                );
            }
        }
    });
    publish_alarm(scope);
}

#[cfg(test)]
mod smoke_tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_DATABASE: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn sqlite_storage_and_transactions_smoke() {
        let sequence = NEXT_DATABASE.fetch_add(1, Ordering::Relaxed);
        let scope = format!("public-storage-smoke-{sequence}");
        let path = std::env::temp_dir().join(format!(
            "cells-public-storage-smoke-{}-{sequence}.sqlite",
            std::process::id(),
        ));
        open(&scope, path.to_str().expect("temporary path is UTF-8"))
            .expect("open smoke database");

        put(&scope, "committed", r#"{"value":1}"#);
        assert_eq!(get(&scope, "committed").as_deref(), Some(r#"{"value":1}"#));

        transaction_control(&scope, "start", false, "unused")
            .expect("start transaction");
        put(&scope, "rolled-back", r#"{"value":2}"#);
        transaction_control(&scope, "rollback", false, "unused")
            .expect("roll back transaction");
        assert_eq!(get(&scope, "rolled-back"), None);

        let (cursor, columns, row, _, _) =
            sql_cursor_start(&scope, "SELECT 42 AS value", &[])
                .expect("execute smoke query");
        assert_eq!(columns, vec!["value"]);
        assert_eq!(row, Some(vec![serde_json::json!(42)]));
        assert_ne!(cursor, 0);
        assert_eq!(sql_cursor_next(cursor).expect("finish cursor").0, None);

        close(&scope);
        let _ = std::fs::remove_file(&path);
        let path = path.to_string_lossy();
        let _ = std::fs::remove_file(format!("{path}-wal"));
        let _ = std::fs::remove_file(format!("{path}-shm"));
    }

    #[test]
    fn alarm_watcher_tracks_only_alarm_mutations() {
        let sequence = NEXT_DATABASE.fetch_add(1, Ordering::Relaxed);
        let scope = format!("public-alarm-watcher-{sequence}");
        let path = std::env::temp_dir().join(format!(
            "cells-public-alarm-watcher-{}-{sequence}.sqlite",
            std::process::id(),
        ));
        open(&scope, path.to_str().expect("temporary path is UTF-8"))
            .expect("open alarm watcher database");

        let watcher = Arc::new(AtomicI64::new(i64::MIN));
        watch_alarm(&scope, watcher.clone());
        assert_eq!(watcher.load(Ordering::Acquire), -1);

        watcher.store(i64::MIN, Ordering::Release);
        transaction_control(&scope, "start", false, "").expect("start transaction");
        put(&scope, "transaction-unrelated", r#"{"value":1}"#);
        transaction_control(&scope, "commit", false, "").expect("commit transaction");
        assert_eq!(watcher.load(Ordering::Acquire), i64::MIN);

        set_alarm(&scope, 1234).unwrap();
        assert_eq!(watcher.load(Ordering::Acquire), 1234);

        put(&scope, "unrelated", r#"{"value":1}"#);
        assert_eq!(watcher.load(Ordering::Acquire), 1234);

        delete_alarm(&scope).unwrap();
        assert_eq!(watcher.load(Ordering::Acquire), -1);

        set_alarm(&scope, 5678).unwrap();
        delete_all_with_alarm(&scope, true).expect("delete all with alarm");
        assert_eq!(watcher.load(Ordering::Acquire), -1);

        unwatch_alarm(&scope);
        close(&scope);
        let _ = std::fs::remove_file(&path);
        let path = path.to_string_lossy();
        let _ = std::fs::remove_file(format!("{path}-wal"));
        let _ = std::fs::remove_file(format!("{path}-shm"));
    }

    #[test]
    fn alarm_watcher_publishes_only_committed_transaction_state() {
        let sequence = NEXT_DATABASE.fetch_add(1, Ordering::Relaxed);
        let scope = format!("public-alarm-watcher-transaction-{sequence}");
        let path = std::env::temp_dir().join(format!(
            "cells-public-alarm-watcher-transaction-{}-{sequence}.sqlite",
            std::process::id(),
        ));
        open(&scope, path.to_str().expect("temporary path is UTF-8"))
            .expect("open alarm watcher database");

        set_alarm(&scope, 100).unwrap();
        let watcher = Arc::new(AtomicI64::new(i64::MIN));
        watch_alarm(&scope, watcher.clone());
        assert_eq!(watcher.load(Ordering::Acquire), 100);

        transaction_control(&scope, "start", false, "").expect("start transaction");
        delete_alarm(&scope).unwrap();
        assert_eq!(watcher.load(Ordering::Acquire), 100);
        transaction_control(&scope, "rollback", false, "").expect("roll back transaction");
        assert_eq!(watcher.load(Ordering::Acquire), 100);

        transaction_control(&scope, "start", false, "").expect("start transaction");
        set_alarm(&scope, 200).unwrap();
        assert_eq!(watcher.load(Ordering::Acquire), 100);
        transaction_control(&scope, "commit", false, "").expect("commit transaction");
        assert_eq!(watcher.load(Ordering::Acquire), 200);

        transaction_control(&scope, "start", false, "").expect("start transaction");
        delete_alarm(&scope).unwrap();
        assert_eq!(watcher.load(Ordering::Acquire), 200);
        transaction_control(&scope, "commit", false, "").expect("commit transaction");
        assert_eq!(watcher.load(Ordering::Acquire), -1);

        unwatch_alarm(&scope);
        close(&scope);
        let _ = std::fs::remove_file(&path);
        let path = path.to_string_lossy();
        let _ = std::fs::remove_file(format!("{path}-wal"));
        let _ = std::fs::remove_file(format!("{path}-shm"));
    }
}

#[cfg(all(test, celld_internal_tests))]
mod conformance_actor_cache_tests {
    include!(env!("CELLD_CONFORMANCE_ACTOR_CACHE_TESTS"));
}

#[cfg(all(test, celld_internal_tests))]
mod conformance_actor_sqlite_tests {
    include!(env!("CELLD_CONFORMANCE_ACTOR_SQLITE_TESTS"));
}

#[cfg(all(test, celld_internal_tests))]
mod conformance_sqlite_kv_tests {
    include!(env!("CELLD_CONFORMANCE_SQLITE_KV_TESTS"));
}
