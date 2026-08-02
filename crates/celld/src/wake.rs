//! Alarm wake for hibernated cells (on by default).
//!
//! Committed alarm state is mirrored into the bucket as
//! `wake/<YYYY-MM-DDTHH:MM>/<cell>` so a wake hint survives fence, crash,
//! and deploy; the sweep hibernates alarm-bearing cells behind a durable
//! entry, a per-node heap plus boot scan re-activates them, and a per-fleet
//! advisory waker revives orphans whose owner died.
//!
//! Invariants, verified by deterministic simulation:
//! - arm durable ⟹ entry exists, within one sweep tick of the commit;
//! - only completed activation or a durable consume deletes an entry — a
//!   stale entry costs one spurious wake, a missing entry costs a lost wake;
//! - the flusher never touches the request path: it reads the lock-free
//!   `next_alarm_ms` mirror on the existing 5 s sweep tick.
use celld_logic::wake::parse_entry_key;
use celld_logic::wake::Op;
use celld_logic::wake::WakeCore;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use std::collections::HashMap;
use std::sync::Mutex;
use tracing::{info, warn};

/// Stay-resident threshold: alarms due sooner than this keep their cell
/// resident when residency is cheaper than a wake cycle. Alarms further out
/// hibernate behind an entry.
pub fn resident_ms() -> i64 {
    static MS: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *MS.get_or_init(|| {
        std::env::var("CELLD_ALARM_RESIDENT_MS").ok()
            .and_then(|v| v.parse().ok()).unwrap_or(3_600_000)
    })
}

/// Scan the due wake buckets: used by the boot-time orphan scan and the
/// Phase 3 waker tick. Keys sort by due minute, so paging stops at the first
/// future bucket — the scan is O(due entries), never O(all entries).
/// Due entries as (cell, minute_ms). The minute is carried out so a reviving
/// node can adopt the entry it acted on: without it, a cell whose restored
/// truth has no alarm leaves the entry that woke it in the bucket forever.
pub async fn due_scan(c: &Client, bucket: &str, now_ms: i64) -> Vec<(String, i64)> {
    let mut due = Vec::new();
    let mut token: Option<String> = None;
    'pages: loop {
        let mut req = c.list_objects_v2().bucket(bucket).prefix("wake/");
        if let Some(t) = &token { req = req.continuation_token(t); }
        let page = match req.send().await {
            Ok(page) => page,
            Err(e) => { warn!(error = %e, "wake due scan list failed"); break; }
        };
        for object in page.contents() {
            let Some(key) = object.key() else { continue };
            if let Some((minute_ms, cell)) = parse_entry_key(key) {
                if minute_ms > now_ms { break 'pages; } // sorted: all later
                due.push((cell, minute_ms));
            }
        }
        match page.next_continuation_token() {
            Some(t) => token = Some(t.to_string()),
            None => break,
        }
    }
    due.sort();
    due.dedup_by(|a, b| a.0 == b.0);
    due
}

/// Advisory waker-role lease: one holder per fleet to avoid N nodes polling.
/// Correctness never depends on it — concurrent wakers race activation CAS
/// harmlessly — so every failure path just returns false and skips a tick.
pub async fn try_hold_waker(
    c: &Client,
    bucket: &str,
    node: &str,
    now_ms: i64,
    ttl_ms: i64,
) -> bool {
    const KEY: &str = "wake/waker.json";
    let body = |expires: i64| {
        ByteStream::from(
            format!("{{\"node\":{node:?},\"expires_ms\":{expires}}}").into_bytes(),
        )
    };
    let current = c.get_object().bucket(bucket).key(KEY).send().await;
    match current {
        Err(_) => {
            // absent (or unreadable): claim if absent
            c.put_object().bucket(bucket).key(KEY)
                .if_none_match("*")
                .body(body(now_ms + ttl_ms)).send().await.is_ok()
        }
        Ok(resp) => {
            let etag = resp.e_tag().unwrap_or_default().to_string();
            let bytes = match resp.body.collect().await {
                Ok(b) => b.into_bytes(),
                Err(_) => return false,
            };
            let text = String::from_utf8_lossy(&bytes);
            let held_by_us = text.contains(&format!("\"node\":{node:?}"));
            let expires = text.rsplit("\"expires_ms\":").next()
                .and_then(|t| t.trim_end_matches('}').trim().parse::<i64>().ok())
                .unwrap_or(0);
            if celld_logic::wake::waker_may_claim(held_by_us, expires, now_ms) {
                c.put_object().bucket(bucket).key(KEY)
                    .if_match(etag)
                    .body(body(now_ms + ttl_ms)).send().await.is_ok()
            } else {
                false
            }
        }
    }
}

/// Mirrors each resident cell's committed next-alarm into the bucket. One
/// instance per node, driven from the eviction sweep tick. The pure transition
/// (`decide` / `covered` / `due_cells` / `adopt`) is the sans-IO
/// `celld_logic::wake::WakeCore`; this facade adds the lock, the async S3
/// executor, and the shadow-pin log.
pub struct WakeFlusher {
    core: Mutex<WakeCore>,
    /// cells whose alarm pin was already shadow-logged this arming
    logged: Mutex<HashMap<String, i64>>,
}

impl WakeFlusher {
    pub fn new() -> Self {
        WakeFlusher { core: Mutex::new(WakeCore::new()), logged: Mutex::new(HashMap::new()) }
    }

    /// Take responsibility for the entry a restored alarm implies (delegated).
    pub fn adopt(&self, cell: &str, due_ms: i64) {
        self.core.lock().unwrap().adopt(cell, due_ms);
    }

    /// Is this cell's entry state already known to this process?
    pub fn tracks(&self, cell: &str) -> bool {
        self.core.lock().unwrap().tracks(cell)
    }

    /// Reconcile one cell against S3 — the async executor for `WakeCore::decide`.
    /// Failed PUTs keep local state unchanged so the next tick retries (an entry
    /// may be late, never silently absent); failed deletes drop matching state
    /// anyway — a stale entry is one spurious wake. `consume_durable` gates the
    /// final delete of a consumed alarm on the consuming commit's replication.
    pub async fn reconcile(
        &self,
        c: &Client,
        bucket: &str,
        cell: &str,
        next_alarm_ms: i64,
        consume_durable: bool,
    ) {
        let ops = self.core.lock().unwrap().decide(cell, next_alarm_ms, consume_durable);
        for op in ops {
            match op {
                Op::Put { key, due_ms } => {
                    let body = format!("{{\"cell\":{:?},\"due_ms\":{}}}", cell, due_ms);
                    match c.put_object().bucket(bucket).key(&key)
                        .body(ByteStream::from(body.into_bytes())).send().await {
                        Ok(_) => self.core.lock().unwrap().confirm_put(cell, due_ms, key),
                        // A later Delete in this batch depends on this PUT:
                        // deleting the old entry without the new one present
                        // would leave the armed alarm entryless. Abort; the
                        // next tick retries.
                        Err(e) => {
                            warn!(%cell, %key, error = %e, "wake entry put failed");
                            return;
                        }
                    }
                }
                Op::Delete { key } => {
                    // The delete is asynchronous; an arm may have ridden in
                    // since `decide` and cancelled it. Re-check under the
                    // lock immediately before issuing it — performing a
                    // cancelled delete strands an acked alarm with no entry.
                    if !self.core.lock().unwrap().take_delete(cell, &key) {
                        continue;
                    }
                    if let Err(e) = c.delete_object().bucket(bucket).key(&key).send().await {
                        warn!(%cell, %key, error = %e, "wake entry delete failed");
                    }
                    self.core.lock().unwrap().retire(cell, &key);
                }
            }
        }
    }

    /// Arm-time decision — the PUT that must land before this arm is acked,
    /// or `None` when the durable bound already covers it. Pure passthrough to
    /// `WakeCore::arm`; the caller performs the PUT and then `confirm_arm`.
    pub fn arm_op(&self, cell: &str, next_alarm_ms: i64) -> Option<Op> {
        self.core.lock().unwrap().arm(cell, next_alarm_ms)
    }

    /// An arm-time PUT landed: record the proven entry.
    pub fn confirm_arm(&self, cell: &str, due_ms: i64, key: String) {
        self.core.lock().unwrap().confirm_put(cell, due_ms, key);
    }

    /// Is this exact committed alarm durably covered by a proven entry? The
    /// fail-closed gate: eviction of an alarm-bearing cell requires it.
    pub fn covered(&self, cell: &str, next_alarm_ms: i64) -> bool {
        self.core.lock().unwrap().covered(cell, next_alarm_ms)
    }

    /// Entries whose cells this node hibernated and whose due time has
    /// arrived — the tier-2 wake heap, derived from flusher state.
    pub fn due_cells(&self, now_ms: i64) -> Vec<String> {
        self.core.lock().unwrap().due_cells(now_ms)
    }

    /// A wake for `cell` resolved to a remote owner: its alarm is no longer
    /// this node's to track.
    pub fn forget(&self, cell: &str) {
        self.core.lock().unwrap().forget(cell);
        self.logged.lock().unwrap().remove(cell);
    }

    /// Shadow log, once per (cell, arming): the pin held a cell resident that
    /// hibernation would otherwise have taken.
    pub fn log_pinned(&self, cell: &str, next_alarm_ms: i64, idle_s: u64) {
        let mut logged = self.logged.lock().unwrap();
        if logged.get(cell) == Some(&next_alarm_ms) { return; }
        logged.insert(cell.to_string(), next_alarm_ms);
        let covered = self.covered(cell, next_alarm_ms);
        info!(%cell, next_alarm_ms, idle_s, covered,
            "alarm pin held cell resident (shadow: would hibernate)");
    }
}

