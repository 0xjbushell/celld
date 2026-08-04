//! In-process replication backend built on `celld-ltx`.
//!
//! The Litestream backend ([`crate::replication::NodeRepl`]) runs an external
//! process that watches a directory and answers durability over a control
//! socket. `LtxRepl` does the same job inside celld: one shared `object_store`
//! client for the whole node, and a managed `celld_ltx::Db` per resident cell
//! that captures the cell's committed WAL and uploads it on demand. No process,
//! no directory-watch lag — a just-written cell is registered the instant it
//! activates, so the output gate can prove a fresh cell durable with no
//! cold-start window. Litestream stays reachable behind `CELLD_REPLICATOR`.
//!
//! The object layout mirrors the Litestream tree (`cells/<cell>/ltx/e<epoch>/`)
//! but the files are LTX, not Litestream generations — the two are not
//! interoperable, so a bucket is written by one backend or the other.

use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::anyhow;
use tokio::sync::Notify;
use tokio::sync::Semaphore;
use celld_ltx::object_store::ObjectStore;
use celld_ltx::replica;
use celld_ltx::Db;
use celld_ltx::ObjectStoreClient;
use celld_ltx::ObjectStoreConfig;
use celld_ltx::Replica;
use celld_ltx::TXID;
use tracing::info;
use tracing::warn;

use crate::replication::prune_watch;
use crate::replication::sqlite_snapshot;
use crate::replication::ActivationOptions;
use crate::replication::ActivationResult;
use crate::replication::RestoredSnapshot;
use crate::replication::StorageCredentials;
use crate::replication::SyncWait;

/// Max cells uploading concurrently across the node. Caps blocking-pool threads
/// and in-flight object-store requests under high write fan-out.
const SYNC_CONCURRENCY: usize = 64;

/// One resident cell's replication state: the `celld_ltx::Db` shadowing its WAL
/// (behind a `std::sync::Mutex` because the `rusqlite` handle is `!Sync` and
/// must never cross an `.await`, so every capture+upload runs inside a
/// `spawn_blocking` closure) plus the durability tickets the output gate waits
/// on. `req_seq` counts durability requests; `synced_seq` is the highest ticket
/// a completed background sync captured. A write waits for `synced_seq >= its
/// ticket`, so concurrent writes to one cell ride a single batched upload —
/// and, because a sync credits only tickets whose writes committed before it
/// started (which the sync's `db.sync` captures), never one it did not upload.
struct Cell {
    replica: Mutex<Replica<ObjectStoreClient>>,
    req_seq: AtomicU64,
    synced_seq: AtomicU64,
    /// Set while a sync for this cell is in flight, so the loop never runs two
    /// at once for one cell (they would serialize on the mutex and waste work).
    syncing: AtomicBool,
    /// Notified when `synced_seq` advances (or a sync fails), waking waiters.
    ready: Notify,
}
type CellHandle = Arc<Cell>;

pub struct LtxRepl {
    /// Local root: cell dbs live at `watch/<cell>/ltx/e<epoch>/db.sqlite`, the
    /// same layout the Litestream backend uses.
    watch: PathBuf,
    bucket: String,
    endpoint: Option<String>,
    region: String,
    credentials: Option<StorageCredentials>,
    /// One connection pool for the whole node, shared by every cell client.
    store: Arc<dyn ObjectStore>,
    cells: Arc<Mutex<HashMap<(String, u64), CellHandle>>>,
    /// Woken when a cell's `committed` advances, so the background loop syncs
    /// without polling; a slow tick backstops any missed notification.
    dirty: Arc<Notify>,
}

impl LtxRepl {
    pub fn start(
        watch: &Path,
        bucket: String,
        endpoint: Option<String>,
        region: String,
        credentials: Option<StorageCredentials>,
    ) -> anyhow::Result<Self> {
        let store = node_config(&bucket, endpoint.as_deref(), &region, credentials.as_ref())
            .build_store()
            .map_err(|error| anyhow!("build shared object store: {error}"))?;
        let cells: Arc<Mutex<HashMap<(String, u64), CellHandle>>> = Arc::default();
        let dirty = Arc::new(Notify::new());
        // Bound how many cells upload at once so one slow cell cannot stall the
        // others and a thousand hot cells cannot open a thousand uploads.
        let slots = Arc::new(Semaphore::new(SYNC_CONCURRENCY));
        tokio::spawn(sync_loop(cells.clone(), dirty.clone(), slots));
        Ok(Self {
            watch: watch.to_path_buf(),
            bucket,
            endpoint,
            region,
            credentials,
            store,
            cells,
            dirty,
        })
    }

    fn db_path(&self, cell: &str, epoch: u64) -> PathBuf {
        self.watch
            .join(cell)
            .join("ltx")
            .join(format!("e{epoch}"))
            .join("db.sqlite")
    }

    /// A per-cell client over the shared store, keyed to the cell's epoch
    /// prefix. `cells/<cell>/ltx/e<epoch>` matches [`Self::db_path`]'s remote
    /// twin so the same coordinates address local and replica state.
    fn client_for(&self, cell: &str, epoch: u64) -> ObjectStoreClient {
        let mut config = node_config(
            &self.bucket,
            self.endpoint.as_deref(),
            &self.region,
            self.credentials.as_ref(),
        );
        config.path = format!("cells/{cell}/ltx/e{epoch}");
        ObjectStoreClient::with_store(config, self.store.clone())
    }

    /// Highest epoch under `cells/<cell>/ltx/` that holds any LTX — the newest
    /// durable copy to restore on takeover. Mirrors the Litestream backend's
    /// `highest_nonempty_epoch`.
    async fn highest_nonempty_epoch(&self, cell: &str) -> anyhow::Result<Option<u64>> {
        use celld_ltx::object_store::path::Path as ObjPath;
        let base = ObjPath::from(format!("cells/{cell}/ltx"));
        let listing = self.store.list_with_delimiter(Some(&base)).await?;
        let mut best: Option<u64> = None;
        for prefix in listing.common_prefixes {
            if let Some(epoch) = prefix
                .filename()
                .and_then(|name| name.strip_prefix('e'))
                .and_then(|value| value.parse::<u64>().ok())
            {
                best = Some(best.map_or(epoch, |current| current.max(epoch)));
            }
        }
        Ok(best)
    }

    /// Does the bucket hold any LTX for this cell at this epoch? The fail-closed
    /// hibernation gate: never delete the last local copy of state the bucket
    /// cannot restore.
    pub async fn epoch_replicated(&self, cell: &str, epoch: u64) -> bool {
        let client = self.client_for(cell, epoch);
        matches!(
            replica::calc_restore_plan(&client, TXID(0)).await,
            Ok(plan) if !plan.is_empty()
        )
    }

    pub async fn activate(
        &self,
        options: ActivationOptions<'_>,
    ) -> anyhow::Result<ActivationResult> {
        let ActivationOptions {
            cell,
            epoch,
            fresh,
            took_over,
            ..
        } = options;
        let dst = self.db_path(cell, epoch);
        std::fs::create_dir_all(dst.parent().unwrap())?;

        // Reuse a preserved local hibernation snapshot when it is safe to: the
        // same epoch always, the previous epoch only when we did not take the
        // cell from another node. Mirrors the Litestream backend.
        let same_epoch = dst.with_extension("hibernated");
        let previous = celld_logic::restore::previous_epoch_reusable(epoch, took_over)
            .then(|| self.db_path(cell, epoch - 1).with_extension("hibernated"));
        let is_file = |path: &PathBuf| path.is_file();
        let local_hibernated = (!fresh)
            .then(|| {
                if is_file(&same_epoch) {
                    Some(same_epoch.clone())
                } else {
                    previous.filter(is_file)
                }
            })
            .flatten();

        let mut restored = false;
        if let Some(hibernated) = local_hibernated {
            std::fs::rename(&hibernated, &dst)?;
            info!(cell, epoch, "reused local hibernation snapshot");
            restored = true;
        } else if !fresh {
            // Restore the newest durable epoch into this epoch's path.
            if let Some(from) = self.highest_nonempty_epoch(cell).await? {
                let client = self.client_for(cell, from);
                let _ = std::fs::remove_file(&dst);
                replica::restore(&client, &dst, TXID(0))
                    .await
                    .map_err(|error| anyhow!("restore {cell} e{from}: {error}"))?;
                info!(cell, from, to = epoch, "restored remote replica");
                restored = true;
            }
        }

        // Open the managed Db (creates a fresh WAL db when nothing was restored)
        // and pair it with this epoch's client. Registration is immediate: the
        // cell can be proved durable on its very first write.
        let dst_ = dst.clone();
        let db = tokio::task::spawn_blocking(move || Db::open(&dst_))
            .await?
            .map_err(|error| anyhow!("open managed db {}: {error}", dst.display()))?;
        let replica = Replica::new(db, self.client_for(cell, epoch));
        self.cells.lock().unwrap().insert(
            (cell.to_string(), epoch),
            Arc::new(Cell {
                replica: Mutex::new(replica),
                req_seq: AtomicU64::new(0),
                synced_seq: AtomicU64::new(0),
                syncing: AtomicBool::new(false),
                ready: Notify::new(),
            }),
        );

        Ok(ActivationResult {
            path: dst,
            restored,
            replica_discovery_us: 0,
            restore_us: 0,
        })
    }

    /// The output gate's primitive: take a durability ticket and return once a
    /// background sync that captured this write has completed, coalescing
    /// concurrent writes to one cell into a single upload. The write committed
    /// before this call, so any sync starting after our ticket captures it —
    /// we wait for `synced_seq >= my ticket`, not for a position, sidestepping
    /// the total_changes↔LTX-txid mismatch that a position compare would hit.
    /// Returns `position` (which the completed sync provably covered) for the
    /// core's coverage check.
    pub async fn await_durable(
        &self,
        cell: &str,
        epoch: u64,
        position: u64,
    ) -> anyhow::Result<u64> {
        let Some(handle) = self.cells.lock().unwrap().get(&(cell.to_string(), epoch)).cloned()
        else {
            anyhow::bail!("ltx cell not resident: {cell} epoch {epoch}");
        };
        let ticket = handle.req_seq.fetch_add(1, Ordering::SeqCst) + 1;
        self.dirty.notify_one();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            // Register the waiter before checking, so a sync that completes
            // between the check and the await is not missed.
            let ready = handle.ready.notified();
            if handle.synced_seq.load(Ordering::SeqCst) >= ticket {
                return Ok(position);
            }
            if tokio::time::timeout_at(deadline, ready).await.is_err() {
                anyhow::bail!("ltx durability timed out for {cell} epoch {epoch}");
            }
        }
    }

    /// A direct, synchronous durability pass for the rare hibernation/eviction
    /// gates (not the hot write path). Also advances the cell's durable position
    /// so any output-gate waiters ride it.
    pub async fn sync_wait(&self, cell: &str, epoch: u64, _timeout: Duration) -> SyncWait {
        let Some(handle) = self.cells.lock().unwrap().get(&(cell.to_string(), epoch)).cloned()
        else {
            return SyncWait::Unsupported;
        };
        match sync_cell(handle).await {
            Some(true) => SyncWait::Durable,
            Some(false) => SyncWait::Failed,
            None => SyncWait::Unsupported,
        }
    }

    pub async fn hibernate(&self, cell: &str, epoch: u64, preserve_local: bool) {
        // A final durability pass so no acknowledged write is stranded, then
        // drop the managed Db (releasing the WAL) before touching the file.
        let _ = self.sync_wait(cell, epoch, Duration::from_secs(10)).await;
        self.cells.lock().unwrap().remove(&(cell.to_string(), epoch));
        let db = self.db_path(cell, epoch);
        if preserve_local {
            let preserved = db.with_extension("hibernated");
            if let Err(error) = std::fs::rename(&db, &preserved) {
                warn!(cell, epoch, %error, "preserve local snapshot failed");
            }
        }
        // Clear the WAL/meta siblings and the live db regardless: a reactivation
        // restores or reuses the `.hibernated` copy.
        for suffix in ["-wal", "-shm"] {
            let mut sibling = db.clone().into_os_string();
            sibling.push(suffix);
            let _ = std::fs::remove_file(PathBuf::from(sibling));
        }
        let _ = std::fs::remove_dir_all(Db::meta_path_for_path(&db));
        if !preserve_local {
            let _ = std::fs::remove_file(&db);
        }
    }

    /// Copy the live epoch into a private read-only snapshot for inspection.
    pub fn snapshot_active(
        &self,
        cell: &str,
        epoch: u64,
    ) -> anyhow::Result<Option<RestoredSnapshot>> {
        let source = self.db_path(cell, epoch);
        if !source.is_file() {
            return Ok(None);
        }
        let directory = self.watch.join(format!(".inspect-{cell}-e{epoch}"));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory)?;
        let path = directory.join("db.sqlite");
        sqlite_snapshot(&source, &path)?;
        Ok(Some(RestoredSnapshot::new(epoch, path, directory)))
    }

    /// Restore the newest durable replica into a private snapshot without
    /// claiming or activating the cell.
    pub async fn restore_snapshot(&self, cell: &str) -> anyhow::Result<Option<RestoredSnapshot>> {
        let Some(epoch) = self.highest_nonempty_epoch(cell).await? else {
            return Ok(None);
        };
        let directory = self.watch.join(format!(".restore-{cell}"));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory)?;
        let path = directory.join("db.sqlite");
        replica::restore(&self.client_for(cell, epoch), &path, TXID(0))
            .await
            .map_err(|error| anyhow!("restore snapshot {cell} e{epoch}: {error}"))?;
        Ok(Some(RestoredSnapshot::new(epoch, path, directory)))
    }

    pub fn prune_local_cache(&self, max_bytes: u64) -> (usize, usize, u64) {
        prune_watch(&self.watch, max_bytes)
    }

    /// No external process to watch: the in-process replicator is healthy as
    /// long as celld is running.
    pub fn process_status(&self) -> std::io::Result<Option<std::process::ExitStatus>> {
        Ok(None)
    }
}

/// One capture+upload for a cell: advance its durable position on success and
/// wake its waiters. Everything committed before the capture is durable once
/// uploaded, so the target is read before `db.sync`. The `rusqlite` handle is
/// `!Sync`, so the whole pass runs on a blocking thread with `block_on` for the
/// async upload. `Some(true)` on success, `Some(false)` on failure, `None` if
/// the replica lost its db.
async fn sync_cell(handle: CellHandle) -> Option<bool> {
    let runtime = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        // Tickets taken before the capture: their writes committed before
        // `db.sync` runs, so it captures them. Read before the capture so a
        // ticket taken during the sync is credited by the next one, not this.
        let captured = handle.req_seq.load(Ordering::SeqCst);
        let mut replica = handle.replica.lock().unwrap();
        let db = replica.db_mut()?;
        if let Err(error) = db.sync() {
            warn!(%error, "ltx wal capture failed");
            handle.ready.notify_waiters();
            return Some(false);
        }
        let ok = match runtime.block_on(replica.sync()) {
            Ok(()) => true,
            Err(error) => {
                warn!(%error, "ltx upload failed");
                false
            }
        };
        drop(replica);
        if ok {
            handle.synced_seq.fetch_max(captured, Ordering::SeqCst);
        }
        handle.ready.notify_waiters();
        Some(ok)
    })
    .await
    .unwrap_or(Some(false))
}

/// The node's background sync loop: wake on a dirty cell (or a slow tick) and
/// launch a sync for every cell whose committed position runs ahead of its
/// durable one. Each cell's sync is an independent, self-rescheduling task —
/// the loop does *not* wait for the batch to finish — so one slow cell's upload
/// never stalls the others (a cell keeps its own cadence up to the concurrency
/// bound). A cell's writes reported between its syncs still clear on one upload:
/// the batching win, without the cross-cell head-of-line blocking.
async fn sync_loop(
    cells: Arc<Mutex<HashMap<(String, u64), CellHandle>>>,
    dirty: Arc<Notify>,
    slots: Arc<Semaphore>,
) {
    loop {
        tokio::select! {
            _ = dirty.notified() => {}
            _ = tokio::time::sleep(Duration::from_millis(25)) => {}
        }
        let work: Vec<CellHandle> = {
            let map = cells.lock().unwrap();
            map.values()
                .filter(|c| c.req_seq.load(Ordering::SeqCst) > c.synced_seq.load(Ordering::SeqCst))
                .cloned()
                .collect()
        };
        for cell in work {
            // Claim the cell; skip if a sync is already in flight for it.
            if cell
                .syncing
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
            {
                continue;
            }
            let slots = slots.clone();
            let dirty = dirty.clone();
            tokio::spawn(async move {
                // Keep syncing this cell while it stays dirty, rather than
                // notifying the main loop to re-scan every completion — that made
                // the loop wake O(cells) times and starved throughput as cells
                // accumulated. This is not a busy loop: each iteration awaits an
                // object-store upload (~one round-trip). A *failed* sync would
                // not, so it backs off, keeping the only tight iterations the
                // ones that actually uploaded.
                loop {
                    let ok = {
                        let _permit = slots.acquire().await;
                        sync_cell(cell.clone()).await
                    };
                    if cell.req_seq.load(Ordering::SeqCst) <= cell.synced_seq.load(Ordering::SeqCst)
                    {
                        break;
                    }
                    if ok != Some(true) {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
                cell.syncing.store(false, Ordering::SeqCst);
                // A write landing in the clear window is picked up next tick;
                // nudge the loop so it does not wait the full interval.
                if cell.req_seq.load(Ordering::SeqCst) > cell.synced_seq.load(Ordering::SeqCst) {
                    dirty.notify_one();
                }
            });
        }
    }
}

/// Node-level object-store config (no per-cell prefix). `build_store` on this
/// yields the one shared client; per-cell clients set only `path`.
fn node_config(
    bucket: &str,
    endpoint: Option<&str>,
    region: &str,
    credentials: Option<&StorageCredentials>,
) -> ObjectStoreConfig {
    let endpoint = endpoint.unwrap_or_default().to_string();
    // Static credentials come from the managed control plane when present, else
    // the `AWS_*` env the node already carries for Litestream. Without this,
    // `build_store` sees empty keys and object_store falls back to the instance
    // credential provider, which off-EC2 sends unsigned requests (R2 answers
    // "404 page not found").
    let env = |key: &str| std::env::var(key).ok().filter(|value| !value.is_empty());
    let access_key_id = credentials
        .map(|c| c.access_key_id.clone())
        .filter(|value| !value.is_empty())
        .or_else(|| env("AWS_ACCESS_KEY_ID"))
        .unwrap_or_default();
    let secret_access_key = credentials
        .map(|c| c.secret_access_key.clone())
        .filter(|value| !value.is_empty())
        .or_else(|| env("AWS_SECRET_ACCESS_KEY"))
        .unwrap_or_default();
    // Temporary R2/STS credentials require the session token, or signing fails.
    let session_token = credentials
        .and_then(|c| c.session_token.clone())
        .filter(|value| !value.is_empty())
        .or_else(|| env("AWS_SESSION_TOKEN"))
        .unwrap_or_default();
    ObjectStoreConfig {
        bucket: bucket.to_string(),
        path: String::new(),
        region: region.to_string(),
        // A custom endpoint (R2/MinIO) uses path-style addressing, matching
        // `ObjectStoreConfig::from_url`'s default for non-AWS hosts.
        force_path_style: !endpoint.is_empty(),
        endpoint,
        access_key_id,
        secret_access_key,
        session_token,
        skip_verify: false,
        part_size: 0,
        concurrency: 0,
    }
}
