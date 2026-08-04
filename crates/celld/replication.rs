//! Node-level LTX replication: ONE litestream process per node in
//! directory-watch mode (litestream HEAD — fsnotify + Store.Register/
//! UnregisterDB). Each cell db lives at
//! `<watch>/<cell>/ltx/e<epoch>/db.sqlite` and replicates to
//! `cells/<cell>/ltx/e<epoch>/db.sqlite/` in the bucket — epoch-in-prefix is
//! the data-path fence: a stale owner writes a dead prefix.
//!
//! `activate` restores the highest non-empty epoch into a temp file, then
//! atomically renames it into the watched tree; litestream registers it live
//! (~250ms debounce). `hibernate` deliberately does NOT checkpoint (litestream
//! owns the WAL); it waits out the sync interval, then removes the file;
//! litestream unregisters it and frees the per-database process memory. One directory-watching process serves every cell on a node.
use aws_sdk_s3::Client;
use fs2::FileExt;
use rusqlite::Connection;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::{mpsc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tracing::debug;
use tracing::info;
use tracing::warn;

const RUNTIME_LOCK: &str = ".celld-runtime.lock";
const SUPERVISOR_ARGUMENT: &str = "__litestream-supervisor";
const DEFAULT_LITESTREAM_GOMEMLIMIT: &str = "512MiB";

pub struct NodeRepl {
    watch: PathBuf,
    /// Litestream control socket (short path: macOS caps UDS paths at 104
    /// bytes and watch dirs can be deep). Absent for daemons that never
    /// created it — `sync_wait` then reports `SyncWait::Unsupported`.
    socket: PathBuf,
    /// Replica addressing for explicit `POST /register` repair: the directory
    /// watcher is the primary registration path, but it misses databases at
    /// density, and an unregistered cell neither replicates nor passes the
    /// durability gate.
    bucket: String,
    endpoint: Option<String>,
    region: String,
    sync_unsupported_warned: AtomicBool,
    child: Mutex<Option<Child>>,
    supervisor_stdin: Option<ChildStdin>,
    _runtime_lock: File,
}

/// Outcome of a blocking replication wait on one cell db.
pub enum SyncWait {
    /// The latest local commit is in the bucket.
    Durable,
    /// The daemon has no control socket; the caller decides its fallback.
    Unsupported,
    /// Socket present but the wait failed or timed out.
    Failed,
}

pub struct RestoredSnapshot {
    pub epoch: u64,
    path: PathBuf,
    directory: PathBuf,
}

impl RestoredSnapshot {
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Construct a snapshot whose `directory` is removed on drop. Used by the
    /// in-process replicator ([`crate::ltx_repl`]) to hand back an inspection
    /// copy with the same RAII cleanup the Litestream path relies on.
    pub(crate) fn new(epoch: u64, path: PathBuf, directory: PathBuf) -> Self {
        Self {
            epoch,
            path,
            directory,
        }
    }
}

impl Drop for RestoredSnapshot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

struct LocalRecovery {
    epoch: u64,
    path: PathBuf,
    _runtime_lock: Option<File>,
}

#[derive(Clone)]
pub struct StorageCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

pub struct ActivationOptions<'a> {
    pub client: &'a Client,
    pub litestream: &'a str,
    pub bucket: &'a str,
    pub cell: &'a str,
    pub epoch: u64,
    /// The epoch-one ownership record was created conditionally by this
    /// activation. No earlier replica can exist for this cell.
    pub fresh: bool,
    /// This activation seized the cell from a DIFFERENT node. When false the
    /// ownership record still named us at `epoch - 1`, so no other process
    /// has written the cell since we hibernated it and our preserved local
    /// state is authoritative.
    pub took_over: bool,
    pub endpoint: Option<&'a str>,
    pub region: &'a str,
    pub credentials: Option<&'a StorageCredentials>,
}

pub struct ActivationResult {
    pub path: PathBuf,
    pub restored: bool,
    pub replica_discovery_us: u64,
    pub restore_us: u64,
}

fn ep_host(endpoint: &str) -> &str {
    endpoint
        .trim_start_matches("http://")
        .trim_start_matches("https://")
}

/// Restore URL for a specific epoch: the per-cell prefix a directory-watch
/// replica writes to (base `s3://<bucket>/cells` + relPath `<cell>/ltx/eN/
/// db.sqlite`).
fn storage_url(base: String, endpoint: Option<&str>, region: &str) -> String {
    match endpoint {
        Some(endpoint) => format!(
            "{base}?endpoint={}&region={region}&force-path-style=true",
            ep_host(endpoint)
        ),
        None => format!("{base}?region={region}"),
    }
}

fn restore_url(
    bucket: &str,
    cell: &str,
    epoch: u64,
    endpoint: Option<&str>,
    region: &str,
) -> String {
    storage_url(
        format!("s3://{bucket}/cells/{cell}/ltx/e{epoch}/db.sqlite"),
        endpoint,
        region,
    )
}

enum SyncIo {
    Io(std::io::Error),
    Timeout,
}

/// One `Connection: close` HTTP POST over the litestream control socket.
async fn control_post(
    socket: &std::path::Path,
    target: &str,
    body: &str,
    timeout: Duration,
) -> Result<String, SyncIo> {
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    let request = format!(
        "POST {target} HTTP/1.1\r\nHost: litestream\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n{body}",
        body.len()
    );
    let io = async {
        let mut stream = tokio::net::UnixStream::connect(socket).await?;
        stream.write_all(request.as_bytes()).await?;
        let mut response = String::new();
        stream.read_to_string(&mut response).await?;
        Ok::<String, std::io::Error>(response)
    };
    match tokio::time::timeout(timeout, io).await {
        Ok(Ok(response)) => Ok(response),
        Ok(Err(error)) => Err(SyncIo::Io(error)),
        Err(_) => Err(SyncIo::Timeout),
    }
}

/// Explicitly register one cell db with the litestream daemon. Idempotent:
/// the daemon answers 200 for both `registered` and `already_registered`.
/// Registered-by-repair databases carry litestream's default checkpoint
/// cadence rather than the tuned watcher config — a bounded divergence,
/// preferred over a cell that never replicates.
async fn register_db(
    socket: &std::path::Path,
    db: &std::path::Path,
    replica_url: &str,
    timeout: Duration,
) -> bool {
    let body = format!(
        "{{\"path\":{:?},\"replica_url\":{:?}}}",
        db.display().to_string(),
        replica_url,
    );
    match control_post(socket, "/register", &body, timeout).await {
        Ok(response) if response.starts_with("HTTP/1.1 200") => true,
        Ok(response) => {
            let status = response.lines().next().unwrap_or("").to_string();
            warn!(db = %db.display(), %status, "litestream register rejected");
            false
        }
        Err(SyncIo::Io(error)) => {
            warn!(db = %db.display(), %error, "litestream register failed");
            false
        }
        Err(SyncIo::Timeout) => {
            warn!(db = %db.display(), "litestream register timed out");
            false
        }
    }
}

/// Highest epoch prefix under `cells/<cell>/ltx/` that contains objects.
/// Fail-closed hibernation check: does the bucket hold ANY replica data for
/// this cell at this epoch? Hibernation deletes the local files; doing that
/// with an empty replica strands the cell (restore finds "no matching backup
/// files"). One LIST per eviction attempt — evictions are rare.
pub async fn epoch_replicated(c: &Client, bucket: &str, cell: &str, epoch: u64) -> bool {
    let prefix = format!("cells/{cell}/ltx/e{epoch}/");
    match c
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .max_keys(1)
        .send()
        .await
    {
        Ok(page) => !page.contents().is_empty(),
        Err(error) => {
            tracing::warn!(%error, cell, epoch, "replica check failed");
            false
        }
    }
}

async fn highest_nonempty_epoch(
    c: &Client,
    bucket: &str,
    cell: &str,
) -> anyhow::Result<Option<u64>> {
    let base = format!("cells/{cell}/ltx/");
    let mut best: Option<u64> = None;
    let mut continuation_token: Option<String> = None;
    loop {
        let mut request = c
            .list_objects_v2()
            .bucket(bucket)
            .prefix(&base)
            .delimiter("/");
        if let Some(token) = continuation_token.as_deref() {
            request = request.continuation_token(token);
        }
        let response = request.send().await?;
        for prefix in response.common_prefixes() {
            if let Some(epoch) = prefix
                .prefix()
                .and_then(|value| value.trim_end_matches('/').rsplit("/e").next())
                .and_then(|value| value.parse::<u64>().ok())
            {
                best = Some(best.map_or(epoch, |current| current.max(epoch)));
            }
        }
        if !response.is_truncated().unwrap_or(false) {
            break;
        }
        continuation_token = response.next_continuation_token().map(str::to_owned);
        if continuation_token.is_none() {
            anyhow::bail!("truncated replication listing omitted its continuation token");
        }
    }
    Ok(best)
}

/// Restore the latest completed bucket replica for read-only operator
/// inspection. This does not claim ownership, create a new epoch, place the
/// database under the watched replication tree, or activate Worker code.
pub async fn restore_snapshot(
    c: &Client,
    litestream: &str,
    bucket: &str,
    cell: &str,
    endpoint: Option<&str>,
    region: &str,
    credentials: Option<&StorageCredentials>,
) -> anyhow::Result<Option<RestoredSnapshot>> {
    use anyhow::Context;

    let Some(epoch) = highest_nonempty_epoch(c, bucket, cell).await? else {
        return Ok(None);
    };
    let directory = std::env::temp_dir().join(format!(
        "celld-inspect-{}-{:016x}",
        std::process::id(),
        rand::random::<u64>(),
    ));
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(&directory)?;
    let path = directory.join("snapshot.sqlite");
    let url = restore_url(bucket, cell, epoch, endpoint, region);
    let mut command = Command::new(litestream);
    command
        .args(["restore", "-o"])
        .arg(&path)
        .arg(&url)
        .env("AWS_REGION", region);
    apply_credentials(&mut command, credentials);
    let snapshot = RestoredSnapshot {
        epoch,
        path,
        directory,
    };
    let mut command = tokio::process::Command::from(command);
    command.kill_on_drop(true);
    let output = tokio::time::timeout(Duration::from_secs(7), command.output())
        .await
        .context("restore replicated snapshot timed out")??;
    if !output.status.success() {
        anyhow::bail!(
            "restore replicated snapshot failed: {}",
            String::from_utf8_lossy(&output.stderr).trim(),
        );
    }
    Ok(Some(snapshot))
}

impl NodeRepl {
    /// Spawn the node's single litestream process in directory-watch mode.
    /// It replicates every `*.sqlite` under `watch` and tracks membership as
    /// files come and go. Quiet levels (G0.1: zero idle bucket ops).
    pub fn start(
        litestream: &str,
        watch: &str,
        bucket: &str,
        endpoint: Option<&str>,
        region: &str,
        credentials: Option<&StorageCredentials>,
    ) -> anyhow::Result<NodeRepl> {
        let watch = PathBuf::from(watch);
        std::fs::create_dir_all(&watch)?;
        let runtime_lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(watch.join(RUNTIME_LOCK))?;
        runtime_lock.try_lock_exclusive().map_err(|error| {
            anyhow::anyhow!(
                "runtime directory {} is already in use: {error}",
                watch.display()
            )
        })?;
        let cfg = watch.join("node.litestream.yml");
        let url = storage_url(format!("s3://{bucket}/cells"), endpoint, region);
        // Short, unique control-socket path: macOS caps UDS paths at 104
        // bytes, and session watch directories can exceed that.
        let mut socket = std::env::temp_dir().join(format!("celld-ls-{}.sock", std::process::id()));
        // A long TMPDIR pushes the path past the cap, the daemon's bind
        // fails, and `sync_wait` degrades to `Unsupported` — every
        // consume-delete then proceeds ungated with one warning as the only
        // evidence. Fall back to a short absolute path rather than lose the
        // replication gate to an environment variable.
        if socket.as_os_str().len() > 100 {
            socket = PathBuf::from(format!("/tmp/celld-ls-{}.sock", std::process::id()));
        }
        let _ = std::fs::remove_file(&socket);
        // A time-based checkpoint makes every low-write database periodically
        // take a writer barrier. At high density those barriers form cohorts
        // and stall unrelated cells together. Keep checkpoints workload-shaped
        // and bounded instead: 1,000 pages is about 4 MiB per resident cell.
        std::fs::write(
            &cfg,
            format!(
                r#"l0-retention: 24h
l0-retention-check-interval: 24h
levels:
  - interval: 24h
snapshot:
  interval: 168h
socket:
  enabled: true
  path: {socket_path}
dbs:
  - dir: {}
    pattern: "*.sqlite"
    recursive: true
    watch: true
    checkpoint-interval: 0s
    min-checkpoint-page-count: 1000
    replica:
      url: {url}
      sync-interval: 1s
"#,
                watch.display(),
                socket_path = socket.display(),
            ),
        )?;
        let (child, supervisor_stdin) =
            spawn_replicator(litestream, &cfg, &watch, region, credentials)?;
        debug!(watch = %watch.display(), "node litestream watching");
        Ok(NodeRepl {
            watch,
            socket,
            bucket: bucket.to_owned(),
            endpoint: endpoint.map(str::to_owned),
            region: region.to_owned(),
            sync_unsupported_warned: std::sync::atomic::AtomicBool::new(false),
            child: Mutex::new(Some(child)),
            supervisor_stdin,
            _runtime_lock: runtime_lock,
        })
    }

    fn db_path(&self, cell: &str, epoch: u64) -> PathBuf {
        self.watch
            .join(cell)
            .join("ltx")
            .join(format!("e{epoch}"))
            .join("db.sqlite")
    }

    /// Force capture plus a blocking upload of one cell's db and return once
    /// the latest local commit is in the bucket — the v0.5.15 control
    /// socket's `POST /sync {wait:true}`. TXID order is commit order, so a
    /// call issued after a commit proves that commit durable. `Unsupported`
    /// means the daemon never created the socket (fake or old litestream);
    /// callers warn once and fall back to ungated behavior.
    ///
    /// A 404 means the watcher never registered this db: the cell has been
    /// replicating NOTHING. Repair by registering explicitly, then ask again
    /// — without this, hibernation wedges on the same unprovable cohort
    /// forever (observed latching nine of ten fleet nodes).
    pub async fn sync_wait(&self, cell: &str, epoch: u64, timeout: Duration) -> SyncWait {
        if !self.socket.exists() {
            if !self.sync_unsupported_warned.swap(true, Ordering::Relaxed) {
                warn!(
                    socket = %self.socket.display(),
                    "litestream control socket absent; sync-wait unsupported"
                );
            }
            return SyncWait::Unsupported;
        }
        let db = self.db_path(cell, epoch);
        let mut repaired = false;
        loop {
            let body = format!("{{\"path\":{:?},\"wait\":true}}", db.display().to_string());
            match control_post(&self.socket, "/sync", &body, timeout).await {
                Ok(response) if response.starts_with("HTTP/1.1 200") => {
                    return SyncWait::Durable;
                }
                Ok(response) if response.starts_with("HTTP/1.1 404") && !repaired => {
                    warn!(cell, epoch, "cell db unregistered; repairing registration");
                    let url = restore_url(
                        &self.bucket,
                        cell,
                        epoch,
                        self.endpoint.as_deref(),
                        &self.region,
                    );
                    if !register_db(&self.socket, &db, &url, timeout).await {
                        return SyncWait::Failed;
                    }
                    repaired = true;
                }
                Ok(response) => {
                    let status = response.lines().next().unwrap_or("").to_string();
                    warn!(cell, epoch, %status, "litestream sync-wait rejected");
                    return SyncWait::Failed;
                }
                Err(SyncIo::Io(error)) => {
                    warn!(cell, epoch, %error, "litestream sync-wait failed");
                    return SyncWait::Failed;
                }
                Err(SyncIo::Timeout) => {
                    warn!(cell, epoch, "litestream sync-wait timed out");
                    return SyncWait::Failed;
                }
            }
        }
    }

    /// Copy one resident database into a private, read-only inspection
    /// snapshot. SQLite's backup API includes committed WAL state without
    /// checkpointing or interfering with Litestream's ownership of the WAL.
    ///
    /// The caller supplies the epoch from the node's resident-cell inventory.
    /// A missing source means the cell crossed the hibernation boundary while
    /// the inspection request was in flight.
    pub fn snapshot_active(
        &self,
        cell: &str,
        epoch: u64,
    ) -> anyhow::Result<Option<RestoredSnapshot>> {
        let source = self.db_path(cell, epoch);
        if !source
            .symlink_metadata()
            .map(|metadata| metadata.file_type().is_file())
            .unwrap_or(false)
        {
            return Ok(None);
        }
        let directory = std::env::temp_dir().join(format!(
            "celld-inspect-{}-{:016x}",
            std::process::id(),
            rand::random::<u64>(),
        ));
        let mut builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&directory)?;
        let path = directory.join("snapshot.sqlite");
        let snapshot = RestoredSnapshot {
            epoch,
            path,
            directory,
        };
        sqlite_snapshot(&source, snapshot.path())?;
        Ok(Some(snapshot))
    }

    /// Poll the node-level replicator without blocking. A returned status is
    /// terminal: directory-watch replication is expected to live as long as
    /// celld, so the runtime must stop serving rather than silently extend its
    /// host-loss durability window.
    pub fn process_status(&self) -> std::io::Result<Option<std::process::ExitStatus>> {
        let mut child = self
            .child
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match child.as_mut() {
            Some(child) => child.try_wait(),
            None => Ok(None),
        }
    }

    /// Find the newest prior epoch left by a dead process on this host.
    ///
    /// A sibling runtime directory is eligible only while its process lock can
    /// be held exclusively. This prevents recovery from reading a database
    /// that another local celld process may still mutate. Ownership has
    /// already advanced to `epoch` before this runs, so only lower epochs are
    /// candidates.
    fn local_recovery(&self, cell: &str, epoch: u64) -> Option<LocalRecovery> {
        let root = self.watch.parent()?;
        let mut best: Option<LocalRecovery> = None;
        for entry in std::fs::read_dir(root).ok()?.flatten() {
            if !entry
                .file_type()
                .map(|file_type| file_type.is_dir())
                .unwrap_or(false)
            {
                continue;
            }
            let candidate_watch = entry.path();
            let runtime_lock = if candidate_watch == self.watch {
                None
            } else {
                let lock = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(candidate_watch.join(RUNTIME_LOCK))
                    .ok();
                let Some(lock) = lock else {
                    continue;
                };
                if lock.try_lock_exclusive().is_err() {
                    continue;
                }
                Some(lock)
            };
            let Some((candidate_epoch, path)) = newest_epoch_in(&candidate_watch, cell, epoch)
            else {
                continue;
            };
            if best
                .as_ref()
                .is_none_or(|current| candidate_epoch > current.epoch)
            {
                best = Some(LocalRecovery {
                    epoch: candidate_epoch,
                    path,
                    _runtime_lock: runtime_lock,
                });
            }
        }
        best
    }

    /// Restore committed state (highest non-empty epoch) and place the db into
    /// the watched tree so litestream begins replicating THIS epoch. Returns
    /// the db path for the caller to open. The restore lands on a temp file
    /// and is renamed in atomically, so the watcher only ever sees a complete
    /// db.
    pub async fn activate(
        &self,
        options: ActivationOptions<'_>,
    ) -> anyhow::Result<ActivationResult> {
        let ActivationOptions {
            client,
            litestream,
            bucket,
            cell,
            epoch,
            fresh,
            took_over,
            endpoint,
            region,
            credentials,
        } = options;
        let dst = self.db_path(cell, epoch);
        let tmp = dst.with_extension("restoring");
        // Sticky pressure re-wakes at the SAME epoch, so its cache sits under
        // this epoch. Ordinary idle hibernation is followed by an epoch
        // advance, so its cache sits under `epoch - 1` — and is only safe to
        // reuse when we did not take the cell from another node, which
        // proves nobody else wrote it while we were hibernated. Without the
        // previous-epoch lookup the preserved file could never be found and
        // every wake paid a full remote restore (measured: 46 sequential
        // storage round trips, 0 local reuses in 910 activations).
        let same_epoch = dst.with_extension("hibernated");
        let previous_epoch = celld_logic::restore::previous_epoch_reusable(epoch, took_over)
            .then(|| self.db_path(cell, epoch - 1).with_extension("hibernated"));
        let is_file = |path: &PathBuf| {
            path.symlink_metadata()
                .map(|metadata| metadata.file_type().is_file())
                .unwrap_or(false)
        };
        let local_hibernated = (!fresh)
            .then(|| {
                if is_file(&same_epoch) {
                    Some(same_epoch)
                } else {
                    previous_epoch.filter(is_file)
                }
            })
            .flatten();
        let discovery_started = Instant::now();
        let remote = if fresh || local_hibernated.is_some() {
            None
        } else {
            highest_nonempty_epoch(client, bucket, cell).await?
        };
        let local = if local_hibernated.is_some() {
            None
        } else {
            self.local_recovery(cell, epoch)
        };
        let replica_discovery_us = discovery_started.elapsed().as_micros() as u64;
        let litestream = litestream.to_owned();
        let bucket = bucket.to_owned();
        let cell_ = cell.to_owned();
        let cell = cell.to_owned();
        let endpoint = endpoint.map(str::to_owned);
        let region = region.to_owned();
        let credentials = credentials.cloned();
        let restore_started = Instant::now();
        let (path, restored) =
            tokio::task::spawn_blocking(move || -> anyhow::Result<(PathBuf, bool)> {
            std::fs::create_dir_all(dst.parent().unwrap())?;
            if let Some(hibernated) = local_hibernated {
                std::fs::rename(&hibernated, &dst)?;
                info!(
                    cell,
                    epoch,
                    source = %hibernated.display(),
                    "restored local hibernation snapshot + activated"
                );
                return Ok((dst, true));
            }
            if let Some(local) = local
                .as_ref()
                .filter(|local| celld_logic::restore::local_epoch_wins(local.epoch, remote))
            {
                let _ = std::fs::remove_file(&tmp);
                match sqlite_snapshot(&local.path, &tmp) {
                    Ok(()) => {
                        std::fs::rename(&tmp, &dst)?;
                        info!(
                            cell,
                            from = local.epoch,
                            to = epoch,
                            source = %local.path.display(),
                            "restored local crash epoch + activated"
                        );
                        return Ok((dst, true));
                    }
                    Err(error) if remote.is_some() => {
                        let _ = std::fs::remove_file(&tmp);
                        warn!(
                            cell,
                            from = local.epoch,
                            %error,
                            "local crash recovery failed; falling back to replicated epoch"
                        );
                    }
                    Err(error) => {
                        let _ = std::fs::remove_file(&tmp);
                        anyhow::bail!(
                            "local crash recovery failed for cell {cell} epoch {}: {error}",
                            local.epoch
                        );
                    }
                }
            }
            if let Some(src) = remote {
                let url = restore_url(&bucket, &cell, src, endpoint.as_deref(), &region);
                let mut command = Command::new(litestream);
                command
                    .args(["restore", "-o"])
                    .arg(&tmp)
                    .arg(&url)
                    .env("AWS_REGION", &region);
                apply_credentials(&mut command, credentials.as_ref());
                let out = command.output()?;
                if out.status.success() {
                    std::fs::rename(&tmp, &dst)?;
                    info!(cell, from = src, to = epoch, "restored + activated");
                    Ok((dst, true))
                } else {
                    let _ = std::fs::remove_file(&tmp);
                    anyhow::bail!(
                        "restore failed for cell {cell} epoch {src}: {}",
                        String::from_utf8_lossy(&out.stderr).trim(),
                    );
                }
            } else {
                Connection::open(&dst)?; // no prior LTX: fresh db in place
                info!(cell, epoch, "fresh cell activated");
                Ok((dst, false))
            }
        })
        .await
        .map_err(|error| anyhow::anyhow!("activation restore task failed: {error}"))??;
        // Deterministic registration backstop. The watcher usually wins the
        // race during its ~250ms debounce and keeps the tuned per-db config;
        // the delayed idempotent register closes its misses so every
        // activated cell replicates. The test override exists so the process
        // suite can prove sync_wait's own 404 repair in isolation.
        if self.socket.exists()
            && std::env::var_os("CELLD_TEST_SKIP_ACTIVATION_REGISTER").is_none()
        {
            let socket = self.socket.clone();
            let db = path.clone();
            let url = restore_url(
                &self.bucket,
                &cell_,
                epoch,
                self.endpoint.as_deref(),
                &self.region,
            );
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(2)).await;
                register_db(&socket, &db, &url, Duration::from_secs(10)).await;
            });
        }
        Ok(ActivationResult {
            path,
            restored,
            replica_discovery_us,
            restore_us: restore_started.elapsed().as_micros() as u64,
        })
    }

    /// Hibernate a cell: let litestream copy the final WAL frames, then remove
    /// the file so litestream unregisters it and releases its RSS. The caller
    /// MUST have closed its own db handle first (isolate torn down).
    ///
    /// Crucially we do NOT checkpoint: litestream owns the WAL, and an external
    /// `wal_checkpoint(TRUNCATE)` truncates frames it has not yet copied — the
    /// classic footgun that loses the last writes. Cells hibernate only after
    /// idle_evict seconds, so the periodic sync (1s) has long since captured
    /// them; the wait below is belt-and-suspenders for a fresh write.
    /// Delete least-recently-used hibernation caches until the tree fits
    /// `max_bytes`. The cache is pure optimization — every entry duplicates
    /// state the bucket already holds — so eviction is always safe and its
    /// only cost is a future remote restore. Returns (kept, evicted, bytes
    /// remaining) for the caller to log.
    pub fn prune_local_cache(&self, max_bytes: u64) -> (usize, usize, u64) {
        prune_watch(&self.watch, max_bytes)
    }

    pub async fn hibernate(&self, cell: &str, epoch: u64, preserve_local: bool) {
        let db = self.db_path(cell, epoch);
        tokio::time::sleep(Duration::from_millis(1500)).await; // > sync-interval
        let mut local_cache = false;
        if preserve_local {
            // Sticky pressure eviction is a local residency-cache operation.
            // Preserve committed WAL state in a standalone SQLite snapshot
            // whose extension does not match Litestream's watched *.sqlite
            // pattern. A same-process wake can atomically move it back into
            // the watched tree without consulting R2; node loss still falls
            // back to the replica authority.
            let snapshot_db = db.clone();
            match tokio::task::spawn_blocking(move || {
                let cache = snapshot_db.with_extension("hibernated");
                let cache_tmp = snapshot_db.with_extension("hibernating");
                let _ = std::fs::remove_file(&cache_tmp);
                let _ = std::fs::remove_file(&cache);
                sqlite_snapshot(&snapshot_db, &cache_tmp)
                    .and_then(|()| {
                        std::fs::rename(&cache_tmp, &cache)?;
                        Ok(())
                    })
                    .inspect_err(|_| {
                        let _ = std::fs::remove_file(&cache_tmp);
                    })
            })
            .await
            {
                Ok(Ok(())) => local_cache = true,
                Ok(Err(error)) => {
                    warn!(%error, cell, epoch, "could not preserve local hibernation snapshot");
                }
                Err(error) => {
                    warn!(%error, cell, epoch, "local hibernation snapshot task failed");
                }
            }
        }
        for suf in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suf}", db.display()));
        }
        info!(
            cell,
            epoch, local_cache, "hibernated (replication released)"
        );
    }
}

fn newest_epoch_in(watch: &std::path::Path, cell: &str, before: u64) -> Option<(u64, PathBuf)> {
    let epochs = watch.join(cell).join("ltx");
    let mut best: Option<(u64, PathBuf)> = None;
    for entry in std::fs::read_dir(epochs).ok()?.flatten() {
        if !entry
            .file_type()
            .map(|file_type| file_type.is_dir())
            .unwrap_or(false)
        {
            continue;
        }
        let name = entry.file_name();
        let Some(epoch) = name
            .to_str()
            .and_then(|name| name.strip_prefix('e'))
            .and_then(|epoch| epoch.parse::<u64>().ok())
        else {
            continue;
        };
        if !celld_logic::restore::recoverable(epoch, before) {
            continue;
        }
        let path = entry.path().join("db.sqlite");
        if !path
            .symlink_metadata()
            .map(|metadata| metadata.file_type().is_file())
            .unwrap_or(false)
        {
            continue;
        }
        if best.as_ref().is_none_or(|(current, _)| epoch > *current) {
            best = Some((epoch, path));
        }
    }
    best
}

/// Enforce a byte ceiling over the `.hibernated` snapshots under `watch`,
/// evicting least-recently-used first. Shared by both replicators; the walk is
/// layout-independent (the `.hibernated` convention is common to both).
pub(crate) fn prune_watch(watch: &std::path::Path, max_bytes: u64) -> (usize, usize, u64) {
    use celld_logic::cache::CacheEntry;
    let mut paths = Vec::new();
    let mut entries = Vec::new();
    let mut stack = vec![watch.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(read) = std::fs::read_dir(&dir) else {
            continue;
        };
        for item in read.flatten() {
            let path = item.path();
            let Ok(meta) = item.metadata() else { continue };
            if meta.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "hibernated") {
                let last_used_ms = meta
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0);
                entries.push(CacheEntry {
                    last_used_ms,
                    bytes: meta.len(),
                });
                paths.push(path);
            }
        }
    }
    let evict = celld_logic::cache::plan_eviction(&entries, max_bytes);
    for &index in &evict {
        let _ = std::fs::remove_file(&paths[index]);
    }
    let remaining: u64 = entries
        .iter()
        .enumerate()
        .filter(|(index, _)| !evict.contains(index))
        .map(|(_, entry)| entry.bytes)
        .sum();
    (entries.len() - evict.len(), evict.len(), remaining)
}

pub(crate) fn sqlite_snapshot(
    source: &std::path::Path,
    destination: &std::path::Path,
) -> anyhow::Result<()> {
    {
        let source =
            Connection::open_with_flags(source, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        let mut destination = Connection::open(destination)?;
        let backup = rusqlite::backup::Backup::new(&source, &mut destination)?;
        backup.run_to_completion(64, Duration::from_millis(5), None)?;
    }
    Ok(())
}

fn apply_credentials(command: &mut Command, credentials: Option<&StorageCredentials>) {
    let Some(credentials) = credentials else {
        return;
    };
    command
        .env("AWS_ACCESS_KEY_ID", &credentials.access_key_id)
        .env("AWS_SECRET_ACCESS_KEY", &credentials.secret_access_key);
    if let Some(session_token) = credentials.session_token.as_deref() {
        command.env("AWS_SESSION_TOKEN", session_token);
    } else {
        command.env_remove("AWS_SESSION_TOKEN");
    }
}

fn apply_litestream_memory_limit(command: &mut Command) {
    command.env(
        "GOMEMLIMIT",
        std::env::var_os("GOMEMLIMIT").unwrap_or_else(|| DEFAULT_LITESTREAM_GOMEMLIMIT.into()),
    );
}

fn pipe_litestream_logs(command: &mut Command) {
    // Litestream emits operational detail for every database sync. A dense
    // node can produce thousands of lines per second, so never let the child
    // inherit the service's journal directly: journal backpressure can stall
    // Litestream while it owns a SQLite checkpoint lock.
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
}

fn forward_litestream_logs(child: &mut Child) {
    for pipe in [
        child
            .stdout
            .take()
            .map(|out| Box::new(out) as Box<dyn std::io::Read + Send>),
        child
            .stderr
            .take()
            .map(|err| Box::new(err) as Box<dyn std::io::Read + Send>),
    ]
    .into_iter()
    .flatten()
    {
        std::thread::spawn(move || {
            use std::io::BufRead;
            for line in std::io::BufReader::new(pipe).lines().map_while(Result::ok) {
                debug!(target: "litestream", "{line}");
            }
        });
    }
}

#[cfg(target_os = "linux")]
fn spawn_replicator(
    litestream: &str,
    cfg: &std::path::Path,
    _watch: &std::path::Path,
    region: &str,
    credentials: Option<&StorageCredentials>,
) -> anyhow::Result<(Child, Option<ChildStdin>)> {
    use anyhow::Context;
    use std::os::unix::process::CommandExt;

    let parent = unsafe { libc::getpid() };
    let mut command = Command::new(litestream);
    command
        .args(["replicate", "-config"])
        .arg(cfg)
        .env("AWS_REGION", region);
    pipe_litestream_logs(&mut command);
    apply_credentials(&mut command, credentials);
    apply_litestream_memory_limit(&mut command);
    // SAFETY: this closure runs after fork and before exec. It invokes only
    // libc syscalls and constructs an io::Error on the immediate failure path.
    unsafe {
        command.pre_exec(move || {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            // Close the fork-to-prctl race: if celld died before the kernel
            // installed the signal, terminate before exec instead.
            if libc::getppid() != parent {
                libc::raise(libc::SIGKILL);
            }
            Ok(())
        });
    }
    let mut child = command.spawn().context(
        "litestream failed to start; install litestream \
         (https://litestream.io) or set LITESTREAM_BIN",
    )?;
    forward_litestream_logs(&mut child);
    Ok((child, None))
}

#[cfg(not(target_os = "linux"))]
fn spawn_replicator(
    litestream: &str,
    cfg: &std::path::Path,
    watch: &std::path::Path,
    region: &str,
    credentials: Option<&StorageCredentials>,
) -> anyhow::Result<(Child, Option<ChildStdin>)> {
    let supervisor_ready = watch.join(".litestream-supervisor-ready");
    let _ = std::fs::remove_file(&supervisor_ready);
    let mut command = Command::new(std::env::current_exe()?);
    command
        .arg(SUPERVISOR_ARGUMENT)
        .arg(&supervisor_ready)
        .arg(litestream)
        .args(["replicate", "-config"])
        .arg(cfg)
        .env("AWS_REGION", region)
        .stdin(Stdio::piped());
    pipe_litestream_logs(&mut command);
    apply_credentials(&mut command, credentials);
    apply_litestream_memory_limit(&mut command);
    use anyhow::Context;
    let mut child = command.spawn().context("litestream failed to start; install litestream (https://litestream.io) or set LITESTREAM_BIN")?;
    let supervisor_stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow::anyhow!("litestream supervisor stdin was not piped"))?;
    forward_litestream_logs(&mut child);
    let ready_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if supervisor_ready.is_file() {
            std::fs::remove_file(&supervisor_ready)?;
            break;
        }
        if let Some(status) = child.try_wait()? {
            anyhow::bail!("litestream supervisor exited before startup completed: {status}");
        }
        if Instant::now() >= ready_deadline {
            drop(supervisor_stdin);
            let _ = child.wait();
            anyhow::bail!("litestream supervisor did not report startup within 5s");
        }
        thread::sleep(Duration::from_millis(10));
    }
    Ok((child, Some(supervisor_stdin)))
}

impl Drop for NodeRepl {
    fn drop(&mut self) {
        // On platforms without a parent-death signal, closing the keepalive
        // pipe asks the supervisor to kill and reap Litestream. Linux launches
        // Litestream directly with PR_SET_PDEATHSIG instead.
        let supervised = self.supervisor_stdin.take().is_some();
        let child = self
            .child
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(mut ch) = child.take() {
            if !supervised {
                let _ = ch.kill();
            }
            let _ = ch.wait();
        }
    }
}

pub fn is_litestream_supervisor_invocation() -> bool {
    std::env::args().nth(1).as_deref() == Some(SUPERVISOR_ARGUMENT)
}

pub fn run_litestream_supervisor() -> anyhow::Result<()> {
    let mut arguments = std::env::args_os().skip(2);
    let ready_path = arguments
        .next()
        .ok_or_else(|| anyhow::anyhow!("litestream supervisor omitted the readiness path"))?;
    let litestream = arguments
        .next()
        .ok_or_else(|| anyhow::anyhow!("litestream supervisor omitted the executable"))?;
    use anyhow::Context;
    let mut child = Command::new(litestream)
        .args(arguments)
        .stdin(Stdio::null())
        .spawn()
        .context("litestream failed to start; install litestream (https://litestream.io) or set LITESTREAM_BIN")?;
    if let Err(error) = std::fs::write(&ready_path, b"ready\n") {
        let _ = child.kill();
        let _ = child.wait();
        return Err(error.into());
    }

    let (keepalive_closed_tx, keepalive_closed_rx) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let mut stdin = std::io::stdin();
        let mut buffer = [0_u8; 64];
        loop {
            match stdin.read(&mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
        let _ = keepalive_closed_tx.send(());
    });

    loop {
        if let Some(status) = child.try_wait()? {
            if status.success() {
                return Ok(());
            }
            anyhow::bail!("litestream exited with {status}");
        }
        match keepalive_closed_rx.try_recv() {
            Ok(()) | Err(mpsc::TryRecvError::Disconnected) => {
                let _ = child.kill();
                let _ = child.wait();
                return Ok(());
            }
            Err(mpsc::TryRecvError::Empty) => {}
        }
        thread::sleep(Duration::from_millis(25));
    }
}
