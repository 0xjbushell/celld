// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! V8 runtime materialization behind core-authorized lifecycle effects.
//!
//! The manager owns handles and filesystem paths, never lifecycle policy.
//! StartRuntime, Publish, and StopRuntime decide when a cell handle moves from
//! starting to externally dispatchable to closed.

use crate::asyncrt;
use crate::js::{
    self, CellJob, FetchRequest, HttpResponse, Worker, WorkerConfig, WorkerConfigOptions,
};
use crate::ltx_repl::LtxRepl;
use crate::replication::{ActivationOptions, StorageCredentials, SyncWait};
use crate::storage;
use crate::wake::WakeFlusher;
use anyhow::{anyhow, Context};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{mpsc, Arc, Mutex, Once};
use std::time::{Duration, Instant};

const REMOTE_ABORT_TTL: Duration = Duration::from_secs(600);

fn prune_remote_aborts(aborts: &mut HashMap<js::RequestId, Instant>) {
    aborts.retain(|_, created| created.elapsed() < REMOTE_ABORT_TTL);
}

#[derive(Clone)]
struct StatelessRuntime {
    pool: Arc<crate::WorkerPool>,
}

struct CellHandle {
    epoch: u64,
    startup_us: u64,
    tx: mpsc::Sender<CellJob>,
    stopped: tokio::sync::oneshot::Receiver<()>,
    next_alarm_ms: Arc<AtomicI64>,
}

pub type AlarmObserver = Arc<dyn Fn(String, Option<i64>) + Send + Sync>;

#[derive(Default)]
struct CellRegistry {
    starting: HashMap<String, CellHandle>,
    published: HashMap<String, CellHandle>,
}

#[derive(Clone)]
pub struct RuntimeManager {
    stateless: StatelessRuntime,
    services: Arc<HashMap<String, StatelessRuntime>>,
    cell_configs: Arc<HashMap<String, Arc<WorkerConfig>>>,
    cells: Arc<Mutex<CellRegistry>>,
    /// A peer abort can arrive before the forwarded fetch. The tombstone and
    /// cell enqueue share this lock so neither ordering can lose cancellation.
    remote_aborts: Arc<Mutex<HashMap<js::RequestId, Instant>>>,
    data_dir: Arc<PathBuf>,
    default_do_class: Option<Arc<str>>,
    replication: Option<Replication>,
    wake: Option<Arc<WakeFlusher>>,
    alarm_observer: AlarmObserver,
    node: Arc<str>,
    region: Arc<str>,
}

pub struct CohostedWorker {
    pub options: WorkerConfigOptions,
    pub services: Vec<(String, String, Option<String>)>,
    pub asset_binding: Option<String>,
    pub workers: usize,
}

pub struct RuntimeOptions {
    pub worker: WorkerConfigOptions,
    pub services: Vec<(String, String, Option<String>)>,
    pub asset_binding: Option<String>,
    pub loader_binding: Option<String>,
    pub cohosted: Vec<CohostedWorker>,
    pub workers: usize,
    pub data_dir: PathBuf,
    pub replication: Option<Replication>,
    pub wake: Option<Arc<WakeFlusher>>,
    pub alarm_observer: AlarmObserver,
    pub node: String,
    pub region: String,
}

/// Owned HTTP request crossing from the async shell into a V8 executor.
pub struct RuntimeFetch {
    pub url: String,
    pub method: String,
    pub body: Vec<u8>,
    pub headers: Vec<(String, String)>,
    pub request_id: Option<js::RequestId>,
}

/// The node's replication engine: the in-process `celld-ltx` replicator,
/// hidden behind this wrapper so nothing else touches the backend directly.
#[derive(Clone)]
pub struct Replication {
    ltx: Arc<LtxRepl>,
}

impl Replication {
    pub fn start(
        bucket: crate::bucket::Bucket,
        watch: &Path,
        endpoint: Option<String>,
        region: String,
        credentials: Option<StorageCredentials>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            ltx: Arc::new(LtxRepl::start(
                watch,
                bucket.name,
                endpoint,
                region,
                credentials,
            )?),
        })
    }

    async fn restore(
        &self,
        cell: &str,
        spec: &celld_logic::RestoreSpec,
    ) -> anyhow::Result<(PathBuf, bool)> {
        let options = ActivationOptions {
            cell,
            epoch: spec.epoch,
            fresh: spec.fresh,
            took_over: spec.took_over,
        };
        let activated = self.ltx.activate(options).await?;
        Ok((activated.path, activated.restored))
    }

    /// Drive/observe this cell's durability, the primitive shared by the two
    /// durability gates and the refusal check.
    async fn sync_wait(&self, cell: &str, epoch: u64) -> SyncWait {
        self.ltx
            .sync_wait(cell, epoch, Duration::from_secs(10))
            .await
    }

    /// True when the replicator actively refused to prove durability (as opposed
    /// to a cell it does not track). Drives the consumed-alarm reconciliation.
    pub async fn sync_refused(&self, cell: &str, epoch: u64) -> bool {
        matches!(self.sync_wait(cell, epoch).await, SyncWait::Failed)
    }

    pub fn process_status(&self) -> std::io::Result<Option<std::process::ExitStatus>> {
        self.ltx.process_status()
    }

    /// Enforce the byte ceiling on preserved hibernation snapshots.
    ///
    /// The directory walk is synchronous, so callers must run this on a
    /// blocking executor rather than the runtime's serving thread.
    pub fn prune_local_cache(&self, max_bytes: u64) -> (usize, usize, u64) {
        self.ltx.prune_local_cache(max_bytes)
    }

    /// Copy the exact published epoch into a private read-only snapshot.
    pub fn snapshot_active(
        &self,
        cell: &str,
        epoch: u64,
    ) -> anyhow::Result<Option<crate::replication::RestoredSnapshot>> {
        self.ltx.snapshot_active(cell, epoch)
    }

    /// Restore the newest completed replica without claiming or activating it.
    pub async fn restore_snapshot(
        &self,
        cell: &str,
    ) -> anyhow::Result<Option<crate::replication::RestoredSnapshot>> {
        self.ltx.restore_snapshot(cell).await
    }

    async fn ensure_durable(&self, cell: &str, epoch: u64) -> anyhow::Result<()> {
        match self.sync_wait(cell, epoch).await {
            SyncWait::Durable => {}
            SyncWait::Unsupported | SyncWait::Failed => {
                return Err(anyhow!(
                    "replica durability could not be proved for {cell} epoch {epoch}"
                ))
            }
        }
        // Then ask the bucket, because these are not the same question.
        // `sync_wait` asks the replicator about a path it must have registered;
        // registration is not guaranteed to cover every published cell.
        // Hibernation deletes the only local copy, so the last thing checked
        // before it goes has to be the artifact itself rather than a report.
        let replicated = self.ltx.epoch_replicated(cell, epoch).await;
        if !replicated {
            return Err(anyhow!(
                "no replica objects for {cell} epoch {epoch}; refusing to \
                 hibernate state the bucket cannot restore"
            ));
        }
        Ok(())
    }

    /// The output-gate durability wait: return the committed-write position the
    /// replica has proved durable, at least covering `position`. The replicator
    /// batches concurrent writes to one cell behind a background sync and
    /// reports the real durable position.
    async fn await_durable(&self, cell: &str, epoch: u64, position: u64) -> anyhow::Result<u64> {
        self.ltx.await_durable(cell, epoch, position).await
    }

    async fn hibernate(&self, cell: &str, epoch: u64, preserve_local: bool) {
        self.ltx.hibernate(cell, epoch, preserve_local).await
    }
}

impl RuntimeManager {
    pub fn node(&self) -> &str {
        &self.node
    }

    pub fn region(&self) -> &str {
        &self.region
    }

    /// A deployment with no Durable Object classes can never land a Worker fetch
    /// on a cell, so the core's round-robin routing always returns `None`. Lets
    /// the request path skip the core round-trip entirely for stateless workers.
    pub fn has_cell_classes(&self) -> bool {
        !self.cell_configs.is_empty()
    }

    pub fn start(options: RuntimeOptions) -> anyhow::Result<Self> {
        let RuntimeOptions {
            worker,
            services,
            asset_binding,
            loader_binding,
            cohosted,
            workers,
            data_dir,
            replication,
            wake,
            alarm_observer,
            node,
            region,
        } = options;
        if workers == 0 {
            return Err(anyhow!("stateless runtime requires at least one worker"));
        }
        static V8_INIT: Once = Once::new();
        V8_INIT.call_once(js::Engine::init);

        let node: Arc<str> = Arc::from(node);
        let region: Arc<str> = Arc::from(region);
        let primary_script = worker.script_name.clone();
        let primary_classes = worker.do_classes.clone();
        let default_do_class =
            (worker.do_classes.len() == 1).then(|| Arc::from(worker.do_classes[0].as_str()));
        let config = Arc::new(
            WorkerConfig::new(worker)
                .with_services(services)
                .with_asset_binding(asset_binding)
                .with_loader(loader_binding),
        );
        let stateless =
            StatelessRuntime::start(config.clone(), workers, node.clone(), region.clone())?;
        let mut service_pools = HashMap::from([(primary_script, stateless.clone())]);
        let mut cell_configs = HashMap::new();
        for class in primary_classes {
            if cell_configs.insert(class.clone(), config.clone()).is_some() {
                return Err(anyhow!("duplicate Durable Object class {class}"));
            }
        }
        for target in cohosted {
            let script = target.options.script_name.clone();
            let target_classes = target.options.do_classes.clone();
            let config = Arc::new(
                WorkerConfig::new(target.options)
                    .with_services(target.services)
                    .with_asset_binding(target.asset_binding),
            );
            let pool = StatelessRuntime::start(
                config.clone(),
                target.workers,
                node.clone(),
                region.clone(),
            )?;
            if service_pools.insert(script.clone(), pool).is_some() {
                return Err(anyhow!("duplicate co-hosted Worker script {script}"));
            }
            for class in target_classes {
                if cell_configs.insert(class.clone(), config.clone()).is_some() {
                    return Err(anyhow!(
                        "Durable Object class {class} is exported by more than one co-hosted script"
                    ));
                }
            }
        }
        Ok(Self {
            stateless,
            services: Arc::new(service_pools),
            cell_configs: Arc::new(cell_configs),
            cells: Arc::new(Mutex::new(CellRegistry::default())),
            remote_aborts: Arc::new(Mutex::new(HashMap::new())),
            data_dir: Arc::new(data_dir),
            default_do_class,
            replication,
            wake,
            alarm_observer,
            node,
            region,
        })
    }

    pub fn cell_scope(&self, id: &str) -> anyhow::Result<String> {
        if id.contains(':') {
            return Ok(id.to_string());
        }
        let class = self.default_do_class.as_deref().ok_or_else(|| {
            anyhow!("a bare cell id requires exactly one configured Durable Object class")
        })?;
        Ok(format!("{class}:{id}"))
    }

    pub async fn fetch_worker(
        &self,
        url: String,
        method: String,
        body: Vec<u8>,
        headers: Vec<(String, String)>,
    ) -> anyhow::Result<HttpResponse> {
        self.stateless.fetch(url, method, body, headers, None).await
    }

    /// Dispatch a cancellable top-level Worker request to the stateless pool.
    pub async fn fetch_worker_pool(
        &self,
        url: String,
        method: String,
        body: Vec<u8>,
        headers: Vec<(String, String)>,
        request_id: js::RequestId,
    ) -> anyhow::Result<HttpResponse> {
        self.stateless
            .fetch(url, method, body, headers, Some(request_id))
            .await
    }

    /// Dispatch a top-level Worker request on the exact resident runtime the
    /// decision core reserved. The activity token pins that lifecycle choice
    /// until the queued event has completely left the isolate loop.
    pub async fn fetch_worker_on_cell(
        &self,
        cell: String,
        epoch: u64,
        request: RuntimeFetch,
        inline_activity: crate::CellActivityGuard,
    ) -> anyhow::Result<HttpResponse> {
        let RuntimeFetch {
            url,
            method,
            body,
            headers,
            request_id,
        } = request;
        let request_id = request_id.context("resident Worker fetch requires a request id")?;
        let tx = self
            .cells
            .lock()
            .expect("cell registry poisoned")
            .published
            .get(&cell)
            .filter(|handle| handle.epoch == epoch)
            .map(|handle| handle.tx.clone())
            .ok_or_else(|| anyhow!("cell runtime is not published at epoch {epoch}: {cell}"))?;
        let (reply, receive) = tokio::sync::oneshot::channel();
        tx.send(CellJob::WorkerFetch {
            request_id,
            url,
            method,
            body,
            headers,
            inline_activity,
            fallback_workers: self.stateless.pool.clone(),
            reply,
        })
        .map_err(|_| anyhow!("cell isolate stopped"))?;
        receive
            .await
            .context("cell isolate dropped Worker response")?
    }

    pub async fn fetch_service(
        &self,
        script: &str,
        url: String,
        method: String,
        body: Vec<u8>,
        headers: Vec<(String, String)>,
        cancel: Option<tokio::sync::oneshot::Receiver<()>>,
    ) -> anyhow::Result<HttpResponse> {
        let pool = self
            .services
            .get(script)
            .cloned()
            .ok_or_else(|| anyhow!("no service Worker for script {script}"))?;
        let request_id = js::next_request_id();
        let response = pool.fetch(url, method, body, headers, Some(request_id));
        match cancel {
            Some(mut cancel) => tokio::select! {
                response = response => response,
                _ = &mut cancel => {
                    js::abort_request(request_id);
                    Err(anyhow!("service-binding caller disconnected"))
                }
            },
            None => response.await,
        }
    }

    pub async fn rpc_service(
        &self,
        script: &str,
        entrypoint: String,
        method: String,
        args: Vec<u8>,
    ) -> anyhow::Result<Vec<u8>> {
        self.services
            .get(script)
            .cloned()
            .ok_or_else(|| anyhow!("no service Worker for script {script}"))?
            .rpc(entrypoint, method, args)
            .await
    }

    pub async fn restore_cell(
        &self,
        cell: &str,
        spec: &celld_logic::RestoreSpec,
    ) -> anyhow::Result<celld_logic::RestoreOutcome> {
        let path = self.db_path(cell, spec.epoch);
        if let Some(replication) = &self.replication {
            let (restored_path, restored) = replication.restore(cell, spec).await?;
            if restored_path != path {
                return Err(anyhow!(
                    "replication restored {} instead of {}",
                    restored_path.display(),
                    path.display()
                ));
            }
            return Ok(celld_logic::RestoreOutcome {
                restored,
                alarm: self.restored_alarm(cell, &path),
            });
        }
        let parent = path.parent().context("cell database has no parent")?;
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create cell data directory {}", parent.display()))?;
        Ok(celld_logic::RestoreOutcome {
            restored: false,
            alarm: self.restored_alarm(cell, &path),
        })
    }

    /// The alarm the restored database already had armed, read directly by
    /// path. Read-only, and the connection is dropped here -- the isolate
    /// opens the same file moments later through `spawn_cell`.
    fn restored_alarm(
        &self,
        cell: &str,
        path: &std::path::Path,
    ) -> Option<celld_logic::RestoredAlarm> {
        let (at_ms, ..) = crate::storage::persisted_alarm(&path.to_string_lossy(), cell)?;
        // The entry this alarm already has in the bucket was written by
        // whoever armed it, which is not this process once the cell has
        // hibernated. Claim it now, while the alarm is in hand.
        crate::js::adopt_wake_entry(cell, at_ms);
        (at_ms >= 0).then(|| celld_logic::RestoredAlarm {
            at_ms,
            covered: self.alarm_covered(cell, Some(at_ms)),
        })
    }

    pub fn replication(&self) -> Option<Replication> {
        self.replication.clone()
    }

    pub fn replication_status(&self) -> std::io::Result<Option<std::process::ExitStatus>> {
        match &self.replication {
            Some(replication) => replication.process_status(),
            None => Ok(None),
        }
    }

    /// Did the replicator refuse to prove this commit durable? Distinct from
    /// `ensure_durable`: an absent control socket is not a refusal here, it is
    /// the historical ungated behaviour.
    pub async fn sync_refused(&self, cell: &str, epoch: u64) -> bool {
        match &self.replication {
            Some(replication) => replication.sync_refused(cell, epoch).await,
            None => false,
        }
    }

    pub async fn ensure_durable(&self, cell: &str, epoch: u64) -> anyhow::Result<()> {
        match &self.replication {
            Some(replication) => replication.ensure_durable(cell, epoch).await,
            None => Ok(()),
        }
    }

    /// The output-gate durability wait (see `Replication::await_durable`).
    /// Returns the proved durable position; with no replicator every position is
    /// trivially durable.
    pub async fn await_durable(
        &self,
        cell: &str,
        epoch: u64,
        position: u64,
    ) -> anyhow::Result<u64> {
        match &self.replication {
            Some(replication) => replication.await_durable(cell, epoch, position).await,
            None => Ok(position),
        }
    }

    /// Materialize an isolate and retain it as non-routable until publication.
    pub async fn start_cell(&self, cell: String, epoch: u64, fresh: bool) -> anyhow::Result<()> {
        let db_path = self.db_path(&cell, epoch);
        let (tx, rx) = mpsc::channel::<CellJob>();
        let (startup_tx, startup_rx) = tokio::sync::oneshot::channel();
        let (stopped_tx, stopped_rx) = tokio::sync::oneshot::channel();
        let class = cell
            .split_once(':')
            .map(|(class, _)| class)
            .ok_or_else(|| anyhow!("cell scope has no class: {cell}"))?;
        let config = self
            .cell_configs
            .get(class)
            .cloned()
            .ok_or_else(|| anyhow!("no Worker exports Durable Object class {class}"))?;
        let thread_tx = tx.clone();
        let next_alarm_ms = Arc::new(AtomicI64::new(-1));
        let thread_alarm = next_alarm_ms.clone();
        let alarm_observer = self.alarm_observer.clone();
        let startup_timing = CellIsolateStartupTiming {
            started: Instant::now(),
            scope: cell.clone(),
            node: self.node.clone(),
            region: self.region.clone(),
            epoch,
            fresh,
        };
        std::thread::Builder::new()
            .name(format!("celld-cell-{cell}"))
            .spawn(move || {
                run_cell(
                    db_path,
                    config,
                    rx,
                    startup_tx,
                    thread_alarm,
                    alarm_observer,
                    startup_timing,
                );
                let _ = stopped_tx.send(());
            })
            .with_context(|| format!("spawn cell isolate {cell}"))?;

        {
            let mut cells = self.cells.lock().expect("cell registry poisoned");
            if cells.starting.contains_key(&cell) || cells.published.contains_key(&cell) {
                let _ = thread_tx.send(CellJob::Shutdown);
                return Err(anyhow!("cell runtime already exists: {cell}"));
            }
            cells.starting.insert(
                cell.clone(),
                CellHandle {
                    epoch,
                    startup_us: 0,
                    tx: thread_tx,
                    stopped: stopped_rx,
                    next_alarm_ms,
                },
            );
        }

        let result = match startup_rx.await {
            Ok(result) => result,
            Err(error) => {
                self.remove_starting(&cell, epoch);
                return Err(error).context("cell isolate exited during startup");
            }
        };
        let startup_us = match result {
            Ok(startup_us) => startup_us,
            Err(error) => {
                self.remove_starting(&cell, epoch);
                return Err(error);
            }
        };
        let mut cells = self.cells.lock().expect("cell registry poisoned");
        let handle = cells
            .starting
            .get_mut(&cell)
            .filter(|handle| handle.epoch == epoch)
            .ok_or_else(|| anyhow!("started cell runtime disappeared: {cell} epoch {epoch}"))?;
        handle.startup_us = startup_us;
        Ok(())
    }

    /// Make the exact started generation visible to request dispatch.
    pub fn publish_cell(&self, cell: &str, epoch: u64) -> anyhow::Result<()> {
        let mut cells = self.cells.lock().expect("cell registry poisoned");
        if !cells
            .starting
            .get(cell)
            .is_some_and(|handle| handle.epoch == epoch)
        {
            return Err(anyhow!("no started cell runtime for {cell} epoch {epoch}"));
        }
        let handle = cells
            .starting
            .remove(cell)
            .expect("checked started runtime");
        let startup_us = handle.startup_us;
        if let Some(replaced) = cells.published.insert(cell.to_string(), handle) {
            let _ = replaced.tx.send(CellJob::Shutdown);
            return Err(anyhow!("replaced published cell runtime for {cell}"));
        }
        drop(cells);
        tracing::info!(
            event = "cell_runtime_publication",
            outcome = "published",
            scope = %cell,
            node = %self.node,
            region = %self.region,
            runtime_version = env!("CARGO_PKG_VERSION"),
            epoch,
            isolate_startup_us = startup_us,
            "cell runtime published"
        );
        Ok(())
    }

    pub async fn stop_cell(&self, cell: &str, epoch: u64, hibernate: bool, preserve_local: bool) {
        let mut stopped = Vec::new();
        {
            let mut cells = self.cells.lock().expect("cell registry poisoned");
            if cells
                .starting
                .get(cell)
                .is_some_and(|handle| handle.epoch == epoch)
            {
                if let Some(handle) = cells.starting.remove(cell) {
                    stopped.push(handle);
                }
            }
            if cells
                .published
                .get(cell)
                .is_some_and(|handle| handle.epoch == epoch)
            {
                if let Some(handle) = cells.published.remove(cell) {
                    stopped.push(handle);
                }
            }
        }
        let stopped_runtime = !stopped.is_empty();
        for handle in stopped {
            let _ = handle.tx.send(CellJob::Shutdown);
            let _ = handle.stopped.await;
        }
        if stopped_runtime && hibernate {
            if let Some(replication) = &self.replication {
                replication.hibernate(cell, epoch, preserve_local).await;
            }
        }
    }

    pub async fn fetch_cell(
        &self,
        cell: String,
        name: Option<String>,
        request: RuntimeFetch,
        cancel: Option<tokio::sync::oneshot::Receiver<()>>,
    ) -> anyhow::Result<HttpResponse> {
        let RuntimeFetch {
            url,
            method,
            body,
            headers,
            request_id,
        } = request;
        let tx = self
            .cells
            .lock()
            .expect("cell registry poisoned")
            .published
            .get(&cell)
            .map(|handle| handle.tx.clone())
            .ok_or_else(|| anyhow!("cell runtime is not published: {cell}"))?;
        let (reply, mut receive) = tokio::sync::oneshot::channel();
        let scope = cell.clone();
        let send = || {
            tx.send(CellJob::Fetch {
                request_id,
                scope: cell,
                name,
                url,
                method,
                body,
                headers,
                reply,
            })
            .map_err(|_| anyhow!("cell isolate stopped"))
        };
        if let Some(request_id) = request_id {
            let mut aborts = self.remote_aborts.lock().expect("abort registry poisoned");
            prune_remote_aborts(&mut aborts);
            if aborts.remove(&request_id).is_some() {
                return Err(anyhow!("the client disconnected before dispatch"));
            }
            send()?;
        } else {
            send()?;
        }
        let result = match (request_id, cancel) {
            (Some(request_id), Some(mut cancel)) => tokio::select! {
                result = &mut receive => result,
                cancelled = &mut cancel => {
                    if cancelled.is_ok() {
                        let _ = tx.send(CellJob::AbortFetch { request_id });
                    }
                    receive.await
                }
            },
            _ => receive.await,
        }
        .context("cell isolate dropped response")?;
        if let Some(request_id) = request_id {
            self.remote_aborts
                .lock()
                .expect("abort registry poisoned")
                .remove(&request_id);
        }
        js::drain_arm_gates(&scope)
            .await
            .map_err(|error| anyhow!(error))?;
        result
    }

    /// Tell a cell to abandon a fetch, by name.
    ///
    /// `fetch_cell`'s own cancellation needs someone still awaiting it, which
    /// is exactly what a dropped connection does not leave behind: the future
    /// carrying the `select!` dies in the same instant as the signal. A caller
    /// that learns about the hang-up in a destructor has to say so directly.
    pub fn abort_fetch(&self, cell: &str, request_id: js::RequestId) {
        let mut aborts = self.remote_aborts.lock().expect("abort registry poisoned");
        prune_remote_aborts(&mut aborts);
        aborts.insert(request_id, Instant::now());
        let tx = self
            .cells
            .lock()
            .expect("cell registry poisoned")
            .published
            .get(cell)
            .map(|handle| handle.tx.clone());
        if let Some(tx) = tx {
            let _ = tx.send(CellJob::AbortFetch { request_id });
        }
    }

    pub fn published_epoch(&self, cell: &str) -> Option<u64> {
        self.cells
            .lock()
            .expect("cell registry poisoned")
            .published
            .get(cell)
            .map(|handle| handle.epoch)
    }

    pub fn alarm(&self, cell: &str) -> Option<i64> {
        let cells = self.cells.lock().expect("cell registry poisoned");
        let at_ms = cells
            .published
            .get(cell)
            .or_else(|| cells.starting.get(cell))?
            .next_alarm_ms
            .load(Ordering::Acquire);
        (at_ms >= 0).then_some(at_ms)
    }

    pub fn alarm_covered(&self, cell: &str, at_ms: Option<i64>) -> bool {
        match (at_ms, &self.wake) {
            (None, _) => true,
            (Some(at_ms), Some(wake)) if self.replication.is_some() => wake.covered(cell, at_ms),
            (Some(_), None) => false,
            (Some(_), Some(_)) => false,
        }
    }

    pub async fn fire_alarm(
        &self,
        cell: String,
        scheduled_ms: i64,
    ) -> anyhow::Result<(Option<i64>, bool)> {
        let tx = self
            .cells
            .lock()
            .expect("cell registry poisoned")
            .published
            .get(&cell)
            .map(|handle| handle.tx.clone())
            .ok_or_else(|| anyhow!("cell runtime is not published: {cell}"))?;
        let (reply, receive) = tokio::sync::oneshot::channel();
        tx.send(CellJob::Alarm {
            scope: cell.clone(),
            scheduled_ms,
            reply,
        })
        .map_err(|_| anyhow!("cell isolate stopped"))?;
        let at_ms = receive
            .await
            .context("cell isolate dropped alarm result")??;
        js::drain_arm_gates(&cell)
            .await
            .map_err(|error| anyhow!(error))?;
        Ok((at_ms, self.alarm_covered(&cell, at_ms)))
    }

    pub async fn ws_open(&self, cell: String, ws_id: u64, protocol: String) -> anyhow::Result<()> {
        let tx = self.cell_sender(&cell)?;
        let (reply, receive) = tokio::sync::oneshot::channel();
        tx.send(CellJob::WsOpen {
            scope: cell,
            ws_id,
            protocol,
            reply,
        })
        .map_err(|_| anyhow!("cell isolate stopped"))?;
        receive
            .await
            .context("cell isolate dropped WebSocket open")?
    }

    pub async fn rpc(
        &self,
        cell: String,
        name: Option<String>,
        method: String,
        args: js::RpcData,
    ) -> anyhow::Result<js::RpcData> {
        let tx = self.cell_sender(&cell)?;
        let (reply, receive) = tokio::sync::oneshot::channel();
        tx.send(CellJob::Rpc {
            scope: cell,
            name,
            method,
            args,
            reply,
        })
        .map_err(|_| anyhow!("cell isolate stopped"))?;
        receive.await.context("cell isolate dropped RPC result")?
    }

    pub async fn ws_message(
        &self,
        cell: String,
        ws_id: u64,
        data: js::WsIn,
    ) -> anyhow::Result<js::WsDispatch> {
        let tx = self.cell_sender(&cell)?;
        let (reply, receive) = tokio::sync::oneshot::channel();
        tx.send(CellJob::WsMessage {
            scope: cell,
            ws_id,
            data,
            reply,
        })
        .map_err(|_| anyhow!("cell isolate stopped"))?;
        receive
            .await
            .context("cell isolate dropped WebSocket message")?
    }

    pub async fn ws_closed(
        &self,
        cell: String,
        ws_id: u64,
        code: u16,
        reason: String,
        was_clean: bool,
    ) -> anyhow::Result<()> {
        let tx = self.cell_sender(&cell)?;
        let (reply, receive) = tokio::sync::oneshot::channel();
        tx.send(CellJob::WsClosed {
            scope: cell,
            ws_id,
            code,
            reason,
            was_clean,
            reply,
        })
        .map_err(|_| anyhow!("cell isolate stopped"))?;
        receive
            .await
            .context("cell isolate dropped WebSocket close")?
    }

    fn cell_sender(&self, cell: &str) -> anyhow::Result<mpsc::Sender<CellJob>> {
        self.cells
            .lock()
            .expect("cell registry poisoned")
            .published
            .get(cell)
            .map(|handle| handle.tx.clone())
            .ok_or_else(|| anyhow!("cell runtime is not published: {cell}"))
    }

    fn db_path(&self, cell: &str, epoch: u64) -> PathBuf {
        self.data_dir
            .join(cell)
            .join("ltx")
            .join(format!("e{epoch}"))
            .join("db.sqlite")
    }

    fn remove_starting(&self, cell: &str, epoch: u64) {
        let handle = {
            let mut cells = self.cells.lock().expect("cell registry poisoned");
            if cells
                .starting
                .get(cell)
                .is_some_and(|handle| handle.epoch == epoch)
            {
                cells.starting.remove(cell)
            } else {
                None
            }
        };
        if let Some(handle) = handle {
            let _ = handle.tx.send(CellJob::Shutdown);
        }
    }
}

impl StatelessRuntime {
    fn start(
        config: Arc<WorkerConfig>,
        workers: usize,
        node: Arc<str>,
        region: Arc<str>,
    ) -> anyhow::Result<Self> {
        let (tx, rx) = mpsc::channel::<crate::WorkerJob>();
        let pool = Arc::new(crate::WorkerPool::new(tx));
        let rx = Arc::new(Mutex::new(rx));
        let (ready_tx, ready_rx) = mpsc::channel::<Result<(), String>>();

        for worker_index in 0..workers {
            let config = config.clone();
            let jobs = rx.clone();
            let ready = ready_tx.clone();
            let node = node.clone();
            let region = region.clone();
            std::thread::Builder::new()
                .name(format!("celld-worker-{worker_index}"))
                .spawn(move || {
                    asyncrt::init();
                    let mut worker = match Worker::load_config(config, &[]) {
                        Ok(worker) => {
                            let _ = ready.send(Ok(()));
                            worker
                        }
                        Err(error) => {
                            let _ = ready.send(Err(format!("{error:#}")));
                            return;
                        }
                    };
                    loop {
                        let job = {
                            let receiver = jobs.lock().expect("stateless worker queue poisoned");
                            receiver.recv()
                        };
                        let Ok(job) = job else {
                            break;
                        };
                        match job {
                            crate::WorkerJob::Fetch {
                                queued_at,
                                url,
                                method,
                                body,
                                headers,
                                request_id,
                                reply,
                            } => {
                                let execution_started = Instant::now();
                                let result = worker.fetch_and_reply_id(
                                    &url,
                                    &method,
                                    &body,
                                    &headers,
                                    request_id,
                                    reply,
                                );
                                // Per-request timing rides the off-by-default
                                // `timing` target: an info!-per-request costs real
                                // throughput on the hot path, and the `enabled!`
                                // guard skips the elapsed math and formatting when
                                // the target is off. The lab turns it on with
                                // RUST_LOG=info,timing=debug.
                                if let Some(request_id) = request_id.filter(|_| {
                                    tracing::enabled!(target: "timing", tracing::Level::DEBUG)
                                }) {
                                    let queue_wait_us = queued_at.elapsed().as_micros() as u64;
                                    let execution_us =
                                        execution_started.elapsed().as_micros() as u64;
                                    tracing::debug!(
                                        target: "timing",
                                        event = "worker_fetch_timing",
                                        outcome = if result.is_ok() {
                                            "completed"
                                        } else {
                                            "reload_error"
                                        },
                                        request_id = %js::request_id_string(request_id),
                                        node = %node,
                                        region = %region,
                                        runtime_version = env!("CARGO_PKG_VERSION"),
                                        total_us = queued_at.elapsed().as_micros() as u64,
                                        queue_wait_us,
                                        execution_us,
                                        worker_index,
                                        "stateless Worker fetch completed"
                                    );
                                }
                                if let Err(error) = result {
                                    tracing::warn!(%error, worker_index, "stateless Worker reload failed");
                                }
                            }
                            crate::WorkerJob::Rpc {
                                entrypoint,
                                method,
                                args,
                                reply,
                            } => {
                                let _ = reply.send(
                                    worker.dispatch_entrypoint_rpc(&entrypoint, &method, args),
                                );
                            }
                        }
                    }
                })
                .with_context(|| format!("spawn stateless worker {worker_index}"))?;
        }
        drop(ready_tx);
        for _ in 0..workers {
            match ready_rx
                .recv()
                .context("stateless Worker exited during startup")?
            {
                Ok(()) => {}
                Err(error) => return Err(anyhow!("stateless Worker failed to load: {error}")),
            }
        }
        Ok(Self { pool })
    }

    async fn fetch(
        &self,
        url: String,
        method: String,
        body: Vec<u8>,
        headers: Vec<(String, String)>,
        request_id: Option<js::RequestId>,
    ) -> anyhow::Result<HttpResponse> {
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.pool
            .send(crate::WorkerJob::Fetch {
                queued_at: Instant::now(),
                url,
                method,
                body,
                headers,
                request_id,
                reply,
            })
            .map_err(|_| anyhow!("stateless Worker pool stopped"))?;
        receive.await.context("stateless Worker dropped response")?
    }

    async fn rpc(
        &self,
        entrypoint: String,
        method: String,
        args: Vec<u8>,
    ) -> anyhow::Result<Vec<u8>> {
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.pool
            .send(crate::WorkerJob::Rpc {
                entrypoint,
                method,
                args,
                reply,
            })
            .map_err(|_| anyhow!("stateless Worker pool stopped"))?;
        receive
            .await
            .context("stateless Worker dropped RPC result")?
    }
}

struct CellIsolateStartupTiming {
    started: Instant,
    scope: String,
    node: Arc<str>,
    region: Arc<str>,
    epoch: u64,
    fresh: bool,
}

impl CellIsolateStartupTiming {
    fn emit(&self, outcome: &str, failure_phase: &str) -> u64 {
        let total_us = self.started.elapsed().as_micros() as u64;
        tracing::info!(
            event = "cell_isolate_startup_timing",
            outcome,
            failure_phase,
            scope = %self.scope,
            node = %self.node,
            region = %self.region,
            runtime_version = env!("CARGO_PKG_VERSION"),
            epoch = self.epoch,
            fresh = self.fresh,
            total_us,
            "cell isolate startup completed"
        );
        total_us
    }
}

fn run_cell(
    db_path: PathBuf,
    config: Arc<WorkerConfig>,
    rx: mpsc::Receiver<CellJob>,
    startup: tokio::sync::oneshot::Sender<anyhow::Result<u64>>,
    next_alarm_ms: Arc<AtomicI64>,
    alarm_observer: AlarmObserver,
    startup_timing: CellIsolateStartupTiming,
) {
    let cell = startup_timing.scope.clone();
    asyncrt::init();
    #[cfg(debug_assertions)]
    if let Ok(barrier) = std::env::var("CELLD_TEST_CELL_STARTUP_BARRIER") {
        while !Path::new(&barrier).exists() {
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    let opened = storage::open(&cell, path_text(&db_path));
    if let Err(error) = opened {
        startup_timing.emit("error", "storage_open");
        let _ = startup.send(Err(error.context("cell storage open failed")));
        return;
    }
    #[cfg(debug_assertions)]
    if std::env::var("CELLD_TEST_CELL_STARTUP_FAILURE").as_deref() == Ok("1") {
        let error = anyhow!("injected cell isolate startup failure");
        startup_timing.emit("error", "worker_load");
        storage::close(&cell);
        let _ = startup.send(Err(error));
        return;
    }
    storage::watch_alarm(&cell, next_alarm_ms);
    alarm_observer(cell.clone(), storage::get_alarm(&cell));
    let mut worker = match Worker::load_config(config, std::slice::from_ref(&cell)) {
        Ok(worker) => worker,
        Err(error) => {
            startup_timing.emit("error", "worker_load");
            storage::close(&cell);
            let _ = startup.send(Err(error.context("cell isolate load failed")));
            return;
        }
    };
    match storage::get_actor_name(&cell) {
        Ok(Some(name)) => {
            if let Err(error) = worker.set_id_name(&cell, &name) {
                startup_timing.emit("error", "actor_name_restore");
                storage::close(&cell);
                let _ = startup.send(Err(error.context("restore actor name")));
                return;
            }
        }
        Ok(None) => {}
        Err(error) => {
            startup_timing.emit("error", "actor_name_read");
            storage::close(&cell);
            let _ = startup.send(Err(error.context("read actor name")));
            return;
        }
    }
    let startup_us = startup_timing.emit("ready", "");
    if startup.send(Ok(startup_us)).is_err() {
        storage::close(&cell);
        return;
    }

    for job in &rx {
        match job {
            CellJob::Fetch {
                request_id,
                scope,
                name,
                url,
                method,
                body,
                headers,
                reply,
            } => {
                if let Some(name) = name {
                    if let Err(error) = worker.set_id_name(&scope, &name) {
                        let _ = reply.send(Err(error));
                        continue;
                    }
                }
                worker.dispatch_to_and_reply(
                    &scope,
                    FetchRequest {
                        url: &url,
                        method: &method,
                        body: &body,
                        headers: &headers,
                        request_id,
                    },
                    &rx,
                    reply,
                );
            }
            CellJob::WorkerFetch {
                request_id,
                url,
                method,
                body,
                headers,
                inline_activity: _inline_activity,
                fallback_workers: _fallback_workers,
                reply,
            } => {
                if let Err(error) = worker.worker_fetch_and_reply(
                    FetchRequest {
                        url: &url,
                        method: &method,
                        body: &body,
                        headers: &headers,
                        request_id: Some(request_id),
                    },
                    &rx,
                    reply,
                ) {
                    tracing::warn!(%error, cell, "cell Worker isolate reload failed");
                    break;
                }
            }
            // An abort observed by the outer loop arrived after the request
            // stopped pumping. The cross-thread pending-abort registry has
            // already made it visible while the handler was live.
            CellJob::AbortFetch { .. } => {}
            CellJob::Alarm {
                scope,
                scheduled_ms,
                reply,
            } => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;
                let result = if now < scheduled_ms {
                    Err(anyhow!("alarm dispatched before its deadline"))
                } else {
                    if let Some((scheduled_at, retry)) = storage::due_alarm_entry(&scope, now) {
                        storage::begin_alarm_handler(&scope, scheduled_at);
                        match worker.fire_alarm(&scope, retry, &rx) {
                            Ok(()) => storage::finish_alarm_handler(&scope, true, now),
                            Err(error) => storage::finish_alarm_handler_with_retry_policy(
                                &scope,
                                false,
                                now,
                                error.counts_against_limit(),
                            ),
                        }
                    }
                    Ok(storage::get_alarm(&scope))
                };
                let _ = reply.send(result);
            }
            CellJob::WsOpen {
                scope,
                ws_id,
                protocol,
                reply,
            } => {
                let result = worker.dispatch_ws_open(&scope, ws_id, &protocol, &rx);
                let _ = reply.send(result);
            }
            CellJob::Rpc {
                scope,
                name,
                method,
                args,
                reply,
            } => {
                let result = name
                    .as_deref()
                    .map_or(Ok(()), |name| worker.set_id_name(&scope, name))
                    .and_then(|()| worker.dispatch_rpc_data(&scope, &method, args, &rx));
                let _ = reply.send(result);
            }
            CellJob::WsMessage {
                scope,
                ws_id,
                data,
                reply,
            } => {
                let result = worker.dispatch_ws(&scope, ws_id, data, &rx);
                let _ = reply.send(result);
            }
            CellJob::WsClosed {
                scope,
                ws_id,
                code,
                reason,
                was_clean,
                reply,
            } => {
                let result =
                    worker.dispatch_ws_closed(&scope, ws_id, code, &reason, was_clean, &rx);
                let _ = reply.send(result);
            }
            CellJob::Shutdown => break,
        }
    }
    storage::unwatch_alarm(&cell);
    storage::close(&cell);
}

fn path_text(path: &Path) -> &str {
    path.to_str().expect("celld data path must be UTF-8")
}
