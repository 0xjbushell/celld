//! Cell ownership using per-node leases: the ONLY thing renewed is one
//! `nodes/<node>.json` lease per NODE. `cells/<cell>/own.json` is written ONCE
//! on acquisition and references the node. A cell is live iff its ownership
//! record exists AND the owner node's lease is fresh. Cost scales with nodes +
//! activations, not cells × time — so an idle cell generates zero bucket ops
//! of its own.
//!
//! The self-fence is per-NODE and monotonic: a node halts (dropping all its
//! cells) when it cannot renew its own lease within the TTL — immune to
//! wall-clock steps.
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use celld_logic::lease::LeaseAction;
use celld_logic::lease::LeaseTick;
use futures_util::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::debug;
use tracing::info;
use tracing::warn;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeRec {
    pub node: String,
    pub expires_ms: u64,
    /// Reachable HTTP address (host:port) for cross-node dispatch.
    #[serde(default)]
    pub addr: String,
    /// Ed25519 public key for direct, challenge-bound reachability probes.
    #[serde(default)]
    pub probe_public_key: String,
    /// Cross-node request protocol spoken by this process.
    #[serde(default)]
    pub peer_protocol: u16,
    /// Per-process generation for the crash-safe node-to-cell recovery index.
    /// Empty records predate the index and require one legacy fleet scan.
    #[serde(default)]
    pub ownership_index_generation: String,
    /// Advisory capacity sample, refreshed with the authoritative node lease.
    /// Routing and fencing never depend on it.
    #[serde(default)]
    pub load: NodeLoad,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct NodeLoad {
    pub sampled_ms: u64,
    pub resident_cells: usize,
    pub host_websockets: usize,
    pub rss_bytes: u64,
    pub cpu_percent_x100: u64,
    pub open_fds: u64,
    pub fd_limit: u64,
    pub pressured: bool,
    pub shed_cells: u64,
}

pub struct NodeLoadSample {
    pub resident_cells: usize,
    pub host_websockets: usize,
    pub rss_bytes: u64,
    pub cpu_percent_x100: u64,
    pub open_fds: u64,
    pub fd_limit: u64,
    pub pressured: bool,
}

#[derive(Default)]
pub struct NodeLoadState {
    sampled_ms: AtomicU64,
    resident_cells: AtomicUsize,
    host_websockets: AtomicUsize,
    rss_bytes: AtomicU64,
    cpu_percent_x100: AtomicU64,
    open_fds: AtomicU64,
    fd_limit: AtomicU64,
    pressured: AtomicBool,
    shed_cells: AtomicU64,
}

impl NodeLoadState {
    pub fn update(&self, sample: NodeLoadSample) {
        self.sampled_ms.store(now_ms(), Ordering::Relaxed);
        self.resident_cells
            .store(sample.resident_cells, Ordering::Relaxed);
        self.host_websockets
            .store(sample.host_websockets, Ordering::Relaxed);
        self.rss_bytes.store(sample.rss_bytes, Ordering::Relaxed);
        self.cpu_percent_x100
            .store(sample.cpu_percent_x100, Ordering::Relaxed);
        self.open_fds.store(sample.open_fds, Ordering::Relaxed);
        self.fd_limit.store(sample.fd_limit, Ordering::Relaxed);
        self.pressured.store(sample.pressured, Ordering::Relaxed);
    }

    pub fn record_shed(&self) {
        self.shed_cells.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> NodeLoad {
        NodeLoad {
            sampled_ms: self.sampled_ms.load(Ordering::Relaxed),
            resident_cells: self.resident_cells.load(Ordering::Relaxed),
            host_websockets: self.host_websockets.load(Ordering::Relaxed),
            rss_bytes: self.rss_bytes.load(Ordering::Relaxed),
            cpu_percent_x100: self.cpu_percent_x100.load(Ordering::Relaxed),
            open_fds: self.open_fds.load(Ordering::Relaxed),
            fd_limit: self.fd_limit.load(Ordering::Relaxed),
            pressured: self.pressured.load(Ordering::Relaxed),
            shed_cells: self.shed_cells.load(Ordering::Relaxed),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Own {
    pub node: String,
    pub epoch: u64,
}

#[derive(Debug, Clone)]
pub struct ResolvedOwner {
    pub node: String,
    pub addr: String,
    pub expires_ms: u64,
    pub epoch: u64,
    pub peer_protocol: u16,
}

/// The per-node lease — the single heartbeat for a node, shared by all its cells.
pub struct NodeLease {
    c: Client,
    bucket: String,
    node: String,
    addr: String,
    probe_public_key: String,
    ttl_ms: u64,
    load: Arc<NodeLoadState>,
    etag: String,
    last_ok: Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseLifecycleMode {
    Continuous,
    Shadow,
    Lazy,
}

pub struct NodeLeaseOptions<'a> {
    pub client: &'a Client,
    pub bucket: &'a str,
    pub node: &'a str,
    pub addr: &'a str,
    pub probe_public_key: &'a str,
    pub ttl_ms: u64,
    pub linger_ms: u64,
    pub mode: LeaseLifecycleMode,
}

pub struct NodeLeaseManager {
    c: Client,
    bucket: String,
    node: String,
    addr: String,
    probe_public_key: String,
    ttl_ms: u64,
    linger: Duration,
    mode: LeaseLifecycleMode,
    active_cells: AtomicUsize,
    /// Monotonic millis (since process start) of the last successful lease
    /// renewal, plus whether a lease is held at all. The watchdog reads ONLY
    /// these atomics: its fence must never be delayed by the renewal's lock
    /// or its in-flight I/O.
    last_ok_mono_ms: AtomicU64,
    lease_live: AtomicBool,
    started_at: Instant,
    last_inactive: Mutex<Instant>,
    shadow_release_reported: AtomicBool,
    lease: tokio::sync::Mutex<Option<NodeLease>>,
    lease_started: Mutex<Option<Instant>>,
    completed_lease_ms: AtomicU64,
    class_a_writes: AtomicU64,
    class_b_reads: AtomicU64,
    // Keep advisory-only state after the authoritative lease fields. Besides
    // making the trust boundary visible, this preserves the established
    // offsets of fields touched by the ownership lifecycle.
    shadow_decisions: Mutex<ShadowDecisionBuffer>,
    load: Arc<NodeLoadState>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeaseMetrics {
    pub active: bool,
    pub active_cells: usize,
    pub active_ms: u64,
    pub class_a_writes: u64,
    pub class_b_reads: u64,
}

/// One independent read of this process's authoritative bucket lease for
/// advisory presence shadowing. The control plane may compare this observation
/// with the node's presence hint, but neither side may act on the result.
#[derive(Serialize)]
pub struct NodeLeaseShadowObservation {
    pub bucket_status: &'static str,
    pub node: Option<String>,
    pub advertise: Option<String>,
    pub expires_ms: Option<u64>,
    pub checked_at_ms: u64,
}

const MAX_SHADOW_DECISIONS: usize = 8;

#[derive(Serialize, Clone, Debug, PartialEq, Eq)]
pub struct LeaseLifecycleShadowDecision {
    sequence: u64,
    observed_at_ms: u64,
    snapshot: LeaseLifecycleShadowSnapshot,
    expected: LeaseLifecycleShadowExpected,
}

#[derive(Serialize, Clone, Debug, PartialEq, Eq)]
struct LeaseLifecycleShadowSnapshot {
    mode: &'static str,
    active_cells: usize,
    serving_cells: usize,
    idle_ms: u64,
    linger_ms: u64,
    lease_active: bool,
    elapsed_since_ok_ms: u64,
    elapsed_since_renew_ms: u64,
    ttl_ms: u64,
    shadow_release_reported: bool,
}

#[derive(Serialize, Clone, Debug, PartialEq, Eq)]
struct LeaseLifecycleShadowExpected {
    shadow_release: bool,
    authority_action: &'static str,
}

#[derive(Serialize, Clone, Debug, PartialEq, Eq)]
pub struct LeaseLifecycleShadowBatch {
    pub dropped: u64,
    pub decisions: Vec<LeaseLifecycleShadowDecision>,
}

#[derive(Default)]
struct ShadowDecisionBuffer {
    next_sequence: u64,
    dropped: u64,
    decisions: VecDeque<LeaseLifecycleShadowDecision>,
}

pub struct ActivationPermit {
    manager: Arc<NodeLeaseManager>,
    committed: bool,
}

async fn read<T: for<'de> Deserialize<'de>>(
    c: &Client,
    bucket: &str,
    key: &str,
) -> anyhow::Result<Option<(T, String)>> {
    match c.get_object().bucket(bucket).key(key).send().await {
        Ok(o) => {
            let etag = o.e_tag().unwrap_or_default().to_string();
            let v: T = serde_json::from_slice(&o.body.collect().await?.into_bytes())?;
            Ok(Some((v, etag)))
        }
        Err(SdkError::ServiceError(e)) if e.err().is_no_such_key() => Ok(None),
        Err(e) => Err(e.into()),
    }
}

impl NodeLease {
    /// Acquire (or take over a stale) node lease.
    async fn acquire(manager: &NodeLeaseManager) -> anyhow::Result<NodeLease> {
        let c = &manager.c;
        let bucket = &manager.bucket;
        let node = &manager.node;
        let addr = &manager.addr;
        let probe_public_key = &manager.probe_public_key;
        let ttl_ms = manager.ttl_ms;
        let load = manager.load.clone();
        let key = format!("nodes/{node}.json");
        let rec = NodeRec {
            node: node.into(),
            expires_ms: now_ms() + ttl_ms,
            addr: addr.into(),
            probe_public_key: probe_public_key.into(),
            peer_protocol: crate::peer_auth::PROTOCOL_VERSION,
            ownership_index_generation: probe_public_key.into(),
            load: load.snapshot(),
        };
        let body = || ByteStream::from(serde_json::to_vec(&rec).unwrap());
        // create-if-absent, else overwrite our own (nodes are singletons by id)
        manager.class_b_reads.fetch_add(1, Ordering::Relaxed);
        let etag = match read::<NodeRec>(c, bucket, &key).await? {
            None => {
                manager.class_a_writes.fetch_add(1, Ordering::Relaxed);
                c.put_object()
                    .bucket(bucket)
                    .key(&key)
                    .if_none_match("*")
                    .body(body())
                    .send()
                    .await
                    .map(|r| r.e_tag().unwrap_or_default().to_string())
                    .map_err(|e| anyhow::anyhow!("node lease create: {e}"))?
            }
            Some((_, etag)) => {
                manager.class_a_writes.fetch_add(1, Ordering::Relaxed);
                c.put_object()
                    .bucket(bucket)
                    .key(&key)
                    .if_match(&etag)
                    .body(body())
                    .send()
                    .await
                    .map(|r| r.e_tag().unwrap_or_default().to_string())?
            }
        };
        Ok(NodeLease {
            c: c.clone(),
            bucket: bucket.into(),
            node: node.into(),
            addr: addr.into(),
            probe_public_key: probe_public_key.into(),
            ttl_ms,
            load,
            etag,
            last_ok: Instant::now(),
        })
    }

    pub async fn renew(&mut self, class_a_writes: &AtomicU64) -> anyhow::Result<bool> {
        let key = format!("nodes/{}.json", self.node);
        let rec = NodeRec {
            node: self.node.clone(),
            expires_ms: now_ms() + self.ttl_ms,
            addr: self.addr.clone(),
            probe_public_key: self.probe_public_key.clone(),
            peer_protocol: crate::peer_auth::PROTOCOL_VERSION,
            ownership_index_generation: self.probe_public_key.clone(),
            load: self.load.snapshot(),
        };
        class_a_writes.fetch_add(1, Ordering::Relaxed);
        match self
            .c
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .if_match(&self.etag)
            .body(ByteStream::from(serde_json::to_vec(&rec)?))
            .send()
            .await
        {
            Ok(r) => {
                self.etag = r.e_tag().unwrap_or_default().into();
                self.last_ok = Instant::now();
                Ok(true)
            }
            Err(SdkError::ServiceError(e)) if e.raw().status().as_u16() == 412 => Ok(false), // someone else took our id
            Err(e) => Err(e.into()),
        }
    }
}

impl ShadowDecisionBuffer {
    fn push(
        &mut self,
        snapshot: LeaseLifecycleShadowSnapshot,
        expected: LeaseLifecycleShadowExpected,
    ) {
        self.next_sequence = self.next_sequence.saturating_add(1);
        if self.decisions.len() == MAX_SHADOW_DECISIONS {
            self.decisions.pop_front();
            self.dropped = self.dropped.saturating_add(1);
        }
        self.decisions.push_back(LeaseLifecycleShadowDecision {
            sequence: self.next_sequence,
            observed_at_ms: now_ms(),
            snapshot,
            expected,
        });
    }

    fn snapshot(&self) -> LeaseLifecycleShadowBatch {
        LeaseLifecycleShadowBatch {
            dropped: self.dropped,
            decisions: self.decisions.iter().cloned().collect(),
        }
    }
}

impl NodeLeaseManager {
    pub async fn start(options: NodeLeaseOptions<'_>) -> anyhow::Result<Arc<Self>> {
        let load = Arc::new(NodeLoadState::default());
        let manager = Arc::new(Self {
            c: options.client.clone(),
            bucket: options.bucket.to_string(),
            node: options.node.to_string(),
            addr: options.addr.to_string(),
            probe_public_key: options.probe_public_key.to_string(),
            ttl_ms: options.ttl_ms,
            linger: Duration::from_millis(options.linger_ms),
            mode: options.mode,
            active_cells: AtomicUsize::new(0),
            last_ok_mono_ms: AtomicU64::new(0),
            lease_live: AtomicBool::new(false),
            started_at: Instant::now(),
            last_inactive: Mutex::new(Instant::now()),
            shadow_release_reported: AtomicBool::new(false),
            lease: tokio::sync::Mutex::new(None),
            lease_started: Mutex::new(None),
            completed_lease_ms: AtomicU64::new(0),
            class_a_writes: AtomicU64::new(0),
            class_b_reads: AtomicU64::new(0),
            shadow_decisions: Mutex::new(ShadowDecisionBuffer::default()),
            load,
        });
        if options.mode != LeaseLifecycleMode::Lazy {
            manager.ensure_lease().await?;
        }
        // The self-fence watchdog runs on its OWN thread and reads only
        // atomics. The 2026-08-01 lab partition proved why: a black-holed
        // R2 endpoint (packets dropped, not refused) leaves the renewal
        // future awaiting inside the lease lock, so the loop never reaches
        // its own fence decision and the node kept serving 124 s past a
        // 10 s TTL. A fence that can be postponed by the very I/O it exists
        // to survive is not a fence.
        let watchdog_manager = manager.clone();
        std::thread::Builder::new()
            .name("celld-lease-watchdog".into())
            .spawn(move || {
                let interval = Duration::from_millis((watchdog_manager.ttl_ms / 5).max(50));
                loop {
                    std::thread::sleep(interval);
                    watchdog_manager.fence_if_lease_expired();
                }
            })
            .map_err(|error| anyhow::anyhow!("start node lease watchdog: {error}"))?;
        let renewal_manager = manager.clone();
        std::thread::Builder::new()
            .name("celld-node-lease".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap_or_else(|error| {
                        warn!(%error, "SELF-FENCE: node lease runtime could not start");
                        std::process::exit(3);
                    });
                runtime.block_on(renewal_manager.renewal_loop());
            })
            .map_err(|error| anyhow::anyhow!("start node lease runtime: {error}"))?;
        Ok(manager)
    }

    fn mono_ms(&self) -> u64 {
        self.started_at.elapsed().as_millis() as u64
    }

    /// Record a successful renewal for the watchdog. Called from the renewal
    /// loop; deliberately lock-free.
    fn mark_lease_ok(&self) {
        self.last_ok_mono_ms.store(self.mono_ms(), Ordering::Release);
        self.lease_live.store(true, Ordering::Release);
    }

    /// The independent self-fence: halt when the node lease has not been
    /// renewed within its TTL, whatever the renewal task is doing. The
    /// decision itself is the shared sans-IO `lease::decide`, so the
    /// watchdog and the renewal loop can never disagree about when a fence
    /// is due.
    fn fence_if_lease_expired(&self) {
        if !self.lease_live.load(Ordering::Acquire) {
            return;
        }
        let elapsed = self
            .mono_ms()
            .saturating_sub(self.last_ok_mono_ms.load(Ordering::Acquire));
        let decision = celld_logic::lease::decide(&LeaseTick {
            lease_held: true,
            active_cells: self.active_cells.load(Ordering::SeqCst),
            idle_long_enough: false,
            elapsed_since_ok_ms: elapsed,
            elapsed_since_renew_ms: 0,
            ttl_ms: self.ttl_ms,
            lazy: self.mode == LeaseLifecycleMode::Lazy,
        });
        if decision == LeaseAction::Fence {
            warn!(
                event = "node_lease_watchdog_fence",
                elapsed_since_ok_ms = elapsed,
                ttl_ms = self.ttl_ms,
                "SELF-FENCE: node lease not renewed within TTL — halting"
            );
            std::process::exit(3);
        }
    }

    /// Reserve the node lease before attempting cell ownership. Dropping an
    /// uncommitted permit rolls the reservation back; committing it keeps one
    /// active-cell reference until hibernation completes durably.
    pub async fn begin_activation(self: &Arc<Self>) -> anyhow::Result<ActivationPermit> {
        self.active_cells.fetch_add(1, Ordering::SeqCst);
        self.shadow_release_reported.store(false, Ordering::Relaxed);
        if let Err(error) = self.ensure_lease().await {
            self.release_reference();
            return Err(error);
        }
        Ok(ActivationPermit {
            manager: self.clone(),
            committed: false,
        })
    }

    pub fn release_cell(&self) {
        self.release_reference();
    }

    pub fn load_state(&self) -> Arc<NodeLoadState> {
        self.load.clone()
    }

    async fn ensure_lease(&self) -> anyhow::Result<()> {
        let mut lease = self.lease.lock().await;
        if lease.is_none() {
            *lease = Some(NodeLease::acquire(self).await?);
            *self.lease_started.lock().unwrap() = Some(Instant::now());
            self.mark_lease_ok();
            debug!(
                node = %self.node,
                mode = ?self.mode,
                "acquired node lease"
            );
        }
        Ok(())
    }

    fn release_reference(&self) {
        let previous = self.active_cells.fetch_sub(1, Ordering::SeqCst);
        assert!(previous > 0, "node lease active-cell reference underflow");
        if previous == 1 {
            *self.last_inactive.lock().unwrap() = Instant::now();
        }
    }

    pub async fn metrics(&self) -> LeaseMetrics {
        let active = self.lease.lock().await.is_some();
        let current_ms = self
            .lease_started
            .lock()
            .unwrap()
            .as_ref()
            .map(|started| started.elapsed().as_millis() as u64)
            .unwrap_or(0);
        LeaseMetrics {
            active,
            active_cells: self.active_cells.load(Ordering::Relaxed),
            active_ms: self
                .completed_lease_ms
                .load(Ordering::Relaxed)
                .saturating_add(current_ms),
            class_a_writes: self.class_a_writes.load(Ordering::Relaxed),
            class_b_reads: self.class_b_reads.load(Ordering::Relaxed),
        }
    }

    /// Return the bounded, replayed decision history for advisory control-plane
    /// ingestion. Repetition is intentional: the receiver deduplicates by
    /// node session and sequence, while any local overflow remains an explicit
    /// rollout blocker rather than silently losing canary evidence.
    pub fn shadow_decisions(&self) -> LeaseLifecycleShadowBatch {
        self.shadow_decisions.lock().unwrap().snapshot()
    }

    /// Read bucket truth without changing the lease lifecycle. This is only
    /// called by the off-by-default presence shadow task; an unavailable or
    /// malformed record becomes an advisory `unavailable` observation.
    #[cold]
    #[inline(never)]
    pub async fn shadow_observation(&self) -> NodeLeaseShadowObservation {
        let checked_at_ms = now_ms();
        self.class_b_reads.fetch_add(1, Ordering::Relaxed);
        match read::<NodeRec>(&self.c, &self.bucket, &format!("nodes/{}.json", self.node)).await {
            Ok(Some((record, _))) => NodeLeaseShadowObservation {
                bucket_status: if record.expires_ms > checked_at_ms {
                    "live"
                } else {
                    "expired"
                },
                node: Some(record.node),
                advertise: Some(record.addr),
                expires_ms: Some(record.expires_ms),
                checked_at_ms,
            },
            Ok(None) => NodeLeaseShadowObservation {
                bucket_status: "missing",
                node: None,
                advertise: None,
                expires_ms: None,
                checked_at_ms,
            },
            Err(_) => NodeLeaseShadowObservation {
                bucket_status: "unavailable",
                node: None,
                advertise: None,
                expires_ms: None,
                checked_at_ms,
            },
        }
    }

    fn record_lease_end(&self) {
        // A deliberate release (lazy idle, or losing the id) is not a fence
        // condition: stand the watchdog down until a lease is held again.
        self.lease_live.store(false, Ordering::Release);
        let Some(started) = self.lease_started.lock().unwrap().take() else {
            return;
        };
        self.completed_lease_ms.fetch_add(
            started.elapsed().as_millis() as u64,
            Ordering::Relaxed,
        );
    }

    async fn renewal_loop(self: Arc<Self>) {
        let interval_ms = (self.ttl_ms / 5).max(50);
        let mut tick = tokio::time::interval(Duration::from_millis(interval_ms));
        let mut since_renew = Instant::now();
        loop {
            tick.tick().await;
            let active = self.active_cells.load(Ordering::SeqCst);
            let idle = self.last_inactive.lock().unwrap().elapsed();
            let idle_long_enough = active == 0 && idle >= self.linger;
            let shadow_release = self.mode == LeaseLifecycleMode::Shadow
                && idle_long_enough
                && !self.shadow_release_reported.swap(true, Ordering::Relaxed);

            let mut lease = self.lease.lock().await;
            if shadow_release {
                let elapsed_since_renew_ms = since_renew.elapsed().as_millis() as u64;
                let (lease_active, elapsed_since_ok_ms) = lease
                    .as_ref()
                    .map(|current| (true, current.last_ok.elapsed().as_millis() as u64))
                    .unwrap_or((false, 0));
                let authority_action = if !lease_active {
                    "no_lease"
                } else if elapsed_since_ok_ms > self.ttl_ms {
                    "fence"
                } else if elapsed_since_renew_ms >= self.ttl_ms / 3 {
                    "renew"
                } else {
                    "hold"
                };
                self.shadow_decisions.lock().unwrap().push(
                    LeaseLifecycleShadowSnapshot {
                        mode: "shadow",
                        active_cells: active,
                        serving_cells: 0,
                        idle_ms: idle.as_millis() as u64,
                        linger_ms: self.linger.as_millis() as u64,
                        lease_active,
                        elapsed_since_ok_ms,
                        elapsed_since_renew_ms,
                        ttl_ms: self.ttl_ms,
                        shadow_release_reported: false,
                    },
                    LeaseLifecycleShadowExpected {
                        shadow_release: true,
                        authority_action,
                    },
                );
                info!(
                    event = "node_lease_shadow_release",
                    node = %self.node,
                    "lazy lease shadow mode would stop renewal"
                );
            }
            let Some(current) = lease.as_mut() else {
                continue;
            };
            let lazy = self.mode == LeaseLifecycleMode::Lazy;
            // The decision is sans-IO; this loop is its executor — turning
            // Fence into the real halt and Renew into the CAS write.
            let decision = celld_logic::lease::decide(&LeaseTick {
                lease_held: true,
                active_cells: active,
                idle_long_enough,
                elapsed_since_ok_ms: current.last_ok.elapsed().as_millis() as u64,
                elapsed_since_renew_ms: since_renew.elapsed().as_millis() as u64,
                ttl_ms: self.ttl_ms,
                lazy,
            });
            match decision {
                LeaseAction::NoLease | LeaseAction::Hold => {}
                LeaseAction::Release => {
                    info!(
                        event = "node_lease_released",
                        node = %self.node,
                        "no locally owned cells; stopping node lease renewal"
                    );
                    lease.take();
                    self.record_lease_end();
                }
                LeaseAction::Fence => {
                    // The renewal loop and the watchdog are INDEPENDENT layers
                    // of the same rule, so either alone halts the node — which
                    // means neither is pinned by a test that only asserts "the
                    // process exited". This switch disables this layer so a
                    // conformance test can prove the WATCHDOG alone fences
                    // within the modelled bound.
                    if std::env::var("CELLD_TEST_DISABLE_RENEWAL_FENCE").as_deref() == Ok("1") {
                        warn!("renewal-loop fence disabled for conformance test");
                    } else {
                        warn!("SELF-FENCE: node lease not renewed within TTL — halting");
                        std::process::exit(3);
                    }
                }
                LeaseAction::Renew => {
                    since_renew = Instant::now();
                    // Bound the renewal: a black-holed endpoint must not park
                    // this loop indefinitely. The watchdog fences regardless,
                    // but a loop that keeps ticking also retries sooner.
                    let renewal = tokio::time::timeout(
                        Duration::from_millis((self.ttl_ms / 2).max(1_000)),
                        current.renew(&self.class_a_writes),
                    )
                    .await
                    .unwrap_or_else(|_| {
                        Err(anyhow::anyhow!("node-lease renew timed out"))
                    });
                    match renewal {
                        Ok(true) => self.mark_lease_ok(),
                        Ok(false) => match celld_logic::lease::on_renew_rejected(active, lazy) {
                            LeaseAction::Fence => {
                                warn!("node id taken by another process — halting");
                                std::process::exit(3);
                            }
                            _ => {
                                lease.take();
                                self.record_lease_end();
                            }
                        },
                        Err(error) => {
                            warn!(%error, "node-lease renew failed; fence fires if it persists")
                        }
                    }
                }
            }
        }
    }
}

impl ActivationPermit {
    pub fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for ActivationPermit {
    fn drop(&mut self) {
        if !self.committed {
            self.manager.release_reference();
        }
    }
}

/// A won acquisition. `took_over` false means the ownership record still
/// named THIS node at the previous epoch, so no other process has written
/// the cell since we last did — the precondition for reusing local state.
pub struct Acquired {
    pub epoch: u64,
    pub took_over: bool,
}

/// Acquire a cell — create if absent, take over a dead owner, defer to a live
/// one — by driving the sans-IO acquisition machine (`celld_logic::lifecycle`)
/// and performing the effects it asks for. One executor for what used to split
/// across create and takeover, so production runs the exact acquisition logic
/// the deterministic simulator tests. Returns the owned epoch, or `None` when a
/// live owner holds the cell or a racing node won the CAS.
///
/// `fresh` = the routing lookup already observed the record absent; the machine
/// then skips re-reading a known-missing key on the latency-critical first
/// activation (a lost create race still 412s and retries).
pub async fn acquire_via_core(
    c: &Client,
    bucket: &str,
    cell: &str,
    node: &str,
    fresh: bool,
) -> anyhow::Result<Option<Acquired>> {
    use celld_logic::cell::Owner;
    use celld_logic::lifecycle::Cell;
    use celld_logic::lifecycle::Effect;
    use celld_logic::lifecycle::Event;
    use celld_logic::lifecycle::Phase;
    let key = format!("cells/{cell}/own.json");
    let mut state = Cell::new(node);
    let mut event = Event::Activate;
    let mut took_over = false;
    loop {
        let mut next: Option<Event> = None;
        for effect in state.on_event(0, now_ms(), event) {
            match effect {
                Effect::Get if fresh => next = Some(Event::GetMissing),
                Effect::Get => {
                    next = Some(match read::<Own>(c, bucket, &key).await? {
                        Some((cur, etag)) => Event::GetOwner {
                            owner: Owner { node: cur.node, epoch: cur.epoch },
                            etag,
                        },
                        None => Event::GetMissing,
                    });
                }
                Effect::GetOwnerLease { node: owner } => {
                    let live = read::<NodeRec>(c, bucket, &format!("nodes/{owner}.json"))
                        .await?
                        .is_some_and(|(rec, _)| rec.expires_ms > now_ms());
                    next = Some(Event::OwnerLive { live });
                }
                Effect::CasCreate { epoch } => {
                    next = Some(put_own(c, bucket, &key, node, epoch, None).await?);
                }
                Effect::CasWrite { etag, epoch, takeover } => {
                    took_over = takeover;
                    next = Some(put_own(c, bucket, &key, node, epoch, Some(&etag)).await?);
                }
                // `Owned` reached: restore, runtime startup, and publication are
                // the caller's next steps. Ownership alone is never routable.
                Effect::RestoreTruth { .. } => {}
                _ => {}
            }
        }
        match &state.phase {
            Phase::Owned { epoch, .. } => {
                crate::advisory_activity().record_acquisition(*epoch, took_over);
                return Ok(Some(Acquired { epoch: *epoch, took_over }));
            }
            Phase::Idle => return Ok(None), // gave up: live owner, or lost race
            _ => event = next.expect("a non-terminal acquisition step yields an event"),
        }
    }
}

/// Perform an ownership CAS — `if-none-match:*` to create, `if-match:etag` to
/// take over — mapping the result to the event the machine expects. The marker
/// is written before the CAS by the `IndexCellMarker` effect, so a crash after
/// winning can never index a node lease whose owned cell is absent from
/// recovery. Transient errors propagate; only a 412 is `CasRejected`.
async fn put_own(
    c: &Client,
    bucket: &str,
    key: &str,
    node: &str,
    epoch: u64,
    etag: Option<&str>,
) -> anyhow::Result<celld_logic::lifecycle::Event> {
    use celld_logic::lifecycle::Event;
    let own = Own { node: node.into(), epoch };
    let body = ByteStream::from(serde_json::to_vec(&own)?);
    let req = c.put_object().bucket(bucket).key(key).body(body);
    let req = match etag {
        Some(etag) => req.if_match(etag),
        None => req.if_none_match("*"),
    };
    match req.send().await {
        Ok(resp) => Ok(Event::CasOk {
            epoch,
            etag: resp.e_tag().unwrap_or_default().to_string(),
        }),
        Err(SdkError::ServiceError(e)) if e.raw().status().as_u16() == 412 => {
            Ok(Event::CasRejected)
        }
        Err(e) => Err(e.into()),
    }
}

/// Publish a fenced, unowned state for one locally owned cell. The record is
/// retained so the next owner must advance the epoch; deleting it would reset
/// the fencing sequence to one.
pub async fn relinquish_cell(
    c: &Client,
    bucket: &str,
    cell: &str,
    node: &str,
    epoch: u64,
) -> anyhow::Result<bool> {
    use celld_logic::cell::{relinquish, Owner};
    let key = format!("cells/{cell}/own.json");
    let Some((current, etag)) = read::<Own>(c, bucket, &key).await? else {
        return Ok(false);
    };
    let owner = Owner { node: current.node.clone(), epoch: current.epoch };
    let Some(released) = relinquish(Some(&owner), node, epoch) else {
        return Ok(false);
    };
    let released = Own { node: released.node, epoch: released.epoch };
    match c
        .put_object()
        .bucket(bucket)
        .key(&key)
        .if_match(etag)
        .body(ByteStream::from(serde_json::to_vec(&released)?))
        .send()
        .await
    {
        Ok(_) => Ok(true),
        Err(SdkError::ServiceError(error)) if error.raw().status().as_u16() == 412 => Ok(false),
        Err(error) => Err(error.into()),
    }
}

/// Resolve the live node currently owning `cell` — the cross-node routing
/// lookup. `None` if unowned or the owner's lease is stale. Two bucket GETs;
/// the caller caches so the warm local path stays at zero ops.
pub async fn resolve_owner(
    c: &Client,
    bucket: &str,
    cell: &str,
) -> anyhow::Result<Option<ResolvedOwner>> {
    let Some(own) = read_owner(c, bucket, cell).await? else {
        return Ok(None);
    };
    if own.node.is_empty() {
        return Ok(None);
    }
    Ok(resolve_node(c, bucket, &own.node)
        .await?
        .map(|rec| ResolvedOwner {
            node: rec.node,
            addr: rec.addr,
            expires_ms: rec.expires_ms,
            epoch: own.epoch,
            peer_protocol: rec.peer_protocol,
        }))
}

/// Read the stable cell-to-node ownership mapping without resolving the
/// referenced node lease.
pub async fn read_owner(c: &Client, bucket: &str, cell: &str) -> anyhow::Result<Option<Own>> {
    Ok(
        read::<Own>(c, bucket, &format!("cells/{cell}/own.json"))
            .await?
            .map(|(own, _)| own),
    )
}

/// Resolve one fresh node lease. Callers may cache this once per node until
/// `expires_ms`; all cells owned by that node share the same record.
pub async fn resolve_node(
    c: &Client,
    bucket: &str,
    node: &str,
) -> anyhow::Result<Option<NodeRec>> {
    Ok(
        match read::<NodeRec>(c, bucket, &format!("nodes/{node}.json")).await? {
            Some((rec, _)) if rec.expires_ms > now_ms() && !rec.addr.is_empty() => Some(rec),
            _ => None,
        },
    )
}

const CAPACITY_LOOKUP_CONCURRENCY: usize = 16;
const CAPACITY_CACHE_MIN_MS: u64 = 250;
const CAPACITY_CACHE_MAX_MS: u64 = 2_000;
const CAPACITY_OBJECT_RECENCY_FLOOR_MS: u64 = 60_000;

#[derive(Default)]
pub struct CapacityPeerCache {
    refreshed_at: Option<Instant>,
    peers: Vec<NodeRec>,
    reservations: HashMap<String, usize>,
}

fn capacity_object_is_recent(last_modified_secs: i64, now_ms: u64, ttl_ms: u64) -> bool {
    let recency_ms = ttl_ms
        .saturating_mul(3)
        .max(CAPACITY_OBJECT_RECENCY_FLOOR_MS);
    let cutoff_secs = now_ms.saturating_sub(recency_ms) / 1_000;
    last_modified_secs >= cutoff_secs as i64
}

async fn capacity_node_ids(c: &Client, bucket: &str, ttl_ms: u64) -> anyhow::Result<Vec<String>> {
    let current_ms = now_ms();
    let mut nodes = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let mut request = c.list_objects_v2().bucket(bucket).prefix("nodes/");
        if let Some(token) = &token {
            request = request.continuation_token(token);
        }
        let page = request.send().await?;
        for object in page.contents() {
            if object.last_modified().is_some_and(|modified| {
                !capacity_object_is_recent(modified.secs(), current_ms, ttl_ms)
            }) {
                continue;
            }
            let Some(node) = object
                .key()
                .and_then(|key| key.strip_prefix("nodes/"))
                .and_then(|key| key.strip_suffix(".json"))
            else {
                continue;
            };
            if !node.is_empty() {
                nodes.push(node.to_string());
            }
        }
        match page.next_continuation_token() {
            Some(next) => token = Some(next.to_string()),
            None => break,
        }
    }
    nodes.sort();
    nodes.dedup();
    Ok(nodes)
}

async fn collect_capacity_records<T, F, Fut>(nodes: Vec<String>, fetch: F) -> anyhow::Result<Vec<T>>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = anyhow::Result<Option<T>>>,
{
    let mut reads =
        stream::iter(nodes.into_iter().map(fetch)).buffer_unordered(CAPACITY_LOOKUP_CONCURRENCY);
    let mut records = Vec::new();
    while let Some(record) = reads.next().await {
        if let Some(record) = record? {
            records.push(record);
        }
    }
    Ok(records)
}

fn select_capacity_peer(
    cache: &mut CapacityPeerCache,
    local_node: &str,
    below_resident_cells: Option<usize>,
    current_ms: u64,
) -> Option<ResolvedOwner> {
    let record = cache
        .peers
        .iter()
        .filter(|record| {
            record.node != local_node
                && record.expires_ms > current_ms
                && !record.addr.is_empty()
                && record.load.sampled_ms != 0
                && !record.load.pressured
        })
        .filter_map(|record| {
            let projected = record
                .load
                .resident_cells
                .saturating_add(*cache.reservations.get(&record.node).unwrap_or(&0));
            if below_resident_cells.is_some_and(|local| projected >= local) {
                return None;
            }
            Some((record, projected))
        })
        .min_by_key(|(record, projected)| {
            (
                *projected,
                record.load.host_websockets,
                record.load.rss_bytes,
                record.node.as_str(),
            )
        })
        .map(|(record, _)| record.clone())?;
    *cache.reservations.entry(record.node.clone()).or_default() += 1;
    Some(ResolvedOwner {
        node: record.node,
        addr: record.addr,
        expires_ms: record.expires_ms,
        epoch: 0,
        peer_protocol: record.peer_protocol,
    })
}

/// Pick a live, non-pressured peer for an unowned cell this node cannot admit.
///
/// This is the standalone scale-out path: node leases are already the
/// authoritative membership records and carry the latest advisory load
/// sample, so no control plane is required. Epoch zero marks a routing
/// candidate rather than an existing owner; the peer acquires the real epoch
/// through the ordinary signed internal request.
pub async fn capacity_peer(
    cache: &mut CapacityPeerCache,
    c: &Client,
    bucket: &str,
    local_node: &str,
    below_resident_cells: Option<usize>,
    ttl_ms: u64,
) -> anyhow::Result<Option<ResolvedOwner>> {
    let refresh_ms = (ttl_ms / 5).clamp(CAPACITY_CACHE_MIN_MS, CAPACITY_CACHE_MAX_MS);
    let refresh = cache
        .refreshed_at
        .is_none_or(|refreshed| refreshed.elapsed() >= Duration::from_millis(refresh_ms));
    if refresh {
        let nodes = capacity_node_ids(c, bucket, ttl_ms).await?;
        cache.peers = collect_capacity_records(nodes, |node| async move {
            Ok(read::<NodeRec>(c, bucket, &format!("nodes/{node}.json"))
                .await?
                .map(|(record, _)| record))
        })
        .await?;
        cache.reservations.clear();
        cache.refreshed_at = Some(Instant::now());
    }
    Ok(select_capacity_peer(
        cache,
        local_node,
        below_resident_cells,
        now_ms(),
    ))
}

/// Stop routing new admissions to a peer whose capacity response disproved
/// its cached load sample. The ordinary bounded refresh will reconsider it
/// once the node publishes a newer sample.
pub fn reject_capacity_peer(cache: &mut CapacityPeerCache, node: &str) {
    cache.peers.retain(|record| record.node != node);
    cache.reservations.remove(node);
}

/// Read one node lease for a non-mutating direct reachability diagnostic.
///
/// An expired lease is a normal result so fleet enumeration can omit dead
/// sessions while an explicitly requested peer can still report that expiry
/// as an operator-visible failure.
pub async fn diagnostic_node(
    c: &Client,
    bucket: &str,
    node: &str,
) -> anyhow::Result<Option<NodeRec>> {
    let key = format!("nodes/{node}.json");
    let Some((record, _)) = read::<NodeRec>(c, bucket, &key).await? else {
        return Err(anyhow::anyhow!("node {node} has no lease in s3://{bucket}"));
    };
    if record.node != node {
        return Err(anyhow::anyhow!(
            "node lease {key} identifies unexpected node {:?}",
            record.node
        ));
    }
    if record.expires_ms <= now_ms() {
        return Ok(None);
    }
    if record.addr.is_empty() {
        return Err(anyhow::anyhow!("node {node} lease has no advertised address"));
    }
    Ok(Some(record))
}

/// Enumerate every node-session ID represented in the fleet bucket. Callers
/// read each record separately so malformed and expired peers remain visible
/// diagnostic results instead of aborting the whole fleet scan.
pub async fn diagnostic_node_ids(c: &Client, bucket: &str) -> anyhow::Result<Vec<String>> {
    let mut nodes = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let mut request = c.list_objects_v2().bucket(bucket).prefix("nodes/");
        if let Some(token) = &token {
            request = request.continuation_token(token);
        }
        let page = request.send().await?;
        for object in page.contents() {
            let Some(node) = object
                .key()
                .and_then(|key| key.strip_prefix("nodes/"))
                .and_then(|key| key.strip_suffix(".json"))
            else {
                continue;
            };
            if !node.is_empty() {
                nodes.push(node.to_string());
            }
        }
        match page.next_continuation_token() {
            Some(next) => token = Some(next.to_string()),
            None => break,
        }
    }
    nodes.sort();
    nodes.dedup();
    Ok(nodes)
}

#[derive(Clone, Debug)]
pub struct DeadNode {
    pub node: String,
    pub ownership_index_generation: String,
}

/// Node sessions whose lease has lapsed or vanished, from one LIST of `nodes/`.
///
/// The signal that some of that node's cells may hold an armed alarm with no
/// wake entry: entries are written by the owner's own reconcile pass, so an
/// owner that died between arming and its next sweep left nothing for any
/// entry-driven path (waker, boot scan) to find.
pub async fn dead_nodes(c: &Client, bucket: &str, now_ms: u64) -> Vec<DeadNode> {
    let mut dead = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let mut req = c.list_objects_v2().bucket(bucket).prefix("nodes/");
        if let Some(t) = &token {
            req = req.continuation_token(t);
        }
        let page = match req.send().await {
            Ok(page) => page,
            Err(e) => {
                warn!(error = %e, "dead-node scan list failed");
                break;
            }
        };
        for object in page.contents() {
            let Some(key) = object.key() else { continue };
            let Some(node) = key
                .strip_prefix("nodes/")
                .and_then(|rest| rest.strip_suffix(".json"))
            else {
                continue;
            };
            let record = match read::<NodeRec>(c, bucket, key).await {
                Ok(record) => record,
                Err(e) => {
                    warn!(%node, error = %e, "dead-node scan read failed");
                    continue;
                }
            };
            // The classification is sans-IO; the scan is its executor.
            let classified = celld_logic::lease::node_liveness(
                record.as_ref().map(|(rec, _)| celld_logic::lease::NodeRecord {
                    expires_ms: rec.expires_ms,
                    node: &rec.node,
                    ownership_index_generation: &rec.ownership_index_generation,
                }),
                node,
                now_ms,
            );
            if let celld_logic::lease::NodeLiveness::Dead { ownership_index_generation } =
                classified
            {
                dead.push(DeadNode { node: node.to_string(), ownership_index_generation });
            }
        }
        match page.next_continuation_token() {
            Some(t) => token = Some(t.to_string()),
            None => break,
        }
    }
    dead
}

/// Delete one node-session record only if it is still the expired version we
/// inspected. A configured node id can be reused by a restarted process, so an
/// unconditional delete after reconciliation could remove a newly live lease.
pub async fn retire_dead_node(
    c: &Client,
    bucket: &str,
    node: &str,
    now_ms: u64,
) -> anyhow::Result<bool> {
    let key = format!("nodes/{node}.json");
    let Some((record, etag)) = read::<NodeRec>(c, bucket, &key).await? else {
        return Ok(true);
    };
    if celld_logic::lease::lease_live(record.expires_ms, now_ms) {
        return Ok(false);
    }
    match c
        .delete_object()
        .bucket(bucket)
        .key(&key)
        .if_match(etag)
        .send()
        .await
    {
        Ok(_) => Ok(true),
        Err(SdkError::ServiceError(error)) if error.raw().status().as_u16() == 412 => Ok(false),
        Err(error) => Err(error.into()),
    }
}

const DEATH_SWEEP_OWNERSHIP_CONCURRENCY: usize = 16;

#[derive(Default)]
pub struct IndexedOwnership {
    pub cells: Vec<String>,
    pub markers: Vec<String>,
}

/// Discover cells associated with indexed dead-node generations. Markers are
/// crash-safe hints written before the ownership CAS, so stale entries are
/// expected and the caller must still resolve each cell through own.json.
/// One global LIST covers an arbitrary dead-node batch.
pub async fn cells_indexed_by_nodes(
    c: &Client,
    bucket: &str,
    nodes: &HashMap<String, String>,
) -> anyhow::Result<HashMap<String, IndexedOwnership>> {
    let started = Instant::now();
    let mut pages = 0_u64;
    let mut markers_scanned = 0_u64;
    let mut markers_matched = 0_u64;
    let mut owned: HashMap<String, IndexedOwnership> = nodes
        .keys()
        .cloned()
        .map(|node| (node, IndexedOwnership::default()))
        .collect();
    let mut token: Option<String> = None;
    loop {
        let mut request = c.list_objects_v2().bucket(bucket).prefix("node-cells/");
        if let Some(token) = &token {
            request = request.continuation_token(token);
        }
        let page = request.send().await?;
        for object in page.contents() {
            markers_scanned += 1;
            let Some(key) = object.key() else { continue };
            // Shared with the DST property test: candidacy depends only on
            // the node's deadness, never the record's current generation.
            let Some((node, cell)) =
                celld_logic::dead_node_reconciliation::parse_marker_key(key)
            else {
                continue;
            };
            if !nodes.contains_key(node) {
                continue;
            }
            if let Some(entry) = owned.get_mut(node) {
                entry.cells.push(cell.to_string());
                entry.markers.push(key.to_string());
                markers_matched += 1;
            }
        }
        pages += 1;
        match page.next_continuation_token() {
            Some(next) => token = Some(next.to_string()),
            None => break,
        }
    }
    info!(
        phase = "indexed_ownership_discovery",
        pages,
        markers_scanned,
        markers_matched,
        dead_nodes = nodes.len(),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "dead-node ownership discovery complete"
    );
    Ok(owned)
}

pub async fn delete_node_cell_markers(
    c: &Client,
    bucket: &str,
    markers: Vec<String>,
) -> anyhow::Result<()> {
    let mut deletes = stream::iter(markers.into_iter().map(|key| async move {
        c.delete_object().bucket(bucket).key(key).send().await?;
        Ok::<_, anyhow::Error>(())
    }))
    .buffer_unordered(DEATH_SWEEP_OWNERSHIP_CONCURRENCY);
    while let Some(result) = deletes.next().await {
        result?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shadow_decision_buffer_replays_bounded_sequences_and_reports_overflow() {
        let mut buffer = ShadowDecisionBuffer::default();
        for idle_ms in 0..(MAX_SHADOW_DECISIONS as u64 + 2) {
            buffer.push(
                LeaseLifecycleShadowSnapshot {
                    mode: "shadow",
                    active_cells: 0,
                    serving_cells: 0,
                    idle_ms,
                    linger_ms: 0,
                    lease_active: true,
                    elapsed_since_ok_ms: idle_ms,
                    elapsed_since_renew_ms: idle_ms,
                    ttl_ms: 1_000,
                    shadow_release_reported: false,
                },
                LeaseLifecycleShadowExpected {
                    shadow_release: true,
                    authority_action: "hold",
                },
            );
        }

        let batch = buffer.snapshot();
        assert_eq!(batch.dropped, 2);
        assert_eq!(batch.decisions.len(), MAX_SHADOW_DECISIONS);
        assert_eq!(batch.decisions.first().unwrap().sequence, 3);
        assert_eq!(
            batch.decisions.last().unwrap().sequence,
            MAX_SHADOW_DECISIONS as u64 + 2
        );
        assert!(
            serde_json::to_vec(&batch).unwrap().len() < 3_000,
            "the maximum replay batch must fit beside the ordinary presence payload"
        );
        let encoded = serde_json::to_value(batch).unwrap();
        assert_eq!(encoded["decisions"][0]["snapshot"]["mode"], "shadow");
        assert_eq!(
            encoded["decisions"][0]["expected"]["authority_action"],
            "hold"
        );
    }

    fn peer(node: &str, expires_ms: u64, resident: usize, pressured: bool) -> NodeRec {
        NodeRec {
            node: node.into(),
            expires_ms,
            addr: format!("{node}:8080"),
            probe_public_key: String::new(),
            peer_protocol: crate::peer_auth::PROTOCOL_VERSION,
            ownership_index_generation: String::new(),
            load: NodeLoad {
                sampled_ms: 1,
                resident_cells: resident,
                pressured,
                ..Default::default()
            },
        }
    }

    // The routing choice is now a deterministic function of the sample and an
    // injected clock: pick the least-loaded live, non-pressured peer strictly
    // below our own residency, skipping self, the expired, and the pressured.
    #[test]
    fn capacity_peer_picks_the_least_loaded_admissible_peer() {
        let now = 1_000;
        let mut cache = CapacityPeerCache {
            refreshed_at: None,
            peers: vec![
                peer("me", 9_000, 0, false),  // self — excluded
                peer("a", 9_000, 50, false),  // live, roomy
                peer("b", 9_000, 30, false),  // live, roomier — the winner
                peer("c", 9_000, 10, true),   // pressured — excluded
                peer("d", 500, 5, false),     // lease expired — excluded
            ],
            reservations: HashMap::new(),
        };

        let chosen = select_capacity_peer(&mut cache, "me", Some(100), now).unwrap();
        assert_eq!(chosen.node, "b");
        assert_eq!(chosen.epoch, 0, "a routing candidate, not an owner");
        assert_eq!(cache.reservations.get("b"), Some(&1), "the pick is reserved");

        // Below-residency gate: nothing is roomier than a caller at 25.
        assert!(select_capacity_peer(&mut cache, "me", Some(25), now).is_none());
    }
}

#[cfg(all(test, celld_internal_tests))]
mod private {
    include!(env!("CELLD_CONFORMANCE_OWNERSHIP_TESTS"));
}
