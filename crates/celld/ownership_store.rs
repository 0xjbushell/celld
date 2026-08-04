//! S3-compatible ownership effect adapter.
//!
//! This module deliberately contains serialization, wall-clock sampling, SDK
//! configuration and error classification only. Ownership decisions remain in
//! `celld-logic`.

use anyhow::Context;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use celld_logic::{
    CapacityPeer, CasGuard, CasOutcome, LeaseCasOutcome, NodeLeaseRecord, OwnerRecord,
};
use futures_util::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Serialize)]
struct OwnerWire<'a> {
    node: &'a str,
    epoch: u64,
}

#[derive(Deserialize)]
struct OwnerWireOwned {
    node: String,
    epoch: u64,
}

#[derive(Deserialize, Serialize)]
struct NodeLeaseWire {
    node: String,
    expires_ms: u64,
    #[serde(default)]
    addr: String,
    #[serde(default)]
    probe_public_key: String,
    #[serde(default)]
    peer_protocol: u16,
    #[serde(default, rename = "ownership_index_generation")]
    generation: String,
    #[serde(default)]
    load: NodeLoadWire,
}

#[derive(Clone, Default, Deserialize, Serialize)]
pub struct NodeLoadWire {
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

/// A node record older than this is not worth reading. Three lease
/// lifetimes, floored, so a fleet with a short TTL does not discard records
/// a slow renewal would have refreshed.
const CAPACITY_RECORD_RECENCY_FLOOR_MS: u64 = 60_000;

fn capacity_record_is_recent(last_modified_secs: i64, now_ms: u64, lease_ttl_ms: u64) -> bool {
    let window_ms = lease_ttl_ms
        .saturating_mul(3)
        .max(CAPACITY_RECORD_RECENCY_FLOOR_MS);
    last_modified_secs >= (now_ms.saturating_sub(window_ms) / 1_000) as i64
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// The production-compatible conditional object store used by ownership
/// effects. A failed write is always reported to the core as ambiguous unless
/// S3 definitively returned HTTP 412.
pub struct S3Ownership {
    client: Client,
    lease_client: Client,
    bucket: String,
    node: String,
    probe_public_key: String,
    live: Arc<LiveLoad>,
    lease_ttl_ms: u64,
}

/// What this node currently looks like, for peers deciding where to place a
/// cell. The executor owns these numbers and publishes them on every lease
/// renewal; nothing here decides anything locally.
#[derive(Debug, Default)]
pub struct LiveLoad {
    pub resident_cells: AtomicUsize,
    pub host_websockets: AtomicUsize,
    pub cpu_percent_x100: AtomicU64,
    pub pressured: AtomicBool,
    /// Cells shed since this process started. Monotonic, and only ever read
    /// by a human or a diagnostic -- placement uses the levels, not the rate.
    pub shed_cells: AtomicU64,
}

impl S3Ownership {
    pub fn new(client: Client, bucket: String, node: String) -> Self {
        Self {
            lease_client: client.clone(),
            client,
            bucket,
            node,
            probe_public_key: String::new(),
            live: Arc::new(LiveLoad::default()),
            lease_ttl_ms: 0,
        }
    }

    /// Configure the per-process key advertised for challenge-bound direct
    /// probes. Read-only ownership adapters deliberately use [`Self::new`]
    /// and cannot accidentally publish a lease.
    pub fn with_probe_public_key(
        client: Client,
        lease_client: Client,
        bucket: String,
        node: String,
        probe_public_key: String,
    ) -> Self {
        Self {
            client,
            lease_client,
            bucket,
            node,
            probe_public_key,
            live: Arc::new(LiveLoad::default()),
            lease_ttl_ms: 0,
        }
    }

    pub async fn from_environment(bucket: String, node: String) -> anyhow::Result<Self> {
        let endpoint = std::env::var("S3_ENDPOINT").ok();
        let region = std::env::var("AWS_REGION")
            .ok()
            .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
            .unwrap_or_else(|| "us-east-1".into());

        // These bounds are a correctness condition for the node self-fence,
        // not merely tuning. Keep them aligned with celld while that lease
        // path is moved into the new core.
        let timeouts = aws_config::timeout::TimeoutConfig::builder()
            .connect_timeout(Duration::from_secs(3))
            .read_timeout(Duration::from_secs(10))
            .operation_attempt_timeout(Duration::from_secs(15))
            .operation_timeout(Duration::from_secs(30))
            .build();
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .timeout_config(timeouts)
            .region(aws_config::Region::new(region));
        if let Some(endpoint) = endpoint.as_deref() {
            loader = loader.endpoint_url(endpoint);
        }
        let shared = loader.load().await;
        let config = aws_sdk_s3::config::Builder::from(&shared)
            .force_path_style(endpoint.is_some())
            .build();
        Ok(Self::new(Client::from_conf(config), bucket, node))
    }

    /// The lease lifetime this fleet renews on, used to decide which node
    /// records are still worth reading.
    pub fn with_lease_ttl_ms(mut self, ttl_ms: u64) -> Self {
        self.lease_ttl_ms = ttl_ms;
        self
    }

    /// The counters this node publishes to its peers.
    pub fn live(&self) -> Arc<LiveLoad> {
        self.live.clone()
    }

    /// Stable identity for this exact lease-writing process.
    pub fn process_generation(&self) -> Option<&str> {
        (!self.probe_public_key.is_empty()).then_some(self.probe_public_key.as_str())
    }

    pub async fn read_owner(&self, cell: &str) -> anyhow::Result<Option<OwnerRecord>> {
        let key = format!("cells/{cell}/own.json");
        let Some((owner, etag)) = self.read_json::<OwnerWireOwned>(&key).await? else {
            return Ok(None);
        };
        Ok(Some(OwnerRecord {
            node: (!owner.node.is_empty()).then_some(owner.node),
            epoch: owner.epoch,
            etag,
        }))
    }

    pub async fn read_node_lease(&self, owner: &str) -> anyhow::Result<Option<NodeLeaseRecord>> {
        self.read_node_lease_with(&self.client, owner).await
    }

    /// Read this process's authority record through the isolated lease pool.
    pub async fn read_self_node_lease(
        &self,
        owner: &str,
    ) -> anyhow::Result<Option<NodeLeaseRecord>> {
        self.read_node_lease_with(&self.lease_client, owner).await
    }

    async fn read_node_lease_with(
        &self,
        client: &Client,
        owner: &str,
    ) -> anyhow::Result<Option<NodeLeaseRecord>> {
        let key = format!("nodes/{owner}.json");
        Ok(self
            .read_json_with::<NodeLeaseWire>(client, &key)
            .await?
            .map(|(lease, etag)| NodeLeaseRecord {
                node: lease.node,
                addr: lease.addr,
                expires_ms: lease.expires_ms,
                peer_protocol: lease.peer_protocol,
                generation: lease.generation,
                etag,
            }))
    }

    /// Enumerate the fleet membership records used for advisory placement.
    /// The adapter owns pagination and bounded I/O concurrency; the core gets
    /// every decoded observation and owns all filtering and selection policy.
    pub async fn read_capacity_peers(&self) -> anyhow::Result<Vec<CapacityPeer>> {
        const READ_CONCURRENCY: usize = 16;
        let current_ms = now_ms();
        let mut nodes = Vec::new();
        let mut continuation = None;
        loop {
            let mut request = self.client.list_objects_v2().bucket(&self.bucket).prefix("nodes/");
            if let Some(token) = &continuation {
                request = request.continuation_token(token);
            }
            let page = request.send().await?;
            for object in page.contents() {
                // A record nothing has rewritten in several lease lifetimes
                // belongs to a node that is not coming back. Skipping it here
                // is the difference between reading the live fleet and
                // reading every node that has ever run: the listing is what
                // the placement decision costs, and it is paid on every
                // unowned cell.
                if object.last_modified().is_some_and(|modified| {
                    !capacity_record_is_recent(modified.secs(), current_ms, self.lease_ttl_ms)
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
                Some(token) => continuation = Some(token.to_string()),
                None => break,
            }
        }
        nodes.sort();
        nodes.dedup();

        let mut reads = stream::iter(nodes.into_iter().map(|node| async move {
            let key = format!("nodes/{node}.json");
            Ok::<_, anyhow::Error>(self
                .read_json::<NodeLeaseWire>(&key)
                .await?
                .map(|(lease, _)| CapacityPeer {
                    node: lease.node,
                    addr: lease.addr,
                    expires_ms: lease.expires_ms,
                    peer_protocol: lease.peer_protocol,
                    sampled_ms: lease.load.sampled_ms,
                    resident_cells: lease.load.resident_cells,
                    host_websockets: lease.load.host_websockets,
                    rss_bytes: lease.load.rss_bytes,
                    pressured: lease.load.pressured,
                }))
        }))
        .buffer_unordered(READ_CONCURRENCY);
        let mut peers = Vec::new();
        while let Some(peer) = reads.next().await {
            if let Some(peer) = peer? {
                peers.push(peer);
            }
        }
        Ok(peers)
    }

    /// Publish a cell as unowned, keeping its epoch.
    ///
    /// Read-then-conditional-write, because the release is only safe against
    /// the exact record this node wrote: a takeover in the meantime means the
    /// cell is someone else's now, and blanking it would strip a live owner's
    /// claim. Rejection is an ordinary outcome, not an error -- the record
    /// keeps naming whoever it names, and nothing was lost.
    pub async fn release_owner(&self, cell: &str, epoch: u64) -> anyhow::Result<CasOutcome> {
        let Some(current) = self.read_owner(cell).await? else {
            return Ok(CasOutcome::Rejected);
        };
        if current.node.as_deref() != Some(self.node.as_str()) || current.epoch != epoch {
            return Ok(CasOutcome::Rejected);
        }
        let key = format!("cells/{cell}/own.json");
        let body = ByteStream::from(serde_json::to_vec(&OwnerWire { node: "", epoch })?);
        match self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(body)
            .if_match(current.etag)
            .send()
            .await
        {
            Ok(_) => Ok(CasOutcome::Applied),
            Err(SdkError::ServiceError(error)) if error.raw().status().as_u16() == 412 => {
                Ok(CasOutcome::Rejected)
            }
            Err(error) => Err(anyhow::anyhow!(error).context(format!(
                "conditional write s3://{}/{key} may have committed",
                self.bucket
            ))),
        }
    }

    pub async fn cas_owner(
        &self,
        cell: &str,
        guard: CasGuard,
        epoch: u64,
    ) -> anyhow::Result<CasOutcome> {
        let key = format!("cells/{cell}/own.json");
        let body = ByteStream::from(serde_json::to_vec(&OwnerWire {
            node: &self.node,
            epoch,
        })?);
        let request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(body);
        let request = match guard {
            CasGuard::Absent => request.if_none_match("*"),
            CasGuard::Match(etag) => request.if_match(etag),
        };
        match request.send().await {
            Ok(_) => Ok(CasOutcome::Applied),
            Err(SdkError::ServiceError(error)) if error.raw().status().as_u16() == 412 => {
                Ok(CasOutcome::Rejected)
            }
            Err(error) => Err(anyhow::anyhow!(error).context(format!(
                "conditional write s3://{}/{key} may have committed",
                self.bucket
            ))),
        }
    }

    pub async fn cas_node_lease(
        &self,
        guard: CasGuard,
        record: &NodeLeaseRecord,
    ) -> anyhow::Result<LeaseCasOutcome> {
        if self.probe_public_key.is_empty() {
            return Err(anyhow::anyhow!(
                "refusing to publish a node lease without a signed-probe key"
            ));
        }
        let key = format!("nodes/{}.json", self.node);
        let body = ByteStream::from(serde_json::to_vec(&NodeLeaseWire {
            node: record.node.clone(),
            expires_ms: record.expires_ms,
            addr: record.addr.clone(),
            probe_public_key: self.probe_public_key.clone(),
            peer_protocol: record.peer_protocol,
            generation: record.generation.clone(),
            load: process_load(&self.live),
        })?);
        let request = self
            .lease_client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(body);
        let request = match guard {
            CasGuard::Absent => request.if_none_match("*"),
            CasGuard::Match(etag) => request.if_match(etag),
        };
        match request.send().await {
            Ok(output) => Ok(LeaseCasOutcome::Applied {
                etag: output.e_tag().unwrap_or_default().to_string(),
            }),
            Err(SdkError::ServiceError(error)) if error.raw().status().as_u16() == 412 => {
                Ok(LeaseCasOutcome::Rejected)
            }
            Err(error) => Err(anyhow::anyhow!(error).context(format!(
                "conditional write s3://{}/{key} may have committed",
                self.bucket
            ))),
        }
    }

    async fn read_json<T: for<'de> Deserialize<'de>>(
        &self,
        key: &str,
    ) -> anyhow::Result<Option<(T, String)>> {
        self.read_json_with(&self.client, key).await
    }

    async fn read_json_with<T: for<'de> Deserialize<'de>>(
        &self,
        client: &Client,
        key: &str,
    ) -> anyhow::Result<Option<(T, String)>> {
        match client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(output) => {
                let etag = output.e_tag().unwrap_or_default().to_string();
                let bytes = output
                    .body
                    .collect()
                    .await
                    .with_context(|| format!("read body s3://{}/{key}", self.bucket))?
                    .into_bytes();
                let value = serde_json::from_slice(&bytes)
                    .with_context(|| format!("decode s3://{}/{key}", self.bucket))?;
                Ok(Some((value, etag)))
            }
            Err(SdkError::ServiceError(error)) if error.err().is_no_such_key() => Ok(None),
            Err(error) => {
                Err(anyhow::anyhow!(error).context(format!("read s3://{}/{key}", self.bucket)))
            }
        }
    }
}

fn process_load(live: &LiveLoad) -> NodeLoadWire {
    #[cfg(target_os = "linux")]
    let rss_bytes = std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|statm| statm.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map(|pages| pages.saturating_mul(4_096))
        .unwrap_or(1);
    #[cfg(not(target_os = "linux"))]
    let rss_bytes = 1;

    #[cfg(target_os = "linux")]
    let open_fds = std::fs::read_dir("/proc/self/fd")
        .map(|entries| entries.count() as u64)
        .unwrap_or_default();
    #[cfg(not(target_os = "linux"))]
    let open_fds = 0;

    #[cfg(unix)]
    let fd_limit = {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } == 0 {
            limit.rlim_cur
        } else {
            0
        }
    };
    #[cfg(not(unix))]
    let fd_limit = 0;

    NodeLoadWire {
        sampled_ms: now_ms(),
        rss_bytes,
        open_fds,
        fd_limit,
        cpu_percent_x100: live.cpu_percent_x100.load(Ordering::Relaxed),
        resident_cells: live.resident_cells.load(Ordering::Relaxed),
        host_websockets: live.host_websockets.load(Ordering::Relaxed),
        pressured: live.pressured.load(Ordering::Relaxed),
        shed_cells: live.shed_cells.load(Ordering::Relaxed),
    }
}
