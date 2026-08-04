//! Compatibility garbage collection for dead celld process generations.
//!
//! Wake entries and lazy ownership takeover provide serving correctness.
//! This adapter retires the historical `node-cells/` index debris and expired
//! node-session records left in fleet buckets shared with celld.

use anyhow::Context as _;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::Client;
use futures_util::stream::{self, StreamExt as _};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use tracing::{info, warn};

const MARKER_GC_CONCURRENCY: usize = 64;

#[derive(Deserialize)]
struct NodeWire {
    node: String,
    expires_ms: u64,
    #[serde(default, rename = "ownership_index_generation")]
    generation: String,
}

#[derive(Clone, Debug)]
struct DeadNode {
    node: String,
    generation: String,
}

#[derive(Default)]
struct IndexedOwnership {
    markers: Vec<String>,
}

#[derive(Default)]
struct MarkerGcSummary {
    markers: usize,
    retired: usize,
    failures: usize,
}

/// Process-local retry state for compatibility GC.
///
/// Durable marker deletions survive cancellation. `swept` avoids repeating a
/// fleet-wide marker scan when only conditional retirement of the node record
/// remains, while `retries` bounds a persistently failing marker store.
#[derive(Default)]
pub struct DeadNodeGc {
    swept: HashSet<String>,
    retries: HashMap<String, (String, u32, Instant)>,
}

impl DeadNodeGc {
    /// Run one pass while renewing the advisory fleet-waker role. No task is
    /// spawned: the caller's existing wake-loop future polls both work and
    /// renewal, and dropping a lost-role pass cancels remaining I/O.
    pub async fn run_elected_pass(
        &mut self,
        client: &Client,
        bucket: &str,
        node: &str,
        tick_ms: u64,
    ) {
        let lease_ttl_ms = tick_ms.saturating_mul(3).min(i64::MAX as u64);
        if !crate::wake::try_hold_waker(
            client,
            bucket,
            node,
            crate::ownership_store::now_ms() as i64,
            lease_ttl_ms as i64,
        )
        .await
        {
            return;
        }

        let renew_ms = (lease_ttl_ms / 3).max(1);
        let mut renewal = tokio::time::interval(Duration::from_millis(renew_ms));
        renewal.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        renewal.tick().await;
        let pass = self.run_pass(client, bucket, tick_ms);
        tokio::pin!(pass);
        loop {
            tokio::select! {
                _ = &mut pass => return,
                _ = renewal.tick() => {
                    if !crate::wake::try_hold_waker(
                        client,
                        bucket,
                        node,
                        crate::ownership_store::now_ms() as i64,
                        lease_ttl_ms as i64,
                    ).await {
                        warn!("waker lease renewal failed; cancelling dead-node GC");
                        return;
                    }
                }
            }
        }
    }

    async fn run_pass(&mut self, client: &Client, bucket: &str, tick_ms: u64) {
        let observed_ms = crate::ownership_store::now_ms();
        let dead = dead_nodes(client, bucket, observed_ms).await;
        let dead_names: HashSet<&str> = dead.iter().map(|record| record.node.as_str()).collect();
        self.swept.retain(|node| dead_names.contains(node.as_str()));
        self.retries
            .retain(|node, _| dead_names.contains(node.as_str()));

        let retry_now = Instant::now();
        let indexed: HashMap<String, String> = dead
            .iter()
            .filter(|record| {
                !self.swept.contains(&record.node)
                    && self
                        .retries
                        .get(&record.node)
                        .is_none_or(|(generation, _, retry_at)| {
                            generation != &record.generation || *retry_at <= retry_now
                        })
                    && !record.generation.is_empty()
            })
            .map(|record| (record.node.clone(), record.generation.clone()))
            .collect();

        let indexed = if indexed.is_empty() {
            Some(HashMap::new())
        } else {
            match cells_indexed_by_nodes(client, bucket, &indexed).await {
                Ok(indexed) => Some(indexed),
                Err(error) => {
                    warn!(%error, nodes = indexed.len(), "dead-node marker scan failed");
                    None
                }
            }
        };
        let mut summaries = match indexed {
            Some(indexed) => gc_markers(client, bucket, indexed).await,
            None => HashMap::new(),
        };

        for record in dead {
            let node = record.node;
            let generation = record.generation;
            if !self.swept.contains(&node) {
                let (markers, retired, failures, complete) = if generation.is_empty() {
                    (0, 0, 0, true)
                } else {
                    let Some(summary) = summaries.remove(&node) else {
                        continue;
                    };
                    (
                        summary.markers,
                        summary.retired,
                        summary.failures,
                        summary.failures == 0,
                    )
                };
                info!(
                    event = "dead_node_reconciliation",
                    %node,
                    markers,
                    retired,
                    failures,
                    complete,
                    "dead-node marker GC complete"
                );
                if !complete {
                    let failure_count = self
                        .retries
                        .get(&node)
                        .filter(|(retry_generation, _, _)| retry_generation == &generation)
                        .map_or(1, |(_, count, _)| count.saturating_add(1));
                    let retry_ms = celld_logic::dead_node_reconciliation::retry_delay_ms(
                        tick_ms,
                        failure_count,
                    );
                    self.retries.insert(
                        node,
                        (
                            generation,
                            failure_count,
                            Instant::now() + Duration::from_millis(retry_ms),
                        ),
                    );
                    continue;
                }
                self.retries.remove(&node);
                self.swept.insert(node.clone());
            }
            match retire_dead_node(client, bucket, &node, observed_ms).await {
                Ok(_) => {
                    self.swept.remove(&node);
                }
                Err(error) => warn!(%node, %error, "dead-node lease retirement failed"),
            }
        }
    }
}

async fn dead_nodes(client: &Client, bucket: &str, now_ms: u64) -> Vec<DeadNode> {
    let mut dead = Vec::new();
    let mut token = None;
    loop {
        let mut request = client.list_objects_v2().bucket(bucket).prefix("nodes/");
        if let Some(token) = &token {
            request = request.continuation_token(token);
        }
        let page = match request.send().await {
            Ok(page) => page,
            Err(error) => {
                warn!(%error, "dead-node scan list failed");
                break;
            }
        };
        for object in page.contents() {
            let Some(key) = object.key() else { continue };
            let Some(node) = key
                .strip_prefix("nodes/")
                .and_then(|value| value.strip_suffix(".json"))
            else {
                continue;
            };
            match read_node(client, bucket, key).await {
                Ok(Some((record, _)))
                    if celld_logic::dead_node_reconciliation::node_record_is_dead(
                        node,
                        &record.node,
                        record.expires_ms,
                        now_ms,
                    ) =>
                {
                    dead.push(DeadNode {
                        node: node.to_string(),
                        generation: record.generation,
                    });
                }
                Ok(_) => {}
                Err(error) => warn!(%node, %error, "dead-node scan read failed"),
            }
        }
        match page.next_continuation_token() {
            Some(next) => token = Some(next.to_string()),
            None => break,
        }
    }
    dead
}

async fn cells_indexed_by_nodes(
    client: &Client,
    bucket: &str,
    nodes: &HashMap<String, String>,
) -> anyhow::Result<HashMap<String, IndexedOwnership>> {
    let mut indexed: HashMap<String, IndexedOwnership> = nodes
        .keys()
        .cloned()
        .map(|node| (node, IndexedOwnership::default()))
        .collect();
    let mut token = None;
    loop {
        let mut request = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix("node-cells/");
        if let Some(token) = &token {
            request = request.continuation_token(token);
        }
        let page = request.send().await?;
        for object in page.contents() {
            let Some(key) = object.key() else { continue };
            let Some((node, _)) = celld_logic::dead_node_reconciliation::parse_marker_key(key)
            else {
                continue;
            };
            if let Some(entry) = indexed.get_mut(node) {
                entry.markers.push(key.to_string());
            }
        }
        match page.next_continuation_token() {
            Some(next) => token = Some(next.to_string()),
            None => break,
        }
    }
    Ok(indexed)
}

async fn gc_markers(
    client: &Client,
    bucket: &str,
    indexed: HashMap<String, IndexedOwnership>,
) -> HashMap<String, MarkerGcSummary> {
    let mut summaries = HashMap::new();
    let mut work = Vec::new();
    for (node, indexed) in indexed {
        summaries
            .entry(node.clone())
            .or_insert_with(|| MarkerGcSummary {
                markers: indexed.markers.len(),
                ..MarkerGcSummary::default()
            });
        for marker in indexed.markers {
            work.push((node.clone(), marker));
        }
    }
    let mut results = stream::iter(work)
        .map(|(node, marker)| async move {
            let result = client
                .delete_object()
                .bucket(bucket)
                .key(marker)
                .send()
                .await;
            (node, result)
        })
        .buffer_unordered(MARKER_GC_CONCURRENCY);
    while let Some((node, result)) = results.next().await {
        let summary = summaries.get_mut(&node).expect("work has a summary");
        match result {
            Ok(_) => summary.retired += 1,
            Err(error) => {
                if summary.failures == 0 {
                    warn!(%node, %error, "dead-node marker delete failed");
                }
                summary.failures += 1;
            }
        }
    }
    summaries
}

async fn retire_dead_node(
    client: &Client,
    bucket: &str,
    node: &str,
    now_ms: u64,
) -> anyhow::Result<bool> {
    let key = format!("nodes/{node}.json");
    let Some((record, etag)) = read_node(client, bucket, &key).await? else {
        return Ok(true);
    };
    if !celld_logic::dead_node_reconciliation::node_record_is_dead(
        node,
        &record.node,
        record.expires_ms,
        now_ms,
    ) {
        return Ok(false);
    }
    match client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .if_match(etag)
        .send()
        .await
    {
        Ok(_) => Ok(true),
        Err(SdkError::ServiceError(error)) if error.raw().status().as_u16() == 412 => Ok(false),
        Err(error) => Err(error.into()),
    }
}

async fn read_node(
    client: &Client,
    bucket: &str,
    key: &str,
) -> anyhow::Result<Option<(NodeWire, String)>> {
    match client.get_object().bucket(bucket).key(key).send().await {
        Ok(output) => {
            let etag = output.e_tag().unwrap_or_default().to_string();
            let bytes = output
                .body
                .collect()
                .await
                .with_context(|| format!("read body s3://{bucket}/{key}"))?
                .into_bytes();
            let record = serde_json::from_slice(&bytes)
                .with_context(|| format!("decode s3://{bucket}/{key}"))?;
            Ok(Some((record, etag)))
        }
        Err(SdkError::ServiceError(error)) if error.err().is_no_such_key() => Ok(None),
        Err(error) => Err(error.into()),
    }
}
