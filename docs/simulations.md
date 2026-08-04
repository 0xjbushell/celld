# Simulations

Confidence in celld is built up in several ways. This page describes one of
them: end-to-end simulations on live fleets, where real nodes coordinate
through a real object store under real workloads, and we observe what actually
happens when things go wrong.

Simulations are empirical evidence at a scale and realism that reasoning alone
cannot reach. Running the whole system on many machines, with real storage
latency and real network partitions, and then breaking it deliberately, shows
whether celld's guarantees hold in practice and not only by design. This page
records those runs in broad strokes and grows as we run more of them, across
larger fleets, denser cell populations, longer partitions, and more varied
failure modes.

Every result shares the same substrate: a fleet of celld nodes coordinating
only through one S3-compatible bucket, one SQLite database per Durable Object,
synchronous durability — a write is replicated to the bucket before its response
is released, so an acknowledged write is durable (RPO=0) — and a single
authoritative writer per cell fenced by an epoch in the object-store path. Each scenario attacks that substrate at a
different seam. Faults are injected between verification passes that fetch cells
through arbitrary nodes and check their durable state exactly — status, body,
and full message ledger — so every run has a clean before-and-after. A cell may
go briefly unavailable while ownership moves, but its committed state must
survive intact and be served again from a live node.

## What survives

**A hard process crash loses no acknowledged write.** Kill a node's process
outright — even mid-write-stream — and its resident cells are orphaned instantly.
Because the output gate holds each write's response until it is durable, every
acknowledged write is already in the bucket: a surviving node re-acquires each
cell and serves it from replicated state, at a bounded time-to-availability
measured as a distribution.

The direct test is deliberately adversarial: issue writes back to back, `SIGKILL`
the process mid-stream, then *delete the node's local database* so recovery can
only come from the bucket, and cold-restore the cell. Twenty writes acknowledged
and killed this way recover all twenty; an eight-cell run of 156 acknowledged
writes recovers all 156. **Zero acknowledged writes lost, across every run.**
Before the output gate, the same kill dropped the recently-acknowledged writes
that had not yet replicated — which is exactly what the gate is there to prevent,
and exactly what these runs check for on every fault.

**A returning partitioned owner cannot corrupt state.** Freeze a node so it
still believes it owns its cells, write to those same cells through peers, then
thaw it. It discovers its lease has moved and refuses to serve stale data
rather than resurrect it. The peer writes land exactly once, the original state
is intact, and ledgers stay consistent — at most one writer per epoch, enforced
through the bucket, even against an owner that comes back to life.

**A node that cannot replicate stops owning.** Cut a node off from the bucket so
it can no longer prove durability, and it self-fences rather than accept writes
it cannot replicate. "I can't replicate" is treated as "I must stop owning," not
as a degraded-but-serving state.

**Losing a whole host is a non-event for the data.** Halt an entire node at the
provider level, mid-workload, and its cells move to survivors; when the host
returns it rejoins cleanly with no duplicate residency.

Across these scenarios, no committed cell state has been lost or corrupted, and
between every fault the verification sweeps return zero body, status, or
missing-message failures.

## A few numbers we trust

Confidence is also quantitative. These are the simplest measurements an engineer
tends to ask for first; each carries the condition it was taken under, because a
number without its conditions is a rumor. Fuller ledgers live in the project's
spec sheet.

- **A warm resident request is local.** A request to an already-resident Durable
  Object does zero bucket operations and returns in **p50 ~1.1 ms, p99 ~7 ms**
  (fixed-host measurement). Only cold activation pays for object storage.
- **A durable write costs one bucket round-trip.** That round-trip *is* what
  RPO=0 means — the response is held until the write is in the bucket — so the
  single-write latency floor is the storage round-trip, not something the engine
  can optimize away. Concurrent writes to one cell batch onto a shared upload, so
  per-cell throughput is not one-round-trip-per-write.
- **Ten small nodes held real scale.** A ten-node fleet of 4-vCPU/8-GB machines
  carried **10,000 resident cells and 20,000 concurrent WebSockets**, and after
  halting two of the ten, every cell's data was available again on a survivor
  within ~11 s at the tail (with reserve headroom).

## The failure edges we intend to find

Confidence also means knowing where a policy stops holding, so those edges are
recorded rather than tuned away. The clearest one is reserve headroom: a fleet
packed to its hard resident ceiling has nowhere to place cells displaced by a
node loss, so simultaneous multi-node failure at the ceiling degrades where a
fleet with headroom does not. Measuring both sides of that line — healthy
restoration with reserve, and the red case without — is part of the work, not
an embarrassment to hide.
