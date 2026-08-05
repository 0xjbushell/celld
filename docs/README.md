# celld documentation

celld is a stateful distributed system. It runs server-side JavaScript on
your machines and keeps all shared data in an S3-compatible bucket that
you own. The JavaScript API is the same API that Cloudflare Workers and
Durable Objects supply.

In Cloudflare terms, a cell is a Durable Object: a small server with a
name and a private SQLite database. You make one cell for each user, each
document, each chat room, or each AI agent. A cell serves HTTP, holds
WebSocket connections, sets alarms, and makes outbound connections. Each
cell runs on one thread, so two requests to the same cell never run at
the same instant. A second request can interleave only while the first
awaits, and storage operations are synchronous, so a storage operation
never interleaves at all. The data in a cell therefore stays consistent.
Cells share no database, and the application divides into cells from the
start.

An idle cell hibernates to the bucket, where it is only an object in S3
and costs almost zero. A resident cell is in memory. One 8 GB node holds
1,000 resident cells, so one resident cell costs approximately $0.05 each
month.

The bucket is the coordinator. There is no membership protocol, no
failure detector, and no consensus service. One atomic write to the
bucket gives a node the ownership of a cell. Replication sends the SQLite
data of each cell to the bucket, and celld does not acknowledge a write
before the data is there (RPO=0). The loss of a node therefore cannot
lose an acknowledged write. To add a node to the fleet, point the node at
the bucket.

## Contents

- [Install](#install)
- [Configure object storage](#configure-object-storage)
- [Deploy an application](#deploy-an-application)
- [Start a node](#start-a-node)
- [Add nodes](#add-nodes)
- [Diagnose a fleet](#diagnose-a-fleet)
- [Environment variables](#environment-variables)
- [Cloudflare compatibility](cloudflare-compat.md)
- [limitations](limitations.md)
- [Security](security.md)
- [Testing](testing.md)

## Install

The installer downloads the `celld` binary. Replication occurs in the
celld process. A node does not need an external replicator. If your
project contains Worker code, `celld deploy` needs esbuild. An asset-only
project does not need esbuild.

```sh
curl -fsSL https://celld.dev/install.sh | sh
```

If the installer tells you, add `~/.local/bin` to `PATH`. To install one
exact release, set `CELLD_VERSION` to the tag of that release, for example
`v0.0.1`. To go back to a previous release, run the installer again with
the tag of that release. The releases are on
[GitHub](https://github.com/denoland/celld/releases). Each release has a
GitHub Actions build attestation. To make sure that a downloaded file is
correct, run `gh attestation verify <asset> --repo denoland/celld`.

## Configure object storage

celld uses the standard AWS credential chain. For Cloudflare R2, do these
steps. Create a bucket. Create an S3 API token that has access to that
bucket. Then set these variables:

```sh
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=auto
export S3_ENDPOINT=https://ACCOUNT_ID.r2.cloudflarestorage.com
export CELLD_BUCKET=s3://YOUR-BUCKET
```

The bucket credentials give full control of the fleet. Keep them safe. The
bucket contains the deployments, the SQLite replicas, the ownership
records, the node leases, and the peer-authentication secret.

## Deploy an application

If the project contains Worker code, install `esbuild` on `PATH`. Then run
`celld deploy` from an applicable Wrangler project:

```sh
git clone https://github.com/denoland/celld
cd celld/examples/counter
celld deploy . \
  --bucket "$CELLD_BUCKET" \
  --endpoint "$S3_ENDPOINT" \
  --region "$AWS_REGION"
```

`celld deploy` accepts module Workers, Durable Object bindings, and static
assets. An asset project can include a Worker or be asset-only. The asset
functions include the assets binding, HTML handling, not-found handling,
worker-first routes, `_headers`, and `_redirects`. If the Wrangler
configuration contains an unknown key, the deploy stops with an error. See
the [limitations](limitations.md) for the current deployment boundary.

## Start a node

For local development, the default listener is sufficient:

```sh
celld \
  --bucket "$CELLD_BUCKET" \
  --endpoint "$S3_ENDPOINT" \
  --region "$AWS_REGION"
```

For a fleet node, bind the service. Advertise an address that the other
nodes and the ingress can reach:

```sh
celld \
  --bucket "$CELLD_BUCKET" \
  --endpoint "$S3_ENDPOINT" \
  --region "$AWS_REGION" \
  --listen 0.0.0.0:8080 \
  --advertise node-a.internal:8080
```

## Add nodes

Start each node with the same bucket settings. Give each node a different
`--advertise` address that the other nodes can reach. The nodes find each
other through the leases in the bucket. There is no join command and no
fixed membership list.

The bucket supplies discovery and authority. The bucket does not supply
network reachability. The peer HTTP protocol has a version, a body
signature, an HMAC, a clock limit, and replay protection. celld does not
terminate TLS. Put the advertised addresses on a private network that you
trust, or on an encrypted overlay such as WireGuard or Tailscale. Do not
show the peer port to the public internet.

## Diagnose a fleet

`celld diagnose` reads the node leases in the bucket. Then it sends a
probe to each live peer. It does not get a lease. It does not change
ownership.

```sh
celld diagnose \
  --bucket "$CELLD_BUCKET" \
  --endpoint "$S3_ENDPOINT" \
  --region "$AWS_REGION"
```

To probe only some nodes, use `--peer NODE_ID` one or more times. The
report identifies expired records, unsafe or incorrect advertised
addresses, peers that it cannot reach, authentication failures, and
protocol versions that do not agree.

## Environment variables

For the full list, run `celld -h`. This table shows the primary settings:

| variable | purpose |
| --- | --- |
| `CELLD_BUCKET` | The fleet bucket. The same as `--bucket` |
| `S3_ENDPOINT` | The S3-compatible endpoint. The same as `--endpoint` |
| `AWS_REGION`, `AWS_DEFAULT_REGION` | The storage region |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | Explicit AWS credentials. The standard AWS credential chain is also available |
| `CELLD_ADDR` | The listener. The same as `--listen` |
| `CELLD_ADVERTISE` | The address that peers can reach. The same as `--advertise` |
| `CELLD_UNSAFE_PUBLIC_ADVERTISE` | Set to `on` to permit a public peer IP |
| `CELLD_NODE` | An explicit node-session ID |
| `CELLD_WATCH` | The local work directory for SQLite and replication |
| `CELLD_ESBUILD` | The path of the esbuild executable |
| `CELLD_WORKERS` | The size of the stateless Worker pool (default: 16) |
| `CELLD_ACTIVATIONS` | The limit for concurrent cold-cell activations (default: the Worker count or 128, the smaller value) |
| `CELLD_WORKER_LOADER` | Bind a Worker Loader (Code Mode) at this `env` name. A Worker can then start isolates at runtime. Off unless set (experimental) |
| `CELLD_MAX_LOADED_WORKERS` | The limit for concurrent loaded workers (default: 256) |
| `CELLD_MAX_RESIDENT_CELLS` | The hard limit for resident cells, enforced at admission |
| `CELLD_MAX_RSS_MB` | The memory threshold for pressure shedding (default: 80% of the available memory; 0 disables it) |
| `CELLD_MAX_CPU_PERCENT` | The CPU threshold for pressure shedding (off unless set) |
| `CELLD_VAR_*`, `CELLD_VARS_FILE` | Worker variable overrides |
| `RUST_LOG` | The runtime log filter |

The help output also shows the advanced tuning switches and their
defaults.
