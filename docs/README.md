# celld documentation

celld runs Cloudflare Workers and Durable Objects on your own machines.
Each Durable Object is its own SQLite database with a single writer, addressed
by name and replicated to an S3-compatible bucket you own. Writes are durable
before they are acknowledged (RPO=0, like Durable Objects): a synchronous output
gate holds a write's response until it is replicated, so losing a node cannot
lose an acknowledged write. A fleet of celld nodes coordinates through that
bucket alone — no control plane, no membership service, no consensus.

Because state is partitioned into one small database per object from the first
line of code, applications shard by construction. The contention, hot spots,
and blast-radius failures of a single shared database are designed out rather
than managed: a runaway object can only affect its own cell.

## Contents

- [Install](#install)
- [Configure object storage](#configure-object-storage)
- [Deploy an application](#deploy-an-application)
- [Start a node](#start-a-node)
- [Add nodes](#add-nodes)
- [Diagnose a fleet](#diagnose-a-fleet)
- [Environment variables](#environment-variables)
- [Cloudflare compatibility](cloudflare-compat.md)
- [Support matrix](support.md)
- [limitations](limitations.md)
- [Security](security.md)
- [Simulations](simulations.md)

## Install

The installer downloads the `celld` binary. Replication is in-process, so a node
needs no external replicator by default; the optional Litestream backend
(`CELLD_REPLICATOR=litestream`) needs [Litestream](https://litestream.io) on
`PATH` (or `LITESTREAM_BIN`). Worker projects deployed with `celld deploy` need
esbuild; asset-only projects do not:

```sh
curl -fsSL https://celld.dev/install.sh | sh
```

Put `~/.local/bin` on `PATH` if the installer asks you to. Install an exact
immutable release by setting `CELLD_VERSION` to its tag, such as `v0.0.1`.
Rerun the installer with a previous tag to roll back both binaries. Releases
are published on [GitHub](https://github.com/denoland/celld/releases) with
GitHub Actions build attestations; verify any downloaded asset with
`gh attestation verify <asset> --repo denoland/celld`.

## Configure object storage

celld uses the standard AWS credential chain. For Cloudflare R2, create a
bucket and an S3 API token with access to that bucket, then set:

```sh
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=auto
export S3_ENDPOINT=https://ACCOUNT_ID.r2.cloudflarestorage.com
export CELLD_BUCKET=s3://YOUR-BUCKET
```

Treat access to the fleet bucket and its credentials as fleet-administrator
access. The bucket contains deployments, SQLite replicas, ownership records,
node leases, and the peer-authentication secret.

## Deploy an application

Install `esbuild` on `PATH` when the project includes Worker code, then run
`celld deploy` from a supported Wrangler project:

```sh
git clone https://github.com/denoland/celld
cd celld/examples/counter
celld deploy . \
  --bucket "$CELLD_BUCKET" \
  --endpoint "$S3_ENDPOINT" \
  --region "$AWS_REGION"
```

`celld deploy` supports module Workers, Durable Object bindings, and
co-deployed or asset-only static assets. The asset subset includes the assets
binding, HTML and not-found handling, worker-first routes, `_headers`, and
`_redirects`. It refuses unknown Wrangler configuration rather than silently
dropping it. See the [limitations](limitations.md) for the current
deployment boundary.

## Start a node

For local development, the default listener is enough:

```sh
celld \
  --bucket "$CELLD_BUCKET" \
  --endpoint "$S3_ENDPOINT" \
  --region "$AWS_REGION"
```

For a reachable fleet node, bind the service and advertise the address other
nodes and ingress can actually reach:

```sh
celld \
  --bucket "$CELLD_BUCKET" \
  --endpoint "$S3_ENDPOINT" \
  --region "$AWS_REGION" \
  --listen 0.0.0.0:8080 \
  --advertise node-a.internal:8080
```

## Add nodes

Start every node with the same bucket settings and a distinct reachable
`--advertise` address. Nodes discover one another through bucket leases; there
is no join command and no fixed membership list.

The bucket provides discovery and authority, not network reachability.
Peer HTTP is versioned, body-bound, HMAC-authenticated, clock-bounded, and
replay-protected, but celld does not terminate TLS. Put advertised addresses
on a trusted private network or an encrypted overlay such as WireGuard or
Tailscale. Do not expose the peer port directly to the public internet.

## Diagnose a fleet

`celld diagnose` reads the bucket's node leases and directly probes every live
peer without acquiring a lease or changing ownership:

```sh
celld diagnose \
  --bucket "$CELLD_BUCKET" \
  --endpoint "$S3_ENDPOINT" \
  --region "$AWS_REGION"
```

Use `--peer NODE_ID` one or more times to restrict the probes. The report
distinguishes expired records, unsafe or malformed advertised addresses,
unreachable peers, authentication failures, and protocol incompatibility.

## Environment variables

`celld -h` is the authoritative compact list. The main settings are:

| variable | purpose |
| --- | --- |
| `CELLD_BUCKET` | Fleet bucket, equivalent to `--bucket` |
| `S3_ENDPOINT` | S3-compatible endpoint, equivalent to `--endpoint` |
| `AWS_REGION`, `AWS_DEFAULT_REGION` | Storage region |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | Explicit AWS credentials; the normal AWS credential chain is also supported |
| `CELLD_ADDR` | Listener, equivalent to `--listen` |
| `CELLD_ADVERTISE` | Peer-reachable address, equivalent to `--advertise` |
| `CELLD_UNSAFE_PUBLIC_ADVERTISE` | Permit a literal public peer IP when set to `on` |
| `CELLD_NODE` | Explicit node-session ID |
| `CELLD_WATCH` | Local SQLite and replication working directory |
| `CELLD_OUTPUT_GATE` | `0` disables the synchronous durability gate (RPO=0), trading it for lower write latency; on by default |
| `CELLD_REPLICATOR` | `litestream` selects the external Litestream backend; in-process replication otherwise |
| `LITESTREAM_BIN` | Litestream executable path (Litestream backend only) |
| `CELLD_ESBUILD` | esbuild executable path |
| `CELLD_WORKERS` | Stateless Worker pool size (default: 16) |
| `CELLD_ACTIVATIONS` | Concurrent cold-cell activation limit (default: the smaller of Worker count and 128) |
| `CELLD_WORKER_LOADER` | Bind a Worker Loader (Code Mode) at this `env` name so a Worker can spawn isolates at runtime; off unless set (experimental) |
| `CELLD_MAX_LOADED_WORKERS` | Cap on concurrent dynamically-loaded workers (default: 256) |
| `CELLD_MAX_RESIDENT_CELLS`, `CELLD_RESIDENT_LOW_WATER` | Resident-cell pressure thresholds |
| `CELLD_MAX_RSS_MB`, `CELLD_MAX_CPU_PERCENT` | Linux process-pressure thresholds |
| `CELLD_VAR_*`, `CELLD_VARS_FILE` | Worker variable overrides |
| `GOMEMLIMIT` | Litestream Go heap limit (default child: `512MiB`) |
| `RUST_LOG` | Runtime log filter |

The help output also names the advanced runtime tuning switches and their
defaults.
