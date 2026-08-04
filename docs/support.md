# Support matrix

celld is a self-supported open-source alpha. The latest published build is
the only supported release line; security and correctness fixes are not
backported to older alpha commits.

## Prebuilt platforms

| target | release status | minimum tested environment |
| --- | --- | --- |
| `x86_64-unknown-linux-gnu` | primary | Ubuntu 22.04 / glibc 2.35 |
| `aarch64-unknown-linux-gnu` | primary | Ubuntu 22.04 / glibc 2.35 |
| `aarch64-apple-darwin` | development | macOS 14 |

Linux is the production target; macOS binaries are for local development. The
default in-process replicator (`celld-ltx`) confirms replication on macOS, so
durability, hibernation, and failover work there. The one macOS caveat applies
only if you opt into the external Litestream backend
(`CELLD_REPLICATOR=litestream`): Litestream's live replication on macOS does not
provide the replica confirmation celld needs, so in that mode celld keeps
affected cells resident rather than claim durability it cannot verify.

Other operating systems, Intel macOS, musl Linux, 32-bit systems, and Windows
do not have prebuilt artifacts. Building from source on them is experimental.

## Supported topology

- One node is complete.
- Two or more nodes are supported when every peer is directly reachable on a
  trusted private network or encrypted overlay.
- All nodes in a fleet use the same application bucket and compatible celld
  versions. Replication is in-process by default; a fleet that opts into the
  Litestream backend pins the same Litestream runtime across nodes.
- Cloudflare R2 is the release-tested object store. MinIO is used in
  conformance and local testing. Other S3-compatible stores must provide the
  conditional-write semantics celld requires and are not yet release-tested.

For bugs and operational questions, include `celld --version`, the target
triple, object-store implementation, topology, and a redacted `celld diagnose`
report in an email to [ry@deno.com](mailto:ry@deno.com). Security reports use
the private process in the [security policy](security.md).
