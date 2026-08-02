# Support matrix

celld v0.0.1 is a self-supported open-source alpha. The latest published
v0.0.1 build is the only supported release line; security and correctness
fixes are not backported to older alpha commits.

## Prebuilt platforms

| target | release status | minimum tested environment |
| --- | --- | --- |
| `x86_64-unknown-linux-gnu` | primary | Ubuntu 22.04 / glibc 2.35 |
| `aarch64-unknown-linux-gnu` | primary | Ubuntu 22.04 / glibc 2.35 |
| `aarch64-apple-darwin` | development | macOS 14 |

Linux is the production target for v0.0.1. macOS binaries are useful for local
development, but Litestream live replication on macOS does not yet provide the
replica confirmation required for reliable hibernation and failover. celld
therefore keeps affected cells resident rather than claiming durability it
cannot verify.

Other operating systems, Intel macOS, musl Linux, 32-bit systems, and Windows
do not have prebuilt artifacts. Building from source on them is experimental.

## Supported topology

- One node is complete.
- Two or more nodes are supported when every peer is directly reachable on a
  trusted private network or encrypted overlay.
- All nodes in a fleet use the same application bucket, compatible celld
  versions, and pinned Litestream runtime.
- Cloudflare R2 is the release-tested object store. MinIO is used in
  conformance and local testing. Other S3-compatible stores must provide the
  conditional-write semantics celld requires and are not yet release-tested.

For bugs and operational questions, include `celld --version`, the target
triple, object-store implementation, topology, and a redacted `celld diagnose`
report in an email to [ry@deno.com](mailto:ry@deno.com). Security reports use
the private process in the [security policy](security.md).
