# celld-ltx

celld's in-process SQLite-to-object-store replication engine. It reads each
cell's SQLite WAL, writes LTX files to a fleet bucket, restores a database from
that bucket, and — as it grows — exposes the replicated position so celld can
gate a write's response until it is durable. This is the foundation of celld's
durability goal: RPO=0 for every cell write, matching Durable Objects.

## Provenance and attribution

Seeded on 2026-08-03 from a read-only snapshot of rustyriver
(https://github.com/mikenomitch/rustyriver), a from-scratch Rust
reimplementation of Litestream v0.5 and the LTX file format. celld owns and
evolves this snapshot as first-class celld source; it does not track an
upstream branch.

Attribution for the vendored and ported work:

- **rustyriver** — Copyright 2026 The rustyriver authors, licensed under the
  Apache License, Version 2.0.
- **Litestream** (https://github.com/benbjohnson/litestream), pinned by celld
  to tag v0.5.11 — Copyright (c) Ben Johnson and the Litestream authors,
  licensed under the Apache License, Version 2.0. Replication behavior and test
  vectors are ported for wire-compatible interoperability.
- **LTX file format and reference implementation**
  (https://github.com/superfly/ltx), tag v0.5.1 — Copyright (c) Superfly, Inc.,
  licensed under the Apache License, Version 2.0.

The full Apache License, Version 2.0 text is in [LICENSE](LICENSE).

## Tests

The suite ports the upstream conformance vectors. `differential_xtool` checks
LTX read and write byte-for-byte against the real Litestream binary in three
directions; it self-skips when the binary is absent, so the fast CI stays green
while the release gate (which builds the pinned Litestream) runs the real
oracle.
