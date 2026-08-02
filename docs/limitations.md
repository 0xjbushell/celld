# v0.0.1 limitations

celld v0.0.1 is an alpha of the standalone engine, not a drop-in replacement
for the entire Cloudflare Workers platform.

- A fleet runs one application deployment. There is no public multi-tenant
  scheduler, account service, managed ingress, or global placement layer.
- celld's peer HTTP does not terminate TLS. Use a private network or encrypted
  overlay and terminate public TLS in a separate ingress proxy.
- The fleet bucket is administrative authority. celld does not make shared or
  broadly scoped object-store credentials safe.
- `celld deploy` requires `esbuild` on `PATH` for Worker code, accepts
  `wrangler.json` or `wrangler.jsonc`, and deliberately rejects unsupported
  configuration instead of silently ignoring it. Static assets are supported,
  including asset-only projects; symlinks and special files are refused, and
  `.assetsignore` still requires Wrangler. `wrangler.toml`, routes, and vars
  remain outside the native deploy path.
- The implemented Worker surface focuses on modules, Durable Objects, SQLite
  storage, alarms, service bindings, fetch, and hibernatable WebSockets. It
  does not promise every Web API, Node API, Cloudflare binding, compatibility
  flag, or arbitrary untrusted tenant program. Which Cloudflare services are
  supported, planned, or out of scope is
  [cloudflare-compat](cloudflare-compat.md).
- WebSocket upgrades and messages route across nodes through the signed
  peer tunnel, so any node can serve as ingress for any cell. Close-code
  propagation and reconnect edge cases across nodes have thinner coverage
  than same-node WebSockets; latency-sensitive deployments still benefit
  from owner-sticky ingress.
- Outbound WebSockets work from both Workers and Durable Objects. A Worker
  socket lives and dies with its request, as it does on Cloudflare: when the
  request and its `waitUntil` work finish, the socket is closed. A Durable
  Object socket outlives individual events and keeps its cell resident for the
  life of the connection; it does not hibernate or preserve a live transport
  across node migration, so applications needing failover must persist
  connection intent and reconnect after activation. A node bounds how much of
  its residency outbound sockets may pin.
- R2 Worker bindings are not implemented. The S3-compatible fleet bucket is
  celld infrastructure, not an application R2 binding.
- Linux is the production target. See [support](support.md) for the macOS
  replication limitation and exact prebuilt targets.
- Pressure shedding is opt-in until release measurements establish safe
  defaults. A new node receives released cells through ordinary traffic; there
  is no central placement controller.
- Updates are explicit. There is no unattended update agent. The installer
  retains immutable releases and switches one `current` pointer, so installing
  an exact previous SHA performs a runtime-pair rollback.

If an unsupported deployment key or runtime API is encountered, celld should
fail loudly. Silent compatibility gaps are bugs.
