# Limitations

The boundaries of the current alpha:

- A fleet runs one application deployment. There is no multi-tenant
  scheduler, no account service, no managed ingress, and no global
  placement layer.
- The peer HTTP protocol does not terminate TLS. Run the fleet on a
  private network or an encrypted overlay, and terminate public TLS in an
  ingress proxy.
- The fleet bucket is the administrative authority, so give its
  credentials a narrow scope; celld does not make shared object-store
  credentials safe.
- The bucket credentials come from the `AWS_*` environment or from
  explicit managed credentials, which includes instance metadata and web
  identity tokens. celld does not read `~/.aws` profiles or SSO logins.
- The [Cloudflare compatibility](cloudflare-compat.md) page shows what
  celld runs of the Workers platform: the available APIs, the deploy
  contract, and what is out of scope (KV, R2, `wrangler.toml`, routes).
  An unknown key or API causes a loud failure; a silent gap is a bug.
- Each node can be the WebSocket ingress for each cell through the signed
  peer tunnel, but the test coverage for close codes and reconnections
  across nodes is thinner than for one node. If latency is important,
  send the traffic of a cell to its owner node.
- An outbound Durable Object WebSocket keeps its cell resident, and the
  connection does not continue when the cell moves to a different node:
  keep the connection intent in storage, and connect again after the
  activation. (A Worker's outbound socket stops with its request, the
  same as on Cloudflare.) A node limits how much residency the outbound
  sockets can hold.
- Windows is not available, and Intel Macs get no prebuilt binaries; a
  build from source works.
- Pressure shedding is off until the release measurements give safe
  defaults. A new node receives cells through the usual traffic; there is
  no central placement controller.
- Updates are manual: the installer keeps immutable releases behind one
  `current` pointer, so a previous SHA is the rollback. There is no
  automatic update agent.
