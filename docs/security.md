# Security

celld is an alpha. It is not hardened for hostile multi-tenant use, and
security fixes land on the latest release line only — older alpha builds do not
receive backports.

## Trust rests on your bucket

The fleet's S3-compatible bucket is its root of authority. Ownership of every
cell is a compare-and-swap lease recorded in that bucket, and the bucket also
holds deployments, cell state, node leases, and the shared peer-authentication
secret. Whoever holds the bucket credentials controls the fleet. Treat them as
administrator access: scope each credential to a single fleet bucket, and rotate
after any suspected exposure.

## Peers authenticate, but do not encrypt

Node-to-node requests are HMAC-authenticated, body-bound, clock-bounded, and
replay-protected, so a peer cannot be impersonated and a captured request cannot
be replayed. celld does not, however, terminate TLS on the peer protocol — that
traffic is plain HTTP. Advertise nodes only on a trusted private network or an
encrypted overlay such as WireGuard or Tailscale, and never expose the peer
listener directly to the public internet.

## Single-writer isolation

Each cell is a single-writer SQLite database owned by exactly one node at a
time, fenced by an ownership epoch so a node that has lost its lease cannot
corrupt state. There is no shared, multi-tenant scheduler or placement layer: a
fleet's blast radius is its own machines, network, and bucket — never another
tenant's workload — and a misbehaving cell can only affect its own database.

## The operator's responsibility

celld serves requests and coordinates ownership; it does not authenticate your
application's end users or terminate public TLS. Front your ingress with your
own authentication and TLS, keep peers on trusted networks, and keep the bucket
credentials secret. See [limitations](limitations.md) for the complete alpha
boundary.
