# Security

celld is an alpha. It is not safe for hostile multi-tenant use, and
security fixes go to the latest release only; older alpha builds do not
receive fixes.

## Trust starts at your bucket

The S3-compatible bucket is the root of authority for the fleet. The
ownership of each cell is a compare-and-swap lease in that bucket, which
also holds the deployments, the cell state, the node leases, and the
shared peer-authentication secret. The person who holds the bucket
credentials controls the fleet, so handle the credentials as
administrator access: give each credential the scope of one fleet bucket
only, and replace a credential if you think that others know it.

## Peers authenticate, but do not encrypt

Each node-to-node request has an HMAC, a body signature, a clock limit,
and replay protection, so a false peer cannot get access and a captured
request cannot be sent again. But celld does not terminate TLS on the
peer protocol: that traffic is plain HTTP. Advertise the nodes only on a
private network that you trust or on an encrypted overlay such as
WireGuard or Tailscale, and do not show the peer listener to the public
internet.

## Single-writer isolation

Each cell is a SQLite database with one writer: one node owns a cell at a
time, and an ownership epoch fences each cell, so a node that lost its
lease cannot damage the state. There is no shared multi-tenant scheduler
and no shared placement layer. The damage limit of a fleet is its own
machines, its own network, and its own bucket, never the workload of a
different tenant, and a defective cell can only touch its own database.

## The operator's responsibility

celld serves requests and coordinates ownership; it does not authenticate
the end users of your application, and it does not terminate public TLS.
Put your own authentication and TLS in front of your ingress, keep the
peers on networks that you trust, and keep the bucket credentials secret.
See the [limitations](limitations.md) for the full alpha boundary.
