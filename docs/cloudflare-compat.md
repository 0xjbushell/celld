# Cloudflare compatibility

celld runs the Workers runtime, with Durable Objects as the stateful
core: module Workers, fetch, JS RPC, service bindings, and static assets.
This page shows that Worker surface, API by API. celld does not run the
rest of the Cloudflare platform, and the scope rule is simple: if
Cloudflare builds a function on Durable Objects, celld can get that
function, and a function on a different primitive is out of scope.
Cloudflare builds D1 on Durable Objects, so a D1 binding is a thin layer
over what celld already has. KV is a global cache with eventual
consistency, and R2 is blob storage: different systems, not on the
roadmap.

A configuration or binding that is not available must fail loudly, at
deploy or at first use. A silent compatibility gap is a bug; the known
gaps have marks below.

## Services

| service | notes |
| --- | --- |
| **Workers** | Module Workers: `fetch`, JS RPC, service bindings, Durable Object bindings, `vars`. |
| **Durable Objects** | The stateful core. SQLite storage, alarms, inbound hibernatable WebSockets, outbound `ws:`/`wss:` WebSocket clients (constructor and `fetch()` upgrade), one writer for each cell, names as addresses, RPC methods on stubs. |
| **Static assets** | Immutable files, served from the fleet bucket: `assets.directory`, `binding`, `html_handling`, `not_found_handling`, `run_worker_first`, plus `_headers` and `_redirects`. An asset-only project deploys without a Worker. |
| **Worker Loader (Code Mode)** | Experimental. Bind a loader with `CELLD_WORKER_LOADER`. A Worker can then start sandboxed isolates at runtime. See [Dynamic Worker loading](#dynamic-worker-loading-code-mode). |

Planned: **D1** (a D1 database is a Durable Object with a SQL API; celld
already has the hard part), **Workflows** (durable execution over cells
and alarms), **Queues** (a Durable Object shape; if demand appears).

Not planned: **KV** (a different consistency model), **R2** (celld runs
*on* blob storage; celld does not provide blob storage; declared
`r2_buckets` bindings load, but each method throws), **Cache API**,
**Workers AI, Vectorize, Hyperdrive, Browser Rendering, Email** (managed
platform services; an experimental HTTP adapter for an AI binding exists
behind `CELLD_AI_URL`), **cron triggers, custom domains, TLS
termination** (platform surface; celld has its own durable alarms; put
TLS in your ingress proxy).

## Runtime APIs

Cloudflare's [Runtime APIs
index](https://developers.cloudflare.com/workers/runtime-apis/), category
by category:

| API | status |
| --- | --- |
| Fetch, Request, Response, Headers | **Yes.** Gaps: `Response.redirect()`, `Response.error()`, and the `cache` request option are missing. |
| Bindings (`env`) | **Yes** for Durable Objects, service bindings, `vars`, assets. Other binding types are out of scope (see Services). |
| Context (`ctx`) | **Yes**: `waitUntil`, `props`, `exports`. `passThroughOnException()` is accepted but has no effect. There is no CDN behind it. |
| Handlers | `fetch`, `alarm`, `webSocketMessage`/`Close`/`Error`, RPC methods. **No** `scheduled` (cron), `queue`, `tail`, or `email` handlers. |
| RPC | **Yes**, for most of the surface. See [RPC](#rpc). |
| Streams | **Yes.** This includes byte streams, BYOB readers, `tee`/`pipeTo`/`pipeThrough`, `IdentityTransformStream`, `FixedLengthStream`, and `CompressionStream`/`DecompressionStream`. Gap: `ReadableStream.from()`. |
| Encoding | **Yes**: `TextEncoder`/`TextDecoder` (legacy encodings included), encoder and decoder streams, `atob`/`btoa`. |
| WebSockets | **Yes**, inbound (hibernatable, with attachments) and outbound. Gap: auto-response (`WebSocketRequestResponsePair`) and `getTags()`. |
| Web Crypto | **Partial**: `digest`, HMAC sign and verify, AES-GCM, RSA-OAEP decrypt, Ed25519 and ECDSA-P256 sign, `getRandomValues`, `randomUUID`. Missing: `deriveKey`/`deriveBits`/`wrapKey`/`unwrapKey`, verify other than HMAC, `DigestStream`. An algorithm that is not available throws. |
| Web standards | **Yes**: `URL`, `URLSearchParams`, `URLPattern`, `AbortController`/`AbortSignal` (with `timeout()`, `any()`), `Blob`/`File`/`FormData`, `Event`/`EventTarget`, `DOMException`, `queueMicrotask`, `structuredClone` (not conformant on exotic types), `navigator.userAgent`. |
| WebAssembly | **Yes** (V8's own, without restrictions). |
| Performance and timers | `setTimeout`/`clearTimeout`, `setImmediate`, `scheduler.wait()`. `setInterval` throws. `performance.now()` has millisecond resolution. The other parts of `performance` are stubs. |
| Console | `log`/`info`/`warn`/`error` are real. `debug`/`trace`/`group`/`table` do nothing. `assert`/`time`/`count` are absent. |
| Node.js compatibility | **Partial.** See [node: imports](#node-imports). |
| Cache (`caches`) | **No.** |
| HTMLRewriter | **No.** |
| TCP sockets (`cloudflare:sockets`) | **No.** Known silent gap: `connect()` currently gives an inert stub. It does not throw. |
| EventSource, MessageChannel, BroadcastChannel | **No.** The classes exist so that bundles load, but they do nothing. |

## RPC

celld implements the Workers [JS RPC
system](https://developers.cloudflare.com/workers/runtime-apis/rpc/):
`WorkerEntrypoint` and `RpcTarget` from `cloudflare:workers`, named
entrypoints on service bindings, and method calls on Durable Object stubs
(this needs `extends DurableObject`, or the `js_rpc` compat flag).
Arguments and returns use structured clone. Functions, streams, and
`RpcTarget`s become stubs. Promise pipelining, `ctx.exports` loopback
stubs, and stubs in DO storage are available.

The current limits: a cross-isolate service binding with a named
entrypoint can do single method calls, but not `fetch()`, awaitable
properties, or pipelined paths; a same-isolate binding has the full
surface.

## Dynamic Worker loading (Code Mode)

Set `CELLD_WORKER_LOADER=LOADER`. Workers then get `env.LOADER`. This is
an experimental port of Cloudflare's [Worker
Loader](https://developers.cloudflare.com/workers/runtime-apis/bindings/worker-loader/).
`loader.get(name, getCode)` (memoized) and `loader.load(code)` start a
new isolate for each loaded worker. These inputs are honored:
`mainModule`, sibling `modules`, `compatibilityDate`/`Flags`, plain-JSON
`env`, and `globalOutbound: null` (no egress). The limits of workerd
apply: 64 MiB of code and 1 MiB of env, plus the
`CELLD_MAX_LOADED_WORKERS` limit. A loaded worker serves `fetch()` and
single RPC method calls. Not yet available: `globalOutbound` as a
Fetcher, capability stubs in `env`, awaitable or pipelined properties.

## node: imports

`node:` specifiers are always available; the `nodejs_compat` flag is not
necessary, and celld does not read it. `celld deploy` externalizes
`node:*` at bundle time. The runtime supplies its own subset (it does not
use the Wrangler-style unenv polyfills):

- **Implemented**: `node:assert`, `node:async_hooks` (a real
  `AsyncLocalStorage`), `node:buffer`, `node:events`, `node:path`,
  `node:stream` (+ `stream/web`, `stream/promises`, `stream/consumers`),
  `node:timers/promises`, `node:util`.
- **Partial**: `node:crypto` (hashes, HMAC, HKDF, PBKDF2, key objects,
  `webcrypto`; signatures, ciphers, and DH throw), `node:zlib` (only the
  sync `gzip`/`deflate` family), `node:fs` (reads fail with `ENOENT`,
  `existsSync` is `false`).
- **Not implemented**: the rest — `node:http(s)`, `node:net`,
  `node:tls`, `node:dns`, `node:os`, `node:process` (the `process`
  global exists, the module does not), `node:worker_threads`, `node:vm`,
  `node:child_process`, and the others. Known silent gap: these
  currently give inert stubs. They do not fail the import.

## Compatibility flags

`compatibility_date` and `compatibility_flags` are honored for the
switches that celld models: `delete_all_deletes_alarm`, `js_rpc`,
`fetcher_no_get_put_delete`, `websocket_standard_binary_type`, and the
assets navigation behavior. `Cloudflare.compatibilityFlags` reports only
the flags that celld honors. A flag that celld does not model is absent
rather than reported as enabled, and celld accepts it without effect.

## Wrangler configuration

`celld deploy` builds a standard Wrangler project (esbuild on `PATH`)
and accepts `wrangler.jsonc` or `wrangler.json`, not `wrangler.toml`.
The available config keys are `name`, `main`,
`compatibility_date`, `compatibility_flags`, `durable_objects`,
`migrations`, `assets`, `services`, and `vars`. An asset-only project can
omit `main`. celld refuses symlinks and special files in the asset
directory, and `.assetsignore` still needs Wrangler. Each other key —
`routes`, `kv_namespaces`, `triggers`, and the rest — stops the deploy
with an error that names the key: remove the key, or deploy that project
with Wrangler.

This page is the reference for the implemented Worker surface. For the
operational boundaries of the current release — TLS, platforms, pressure
shedding, updates — see the [limitations](limitations.md).
