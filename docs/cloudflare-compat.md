# Cloudflare compatibility

celld runs the Workers runtime — module Workers, fetch, JS RPC, service
bindings, static assets — with Durable Objects as its stateful core. That
Worker surface is the bulk of this page, documented API by API below. What
celld does not run is the rest of the Cloudflare platform, and there the
scope rule is simple: **anything built on Durable Objects is in reach; a
fundamentally different primitive is out.** Cloudflare builds D1 on Durable
Objects, so a D1 binding is a thin layer over what celld already has. KV is
a globally-replicated eventually-consistent edge cache and R2 is blob
storage — different systems, not on the roadmap.

Unsupported configuration and bindings should fail loudly at deploy or at
first use. Silent compatibility gaps are bugs; the known ones are marked
below.

## Services

| service | notes |
| --- | --- |
| **Workers** | Module Workers: `fetch`, JS RPC, service bindings, Durable Object bindings, `vars`. |
| **Durable Objects** | The stateful core. SQLite storage, alarms, inbound hibernatable WebSockets, outbound `ws:`/`wss:` WebSocket clients (constructor and `fetch()` upgrade), single-writer semantics, addressed by name, RPC methods on stubs. |
| **Static assets** | Co-deployed immutable files, served from the fleet bucket: `assets.directory`, `binding`, `html_handling`, `not_found_handling`, `run_worker_first`, plus `_headers` and `_redirects`. Asset-only projects deploy without a Worker. |
| **Worker Loader (Code Mode)** | Experimental: bind a loader with `CELLD_WORKER_LOADER` and a Worker can spawn sandboxed isolates at runtime. See [Dynamic Worker loading](#dynamic-worker-loading-code-mode). |

Planned: **D1** (a D1 database is a Durable Object with a SQL API; celld
already has the hard part), **Workflows** (durable execution over cells and
alarms), **Queues** (DO-shaped; if demand appears).

Not planned: **KV** (a different consistency model), **R2** (blob storage is
what celld runs *on*, not what it provides — declared `r2_buckets` bindings
load, but every method throws), **Cache API**, **Workers AI, Vectorize,
Hyperdrive, Browser Rendering, Email** (managed platform services; an
experimental HTTP adapter for an AI binding exists behind `CELLD_AI_URL`),
**cron triggers, custom domains, TLS termination** (platform surface — celld
has its own durable alarms; put TLS in your ingress proxy).

## Runtime APIs

Cloudflare's [Runtime APIs
index](https://developers.cloudflare.com/workers/runtime-apis/), category by
category:

| API | status |
| --- | --- |
| Fetch, Request, Response, Headers | **Yes.** Gaps: `Response.redirect()`/`Response.error()` and the `cache` request option are missing. |
| Bindings (`env`) | **Yes** for Durable Objects, service bindings, `vars`, assets. Other binding types are out of scope (see Services). |
| Context (`ctx`) | **Yes**: `waitUntil`, `props`, `exports`. `passThroughOnException()` is accepted but has no effect — there is no CDN behind it. |
| Handlers | `fetch`, `alarm`, `webSocketMessage`/`Close`/`Error`, RPC methods. **No** `scheduled` (cron), `queue`, `tail`, or `email` handlers. |
| RPC | **Yes**, substantially — see [RPC](#rpc). |
| Streams | **Yes**, including byte streams, BYOB readers, `tee`/`pipeTo`/`pipeThrough`, `IdentityTransformStream`, `FixedLengthStream`, `CompressionStream`/`DecompressionStream`. Gap: `ReadableStream.from()`. |
| Encoding | **Yes**: `TextEncoder`/`TextDecoder` (legacy encodings included), encoder/decoder streams, `atob`/`btoa`. |
| WebSockets | **Yes**, inbound (hibernatable, with attachments) and outbound. Gap: auto-response (`WebSocketRequestResponsePair`) and `getTags()`. |
| Web Crypto | **Partial**: `digest`, HMAC sign/verify, AES-GCM, RSA-OAEP decrypt, Ed25519/ECDSA-P256 sign, `getRandomValues`, `randomUUID`. Missing: `deriveKey`/`deriveBits`/`wrapKey`/`unwrapKey`, non-HMAC `verify`, `DigestStream`. Unsupported algorithms throw. |
| Web standards | **Yes**: `URL`, `URLSearchParams`, `URLPattern`, `AbortController`/`AbortSignal` (incl. `timeout()`, `any()`), `Blob`/`File`/`FormData`, `Event`/`EventTarget`, `DOMException`, `queueMicrotask`, `structuredClone` (non-conformant on exotic types), `navigator.userAgent`. |
| WebAssembly | **Yes** (V8's own, unrestricted). |
| Performance and timers | `setTimeout`/`clearTimeout`, `setImmediate`, `scheduler.wait()`. `setInterval` throws. `performance.now()` has millisecond resolution and `performance` is otherwise a stub. |
| Console | `log`/`info`/`warn`/`error` are real; `debug`/`trace`/`group`/`table` are no-ops; `assert`/`time`/`count` are absent. |
| Node.js compatibility | **Partial** — see [node: imports](#node-imports). |
| Cache (`caches`) | **No.** |
| HTMLRewriter | **No.** |
| TCP sockets (`cloudflare:sockets`) | **No.** Known silent gap: `connect()` currently resolves to an inert stub instead of throwing. |
| EventSource, MessageChannel, BroadcastChannel | **No.** The classes exist so bundles load, but they are inert. |

## RPC

The Workers [JS RPC
system](https://developers.cloudflare.com/workers/runtime-apis/rpc/) is
implemented: `WorkerEntrypoint` and `RpcTarget` from `cloudflare:workers`,
named entrypoints on service bindings, arbitrary method calls on Durable
Object stubs (requires `extends DurableObject`, or the `js_rpc` compat
flag), structured-clone arguments and returns with stub lifting for
functions, streams, and `RpcTarget`s, promise pipelining, `ctx.exports`
loopback stubs, and stubs persisted in DO storage.

Current limits: a cross-isolate service binding with a named entrypoint
supports single method calls but not `fetch()`, awaitable properties, or
pipelined paths; same-isolate bindings support the full surface.

## Dynamic Worker loading (Code Mode)

Set `CELLD_WORKER_LOADER=LOADER` and Workers get `env.LOADER`, an
experimental port of Cloudflare's [Worker
Loader](https://developers.cloudflare.com/workers/runtime-apis/bindings/worker-loader/):
`loader.get(name, getCode)` (memoized) and `loader.load(code)` spawn a fresh
isolate per loaded worker; `mainModule`, sibling `modules`,
`compatibilityDate`/`Flags`, plain-JSON `env`, and `globalOutbound: null`
(deny egress) are honored, with workerd's 64 MiB code and 1 MiB env limits
and a `CELLD_MAX_LOADED_WORKERS` cap. Loaded workers serve `fetch()` and
single RPC method calls. Not yet: `globalOutbound` as a Fetcher, capability
stubs in `env`, awaitable or pipelined properties.

## node: imports

`node:` specifiers are always available — the `nodejs_compat` flag is not
required and not read. `celld deploy` externalizes `node:*` at bundle time
and the runtime provides its own subset (Wrangler-style unenv polyfills are
not used):

- **Implemented**: `node:assert`, `node:async_hooks` (a real
  `AsyncLocalStorage`), `node:buffer`, `node:events`, `node:path`,
  `node:stream` (+ `stream/web`, `stream/promises`, `stream/consumers`),
  `node:timers/promises`, `node:util`.
- **Partial**: `node:crypto` (hashes, HMAC, HKDF, PBKDF2, key objects,
  `webcrypto`; signatures, ciphers, and DH throw), `node:zlib` (sync
  `gzip`/`deflate` family only), `node:fs` (reads fail with `ENOENT`,
  `existsSync` is `false`).
- **Not implemented**: everything else — `node:http(s)`, `node:net`,
  `node:tls`, `node:dns`, `node:os`, `node:process` (the `process` global
  exists, the module does not), `node:worker_threads`, `node:vm`,
  `node:child_process`, and the rest. Known silent gap: these currently
  resolve to inert stubs instead of failing the import.

## Compatibility flags

`compatibility_date` and `compatibility_flags` are honored for the switches
celld models: `delete_all_deletes_alarm`, `js_rpc`,
`fetcher_no_get_put_delete`, `websocket_standard_binary_type`, and the
assets navigation behavior. `Cloudflare.compatibilityFlags` reports only
flags celld actually honors; a flag celld does not model is absent rather
than reported as enabled — and is otherwise accepted without effect.

## Wrangler configuration

`celld deploy` builds a standard Wrangler project (esbuild on `PATH`) and
accepts `wrangler.jsonc` or `wrangler.json` — not `wrangler.toml`. The
supported config keys are `name`, `main`, `compatibility_date`,
`compatibility_flags`, `durable_objects`, `migrations`, `assets`,
`services`, and `vars`; `main` may be omitted for an asset-only project.
Symlinks and special files in the asset directory are refused, and
`.assetsignore` still requires Wrangler. Any other key — `routes`,
`kv_namespaces`, `triggers`, and the rest — fails the deploy with an error
naming it rather than being silently dropped. Remove it or deploy that
project with Wrangler.

This page is the definitive reference for the implemented Worker surface.
For the operational boundaries of the current release — TLS, platform
support, pressure shedding, updates — see the
[limitations](limitations.md).
