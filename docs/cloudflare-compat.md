# Cloudflare service compatibility

celld runs the Workers and Durable Objects programming model, not the whole
Cloudflare platform. The scope rule is simple: **anything built on Durable
Objects is in reach; a fundamentally different primitive is out.** Cloudflare
builds D1 on Durable Objects, so a D1 binding is a thin layer over what celld
already has. KV is a globally-replicated eventually-consistent edge cache and
R2 is blob storage — different systems, not on the roadmap.

Unsupported bindings fail loudly at deploy or at first use. Silent
compatibility gaps are bugs.

## Supported

| service | notes |
| --- | --- |
| **Durable Objects** | The core. SQLite storage, alarms, inbound hibernatable WebSockets, outbound `ws:`/`wss:` WebSocket clients (constructor and `fetch()` upgrade), single-writer semantics, addressed by name. |
| **Workers** | Module Workers: `fetch`, service bindings, Durable Object bindings. |
| **Static assets** | Co-deployed immutable files, served from the fleet bucket: `assets.directory`, `binding`, `html_handling`, `not_found_handling`, `run_worker_first`, plus `_headers` and `_redirects`. Asset-only projects deploy without a Worker. |

## Planned

| service | notes |
| --- | --- |
| **D1** | A D1 database is a Durable Object with a SQL API; celld already has the hard part. Coming soon. |
| **Workflows** | Durable execution over cells and alarms. |
| **Queues** | DO-shaped; will be added if demand appears. |

## Not planned

| service | why |
| --- | --- |
| **KV** | A globally-distributed eventually-consistent edge cache — a different consistency model, not built on Durable Objects. |
| **R2** | Blob object storage is what celld runs *on* (your bucket), not what it provides. Declared `r2_buckets` bindings load, but every method throws. |
| **Cache API** | An edge HTTP cache is a platform feature, not a durability primitive. |
| **Workers AI, Vectorize, Hyperdrive, Browser Rendering, Email** | Managed platform services with no self-hosted equivalent in scope. |
| **Cron triggers, custom domains, TLS termination** | Platform surface. celld has its own durable alarms; put TLS in your ingress proxy. |

## Wrangler configuration

`celld deploy` builds a standard Wrangler project (esbuild on `PATH`) and
accepts `wrangler.jsonc` or `wrangler.json` — not `wrangler.toml`. The
supported config keys are `name`, `main`, `compatibility_date`,
`compatibility_flags`, `durable_objects`, `migrations`, and `assets`; `main`
may be omitted for an asset-only project. Symlinks and special files in the
asset directory are refused, and `.assetsignore` still requires Wrangler. Any
other key — `routes`, `vars`, `kv_namespaces`, and the rest — fails the
deploy with an error naming it rather than being silently dropped. Remove it
or deploy that project with Wrangler.

See the [v0.0.1 limitations](limitations.md) for the exact boundary of the
implemented Worker surface in this release.
