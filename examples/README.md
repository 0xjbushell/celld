# Examples

These small Wrangler projects demonstrate progressively more of the Worker and
Durable Object surface supported by `celld`:

- `hello/` — a stateless Worker `fetch` handler
- `webapi/` — common Web Platform APIs
- `counter/` — a SQLite-backed Durable Object
- `async/` — asynchronous storage and `waitUntil`
- `body/` — request and response bodies
- `router/` — Worker-to-Durable-Object routing
- `wsecho/` — WebSocket echo with hibernation
- `wsclient/` — outbound WebSocket client from a Durable Object
- `alarm/` — a Durable Object alarm handler

Deploy an example from its directory to the same bucket the nodes use:

```sh
celld deploy . --bucket s3://my-cells-bucket
```

They are examples, not the complete compatibility test suite.
