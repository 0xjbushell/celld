// Exercises the async-op layer: a DO that awaits a timer and an outbound fetch.
// Both need a real event loop — under a synchronous busy-loop they hang forever.
// Plain DO code: runs identically on Cloudflare.
export class A {
  constructor(state, env) { this.state = state; }
  async fetch(request) {
    const u = new URL(request.url);
    if (u.pathname === "/timer") {
      const t0 = Date.now();
      await new Promise((r) => setTimeout(r, 30));
      return new Response(JSON.stringify({ waitedMs: Date.now() - t0 }));
    }
    if (u.pathname === "/fetch") {
      const resp = await fetch(u.searchParams.get("url"));
      const body = await resp.text();
      return new Response(JSON.stringify({ status: resp.status, len: body.length }));
    }
    // combined: await a timer, then read-modify-write persistent state
    await new Promise((r) => setTimeout(r, 5));
    let n = (await this.state.storage.get("n")) ?? 0;
    n++;
    await this.state.storage.put("n", n);
    return new Response(JSON.stringify({ n }));
  }
}
export default {
  async fetch(request, env) {
    return env.A.get(env.A.idFromName("a")).fetch(request);
  }
};
