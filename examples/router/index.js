// The default fetch routes by path: /alice → the "alice" Counter, /bob → "bob".
// On a multi-node cluster those cells may live on different nodes; env.COUNTER
// .get(id) transparently proxies to the owner. This fixture is plain DO code —
// it runs identically on Cloudflare.
export class Counter {
  constructor(state, env) { this.state = state; }
  async fetch(request) {
    const name = new URL(request.url).pathname.slice(1) || "default";
    let n = (await this.state.storage.get("n")) ?? 0;
    n++;
    await this.state.storage.put("n", n);
    return new Response(JSON.stringify({ name, n }), { status: 200 });
  }
}
export default {
  async fetch(request, env) {
    const name = new URL(request.url).pathname.slice(1) || "default";
    const id = env.COUNTER.idFromName(name);
    return env.COUNTER.get(id).fetch(request);
  }
};
