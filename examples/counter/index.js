export class Counter {
  constructor(state, env) { this.state = state; }
  async fetch(request) {
    let n = (await this.state.storage.get("n")) ?? 0;
    n++;
    await this.state.storage.put("n", n);
    return new Response(JSON.stringify({ n, url: request.url }), { status: 200 });
  }
}
export default {
  async fetch(request, env) {
    const id = env.COUNTER.idFromName("room-42");
    return env.COUNTER.get(id).fetch(request);
  }
};
