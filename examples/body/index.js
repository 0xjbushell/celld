// Reads the request body — the gap that make_request left open. Routes by path
// so the body must also survive a cross-node proxy hop. Plain DO code.
export class Echo {
  constructor(state, env) { this.state = state; }
  async fetch(request) {
    if (request.method === "POST" || request.method === "PUT") {
      const data = await request.json();
      let sum = (await this.state.storage.get("sum")) ?? 0;
      sum += data.n ?? 0;
      await this.state.storage.put("sum", sum);
      return new Response(JSON.stringify({ echoed: data, sum }));
    }
    const text = await request.text();
    return new Response(JSON.stringify({ method: request.method, bodyLen: text.length }));
  }
}
export default {
  async fetch(request, env) {
    const name = new URL(request.url).pathname.slice(1) || "default";
    return env.ECHO.get(env.ECHO.idFromName(name)).fetch(request);
  }
};
