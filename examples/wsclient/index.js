export class Client {
  constructor(state) {
    this.state = state;
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/connect") {
      const input = request.method === "POST" ? await request.json() : {};
      const target = input.url ?? url.searchParams.get("url");
      if (!target) return new Response("url required", { status: 400 });
      const protocol = input.protocol ?? url.searchParams.get("protocol");
      const socket = new WebSocket(target, protocol ? [protocol] : []);
      const message = input.message ?? url.searchParams.get("message");
      socket.addEventListener("open", () => {
        if (Array.isArray(input.binary)) socket.send(new Uint8Array(input.binary));
        else if (message) socket.send(message);
      });
      socket.addEventListener("message", async (event) => {
        const binary = event.data instanceof ArrayBuffer;
        await this.state.storage.put(
          "message",
          binary ? Array.from(new Uint8Array(event.data)) : event.data,
        );
        await this.state.storage.put("binary", binary);
        socket.close(1000, "done");
      });
      socket.addEventListener("error", async () => {
        await this.state.storage.put("error", true);
      });
      return new Response(JSON.stringify({ connecting: true }), { status: 202 });
    }
    return new Response(JSON.stringify({
      message: await this.state.storage.get("message"),
      binary: await this.state.storage.get("binary") ?? false,
      error: await this.state.storage.get("error") ?? false,
    }));
  }
}

export default {
  async fetch(request, env) {
    return env.CLIENT.get(env.CLIENT.idFromName("client")).fetch(request);
  },
};
