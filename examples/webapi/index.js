export default {
  async fetch(request, env) {
    const u = new URL(request.url);
    const enc = new TextEncoder().encode("cells");
    const b64 = btoa("hello");
    const h = new Headers({ "x-test": "1" });
    h.append("x-test", "2");
    return new Response(JSON.stringify({
      pathname: u.pathname,
      search: u.search,
      query_foo: u.searchParams.get("foo"),
      encoded_len: enc.length,
      b64, back: atob(b64),
      header: h.get("x-test"),        // spec: "1, 2"
      protocol: u.protocol, host: u.host,
    }), { status: 200 });
  }
};
