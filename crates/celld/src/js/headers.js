const _tokenRe = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;
function _checkName(name) {
  name = String(name);
  if (!_tokenRe.test(name))
    throw new TypeError(
      "Invalid header name: " + name);
  return name.toLowerCase();
}
// Byte sequence, minus NUL/LF/CR. Strip HTTP
// whitespace (HT/LF/CR/SP) from both ends first per
// WHATWG fetch — trailing LF/CR are permitted input.
function _checkValue(value) {
  value = String(value).replace(
    /^[\t\n\r ]+|[\t\n\r ]+$/g, '');
  for (let i = 0; i < value.length; i++) {
    const c = value.charCodeAt(i);
    if (c === 0 || c === 0x0A || c === 0x0D
        || c > 0xFF)
      throw new TypeError(
        "Invalid header value");
  }
  return value;
}

// Enumerable function-valued brand, shared by
// every instance: V8's structured clone throws
// on functions, so a Headers can never silently
// flatten into a plain object — RPC
// serialization lifts it natively instead.
const _noClone = () => {};

class Headers {
  #map = {};

  constructor(init) {
    this.__celldHost = _noClone;
    if (init === undefined) return;
    if (init === null || typeof init !== "object")
      throw new TypeError("Invalid Headers init");
    // Sequence-like (includes Headers with its default
    // iterator or any user-overridden one) takes precedence
    // over record so custom iterators are honored.
    if (typeof init[Symbol.iterator] === "function") {
      for (const pair of init) {
        if (!pair || typeof pair !== "object"
            || pair.length !== 2)
          throw new TypeError(
            "Headers init entry must be a 2-tuple");
        this.append(pair[0], pair[1]);
      }
    } else {
      // Record: explicit Reflect walk so Proxy handlers
      // see the WebIDL es-to-record trace exactly —
      // ownKeys → for each key, getOwnPropertyDescriptor;
      // if enumerable, coerce key to ByteString (throws
      // for Symbols), then [[Get]], then validate value.
      for (const k of Reflect.ownKeys(init)) {
        const desc = Reflect
          .getOwnPropertyDescriptor(init, k);
        if (!desc || !desc.enumerable) continue;
        if (typeof k === "symbol")
          throw new TypeError(
            "Headers init key must be a string");
        const ck = _checkName(k);
        const v = Reflect.get(init, k, init);
        const cv = _checkValue(v);
        (this.#map[ck] ??= []).push(cv);
      }
    }
  }

  append(name, value) {
    const k = _checkName(name);
    const v = _checkValue(value);
    (this.#map[k] ??= []).push(v);
  }
  get(name) {
    const arr = this.#map[_checkName(name)];
    return arr ? arr.join(", ") : null;
  }
  set(name, value) {
    this.#map[_checkName(name)] =
      [_checkValue(value)];
  }
  has(name) {
    return _checkName(name) in this.#map;
  }
  delete(name) {
    delete this.#map[_checkName(name)];
  }
  getSetCookie() {
    return this.#map["set-cookie"]
      ? [...this.#map["set-cookie"]]
      : [];
  }
  // Iteration: per WHATWG, re-sort on each step so
  // live mutations (append/delete) are visible. Uses
  // hand-rolled iterator so `next` has enumerable=true
  // per WebIDL (generators would be enumerable=false).
  entries() { return _makeHeadersIter(this.#map, 2); }
  keys() { return _makeHeadersIter(this.#map, 0); }
  values() { return _makeHeadersIter(this.#map, 1); }
  [Symbol.iterator]() { return this.entries(); }
  forEach(cb, thisArg) {
    if (typeof cb !== "function")
      throw new TypeError(
        "forEach callback must be a function");
    for (const [k, v] of this.entries())
      cb.call(thisArg, v, k, this);
  }
}

// Headers iterator prototype — chained to
// %IteratorPrototype% so prototype-chain checks pass.
// kind: 0=keys, 1=values, 2=entries.
const _headersIterProto = Object.create(
  Object.getPrototypeOf(
    Object.getPrototypeOf([][Symbol.iterator]())));
Object.defineProperty(_headersIterProto, "next", {
  configurable: true, enumerable: true, writable: true,
  value: function next() {
    // Re-flatten (pairs count is what the iterator
    // tracks, so mutations before/after already-emitted
    // positions are handled correctly).
    const m = this._map;
    const keys = Object.keys(m).sort();
    const pairs = [];
    for (const k of keys) {
      const arr = m[k];
      if (!arr) continue;
      if (k === "set-cookie") {
        for (const v of arr) pairs.push([k, v]);
      } else {
        pairs.push([k, arr.join(", ")]);
      }
    }
    if (this._cnt >= pairs.length)
      return { value: undefined, done: true };
    const [k, v] = pairs[this._cnt++];
    const value = this._kind === 0 ? k
      : this._kind === 1 ? v : [k, v];
    return { value, done: false };
  },
});
Object.defineProperty(_headersIterProto,
  Symbol.iterator, { value() { return this; } });
function _makeHeadersIter(map, kind) {
  const it = Object.create(_headersIterProto);
  it._map = map; it._cnt = 0; it._kind = kind;
  return it;
}

// Coerce a Request/Response body init to one of the
// `BodyInit` member types or null. Pre-fix
// `new Request(url, {body: 42})` stored 42 as the
// body and then `.text()` ran TextDecoder on it,
// producing 42 NUL bytes (V8 interprets a number
// passed to TextDecoder.decode as a BufferSource of
// that byte length). Per Web IDL union conversion,
// the BodyInit dictionary member's USVString branch
// accepts any non-BodyInit value via ToString. Match
// that here so primitives go through cleanly.

globalThis.Headers = Headers;
