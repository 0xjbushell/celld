// JS RPC: https://developers.cloudflare.com/workers/runtime-apis/rpc/
//
// Calling a method on a binding runs it in the other object. Arguments and
// return values are structured-cloned. Values that cannot be cloned — a class
// instance, a function — travel as *stubs*: the receiver gets a handle, and
// calling it runs the code back where it came from. Stubs are what make
// callbacks and pipelining work.
//
// One celld limit worth knowing, because it decides how you shape an API: a
// stub cannot cross an isolate boundary yet. A Durable Object is its own
// isolate, so a DO method takes and returns cloneable values. Named
// entrypoints reached through `ctx.exports` share this isolate, so they get
// the whole surface. Passing a function into a DO method throws rather than
// failing quietly.
import { DurableObject, RpcTarget, WorkerEntrypoint } from "cloudflare:workers";

// An RpcTarget may travel as a live object rather than a copy. A plain class
// cannot be sent over RPC at all.
class Receipt extends RpcTarget {
  constructor(id, balance) {
    super();
    this.id = id;
    this.balance = balance;
  }
  describe() {
    return `${this.id} holds ${this.balance}`;
  }
}

// A Durable Object exposes RPC methods directly: no fetch(), no URL, no
// Request. `super(state, env)` is what makes the methods reachable.
export class Account extends DurableObject {
  constructor(state, env) {
    super(state, env);
    this.state = state;
  }

  async deposit(amount) {
    const balance = ((await this.state.storage.get("balance")) ?? 0) + amount;
    await this.state.storage.put("balance", balance);
    return balance;
  }

  async balance() {
    return (await this.state.storage.get("balance")) ?? 0;
  }

  // Cloneable in, cloneable out — the shape a cross-isolate call needs.
  async depositMany(amounts) {
    const running = [];
    for (const amount of amounts) running.push(await this.deposit(amount));
    return { total: running.at(-1), running };
  }
}

// A named entrypoint is a second, separately addressable interface on the
// same Worker. `ctx.exports` reaches it in-process; a service binding in
// another project reaches it by name.
export class Ledger extends WorkerEntrypoint {
  add(left, right) {
    return left + right;
  }

  // A returned function becomes a stub, so this is a callback factory.
  adder(left) {
    return (right) => left + right;
  }

  // A function argument arrives as a stub, so the callee calls back into the
  // caller while the call is still in flight.
  async tally(values, onEach) {
    let total = 0;
    for (const value of values) {
      total += value;
      await onEach(value, total);
    }
    return total;
  }

  // Returning an RpcTarget lets the caller pipeline: it can call a method on
  // the result before the result exists.
  open(id, balance) {
    return new Receipt(id, balance);
  }
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const account = env.ACCOUNT.getByName("acct-1");

    switch (url.pathname) {
      // Durable Object RPC: a method call on the stub.
      case "/deposit":
        return Response.json({ balance: await account.deposit(10) });

      // Still cross-isolate, so the argument and the result are cloneable.
      case "/deposit-many":
        return Response.json(await account.depositMany([1, 2, 3]));

      // ctx.exports reaches a named entrypoint in this same Worker.
      case "/entrypoint":
        return Response.json({ sum: await ctx.exports.Ledger.add(20, 22) });

      // The returned function is a stub; calling it runs in the entrypoint.
      case "/adder": {
        using addFive = await ctx.exports.Ledger.adder(5);
        return Response.json({ sum: await addFive(37) });
      }

      // The function argument is a stub the entrypoint calls back.
      case "/callback": {
        const steps = [];
        const total = await ctx.exports.Ledger.tally([1, 2, 3], (v, running) => {
          steps.push({ v, running });
        });
        return Response.json({ total, steps });
      }

      // An RpcTarget crosses as a live object, and `using` disposes the stub.
      case "/receipt": {
        using receipt = await ctx.exports.Ledger.open("acct-1", 100);
        return Response.json({ describe: await receipt.describe() });
      }

      // Promise pipelining: no await on open(), so both hops leave together.
      case "/pipeline": {
        const describe = await ctx.exports.Ledger.open("acct-9", 7).describe();
        return Response.json({ describe });
      }

      default:
        return Response.json({ balance: await account.balance() });
    }
  },
};
