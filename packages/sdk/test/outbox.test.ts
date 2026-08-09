import { describe, expect, test } from "bun:test";
import { z } from "zod";
import { memoryAdapter, outbox, webhooks } from "../src/index";

const events = {
  "user.created": z.object({ userId: z.string() }),
};

function makeWh(adapter = memoryAdapter(), opts: Parameters<typeof outbox>[1] = {}) {
  return webhooks({
    events,
    subscribers: [{ url: "https://consumer.test/hook" }],
    subject: "optional",
    signing: { secret: "s3cret" },
    delivery: outbox(adapter, { backoffMs: () => 0, ...opts }),
  });
}

const stubFetch = (status: number) =>
  (async () => new Response(null, { status })) as unknown as typeof fetch;

describe("outbox delivery", () => {
  test("send() inserts a pending row, no HTTP", async () => {
    const adapter = memoryAdapter();
    const wh = makeWh(adapter);

    const result = await wh.send({ event: "user.created", data: { userId: "u_1" } });
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.value.enqueued).toBe(1);
    expect(adapter.rows.length).toBe(1);
    expect(adapter.rows[0]).toMatchObject({
      eventId: result.value.eventId,
      eventType: "user.created",
      status: "pending",
      attempts: 0,
      url: "https://consumer.test/hook",
    });
  });

  test("subject is stored on the outbox row (null when omitted)", async () => {
    const adapter = memoryAdapter();
    const wh = makeWh(adapter);

    await wh.send({ event: "user.created", subject: "user_9", data: { userId: "u_9" } });
    await wh.send({ event: "user.created", data: { userId: "u_10" } });

    expect(adapter.rows[0]).toMatchObject({ subject: "user_9" });
    expect(adapter.rows[1]).toMatchObject({ subject: null });
  });

  test("with(tx) forwards the transaction to the adapter", async () => {
    const adapter = memoryAdapter();
    const wh = makeWh(adapter);
    const tx = { fake: "tx" };

    const result = await wh.with(tx).send({ event: "user.created", data: { userId: "u_2" } });
    expect(result.ok).toBe(true);
    expect(adapter.lastTx).toBe(tx);
  });

  test("worker tick delivers pending rows", async () => {
    const adapter = memoryAdapter();
    const wh = makeWh(adapter);
    await wh.send({ event: "user.created", data: { userId: "u_3" } });

    const report = await wh.worker({ fetchImpl: stubFetch(200) }).tick();

    expect(report).toEqual({ claimed: 1, delivered: 1, retried: 0, dead: 0 });
    expect(adapter.rows[0]!.status).toBe("delivered");
  });

  test("failed delivery schedules a retry, then dead-letters at maxAttempts", async () => {
    const adapter = memoryAdapter();
    const wh = makeWh(adapter, { maxAttempts: 2 });
    await wh.send({ event: "user.created", data: { userId: "u_4" } });

    const worker = wh.worker({ fetchImpl: stubFetch(500) });

    const first = await worker.tick();
    expect(first).toEqual({ claimed: 1, delivered: 0, retried: 1, dead: 0 });
    expect(adapter.rows[0]).toMatchObject({
      status: "pending",
      attempts: 1,
      lastError: "endpoint responded 500",
    });

    const second = await worker.tick();
    expect(second).toEqual({ claimed: 1, delivered: 0, retried: 0, dead: 1 });
    expect(adapter.rows[0]).toMatchObject({ status: "dead", attempts: 2 });
  });

  test("rejects invalid maxAttempts at configuration time", () => {
    const adapter = memoryAdapter();
    expect(() => outbox(adapter, { maxAttempts: Number("oops") })).toThrow(RangeError);
    expect(() => outbox(adapter, { maxAttempts: 0 })).toThrow(RangeError);
  });

  test("worker signs deliveries the same way as direct mode", async () => {
    const adapter = memoryAdapter();
    const wh = makeWh(adapter);
    await wh.send({ event: "user.created", data: { userId: "u_5" } });

    let captured: { body: string; headers: Headers } | null = null;
    const fetchImpl = (async (_url: URL | Request | string, init?: RequestInit) => {
      captured = { body: String(init?.body), headers: new Headers(init?.headers) };
      return new Response(null, { status: 200 });
    }) as unknown as typeof fetch;
    await wh.worker({ fetchImpl }).tick();

    const { createVerifier } = await import("@webhooks/verify");
    const verified = await createVerifier("s3cret").verifyPayload(
      captured!.body,
      captured!.headers,
    );
    expect(verified.ok).toBe(true);
  });

  test("one row's adapter failure doesn't abandon the other rows", async () => {
    const inner = memoryAdapter();
    let completeCalls = 0;
    const failing = {
      ...inner,
      complete: async (rowId: string) => {
        completeCalls++;
        if (completeCalls === 1) throw new Error("db down");
        return inner.complete(rowId);
      },
    };
    const wh = makeWh(failing as typeof inner);
    await wh.send({ event: "user.created", data: { userId: "u_a" } });
    await wh.send({ event: "user.created", data: { userId: "u_b" } });

    const errors: unknown[] = [];
    const report = await wh
      .worker({ fetchImpl: stubFetch(200), hooks: { onError: (error) => errors.push(error) } })
      .tick();

    expect(report.claimed).toBe(2);
    expect(report.delivered).toBe(1);
    expect(errors.length).toBe(1);
    expect(inner.rows.filter((row) => row.status === "delivered").length).toBe(1);
  });

  test("start() during a pending stop() doesn't resurrect the old loop", async () => {
    const inner = memoryAdapter();
    let claims = 0;
    const slow = {
      ...inner,
      claim: async (limit: number, now: Date) => {
        claims++;
        await new Promise((resolve) => setTimeout(resolve, 20));
        return inner.claim(limit, now);
      },
    };
    const wh = makeWh(slow as typeof inner);
    const worker = wh.worker({ fetchImpl: stubFetch(200), pollIntervalMs: 5 });

    worker.start();
    await new Promise((resolve) => setTimeout(resolve, 10)); // tick in flight
    const pending = worker.stop();
    worker.start();
    await pending;
    await worker.stop();

    const settled = claims;
    await new Promise((resolve) => setTimeout(resolve, 60));
    expect(claims).toBe(settled); // no zombie loop still claiming
  });

  test("throwing onError hook doesn't reject stop() or leak rejections", async () => {
    const inner = memoryAdapter();
    const broken = {
      ...inner,
      claim: async () => {
        throw new Error("db down");
      },
    };
    const wh = makeWh(broken as typeof inner);
    const worker = wh.worker({
      pollIntervalMs: 5,
      hooks: {
        onError: () => {
          throw new Error("logger died");
        },
      },
    });

    worker.start();
    await new Promise((resolve) => setTimeout(resolve, 25));
    await worker.stop(); // must resolve despite claim + hook both throwing
  });

  test("start/stop polling loop delivers in the background", async () => {
    const adapter = memoryAdapter();
    const wh = makeWh(adapter);
    await wh.send({ event: "user.created", data: { userId: "u_6" } });

    const worker = wh.worker({ fetchImpl: stubFetch(200), pollIntervalMs: 5 });
    worker.start();
    await new Promise((resolve) => setTimeout(resolve, 50));
    await worker.stop();

    expect(adapter.rows[0]!.status).toBe("delivered");
  });
});
