import { describe, expect, test } from "bun:test";
import { z } from "zod";
import { createVerifier } from "@webhooks/verify";
import { direct, webhooks } from "../src/index";

const events = {
  "invoice.paid": z.object({ invoiceId: z.string(), amountCents: z.number() }),
};

function captureFetch(responses: number[]) {
  const calls: { url: string; body: string; headers: Headers }[] = [];
  const fetchImpl = (async (url: URL | Request | string, init?: RequestInit) => {
    calls.push({
      url: String(url),
      body: String(init?.body),
      headers: new Headers(init?.headers),
    });
    const status = responses[Math.min(calls.length - 1, responses.length - 1)]!;
    return new Response(null, { status });
  }) as unknown as typeof fetch;
  return { calls, fetchImpl };
}

describe("direct delivery", () => {
  test("delivers a signed payload that @webhooks/verify accepts", async () => {
    const { calls, fetchImpl } = captureFetch([200]);
    const wh = webhooks({
      events,
      endpoints: [{ url: "https://consumer.test/hook" }],
      signing: { secret: "whsec_dGVzdC1zZWNyZXQ=" },
      delivery: direct({ fetchImpl }),
    });

    const result = await wh.send({
      type: "invoice.paid",
      data: { invoiceId: "inv_1", amountCents: 4200 },
    });
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.value.endpoints).toEqual([
      { url: "https://consumer.test/hook", delivered: true, attempts: 1, status: 200 },
    ]);

    const call = calls[0]!;
    expect(call.headers.get("webhook-id")).toBe(result.value.eventId);

    const verifier = createVerifier("whsec_dGVzdC1zZWNyZXQ=");
    const verified = await verifier.verifyPayload(call.body, call.headers);
    expect(verified.ok).toBe(true);
    if (!verified.ok) return;
    expect(verified.event).toEqual({
      type: "invoice.paid",
      subject: null,
      data: { invoiceId: "inv_1", amountCents: 4200 },
    });
    expect(verified.id).toBe(result.value.eventId);
  });

  test("subject travels inside the signed body", async () => {
    const { calls, fetchImpl } = captureFetch([200]);
    const wh = webhooks({
      events,
      endpoints: [{ url: "https://consumer.test/hook", secret: "s3cret" }],
      delivery: direct({ fetchImpl }),
    });

    const result = await wh.send({
      type: "invoice.paid",
      subject: "acct_42",
      data: { invoiceId: "inv_sub", amountCents: 1 },
    });
    expect(result.ok).toBe(true);

    const verified = await createVerifier("s3cret").verifyPayload(
      calls[0]!.body,
      calls[0]!.headers,
    );
    expect(verified.ok).toBe(true);
    if (!verified.ok) return;
    expect(verified.event).toMatchObject({ subject: "acct_42" });
  });

  test("subject: 'required' config rejects subjectless sends at runtime (JS backstop)", async () => {
    const { calls, fetchImpl } = captureFetch([200]);
    const wh = webhooks({
      events,
      endpoints: [{ url: "https://consumer.test/hook", secret: "s3cret" }],
      subject: "required",
      delivery: direct({ fetchImpl }),
    });

    const send = wh.send as unknown as (input: {
      type: string;
      data: unknown;
    }) => Promise<{ ok: boolean }>;
    const result = await send({
      type: "invoice.paid",
      data: { invoiceId: "inv_ns", amountCents: 1 },
    });
    expect(result.ok).toBe(false);
    expect(calls.length).toBe(0);
  });

  test("rejects empty-string subjects", async () => {
    const { fetchImpl } = captureFetch([200]);
    const wh = webhooks({
      events,
      endpoints: [{ url: "https://consumer.test/hook", secret: "s3cret" }],
      delivery: direct({ fetchImpl }),
    });

    const result = await wh.send({
      type: "invoice.paid",
      subject: "",
      data: { invoiceId: "inv_es", amountCents: 1 },
    });
    expect(result.ok).toBe(false);
  });

  test("retries failures then succeeds", async () => {
    const { calls, fetchImpl } = captureFetch([500, 500, 200]);
    const wh = webhooks({
      events,
      endpoints: [{ url: "https://consumer.test/hook", secret: "s3cret" }],
      delivery: direct({ fetchImpl, backoffMs: () => 0 }),
    });

    const result = await wh.send({
      type: "invoice.paid",
      data: { invoiceId: "inv_2", amountCents: 1 },
    });
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.value.endpoints[0]).toMatchObject({ delivered: true, attempts: 3 });
    expect(calls.length).toBe(3);
  });

  test("reports failure after max attempts", async () => {
    const { calls, fetchImpl } = captureFetch([500]);
    const wh = webhooks({
      events,
      endpoints: [{ url: "https://consumer.test/hook", secret: "s3cret" }],
      delivery: direct({ fetchImpl, backoffMs: () => 0, maxAttempts: 3 }),
    });

    const result = await wh.send({
      type: "invoice.paid",
      data: { invoiceId: "inv_3", amountCents: 1 },
    });
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.value.endpoints[0]).toMatchObject({
      delivered: false,
      attempts: 3,
      error: "endpoint responded 500",
    });
    expect(calls.length).toBe(3);
  });

  test("rejects invalid payloads without sending", async () => {
    const { calls, fetchImpl } = captureFetch([200]);
    const wh = webhooks({
      events,
      endpoints: [{ url: "https://consumer.test/hook", secret: "s3cret" }],
      delivery: direct({ fetchImpl }),
    });

    const result = await wh.send({
      type: "invoice.paid",
      data: { invoiceId: "inv_4", amountCents: "not-a-number" as unknown as number },
    });
    expect(result.ok).toBe(false);
    expect(calls.length).toBe(0);
  });

  test("errors when no signing secret is available", async () => {
    const { fetchImpl } = captureFetch([200]);
    const wh = webhooks({
      events,
      endpoints: [{ url: "https://consumer.test/hook" }],
      delivery: direct({ fetchImpl }),
    });

    const result = await wh.send({
      type: "invoice.paid",
      data: { invoiceId: "inv_5", amountCents: 1 },
    });
    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.error).toContain("no signing secret");
  });

  test("treats redirects as delivery failures (no follow)", async () => {
    const { calls, fetchImpl } = captureFetch([302]);
    const wh = webhooks({
      events,
      endpoints: [{ url: "https://consumer.test/hook", secret: "s3cret" }],
      delivery: direct({ fetchImpl, backoffMs: () => 0, maxAttempts: 2 }),
    });

    const result = await wh.send({
      type: "invoice.paid",
      data: { invoiceId: "inv_r", amountCents: 1 },
    });
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.value.endpoints[0]).toMatchObject({
      delivered: false,
      error: "endpoint responded 302",
    });
    expect(calls.length).toBe(2);
  });

  test("returns err for non-JSON-serializable payloads instead of throwing", async () => {
    const { calls, fetchImpl } = captureFetch([200]);
    const wh = webhooks({
      events: { "big.event": z.object({ n: z.bigint() }) },
      endpoints: [{ url: "https://consumer.test/hook", secret: "s3cret" }],
      delivery: direct({ fetchImpl }),
    });

    const result = await wh.send({ type: "big.event", data: { n: 10n } });
    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.error).toContain("not JSON-serializable");
    expect(calls.length).toBe(0);
  });

  test("returns err for prototype-member event names instead of throwing", async () => {
    const { fetchImpl } = captureFetch([200]);
    const wh = webhooks({
      events,
      endpoints: [{ url: "https://consumer.test/hook", secret: "s3cret" }],
      delivery: direct({ fetchImpl }),
    });

    const send = wh.send as (input: { type: string; data: unknown }) => Promise<{ ok: boolean }>;
    for (const name of ["toString", "constructor", "valueOf"]) {
      const result = await send({ type: name, data: {} });
      expect(result.ok).toBe(false);
    }
  });

  test("returns err when the endpoint resolver throws synchronously", async () => {
    const { fetchImpl } = captureFetch([200]);
    const wh = webhooks({
      events,
      endpoints: () => {
        throw new Error("sync boom");
      },
      delivery: direct({ fetchImpl }),
    });

    const result = await wh.send({
      type: "invoice.paid",
      data: { invoiceId: "inv_s", amountCents: 1 },
    });
    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.error).toContain("endpoint resolver failed");
  });

  test("returns err when a schema transform throws instead of rejecting send()", async () => {
    const { calls, fetchImpl } = captureFetch([200]);
    const wh = webhooks({
      events: {
        "boom.event": z.object({ x: z.string() }).transform(() => {
          throw new Error("transform blew up");
        }),
      },
      endpoints: [{ url: "https://consumer.test/hook", secret: "s3cret" }],
      delivery: direct({ fetchImpl }),
    });

    const result = await wh.send({ type: "boom.event", data: { x: "1" } });
    expect(result.ok).toBe(false);
    expect(calls.length).toBe(0);
  });

  test("returns err for whsec_ secrets with empty/malformed base64", async () => {
    const { calls, fetchImpl } = captureFetch([200]);
    const wh = webhooks({
      events,
      endpoints: [{ url: "https://consumer.test/hook" }],
      signing: { secret: "whsec_" },
      delivery: direct({ fetchImpl }),
    });

    const result = await wh.send({
      type: "invoice.paid",
      data: { invoiceId: "inv_w", amountCents: 1 },
    });
    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.error).toContain("invalid signing secret");
    expect(calls.length).toBe(0);
  });

  test("returns err when the endpoint resolver rejects", async () => {
    const { fetchImpl } = captureFetch([200]);
    const wh = webhooks({
      events,
      endpoints: () => Promise.reject(new Error("subscriber db down")),
      delivery: direct({ fetchImpl }),
    });

    const result = await wh.send({
      type: "invoice.paid",
      data: { invoiceId: "inv_e", amountCents: 1 },
    });
    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.error).toContain("endpoint resolver failed");
  });

  test("resolver receives type and subject", async () => {
    const { calls, fetchImpl } = captureFetch([200]);
    const seen: { type: string; subject: string | null }[] = [];
    const wh = webhooks({
      events,
      endpoints: (event) => {
        seen.push({ type: event.type, subject: event.subject });
        return [{ url: `https://consumer.test/${event.type}`, secret: "s3cret" }];
      },
      delivery: direct({ fetchImpl }),
    });

    const result = await wh.send({
      type: "invoice.paid",
      subject: "acct_7",
      data: { invoiceId: "inv_6", amountCents: 1 },
    });
    expect(result.ok).toBe(true);
    expect(calls[0]!.url).toBe("https://consumer.test/invoice.paid");
    expect(seen).toEqual([{ type: "invoice.paid", subject: "acct_7" }]);
  });
});
