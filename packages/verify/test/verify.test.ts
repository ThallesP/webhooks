import { describe, expect, test } from "bun:test";
import { createVerifier } from "../src/index";

const encoder = new TextEncoder();

async function sign(secret: string, id: string, timestampSec: number, payload: string) {
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const signature = await crypto.subtle.sign(
    "HMAC",
    key,
    encoder.encode(`${id}.${timestampSec}.${payload}`),
  );
  return `v1,${btoa(String.fromCharCode(...new Uint8Array(signature)))}`;
}

const SECRET = "test-secret";
const PAYLOAD = JSON.stringify({ type: "invoice.paid", data: { invoiceId: "inv_1" } });

async function signedHeaders(overrides: Record<string, string> = {}) {
  const timestampSec = Math.floor(Date.now() / 1000);
  return {
    "webhook-id": "evt_123",
    "webhook-timestamp": String(timestampSec),
    "webhook-signature": await sign(SECRET, "evt_123", timestampSec, PAYLOAD),
    ...overrides,
  };
}

describe("createVerifier", () => {
  test("accepts a valid payload (record headers)", async () => {
    const result = await createVerifier(SECRET).verifyPayload(PAYLOAD, await signedHeaders());
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.id).toBe("evt_123");
    expect(result.event).toEqual({ type: "invoice.paid", data: { invoiceId: "inv_1" } });
  });

  test("accepts a fetch Request", async () => {
    const request = new Request("https://consumer.test/hook", {
      method: "POST",
      headers: await signedHeaders(),
      body: PAYLOAD,
    });
    const result = await createVerifier(SECRET).verify(request);
    expect(result.ok).toBe(true);
  });

  test("accepts Uint8Array payloads (express raw body)", async () => {
    const result = await createVerifier(SECRET).verifyPayload(
      encoder.encode(PAYLOAD),
      await signedHeaders(),
    );
    expect(result.ok).toBe(true);
  });

  test("accepts svix-* header names", async () => {
    const headers = await signedHeaders();
    const result = await createVerifier(SECRET).verifyPayload(PAYLOAD, {
      "svix-id": headers["webhook-id"]!,
      "svix-timestamp": headers["webhook-timestamp"]!,
      "svix-signature": headers["webhook-signature"]!,
    });
    expect(result.ok).toBe(true);
  });

  test("accepts multiple signatures when one matches (key rotation)", async () => {
    const headers = await signedHeaders();
    const bogus = await sign("old-secret", "evt_123", Number(headers["webhook-timestamp"]), PAYLOAD);
    const result = await createVerifier(SECRET).verifyPayload(PAYLOAD, {
      ...headers,
      "webhook-signature": `${bogus} ${headers["webhook-signature"]}`,
    });
    expect(result.ok).toBe(true);
  });

  test("rejects a tampered payload", async () => {
    const result = await createVerifier(SECRET).verifyPayload(
      PAYLOAD.replace("inv_1", "inv_2"),
      await signedHeaders(),
    );
    expect(result).toEqual({ ok: false, reason: "invalid_signature" });
  });

  test("rejects a wrong secret", async () => {
    const result = await createVerifier("other-secret").verifyPayload(
      PAYLOAD,
      await signedHeaders(),
    );
    expect(result).toEqual({ ok: false, reason: "invalid_signature" });
  });

  test("rejects timestamps outside tolerance", async () => {
    const old = Math.floor(Date.now() / 1000) - 3600;
    const result = await createVerifier(SECRET).verifyPayload(PAYLOAD, {
      "webhook-id": "evt_123",
      "webhook-timestamp": String(old),
      "webhook-signature": await sign(SECRET, "evt_123", old, PAYLOAD),
    });
    expect(result).toEqual({ ok: false, reason: "timestamp_out_of_tolerance" });
  });

  test("rejects missing headers", async () => {
    const result = await createVerifier(SECRET).verifyPayload(PAYLOAD, {});
    expect(result).toEqual({ ok: false, reason: "missing_headers" });
  });

  test("rejects a garbage timestamp", async () => {
    const result = await createVerifier(SECRET).verifyPayload(PAYLOAD, {
      ...(await signedHeaders()),
      "webhook-timestamp": "not-a-number",
    });
    expect(result).toEqual({ ok: false, reason: "invalid_timestamp" });
  });

  test("rejects non-JSON bodies after signature passes", async () => {
    const timestampSec = Math.floor(Date.now() / 1000);
    const body = "not json";
    const result = await createVerifier(SECRET).verifyPayload(body, {
      "webhook-id": "evt_123",
      "webhook-timestamp": String(timestampSec),
      "webhook-signature": await sign(SECRET, "evt_123", timestampSec, body),
    });
    expect(result).toEqual({ ok: false, reason: "invalid_json" });
  });

  test("returns unreadable_body for an already-consumed Request", async () => {
    const request = new Request("https://consumer.test/hook", {
      method: "POST",
      headers: await signedHeaders(),
      body: PAYLOAD,
    });
    await request.text(); // consume
    const result = await createVerifier(SECRET).verify(request);
    expect(result).toEqual({ ok: false, reason: "unreadable_body" });
  });

  test("throws at construction for empty secrets (misconfig, not a request failure)", () => {
    expect(() => createVerifier("")).toThrow(RangeError);
  });

  test("throws at construction for whsec_ secrets with empty/malformed base64", () => {
    // Falling back to the literal string would mean a publicly guessable key.
    expect(() => createVerifier("whsec_")).toThrow(RangeError);
    expect(() => createVerifier("whsec_!!!not-base64")).toThrow(RangeError);
  });

  test("rejects ids containing '.' (boundary-shift signature ambiguity)", async () => {
    const timestampSec = Math.floor(Date.now() / 1000);
    // Signed string "evt.T.{T+1}.0" could also parse as id "evt.T", ts T+1, body "0".
    const result = await createVerifier(SECRET).verifyPayload(PAYLOAD, {
      "webhook-id": "evt.123",
      "webhook-timestamp": String(timestampSec),
      "webhook-signature": await sign(SECRET, "evt.123", timestampSec, PAYLOAD),
    });
    expect(result).toEqual({ ok: false, reason: "invalid_id" });
  });

  test("does not buffer the body when headers already fail", async () => {
    const request = new Request("https://consumer.test/hook", {
      method: "POST",
      body: PAYLOAD, // no webhook headers at all
    });
    const result = await createVerifier(SECRET).verify(request);
    expect(result).toEqual({ ok: false, reason: "missing_headers" });
    expect(request.bodyUsed).toBe(false);
  });

  test("throws at construction for NaN tolerance (would fail open)", () => {
    expect(() => createVerifier(SECRET, { toleranceSec: Number("oops") })).toThrow(RangeError);
    expect(() => createVerifier(SECRET, { toleranceSec: -1 })).toThrow(RangeError);
  });

  test("accepts mixed-case record header names", async () => {
    const headers = await signedHeaders();
    const result = await createVerifier(SECRET).verifyPayload(PAYLOAD, {
      "Webhook-Id": headers["webhook-id"]!,
      "Webhook-Timestamp": headers["webhook-timestamp"]!,
      "Webhook-Signature": headers["webhook-signature"]!,
    });
    expect(result.ok).toBe(true);
  });

  test("caps signature candidates at 8 (unauthenticated CPU burn)", async () => {
    const headers = await signedHeaders();
    const good = headers["webhook-signature"]!;
    const bogus = await sign("wrong-secret", "evt_123", Number(headers["webhook-timestamp"]), PAYLOAD);

    const withinCap = [...Array(7).fill(bogus), good].join(" ");
    const beyondCap = [...Array(8).fill(bogus), good].join(" ");

    const okResult = await createVerifier(SECRET).verifyPayload(PAYLOAD, {
      ...headers,
      "webhook-signature": withinCap,
    });
    expect(okResult.ok).toBe(true);

    const capped = await createVerifier(SECRET).verifyPayload(PAYLOAD, {
      ...headers,
      "webhook-signature": beyondCap,
    });
    expect(capped).toEqual({ ok: false, reason: "invalid_signature" });
  });

  test("verifies over raw bytes, not re-encoded text", async () => {
    // 0xFF is invalid UTF-8: decode+re-encode would turn it into U+FFFD and
    // the two different byte payloads would share one signature.
    const rawBytes = new Uint8Array([
      ...encoder.encode('{"type":"note.created","data":{"text":"hi'),
      0xff,
      ...encoder.encode('"}}'),
    ]);
    const timestampSec = Math.floor(Date.now() / 1000);
    const key = await crypto.subtle.importKey(
      "raw",
      encoder.encode(SECRET),
      { name: "HMAC", hash: "SHA-256" },
      false,
      ["sign"],
    );
    const prefix = encoder.encode(`evt_123.${timestampSec}.`);
    const message = new Uint8Array(prefix.length + rawBytes.length);
    message.set(prefix);
    message.set(rawBytes, prefix.length);
    const signature = await crypto.subtle.sign("HMAC", key, message);
    const headers = {
      "webhook-id": "evt_123",
      "webhook-timestamp": String(timestampSec),
      "webhook-signature": `v1,${btoa(String.fromCharCode(...new Uint8Array(signature)))}`,
    };

    const verifier = createVerifier(SECRET);
    const exact = await verifier.verifyPayload(rawBytes, headers);
    expect(exact.ok).toBe(true);

    // Same bytes after a lossy decode/re-encode round trip must NOT verify.
    const mangled = new TextDecoder().decode(rawBytes);
    const reEncoded = await verifier.verifyPayload(mangled, headers);
    expect(reEncoded).toEqual({ ok: false, reason: "invalid_signature" });
  });

  test("rejects malformed signature headers without throwing", async () => {
    const result = await createVerifier(SECRET).verifyPayload(PAYLOAD, {
      ...(await signedHeaders()),
      "webhook-signature": "v1,!!!not-base64!!! v2,abc ,",
    });
    expect(result).toEqual({ ok: false, reason: "invalid_signature" });
  });
});
