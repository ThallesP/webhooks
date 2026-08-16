/**
 * Zero-dependency webhook verification. WebCrypto only — runs on Node 20+,
 * Bun, Deno, Cloudflare Workers, and edge runtimes.
 *
 * Verifies Standard Webhooks signatures (`webhook-*` headers, `svix-*`
 * accepted as fallback): HMAC-SHA256 over `{id}.{timestamp}.{body}`.
 *
 * Verification failures come back as reason codes, never exceptions.
 * Constructor misconfiguration (empty or malformed secret, invalid tolerance)
 * throws at construction time — loud at boot, not silent at request time.
 */

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** Cap on HMAC computations per request — the header is attacker-controlled. */
const MAX_SIGNATURE_CANDIDATES = 8;

export type VerifyFailure =
  | "missing_headers"
  | "invalid_id"
  | "invalid_timestamp"
  | "timestamp_out_of_tolerance"
  | "invalid_signature"
  | "invalid_secret"
  | "unreadable_body"
  | "invalid_json";

export type VerifyResult<T> =
  | { ok: true; id: string; timestamp: Date; payload: T }
  | { ok: false; reason: VerifyFailure };

export type HeadersLike = Headers | Record<string, string | string[] | undefined>;

export interface Verifier {
  /** Fetch-style frameworks (Next, Hono, Remix, Workers). Consumes the body — pass a clone if you need it later. */
  verify<T = unknown>(request: Request): Promise<VerifyResult<T>>;
  /** Node-style frameworks (express, fastify). `payload` must be the RAW body — parsed/re-serialized JSON breaks signatures. */
  verifyPayload<T = unknown>(
    payload: string | Uint8Array,
    headers: HeadersLike,
  ): Promise<VerifyResult<T>>;
}

export function createVerifier(
  secret: string,
  opts: { toleranceSec?: number } = {},
): Verifier {
  if (!isValidSecret(secret)) {
    // A "whsec_" secret with empty/malformed base64 must not fall back to the
    // literal string — that key would be publicly guessable.
    throw new RangeError(
      'invalid webhook secret: must be non-empty, and "whsec_" secrets must contain non-empty base64',
    );
  }
  const toleranceSec = opts.toleranceSec ?? 300;
  if (!Number.isFinite(toleranceSec) || toleranceSec < 0) {
    // NaN would make the timestamp check fail open (accept every replay).
    throw new RangeError(`toleranceSec must be a non-negative finite number, got ${toleranceSec}`);
  }

  const keyPromise = importKey(secret);
  // No floating rejection between construction and first verify call.
  keyPromise.catch(() => {});

  interface HeaderData {
    id: string;
    timestampSec: number;
    signatureHeader: string;
  }

  function checkHeaders(
    headers: HeadersLike,
  ): { ok: true; head: HeaderData } | { ok: false; reason: VerifyFailure } {
    const id = getHeader(headers, "webhook-id") ?? getHeader(headers, "svix-id");
    const timestampRaw =
      getHeader(headers, "webhook-timestamp") ?? getHeader(headers, "svix-timestamp");
    const signatureHeader =
      getHeader(headers, "webhook-signature") ?? getHeader(headers, "svix-signature");
    if (!id || !timestampRaw || !signatureHeader) {
      return { ok: false, reason: "missing_headers" };
    }

    // A "." in the id makes `{id}.{timestamp}.{payload}` ambiguous: shifting
    // the boundaries yields a different (id, timestamp, payload) triple with
    // the same valid signature. The spec forbids dots in ids for this reason.
    if (id.includes(".")) return { ok: false, reason: "invalid_id" };

    const timestampSec = Number(timestampRaw);
    if (!Number.isInteger(timestampSec)) return { ok: false, reason: "invalid_timestamp" };
    const nowSec = Math.floor(Date.now() / 1000);
    if (Math.abs(nowSec - timestampSec) > toleranceSec) {
      return { ok: false, reason: "timestamp_out_of_tolerance" };
    }
    return { ok: true, head: { id, timestampSec, signatureHeader } };
  }

  async function verifyChecked<T>(
    head: HeaderData,
    payload: string | Uint8Array,
  ): Promise<VerifyResult<T>> {
    const key = await keyPromise.catch(() => null);
    if (!key) return { ok: false, reason: "invalid_secret" };

    // MAC over the raw bytes as received. Decoding to a string and re-encoding
    // would collapse invalid UTF-8 to U+FFFD, letting two different byte
    // sequences share one valid signature.
    const payloadBytes = payload instanceof Uint8Array ? payload : encoder.encode(payload);
    const prefix = encoder.encode(`${head.id}.${head.timestampSec}.`);
    const message = new Uint8Array(prefix.length + payloadBytes.length);
    message.set(prefix);
    message.set(payloadBytes, prefix.length);

    let valid = false;
    let candidates = 0;
    for (const part of head.signatureHeader.split(" ")) {
      if (candidates >= MAX_SIGNATURE_CANDIDATES) break;
      const [version, signature] = part.split(",");
      if (version !== "v1" || !signature) continue;
      candidates++;
      const signatureBytes = base64Decode(signature);
      if (!signatureBytes) continue;
      if (await crypto.subtle.verify("HMAC", key, signatureBytes, message)) {
        valid = true;
        break;
      }
    }
    if (!valid) return { ok: false, reason: "invalid_signature" };

    const text = payload instanceof Uint8Array ? decoder.decode(payload) : payload;
    const parsed = parseJson(text);
    if (!parsed.ok) return { ok: false, reason: "invalid_json" };
    return {
      ok: true,
      id: head.id,
      timestamp: new Date(head.timestampSec * 1000),
      payload: parsed.value as T,
    };
  }

  return {
    async verifyPayload(payload, headers) {
      const checked = checkHeaders(headers);
      if (!checked.ok) return checked;
      return verifyChecked(checked.head, payload);
    },
    async verify(request) {
      // Headers first: don't buffer an attacker's body when the request is
      // rejectable from headers alone.
      const checked = checkHeaders(request.headers);
      if (!checked.ok) return checked;
      // arrayBuffer, not text(): keeps the MAC over the exact bytes received
      // (text() strips BOMs and collapses invalid UTF-8). A consumed/aborted
      // body is a reason code, not an exception.
      const body = await request.arrayBuffer().catch(() => null);
      if (body === null) return { ok: false, reason: "unreadable_body" };
      return verifyChecked(checked.head, new Uint8Array(body));
    },
  };
}

// isValidSecret / secretBytes / base64Decode are duplicated from the sdk's
// signing.ts on purpose — this package ships with zero dependencies. Keep the
// copies in sync.
function isValidSecret(secret: string): boolean {
  if (!secret.startsWith("whsec_")) return secret.length > 0;
  const decoded = base64Decode(secret.slice("whsec_".length));
  return decoded !== null && decoded.length > 0;
}

async function importKey(secret: string): Promise<CryptoKey> {
  return crypto.subtle.importKey(
    "raw",
    secretBytes(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["verify"],
  );
}

function secretBytes(secret: string): Uint8Array<ArrayBuffer> {
  if (secret.startsWith("whsec_")) {
    const decoded = base64Decode(secret.slice("whsec_".length));
    if (decoded && decoded.length > 0) return decoded;
  }
  return encoder.encode(secret);
}

// Explicit <ArrayBuffer>: newer @types/node WebCrypto signatures reject
// ArrayBufferLike-backed views as BufferSource.
function base64Decode(input: string): Uint8Array<ArrayBuffer> | null {
  if (input.length % 4 !== 0 || !/^[A-Za-z0-9+/]*={0,2}$/.test(input)) return null;
  const binary = atob(input);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

function getHeader(headers: HeadersLike, name: string): string | undefined {
  if (headers instanceof Headers) return headers.get(name) ?? undefined;
  const exact = headers[name];
  if (exact !== undefined) return Array.isArray(exact) ? exact[0] : exact;
  // Plain-object headers aren't guaranteed lowercase (hand-built proxies,
  // frameworks preserving original casing) — fall back to a scan.
  for (const key of Object.keys(headers)) {
    if (key.toLowerCase() === name) {
      const value = headers[key];
      return Array.isArray(value) ? value[0] : value;
    }
  }
  return undefined;
}

function parseJson(text: string): { ok: true; value: unknown } | { ok: false } {
  try {
    return { ok: true, value: JSON.parse(text) };
  } catch {
    return { ok: false };
  }
}
