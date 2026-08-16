const encoder = new TextEncoder();

function base64Encode(data: ArrayBuffer): string {
  const bytes = new Uint8Array(data);
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
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

/**
 * `whsec_` secrets must carry non-empty base64. Falling back to the literal
 * string would silently sign/verify with a publicly guessable key ("whsec_").
 * Callers reject invalid secrets up front (send() -> err, verifier -> throw).
 */
export function isValidSecret(secret: string): boolean {
  if (!secret.startsWith("whsec_")) return secret.length > 0;
  const decoded = base64Decode(secret.slice("whsec_".length));
  return decoded !== null && decoded.length > 0;
}

function secretBytes(secret: string): Uint8Array<ArrayBuffer> {
  if (secret.startsWith("whsec_")) {
    const decoded = base64Decode(secret.slice("whsec_".length));
    if (decoded && decoded.length > 0) return decoded;
  }
  return encoder.encode(secret);
}

/**
 * Standard Webhooks signature: HMAC-SHA256 over `{id}.{timestamp}.{payload}`,
 * emitted as `v1,<base64>`.
 */
export async function signPayload(
  secret: string,
  eventId: string,
  timestampSec: number,
  payload: string,
): Promise<string> {
  const key = await crypto.subtle.importKey(
    "raw",
    secretBytes(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const signature = await crypto.subtle.sign(
    "HMAC",
    key,
    encoder.encode(`${eventId}.${timestampSec}.${payload}`),
  );
  return `v1,${base64Encode(signature)}`;
}
