import { type Result, err, ok, toResult } from "./types";
import { signPayload } from "./signing";

export interface DeliverOptions {
  url: string;
  secret: string;
  eventId: string;
  body: string;
  timeoutMs: number;
  fetchImpl: typeof fetch;
}

/**
 * One signed delivery attempt. Signature timestamp is fresh per attempt so
 * retries never fall out of the receiver's replay-tolerance window.
 */
export async function deliverOnce(
  opts: DeliverOptions,
): Promise<Result<{ status: number }>> {
  const timestampSec = Math.floor(Date.now() / 1000);
  const signature = await signPayload(opts.secret, opts.eventId, timestampSec, opts.body);
  // A synchronously-throwing fetchImpl (or AbortSignal.timeout on a bad
  // value) becomes a failed attempt instead of a send()/tick() rejection.
  const attempt = await toResult(
    () =>
      opts.fetchImpl(opts.url, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "webhook-id": opts.eventId,
          "webhook-timestamp": String(timestampSec),
          "webhook-signature": signature,
        },
        body: opts.body,
        signal: AbortSignal.timeout(opts.timeoutMs),
        // Never follow redirects: 301/302 turn the signed POST into a bodyless
        // GET, and a redirecting endpoint could replay the signed payload to an
        // attacker-chosen host. 3xx is a delivery failure.
        redirect: "manual",
      }),
    (cause) => `request failed: ${String(cause)}`,
  );
  if (!attempt.ok) return attempt;
  const response = attempt.value;
  // Undici ties the connection up until the body is consumed or cancelled —
  // unread 5xx bodies exhaust the socket pool under retry load.
  await response.body?.cancel().catch(() => {});
  if (!response.ok) return err(`endpoint responded ${response.status}`);
  return ok({ status: response.status });
}

export const sleep = (ms: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));
