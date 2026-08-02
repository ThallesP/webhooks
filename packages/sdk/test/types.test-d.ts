/**
 * Compile-time API contract. Checked by `tsc --noEmit`, never executed.
 * The load-bearing claims: outbox-only members don't exist in direct mode.
 */
import { z } from "zod";
import {
  type EventUnion,
  type OutboxAdapter,
  type OutboxWebhooks,
  direct,
  memoryAdapter,
  outbox,
  webhooks,
} from "../src/index";

const events = {
  "invoice.paid": z.object({ invoiceId: z.string() }),
};

export async function directMode() {
  const wh = webhooks({
    events,
    endpoints: [],
    signing: { secret: "s" },
  });

  await wh.send("invoice.paid", { invoiceId: "inv_1" });

  // @ts-expect-error unknown event type
  await wh.send("nope", { invoiceId: "inv_1" });

  // @ts-expect-error wrong payload shape
  await wh.send("invoice.paid", { invoiceId: 42 });

  // @ts-expect-error worker() exists only with outbox delivery
  wh.worker;

  // @ts-expect-error with() exists only with outbox delivery
  wh.with;
}

export async function explicitDirectMode() {
  const wh = webhooks({
    events,
    endpoints: [],
    signing: { secret: "s" },
    delivery: direct(),
  });

  // @ts-expect-error worker() exists only with outbox delivery
  wh.worker;
}

export async function outboxMode() {
  const wh = webhooks({
    events,
    endpoints: [],
    signing: { secret: "s" },
    delivery: outbox(memoryAdapter()),
  });

  wh.worker();
  await wh.send("invoice.paid", { invoiceId: "inv_1" });
  await wh.with({ tx: true }).send("invoice.paid", { invoiceId: "inv_1" });

  // @ts-expect-error unknown event type, even through with(tx)
  await wh.with({}).send("nope", {});
}

export function noOutboxTypeWithoutDelivery() {
  // The old conditional-type shape let callers annotate the outbox variant
  // while omitting `delivery`, getting a direct runtime with an outbox type.
  // @ts-expect-error outbox-typed instance requires delivery: outbox(...)
  const bad: OutboxWebhooks<typeof events, unknown> = webhooks({
    events,
    endpoints: [],
    signing: { secret: "s" },
  });
  return bad;
}

export function noAdapterTxWidening() {
  const pgAdapter = {} as OutboxAdapter<{ query: (sql: string) => Promise<void> }>;
  // @ts-expect-error a Tx-specific adapter must not widen to OutboxAdapter<unknown> —
  // that would let with(anything) compile against it (method bivariance hole)
  const widened: OutboxAdapter<unknown> = pgAdapter;
  return widened;
}

export function eventUnionReflectsJsonWire() {
  const dateEvents = {
    "thing.happened": z.object({ at: z.date() }),
  };
  type Wire = Extract<EventUnion<typeof dateEvents>, { type: "thing.happened" }>["data"]["at"];

  const wireIsString: Wire = "2026-01-01T00:00:00.000Z";
  // @ts-expect-error Date does not survive the JSON round-trip — wire type is string
  const wireIsNotDate: Wire = new Date();
  return [wireIsString, wireIsNotDate];
}
