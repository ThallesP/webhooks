/**
 * Compile-time API contract. Checked by `tsc --noEmit`, never executed.
 * The load-bearing claims: outbox-only members don't exist in direct mode,
 * and subject: "required" makes subjectless sends refuse to compile.
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
  "user.created": z.object({ userId: z.string() }),
};

export async function directMode() {
  const wh = webhooks({
    events,
    endpoints: [],
    signing: { secret: "s" },
  });

  await wh.send({ type: "invoice.paid", data: { invoiceId: "inv_1" } });
  await wh.send({ type: "invoice.paid", subject: "acct_1", data: { invoiceId: "inv_1" } });

  // @ts-expect-error unknown event type
  await wh.send({ type: "nope", data: { invoiceId: "inv_1" } });

  // @ts-expect-error wrong payload shape
  await wh.send({ type: "invoice.paid", data: { invoiceId: 42 } });

  // @ts-expect-error type and data must correlate — user.created payload with invoice.paid type
  await wh.send({ type: "invoice.paid", data: { userId: "u_1" } });

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

export async function requiredSubjectMode() {
  const wh = webhooks({
    events,
    endpoints: [],
    signing: { secret: "s" },
    subject: "required",
  });

  await wh.send({ type: "invoice.paid", subject: "acct_1", data: { invoiceId: "inv_1" } });

  // @ts-expect-error subject is required by this config
  await wh.send({ type: "invoice.paid", data: { invoiceId: "inv_1" } });
}

export async function outboxMode() {
  const wh = webhooks({
    events,
    endpoints: [],
    signing: { secret: "s" },
    delivery: outbox(memoryAdapter()),
  });

  wh.worker();
  await wh.send({ type: "invoice.paid", data: { invoiceId: "inv_1" } });
  await wh.with({ tx: true }).send({ type: "invoice.paid", data: { invoiceId: "inv_1" } });

  // @ts-expect-error unknown event type, even through with(tx)
  await wh.with({}).send({ type: "nope", data: {} });
}

export async function outboxRequiredSubjectMode() {
  const wh = webhooks({
    events,
    endpoints: [],
    signing: { secret: "s" },
    subject: "required",
    delivery: outbox(memoryAdapter()),
  });

  await wh.with({}).send({ type: "user.created", subject: "user_1", data: { userId: "u" } });

  // @ts-expect-error subject required through with(tx) too
  await wh.with({}).send({ type: "user.created", data: { userId: "u" } });
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
  type Wire = Extract<EventUnion<typeof dateEvents>, { type: "thing.happened" }>;

  const wireIsString: Wire["data"]["at"] = "2026-01-01T00:00:00.000Z";
  // @ts-expect-error Date does not survive the JSON round-trip — wire type is string
  const wireIsNotDate: Wire["data"]["at"] = new Date();

  const subjectOnWire: Wire["subject"] = null;
  return [wireIsString, wireIsNotDate, subjectOnWire];
}
