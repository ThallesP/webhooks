/**
 * Compile-time API contract. Checked by `tsc --noEmit`, never executed.
 * The load-bearing claims: outbox-only members don't exist in direct mode,
 * and subject is required by default (subject: "optional" relaxes it).
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

export async function subjectRequiredByDefault() {
  const wh = webhooks({
    events,
    subscribers: [],
    signing: { secret: "s" },
  });

  await wh.send({ event: "invoice.paid", subject: "acct_1", data: { invoiceId: "inv_1" } });

  // @ts-expect-error subject is required unless the config says subject: "optional"
  await wh.send({ event: "invoice.paid", data: { invoiceId: "inv_1" } });
}

export async function optionalSubjectMode() {
  const wh = webhooks({
    events,
    subscribers: [],
    signing: { secret: "s" },
    subject: "optional",
  });

  await wh.send({ event: "invoice.paid", data: { invoiceId: "inv_1" } });
  await wh.send({ event: "invoice.paid", subject: "acct_1", data: { invoiceId: "inv_1" } });

  // @ts-expect-error unknown event type
  await wh.send({ event: "nope", data: { invoiceId: "inv_1" } });

  // @ts-expect-error wrong payload shape
  await wh.send({ event: "invoice.paid", data: { invoiceId: 42 } });

  // @ts-expect-error event and data must correlate — user.created payload with invoice.paid event
  await wh.send({ event: "invoice.paid", data: { userId: "u_1" } });

  // @ts-expect-error worker() exists only with outbox delivery
  wh.worker;

  // @ts-expect-error with() exists only with outbox delivery
  wh.with;
}

export async function explicitDirectMode() {
  const wh = webhooks({
    events,
    subscribers: [],
    signing: { secret: "s" },
    delivery: direct(),
  });

  // @ts-expect-error worker() exists only with outbox delivery
  wh.worker;
}

export async function outboxMode() {
  const wh = webhooks({
    events,
    subscribers: [],
    signing: { secret: "s" },
    subject: "optional",
    delivery: outbox(memoryAdapter()),
  });

  wh.worker();
  await wh.send({ event: "invoice.paid", data: { invoiceId: "inv_1" } });
  await wh.with({ tx: true }).send({ event: "invoice.paid", data: { invoiceId: "inv_1" } });

  // @ts-expect-error unknown event type, even through with(tx)
  await wh.with({}).send({ event: "nope", data: {} });
}

export async function outboxSubjectRequired() {
  const wh = webhooks({
    events,
    subscribers: [],
    signing: { secret: "s" },
    delivery: outbox(memoryAdapter()),
  });

  await wh.with({}).send({ event: "user.created", subject: "user_1", data: { userId: "u" } });

  // @ts-expect-error subject required through with(tx) too
  await wh.with({}).send({ event: "user.created", data: { userId: "u" } });
}

export function noOutboxTypeWithoutDelivery() {
  // The old conditional-type shape let callers annotate the outbox variant
  // while omitting `delivery`, getting a direct runtime with an outbox type.
  // @ts-expect-error outbox-typed instance requires delivery: outbox(...)
  const bad: OutboxWebhooks<typeof events, unknown> = webhooks({
    events,
    subscribers: [],
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
  type Wire = Extract<EventUnion<typeof dateEvents>, { event: "thing.happened" }>;

  const wireIsString: Wire["data"]["at"] = "2026-01-01T00:00:00.000Z";
  // @ts-expect-error Date does not survive the JSON round-trip — wire type is string
  const wireIsNotDate: Wire["data"]["at"] = new Date();

  const subjectOnWire: Wire["subject"] = null;
  return [wireIsString, wireIsNotDate, subjectOnWire];
}
