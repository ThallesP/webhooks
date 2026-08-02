import {
  type Endpoint,
  type EndpointResolver,
  type EventMap,
  type Result,
  type StandardSchemaV1,
  err,
  ok,
  toResult,
} from "./types";
import { deliverOnce, sleep } from "./deliver";
import { isValidSecret } from "./signing";
import type { OutboxAdapter, OutboxRow } from "./outbox";
import { type OutboxWorker, type WorkerOpts, createWorker } from "./worker";

export type {
  Endpoint,
  EndpointResolver,
  EventMap,
  EventUnion,
  Jsonify,
  Result,
  StandardSchemaV1,
} from "./types";
export { ok, err } from "./types";
export { isValidSecret, signPayload } from "./signing";
export {
  type MemoryAdapter,
  type OutboxAdapter,
  type OutboxRow,
  memoryAdapter,
} from "./outbox";
export type {
  OutboxWorker,
  TickReport,
  WorkerHooks,
  WorkerOpts,
} from "./worker";

export interface DirectDelivery {
  kind: "direct";
  maxAttempts: number;
  /** Delay in ms before the next try, given how many attempts have failed so far (starts at 1). */
  backoffMs: (attempts: number) => number;
  timeoutMs: number;
  fetchImpl: typeof fetch;
}

export interface OutboxDelivery<Tx = unknown> {
  kind: "outbox";
  adapter: OutboxAdapter<Tx>;
  maxAttempts: number;
  /** Delay in ms before the next try, given how many attempts have failed so far (starts at 1). */
  backoffMs: (attempts: number) => number;
}

// Internal erased form: Tx is never, the bottom type every OutboxAdapter<Tx>
// is assignable to. The webhooks() overloads carry the real Tx; enqueue casts
// back with `tx as never`.
export type Delivery = DirectDelivery | OutboxDelivery<never>;

// NaN/0/fractional maxAttempts would mean zero requests in direct mode or
// infinite retries in outbox mode (attempts >= NaN is never true). Fail at boot.
function checkMaxAttempts(value: number): number {
  if (!Number.isInteger(value) || value < 1) {
    throw new RangeError(`maxAttempts must be a positive integer, got ${value}`);
  }
  return value;
}

/** Deliver inline from send(). Retries are in-process only — a crash loses them. */
export function direct(
  opts: Partial<Omit<DirectDelivery, "kind">> = {},
): DirectDelivery {
  return {
    kind: "direct",
    maxAttempts: checkMaxAttempts(opts.maxAttempts ?? 5),
    backoffMs: opts.backoffMs ?? ((attempts) => Math.min(30_000, 500 * 2 ** (attempts - 1))),
    timeoutMs: opts.timeoutMs ?? 10_000,
    fetchImpl: opts.fetchImpl ?? fetch,
  };
}

/** send() only inserts rows; a worker() delivers them. Durable across crashes. */
export function outbox<Tx>(
  adapter: OutboxAdapter<Tx>,
  opts: { maxAttempts?: number; backoffMs?: (attempts: number) => number } = {},
): OutboxDelivery<Tx> {
  return {
    kind: "outbox",
    adapter,
    maxAttempts: checkMaxAttempts(opts.maxAttempts ?? 20),
    backoffMs:
      opts.backoffMs ?? ((attempts) => Math.min(3_600_000, 30_000 * 2 ** (attempts - 1))),
  };
}

export interface WebhooksConfig<E extends EventMap> {
  events: E;
  endpoints: Endpoint[] | EndpointResolver;
  /** Default signing secret for endpoints that don't carry their own. */
  signing?: { secret: string };
}

export interface DeliveryOutcome {
  url: string;
  delivered: boolean;
  attempts: number;
  status?: number;
  error?: string;
}

export interface DirectSendReport {
  eventId: string;
  endpoints: DeliveryOutcome[];
}

export interface EnqueueReport {
  eventId: string;
  enqueued: number;
}

type SendData<E extends EventMap, K extends keyof E> = StandardSchemaV1.InferInput<E[K]>;

export interface DirectWebhooks<E extends EventMap> {
  send<K extends keyof E & string>(
    type: K,
    data: SendData<E, K>,
  ): Promise<Result<DirectSendReport>>;
}

export interface OutboxWebhooks<E extends EventMap, Tx> {
  /** Enqueue outside a transaction — adapter uses its own connection. */
  send<K extends keyof E & string>(
    type: K,
    data: SendData<E, K>,
  ): Promise<Result<EnqueueReport>>;
  /** Enqueue inside your transaction — the outbox row commits with your data. */
  with(tx: Tx): {
    send<K extends keyof E & string>(
      type: K,
      data: SendData<E, K>,
    ): Promise<Result<EnqueueReport>>;
  };
  worker(opts?: WorkerOpts): OutboxWorker;
}

// Overloads instead of one conditional type: `delivery` must be PRESENT to get
// the outbox API. With a single generic signature and `delivery?: D`, callers
// could annotate D = OutboxDelivery<Tx>, omit delivery, and receive an
// outbox-typed instance whose runtime is direct — worker() would crash.
export function webhooks<E extends EventMap>(
  config: WebhooksConfig<E> & { delivery?: DirectDelivery },
): DirectWebhooks<E>;
export function webhooks<E extends EventMap, Tx>(
  config: WebhooksConfig<E> & { delivery: OutboxDelivery<Tx> },
): OutboxWebhooks<E, Tx>;
export function webhooks<E extends EventMap>(
  config: WebhooksConfig<E> & { delivery?: Delivery },
): DirectWebhooks<E> | OutboxWebhooks<E, unknown> {
  const delivery: Delivery = config.delivery ?? direct();

  if (delivery.kind === "outbox") {
    const api: OutboxWebhooks<E, unknown> = {
      send: (type, data) => enqueue(config, delivery, type, data, undefined),
      with: (tx) => ({
        send: (type, data) => enqueue(config, delivery, type, data, tx),
      }),
      worker: (opts) =>
        createWorker(
          {
            adapter: delivery.adapter,
            maxAttempts: delivery.maxAttempts,
            backoffMs: delivery.backoffMs,
            defaultSecret: config.signing?.secret,
          },
          opts,
        ),
    };
    return api;
  }

  const api: DirectWebhooks<E> = {
    send: (type, data) => sendDirect(config, delivery, type, data),
  };
  return api;
}

interface Prepared {
  eventId: string;
  body: string;
  targets: { url: string; secret: string }[];
}

/** The one Result boundary around JSON.stringify (throws on bigint, cycles, …). */
function safeStringify(value: unknown): Result<string> {
  try {
    return ok(JSON.stringify(value));
  } catch (cause) {
    return err(String(cause));
  }
}

async function prepare<E extends EventMap>(
  config: WebhooksConfig<E>,
  type: string,
  data: unknown,
): Promise<Result<Prepared>> {
  // Own-property lookup: `config.events["toString"]` must not resolve to
  // Object.prototype members and then explode on `["~standard"]`.
  const schema = Object.hasOwn(config.events, type) ? config.events[type] : undefined;
  if (!schema) return err(`unknown event type "${type}"`);

  // toResult also contains cross-realm thenables from the validator.
  const validation = await toResult(
    () => schema["~standard"].validate(data),
    (cause) => `schema validation threw for "${type}": ${String(cause)}`,
  );
  if (!validation.ok) return validation;
  const outcome = validation.value;
  if (outcome.issues) {
    const detail = outcome.issues.map((issue) => issue.message).join("; ");
    return err(`invalid payload for "${type}": ${detail}`);
  }

  const body = safeStringify({ type, data: outcome.value });
  if (!body.ok) return err(`payload for "${type}" is not JSON-serializable: ${body.error}`);
  const eventId = `evt_${crypto.randomUUID()}`;

  const endpoints = config.endpoints;
  const resolved = Array.isArray(endpoints)
    ? ok(endpoints)
    : await toResult(
        () => endpoints({ type, data: outcome.value }),
        (cause) => `endpoint resolver failed: ${String(cause)}`,
      );
  if (!resolved.ok) return resolved;

  const targets: Prepared["targets"] = [];
  for (const endpoint of resolved.value) {
    const secret = endpoint.secret ?? config.signing?.secret;
    if (!secret) return err(`no signing secret for endpoint ${endpoint.url}`);
    if (!isValidSecret(secret)) {
      return err(
        `invalid signing secret for endpoint ${endpoint.url}: "whsec_" secrets must contain non-empty base64`,
      );
    }
    targets.push({ url: endpoint.url, secret });
  }
  return ok({ eventId, body: body.value, targets });
}

async function sendDirect<E extends EventMap>(
  config: WebhooksConfig<E>,
  delivery: DirectDelivery,
  type: string,
  data: unknown,
): Promise<Result<DirectSendReport>> {
  const prepared = await prepare(config, type, data);
  if (!prepared.ok) return prepared;
  const { eventId, body, targets } = prepared.value;

  const endpoints = await Promise.all(
    targets.map(async (target): Promise<DeliveryOutcome> => {
      let lastError = "";
      for (let attempt = 1; attempt <= delivery.maxAttempts; attempt++) {
        if (attempt > 1) await sleep(delivery.backoffMs(attempt - 1));
        const result = await deliverOnce({
          url: target.url,
          secret: target.secret,
          eventId,
          body,
          timeoutMs: delivery.timeoutMs,
          fetchImpl: delivery.fetchImpl,
        });
        if (result.ok) {
          return { url: target.url, delivered: true, attempts: attempt, status: result.value.status };
        }
        lastError = result.error;
      }
      return { url: target.url, delivered: false, attempts: delivery.maxAttempts, error: lastError };
    }),
  );
  return ok({ eventId, endpoints });
}

async function enqueue<E extends EventMap>(
  config: WebhooksConfig<E>,
  delivery: OutboxDelivery<never>,
  type: string,
  data: unknown,
  tx: unknown,
): Promise<Result<EnqueueReport>> {
  const prepared = await prepare(config, type, data);
  if (!prepared.ok) return prepared;
  const { eventId, body, targets } = prepared.value;

  const now = new Date();
  const rows: OutboxRow[] = targets.map((target) => ({
    id: `obx_${crypto.randomUUID()}`,
    eventId,
    type,
    body,
    url: target.url,
    secret: target.secret,
    attempts: 0,
    nextAttemptAt: now,
    status: "pending",
    lastError: null,
    createdAt: now,
  }));

  // Adapter rejections propagate: inside a transaction that's what rolls it back.
  if (rows.length > 0) await delivery.adapter.insert(rows, tx as never);
  return ok({ eventId, enqueued: rows.length });
}
