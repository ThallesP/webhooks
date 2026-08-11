/**
 * Vendored subset of @standard-schema/spec v1.
 * Implemented by zod v3.24+/v4, valibot, arktype — any of them work as event schemas.
 */
interface StandardSchemaV1<Input = unknown, Output = Input> {
    readonly "~standard": StandardSchemaV1.Props<Input, Output>;
}
declare namespace StandardSchemaV1 {
    interface Props<Input = unknown, Output = Input> {
        readonly version: 1;
        readonly vendor: string;
        readonly validate: (value: unknown) => Result<Output> | Promise<Result<Output>>;
        readonly types?: Types<Input, Output> | undefined;
    }
    type Result<Output> = SuccessResult<Output> | FailureResult;
    interface SuccessResult<Output> {
        readonly value: Output;
        readonly issues?: undefined;
    }
    interface FailureResult {
        readonly issues: ReadonlyArray<Issue>;
    }
    interface Issue {
        readonly message: string;
        readonly path?: ReadonlyArray<PropertyKey | PathSegment> | undefined;
    }
    interface PathSegment {
        readonly key: PropertyKey;
    }
    interface Types<Input = unknown, Output = Input> {
        readonly input: Input;
        readonly output: Output;
    }
    type InferInput<S extends StandardSchemaV1> = NonNullable<S["~standard"]["types"]>["input"];
    type InferOutput<S extends StandardSchemaV1> = NonNullable<S["~standard"]["types"]>["output"];
}
type Result<T, E = string> = {
    ok: true;
    value: T;
} | {
    ok: false;
    error: E;
};
declare const ok: <T>(value: T) => {
    ok: true;
    value: T;
};
declare const err: <E>(error: E) => {
    ok: false;
    error: E;
};
type EventMap = Record<string, StandardSchemaV1>;
/**
 * What a value looks like after JSON.stringify + JSON.parse: Date becomes
 * string (via toJSON), bigint/function/symbol become never. Approximate but
 * catches the common wire-type lies.
 */
type Jsonify<T> = [unknown] extends [T] ? T : T extends {
    toJSON(): infer R;
} ? Jsonify<R> : T extends string | number | boolean | null ? T : T extends Array<infer U> ? Jsonify<U>[] : T extends object ? {
    [K in keyof T]: Jsonify<T[K]>;
} : never;
/**
 * Discriminated union of all events in a map, as they appear ON THE WIRE
 * (after the JSON round-trip — e.g. a z.date() field is a string here).
 * Export this from your sender app so consumers can do
 * `verifier.verify<Events>(req)` with a type-only import.
 */
type EventUnion<E extends EventMap> = {
    [K in keyof E]: {
        event: K & string;
        subject: string | null;
        data: Jsonify<StandardSchemaV1.InferOutput<E[K]>>;
    };
}[keyof E];
interface Subscriber {
    url: string;
    /** Falls back to the top-level `signing.secret` when omitted. */
    secret?: string;
}
type SubscriberResolver = (event: {
    event: string;
    subject: string | null;
    data: unknown;
}) => Subscriber[] | Promise<Subscriber[]>;

interface OutboxRow {
    id: string;
    eventId: string;
    eventType: string;
    /** What the event is about (user, account, row — caller's vocabulary). */
    subject: string | null;
    /** Pre-serialized JSON body — signed and sent verbatim at delivery time. */
    body: string;
    url: string;
    secret: string | null;
    attempts: number;
    nextAttemptAt: Date;
    status: "pending" | "delivered" | "dead";
    lastError: string | null;
    createdAt: Date;
}
/**
 * Bring-your-own-storage. `claim` must return pending rows whose
 * `nextAttemptAt <= now`; with concurrent workers, claim atomically
 * (e.g. `FOR UPDATE SKIP LOCKED` on Postgres).
 *
 * Property-style (not method) signatures on purpose: methods are bivariant, so
 * an OutboxAdapter<PgTx> would silently widen to OutboxAdapter<unknown> and
 * `with(anything)` would compile against a Tx-specific adapter.
 */
interface OutboxAdapter<Tx = unknown> {
    insert: (rows: OutboxRow[], tx?: Tx) => Promise<void>;
    claim: (limit: number, now: Date) => Promise<OutboxRow[]>;
    complete: (rowId: string) => Promise<void>;
    retry: (rowId: string, attempts: number, nextAttemptAt: Date, lastError: string) => Promise<void>;
    dead: (rowId: string, attempts: number, lastError: string) => Promise<void>;
}
interface MemoryAdapter extends OutboxAdapter<unknown> {
    rows: OutboxRow[];
    lastTx: unknown;
}
/** In-memory adapter for tests and local dev. Single worker only. */
declare function memoryAdapter(): MemoryAdapter;

interface WorkerHooks {
    onDelivered?(row: OutboxRow): void;
    onRetry?(row: OutboxRow, error: string): void;
    onDead?(row: OutboxRow, error: string): void;
    /** Row/loop-level failures (adapter down, throwing hooks, etc). Loop keeps polling. */
    onError?(error: unknown): void;
}
interface WorkerOpts {
    pollIntervalMs?: number;
    batch?: number;
    timeoutMs?: number;
    fetchImpl?: typeof fetch;
    hooks?: WorkerHooks;
}
interface TickReport {
    claimed: number;
    delivered: number;
    retried: number;
    dead: number;
}
interface OutboxWorker {
    start(): void;
    stop(): Promise<void>;
    /** One claim+deliver pass. Public so you can drive it from cron instead of start(). */
    tick(): Promise<TickReport>;
}

/**
 * `whsec_` secrets must carry non-empty base64. Falling back to the literal
 * string would silently sign/verify with a publicly guessable key ("whsec_").
 * Callers reject invalid secrets up front (send() -> err, verifier -> throw).
 */
declare function isValidSecret(secret: string): boolean;
/**
 * Standard Webhooks signature: HMAC-SHA256 over `{id}.{timestamp}.{payload}`,
 * emitted as `v1,<base64>`.
 */
declare function signPayload(secret: string, eventId: string, timestampSec: number, payload: string): Promise<string>;

interface DirectDelivery {
    kind: "direct";
    maxAttempts: number;
    /** Delay in ms before the next try, given how many attempts have failed so far (starts at 1). */
    backoffMs: (attempts: number) => number;
    timeoutMs: number;
    fetchImpl: typeof fetch;
}
interface OutboxDelivery<Tx = unknown> {
    kind: "outbox";
    adapter: OutboxAdapter<Tx>;
    maxAttempts: number;
    /** Delay in ms before the next try, given how many attempts have failed so far (starts at 1). */
    backoffMs: (attempts: number) => number;
}
type Delivery = DirectDelivery | OutboxDelivery<never>;
/** Deliver inline from send(). Retries are in-process only — a crash loses them. */
declare function direct(opts?: Partial<Omit<DirectDelivery, "kind">>): DirectDelivery;
/** send() only inserts rows; a worker() delivers them. Durable across crashes. */
declare function outbox<Tx>(adapter: OutboxAdapter<Tx>, opts?: {
    maxAttempts?: number;
    backoffMs?: (attempts: number) => number;
}): OutboxDelivery<Tx>;
interface WebhooksConfig<E extends EventMap> {
    events: E;
    /** Who receives events — a resolver (usually a DB lookup) or a static list. */
    subscribers: Subscriber[] | SubscriberResolver;
    /** Default signing secret for subscribers that don't carry their own. */
    signing?: {
        secret: string;
    };
    /** Subject is mandatory by default; "optional" relaxes it (single-tenant setups). */
    subject?: "optional";
}
interface DeliveryOutcome {
    url: string;
    delivered: boolean;
    attempts: number;
    status?: number;
    error?: string;
}
interface DirectSendReport {
    eventId: string;
    deliveries: DeliveryOutcome[];
}
interface EnqueueReport {
    eventId: string;
    enqueued: number;
}
type SubjectMode = "optional" | undefined;
/**
 * One object per send. `event` narrows `data` (correlated even when the caller
 * holds a union of event names); `subject` is what the event is about — user,
 * account, row id — in the caller's vocabulary. Mandatory unless the config
 * says subject: "optional".
 */
type SendInput<E extends EventMap, K extends keyof E & string, S extends SubjectMode> = {
    event: K;
    data: StandardSchemaV1.InferInput<E[K]>;
} & (S extends "optional" ? {
    subject?: string;
} : {
    subject: string;
});
interface DirectWebhooks<E extends EventMap, S extends SubjectMode = undefined> {
    send<K extends keyof E & string>(input: SendInput<E, K, S>): Promise<Result<DirectSendReport>>;
}
interface OutboxWebhooks<E extends EventMap, Tx, S extends SubjectMode = undefined> {
    /** Enqueue outside a transaction — adapter uses its own connection. */
    send<K extends keyof E & string>(input: SendInput<E, K, S>): Promise<Result<EnqueueReport>>;
    /** Enqueue inside your transaction — the outbox row commits with your data. */
    with(tx: Tx): {
        send<K extends keyof E & string>(input: SendInput<E, K, S>): Promise<Result<EnqueueReport>>;
    };
    worker(opts?: WorkerOpts): OutboxWorker;
}
declare function webhooks<E extends EventMap, S extends SubjectMode = undefined>(config: WebhooksConfig<E> & {
    subject?: S;
    delivery?: DirectDelivery;
}): DirectWebhooks<E, S>;
declare function webhooks<E extends EventMap, Tx, S extends SubjectMode = undefined>(config: WebhooksConfig<E> & {
    subject?: S;
    delivery: OutboxDelivery<Tx>;
}): OutboxWebhooks<E, Tx, S>;

export { type Delivery, type DeliveryOutcome, type DirectDelivery, type DirectSendReport, type DirectWebhooks, type EnqueueReport, type EventMap, type EventUnion, type Jsonify, type MemoryAdapter, type OutboxAdapter, type OutboxDelivery, type OutboxRow, type OutboxWebhooks, type OutboxWorker, type Result, type SendInput, StandardSchemaV1, type SubjectMode, type Subscriber, type SubscriberResolver, type TickReport, type WebhooksConfig, type WorkerHooks, type WorkerOpts, direct, err, isValidSecret, memoryAdapter, ok, outbox, signPayload, webhooks };
