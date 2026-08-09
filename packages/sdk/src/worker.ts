import { deliverOnce } from "./deliver";
import type { OutboxAdapter, OutboxRow } from "./outbox";

export interface WorkerHooks {
  onDelivered?(row: OutboxRow): void;
  onRetry?(row: OutboxRow, error: string): void;
  onDead?(row: OutboxRow, error: string): void;
  /** Row/loop-level failures (adapter down, throwing hooks, etc). Loop keeps polling. */
  onError?(error: unknown): void;
}

export interface WorkerOpts {
  pollIntervalMs?: number;
  batch?: number;
  timeoutMs?: number;
  fetchImpl?: typeof fetch;
  hooks?: WorkerHooks;
}

export interface TickReport {
  claimed: number;
  delivered: number;
  retried: number;
  dead: number;
}

export interface OutboxWorker {
  start(): void;
  stop(): Promise<void>;
  /** One claim+deliver pass. Public so you can drive it from cron instead of start(). */
  tick(): Promise<TickReport>;
}

export interface WorkerConfig {
  adapter: OutboxAdapter<never>;
  maxAttempts: number;
  backoffMs: (attempts: number) => number;
  defaultSecret: string | undefined;
}

/** Run a user hook without letting its throw poison the loop or stop(). */
function safeHook(invoke: () => void): Promise<void> {
  return Promise.resolve()
    .then(invoke)
    .catch(() => {});
}

export function createWorker(config: WorkerConfig, opts: WorkerOpts = {}): OutboxWorker {
  const pollIntervalMs = opts.pollIntervalMs ?? 1_000;
  const batch = opts.batch ?? 100;
  const timeoutMs = opts.timeoutMs ?? 10_000;
  const fetchImpl = opts.fetchImpl ?? fetch;
  const hooks = opts.hooks ?? {};
  const { adapter, maxAttempts, backoffMs, defaultSecret } = config;

  let running = false;
  // Bumped on every start() and stop(). A tick spawned under an older
  // generation must not reschedule — otherwise start() during a pending
  // stop() resurrects the old loop and two loops poll concurrently.
  let generation = 0;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const inflight = new Set<Promise<unknown>>();

  async function handleRow(row: OutboxRow): Promise<"delivered" | "retried" | "dead"> {
    const secret = row.secret ?? defaultSecret;
    if (!secret) {
      await adapter.dead(row.id, row.attempts, "no signing secret");
      await safeHook(() => hooks.onDead?.(row, "no signing secret"));
      return "dead";
    }
    const result = await deliverOnce({
      url: row.url,
      secret,
      eventId: row.eventId,
      body: row.body,
      timeoutMs,
      fetchImpl,
    });
    if (result.ok) {
      await adapter.complete(row.id);
      await safeHook(() => hooks.onDelivered?.(row));
      return "delivered";
    }
    const attempts = row.attempts + 1;
    if (attempts >= maxAttempts) {
      await adapter.dead(row.id, attempts, result.error);
      await safeHook(() => hooks.onDead?.(row, result.error));
      return "dead";
    }
    await adapter.retry(
      row.id,
      attempts,
      new Date(Date.now() + backoffMs(attempts)),
      result.error,
    );
    await safeHook(() => hooks.onRetry?.(row, result.error));
    return "retried";
  }

  async function tick(): Promise<TickReport> {
    const rows = await adapter.claim(batch, new Date());
    const report: TickReport = { claimed: rows.length, delivered: 0, retried: 0, dead: 0 };
    // allSettled: one row's adapter failure must not abandon the other rows
    // mid-flight (a fail-fast reject would let the next tick re-claim and
    // double-deliver them while these attempts are still running).
    const outcomes = await Promise.allSettled(rows.map((row) => handleRow(row)));
    for (const outcome of outcomes) {
      if (outcome.status === "rejected") {
        await safeHook(() => hooks.onError?.(outcome.reason));
        continue;
      }
      report[outcome.value]++;
    }
    return report;
  }

  function runTick(gen: number) {
    const promise: Promise<unknown> = tick()
      .catch((error: unknown) => safeHook(() => hooks.onError?.(error)))
      .finally(() => {
        inflight.delete(promise);
        if (running && gen === generation) {
          timer = setTimeout(() => runTick(gen), pollIntervalMs);
        }
      });
    inflight.add(promise);
  }

  return {
    tick,
    start() {
      if (running) return;
      running = true;
      generation++;
      runTick(generation);
    },
    async stop() {
      running = false;
      generation++;
      clearTimeout(timer);
      // Every inflight promise already has catch attached — this never rejects.
      await Promise.all([...inflight]);
    },
  };
}
