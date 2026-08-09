export interface OutboxRow {
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
export interface OutboxAdapter<Tx = unknown> {
  insert: (rows: OutboxRow[], tx?: Tx) => Promise<void>;
  claim: (limit: number, now: Date) => Promise<OutboxRow[]>;
  complete: (rowId: string) => Promise<void>;
  retry: (
    rowId: string,
    attempts: number,
    nextAttemptAt: Date,
    lastError: string,
  ) => Promise<void>;
  dead: (rowId: string, attempts: number, lastError: string) => Promise<void>;
}

export interface MemoryAdapter extends OutboxAdapter<unknown> {
  rows: OutboxRow[];
  lastTx: unknown;
}

/** In-memory adapter for tests and local dev. Single worker only. */
export function memoryAdapter(): MemoryAdapter {
  const rows: OutboxRow[] = [];

  function update(rowId: string, patch: Partial<OutboxRow>) {
    const index = rows.findIndex((row) => row.id === rowId);
    if (index >= 0) rows[index] = { ...rows[index]!, ...patch };
  }

  const adapter: MemoryAdapter = {
    rows,
    lastTx: undefined,
    async insert(newRows, tx) {
      adapter.lastTx = tx;
      rows.push(...newRows.map((row) => ({ ...row })));
    },
    async claim(limit, now) {
      return rows
        .filter(
          (row) =>
            row.status === "pending" &&
            row.nextAttemptAt.getTime() <= now.getTime(),
        )
        .sort((a, b) => a.nextAttemptAt.getTime() - b.nextAttemptAt.getTime())
        .slice(0, limit)
        .map((row) => ({ ...row }));
    },
    async complete(rowId) {
      update(rowId, { status: "delivered" });
    },
    async retry(rowId, attempts, nextAttemptAt, lastError) {
      update(rowId, { attempts, nextAttemptAt, lastError });
    },
    async dead(rowId, attempts, lastError) {
      update(rowId, { status: "dead", attempts, lastError });
    },
  };
  return adapter;
}
