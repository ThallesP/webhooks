# webhooks

Two packages:

| Package | For | Notes |
| --- | --- | --- |
| `@thallesp/webhooks` | Producers — define events, send webhooks | direct or outbox delivery |
| `@thallesp/webhooks-verify` | Consumers — verify incoming webhooks | zero deps, WebCrypto, runs anywhere |

Wire format is [Standard Webhooks](https://www.standardwebhooks.com/): HMAC-SHA256 over
`{id}.{timestamp}.{body}`, sent as `webhook-id` / `webhook-timestamp` / `webhook-signature`
headers. `@thallesp/webhooks-verify` also accepts `svix-*` header names, so it verifies svix-style
senders too.

## Sending

```ts
import { webhooks, direct } from "@thallesp/webhooks";
import { z } from "zod"; // any Standard Schema library works (zod v4, valibot, arktype)

const events = {
  "invoice.paid": z.object({ invoiceId: z.string(), amountCents: z.number() }),
  "user.created": z.object({ userId: z.string(), email: z.string() }),
};

const wh = webhooks({
  events,
  // usually a resolver — look subscribers up per event (a static
  // Subscriber[] also works for single-endpoint/internal setups)
  subscribers: async ({ event, subject }) =>
    db.select({ url: subs.url, secret: subs.secret })
      .from(subs)
      .where(and(eq(subs.subject, subject), arrayContains(subs.events, [event]))),
  signing: { secret: process.env.WEBHOOK_SECRET! }, // per-subscriber `secret` overrides
  delivery: direct({ maxAttempts: 5 }),             // default when omitted
});

const result = await wh.send({
  event: "invoice.paid",                   // narrows data — correlated at the type level
  subject: "acct_42",                      // what the event is about (user/account/row id)
  data: { invoiceId: "inv_1", amountCents: 4200 },
});
if (!result.ok) console.error(result.error);
// err covers: schema validation, unknown event type, non-JSON-serializable
// payload, subscriber resolver failure, missing signing secret, missing
// subject. Per-subscriber HTTP outcomes are inside result.value.deliveries.
```

### Subject

`subject` ties an event to a domain entity, in your vocabulary (`user_123`,
`acct_9`, a row id). It flows everywhere: typed into the subscriber resolver (no
digging through payloads to scope multi-tenant lookups), stored on the outbox row
(indexable — "all deliveries for acct_42" is one query), and sent inside the signed
body so consumers can trust it.

**Required by default** — a subjectless `send()` refuses to compile. Single-tenant or
internal setups where there's genuinely no subject can relax it:

```ts
const wh = webhooks({ events, subscribers, signing, subject: "optional" });

await wh.send({ event: "invoice.paid", data }); // fine with subject: "optional"
```

Direct mode delivers inline from `send()` and retries in-process with backoff.
Retries live in memory: a crash mid-retry loses them. When that matters, use the outbox.

Redirects are never followed — a 3xx response counts as a failed delivery (following
would convert the signed POST into a bodyless GET, and would replay the signed payload
to wherever the endpoint chooses to redirect).

## Outbox mode

```ts
import { webhooks, outbox } from "@thallesp/webhooks";

const wh = webhooks({
  events,
  subscribers: (event) => lookupSubscribers(event),
  signing: { secret: process.env.WEBHOOK_SECRET! },
  delivery: outbox(myAdapter, { maxAttempts: 20 }),
});

// event row commits atomically with your business data
await db.transaction(async (tx) => {
  await tx.insert(invoices).values(row);
  await wh.with(tx).send({
    event: "invoice.paid",
    subject: `acct_${row.accountId}`,
    data: { invoiceId: row.id, amountCents: row.total },
  });
});

// any process — same process, a worker dyno, or drive tick() from cron
const worker = wh.worker({ pollIntervalMs: 1000, batch: 100 });
worker.start();
```

In outbox mode `send()` only inserts rows — the worker delivers them. Adapter rejections
propagate out of `send()`, so a wrapping DB transaction rolls back as expected.

> **Outbox-only API.** `wh.with(tx)` and `wh.worker()` exist **only** when
> `delivery: outbox(...)` is configured. With `direct()` (or no `delivery` at all) those
> properties are absent from the type — calling them is a **compile error**, not a runtime
> surprise. You can't forget a worker that doesn't exist, and you can't accidentally
> depend on one in direct mode.

### Outbox adapter

Bring your own storage — five methods:

```ts
interface OutboxAdapter<Tx = unknown> {
  insert: (rows: OutboxRow[], tx?: Tx) => Promise<void>;
  claim: (limit: number, now: Date) => Promise<OutboxRow[]>; // pending rows due now
  complete: (rowId: string) => Promise<void>;
  retry: (rowId: string, attempts: number, nextAttemptAt: Date, lastError: string) => Promise<void>;
  dead: (rowId: string, attempts: number, lastError: string) => Promise<void>;
}
```

A `memoryAdapter()` ships for tests/dev. Postgres table sketch:

```sql
create table webhook_outbox (
  id text primary key,
  event_id text not null,
  event_type text not null,
  subject text, -- "all deliveries for acct_42" / dead-letter triage per tenant
  body text not null,
  url text not null,
  secret text, -- stored as-is; encrypt or resolve at delivery time if that worries you
  attempts int not null default 0,
  next_attempt_at timestamptz not null,
  status text not null default 'pending', -- pending | delivered | dead
  claimed_at timestamptz,
  last_error text,
  created_at timestamptz not null default now()
);
create index on webhook_outbox (status, next_attempt_at);
create index on webhook_outbox (subject);
```

With multiple workers, `claim` must take a **lease**, not just a lock — a plain
`select ... for update skip locked` releases its lock the moment `claim()` returns,
before delivery happens, so another worker could claim the same row. Mark the row in
the claiming statement and treat a stale mark as reclaimable (crash recovery):

```sql
update webhook_outbox set claimed_at = now()
where id in (
  select id from webhook_outbox
  where status = 'pending' and next_attempt_at <= $1
    and (claimed_at is null or claimed_at < now() - interval '2 minutes')
  order by next_attempt_at
  limit $2
  for update skip locked
)
returning *;
```

## Verifying (end clients)

`@thallesp/webhooks-verify` is the piece you hand to consumers: zero dependencies, WebCrypto only —
Node 20+, Bun, Deno, Cloudflare Workers, edge. (Stock Node 18 lacks global WebCrypto.)

```ts
import { createVerifier } from "@thallesp/webhooks-verify";

const verifier = createVerifier(process.env.WEBHOOK_SECRET!);

// fetch-style frameworks (Next.js, Hono, Remix, Workers)
export async function POST(req: Request) {
  const result = await verifier.verify(req);
  if (!result.ok) return new Response(result.reason, { status: 400 });
  result.payload; // parsed JSON body
  return new Response(null, { status: 204 });
}
```

```ts
// express — MUST use the raw body; parsed JSON breaks signatures
app.post("/hook", express.raw({ type: "application/json" }), async (req, res) => {
  const result = await verifier.verifyPayload(req.body, req.headers);
  if (!result.ok) return res.status(400).end();
  res.status(204).end();
});
```

Verification failures come back as reason codes, never exceptions: `missing_headers`,
`invalid_id` (ids containing `.` are rejected — they make the signed string ambiguous),
`invalid_timestamp`, `timestamp_out_of_tolerance` (default ±300s, `toleranceSec` to
change), `invalid_signature`, `invalid_secret`, `unreadable_body`, `invalid_json`. Constructor
misconfiguration is different — an empty secret, a `whsec_` secret with empty/malformed
base64, or a non-finite/negative `toleranceSec` throws immediately, so a broken deploy
fails at boot instead of accepting replays (or verifying against a guessable key) at
request time.

Signatures are checked over the **raw bytes** received (pass the `Buffer`/`Uint8Array`
straight through; `verify(request)` reads bytes too), and at most 8 signature
candidates from the header are tried per request.

### Duplicates (read this)

Delivery is **at-least-once** — the sender only marks an event delivered after your
2xx, so a crash or DB blip between your response and that write means you'll see the
same event again. This is inherent to webhooks (Stripe, Shopify, GitHub all document
the same contract), and the fix is yours to apply: `webhook-id` is **stable across
every retry** of an event. Dedupe on it — unique-insert the id and skip on conflict —
and processing becomes effectively exactly-once. Ordering is also not guaranteed;
don't assume event N arrives before N+1.

Secret format: `whsec_<base64>` secrets are base64-decoded (Standard Webhooks);
anything else is used as raw UTF-8 bytes. A `whsec_` secret whose payload is empty or
not valid base64 is rejected (verifier throws at construction, sender's `send()`
returns an err) — never silently reinterpreted. An unprefixed base64 string will not
interoperate with libraries that always base64-decode.

### Typed events for consumers

The sender exports its event union; consumers use it with a **type-only** import — no
runtime dependency on the sender:

```ts
// sender package
export type Events = EventUnion<typeof events>;

// consumer
import type { Events } from "acme-sdk";

const result = await verifier.verify<Events>(req);
if (result.ok) {
  result.payload.subject; // string | null — authenticated, it's inside the signed body
  switch (result.payload.event) {
    case "invoice.paid":
      result.payload.data.amountCents; // typed
  }
}
```

`EventUnion` gives **wire** types — the payload after its JSON round-trip — so a
`z.date()` field is typed `string` on the consumer side, and a `z.bigint()` field
types as `never` (it can't be sent; `send()` returns an err for it). Note the verifier
proves *who sent* the payload, not its shape: `result.payload` is parsed JSON typed as
`T` by assertion. If the signing secret is shared with parties you don't fully trust,
validate the shape with your own schema before using it.

## Development

```sh
bun install
bun test          # runtime tests
bun run typecheck # includes compile-time API tests (packages/sdk/test/types.test-d.ts)
```

Packages currently export TypeScript source (fine under Bun and bundlers). Add a build
step (e.g. tsup) with `dist/` exports before publishing to npm for plain Node
consumers.
