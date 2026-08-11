# @thallesp/webhooks

Type-safe webhook sender. Standard Webhooks wire format, two delivery modes behind
one `send()` API:

- **direct** — deliver inline with in-process retries
- **outbox** — rows commit atomically with your DB transaction via `with(tx)`; a
  polling worker delivers them (at-least-once, crash-safe)

```ts
const wh = webhooks({
  events: { "invoice.paid": z.object({ invoiceId: z.string() }) },
  subscribers: async ({ event, subject }) => lookupSubscribers(event, subject),
  signing: { secret: process.env.WEBHOOK_SECRET! },
  delivery: outbox(adapter),
});

await db.transaction(async (tx) => {
  await tx.insert(invoices).values(row);
  await wh.with(tx).send({
    event: "invoice.paid",
    subject: `acct_${row.accountId}`,
    data: { invoiceId: row.id },
  });
});

wh.worker().start();
```

`with(tx)` / `worker()` exist on the type only when delivery is `outbox(...)`.
Consumers verify with the zero-dependency [`@thallesp/webhooks-verify`](https://www.npmjs.com/package/@thallesp/webhooks-verify).

Full docs: https://github.com/ThallesP/webhooks
