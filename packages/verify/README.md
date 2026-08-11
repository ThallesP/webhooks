# @thallesp/webhooks-verify

Zero-dependency webhook signature verification. WebCrypto only — Node 20+, Bun,
Deno, Cloudflare Workers, edge. Verifies [Standard Webhooks](https://www.standardwebhooks.com/)
signatures (`svix-*` headers accepted too), so it works with svix-style senders as
well as [`@thallesp/webhooks`](https://www.npmjs.com/package/@thallesp/webhooks).

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

// express / fastify — pass the RAW body
const result = await verifier.verifyPayload(req.body, req.headers);
```

Failures are reason codes, never exceptions. Signatures are checked over the raw
bytes received. Delivery is at-least-once — dedupe on `result.id` (stable across
retries).

Full docs: https://github.com/ThallesP/webhooks
