"use strict";
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);

// src/index.ts
var index_exports = {};
__export(index_exports, {
  direct: () => direct,
  err: () => err,
  isValidSecret: () => isValidSecret,
  memoryAdapter: () => memoryAdapter,
  ok: () => ok,
  outbox: () => outbox,
  signPayload: () => signPayload,
  webhooks: () => webhooks
});
module.exports = __toCommonJS(index_exports);

// src/types.ts
var ok = (value) => ({ ok: true, value });
var err = (error) => ({ ok: false, error });
var toResult = (fn, describe) => Promise.resolve().then(fn).then(ok, (cause) => err(describe(cause)));

// src/signing.ts
var encoder = new TextEncoder();
function base64Encode(data) {
  const bytes = new Uint8Array(data);
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
}
function base64Decode(input) {
  if (input.length % 4 !== 0 || !/^[A-Za-z0-9+/]*={0,2}$/.test(input)) return null;
  const binary = atob(input);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
}
function isValidSecret(secret) {
  if (!secret.startsWith("whsec_")) return secret.length > 0;
  const decoded = base64Decode(secret.slice("whsec_".length));
  return decoded !== null && decoded.length > 0;
}
function secretBytes(secret) {
  if (secret.startsWith("whsec_")) {
    const decoded = base64Decode(secret.slice("whsec_".length));
    if (decoded && decoded.length > 0) return decoded;
  }
  return encoder.encode(secret);
}
async function signPayload(secret, eventId, timestampSec, payload) {
  const key = await crypto.subtle.importKey(
    "raw",
    secretBytes(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const signature = await crypto.subtle.sign(
    "HMAC",
    key,
    encoder.encode(`${eventId}.${timestampSec}.${payload}`)
  );
  return `v1,${base64Encode(signature)}`;
}

// src/deliver.ts
async function deliverOnce(opts) {
  const timestampSec = Math.floor(Date.now() / 1e3);
  const signature = await signPayload(opts.secret, opts.eventId, timestampSec, opts.body);
  const attempt = await toResult(
    () => opts.fetchImpl(opts.url, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "webhook-id": opts.eventId,
        "webhook-timestamp": String(timestampSec),
        "webhook-signature": signature
      },
      body: opts.body,
      signal: AbortSignal.timeout(opts.timeoutMs),
      // Never follow redirects: 301/302 turn the signed POST into a bodyless
      // GET, and a redirecting endpoint could replay the signed payload to an
      // attacker-chosen host. 3xx is a delivery failure.
      redirect: "manual"
    }),
    (cause) => `request failed: ${String(cause)}`
  );
  if (!attempt.ok) return attempt;
  const response = attempt.value;
  await response.body?.cancel().catch(() => {
  });
  if (!response.ok) return err(`endpoint responded ${response.status}`);
  return ok({ status: response.status });
}
var sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// src/worker.ts
function safeHook(invoke) {
  return Promise.resolve().then(invoke).catch(() => {
  });
}
function createWorker(config, opts = {}) {
  const pollIntervalMs = opts.pollIntervalMs ?? 1e3;
  const batch = opts.batch ?? 100;
  const timeoutMs = opts.timeoutMs ?? 1e4;
  const fetchImpl = opts.fetchImpl ?? fetch;
  const hooks = opts.hooks ?? {};
  const { adapter, maxAttempts, backoffMs, defaultSecret } = config;
  let running = false;
  let generation = 0;
  let timer;
  const inflight = /* @__PURE__ */ new Set();
  async function handleRow(row) {
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
      fetchImpl
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
      result.error
    );
    await safeHook(() => hooks.onRetry?.(row, result.error));
    return "retried";
  }
  async function tick() {
    const rows = await adapter.claim(batch, /* @__PURE__ */ new Date());
    const report = { claimed: rows.length, delivered: 0, retried: 0, dead: 0 };
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
  function runTick(gen) {
    const promise = tick().catch((error) => safeHook(() => hooks.onError?.(error))).finally(() => {
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
      await Promise.all([...inflight]);
    }
  };
}

// src/outbox.ts
function memoryAdapter() {
  const rows = [];
  function update(rowId, patch) {
    const index = rows.findIndex((row) => row.id === rowId);
    if (index >= 0) rows[index] = { ...rows[index], ...patch };
  }
  const adapter = {
    rows,
    lastTx: void 0,
    async insert(newRows, tx) {
      adapter.lastTx = tx;
      rows.push(...newRows.map((row) => ({ ...row })));
    },
    async claim(limit, now) {
      return rows.filter(
        (row) => row.status === "pending" && row.nextAttemptAt.getTime() <= now.getTime()
      ).sort((a, b) => a.nextAttemptAt.getTime() - b.nextAttemptAt.getTime()).slice(0, limit).map((row) => ({ ...row }));
    },
    async complete(rowId) {
      update(rowId, { status: "delivered" });
    },
    async retry(rowId, attempts, nextAttemptAt, lastError) {
      update(rowId, { attempts, nextAttemptAt, lastError });
    },
    async dead(rowId, attempts, lastError) {
      update(rowId, { status: "dead", attempts, lastError });
    }
  };
  return adapter;
}

// src/index.ts
function checkMaxAttempts(value) {
  if (!Number.isInteger(value) || value < 1) {
    throw new RangeError(`maxAttempts must be a positive integer, got ${value}`);
  }
  return value;
}
function direct(opts = {}) {
  return {
    kind: "direct",
    maxAttempts: checkMaxAttempts(opts.maxAttempts ?? 5),
    backoffMs: opts.backoffMs ?? ((attempts) => Math.min(3e4, 500 * 2 ** (attempts - 1))),
    timeoutMs: opts.timeoutMs ?? 1e4,
    fetchImpl: opts.fetchImpl ?? fetch
  };
}
function outbox(adapter, opts = {}) {
  return {
    kind: "outbox",
    adapter,
    maxAttempts: checkMaxAttempts(opts.maxAttempts ?? 20),
    backoffMs: opts.backoffMs ?? ((attempts) => Math.min(36e5, 3e4 * 2 ** (attempts - 1)))
  };
}
function webhooks(config) {
  const delivery = config.delivery ?? direct();
  if (delivery.kind === "outbox") {
    const api2 = {
      send: (input) => enqueue(config, delivery, input, void 0),
      with: (tx) => ({
        send: (input) => enqueue(config, delivery, input, tx)
      }),
      worker: (opts) => createWorker(
        {
          adapter: delivery.adapter,
          maxAttempts: delivery.maxAttempts,
          backoffMs: delivery.backoffMs,
          defaultSecret: config.signing?.secret
        },
        opts
      )
    };
    return api2;
  }
  const api = {
    send: (input) => sendDirect(config, delivery, input)
  };
  return api;
}
function safeStringify(value) {
  try {
    return ok(JSON.stringify(value));
  } catch (cause) {
    return err(String(cause));
  }
}
async function prepare(config, input) {
  const { event, data } = input;
  const subject = input.subject ?? null;
  if (config.subject !== "optional" && subject === null) {
    return err(`event "${event}" requires a subject`);
  }
  if (subject !== null && subject.length === 0) {
    return err("subject must be a non-empty string");
  }
  const schema = Object.hasOwn(config.events, event) ? config.events[event] : void 0;
  if (!schema) return err(`unknown event type "${event}"`);
  const validation = await toResult(
    () => schema["~standard"].validate(data),
    (cause) => `schema validation threw for "${event}": ${String(cause)}`
  );
  if (!validation.ok) return validation;
  const outcome = validation.value;
  if (outcome.issues) {
    const detail = outcome.issues.map((issue) => issue.message).join("; ");
    return err(`invalid payload for "${event}": ${detail}`);
  }
  const body = safeStringify({ event, subject, data: outcome.value });
  if (!body.ok) return err(`payload for "${event}" is not JSON-serializable: ${body.error}`);
  const eventId = `evt_${crypto.randomUUID()}`;
  const subscribers = config.subscribers;
  const resolved = Array.isArray(subscribers) ? ok(subscribers) : await toResult(
    () => subscribers({ event, subject, data: outcome.value }),
    (cause) => `subscriber resolver failed: ${String(cause)}`
  );
  if (!resolved.ok) return resolved;
  const targets = [];
  for (const subscriber of resolved.value) {
    const secret = subscriber.secret ?? config.signing?.secret;
    if (!secret) return err(`no signing secret for subscriber ${subscriber.url}`);
    if (!isValidSecret(secret)) {
      return err(
        `invalid signing secret for subscriber ${subscriber.url}: "whsec_" secrets must contain non-empty base64`
      );
    }
    targets.push({ url: subscriber.url, secret });
  }
  return ok({ eventId, subject, body: body.value, targets });
}
async function sendDirect(config, delivery, input) {
  const prepared = await prepare(config, input);
  if (!prepared.ok) return prepared;
  const { eventId, body, targets } = prepared.value;
  const deliveries = await Promise.all(
    targets.map(async (target) => {
      let lastError = "";
      for (let attempt = 1; attempt <= delivery.maxAttempts; attempt++) {
        if (attempt > 1) await sleep(delivery.backoffMs(attempt - 1));
        const result = await deliverOnce({
          url: target.url,
          secret: target.secret,
          eventId,
          body,
          timeoutMs: delivery.timeoutMs,
          fetchImpl: delivery.fetchImpl
        });
        if (result.ok) {
          return { url: target.url, delivered: true, attempts: attempt, status: result.value.status };
        }
        lastError = result.error;
      }
      return { url: target.url, delivered: false, attempts: delivery.maxAttempts, error: lastError };
    })
  );
  return ok({ eventId, deliveries });
}
async function enqueue(config, delivery, input, tx) {
  const prepared = await prepare(config, input);
  if (!prepared.ok) return prepared;
  const { eventId, subject, body, targets } = prepared.value;
  const now = /* @__PURE__ */ new Date();
  const rows = targets.map((target) => ({
    id: `obx_${crypto.randomUUID()}`,
    eventId,
    eventType: input.event,
    subject,
    body,
    url: target.url,
    secret: target.secret,
    attempts: 0,
    nextAttemptAt: now,
    status: "pending",
    lastError: null,
    createdAt: now
  }));
  if (rows.length > 0) await delivery.adapter.insert(rows, tx);
  return ok({ eventId, enqueued: rows.length });
}
// Annotate the CommonJS export names for ESM import in node:
0 && (module.exports = {
  direct,
  err,
  isValidSecret,
  memoryAdapter,
  ok,
  outbox,
  signPayload,
  webhooks
});
