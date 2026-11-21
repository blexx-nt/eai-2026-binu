import express from "express";
import amqp from "amqplib";
import Ajv from "ajv";
import addFormats from "ajv-formats";
import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { rename, writeFile } from "node:fs/promises";
import path from "node:path";

// ------------------------------------------------------------------ config --
const PORT = Number(process.env.PORT ?? 8802);
const AMQP_URL = process.env.RABBITMQ_URL!;
const URLS = {
  payment: (process.env.PAYMENT_URL ?? "http://payment:4001").replace(/\/+$/, ""),
  inventory: (process.env.INVENTORY_URL ?? "http://inventory:4002").replace(/\/+$/, ""),
  shipping: (process.env.SHIPPING_URL ?? "http://shipping:4003").replace(/\/+$/, ""),
  notification: (process.env.NOTIFICATION_URL ?? "http://notification:4004").replace(/\/+$/, ""),
};
const TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS ?? 2500);
const MAX_RETRIES = Number(process.env.MAX_RETRIES ?? 3);
const CONCURRENCY = Number(process.env.SAGA_CONCURRENCY ?? 5);
const DATA_DIR = process.env.DATA_DIR ?? "/data";
const SCHEMA_PATH = process.env.CANONICAL_SCHEMA ?? "/canonical/order.schema.json";
const QUEUE = "saga.queue";
const RUN_ID = randomUUID();
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

const AjvCtor: any = (Ajv as any).default ?? Ajv;
const addFormatsFn: any = (addFormats as any).default ?? addFormats;
const ajv = new AjvCtor({ allErrors: true, strict: false });
addFormatsFn(ajv);
const validateOrder = ajv.compile(JSON.parse(readFileSync(SCHEMA_PATH, "utf8")));

// ------------------------------------------------------------------- store --
const STORE_FILE = path.join(DATA_DIR, "saga-store.json");
let store: { sagas: Record<string, any>; businessKeys: Record<string, string> } = { sagas: {}, businessKeys: {} };
try {
  const raw = JSON.parse(readFileSync(STORE_FILE, "utf8"));
  store = { sagas: raw.sagas ?? {}, businessKeys: raw.businessKeys ?? {} };
} catch { /* first start */ }

let chain: Promise<unknown> = Promise.resolve();
function save(): Promise<unknown> {
  chain = chain
    .then(async () => {
      const tmp = `${STORE_FILE}.tmp`;
      await writeFile(tmp, JSON.stringify(store), "utf8");
      await rename(tmp, STORE_FILE);
    })
    .catch((e) => console.error("save failed", e));
  return chain;
}

// ------------------------------------------------------------------ broker --
let up = false;
let pubCh: any;
let traceCh: any;
const consumers: Record<string, number> = {};
let scanned = false;

function publishConfirmed(exchange: string, rk: string, content: Buffer, options: any): Promise<void> {
  return new Promise((resolve, reject) => {
    if (!up || !pubCh) return reject(new Error("broker down"));
    const t = setTimeout(() => reject(new Error("confirm timeout")), 5000);
    try {
      pubCh.publish(exchange, rk, content, options, (err: any) => {
        clearTimeout(t);
        err ? reject(err) : resolve();
      });
    } catch (e) { clearTimeout(t); reject(e); }
  });
}

function getRetryCount(msg: any): number {
  const xd = msg.properties.headers?.["x-death"];
  if (!Array.isArray(xd)) return 0;
  return xd.filter((e: any) => e?.reason === "rejected").reduce((n: number, e: any) => n + (Number(e?.count) || 0), 0);
}
function withoutRetryHistory(h: Record<string, any> | undefined) {
  const k: Record<string, any> = {};
  for (const [key, v] of Object.entries(h ?? {})) {
    if (key === "x-death" || key.startsWith("x-first-death-") || key.startsWith("x-last-death-")) continue;
    k[key] = v;
  }
  return k;
}

// -------------------------------------------------------------------- hops --
interface Hop { hopId: string; op: string; causationId: string | null; startedAt: Date }
const newHop = (op: string, causationId: string | null): Hop => ({ hopId: randomUUID(), op, causationId, startedAt: new Date() });
function endHop(cid: string, h: Hop, status: string, extra: { attempt?: number; detail?: any; messageId?: string } = {}) {
  const fin = new Date();
  const rec: any = {
    hopId: h.hopId, correlationId: cid, causationId: h.causationId, service: "orchestrator", operation: h.op,
    status, attempt: extra.attempt ?? 1, startedAt: h.startedAt.toISOString(), finishedAt: fin.toISOString(),
    durationMs: Math.max(0, fin.getTime() - h.startedAt.getTime()),
  };
  if (extra.detail) rec.detail = extra.detail;
  if (extra.messageId) rec.messageId = extra.messageId;
  try {
    traceCh?.publish("trace.events", "", Buffer.from(JSON.stringify(rec)), { persistent: true, contentType: "application/json", messageId: randomUUID() });
  } catch { /* a lost hop never fails the order */ }
}

// ------------------------------------------------------------ mock calls ----
type CallResult = { ok: true } | { ok: false; permanent: boolean; timeout: boolean; code: string };
async function call(url: string, body: any, cid: string, causation: string, key?: string): Promise<CallResult> {
  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Correlation-Id": cid, "X-Causation-Id": causation, ...(key ? { "Idempotency-Key": key } : {}) },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    if (res.ok) { await res.arrayBuffer().catch(() => {}); return { ok: true }; }
    let code = res.status >= 500 ? "service_unavailable" : `http_${res.status}`;
    try { const b: any = await res.json(); if (b && typeof b.code === "string") code = b.code; } catch { /* keep default */ }
    return { ok: false, permanent: res.status >= 400 && res.status < 500, timeout: false, code };
  } catch (e: any) {
    const timeout = e?.name === "TimeoutError" || e?.name === "AbortError";
    return { ok: false, permanent: false, timeout, code: timeout ? "timeout" : "unreachable" };
  }
}

function amountOf(order: any): string {
  let cents = 0;
  for (const i of order.items) cents += parseInt(String(i.unitPrice).replace(".", ""), 10) * i.quantity;
  return `${Math.floor(cents / 100)}.${String(cents % 100).padStart(2, "0")}`;
}
const STEP_URL: Record<string, string> = {
  payment: `${URLS.payment}/payment/authorize`,
  inventory: `${URLS.inventory}/inventory/reserve`,
  shipping: `${URLS.shipping}/shipping/create`,
  notification: `${URLS.notification}/notification/send`,
};
function stepBody(step: string, o: any) {
  if (step === "payment") return { orderId: o.orderId, amount: amountOf(o) };
  if (step === "inventory") return { orderId: o.orderId, items: o.items };
  if (step === "shipping") return { orderId: o.orderId };
  return { orderId: o.orderId, recipient: o.customer.email };
}
const stepKey = (step: string, bk: string) => (step === "payment" ? `authorize:${bk}` : step === "inventory" ? `reserve:${bk}` : undefined);
const COMP: Record<string, { op: string; url: string; key: (bk: string) => string }> = {
  inventory: { op: "inventory_release", url: `${URLS.inventory}/inventory/release`, key: (bk) => `release:${bk}` },
  payment: { op: "payment_refund", url: `${URLS.payment}/payment/refund`, key: (bk) => `refund:${bk}` },
};

// ---------------------------------------------------------------- outcomes --
function envelope(cid: string, s: any, state: string, code?: string) {
  return {
    correlationId: cid, businessKey: s.businessKey, orderId: s.meta.orderId, source: s.meta.source, orderType: s.meta.orderType,
    recipient: s.meta.recipient, state, ...(code ? { code } : {}), ...(s.duplicateOf ? { duplicateOf: s.duplicateOf } : {}),
    occurredAt: new Date().toISOString(),
  };
}
function publishEvent(rk: string, env: any, cid: string, bk: string, causation: string | null) {
  return publishConfirmed("orders.outcome", rk, Buffer.from(JSON.stringify(env)), {
    persistent: true, contentType: "application/json", messageId: randomUUID(),
    headers: { correlationId: cid, causationId: causation ?? undefined, businessKey: bk },
  });
}

// persist the new state WITH its outcome, then publish it (R-4.2.18). Caller acks afterwards.
async function finishSaga(cid: string, s: any, state: string, code: string | undefined, causation: string | null): Promise<void> {
  const rk = `order.${state}`;
  const outHop = newHop("saga.outcome", causation);
  const env = envelope(cid, s, state, code);
  s.state = state; s.code = code; s.retrying = false; s.lastHop = outHop.hopId;
  s.outcome = { routingKey: rk, envelope: env }; s.updatedAt = new Date().toISOString();
  await save();
  await publishEvent(rk, env, cid, s.businessKey, outHop.hopId);
  endHop(cid, outHop, "success", { detail: code ? { code } : undefined });
}

async function runCompensations(cid: string, s: any, completed: string[], causation: string | null):
  Promise<{ ok: true; last: string | null } | { ok: false; code: string; last: string | null }> {
  let prev = causation;
  for (const step of [...completed].reverse()) {
    const def = COMP[step];
    if (!def) continue;
    if (s.compensations.some((c: any) => c.step === def.op && c.status === "completed")) continue;
    const h = newHop(def.op, prev);
    const r = await call(def.url, { orderId: s.meta.orderId }, cid, h.hopId, def.key(s.businessKey));
    s.compensations.push({ step: def.op, status: r.ok ? "completed" : "failed", at: new Date().toISOString() });
    endHop(cid, h, r.ok ? "success" : r.timeout ? "timeout" : "failed", r.ok ? {} : { detail: { code: r.code } });
    s.lastHop = h.hopId;
    await save();
    if (!r.ok) return { ok: false, code: r.code, last: h.hopId };
    prev = h.hopId;
  }
  return { ok: true, last: prev };
}

// ------------------------------------------------------------- the consumer --
async function handle(ch: any, msg: any) {
  try {
    await handleInner(ch, msg);
  } catch (e) {
    console.error("handler error", e);
    setTimeout(() => { try { ch.nack(msg, false, true); } catch { /* channel gone */ } }, 1000);
  }
}

async function handleInner(ch: any, msg: any) {
  const ack = () => { try { ch.ack(msg); } catch { /* channel gone */ } };
  const headers: Record<string, any> = msg.properties.headers ?? {};
  const cid: string | undefined = typeof headers.correlationId === "string" ? headers.correlationId : undefined;
  let order: any;
  try {
    order = JSON.parse(msg.content.toString("utf8"));
    if (!validateOrder(order)) order = undefined;
  } catch { order = undefined; }

  // R-4.2.2: permanent failures go to the invalid channel, never retried
  if (!cid || !order) {
    const qh = cid ? newHop("invalid.quarantine", typeof headers.causationId === "string" ? headers.causationId : null) : null;
    await publishConfirmed("orders.invalid.exchange", "", msg.content, { persistent: true, headers: msg.properties.headers, messageId: randomUUID() });
    if (cid && qh) endHop(cid, qh, "quarantined");
    return ack();
  }

  const causation = typeof headers.causationId === "string" ? headers.causationId : null;
  const bk = typeof headers.businessKey === "string" ? headers.businessKey : `${order.source}:${order.orderId}`;
  const attempt = getRetryCount(msg) + 1;
  const consumeHop = newHop("saga.consume", causation);
  const existing = store.sagas[cid];
  let s: any;

  if (existing) {
    if (existing.outcome) { // R-4.2.4: a redelivery: republish the stored outcome
      const dh = newHop("saga.dedupe", consumeHop.hopId);
      await publishEvent(existing.outcome.routingKey, existing.outcome.envelope, cid, existing.businessKey, dh.hopId);
      endHop(cid, dh, "skipped");
      endHop(cid, consumeHop, "success", { attempt });
      return ack();
    }
    if (!(existing.retrying || existing.state === "parked")) { // running now, or stuck: never re-run (R-4.2.5, R-4.2.6)
      endHop(cid, consumeHop, "skipped", { attempt });
      return ack();
    }
    s = existing; // a retry, or a replay from the DLQ
  } else {
    const first = store.businessKeys[bk];
    const meta = { orderId: order.orderId, source: order.source, orderType: order.orderType, recipient: order.customer.email };
    s = { businessKey: bk, state: "processing", steps: [], compensations: [], owner: RUN_ID, retrying: false, updatedAt: new Date().toISOString(), meta };
    store.sagas[cid] = s;
    if (first && first !== cid) { // R-4.2.3: the same business key, a different correlationId
      s.state = "duplicate"; s.duplicateOf = first;
      const dh = newHop("saga.dedupe", consumeHop.hopId);
      endHop(cid, dh, "duplicate");
      await finishSaga(cid, s, "duplicate", undefined, dh.hopId);
      endHop(cid, consumeHop, "success", { attempt });
      return ack();
    }
    store.businessKeys[bk] = cid; // set synchronously: no second saga for one key
    await save();
  }

  // the semantic lock (R-4.2.7), on a new saga and on a replay from the DLQ
  if (!s.retrying) {
    s.state = "processing"; s.updatedAt = new Date().toISOString();
    await save();
    await publishEvent("order.processing", envelope(cid, s, "processing"), cid, bk, consumeHop.hopId);
  }
  s.owner = RUN_ID;

  const steps = order.orderType === "b2b" ? ["inventory", "shipping", "notification"] : ["payment", "inventory", "shipping", "notification"];
  let prev = consumeHop.hopId;

  for (let i = 0; i < steps.length; i++) {
    const step = steps[i];
    const h = newHop(step, prev);
    const r = await call(STEP_URL[step], stepBody(step, order), cid, h.hopId, stepKey(step, bk));
    if (r.ok) {
      s.steps.push({ step, status: "completed", at: new Date().toISOString() });
      s.lastHop = h.hopId; s.updatedAt = new Date().toISOString();
      endHop(cid, h, "success", { attempt });
      await save();
      prev = h.hopId;
      continue;
    }
    endHop(cid, h, r.timeout ? "timeout" : "failed", { attempt, detail: { code: r.code } });
    s.lastHop = h.hopId;

    if (i === 0) {
      if (r.permanent) { // R-4.2.15
        s.steps.push({ step, status: "failed", at: new Date().toISOString() });
        await finishSaga(cid, s, "failed", r.code, h.hopId);
        endHop(cid, consumeHop, "success", { attempt });
        return ack();
      }
      // R-4.2.12: nothing is known, the provider may have acted: retry with the same key
      if (attempt < MAX_RETRIES) {
        s.state = "processing"; s.retrying = true; s.updatedAt = new Date().toISOString();
        await save();
        const rh = newHop("saga.retry", consumeHop.hopId);
        endHop(cid, rh, "retried", { attempt, detail: { code: r.code } });
        endHop(cid, consumeHop, "success", { attempt });
        try { ch.nack(msg, false, false); } catch { /* channel gone */ }
        return;
      }
      // R-4.2.13: out of attempts: dead letter, park
      const dl = newHop("saga.dead_letter", consumeHop.hopId);
      await publishConfirmed("orders.dlq.exchange", "", msg.content, {
        persistent: true, contentType: "application/json", messageId: randomUUID(),
        headers: { ...withoutRetryHistory(headers), correlationId: cid, causationId: dl.hopId, businessKey: bk, originQueue: QUEUE, lastError: `${step} ${r.code}` },
      });
      endHop(cid, dl, "dead_lettered", { attempt, detail: { code: r.code } });
      s.state = "parked"; s.retrying = false; s.lastHop = dl.hopId; s.updatedAt = new Date().toISOString();
      await save();
      await publishEvent("order.parked", envelope(cid, s, "parked", r.code), cid, bk, dl.hopId);
      endHop(cid, consumeHop, "success", { attempt });
      return ack();
    }

    // R-4.2.16: a later step failed: compensate in reverse
    s.steps.push({ step, status: "failed", at: new Date().toISOString() });
    s.failureCode = r.code;
    await save();
    const completed = s.steps.filter((x: any) => x.status === "completed").map((x: any) => x.step);
    const c = await runCompensations(cid, s, completed, h.hopId);
    if (c.ok) await finishSaga(cid, s, "compensated", r.code, c.last ?? h.hopId);
    else await finishSaga(cid, s, "compensation_failed", c.code, c.last ?? h.hopId);
    endHop(cid, consumeHop, "success", { attempt });
    return ack();
  }

  await finishSaga(cid, s, "completed", undefined, prev);
  endHop(cid, consumeHop, "success", { attempt });
  ack();
}

// R-4.7.1: a saga whose steps ran under a previous run of this process is stuck
async function startupScan() {
  if (scanned) return;
  for (const [cid, s] of Object.entries<any>(store.sagas)) {
    if (s.state === "processing" && !s.retrying && !s.outcome && s.owner !== RUN_ID) {
      const h = newHop("saga.stuck", s.lastHop ?? null);
      endHop(cid, h, "success");
      await finishSaga(cid, s, "needs_attention", undefined, h.hopId);
    }
  }
  scanned = true;
}

async function connect(): Promise<void> {
  for (;;) {
    let c: any;
    try {
      c = await amqp.connect(AMQP_URL);
      const cc = await c.createChannel();
      const pc = await c.createConfirmChannel();
      const tc = await c.createChannel();
      pubCh = pc; traceCh = tc; up = true;
      let handled = false;
      const down = () => {
        if (handled) return;
        handled = true; up = false; pubCh = undefined; traceCh = undefined; delete consumers[QUEUE];
        c.close().catch(() => {});
        setTimeout(() => void connect(), 1000);
      };
      c.on("error", () => {});
      c.on("close", down);
      for (const ch of [cc, pc, tc]) { ch.on("error", () => {}); ch.on("close", () => { c.close().catch(() => {}); }); }
      await startupScan();
      await cc.prefetch(CONCURRENCY);
      await cc.consume(QUEUE, (msg: any) => { if (msg) void handle(cc, msg); });
      consumers[QUEUE] = 1;
      return;
    } catch (e) {
      up = false;
      try { await c?.close(); } catch { /* ignore */ }
      await sleep(2000);
    }
  }
}

const app = express();
const busy = new Set<string>();

app.get("/health", (_req, res) => {
  const ok = up && consumers[QUEUE] === 1;
  res.status(ok ? 200 : 503).json({ status: ok ? "ok" : "degraded", checks: { broker: up ? "up" : "down", consumers: { ...consumers } } });
});

app.get("/sagas/stuck", (_req, res) => {
  const sagas = Object.entries<any>(store.sagas)
    .filter(([, s]) => s.state === "needs_attention" || s.state === "compensation_failed")
    .map(([cid, s]) => ({
      correlationId: cid, businessKey: s.businessKey, state: s.state,
      completedSteps: s.steps.filter((x: any) => x.status === "completed").map((x: any) => x.step),
      compensatedSteps: s.compensations.filter((x: any) => x.status === "completed").map((x: any) => x.step),
      since: s.updatedAt,
    }));
  res.json({ sagas });
});

app.get("/sagas/:cid", (req, res) => {
  const s = store.sagas[req.params.cid];
  if (!s) return res.status(404).json({ code: "not_found" });
  res.json({ state: s.state, code: s.code, duplicateOf: s.duplicateOf, updatedAt: s.updatedAt });
});

app.post("/sagas/:cid/compensate", async (req, res) => {
  const cid = req.params.cid;
  const s = store.sagas[cid];
  if (!s) return res.status(404).json({ code: "not_found" });
  if (busy.has(cid)) return res.status(409).json({ code: "compensation_in_progress" });
  if (s.state === "compensated" && s.operatorCompensation) return res.json({ correlationId: cid, state: "compensated" });
  if (s.state !== "needs_attention" && s.state !== "compensation_failed") return res.status(409).json({ code: "saga_not_stuck" });
  busy.add(cid);
  try {
    const prior = s.state;
    const h = newHop("saga.operator_compensate", s.lastHop ?? null);
    const completed = s.steps.filter((x: any) => x.status === "completed").map((x: any) => x.step);
    const r = await runCompensations(cid, s, completed, h.hopId);
    s.operatorCompensation = { at: new Date().toISOString(), from: prior };
    endHop(cid, h, r.ok ? "success" : "failed");
    if (r.ok) await finishSaga(cid, s, "compensated", prior === "needs_attention" ? "operator_compensated" : (s.failureCode ?? "operator_compensated"), r.last ?? h.hopId);
    else await finishSaga(cid, s, "compensation_failed", r.code, r.last ?? h.hopId);
    res.json({ correlationId: cid, state: s.state });
  } catch (e) {
    console.error(e);
    res.status(500).json({ code: "internal_error" });
  } finally {
    busy.delete(cid);
  }
});

app.listen(PORT, () => console.log(`orchestrator listening on ${PORT}`));
void connect();
