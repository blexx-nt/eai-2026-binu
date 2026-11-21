import express from "express";
import amqp from "amqplib";
import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { appendFile, rename, writeFile } from "node:fs/promises";
import path from "node:path";

const PORT = Number(process.env.PORT ?? 8803);
const AMQP_URL = process.env.RABBITMQ_URL!;
const NOTIFY_URL = (process.env.NOTIFICATION_URL ?? "http://notification:4004").replace(/\/+$/, "");
const TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS ?? 2500);
const MAX_RETRIES = Number(process.env.MAX_RETRIES ?? 3);
const DATA_DIR = process.env.DATA_DIR ?? "/data";
const QUEUE = "notifications.queue";
const IDS_FILE = path.join(DATA_DIR, "processed-ids.json");
const LOG_FILE = path.join(DATA_DIR, "notification.log");
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

const seen = new Set<string>();
try { for (const id of JSON.parse(readFileSync(IDS_FILE, "utf8"))) seen.add(String(id)); } catch { /* first start */ }
let chain: Promise<unknown> = Promise.resolve();
function saveIds(): Promise<unknown> {
  chain = chain.then(async () => {
    await writeFile(IDS_FILE + ".tmp", JSON.stringify([...seen]), "utf8");
    await rename(IDS_FILE + ".tmp", IDS_FILE);
  }).catch((e) => console.error("save failed", e));
  return chain;
}

let up = false;
let pubCh: any;
let traceCh: any;
const consumers: Record<string, number> = {};

function publishConfirmed(exchange: string, rk: string, content: Buffer, options: any): Promise<void> {
  return new Promise((resolve, reject) => {
    if (!up || !pubCh) return reject(new Error("broker down"));
    const t = setTimeout(() => reject(new Error("confirm timeout")), 5000);
    try { pubCh.publish(exchange, rk, content, options, (err: any) => { clearTimeout(t); err ? reject(err) : resolve(); }); }
    catch (e) { clearTimeout(t); reject(e); }
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

interface Hop { hopId: string; op: string; causationId: string | null; startedAt: Date }
const newHop = (op: string, causationId: string | null): Hop => ({ hopId: randomUUID(), op, causationId, startedAt: new Date() });
function endHop(cid: string, h: Hop, status: string, attempt = 1, detail?: any) {
  const fin = new Date();
  const rec: any = { hopId: h.hopId, correlationId: cid, causationId: h.causationId, service: "notifier", operation: h.op, status, attempt,
    startedAt: h.startedAt.toISOString(), finishedAt: fin.toISOString(), durationMs: Math.max(0, fin.getTime() - h.startedAt.getTime()) };
  if (detail) rec.detail = detail;
  try { traceCh?.publish("trace.events", "", Buffer.from(JSON.stringify(rec)), { persistent: true, contentType: "application/json", messageId: randomUUID() }); } catch { /* ignore */ }
}

async function send(ev: any, cid: string, causation: string): Promise<{ ok: true } | { ok: false; timeout: boolean; code: string }> {
  try {
    const res = await fetch(NOTIFY_URL + "/notification/send", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Correlation-Id": cid, "X-Causation-Id": causation },
      body: JSON.stringify({ orderId: ev.orderId, recipient: ev.recipient, kind: "order_not_completed", reason: ev.code ?? ev.state }),
      signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    if (res.ok) { await res.arrayBuffer().catch(() => {}); return { ok: true }; }
    return { ok: false, timeout: false, code: "http_" + res.status };
  } catch (e: any) {
    const t = e?.name === "TimeoutError" || e?.name === "AbortError";
    return { ok: false, timeout: t, code: t ? "timeout" : "unreachable" };
  }
}

async function handle(ch: any, msg: any) {
  try { await inner(ch, msg); } catch (e) {
    console.error("handler error", e);
    setTimeout(() => { try { ch.nack(msg, false, true); } catch { /* gone */ } }, 1000);
  }
}
async function inner(ch: any, msg: any) {
  const ack = () => { try { ch.ack(msg); } catch { /* gone */ } };
  const headers: Record<string, any> = msg.properties.headers ?? {};
  const cid = typeof headers.correlationId === "string" ? headers.correlationId : undefined;
  const causation = typeof headers.causationId === "string" ? headers.causationId : null;
  let ev: any;
  try { ev = JSON.parse(msg.content.toString("utf8")); } catch { ev = undefined; }
  if (!ev || typeof ev !== "object" || typeof ev.orderId !== "string" || typeof ev.recipient !== "string") ev = undefined;
  if (!cid || !ev) {
    const qh = cid ? newHop("invalid.quarantine", causation) : null;
    await publishConfirmed("orders.invalid.exchange", "", msg.content, { persistent: true, headers: msg.properties.headers, messageId: randomUUID() });
    if (cid && qh) endHop(cid, qh, "quarantined");
    return ack();
  }
  const attempt = getRetryCount(msg) + 1;
  const consume = newHop("notify.consume", causation);
  if (seen.has(cid)) {
    endHop(cid, newHop("notify.duplicate", consume.hopId), "duplicate", attempt);
    endHop(cid, consume, "success", attempt);
    return ack();
  }
  const sendHop = newHop("notify.send", consume.hopId);
  const r = await send(ev, cid, sendHop.hopId);
  if (r.ok) {
    await appendFile(LOG_FILE, JSON.stringify({ at: new Date().toISOString(), correlationId: cid, orderId: ev.orderId, recipient: ev.recipient, reason: ev.code ?? ev.state }) + "\n");
    seen.add(cid);
    await saveIds();
    endHop(cid, sendHop, "success", attempt);
    endHop(cid, consume, "success", attempt);
    return ack();
  }
  endHop(cid, sendHop, r.timeout ? "timeout" : "failed", attempt, { code: r.code });
  if (attempt < MAX_RETRIES) {
    endHop(cid, newHop("notify.retry", consume.hopId), "retried", attempt, { code: r.code });
    endHop(cid, consume, "success", attempt);
    try { ch.nack(msg, false, false); } catch { /* gone */ }
    return;
  }
  const dl = newHop("notify.dead_letter", consume.hopId);
  await publishConfirmed("orders.dlq.exchange", "", msg.content, { persistent: true, contentType: "application/json", messageId: randomUUID(),
    headers: { ...withoutRetryHistory(headers), correlationId: cid, causationId: dl.hopId, originQueue: QUEUE, lastError: "notification " + r.code, attempts: attempt, deadLetteredAt: new Date().toISOString() } });
  endHop(cid, dl, "dead_lettered", attempt, { code: r.code });
  endHop(cid, consume, "success", attempt);
  ack();
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
      await cc.prefetch(5);
      await cc.consume(QUEUE, (msg: any) => { if (msg) void handle(cc, msg); });
      consumers[QUEUE] = 1;
      return;
    } catch {
      up = false;
      try { await c?.close(); } catch { /* ignore */ }
      await sleep(2000);
    }
  }
}

const app = express();
app.get("/health", (_req, res) => {
  const ok = up && consumers[QUEUE] === 1;
  res.status(ok ? 200 : 503).json({ status: ok ? "ok" : "degraded", checks: { broker: up ? "up" : "down", consumers: { ...consumers } } });
});
app.listen(PORT, () => console.log("notifier listening on " + PORT));
void connect();
