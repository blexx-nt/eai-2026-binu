import express from "express";
import amqp from "amqplib";
import { readFileSync } from "node:fs";
import { appendFile } from "node:fs/promises";
import path from "node:path";

const PORT = Number(process.env.PORT ?? 8801);
const AMQP_URL = process.env.RABBITMQ_URL!;
const MGMT_URL = (process.env.MGMT_URL ?? "http://rabbitmq:15672").replace(/\/+$/, "");
const MGMT_AUTH = "Basic " + Buffer.from(process.env.MGMT_AUTH ?? "eai:eai-capstone").toString("base64");
const ORCH_URL = (process.env.ORCHESTRATOR_URL ?? "http://orchestrator:8802").replace(/\/+$/, "");
const DATA_DIR = process.env.DATA_DIR ?? "/data";
const FILE = path.join(DATA_DIR, "trace-store.jsonl");
const QUEUE = "trace.store";
const QUEUES = ["saga.queue", "notifications.queue", "orders.dlq", "orders.invalid", "trace.store"];
const LAG_SAGA = 10;
const LAG_NOTIFIER = 10;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

const byCid = new Map<string, any[]>();
const seenHops = new Set<string>();
function addHop(h: any) {
  seenHops.add(h.hopId);
  const a = byCid.get(h.correlationId) ?? [];
  a.push(h);
  byCid.set(h.correlationId, a);
}
try {
  for (const line of readFileSync(FILE, "utf8").split("\n")) {
    if (!line.trim()) continue;
    try { const h = JSON.parse(line); if (h?.hopId && h?.correlationId && !seenHops.has(h.hopId)) addHop(h); } catch { /* skip */ }
  }
} catch { /* first start */ }

let up = false;
let mch: any;
const consumers: Record<string, number> = {};

async function unacked(q: string): Promise<number> {
  try {
    const r = await fetch(`${MGMT_URL}/api/queues/%2F/${encodeURIComponent(q)}`, { headers: { Authorization: MGMT_AUTH }, signal: AbortSignal.timeout(1500) });
    if (!r.ok) return 0;
    const j: any = await r.json();
    return Number(j.messages_unacknowledged ?? 0);
  } catch { return 0; }
}

async function onHop(ch: any, msg: any) {
  try {
    let h: any;
    try { h = JSON.parse(msg.content.toString("utf8")); } catch { h = undefined; }
    const good = h && typeof h.hopId === "string" && typeof h.correlationId === "string" && typeof h.operation === "string";
    if (good && !seenHops.has(h.hopId)) {
      await appendFile(FILE, JSON.stringify(h) + "\n");
      addHop(h);
    }
    ch.ack(msg);
  } catch (e) {
    console.error("trace error", e);
    setTimeout(() => { try { ch.nack(msg, false, true); } catch { /* gone */ } }, 1000);
  }
}

async function connect(): Promise<void> {
  for (;;) {
    let c: any;
    try {
      c = await amqp.connect(AMQP_URL);
      const cc = await c.createChannel();
      const mc = await c.createChannel();
      mch = mc; up = true;
      let handled = false;
      const down = () => {
        if (handled) return;
        handled = true; up = false; mch = undefined; delete consumers[QUEUE];
        c.close().catch(() => {});
        setTimeout(() => void connect(), 1000);
      };
      c.on("error", () => {});
      c.on("close", down);
      for (const ch of [cc, mc]) { ch.on("error", () => {}); ch.on("close", () => { c.close().catch(() => {}); }); }
      await cc.prefetch(20);
      await cc.consume(QUEUE, (msg: any) => { if (msg) void onHop(cc, msg); });
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

app.get("/trace/:cid", (req, res) => {
  const hops = byCid.get(req.params.cid);
  if (!hops || hops.length === 0) return res.status(404).json({ code: "not_found" });
  const sorted = [...hops].sort((a, b) => String(a.startedAt).localeCompare(String(b.startedAt)));
  res.json({ correlationId: req.params.cid, hops: sorted });
});

app.get("/ops/metrics", async (_req, res) => {
  try {
    if (!up || !mch) throw new Error("broker down");
    const queues: Record<string, any> = {};
    for (const q of QUEUES) {
      const ok = await mch.checkQueue(q);
      queues[q] = { ready: ok.messageCount, unacked: await unacked(q), consumers: ok.consumerCount };
    }
    let stuckSagas = 0;
    try {
      const r = await fetch(ORCH_URL + "/sagas/stuck", { signal: AbortSignal.timeout(1500) });
      if (r.ok) { const j: any = await r.json(); stuckSagas = Array.isArray(j.sagas) ? j.sagas.length : 0; }
    } catch { /* orchestrator unreachable */ }
    const dlqDepth = queues["orders.dlq"].ready + queues["orders.dlq"].unacked;
    const invalidDepth = queues["orders.invalid"].ready + queues["orders.invalid"].unacked;
    const a = (key: string, value: number, firing: boolean, threshold: number | null) => ({ key, firing, value, threshold });
    const sagaC = queues["saga.queue"].consumers, notC = queues["notifications.queue"].consumers;
    const sagaR = queues["saga.queue"].ready, notR = queues["notifications.queue"].ready;
    res.json({
      generatedAt: new Date().toISOString(), queues, dlqDepth, invalidDepth, stuckSagas,
      alerts: [
        a("dlq_not_empty", dlqDepth, dlqDepth > 0, null),
        a("invalid_not_empty", invalidDepth, invalidDepth > 0, null),
        a("saga_consumer_missing", sagaC, sagaC === 0, null),
        a("notifier_consumer_missing", notC, notC === 0, null),
        a("saga_consumer_lag", sagaR, sagaR > LAG_SAGA, LAG_SAGA),
        a("notifier_consumer_lag", notR, notR > LAG_NOTIFIER, LAG_NOTIFIER),
        a("stuck_sagas", stuckSagas, stuckSagas > 0, null),
      ],
    });
  } catch {
    res.status(503).json({ code: "broker_unavailable" });
  }
});

app.listen(PORT, () => console.log("observer listening on " + PORT));
void connect();
