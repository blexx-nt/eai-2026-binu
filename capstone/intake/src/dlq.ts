import express from "express";
import { randomUUID } from "node:crypto";

type Deps = { getCh: () => any; getTrace: () => any; confirm: (ex: string, rk: string, c: Buffer, o: any) => Promise<void> };
const str = (v: any) => (v === undefined || v === null ? undefined : String(v));
const hdr = (m: any) => m.properties.headers ?? {};
function stripRetry(h: Record<string, any>) {
  const k: Record<string, any> = {};
  for (const [key, v] of Object.entries(h)) {
    if (key === "x-death" || key.startsWith("x-first-death-") || key.startsWith("x-last-death-")) continue;
    k[key] = v;
  }
  return k;
}
async function drain(ch: any): Promise<any[]> {
  const out: any[] = [];
  for (;;) { const m = await ch.get("orders.dlq", { noAck: false }); if (!m) break; out.push(m); }
  return out;
}
const entry = (m: any) => { const h = hdr(m); return {
  messageId: String(m.properties.messageId ?? "unknown"), correlationId: str(h.correlationId) ?? "", originQueue: str(h.originQueue) ?? "",
  attempts: Number(h.attempts) || Number(process.env.MAX_RETRIES ?? 3), lastError: str(h.lastError) ?? "",
  deadLetteredAt: str(h.deadLetteredAt) ?? new Date().toISOString() }; };

export function registerDlq(app: any, deps: Deps) {
  let busy: Promise<unknown> = Promise.resolve();
  const lock = <T>(fn: () => Promise<T>): Promise<T> => { const p = busy.then(fn, fn); busy = p.catch(() => {}); return p; };
  const down = (res: any) => res.status(503).set("Retry-After", "1").json({ code: "broker_unavailable" });
  function emit(cid: string | undefined, id: string, start: Date, causation: string | undefined) {
    if (!cid) return;
    const fin = new Date();
    const rec = { hopId: id, correlationId: cid, causationId: causation ?? null, service: "intake", operation: "dlq.replay", status: "success", attempt: 1,
      startedAt: start.toISOString(), finishedAt: fin.toISOString(), durationMs: Math.max(0, fin.getTime() - start.getTime()) };
    try { deps.getTrace()?.publish("trace.events", "", Buffer.from(JSON.stringify(rec)), { persistent: true, contentType: "application/json", messageId: randomUUID() }); } catch { /* ignore */ }
  }
  app.get("/dlq", async (_req: any, res: any) => {
    try {
      res.json(await lock(async () => {
        const ch = deps.getCh(); if (!ch) throw new Error("down");
        const ms = await drain(ch); const entries = ms.map(entry);
        if (ms.length) ch.nackAll(true);
        return { count: entries.length, entries };
      }));
    } catch { down(res); }
  });
  app.post("/dlq/replay", express.json(), async (req: any, res: any) => {
    const only = req.body && typeof req.body === "object" ? str(req.body.correlationId) : undefined;
    try {
      res.json(await lock(async () => {
        const ch = deps.getCh(); if (!ch) throw new Error("down");
        const ms = await drain(ch); const keep: any[] = []; let replayed = 0;
        for (const m of ms) {
          const h = hdr(m); const cid = str(h.correlationId);
          if (only && cid !== only) { keep.push(m); continue; }
          const id = randomUUID(); const start = new Date();
          const { originQueue, lastError, attempts, deadLetteredAt, ...rest } = stripRetry(h);
          try {
            await deps.confirm("orders.return.exchange", str(originQueue) ?? "saga.queue", m.content,
              { persistent: true, contentType: "application/json", messageId: randomUUID(), headers: { ...rest, causationId: id } });
            ch.ack(m); replayed++; emit(cid, id, start, str(h.causationId));
          } catch { keep.push(m); }
        }
        for (const m of keep) ch.nack(m, false, true);
        return { replayed, remaining: keep.length };
      }));
    } catch { down(res); }
  });
}
