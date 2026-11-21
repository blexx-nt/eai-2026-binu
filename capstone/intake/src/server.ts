import express from "express";
import amqp from "amqplib";
import Ajv from "ajv";
import addFormats from "ajv-formats";
import { XMLParser } from "fast-xml-parser";
import { createHash, randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { rename, writeFile } from "node:fs/promises";
import path from "node:path";

// ------------------------------------------------------------------ config --
const PORT = Number(process.env.PORT ?? 8800);
const AMQP_URL = process.env.RABBITMQ_URL!;
const PRICING_URL = (process.env.PRICING_URL ?? "http://pricing:4100").replace(/\/+$/, "");
const PRICING_KEY = process.env.PRICING_API_KEY ?? "pa4-pricing-key-2026";
const TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS ?? 2500);
const ITEMS_MAX = Number(process.env.ORDER_ITEMS_MAX ?? 50);
const DATA_DIR = process.env.DATA_DIR ?? "/data";
const SCHEMA_PATH = process.env.CANONICAL_SCHEMA ?? "/canonical/order.schema.json";
const ORCH_URL = (process.env.ORCHESTRATOR_URL || "http://orchestrator:8802");
const KEY_RE = /^[A-Za-z0-9._:-]{1,128}$/;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// -------------------------------------------------------------- validation --
const AjvCtor: any = (Ajv as any).default ?? Ajv;
const addFormatsFn: any = (addFormats as any).default ?? addFormats;
const ajv = new AjvCtor({ allErrors: true, strict: false });
addFormatsFn(ajv);
const validateOrder = ajv.compile(JSON.parse(readFileSync(SCHEMA_PATH, "utf8")));

// ------------------------------------------------------------------ stores --
const IDEM_FILE = path.join(DATA_DIR, "idempotency-store.json");
const ORDERS_FILE = path.join(DATA_DIR, "orders.json");
function load(file: string): Record<string, any> {
  try { return JSON.parse(readFileSync(file, "utf8")); } catch { return {}; }
}
const idem: Record<string, any> = load(IDEM_FILE);
for (const k of Object.keys(idem)) if (idem[k]?.state === "in_flight") delete idem[k]; // died with the old run
const orders: Record<string, any> = load(ORDERS_FILE);

async function atomicWrite(file: string, text: string) {
  const tmp = `${file}.tmp`;
  await writeFile(tmp, text, "utf8");
  await rename(tmp, file);
}
let chain: Promise<unknown> = Promise.resolve();
function persistAll(): Promise<unknown> {
  chain = chain
    .then(async () => {
      const done = Object.fromEntries(Object.entries(idem).filter(([, v]) => v.state === "done"));
      await atomicWrite(IDEM_FILE, JSON.stringify(done));
      await atomicWrite(ORDERS_FILE, JSON.stringify(orders));
    })
    .catch((e) => console.error("persist failed", e));
  return chain;
}

// ------------------------------------------------------------------ broker --
let up = false;
let pubCh: any;
let traceCh: any;
async function connect(): Promise<void> {
  for (;;) {
    try {
      const c: any = await amqp.connect(AMQP_URL);
      const pc = await c.createConfirmChannel();
      const tc = await c.createChannel();
      pubCh = pc; traceCh = tc; up = true;
      let handled = false;
      const down = () => {
        if (handled) return;
        handled = true; up = false; pubCh = undefined; traceCh = undefined;
        c.close().catch(() => {});
        setTimeout(() => void connect(), 1000);
      };
      c.on("error", () => {});
      c.on("close", down);
      for (const ch of [pc, tc]) {
        ch.on("error", () => {});
        ch.on("close", () => { c.close().catch(() => {}); });
      }
      return;
    } catch {
      await sleep(2000);
    }
  }
}

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

// -------------------------------------------------------------------- hops --
interface Hop { hopId: string; operation: string; causationId: string | null; startedAt: Date; finishedAt?: Date }
function newHop(operation: string, causationId: string | null): Hop {
  return { hopId: randomUUID(), operation, causationId, startedAt: new Date() };
}
function emitHops(cid: string, hops: Hop[], publishedMessageId: string) {
  for (const h of hops) {
    const fin = h.finishedAt ?? new Date();
    const rec: any = {
      hopId: h.hopId, correlationId: cid, causationId: h.causationId,
      service: "intake", operation: h.operation, status: "success", attempt: 1,
      startedAt: h.startedAt.toISOString(), finishedAt: fin.toISOString(),
      durationMs: Math.max(0, fin.getTime() - h.startedAt.getTime()),
    };
    if (h.operation === "intake.publish") rec.messageId = publishedMessageId;
    try {
      traceCh?.publish("trace.events", "", Buffer.from(JSON.stringify(rec)), {
        persistent: true, contentType: "application/json", messageId: randomUUID(),
      });
    } catch { /* a lost hop never fails the order */ }
  }
}

// ------------------------------------------------------------- translators --
type Lines = Array<{ productId: string; productName: string; quantity: number }>;
interface Draft { base: any; lines: Lines }

const toIso = (v: any) => new Date(v).toISOString(); // throws on invalid -> 400
const currencyMap: Record<number, string> = { 978: "EUR", 840: "USD", 826: "GBP" };
const mobileStatus: Record<number, string> = { 1: "new", 2: "processing", 3: "shipped", 4: "delivered" };

function translateWeb(raw: any): Draft {
  const c = raw.customer, a = c.address;
  return {
    base: {
      orderId: raw.orderId, orderType: raw.orderType, source: "web", orderDate: toIso(raw.orderDate),
      customer: { name: c.name, email: c.email,
        address: { street: a.street, city: a.city, postalCode: a.postalCode, country: a.country } },
      currency: raw.currency, status: raw.status,
    },
    lines: raw.items.map((i: any) => ({ productId: String(i.productId).trim(), productName: i.productName, quantity: i.quantity })),
  };
}

function parseMobileAddress(addr: string) {
  const parts = String(addr).split(",").map((p) => p.trim());
  if (parts.length < 4) throw new Error(`cannot parse addr`);
  const country = parts.pop()!, postalCode = parts.pop()!, city = parts.pop()!;
  return { street: parts.join(", "), city, postalCode, country };
}

function translateMobile(raw: any): Draft {
  const status = mobileStatus[raw.st];
  if (!status) throw new Error(`unknown st ${raw.st}`);
  if (typeof raw.ts !== "number") throw new Error("ts must be a number");
  return {
    base: {
      orderId: raw.oid, orderType: raw.ot, source: "mobile", orderDate: new Date(raw.ts * 1000).toISOString(),
      customer: { name: raw.cust_name, email: raw.cust_email, address: parseMobileAddress(raw.addr) },
      currency: currencyMap[raw.cur] ?? String(raw.cur), status,
    },
    lines: raw.items.map((i: any) => ({ productId: `PROD-${String(i.pid).trim()}`, productName: i.pname, quantity: i.qty })),
  };
}

const xmlParser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "@_", parseTagValue: false, parseAttributeValue: false });

function decodeXml(bytes: Buffer, charsetParam?: string): string {
  let enc = charsetParam;
  if (!enc) {
    const m = /^\s*<\?xml[^>]*encoding\s*=\s*["']([^"']+)["']/i.exec(bytes.subarray(0, 200).toString("latin1"));
    enc = m?.[1] ?? "utf-8";
  }
  return new TextDecoder(enc).decode(bytes); // throws on an unknown label -> 400
}

function translateB2B(bytes: Buffer, charsetParam?: string): Draft {
  const po: any = (xmlParser.parse(decodeXml(bytes, charsetParam)) as any).PurchaseOrder;
  const ship = po.BuyerParty.ShipToAddress;
  const rawItems = po.LineItems.LineItem;
  const list: any[] = Array.isArray(rawItems) ? rawItems : rawItems == null ? [] : [rawItems];
  const mapStatus = String(po.Status).trim().toLowerCase();
  if (!["new", "processing", "shipped", "delivered"].includes(mapStatus)) throw new Error("unknown status");
  return {
    base: {
      orderId: String(po["@_orderId"]), orderType: po["@_orderType"], source: "b2b", orderDate: toIso(po["@_orderDate"]),
      customer: { name: String(po.BuyerParty.Name), email: String(po.BuyerParty.ContactEmail),
        address: { street: String(ship.Street), city: String(ship.City), postalCode: String(ship.PostalCode), country: String(ship["@_country"]) } },
      currency: String(po.LineItems["@_currency"]), status: mapStatus,
    },
    lines: list.map((li) => ({
      productId: String(li["@_sku"]).trim().replace(/^SKU-/, ""),
      productName: String(li.Description), quantity: Number(li["@_quantity"]),
    })),
  };
}

// ---------------------------------------------------------------- enrichment --
type Priced = { kind: "ok"; unitPrice: string; currency: string; taxRate: number } | { kind: "unknown" } | { kind: "error" };
async function price(productId: string, cid: string, causationId: string): Promise<Priced> {
  try {
    const res = await fetch(`${PRICING_URL}/pricing/${encodeURIComponent(productId)}`, {
      headers: { "X-API-Key": PRICING_KEY, "X-Correlation-Id": cid, "X-Causation-Id": causationId },
      signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    if (res.status === 404) return { kind: "unknown" };
    if (!res.ok) return { kind: "error" };
    const b: any = await res.json();
    if (typeof b.unitPrice !== "number" || !Number.isFinite(b.unitPrice)) return { kind: "error" };
    return { kind: "ok", unitPrice: b.unitPrice.toFixed(2), currency: b.currency, taxRate: b.taxRate };
  } catch {
    return { kind: "error" };
  }
}

// ----------------------------------------------------------------- the flow --
interface Result { status: number; body: any; keep: boolean; headers?: Record<string, string>; cid?: string }
const bad = (message: string): Result => ({ status: 400, body: { code: "validation_error", message }, keep: false });

async function processOrder(body: Buffer, contentType: string | undefined): Promise<Result> {
  const [mediaRaw, ...params] = (contentType ?? "").split(";");
  const media = mediaRaw.trim().toLowerCase();
  const charset = params.map((p) => p.trim()).find((p) => /^charset\s*=/i.test(p))?.split("=")[1]?.trim().replace(/^"|"$/g, "");
  const isXml = media === "application/xml" || media === "text/xml";
  if (!isXml && media !== "application/json") {
    return { status: 415, body: { code: "unsupported_media_type", message: "use application/json or application/xml" }, keep: false };
  }

  const cid = randomUUID();
  const receive = newHop("intake.receive", null);
  const translateHop = newHop("intake.translate", receive.hopId);
  receive.finishedAt = translateHop.startedAt;

  // 1. detect + translate
  let draft: Draft;
  try {
    if (isXml) {
      draft = translateB2B(body, charset);
    } else {
      let json: any;
      try { json = JSON.parse(body.toString("utf8")); } catch { return bad("body is not valid JSON"); }
      if (json === null || typeof json !== "object" || Array.isArray(json)) return bad("body must be a JSON object");
      if (json.orderId !== undefined && json.customer && typeof json.customer === "object") draft = translateWeb(json);
      else if (json.oid !== undefined) draft = translateMobile(json);
      else return bad("not a recognised order shape");
    }
  } catch (e: any) {
    return bad(`cannot translate order: ${e?.message ?? e}`);
  }
  translateHop.finishedAt = new Date();

  // 2. enrich from pricing
  const enrichHop = newHop("intake.enrich", translateHop.hopId);
  const priced = await Promise.all(draft.lines.map((l) => price(l.productId, cid, enrichHop.hopId)));
  enrichHop.finishedAt = new Date();
  if (priced.some((p) => p.kind === "error")) {
    return { status: 503, body: { code: "pricing_unavailable" }, keep: false, headers: { "Retry-After": "1" } };
  }
  const unknown = [...new Set(draft.lines.filter((_, i) => priced[i].kind === "unknown").map((l) => l.productId))];
  if (unknown.length > 0) {
    return { status: 422, body: { code: "unknown_product", productIds: unknown }, keep: true };
  }

  // 3. build (this is also the content filter: payment data is never copied), validate
  const order = {
    ...draft.base,
    receivedAt: new Date().toISOString(),
    items: draft.lines.map((l, i) => {
      const p = priced[i] as Extract<Priced, { kind: "ok" }>;
      return { productId: l.productId, productName: l.productName, quantity: l.quantity, unitPrice: p.unitPrice, currency: p.currency, taxRate: p.taxRate };
    }),
  };
  if (!validateOrder(order)) return bad(`canonical validation failed: ${ajv.errorsText(validateOrder.errors)}`);
  if (order.items.length > ITEMS_MAX) return bad(`too many items (max ${ITEMS_MAX})`);

  // 4. publish, confirmed
  const publishHop = newHop("intake.publish", enrichHop.hopId);
  const businessKey = `${order.source}:${order.orderId}`;
  const messageId = randomUUID();
  try {
    await publishConfirmed("orders.exchange", "", Buffer.from(JSON.stringify(order)), {
      persistent: true, contentType: "application/json", messageId,
      headers: { correlationId: cid, causationId: publishHop.hopId, businessKey },
    });
  } catch {
    return { status: 503, body: { code: "broker_unavailable" }, keep: false, headers: { "Retry-After": "1" } };
  }
  publishHop.finishedAt = new Date();

  orders[cid] = {
    correlationId: cid, businessKey, orderId: order.orderId, source: order.source,
    orderType: order.orderType, state: "accepted", updatedAt: new Date().toISOString(),
  };
  emitHops(cid, [receive, translateHop, enrichHop, publishHop], messageId);
  return { status: 202, body: { correlationId: cid, status: "accepted" }, keep: true, cid };
}

// --------------------------------------------------------------------- http --
const app = express();

app.post("/orders", express.raw({ type: () => true, limit: "1mb" }), async (req, res) => {
  const body = Buffer.isBuffer(req.body) ? req.body : Buffer.alloc(0);
  const key = req.header("idempotency-key");
  if (!key || !KEY_RE.test(key)) {
    return res.status(400).json({ code: "validation_error", message: "Idempotency-Key header is required (1-128 chars of A-Za-z0-9._:-)" });
  }
  const sha = createHash("sha256").update(body).digest("hex");
  const ex = idem[key];
  if (ex) {
    if (ex.bodySha256 !== sha) return res.status(409).json({ code: "idempotency_payload_mismatch" });
    if (ex.state === "in_flight") return res.status(409).json({ code: "idempotency_conflict" });
    return res.status(ex.response.status).json(ex.response.body);
  }
  idem[key] = { bodySha256: sha, state: "in_flight", createdAt: new Date().toISOString() };

  let r: Result;
  try {
    r = await processOrder(body, req.header("content-type"));
  } catch (e) {
    console.error(e);
    r = { status: 500, body: { code: "internal_error" }, keep: false };
  }
  if (r.keep) {
    idem[key] = { ...idem[key], state: "done", response: { status: r.status, body: r.body }, ...(r.cid ? { correlationId: r.cid } : {}) };
  } else {
    delete idem[key];
  }
  await persistAll();
  for (const [h, v] of Object.entries(r.headers ?? {})) res.setHeader(h, v);
  res.status(r.status).json(r.body);
});

app.get("/orders/:cid", async (req, res) => { const o = orders[req.params.cid]; if (!o) return res.status(404).json({ code: "not_found" }); try { const r = await fetch(ORCH_URL + "/sagas/" + encodeURIComponent(req.params.cid), { signal: AbortSignal.timeout(1500) }); if (r.ok) { const s: any = await r.json(); return res.json({ ...o, state: s.state, ...(s.code ? { code: s.code } : {}), ...(s.duplicateOf ? { duplicateOf: s.duplicateOf } : {}), updatedAt: s.updatedAt ?? o.updatedAt }); } } catch {} res.json(o); });
app.get("/orders-legacy/:cid", (req, res) => {
  const o = orders[req.params.cid];
  if (!o) return res.status(404).json({ code: "not_found" });
  res.json(o);
});

app.get("/health", (_req, res) => {
  res.status(up ? 200 : 503).json({
    status: up ? "ok" : "degraded",
    checks: { broker: up ? "up" : "down", consumers: {} },
  });
});

app.use((err: any, _req: any, res: any, _next: any) => {
  res.status(err?.status === 413 ? 413 : 400).json({ code: "validation_error", message: String(err?.message ?? err) });
});

app.listen(PORT, () => console.log(`intake listening on ${PORT}`));
void connect();
