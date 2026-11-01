import type { CanonicalOrder } from "./canonical-order";

export type TraceStatus = "success" | "failed" | "timeout";

export interface TraceEntry {
  step: string;
  status: TraceStatus;
  startedAt: string;
  finishedAt: string;
  durationMs: number;
}

export interface SagaBody {
  orderId: string;
  status: "completed" | "failed" | "compensated";
  code?: string;
  trace: TraceEntry[];
}

export interface SagaResult {
  httpStatus: number;
  body: SagaBody;
}

// money: integer cents, format once at the end
export function toCents(price: string): number {
  const m = /^(-?)(\d+)(?:\.(\d{1,2}))?$/.exec(price.trim());
  if (!m) throw new Error(`invalid money string: ${price}`);
  const cents = parseInt(m[2] ?? "0", 10) * 100 + parseInt((m[3] ?? "").padEnd(2, "0"), 10);
  return m[1] === "-" ? -cents : cents;
}

export function formatCents(cents: number): string {
  const sign = cents < 0 ? "-" : "";
  const abs = Math.abs(cents);
  return `${sign}${Math.trunc(abs / 100)}.${String(abs % 100).padStart(2, "0")}`;
}

export function computeAmount(items: CanonicalOrder["items"]): string {
  const total = items.reduce((sum, i) => sum + toCents(i.unitPrice) * i.quantity, 0);
  return formatCents(total);
}

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`missing environment variable ${name}`);
  return v;
}

function extractCode(payload: unknown): string {
  if (payload && typeof payload === "object") {
    const p = payload as Record<string, unknown>;
    if (typeof p.code === "string") return p.code;
    if (typeof p.error === "string") return p.error;
    if (p.error && typeof p.error === "object") {
      const c = (p.error as Record<string, unknown>).code;
      if (typeof c === "string") return c;
    }
  }
  return "downstream_error";
}

async function callStep(
  step: string,
  url: string,
  body: unknown,
  trace: TraceEntry[],
): Promise<{ status: TraceStatus; code?: string }> {
  const timeoutMs = Number(process.env.REQUEST_TIMEOUT_MS ?? 2500);
  const t0 = Date.now();
  const startedAt = new Date(t0).toISOString();
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);

  let status: TraceStatus = "success";
  let code: string | undefined;
  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal: ctrl.signal,
    });
    if (!res.ok) {
      status = "failed";
      code = extractCode(await res.json().catch(() => ({})));
    } else {
      await res.text().catch(() => "");
    }
  } catch (e) {
    const aborted = (e as { name?: string })?.name === "AbortError";
    status = aborted ? "timeout" : "failed";
    code = aborted ? "timeout" : "downstream_unreachable";
  } finally {
    clearTimeout(timer);
  }

  const t1 = Date.now();
  trace.push({
    step,
    status,
    startedAt,
    finishedAt: new Date(t1).toISOString(),
    durationMs: t1 - t0,
  });
  return { status, code };
}

interface Step {
  name: string;
  url: string;
  body: unknown;
  comp?: { name: string; url: string; body: unknown };
}

export async function runSaga(order: CanonicalOrder): Promise<SagaResult> {
  const PAYMENT_URL = requireEnv("PAYMENT_URL");
  const INVENTORY_URL = requireEnv("INVENTORY_URL");
  const SHIPPING_URL = requireEnv("SHIPPING_URL");
  const NOTIFICATION_URL = requireEnv("NOTIFICATION_URL");

  const { orderId } = order;
  const trace: TraceEntry[] = [];

  const steps: Step[] = [
    {
      name: "payment",
      url: `${PAYMENT_URL}/payment/authorize`,
      body: { orderId, amount: computeAmount(order.items) },
      comp: { name: "payment_refund", url: `${PAYMENT_URL}/payment/refund`, body: { orderId } },
    },
    {
      name: "inventory",
      url: `${INVENTORY_URL}/inventory/reserve`,
      body: { orderId, items: order.items },
      comp: { name: "inventory_release", url: `${INVENTORY_URL}/inventory/release`, body: { orderId } },
    },
    { name: "shipping", url: `${SHIPPING_URL}/shipping/create`, body: { orderId } },
    {
      name: "notification",
      url: `${NOTIFICATION_URL}/notification/send`,
      body: { orderId, recipient: order.customer.email },
    },
  ];

  const completed: Step[] = [];

  for (const step of steps) {
    const r = await callStep(step.name, step.url, step.body, trace);
    if (r.status === "success") {
      completed.push(step);
      continue;
    }

    const timedOut = r.status === "timeout";
    const code = timedOut ? "timeout" : (r.code ?? "downstream_error");

    for (const done of [...completed].reverse()) {
      if (!done.comp) continue;
      const c = await callStep(done.comp.name, done.comp.url, done.comp.body, trace);
      if (c.status !== "success") {
        return {
          httpStatus: 422,
          body: { orderId, status: "failed", code: "compensation_failed", trace },
        };
      }
    }

    return {
      httpStatus: timedOut ? 504 : 422,
      body: {
        orderId,
        status: completed.length === 0 ? "failed" : "compensated",
        code,
        trace,
      },
    };
  }

  return { httpStatus: 200, body: { orderId, status: "completed", trace } };
}
