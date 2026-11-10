import {
  proxyActivities,
  ActivityFailure,
  ApplicationFailure,
  TimeoutFailure,
} from "@temporalio/workflow";
import type * as activities from "../activities";
import type { CanonicalOrder, CheckoutResult, TraceItem } from "../types";

const act = proxyActivities<typeof activities>({
  startToCloseTimeout: "2500 ms",
  retry: { maximumAttempts: 3, initialInterval: "200 ms", backoffCoefficient: 2 },
});

function toCents(price: string): number {
  const [whole = "0", frac = ""] = price.split(".");
  return parseInt(whole, 10) * 100 + parseInt((frac + "00").slice(0, 2), 10);
}

function totalAmount(order: CanonicalOrder): string {
  const cents = order.items.reduce((sum, i) => sum + toCents(i.unitPrice) * i.quantity, 0);
  return `${Math.floor(cents / 100)}.${String(cents % 100).padStart(2, "0")}`;
}

type Outcome = { ok: true } | { ok: false; code: string; timeout: boolean };

export async function checkout(order: CanonicalOrder): Promise<CheckoutResult> {
  const orderId = order.orderId;
  const trace: TraceItem[] = [];

  async function run(step: string, call: () => Promise<void>): Promise<Outcome> {
    const start = new Date();
    let outcome: Outcome = { ok: true };
    try {
      await call();
    } catch (err) {
      if (err instanceof ActivityFailure && err.cause instanceof TimeoutFailure) {
        outcome = { ok: false, code: "timeout", timeout: true };
      } else if (err instanceof ActivityFailure && err.cause instanceof ApplicationFailure) {
        outcome = { ok: false, code: err.cause.type ?? "unknown", timeout: false };
      } else {
        outcome = { ok: false, code: "unknown", timeout: false };
      }
    }
    const end = new Date();
    trace.push({
      step,
      status: outcome.ok ? "success" : outcome.timeout ? "timeout" : "failed",
      startedAt: start.toISOString(),
      finishedAt: end.toISOString(),
      durationMs: end.getTime() - start.getTime(),
    });
    return outcome;
  }

  const steps: Array<[string, () => Promise<void>]> = [
    ["payment", () => act.authorizePayment(orderId, totalAmount(order))],
    ["inventory", () => act.reserveInventory(orderId, order.items)],
    ["shipping", () => act.createShipment(orderId)],
    ["notification", () => act.sendNotification(orderId, order.customer.email)],
  ];

  const done: string[] = [];
  for (const [name, call] of steps) {
    const r = await run(name, call);
    if (r.ok) {
      done.push(name);
      continue;
    }

    if (done.length === 0) {
      return { orderId, status: "failed", code: r.code, trace };
    }

    let compFailed = false;
    if (done.includes("inventory")) {
      if (!(await run("inventory_release", () => act.releaseInventory(orderId))).ok) compFailed = true;
    }
    if (done.includes("payment")) {
      if (!(await run("payment_refund", () => act.refundPayment(orderId))).ok) compFailed = true;
    }
    return compFailed
      ? { orderId, status: "failed", code: "compensation_failed", trace }
      : { orderId, status: "compensated", code: r.code, trace };
  }

  return { orderId, status: "completed", trace };
}