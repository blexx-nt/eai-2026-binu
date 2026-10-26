/**
 * PA5 order-service: HTTP producer + DLQ replay.
 */

import { randomUUID } from "node:crypto";
import express from "express";
import { connectWithRetry, withoutRetryHistory } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;

function isNonEmptyString(v: unknown): v is string {
  return typeof v === "string" && v.length > 0;
}

// A handful of typeof checks, enough for this assignment.
function looksLikeOrder(o: any): o is CanonicalOrder {
  return (
    o && typeof o === "object" &&
    isNonEmptyString(o.orderId) &&
    isNonEmptyString(o.orderType) &&
    isNonEmptyString(o.source) &&
    isNonEmptyString(o.receivedAt) &&
    isNonEmptyString(o.orderDate) &&
    isNonEmptyString(o.currency) &&
    isNonEmptyString(o.status) &&
    o.customer && typeof o.customer === "object" &&
    isNonEmptyString(o.customer.name) &&
    isNonEmptyString(o.customer.email) &&
    o.customer.address && typeof o.customer.address === "object" &&
    Array.isArray(o.items) && o.items.length > 0
  );
}

async function main(): Promise<void> {
  const { channel } = await connectWithRetry(process.env.RABBITMQ_URL!);

  // Dead letter channel: replay reads from it. Same properties as the consumers.
  await channel.assertExchange("orders.dlq.exchange", "fanout", { durable: true });
  await channel.assertQueue("orders.dlq", { durable: true });
  await channel.bindQueue("orders.dlq", "orders.dlq.exchange", "");
  // Invalid channel, same properties: used for DLQ messages that cannot be replayed.
  await channel.assertExchange("orders.invalid.exchange", "fanout", { durable: true });
  await channel.assertQueue("orders.invalid", { durable: true });
  await channel.bindQueue("orders.invalid", "orders.invalid.exchange", "");

  const orders = new Map<string, CanonicalOrder>();
  const app = express();
  app.use(express.json());

  app.get("/health", (_req, res) => {
    res.json({ status: "ok" });
  });

  app.post("/orders", (req, res) => {
    const order = req.body;
    if (!looksLikeOrder(order)) {
      res.status(400).json({ error: "body is not a canonical order" });
      return;
    }
    const correlationId = randomUUID();
    channel.publish("orders.exchange", "", Buffer.from(JSON.stringify(order)), {
      headers: { correlationId }, // header only, never a body field
      contentType: "application/json",
      persistent: true,
    });
    orders.set(correlationId, order);
    res.status(201).json({ correlationId, status: "accepted" });
  });

  app.get("/orders/:correlationId", (req, res) => {
    const order = orders.get(req.params.correlationId);
    if (!order) {
      res.sendStatus(404);
      return;
    }
    res.json({ correlationId: req.params.correlationId, order });
  });

  // Replay: send each DLQ message back to ONLY the consumer that failed it.
  app.post("/dlq/replay", async (_req, res) => {
    try {
      const { messageCount } = await channel.checkQueue("orders.dlq");
      let replayed = 0;
      for (let i = 0; i < messageCount; i++) {
        const msg = await channel.get("orders.dlq", { noAck: false });
        if (!msg) break;
        const origin = msg.properties.headers?.["originQueue"] as string | undefined;
        if (!origin) {
          // Cannot tell who failed it: move to invalid rather than lose or loop it.
          channel.publish("orders.invalid.exchange", "", msg.content, {
            headers: msg.properties.headers,
            persistent: true,
          });
          channel.ack(msg);
          continue;
        }
        channel.publish("orders.return.exchange", origin, msg.content, {
          headers: withoutRetryHistory(msg.properties.headers),
          persistent: true,
        });
        channel.ack(msg); // only after republishing
        replayed++;
      }
      res.json({ replayed });
    } catch (err) {
      console.error("[Order] replay failed", err);
      res.status(500).json({ error: "replay failed" });
    }
  });

  app.listen(PORT, () => console.log(`[Order] listening on ${PORT}`));
}

main().catch(console.error);
