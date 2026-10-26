/**
 * PA5 notification-service: idempotent receiver.
 * Idempotency key = correlationId (one notification per order).
 * The processed set lives in a bind-mounted file so it survives restarts.
 */

import fs from "node:fs";
import type { Channel, ConsumeMessage } from "amqplib";
import { connectWithRetry, getRetryCount } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const QUEUE = "notifications.queue";
const RESULTS_EXCHANGE = "results.notification";
const DLQ_EXCHANGE = process.env.DLQ_EXCHANGE ?? "orders.dlq.exchange";
const MAX_RETRIES = Number(process.env.MAX_RETRIES) || 3;
const FAIL_RATE = Number(process.env.NOTIFICATION_FAIL_RATE ?? "0");

const IDS_FILE = "/data/processed-ids.json";
const LOG_FILE = "/data/notification.log";

class PermanentError extends Error {}

async function declareChannels(channel: Channel): Promise<void> {
  await channel.assertExchange("orders.dlq.exchange", "fanout", { durable: true });
  await channel.assertQueue("orders.dlq", { durable: true });
  await channel.bindQueue("orders.dlq", "orders.dlq.exchange", "");

  await channel.assertExchange("orders.invalid.exchange", "fanout", { durable: true });
  await channel.assertQueue("orders.invalid", { durable: true });
  await channel.bindQueue("orders.invalid", "orders.invalid.exchange", "");
}

function loadProcessed(): Set<string> {
  try {
    if (!fs.existsSync(IDS_FILE)) {
      fs.writeFileSync(IDS_FILE, "[]");
      return new Set();
    }
    return new Set(JSON.parse(fs.readFileSync(IDS_FILE, "utf8")) as string[]);
  } catch {
    return new Set();
  }
}

async function main(): Promise<void> {
  const { channel } = await connectWithRetry(process.env.RABBITMQ_URL!);
  await channel.prefetch(1);
  await declareChannels(channel);

  const processed = loadProcessed();
  console.log(`[Notification] Consuming from ${QUEUE}, ${processed.size} ids already processed`);

  await channel.consume(QUEUE, async (msg: ConsumeMessage | null) => {
    if (!msg) return;

    const correlationId = msg.properties.headers?.["correlationId"] as string | undefined;
    const retryCount = getRetryCount(msg);

    try {
      const order = JSON.parse(msg.content.toString()) as CanonicalOrder;
      if (!correlationId) throw new PermanentError("missing correlationId header");

      if (processed.has(correlationId)) {
        console.log(`[Notification] duplicate skipped ${correlationId}`);
        channel.ack(msg);
        return;
      }

      if (Math.random() * 100 < FAIL_RATE) {
        throw new Error("Simulated notification failure");
      }

      fs.appendFileSync(
        LOG_FILE,
        JSON.stringify({
          correlationId,
          orderId: order.orderId,
          customerEmail: order.customer.email,
          timestamp: new Date().toISOString(),
          message: "Order received",
        }) + "\n",
      );
      processed.add(correlationId);
      fs.writeFileSync(IDS_FILE, JSON.stringify([...processed]));

      channel.publish(
        RESULTS_EXCHANGE,
        "",
        Buffer.from(
          JSON.stringify({
            correlationId,
            source: "notification",
            status: "success",
            timestamp: new Date().toISOString(),
            details: { message: `Notification sent for order ${order.orderId}` },
          }),
        ),
        { headers: { correlationId }, contentType: "application/json", persistent: true },
      );
      channel.ack(msg);
    } catch (err) {
      if (err instanceof SyntaxError || err instanceof PermanentError) {
        channel.publish("orders.invalid.exchange", "", msg.content, {
          headers: msg.properties.headers,
          persistent: true,
        });
        channel.ack(msg);
        console.log(`[Notification] -> INVALID (not retried): ${(err as Error).message}`);
        return;
      }

      if (retryCount >= MAX_RETRIES - 1) {
        channel.publish(DLQ_EXCHANGE, "", msg.content, {
          headers: { ...msg.properties.headers, originQueue: QUEUE },
          persistent: true,
        });
        channel.ack(msg);
        console.log(`[Notification] -> DLQ after ${retryCount + 1} attempts: ${(err as Error).message}`);
      } else {
        channel.nack(msg, false, false);
        console.log(`[Notification] -> Retry (attempt ${retryCount + 1}): ${(err as Error).message}`);
      }
    }
  });
}

main().catch(console.error);
