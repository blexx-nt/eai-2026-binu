/**
 * PA5 payment-service: the reference consumer.
 */

import type { Channel, ConsumeMessage } from "amqplib";
import { connectWithRetry, getRetryCount } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const QUEUE = "payments.queue";
const RESULTS_EXCHANGE = "results.payment";
const DLQ_EXCHANGE = process.env.DLQ_EXCHANGE ?? "orders.dlq.exchange";
const MAX_RETRIES = Number(process.env.MAX_RETRIES) || 3;
const FAIL_RATE = Number(process.env.PAYMENT_FAIL_RATE ?? "20"); // ?? so that 0 stays 0

// A failure that will repeat identically on every attempt: never retried.
class PermanentError extends Error {}

// Exact properties from the README. Must be identical in every service.
async function declareChannels(channel: Channel): Promise<void> {
  await channel.assertExchange("orders.dlq.exchange", "fanout", { durable: true });
  await channel.assertQueue("orders.dlq", { durable: true });
  await channel.bindQueue("orders.dlq", "orders.dlq.exchange", "");

  await channel.assertExchange("orders.invalid.exchange", "fanout", { durable: true });
  await channel.assertQueue("orders.invalid", { durable: true });
  await channel.bindQueue("orders.invalid", "orders.invalid.exchange", "");
}

async function main(): Promise<void> {
  const { channel } = await connectWithRetry(process.env.RABBITMQ_URL!);
  await channel.prefetch(1);
  await declareChannels(channel); // before consuming

  console.log(`[Payment] Consuming from ${QUEUE}, fail rate: ${FAIL_RATE}%`);

  await channel.consume(QUEUE, async (msg: ConsumeMessage | null) => {
    if (!msg) return;

    const correlationId = msg.properties.headers?.["correlationId"] as string | undefined;
    const retryCount = getRetryCount(msg);

    try {
      // Parsing is inside the try: a bad body must not crash the consumer.
      const order = JSON.parse(msg.content.toString()) as CanonicalOrder;

      if (!correlationId) {
        throw new PermanentError("missing correlationId header");
      }

      console.log(`[Payment] Processing ${correlationId} (attempt ${retryCount + 1})`);

      if (Math.random() * 100 < FAIL_RATE) {
        throw new Error("Simulated payment failure");
      }

      channel.publish(
        RESULTS_EXCHANGE,
        "",
        Buffer.from(
          JSON.stringify({
            correlationId,
            source: "payment",
            status: "success",
            timestamp: new Date().toISOString(),
            details: { message: `Payment accepted for order ${order.orderId}` },
          }),
        ),
        { headers: { correlationId }, contentType: "application/json", persistent: true },
      );
      channel.ack(msg);
    } catch (err) {
      // 1. Permanent failure: original bytes and headers to the invalid channel, ack, no retry.
      if (err instanceof SyntaxError || err instanceof PermanentError) {
        channel.publish("orders.invalid.exchange", "", msg.content, {
          headers: msg.properties.headers,
          persistent: true,
        });
        channel.ack(msg);
        console.log(`[Payment] -> INVALID (not retried): ${(err as Error).message}`);
        return;
      }

      // 2. Temporary failure: provided retry/DLQ logic.
      // Only changes: persistent: true, and the originQueue header that replay needs.
      if (retryCount >= MAX_RETRIES - 1) {
        channel.publish(DLQ_EXCHANGE, "", msg.content, {
          headers: { ...msg.properties.headers, originQueue: QUEUE },
          persistent: true,
        });
        channel.ack(msg);
        console.log(`[Payment] -> DLQ after ${retryCount + 1} attempts: ${(err as Error).message}`);
      } else {
        channel.nack(msg, false, false);
        console.log(`[Payment] -> Retry (attempt ${retryCount + 1}): ${(err as Error).message}`);
      }
    }
  });
}

main().catch(console.error);
