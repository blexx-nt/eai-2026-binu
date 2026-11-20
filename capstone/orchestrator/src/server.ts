import express from "express";
import amqp from "amqplib";

const PORT = Number(process.env.PORT ?? 8000);
const URL = process.env.RABBITMQ_URL!;
const QUEUE = process.env.CONSUME_QUEUE || "";
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

let up = false;
const consumers: Record<string, number> = {};

async function connect(): Promise<void> {
  for (;;) {
    try {
      const conn = await amqp.connect(URL);
      const ch = await conn.createChannel();
      ch.prefetch(5);
      if (QUEUE) {
        await ch.consume(QUEUE, (msg) => { if (msg) ch.ack(msg); });
        consumers[QUEUE] = 1;
      }
      up = true;
      conn.on("error", () => {});
      conn.on("close", () => {
        up = false;
        for (const k of Object.keys(consumers)) delete consumers[k];
        void connect();
      });
      return;
    } catch {
      await sleep(2000);
    }
  }
}

const app = express();
app.get("/health", (_req, res) => {
  const ok = up && (!QUEUE || (consumers[QUEUE] ?? 0) > 0);
  res.status(ok ? 200 : 503).json({
    status: ok ? "ok" : "degraded",
    checks: { broker: up ? "up" : "down", consumers: { ...consumers } },
  });
});
app.listen(PORT, () => console.log(`listening on ${PORT}`));
void connect();
