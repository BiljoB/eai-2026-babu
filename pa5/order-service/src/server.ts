import express from "express";
import crypto from "node:crypto";
import { connectWithRetry } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;
const orders = new Map<string, CanonicalOrder>();

// Local stand-in for the missing shared helper: keeps correlationId,
// drops x-death so a replayed message gets a fresh set of attempts.
// TODO: switch back to the import from shared/rabbit once you have the official file.
function withoutRetryHistory(headers: Record<string, unknown> | undefined): Record<string, unknown> {
  const h = { ...(headers ?? {}) };
  delete h["x-death"];
  delete h["x-first-death-exchange"];
  delete h["x-first-death-queue"];
  delete h["x-first-death-reason"];
  return h;
}

async function main(): Promise<void> {
  const { channel } = await connectWithRetry(process.env.RABBITMQ_URL!);

  // Same properties as in every other service. Replay reads from the DLQ.
  await channel.assertExchange("orders.dlq.exchange", "fanout", { durable: true });
  await channel.assertQueue("orders.dlq", { durable: true });
  await channel.bindQueue("orders.dlq", "orders.dlq.exchange", "");
  await channel.assertExchange("orders.invalid.exchange", "fanout", { durable: true });
  await channel.assertQueue("orders.invalid", { durable: true });
  await channel.bindQueue("orders.invalid", "orders.invalid.exchange", "");

  const app = express();
  app.use(express.json());

  app.get("/health", (_req, res) => {
    res.json({ status: "ok" });
  });

  app.post("/orders", (req, res) => {
    const order = req.body as CanonicalOrder;
    if (!order || typeof order !== "object" || typeof order.orderId !== "string") {
      res.status(400).json({ error: "body must be a canonical order with a string orderId" });
      return;
    }
    const correlationId = crypto.randomUUID();
    channel.publish("orders.exchange", "", Buffer.from(JSON.stringify(order)), {
      headers: { correlationId },
      contentType: "application/json",
      persistent: true,
    });
    orders.set(correlationId, order);
    console.log(`[Order] accepted ${order.orderId} as ${correlationId}`);
    res.status(201).json({ correlationId, status: "accepted" });
  });

  app.get("/orders/:correlationId", (req, res) => {
    const order = orders.get(req.params.correlationId);
    if (!order) {
      res.status(404).json({ error: "not found" });
      return;
    }
    res.json({ correlationId: req.params.correlationId, order });
  });

  app.post("/dlq/replay", async (_req, res) => {
    let replayed = 0;
    try {
      while (true) {
        const msg = await channel.get("orders.dlq");
        if (!msg) break;
        const originQueue = msg.properties.headers?.["originQueue"] as string | undefined;
        if (!originQueue) {
          // Can't tell who failed it: put it back and stop rather than guess.
          channel.nack(msg, false, true);
          break;
        }
        // Return exchange + queue name as routing key = only the failing consumer sees it.
        channel.publish("orders.return.exchange", originQueue, msg.content, {
          persistent: true,
          contentType: msg.properties.contentType,
          headers: withoutRetryHistory(msg.properties.headers),
        });
        channel.ack(msg); // ack only after republishing
        replayed++;
      }
      res.json({ replayed });
    } catch (err) {
      console.error("[Order] replay failed", err);
      res.status(500).json({ error: "replay failed", replayed });
    }
  });

  app.listen(PORT, () => console.log(`[Order] listening on ${PORT}`));
}

main().catch(console.error);