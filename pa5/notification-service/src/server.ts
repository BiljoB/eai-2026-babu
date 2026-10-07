import fs from "node:fs";
import type { ConsumeMessage } from "amqplib";
import { connectWithRetry, getRetryCount } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const QUEUE = "notifications.queue";
const RESULTS_EXCHANGE = "results.notification";
const DLQ_EXCHANGE = process.env.DLQ_EXCHANGE ?? "orders.dlq.exchange";
const INVALID_EXCHANGE = "orders.invalid.exchange";
const MAX_RETRIES = Number(process.env.MAX_RETRIES ?? "3");
const PROCESSED_FILE = "/data/processed-ids.json";
const LOG_FILE = "/data/notification.log";

class PermanentError extends Error {}

function loadProcessed(): Set<string> {
  try {
    return new Set<string>(JSON.parse(fs.readFileSync(PROCESSED_FILE, "utf8")));
  } catch {
    return new Set<string>(); // missing or unreadable file = nothing processed yet
  }
}

function saveProcessed(ids: Set<string>): void {
  const tmp = `${PROCESSED_FILE}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify([...ids]));
  fs.renameSync(tmp, PROCESSED_FILE); // atomic replace: never a half-written file
}

async function main(): Promise<void> {
  const { channel } = await connectWithRetry(process.env.RABBITMQ_URL!);

  await channel.assertExchange(DLQ_EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue("orders.dlq", { durable: true });
  await channel.bindQueue("orders.dlq", DLQ_EXCHANGE, "");
  await channel.assertExchange(INVALID_EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue("orders.invalid", { durable: true });
  await channel.bindQueue("orders.invalid", INVALID_EXCHANGE, "");

  await channel.prefetch(1);
  console.log(`[Notification] Consuming from ${QUEUE}`);

  await channel.consume(QUEUE, async (msg: ConsumeMessage | null) => {
    if (!msg) return;

    const correlationId = msg.properties.headers?.["correlationId"] as string | undefined;
    const retryCount = getRetryCount(msg);

    try {
      let order: CanonicalOrder;
      try {
        order = JSON.parse(msg.content.toString()) as CanonicalOrder;
      } catch {
        throw new PermanentError("body is not valid JSON");
      }

      // Valid JSON, but not an order: also permanent.
      if (typeof order !== "object" || order === null) {
        throw new PermanentError("body is not a JSON object");
      }
      if (typeof (order as { orderId?: unknown }).orderId !== "string") {
        throw new PermanentError("order has no orderId");
      }
      if (!correlationId) throw new PermanentError("missing correlationId header");

      // Idempotency: re-read the file each time so it stays correct across restarts.
      const processed = loadProcessed();
      if (processed.has(correlationId)) {
        console.log(`[Notification] duplicate skipped ${correlationId}`);
        channel.ack(msg);
        return;
      }

      fs.appendFileSync(
        LOG_FILE,
        JSON.stringify({
          correlationId,
          orderId: order.orderId,
          customerEmail: (order as any).customerEmail ?? (order as any).customer?.email,
          timestamp: new Date().toISOString(),
          message: "Order received",
        }) + "\n"
      );
      processed.add(correlationId);
      saveProcessed(processed);

      channel.publish(
        RESULTS_EXCHANGE,
        "",
        Buffer.from(
          JSON.stringify({
            correlationId,
            source: "notification",
            status: "success",
            timestamp: new Date().toISOString(),
            details: { message: `Customer notified for order ${order.orderId}` },
          })
        ),
        { persistent: true, contentType: "application/json", headers: { correlationId } }
      );
      channel.ack(msg);
      console.log(`[Notification] notified ${correlationId}`);
    } catch (err) {
      if (err instanceof PermanentError) {
        channel.publish(INVALID_EXCHANGE, "", msg.content, {
          persistent: true,
          headers: msg.properties.headers,
        });
        channel.ack(msg);
        console.log(`[Notification] -> INVALID: ${err.message}`);
        return;
      }

      if (retryCount >= MAX_RETRIES - 1) {
        channel.publish(DLQ_EXCHANGE, "", msg.content, {
          persistent: true,
          headers: { ...msg.properties.headers, originQueue: QUEUE },
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