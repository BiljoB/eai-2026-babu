import type { ConsumeMessage } from "amqplib";
import { connectWithRetry, getRetryCount } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const QUEUE = "inventory.queue";
const RESULTS_EXCHANGE = "results.inventory";
const DLQ_EXCHANGE = process.env.DLQ_EXCHANGE ?? "orders.dlq.exchange";
const INVALID_EXCHANGE = "orders.invalid.exchange";
const MAX_RETRIES = Number(process.env.MAX_RETRIES ?? "3");
const FAIL_RATE = Number(process.env.INVENTORY_FAIL_RATE ?? "20");

class PermanentError extends Error {}

async function main(): Promise<void> {
  const { channel } = await connectWithRetry(process.env.RABBITMQ_URL!);

  await channel.assertExchange(DLQ_EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue("orders.dlq", { durable: true });
  await channel.bindQueue("orders.dlq", DLQ_EXCHANGE, "");
  await channel.assertExchange(INVALID_EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue("orders.invalid", { durable: true });
  await channel.bindQueue("orders.invalid", INVALID_EXCHANGE, "");

  await channel.prefetch(1);
  console.log(`[Inventory] Consuming from ${QUEUE}, fail rate: ${FAIL_RATE}%`);

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
      if (!correlationId) throw new PermanentError("missing correlationId header");

      console.log(`[Inventory] Processing ${correlationId} (attempt ${retryCount + 1})`);

      if (Math.random() * 100 < FAIL_RATE) {
        throw new Error("simulated inventory failure");
      }

      channel.publish(
        RESULTS_EXCHANGE,
        "",
        Buffer.from(
          JSON.stringify({
            correlationId,
            source: "inventory",
            status: "success",
            timestamp: new Date().toISOString(),
            details: { message: `Stock reserved for order ${order.orderId}` },
          })
        ),
        { persistent: true, contentType: "application/json", headers: { correlationId } }
      );
      channel.ack(msg);
    } catch (err) {
      if (err instanceof PermanentError) {
        channel.publish(INVALID_EXCHANGE, "", msg.content, {
          persistent: true,
          headers: msg.properties.headers,
        });
        channel.ack(msg);
        console.log(`[Inventory] -> INVALID: ${err.message}`);
        return;
      }

      if (retryCount >= MAX_RETRIES - 1) {
        channel.publish(DLQ_EXCHANGE, "", msg.content, {
          persistent: true,
          headers: { ...msg.properties.headers, originQueue: QUEUE },
        });
        channel.ack(msg);
        console.log(`[Inventory] -> DLQ after ${retryCount + 1} attempts: ${(err as Error).message}`);
      } else {
        channel.nack(msg, false, false);
        console.log(`[Inventory] -> Retry (attempt ${retryCount + 1}): ${(err as Error).message}`);
      }
    }
  });
}

main().catch(console.error);