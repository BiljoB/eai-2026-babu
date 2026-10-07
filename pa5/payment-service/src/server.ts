import type { ConsumeMessage } from "amqplib";
import { connectWithRetry, getRetryCount } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const QUEUE = "payments.queue";
const RESULTS_EXCHANGE = "results.payment";
const DLQ_EXCHANGE = process.env.DLQ_EXCHANGE ?? "orders.dlq.exchange";
const INVALID_EXCHANGE = "orders.invalid.exchange";
const MAX_RETRIES = Number(process.env.MAX_RETRIES ?? "3");
const FAIL_RATE = Number(process.env.PAYMENT_FAIL_RATE ?? "20");

// A failure that will repeat identically on every attempt: never retry it.
class PermanentError extends Error {}

async function main(): Promise<void> {
  const { channel } = await connectWithRetry(process.env.RABBITMQ_URL!);

  // TODO 1: declare the two channels (exact properties, same in every service)
  await channel.assertExchange(DLQ_EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue("orders.dlq", { durable: true });
  await channel.bindQueue("orders.dlq", DLQ_EXCHANGE, "");
  await channel.assertExchange(INVALID_EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue("orders.invalid", { durable: true });
  await channel.bindQueue("orders.invalid", INVALID_EXCHANGE, "");

  await channel.prefetch(1);
  console.log(`[Payment] Consuming from ${QUEUE}, fail rate: ${FAIL_RATE}%`);

  await channel.consume(QUEUE, async (msg: ConsumeMessage | null) => {
    if (!msg) return;

    const correlationId = msg.properties.headers?.["correlationId"] as string | undefined;
    const retryCount = getRetryCount(msg);

    try {
      // Parse INSIDE the try so a bad body can't crash the consumer.
      let order: CanonicalOrder;
      try {
        order = JSON.parse(msg.content.toString()) as CanonicalOrder;
      } catch {
        throw new PermanentError("body is not valid JSON");
      }
      if (!correlationId) throw new PermanentError("missing correlationId header");

      console.log(`[Payment] Processing ${correlationId} (attempt ${retryCount + 1})`);

      // TODO 2: simulate the payment
      if (Math.random() * 100 < FAIL_RATE) {
        throw new Error("simulated payment failure");
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
          })
        ),
        { persistent: true, contentType: "application/json", headers: { correlationId } }
      );
      channel.ack(msg);
    } catch (err) {
      // TODO 3: classify
      if (err instanceof PermanentError) {
        channel.publish(INVALID_EXCHANGE, "", msg.content, {
          persistent: true,
          headers: msg.properties.headers,
        });
        channel.ack(msg);
        console.log(`[Payment] -> INVALID: ${err.message}`);
        return;
      }

      // Provided retry/DLQ logic, plus persistent + originQueue (needed for replay).
      if (retryCount >= MAX_RETRIES - 1) {
        channel.publish(DLQ_EXCHANGE, "", msg.content, {
          persistent: true,
          headers: { ...msg.properties.headers, originQueue: QUEUE },
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