import express from "express";
import amqp from "amqplib";

const QUEUE = "";
let connected = false;
let consuming = false;

async function connectLoop() {
  for (;;) {
    try {
      const conn = await amqp.connect(process.env.RABBITMQ_URL!);
      conn.on("error", () => {});
      const closed = new Promise<void>((r) => conn.on("close", () => r()));
      const ch = await conn.createChannel();
      ch.on("error", () => {});
      if (QUEUE) {
        await ch.prefetch(10);
        await ch.consume(QUEUE, (msg) => { if (msg) ch.ack(msg); });
        consuming = true;
      }
      connected = true;
      console.log("broker connected");
      await closed;
    } catch (e) {
      console.error("broker error:", (e as Error).message);
    }
    connected = false;
    consuming = false;
    await new Promise((r) => setTimeout(r, 1000));
  }
}

const app = express();
app.get("/health", (_req, res) => {
  const ok = connected && (!QUEUE || consuming);
  res.status(ok ? 200 : 503).json({
    status: ok ? "ok" : "degraded",
    checks: {
      broker: connected ? "up" : "down",
      consumers: ok && QUEUE ? { [QUEUE]: 1 } : {},
    },
  });
});

app.listen(Number(process.env.PORT), () => console.log("listening"));
connectLoop();