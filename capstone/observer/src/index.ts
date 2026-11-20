// @ts-nocheck
import express from "express";
import amqp from "amqplib";
import fs from "node:fs";
import path from "node:path";

const env = process.env;
const DATA_DIR = env.DATA_DIR || "/data";
const TRACE = path.join(DATA_DIR, "trace-store.jsonl");
const QUEUE = "trace.store";
const LAG = 5;
const MGMT = env.RABBITMQ_MGMT_URL || "http://rabbitmq:15672";
const ORCH = env.ORCHESTRATOR_URL || "http://orchestrator:8802";

let conn = null, connected = false, consuming = false;
const seen = new Set();
const byCid = new Map();

function index(hop) {
  seen.add(hop.hopId);
  if (!byCid.has(hop.correlationId)) byCid.set(hop.correlationId, []);
  byCid.get(hop.correlationId).push(hop);
}
try {
  for (const line of fs.readFileSync(TRACE, "utf8").split("\n")) {
    if (!line.trim()) continue;
    try { index(JSON.parse(line)); } catch {}
  }
} catch {}

async function connectLoop() {
  for (;;) {
    try {
      const c = await amqp.connect(env.RABBITMQ_URL);
      c.on("error", () => {});
      const closed = new Promise((r) => c.on("close", () => r()));
      const ch = await c.createChannel();
      ch.on("error", () => {});
      await ch.prefetch(20);
      await ch.consume(QUEUE, (m) => {
        if (!m) return;
        try {
          const hop = JSON.parse(m.content.toString("utf8"));
          if (hop && hop.hopId && hop.correlationId && !seen.has(hop.hopId)) {
            fs.appendFileSync(TRACE, JSON.stringify(hop) + "\n");
            index(hop);
          }
          ch.ack(m);
        } catch (e) {
          console.error("hop error:", e.message);
          ch.ack(m);
        }
      });
      conn = c; connected = true; consuming = true;
      console.log("broker connected");
      await closed;
    } catch (e) { console.error("broker error:", e.message); }
    conn = null; connected = false; consuming = false;
    await new Promise((r) => setTimeout(r, 1000));
  }
}

async function unacked(q) {
  try {
    const r = await fetch(`${MGMT}/api/queues/%2F/${q}`, {
      headers: { Authorization: "Basic " + Buffer.from("eai:eai-capstone").toString("base64") },
      signal: AbortSignal.timeout(2000),
    });
    const j = await r.json();
    return Number(j.messages_unacknowledged ?? 0);
  } catch { return 0; }
}

const app = express();

app.get("/health", (_req, res) => {
  const ok = connected && consuming;
  res.status(ok ? 200 : 503).json({
    status: ok ? "ok" : "degraded",
    checks: { broker: connected ? "up" : "down", consumers: consuming ? { [QUEUE]: 1 } : {} },
  });
});

app.get("/trace/:cid", (req, res) => {
  const hops = [...(byCid.get(req.params.cid) || [])].sort((a, b) => a.startedAt.localeCompare(b.startedAt));
  if (!hops.length) return res.status(404).json({ code: "not_found" });
  res.json({ correlationId: req.params.cid, hops });
});

app.get("/ops/metrics", async (_req, res) => {
  if (!conn) return res.status(503).json({ status: "degraded" });
  const names = ["saga.queue", "notifications.queue", "orders.dlq", "orders.invalid", "trace.store"];
  const queues = {};
  let ch;
  try {
    ch = await conn.createChannel();
    ch.on("error", () => {});
    for (const q of names) {
      const c = await ch.checkQueue(q);
      queues[q] = { ready: c.messageCount, unacked: await unacked(q), consumers: c.consumerCount };
    }
  } catch { return res.status(503).json({ status: "degraded" }); }
  finally { try { await ch?.close(); } catch {} }

  let stuck = 0;
  try {
    const r = await fetch(`${ORCH}/sagas/stuck`, { signal: AbortSignal.timeout(2000) });
    stuck = (await r.json()).sagas.length;
  } catch {}
  const dlq = queues["orders.dlq"].ready + queues["orders.dlq"].unacked;
  const inv = queues["orders.invalid"].ready + queues["orders.invalid"].unacked;
  const a = (key, value, firing, threshold) => ({ key, firing, value, threshold });
  res.json({
    generatedAt: new Date().toISOString(),
    queues, dlqDepth: dlq, invalidDepth: inv, stuckSagas: stuck,
    alerts: [
      a("dlq_not_empty", dlq, dlq > 0, null),
      a("invalid_not_empty", inv, inv > 0, null),
      a("saga_consumer_missing", queues["saga.queue"].consumers, queues["saga.queue"].consumers === 0, null),
      a("notifier_consumer_missing", queues["notifications.queue"].consumers, queues["notifications.queue"].consumers === 0, null),
      a("saga_consumer_lag", queues["saga.queue"].ready, queues["saga.queue"].ready > LAG, LAG),
      a("notifier_consumer_lag", queues["notifications.queue"].ready, queues["notifications.queue"].ready > LAG, LAG),
      a("stuck_sagas", stuck, stuck > 0, null),
    ],
  });
});

process.on("unhandledRejection", (e) => console.error("unhandled:", e));
app.listen(Number(env.PORT), () => console.log("listening"));
connectLoop();