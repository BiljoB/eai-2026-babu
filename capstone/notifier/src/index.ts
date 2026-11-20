// @ts-nocheck
import express from "express";
import amqp from "amqplib";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

const env = process.env;
const DATA_DIR = env.DATA_DIR || "/data";
const TIMEOUT = Number(env.REQUEST_TIMEOUT_MS || 2500);
const MAX_RETRIES = Number(env.MAX_RETRIES || 3);
const Q = "notifications.queue";

let processed = new Set();
try { processed = new Set(JSON.parse(fs.readFileSync(path.join(DATA_DIR, "processed-ids.json"), "utf8"))); } catch {}
let writing = Promise.resolve();
function saveProcessed() {
  const snap = JSON.stringify([...processed]);
  writing = writing.then(async () => {
    const p = path.join(DATA_DIR, "processed-ids.json");
    await fs.promises.writeFile(p + ".tmp", snap);
    await fs.promises.rename(p + ".tmp", p);
  }).catch((e) => console.error("save failed", e.message));
  return writing;
}

let pubCh = null, subCh = null, connected = false, consuming = false;
const inflight = new Set();

function publishConfirmed(ex, rk, body, opts) {
  return new Promise((resolve, reject) => {
    if (!pubCh) return reject(new Error("no channel"));
    const t = setTimeout(() => reject(new Error("confirm timeout")), 5000);
    try {
      pubCh.publish(ex, rk, Buffer.isBuffer(body) ? body : Buffer.from(JSON.stringify(body)),
        { persistent: true, contentType: "application/json", ...opts }, (err) => { clearTimeout(t); err ? reject(err) : resolve(); });
    } catch (e) { clearTimeout(t); reject(e); }
  });
}
function hop(h) {
  const id = h.id || randomUUID();
  try {
    const t0 = h.t0 || new Date(), t1 = h.t1 || new Date();
    pubCh?.publish("trace.events", "", Buffer.from(JSON.stringify({
      hopId: id, correlationId: h.cid, causationId: h.cause ?? null, service: "notifier", operation: h.op,
      status: h.status || "success", attempt: h.attempt || 1, ...(h.messageId ? { messageId: h.messageId } : {}),
      startedAt: t0.toISOString(), finishedAt: t1.toISOString(), durationMs: t1 - t0, ...(h.detail ? { detail: h.detail } : {}),
    })), { persistent: true, contentType: "application/json", messageId: randomUUID(), headers: { correlationId: h.cid } });
  } catch {}
  return id;
}
const ack = (m) => { try { subCh.ack(m); } catch {} };
const nack = (m) => { try { subCh.nack(m, false, false); } catch {} };
function attemptOf(hdr) {
  const d = hdr["x-death"]; let n = 0;
  if (Array.isArray(d)) for (const e of d) if (e.queue === Q && e.reason === "rejected") n += Number(e.count);
  return n + 1;
}

async function send(ev, cid, hopId) {
  try {
    const r = await fetch(env.NOTIFICATION_URL + "/notification/send", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Correlation-Id": cid, "X-Causation-Id": hopId },
      body: JSON.stringify({ orderId: ev.orderId, recipient: ev.recipient, kind: "order_not_completed", reason: ev.code || "unknown" }),
      signal: AbortSignal.timeout(TIMEOUT),
    });
    if (r.ok) { await r.text().catch(() => {}); return { ok: true }; }
    return { ok: false, code: String(r.status) };
  } catch (e) {
    return { ok: false, code: (e.name === "TimeoutError" || e.name === "AbortError") ? "timeout" : "unreachable" };
  }
}

async function handle(msg) {
  const t0 = new Date();
  const hdr = msg.properties.headers || {};
  const cid = hdr.correlationId;
  let ev = null;
  try {
    ev = JSON.parse(msg.content.toString("utf8"));
    if (!cid || !ev || typeof ev !== "object" || !ev.orderId || !ev.recipient || !ev.correlationId) ev = null;
  } catch { ev = null; }
  if (!ev) {
    await publishConfirmed("orders.invalid.exchange", "", msg.content, { messageId: msg.properties.messageId, headers: hdr });
    if (cid) hop({ cid, cause: hdr.causationId, op: "invalid.quarantine", status: "quarantined", t0, t1: new Date() });
    return ack(msg);
  }
  const attempt = attemptOf(hdr);
  const consumeId = hop({ cid, cause: hdr.causationId, op: "notify.consume", attempt, t0, messageId: msg.properties.messageId });
  if (processed.has(cid)) {
    hop({ cid, cause: consumeId, op: "notify.duplicate", status: "duplicate", attempt });
    return ack(msg);
  }
  if (inflight.has(cid)) return ack(msg);
  inflight.add(cid);
  try {
    const sendId = randomUUID(), s0 = new Date();
    const r = await send(ev, cid, sendId);
    hop({ id: sendId, cid, cause: consumeId, op: "notify.send", attempt, t0: s0, t1: new Date(),
      status: r.ok ? "success" : (r.code === "timeout" ? "timeout" : "failed"), detail: r.ok ? undefined : { code: r.code } });
    if (r.ok) {
      fs.appendFileSync(path.join(DATA_DIR, "notification.log"),
        JSON.stringify({ at: new Date().toISOString(), correlationId: cid, orderId: ev.orderId, recipient: ev.recipient, kind: "order_not_completed", reason: ev.code || "unknown" }) + "\n");
      processed.add(cid);
      await saveProcessed();
      return ack(msg);
    }
    if (attempt < MAX_RETRIES) {
      hop({ cid, cause: consumeId, op: "notify.retry", status: "retried", attempt });
      return nack(msg);
    }
    const dlId = randomUUID();
    await publishConfirmed("orders.dlq.exchange", "", msg.content, { messageId: randomUUID(),
      headers: { correlationId: cid, causationId: dlId, businessKey: ev.businessKey, originQueue: Q, lastError: `notify ${r.code}`, attempts: attempt, deadLetteredAt: new Date().toISOString() } });
    hop({ id: dlId, cid, cause: sendId, op: "notify.dead_letter", status: "dead_lettered", attempt, detail: { code: r.code } });
    return ack(msg);
  } finally { inflight.delete(cid); }
}

async function connectLoop() {
  for (;;) {
    try {
      const conn = await amqp.connect(env.RABBITMQ_URL);
      conn.on("error", () => {});
      const closed = new Promise((r) => conn.on("close", () => r()));
      const pub = await conn.createConfirmChannel(); pub.on("error", () => {});
      const sub = await conn.createChannel(); sub.on("error", () => {});
      pubCh = pub; subCh = sub;
      await sub.prefetch(5);
      await sub.consume(Q, (m) => {
        if (!m) return;
        handle(m).catch((e) => { console.error("handler error:", e.message); nack(m); });
      });
      connected = true; consuming = true;
      console.log("broker connected");
      await closed;
    } catch (e) { console.error("broker error:", e.message); }
    pubCh = null; subCh = null; connected = false; consuming = false;
    await new Promise((r) => setTimeout(r, 1000));
  }
}

const app = express();
app.get("/health", (_req, res) => {
  const ok = connected && consuming;
  res.status(ok ? 200 : 503).json({ status: ok ? "ok" : "degraded", checks: { broker: connected ? "up" : "down", consumers: consuming ? { [Q]: 1 } : {} } });
});
process.on("unhandledRejection", (e) => console.error("unhandled:", e));
app.listen(Number(env.PORT), () => console.log("listening"));
connectLoop();