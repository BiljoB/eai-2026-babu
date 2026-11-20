// @ts-nocheck
import express from "express";
import amqp from "amqplib";
import Ajv from "ajv";
import addFormats from "ajv-formats";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

const env = process.env;
const DATA_DIR = env.DATA_DIR || "/data";
const TIMEOUT = Number(env.REQUEST_TIMEOUT_MS || 2500);
const MAX_RETRIES = Number(env.MAX_RETRIES || 3);
const CONC = Number(env.SAGA_CONCURRENCY || 5);
const RUN_ID = randomUUID();
const SAGA_Q = "saga.queue";
const URLS = { payment: env.PAYMENT_URL, inventory: env.INVENTORY_URL, shipping: env.SHIPPING_URL, notification: env.NOTIFICATION_URL };
const TERMINAL = ["completed", "failed", "compensated", "compensation_failed", "needs_attention", "duplicate"];

/* ---------- store ---------- */
let store = { sagas: {}, businessKeys: {} };
try {
  store = JSON.parse(fs.readFileSync(path.join(DATA_DIR, "saga-store.json"), "utf8"));
  store.sagas = store.sagas || {}; store.businessKeys = store.businessKeys || {};
} catch {}
let writing = Promise.resolve();
function save() {
  const snap = JSON.stringify(store);
  writing = writing.then(async () => {
    const p = path.join(DATA_DIR, "saga-store.json");
    await fs.promises.writeFile(p + ".tmp", snap);
    await fs.promises.rename(p + ".tmp", p);
  }).catch((e) => console.error("save failed", e.message));
  return writing;
}
const now = () => new Date().toISOString();

/* ---------- validation ---------- */
const ajv = new Ajv({ allErrors: true, strict: false });
addFormats(ajv);
const validate = ajv.compile(JSON.parse(fs.readFileSync(path.join(env.CANONICAL_DIR || "/canonical", "order.schema.json"), "utf8")));

/* ---------- broker ---------- */
let pubCh = null, subCh = null, connected = false, consuming = false;
const inflight = new Set();
const compensating = new Set();
const lastHop = new Map();

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
  lastHop.set(h.cid, id);
  try {
    const t0 = h.t0 || new Date(), t1 = h.t1 || new Date();
    pubCh?.publish("trace.events", "", Buffer.from(JSON.stringify({
      hopId: id, correlationId: h.cid, causationId: h.cause ?? null, service: "orchestrator", operation: h.op,
      status: h.status || "success", attempt: h.attempt || 1, ...(h.messageId ? { messageId: h.messageId } : {}),
      startedAt: t0.toISOString(), finishedAt: t1.toISOString(), durationMs: t1 - t0, ...(h.detail ? { detail: h.detail } : {}),
    })), { persistent: true, contentType: "application/json", messageId: randomUUID(), headers: { correlationId: h.cid } });
  } catch {}
  return id;
}
async function publishOutcome(saga, cid, cause) {
  const id = randomUUID(), t0 = new Date(), mid = randomUUID();
  await publishConfirmed("orders.outcome", saga.outcome.routingKey, saga.outcome.envelope,
    { messageId: mid, headers: { correlationId: cid, causationId: id } });
  hop({ id, cid, cause, op: "saga.outcome", t0, t1: new Date(), messageId: mid });
  return id;
}

/* ---------- provider calls ---------- */
async function call(base, p, body, key, cid, hopId) {
  const headers = { "Content-Type": "application/json", "X-Correlation-Id": cid, "X-Causation-Id": hopId };
  if (key) headers["Idempotency-Key"] = key;
  try {
    const r = await fetch(URLS[base] + p, { method: "POST", headers, body: JSON.stringify(body), signal: AbortSignal.timeout(TIMEOUT) });
    if (r.ok) { await r.text().catch(() => {}); return { ok: true }; }
    let code; try { const j = await r.json(); code = j.code || j.error; } catch {}
    return { ok: false, temp: r.status >= 500, code: code || (r.status >= 500 ? "service_unavailable" : "error_" + r.status) };
  } catch (e) {
    return { ok: false, temp: true, code: (e.name === "TimeoutError" || e.name === "AbortError") ? "timeout" : "unreachable" };
  }
}
async function doStep(op, base, p, body, key, cid, cause, attempt, messageId) {
  const id = randomUUID(), t0 = new Date();
  const r = await call(base, p, body, key, cid, id);
  hop({ id, cid, cause, op, attempt, messageId, t0, t1: new Date(),
    status: r.ok ? "success" : (r.code === "timeout" ? "timeout" : "failed"), detail: r.ok ? undefined : { code: r.code } });
  return { ...r, hopId: id };
}
const STEPS = {
  payment: { base: "payment", path: "/payment/authorize", key: (bk) => `authorize:${bk}` },
  inventory: { base: "inventory", path: "/inventory/reserve", key: (bk) => `reserve:${bk}` },
  shipping: { base: "shipping", path: "/shipping/create" },
  notification: { base: "notification", path: "/notification/send" },
};
const COMP = {
  payment: { op: "payment_refund", base: "payment", path: "/payment/refund", key: (bk) => `refund:${bk}` },
  inventory: { op: "inventory_release", base: "inventory", path: "/inventory/release", key: (bk) => `release:${bk}` },
};
function amountOf(order) {
  let cents = 0;
  for (const i of order.items) cents += Math.round(Number(i.unitPrice) * 100) * i.quantity;
  return `${Math.floor(cents / 100)}.${String(cents % 100).padStart(2, "0")}`;
}
function orderIdOf(bk) { return bk.slice(bk.indexOf(":") + 1); }
function completedOf(saga) { return saga.steps.filter((s) => s.status === "completed").map((s) => s.step); }
function compensatedOf(saga) { return saga.compensations.filter((c) => c.status === "completed").map((c) => c.step); }

async function compensateAll(saga, cid, cause) {
  const orderId = orderIdOf(saga.businessKey);
  let chain = cause;
  for (const st of [...completedOf(saga)].reverse()) {
    const c = COMP[st];
    if (!c || compensatedOf(saga).includes(c.op)) continue;
    const r = await doStep(c.op, c.base, c.path, { orderId }, c.key(saga.businessKey), cid, chain, 1);
    chain = r.hopId;
    saga.compensations.push({ step: c.op, status: r.ok ? "completed" : "failed", at: now() });
    saga.updatedAt = now();
    await save();
    if (!r.ok) return { ...r, chain };
  }
  return { ok: true, chain };
}

/* ---------- saga ---------- */
async function finish(saga, cid, base, state, code, cause) {
  saga.state = state; delete saga.retry; saga.updatedAt = now();
  saga.outcome = { routingKey: "order." + state, envelope: { ...base, state, ...(code ? { code } : {}), occurredAt: now() } };
  await save();
  await publishOutcome(saga, cid, cause);
}
function markStuck(saga, cid) {
  const base = { ...saga.outcome.envelope }; delete base.code; delete base.duplicateOf;
  saga.state = "needs_attention"; delete saga.retry; saga.updatedAt = now();
  saga.outcome = { routingKey: "order.needs_attention", envelope: { ...base, state: "needs_attention", occurredAt: now() } };
  return hop({ cid, cause: lastHop.get(cid) || randomUUID(), op: "saga.stuck", detail: { reason: "orchestrator restarted mid-saga" } });
}
async function detectStuck() {
  for (const [cid, saga] of Object.entries(store.sagas)) {
    if (saga.state === "processing" && !saga.retry && saga.owner !== RUN_ID && !inflight.has(cid) && saga.outcome) {
      const h = markStuck(saga, cid);
      await save();
      try { await publishOutcome(saga, cid, h); } catch (e) { console.error("stuck publish failed", e.message); }
    }
  }
}
const ack = (m) => { try { subCh.ack(m); } catch {} };
const nack = (m) => { try { subCh.nack(m, false, false); } catch {} };
function attemptOf(hdr) {
  const d = hdr["x-death"]; let n = 0;
  if (Array.isArray(d)) for (const e of d) if (e.queue === SAGA_Q && e.reason === "rejected") n += Number(e.count);
  return n + 1;
}

async function handle(msg) {
  const t0 = new Date();
  const hdr = msg.properties.headers || {};
  const cid = hdr.correlationId;
  let order = null;
  try {
    order = JSON.parse(msg.content.toString("utf8"));
    if (!cid || !validate(order)) { console.error("invalid order:", cid, ajv.errorsText(validate.errors)); order = null; }
  } catch (e) { console.error("unparseable message:", e.message); order = null; }
  if (!order) {
    await publishConfirmed("orders.invalid.exchange", "", msg.content, { messageId: msg.properties.messageId, headers: hdr });
    if (cid) hop({ cid, cause: hdr.causationId, op: "invalid.quarantine", status: "quarantined", t0, t1: new Date() });
    return ack(msg);
  }
  if (inflight.has(cid)) return ack(msg);
  inflight.add(cid);
  try {
    const bk = `${order.source}:${order.orderId}`;
    const attempt = attemptOf(hdr);
    const consumeId = hop({ cid, cause: hdr.causationId, op: "saga.consume", attempt, t0, messageId: msg.properties.messageId });
    const base = { correlationId: cid, businessKey: bk, orderId: order.orderId, source: order.source, orderType: order.orderType, recipient: order.customer.email };
    let saga = store.sagas[cid];

    if (saga && TERMINAL.includes(saga.state)) {
      await publishOutcome(saga, cid, consumeId);
      return ack(msg);
    }
    if (saga && saga.state === "processing" && !saga.retry) {
      const h = markStuck(saga, cid);
      await save();
      await publishOutcome(saga, cid, h);
      return ack(msg);
    }
    if (!saga) {
      const first = store.businessKeys[bk];
      if (first && first !== cid) {
        saga = store.sagas[cid] = { businessKey: bk, state: "duplicate", steps: [], compensations: [], owner: RUN_ID, updatedAt: now(),
          outcome: { routingKey: "order.duplicate", envelope: { ...base, state: "duplicate", duplicateOf: first, occurredAt: now() } } };
        const dh = hop({ cid, cause: consumeId, op: "saga.dedupe", status: "duplicate" });
        await save();
        await publishOutcome(saga, cid, dh);
        return ack(msg);
      }
      saga = store.sagas[cid] = { businessKey: bk, state: "processing", steps: [], compensations: [], owner: RUN_ID, updatedAt: now(), outcome: null };
      store.businessKeys[bk] = cid;
    }

    let prev = consumeId;
    if (!saga.retry) {
      saga.state = "processing"; saga.owner = RUN_ID; saga.steps = []; saga.compensations = []; delete saga.failureCode;
      saga.outcome = { routingKey: "order.processing", envelope: { ...base, state: "processing", occurredAt: now() } };
      saga.updatedAt = now();
      await save();
      prev = await publishOutcome(saga, cid, consumeId);
    }
    saga.owner = RUN_ID;

    const plan = order.orderType === "b2b" ? ["inventory", "shipping", "notification"] : ["payment", "inventory", "shipping", "notification"];
    const amount = amountOf(order);
    for (let i = 0; i < plan.length; i++) {
      const s = plan[i], d = STEPS[s];
      const body = s === "payment" ? { orderId: order.orderId, amount }
        : s === "inventory" ? { orderId: order.orderId, items: order.items }
        : s === "shipping" ? { orderId: order.orderId } : { orderId: order.orderId, recipient: order.customer.email };
      const r = await doStep(s, d.base, d.path, body, d.key ? d.key(bk) : undefined, cid, prev, attempt, msg.properties.messageId);
      prev = r.hopId;
      if (r.ok) {
        if (i === 0) delete saga.retry;
        saga.steps.push({ step: s, status: "completed", at: now() });
        saga.updatedAt = now();
        await save();
        continue;
      }
      saga.steps.push({ step: s, status: "failed", at: now() });
      if (i === 0) {
        if (r.temp) {
          if (attempt < MAX_RETRIES) {
            saga.retry = true; saga.updatedAt = now(); await save();
            hop({ cid, cause: consumeId, op: "saga.retry", status: "retried", attempt });
            return nack(msg);
          }
          const dlId = randomUUID();
          await publishConfirmed("orders.dlq.exchange", "", msg.content, { messageId: randomUUID(),
            headers: { correlationId: cid, causationId: dlId, businessKey: bk, originQueue: SAGA_Q, lastError: `${s} ${r.code}`, attempts: attempt, deadLetteredAt: now() } });
          hop({ id: dlId, cid, cause: prev, op: "saga.dead_letter", status: "dead_lettered", attempt, detail: { code: r.code } });
          await finish(saga, cid, base, "parked", undefined, dlId);
          return ack(msg);
        }
        await finish(saga, cid, base, "failed", r.code, prev);
        return ack(msg);
      }
      saga.failureCode = r.code;
      const c = await compensateAll(saga, cid, prev);
      if (c.ok) await finish(saga, cid, base, "compensated", r.code, c.chain);
      else await finish(saga, cid, base, "compensation_failed", c.code, c.chain);
      return ack(msg);
    }
    await finish(saga, cid, base, "completed", undefined, prev);
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
      await detectStuck();
      await sub.prefetch(CONC);
      await sub.consume(SAGA_Q, (m) => {
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

/* ---------- HTTP ---------- */
const app = express();
app.use(express.json());
app.get("/health", (_req, res) => {
  const ok = connected && consuming;
  res.status(ok ? 200 : 503).json({ status: ok ? "ok" : "degraded", checks: { broker: connected ? "up" : "down", consumers: consuming ? { [SAGA_Q]: 1 } : {} } });
});
app.get("/sagas/stuck", (_req, res) => {
  const sagas = Object.entries(store.sagas).filter(([, s]) => s.state === "needs_attention" || s.state === "compensation_failed")
    .map(([cid, s]) => ({ correlationId: cid, businessKey: s.businessKey, state: s.state, completedSteps: completedOf(s), compensatedSteps: compensatedOf(s), since: s.updatedAt }));
  res.json({ sagas });
});
app.post("/sagas/:cid/compensate", async (req, res) => {
  const cid = req.params.cid, saga = store.sagas[cid];
  if (!saga) return res.status(404).json({ code: "not_found" });
  if (compensating.has(cid)) return res.status(409).json({ code: "compensation_in_progress" });
  if (saga.state === "compensated" && saga.operatorCompensation) return res.json({ correlationId: cid, state: "compensated" });
  if (saga.state !== "needs_attention" && saga.state !== "compensation_failed") return res.status(409).json({ code: "saga_not_stuck" });
  compensating.add(cid);
  try {
    const wasStuck = saga.state === "needs_attention";
    const t0 = new Date(), opId = randomUUID();
    const c = await compensateAll(saga, cid, opId);
    const base = { ...saga.outcome.envelope }; delete base.code; delete base.duplicateOf;
    const state = c.ok ? "compensated" : "compensation_failed";
    const code = c.ok ? (wasStuck ? "operator_compensated" : (saga.failureCode || "operator_compensated")) : c.code;
    saga.operatorCompensation = { at: now(), state };
    await finish(saga, cid, base, state, code, opId);
    hop({ id: opId, cid, cause: lastHop.get(cid), op: "saga.operator_compensate", t0, t1: new Date(), detail: { state } });
    res.json({ correlationId: cid, state });
  } catch (e) {
    console.error(e); res.status(500).json({ code: "internal_error" });
  } finally { compensating.delete(cid); }
});

process.on("unhandledRejection", (e) => console.error("unhandled:", e));
app.listen(Number(env.PORT), () => console.log("listening"));
connectLoop();