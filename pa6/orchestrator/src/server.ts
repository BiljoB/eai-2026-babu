/**
 * PA6 orchestrator: saga over payment, inventory, shipping, notification.
 * Strict sequencing, timeouts, compensation in reverse order, idempotency,
 * restart-safe file persistence.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import express, { type Request, type Response } from "express";
import Ajv from "ajv";
import addFormats from "ajv-formats";
import type { CanonicalOrder } from "./canonical-order";

const app = express();
app.use(express.json());
app.use(express.static(path.join(process.cwd(), "public")));

interface Config {
  port: number;
  paymentUrl: string;
  inventoryUrl: string;
  shippingUrl: string;
  notificationUrl: string;
  requestTimeoutMs: number;
}

function readRequiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function loadConfig(): Config {
  return {
    port: Number(process.env.ORCHESTRATOR_PORT || 3000),
    paymentUrl: readRequiredEnv("PAYMENT_URL"),
    inventoryUrl: readRequiredEnv("INVENTORY_URL"),
    shippingUrl: readRequiredEnv("SHIPPING_URL"),
    notificationUrl: readRequiredEnv("NOTIFICATION_URL"),
    requestTimeoutMs: Number(process.env.REQUEST_TIMEOUT_MS || 2500),
  };
}

const config = loadConfig();

const DATA_DIR = "/data";
const IDEMPOTENCY_STORE_PATH = path.join(DATA_DIR, "idempotency-store.json");
const SAGA_STORE_PATH = path.join(DATA_DIR, "saga-store.json");

// Must match maxItems on the items array in openapi.yaml.
const MAX_ITEMS = 25;

// ---------------------------------------------------------------- contract --
// These shapes mirror grading/schema/*.json.

export interface TraceItem {
  step: string;
  status: string;
  startedAt: string;
  finishedAt: string;
  durationMs: number;
}

export type SagaState = "completed" | "failed" | "compensated";
export type IdempotencyState = "in_progress" | "completed" | "failed" | "compensated";

interface IdempotencyRecord {
  requestHash: string;
  state: IdempotencyState;
  httpStatus: number;
  response: unknown;
  updatedAt: string;
}

interface IdempotencyStore {
  records: Record<string, IdempotencyRecord>;
}

interface SagaRecord {
  idempotencyKey: string;
  state: SagaState;
  steps: TraceItem[];
  updatedAt: string;
}

interface SagaStore {
  sagas: Record<string, SagaRecord>;
}

function ensureJsonFile(filePath: string, initialData: unknown): void {
  const dirPath = path.dirname(filePath);
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
  if (!fs.existsSync(filePath)) {
    fs.writeFileSync(filePath, JSON.stringify(initialData, null, 2), "utf8");
  }
}

function readJsonFile<T>(filePath: string): T {
  ensureJsonFile(filePath, {});
  const raw = fs.readFileSync(filePath, "utf8");
  return JSON.parse(raw || "{}") as T;
}

function writeJsonFile(filePath: string, value: unknown): void {
  fs.writeFileSync(filePath, JSON.stringify(value, null, 2), "utf8");
}

function nowIso(): string {
  return new Date().toISOString();
}

function payloadHash(payload: unknown): string {
  const normalized = JSON.stringify(payload);
  const hash = crypto.createHash("sha256").update(normalized).digest("hex");
  return `sha256:${hash}`;
}

// The request body is a canonical order. docker-compose.yml mounts the
// canonical/ folder read-only at /canonical.
const CANONICAL_SCHEMA_PATH = "/canonical/order.schema.json";

if (!fs.existsSync(CANONICAL_SCHEMA_PATH)) {
  throw new Error(
    `${CANONICAL_SCHEMA_PATH} not found. docker-compose.yml mounts ../canonical ` +
      "from your repository root: copy canonical/ next to pa6/, then run docker compose up again.",
  );
}

const ajv = new Ajv({ allErrors: true });
addFormats(ajv);
const validateCanonicalOrder = ajv.compile(JSON.parse(fs.readFileSync(CANONICAL_SCHEMA_PATH, "utf8")));

function validateCheckoutPayload(payload: unknown): string | null {
  if (validateCanonicalOrder(payload)) {
    return null;
  }
  return `Request body is not a canonical order: ${ajv.errorsText(validateCanonicalOrder.errors, { dataVar: "body" })}`;
}

function bootstrapStores(): void {
  ensureJsonFile(IDEMPOTENCY_STORE_PATH, { records: {} });
  ensureJsonFile(SAGA_STORE_PATH, { sagas: {} });
}

// ------------------------------------------------------------ saga helpers --

// Money is handled as whole cents (integers). No floats anywhere.
function toCents(price: string): number {
  const m = /^(\d+)(?:\.(\d{1,2}))?$/.exec(String(price).trim());
  if (!m) throw new Error(`Invalid money value: ${price}`);
  const whole = parseInt(m[1] ?? "0", 10);
  const frac = parseInt((m[2] ?? "").padEnd(2, "0"), 10);
  return whole * 100 + frac;
}

function fromCents(cents: number): string {
  return `${Math.floor(cents / 100)}.${String(cents % 100).padStart(2, "0")}`;
}

interface CallOutcome {
  ok: boolean;
  timedOut: boolean;
  code: string;
  message: string;
  trace: TraceItem;
}

function extractErrorCode(text: string): { code: string; message: string } {
  let parsed: any;
  try {
    parsed = JSON.parse(text);
  } catch {
    parsed = undefined;
  }
  const code =
    parsed?.code ??
    parsed?.error?.code ??
    (typeof parsed?.error === "string" ? parsed.error : undefined) ??
    "downstream_error";
  const message = parsed?.message ?? parsed?.error?.message ?? "Downstream call failed";
  return { code: String(code), message: String(message) };
}

async function callDownstream(step: string, url: string, body: unknown): Promise<CallOutcome> {
  const startedMs = Date.now();
  const startedAt = new Date(startedMs).toISOString();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), config.requestTimeoutMs);

  let ok = false;
  let timedOut = false;
  let code = "downstream_error";
  let message = "Downstream call failed";

  try {
    const r = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal: controller.signal,
    });
    const text = await r.text();
    ok = r.ok;
    if (!ok) {
      ({ code, message } = extractErrorCode(text));
    }
  } catch {
    if (controller.signal.aborted) {
      timedOut = true;
      code = "timeout";
      message = `${step} did not answer within ${config.requestTimeoutMs} ms`;
    } else {
      code = "downstream_unavailable";
      message = `${step} could not be reached`;
    }
  } finally {
    clearTimeout(timer);
  }

  const finishedMs = Date.now();
  return {
    ok,
    timedOut,
    code,
    message,
    trace: {
      step,
      status: ok ? "success" : timedOut ? "timeout" : "failed",
      startedAt,
      finishedAt: new Date(finishedMs).toISOString(),
      durationMs: finishedMs - startedMs,
    },
  };
}

interface Step {
  name: string;
  url: string;
  body: unknown;
  comp?: { name: string; url: string; body: unknown };
}

function persistAndRespond(
  res: Response,
  idempotencyKey: string,
  requestHash: string,
  orderId: string,
  httpStatus: number,
  state: SagaState,
  responseBody: unknown,
  trace: TraceItem[],
): void {
  // Saga store first, idempotency record last, so a replay always has a saga behind it.
  const sagaStore = readJsonFile<SagaStore>(SAGA_STORE_PATH);
  if (!sagaStore.sagas) sagaStore.sagas = {};
  sagaStore.sagas[orderId] = { idempotencyKey, state, steps: trace, updatedAt: nowIso() };
  writeJsonFile(SAGA_STORE_PATH, sagaStore);

  const idemStore = readJsonFile<IdempotencyStore>(IDEMPOTENCY_STORE_PATH);
  if (!idemStore.records) idemStore.records = {};
  idemStore.records[idempotencyKey] = {
    requestHash,
    state,
    httpStatus,
    response: responseBody,
    updatedAt: nowIso(),
  };
  writeJsonFile(IDEMPOTENCY_STORE_PATH, idemStore);

  res.status(httpStatus).json(responseBody);
}

async function handleCheckout(req: Request, res: Response): Promise<void> {
  const idempotencyKey = req.header("Idempotency-Key");
  if (!idempotencyKey) {
    res.status(400).json({ code: "validation_error", message: "Idempotency-Key header is required" });
    return;
  }

  const validationError = validateCheckoutPayload(req.body);
  if (validationError) {
    res.status(400).json({ code: "validation_error", message: validationError });
    return;
  }

  const order = req.body as CanonicalOrder;
  const orderId = order.orderId;
  const items = order.items as unknown as Array<{ unitPrice: string; quantity: number }>;

  // The bound promised by the contract (the canonical schema does not enforce it).
  if (items.length > MAX_ITEMS) {
    res.status(400).json({
      code: "validation_error",
      message: `Order has ${items.length} items; the maximum is ${MAX_ITEMS}`,
    });
    return;
  }

  let amount: string;
  try {
    amount = fromCents(items.reduce((sum, it) => sum + toCents(it.unitPrice) * it.quantity, 0));
  } catch (err) {
    res.status(400).json({ code: "validation_error", message: (err as Error).message });
    return;
  }

  // ---- idempotency: check and claim the key with no await in between ----
  const requestHash = payloadHash(req.body);
  const idempotencyStore = readJsonFile<IdempotencyStore>(IDEMPOTENCY_STORE_PATH);
  if (!idempotencyStore.records) idempotencyStore.records = {};

  const existing = idempotencyStore.records[idempotencyKey];
  if (existing) {
    if (existing.requestHash !== requestHash) {
      res.status(409).json({
        code: "idempotency_payload_mismatch",
        message: "This Idempotency-Key is already used for a different payload",
      });
      return;
    }
    if (existing.state === "in_progress") {
      res.status(409).json({
        code: "idempotency_conflict",
        message: "A request with this Idempotency-Key is still being processed",
      });
      return;
    }
    // Finished: replay exactly. The saga does not run again.
    res.status(existing.httpStatus).json(existing.response);
    return;
  }

  idempotencyStore.records[idempotencyKey] = {
    requestHash,
    state: "in_progress",
    httpStatus: 202,
    response: { orderId, status: "in_progress" },
    updatedAt: nowIso(),
  };
  writeJsonFile(IDEMPOTENCY_STORE_PATH, idempotencyStore);

  // ---- the saga ----
  const trace: TraceItem[] = [];
  try {
    const steps: Step[] = [
      {
        name: "payment",
        url: `${config.paymentUrl}/payment/authorize`,
        body: { orderId, amount },
        comp: { name: "payment_refund", url: `${config.paymentUrl}/payment/refund`, body: { orderId } },
      },
      {
        name: "inventory",
        url: `${config.inventoryUrl}/inventory/reserve`,
        body: { orderId, items: order.items },
        comp: { name: "inventory_release", url: `${config.inventoryUrl}/inventory/release`, body: { orderId } },
      },
      { name: "shipping", url: `${config.shippingUrl}/shipping/create`, body: { orderId } },
      {
        name: "notification",
        url: `${config.notificationUrl}/notification/send`,
        body: { orderId, recipient: order.customer.email },
      },
    ];

    const completed: Step[] = [];
    for (const step of steps) {
      const result = await callDownstream(step.name, step.url, step.body); // strictly one at a time
      trace.push(result.trace);
      if (result.ok) {
        completed.push(step);
        continue;
      }

      // Failure: compensate the completed steps in reverse order.
      let compensationFailed = false;
      for (const done of [...completed].reverse()) {
        if (!done.comp) continue;
        const c = await callDownstream(done.comp.name, done.comp.url, done.comp.body);
        trace.push(c.trace);
        if (!c.ok) compensationFailed = true; // still try the remaining compensations
      }

      if (compensationFailed) {
        persistAndRespond(
          res, idempotencyKey, requestHash, orderId, 422, "failed",
          {
            orderId,
            status: "failed",
            code: "compensation_failed",
            message: "A compensation call failed; manual reconciliation is needed",
            trace,
          },
          trace,
        );
        return;
      }

      const state: SagaState = completed.length > 0 ? "compensated" : "failed";
      const httpStatus = result.timedOut ? 504 : 422;
      persistAndRespond(
        res, idempotencyKey, requestHash, orderId, httpStatus, state,
        {
          orderId,
          status: state,
          code: result.timedOut ? "timeout" : result.code,
          message: result.message,
          trace,
        },
        trace,
      );
      return;
    }

    persistAndRespond(
      res, idempotencyKey, requestHash, orderId, 200, "completed",
      { orderId, status: "completed", trace },
      trace,
    );
  } catch (err) {
    // Unexpected crash: free the key so the client can retry.
    const store = readJsonFile<IdempotencyStore>(IDEMPOTENCY_STORE_PATH);
    if (store.records) {
      delete store.records[idempotencyKey];
      writeJsonFile(IDEMPOTENCY_STORE_PATH, store);
    }
    throw err;
  }
}

// ------------------------------------------------------------------ routes --

app.get("/health", (_req: Request, res: Response) => {
  res.status(200).json({ status: "ok" });
});

app.get("/debug/trace/:orderId", (req: Request, res: Response) => {
  const orderIdParam = req.params.orderId ?? "";
  const sagaStore = readJsonFile<SagaStore>(SAGA_STORE_PATH);
  const saga = sagaStore?.sagas?.[orderIdParam];
  if (!saga) {
    res.status(404).json({ code: "not_found", message: "No saga found for this orderId" });
    return;
  }
  res.status(200).json(saga);
});

app.post("/checkout", async (req: Request, res: Response) => {
  try {
    await handleCheckout(req, res);
  } catch (err) {
    console.error("[orchestrator] unexpected error", err);
    if (!res.headersSent) {
      res.status(500).json({ code: "internal_error", message: "Unexpected error" });
    }
  }
});

bootstrapStores();

app.listen(config.port, () => {
  console.log(`[orchestrator] listening on port ${config.port}`);
  console.log("[orchestrator] downstream targets loaded from env", {
    paymentUrl: config.paymentUrl,
    inventoryUrl: config.inventoryUrl,
    shippingUrl: config.shippingUrl,
    notificationUrl: config.notificationUrl,
    requestTimeoutMs: config.requestTimeoutMs,
  });
});