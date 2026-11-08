/**
 * The checkout workflow: the PA6 saga ported to Temporal.
 * No I/O here. Every call to the outside world is an activity, through `act`.
 */
import {
  proxyActivities,
  ActivityFailure,
  ApplicationFailure,
  TimeoutFailure,
} from "@temporalio/workflow";
import type * as activities from "../activities";
import type { CanonicalOrder, CheckoutResult, TraceItem } from "../types";

const act = proxyActivities<typeof activities>({
  startToCloseTimeout: "2500 ms",
  retry: {
    maximumAttempts: 3,
    initialInterval: "200 ms",
    backoffCoefficient: 2,
  },
});

interface StepFailure {
  code: string;
  timeout: boolean;
}

function describeFailure(err: unknown): StepFailure {
  if (err instanceof ActivityFailure) {
    if (err.cause instanceof TimeoutFailure) return { code: "timeout", timeout: true };
    if (err.cause instanceof ApplicationFailure) {
      return { code: err.cause.type ?? "unknown", timeout: false };
    }
  }
  return { code: "unknown", timeout: false };
}

// Money is handled as whole cents (integers). No floats anywhere. (From PA6.)
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

function totalAmount(order: CanonicalOrder): string {
  return fromCents(
    order.items.reduce((sum, it) => sum + toCents(it.unitPrice) * it.quantity, 0),
  );
}

export async function checkout(order: CanonicalOrder): Promise<CheckoutResult> {
  const orderId = order.orderId;
  const trace: TraceItem[] = [];
  const done = new Set<string>();

  async function runStep(
    step: string,
    fn: () => Promise<void>,
  ): Promise<StepFailure | null> {
    const start = new Date();
    try {
      await fn();
      const end = new Date();
      trace.push({
        step,
        status: "success",
        startedAt: start.toISOString(),
        finishedAt: end.toISOString(),
        durationMs: end.getTime() - start.getTime(),
      });
      return null;
    } catch (err) {
      const end = new Date();
      const failure = describeFailure(err);
      trace.push({
        step,
        status: failure.timeout ? "timeout" : "failed",
        startedAt: start.toISOString(),
        finishedAt: end.toISOString(),
        durationMs: end.getTime() - start.getTime(),
      });
      return failure;
    }
  }

  const amount = totalAmount(order);

  const forward: Array<[string, () => Promise<void>]> = [
    ["payment", () => act.authorizePayment(orderId, amount)],
    ["inventory", () => act.reserveInventory(orderId, order.items)],
    ["shipping", () => act.createShipment(orderId)],
    ["notification", () => act.sendNotification(orderId, order.customer.email)],
  ];

  let failure: StepFailure | null = null;
  for (const [step, fn] of forward) {
    failure = await runStep(step, fn);
    if (failure) break;
    done.add(step);
  }

  if (!failure) {
    return { orderId, status: "completed", trace };
  }

  // Payment itself failed: nothing to undo.
  if (!done.has("payment")) {
    return { orderId, status: "failed", code: failure.code, trace };
  }

  // Compensate completed steps, newest first. Shipping and notification
  // have nothing to undo. Like PA6, keep going if one compensation fails.
  let compensationOk = true;
  if (done.has("inventory")) {
    const f = await runStep("inventory_release", () => act.releaseInventory(orderId));
    if (f) compensationOk = false;
  }
  const refundFailure = await runStep("payment_refund", () => act.refundPayment(orderId));
  if (refundFailure) compensationOk = false;

  if (!compensationOk) {
    return { orderId, status: "failed", code: "compensation_failed", trace };
  }
  return { orderId, status: "compensated", code: failure.code, trace };
}