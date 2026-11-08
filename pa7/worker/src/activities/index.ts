/**
 * The six activities. Each makes one HTTP call to one mock service.
 * 4xx  -> non-retryable (business refusal, will refuse again)
 * 5xx / no answer -> retryable (temporary)
 * Slow calls are failed by Temporal's startToCloseTimeout, not here.
 */
import { ApplicationFailure } from "@temporalio/activity";
import type { CanonicalItem } from "../types";

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required environment variable: ${name}`);
  return value;
}

// docker-compose.yml sets these. Never hardcode a downstream URL.
const PAYMENT_URL = requiredEnv("PAYMENT_URL");
const INVENTORY_URL = requiredEnv("INVENTORY_URL");
const SHIPPING_URL = requiredEnv("SHIPPING_URL");
const NOTIFICATION_URL = requiredEnv("NOTIFICATION_URL");

async function post(url: string, body: unknown): Promise<void> {
  let res: Response;
  try {
    res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
  } catch (err) {
    // No answer at all: temporary, let the retry policy decide.
    throw ApplicationFailure.retryable(
      `No response from ${url}: ${(err as Error).message}`,
      "downstream_unavailable",
    );
  }

  if (res.ok) return;

  // Read the mock's own error code from the response body (same as PA6).
  let code = "downstream_error";
  try {
    const data: any = await res.json();
    code =
      data?.code ??
      data?.error?.code ??
      (typeof data?.error === "string" ? data.error : undefined) ??
      "downstream_error";
  } catch {
    // body was not JSON; keep the fallback code
  }
  code = String(code);

  const message = `POST ${url} failed with ${res.status} (${code})`;

  if (res.status >= 400 && res.status < 500) {
    // Business refusal: it will refuse again, so never retry.
    throw ApplicationFailure.nonRetryable(message, code);
  }
  // 5xx: temporary.
  throw ApplicationFailure.retryable(message, code);
}

/** POST {PAYMENT_URL}/payment/authorize with { orderId, amount }. */
export async function authorizePayment(orderId: string, amount: string): Promise<void> {
  await post(`${PAYMENT_URL}/payment/authorize`, { orderId, amount });
}

/** POST {INVENTORY_URL}/inventory/reserve with { orderId, items }. */
export async function reserveInventory(orderId: string, items: CanonicalItem[]): Promise<void> {
  await post(`${INVENTORY_URL}/inventory/reserve`, { orderId, items });
}

/** POST {SHIPPING_URL}/shipping/create with { orderId }. */
export async function createShipment(orderId: string): Promise<void> {
  await post(`${SHIPPING_URL}/shipping/create`, { orderId });
}

/** POST {NOTIFICATION_URL}/notification/send with { orderId, recipient }. */
export async function sendNotification(orderId: string, recipient: string): Promise<void> {
  await post(`${NOTIFICATION_URL}/notification/send`, { orderId, recipient });
}

/** Compensates authorizePayment: POST {PAYMENT_URL}/payment/refund with { orderId }. */
export async function refundPayment(orderId: string): Promise<void> {
  await post(`${PAYMENT_URL}/payment/refund`, { orderId });
}

/** Compensates reserveInventory: POST {INVENTORY_URL}/inventory/release with { orderId }. */
export async function releaseInventory(orderId: string): Promise<void> {
  await post(`${INVENTORY_URL}/inventory/release`, { orderId });
}