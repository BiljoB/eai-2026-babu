
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { XMLParser } from "fast-xml-parser";


export interface Address {
  street: string;
  city: string;
  postalCode: string;
  country: string;
}

export interface CanonicalCustomer {
  name: string;
  email: string;
  address: Address;
}

export interface CanonicalItem {
  productId: string;
  productName: string;
  quantity: number;
  
  unitPrice: string;
  currency: string;
  taxRate: number;
}

export type OrderStatus = "new" | "processing" | "shipped" | "delivered";

export interface CanonicalOrder {
  orderId: string;
  orderType: "standard" | "express" | "b2b";
  source: "web" | "mobile" | "b2b";

  receivedAt: string;
  
  orderDate: string;
  customer: CanonicalCustomer;
  items: CanonicalItem[];
  currency: string;
  status: OrderStatus;
}

export type TransformWarningCode = "UNKNOWN_PRODUCT" | "PRICING_API_ERROR";

export interface TransformWarning {
  code: TransformWarningCode;
  productId: string;
  message: string;
}

export interface TransformResult {
  
  order: CanonicalOrder | null;
  warnings: TransformWarning[];
}

export interface TranslateOptions {
  
  pricingBaseUrl: string;
 
  apiKey?: string;
  
  now?: () => Date;
}

export const DEFAULT_PRICING_API_KEY = "pa4-pricing-key-2026";


const PA4_ROOT = fileURLToPath(new URL("../../", import.meta.url));

export const DEFAULT_WEB_ORDER_PATH = path.join(PA4_ROOT, "data", "web-order.json");
export const DEFAULT_MOBILE_ORDER_PATH = path.join(PA4_ROOT, "data", "mobile-order.json");
export const DEFAULT_B2B_ORDER_PATH = path.join(PA4_ROOT, "data", "b2b-order.xml");
export const DEFAULT_PRICING_BASE_URL = "http://localhost:4100";

const OUT_DIR = path.join(PA4_ROOT, "out");



export interface WebOrderInput {
  orderId: string;
  orderType: string;
  customer: {
    name: string;
    email: string;
    address: Address;
    payment: {
      method: string;
      cardHolder: string;
      cardNumber: string;
      expiryMonth: number;
      expiryYear: number;
    };
  };
  items: Array<{ productId: string; productName: string; quantity: number }>;
  orderDate: string;
  status: string;
  currency: string;
}

export interface MobileOrderInput {
  oid: string;
  ot: string;
  cust_name: string;
  cust_email: string;
  
  addr: string;
  items: Array<{ pid: string; pname: string; qty: number }>;
  
  ts: number;

  st: number;
  
  cur: number;
  pm: string;
  pan: string;
  pexp: string;
}


export interface ParsedB2BOrder {
  PurchaseOrder: {
    "@_orderId": string;
    "@_orderType": string;
    "@_orderDate": string;
    BuyerParty: {
      Name: string;
      ContactEmail: string;
      ShipToAddress: {
        "@_country": string;
        Street: string;
        City: string;
        PostalCode: string;
      };
      
      [key: string]: unknown;
    };
    LineItems: {
      "@_currency": string;
      
      LineItem: unknown;
    };
    Status: string;
    [key: string]: unknown;
  };
}


const xmlParser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "@_" });


export function normalizeProductId(source: "web" | "mobile" | "b2b", rawId: string): string {
  const id = rawId.trim();
  if (source === "mobile") return `PROD-${id}`;
  if (source === "b2b") return id.replace(/^SKU-/, "");
  return id;
}


export function toDecimalAmount(price: number): string {
  return price.toFixed(2);
}

export type EnrichResult =
  | { status: "ok"; unitPrice: string; currency: string; taxRate: number; productName: string }
  | { status: "unknown_product" }
  | { status: "error"; httpStatus: number };


export async function enrichFromPricing(
  productId: string,
  options: TranslateOptions,
): Promise<EnrichResult> {
  const base = options.pricingBaseUrl.replace(/\/+$/, "");
  const url = `${base}/pricing/${encodeURIComponent(productId)}`;
  const apiKey = options.apiKey ?? DEFAULT_PRICING_API_KEY;

  let res: Response;
  try {
    res = await fetch(url, { headers: { "X-API-Key": apiKey } });
  } catch {
    return { status: "error", httpStatus: 0 }; 
  }

  if (res.status === 404) return { status: "unknown_product" };
  if (!res.ok) return { status: "error", httpStatus: res.status };

  try {
    const body = (await res.json()) as {
      unitPrice: number;
      currency: string;
      taxRate: number;
      productName: string;
    };
    return {
      status: "ok",
      unitPrice: toDecimalAmount(body.unitPrice),
      currency: body.currency,
      taxRate: body.taxRate,
      productName: body.productName,
    };
  } catch {
    return { status: "error", httpStatus: res.status }; 
  }
}


export async function enrichItems(
  lines: Array<{ productId: string; productName: string; quantity: number }>,
  options: TranslateOptions,
): Promise<{ items: CanonicalItem[]; warnings: TransformWarning[] }> {
  const items: CanonicalItem[] = [];
  const warnings: TransformWarning[] = [];

  for (const line of lines) {
    const r = await enrichFromPricing(line.productId, options);
    if (r.status === "ok") {
      items.push({
        productId: line.productId,
        productName: line.productName,
        quantity: line.quantity,
        unitPrice: r.unitPrice,
        currency: r.currency,
        taxRate: r.taxRate,
      });
    } else if (r.status === "unknown_product") {
      warnings.push({
        code: "UNKNOWN_PRODUCT",
        productId: line.productId,
        message: `Product ${line.productId} not found in pricing catalog`,
      });
    } else {
      warnings.push({
        code: "PRICING_API_ERROR",
        productId: line.productId,
        message: `Pricing API error (HTTP ${r.httpStatus}) for ${line.productId}`,
      });
    }
  }
  return { items, warnings };
}


export function epochSecondsToIso(epochSeconds: number): string {
  return new Date(epochSeconds * 1000).toISOString();
}


export function mapCurrencyCode(numericCode: number): string {
  switch (numericCode) {
    case 978: return "EUR";
    case 840: return "USD";
    case 826: return "GBP";
    default: throw new Error(`Unhandled currency code: ${numericCode}`);
  }
}


export function mapMobileStatus(code: number): OrderStatus {
  switch (code) {
    case 1: return "new";
    case 2: return "processing";
    case 3: return "shipped";
    case 4: return "delivered";
    default: throw new Error(`Unknown mobile status code: ${code}`);
  }
}

export function mapB2BStatus(raw: string): OrderStatus {
  const s = String(raw).trim().toLowerCase();
  if (s === "new" || s === "processing" || s === "shipped" || s === "delivered") return s;
  throw new Error(`Unknown B2B status: ${raw}`);
}

export function parseMobileAddress(addr: string): Address {
  const parts = addr.split(",").map((p) => p.trim());

  return {
    street: parts[0] ?? "",
    city: parts[1] ?? "",
    postalCode: parts[2] ?? "",
    country: parts[3] ?? "",
  };
}

export function decodeB2BXmlBytes(bytes: Buffer): string {
  return new TextDecoder("windows-1257").decode(bytes);
}


export async function translateWeb(
  webOrderPath: string,
  options: TranslateOptions,
): Promise<TransformResult> {
  const src = JSON.parse(readFileSync(webOrderPath, "utf8")) as WebOrderInput;

  const lines = src.items.map((i) => ({
    productId: normalizeProductId("web", i.productId),
    productName: i.productName,
    quantity: i.quantity,
  }));
  const { items, warnings } = await enrichItems(lines, options);
  if (items.length === 0) return { order: null, warnings };

  const order: CanonicalOrder = {
    orderId: src.orderId,
    orderType: src.orderType as CanonicalOrder["orderType"],
    source: "web",
    receivedAt: (options.now?.() ?? new Date()).toISOString(),
    orderDate: new Date(src.orderDate).toISOString(),
    customer: {
      name: src.customer.name,
      email: src.customer.email,
      address: {
        street: src.customer.address.street,
        city: src.customer.address.city,
        postalCode: src.customer.address.postalCode,
        country: src.customer.address.country,
      },
    },
    items,
    currency: src.currency,
    status: src.status as OrderStatus,
  };
  return { order, warnings };
}


export async function translateMobile(
  mobileOrderPath: string,
  options: TranslateOptions,
): Promise<TransformResult> {
  const src = JSON.parse(readFileSync(mobileOrderPath, "utf8")) as MobileOrderInput;

  const lines = src.items.map((i) => ({
    productId: normalizeProductId("mobile", i.pid),
    productName: i.pname,
    quantity: i.qty,
  }));
  const { items, warnings } = await enrichItems(lines, options);
  if (items.length === 0) return { order: null, warnings };

  const order: CanonicalOrder = {
    orderId: src.oid,
    orderType: src.ot as CanonicalOrder["orderType"],
    source: "mobile",
    receivedAt: (options.now?.() ?? new Date()).toISOString(),
    orderDate: epochSecondsToIso(src.ts),
    customer: {
      name: src.cust_name,
      email: src.cust_email,
      address: parseMobileAddress(src.addr),
    },
    items,
    currency: mapCurrencyCode(src.cur),
    status: mapMobileStatus(src.st),
  };
  return { order, warnings };
}


export async function translateB2B(
  b2bOrderPath: string,
  options: TranslateOptions,
): Promise<TransformResult> {
  const xml = decodeB2BXmlBytes(readFileSync(b2bOrderPath));
  const po = (xmlParser.parse(xml) as ParsedB2BOrder).PurchaseOrder;
  const buyer = po.BuyerParty;
  const addr = buyer.ShipToAddress;

  const rawItems = po.LineItems.LineItem;
  const list = (rawItems == null ? [] : Array.isArray(rawItems) ? rawItems : [rawItems]) as Array<
    Record<string, unknown>
  >;

  const lines = list.map((li) => ({
    productId: normalizeProductId("b2b", String(li["@_sku"])),
    productName: String(li["Description"]),
    quantity: Number(li["@_quantity"]),
  }));
  const { items, warnings } = await enrichItems(lines, options);
  if (items.length === 0) return { order: null, warnings };

  const order: CanonicalOrder = {
    orderId: String(po["@_orderId"]),
    orderType: po["@_orderType"] as CanonicalOrder["orderType"],
    source: "b2b",
    receivedAt: (options.now?.() ?? new Date()).toISOString(),
    orderDate: new Date(po["@_orderDate"]).toISOString(),
    customer: {
      name: String(buyer.Name),
      email: String(buyer.ContactEmail),
      address: {
        street: String(addr.Street),
        city: String(addr.City),
        postalCode: String(addr.PostalCode),
        country: String(addr["@_country"]),
      },
    },
    items,
    currency: String(po.LineItems["@_currency"]),
    status: mapB2BStatus(po.Status),
  };
  return { order, warnings };
}


export async function main(): Promise<void> {
  const options: TranslateOptions = { pricingBaseUrl: DEFAULT_PRICING_BASE_URL };

  const results = {
    web: await translateWeb(DEFAULT_WEB_ORDER_PATH, options),
    mobile: await translateMobile(DEFAULT_MOBILE_ORDER_PATH, options),
    b2b: await translateB2B(DEFAULT_B2B_ORDER_PATH, options),
  };

  mkdirSync(OUT_DIR, { recursive: true });
  for (const [name, result] of Object.entries(results)) {
    const outPath = path.join(OUT_DIR, `${name}.json`);
    writeFileSync(outPath, JSON.stringify(result, null, 2) + "\n", "utf8");
    console.log(`wrote ${outPath}`);
  }
}


if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((err) => {
    console.error(err);
    process.exitCode = 1;
  });
}
