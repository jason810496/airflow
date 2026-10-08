/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Deterministic stand-ins for the storefront's order, refund and inventory exports. Everything is
// seeded by the batch id, so a retried task writes the same file and the three exports agree on
// which orders exist.

import { createHash } from "node:crypto";

import { PRODUCTS, PRODUCTS_BY_SKU, leadTimeDays, type Product } from "./catalog.js";
import { CHECKOUT_RULES } from "./checkout-rules.js";
import { DEFAULT_FX_RATES, fromUsdCents, toUsdCents } from "./money.js";
import type { Currency, InventoryItem, LineItem, Order, Refund } from "./types.js";

export interface Rng {
  next(): number;
  int(min: number, max: number): number;
  pick<T>(items: readonly T[]): T;
  chance(probability: number): boolean;
  weighted<T>(items: readonly (readonly [T, number])[]): T;
  shuffle<T>(items: readonly T[]): T[];
}

/** mulberry32 over a sha256 of the seed. */
export function createRng(seed: string): Rng {
  let state = createHash("sha256").update(seed).digest().readUInt32LE(0);
  const next = (): number => {
    state = (state + 0x6d2b79f5) | 0;
    let t = Math.imul(state ^ (state >>> 15), 1 | state);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  const int = (min: number, max: number): number => min + Math.floor(next() * (max - min + 1));
  const pick = <T>(items: readonly T[]): T => items[int(0, items.length - 1)] as T;
  const weighted = <T>(items: readonly (readonly [T, number])[]): T => {
    let roll = next() * items.reduce((total, [, weight]) => total + weight, 0);
    for (const [value, weight] of items) {
      roll -= weight;
      if (roll < 0) return value;
    }
    return (items[items.length - 1] as readonly [T, number])[0];
  };
  const shuffle = <T>(items: readonly T[]): T[] => {
    const copy = [...items];
    for (let i = copy.length - 1; i > 0; i -= 1) {
      const j = int(0, i);
      [copy[i], copy[j]] = [copy[j] as T, copy[i] as T];
    }
    return copy;
  };
  return { next, int, pick, chance: (p) => next() < p, weighted, shuffle };
}

interface Country {
  code: string;
  currency: Currency;
}

const HOME_COUNTRIES: readonly (readonly [Country, number])[] = [
  [{ code: "US", currency: "USD" }, 46],
  [{ code: "CA", currency: "USD" }, 6],
  [{ code: "AU", currency: "USD" }, 3],
  [{ code: "GB", currency: "GBP" }, 10],
  [{ code: "DE", currency: "EUR" }, 9],
  [{ code: "FR", currency: "EUR" }, 6],
  [{ code: "NL", currency: "EUR" }, 3],
  [{ code: "ES", currency: "EUR" }, 3],
  [{ code: "IE", currency: "EUR" }, 2],
  [{ code: "JP", currency: "JPY" }, 7],
];
const FRAUD_SHIP_TO = ["NG", "VN", "BR", "ID", "PH"] as const;
const FIRST_NAMES = [
  "alex",
  "sam",
  "maya",
  "liam",
  "noah",
  "emma",
  "olivia",
  "ava",
  "kai",
  "yuki",
  "lena",
  "omar",
  "ines",
  "theo",
];
const LAST_NAMES = [
  "smith",
  "tanaka",
  "garcia",
  "mueller",
  "dupont",
  "jones",
  "kim",
  "nguyen",
  "silva",
  "rossi",
  "brown",
  "lee",
];
const EMAIL_DOMAINS: readonly (readonly [string, number])[] = [
  ["gmail.com", 50],
  ["outlook.com", 14],
  ["yahoo.com", 12],
  ["icloud.com", 10],
  ["hotmail.com", 8],
  ["proton.me", 4],
];
const DISPOSABLE_DOMAINS = ["tempmail.dev", "mailinator.example", "burner.example"] as const;
const REFUND_REASONS: readonly (readonly [string, number])[] = [
  ["wrong_size", 30],
  ["changed_mind", 25],
  ["damaged", 15],
  ["not_as_described", 15],
  ["late_delivery", 10],
  ["duplicate_order", 5],
];
const HOUR_WEIGHTS = [1, 1, 1, 1, 1, 2, 3, 5, 7, 8, 8, 9, 10, 9, 8, 8, 8, 9, 10, 11, 10, 8, 5, 3];

/** Organic orders stay this far below the rule thresholds, so a modest FX move cannot trip them. */
const ORGANIC_HEADROOM = 0.7;
/** Chance that a batch holds one legitimate order that checkout would still flag. */
const BASELINE_NOISE_RATE = 0.08;

export interface BatchParams {
  batchId: string;
  businessDate: string;
  targetOrders: number;
}

interface Customer {
  id: string;
  ageDays: number;
  email: string;
  cards: string[];
  country: Country;
}

function cardFingerprint(seed: string): string {
  return `card_${createHash("sha256").update(seed).digest("hex").slice(0, 12)}`;
}

function priceIn(product: Product, currency: Currency): number {
  const minor = fromUsdCents(product.priceUsdCents, currency, DEFAULT_FX_RATES);
  return currency === "JPY"
    ? Math.round(minor / 10) * 10
    : Math.max(99, Math.round(minor / 100) * 100 - 1);
}

function totalOf(items: readonly LineItem[]): number {
  return items.reduce((total, item) => total + item.qty * item.unit_price_minor, 0);
}

function usdOf(items: readonly LineItem[], currency: Currency): number {
  return toUsdCents(totalOf(items), currency, DEFAULT_FX_RATES);
}

function pickItems(rng: Rng, currency: Currency, maxUsdCents?: number): LineItem[] {
  const weights = PRODUCTS.map((p) => [p, p.popularity] as const);
  for (let attempt = 0; attempt < 6; attempt += 1) {
    const lines = rng.weighted([
      [1, 55],
      [2, 28],
      [3, 12],
      [4, 5],
    ] as const);
    const items: LineItem[] = [];
    for (let i = 0; i < lines; i += 1) {
      const product = rng.weighted(weights);
      if (items.some((item) => item.sku === product.sku)) continue;
      items.push({
        sku: product.sku,
        qty: rng.weighted([
          [1, 80],
          [2, 15],
          [3, 5],
        ] as const),
        unit_price_minor: priceIn(product, currency),
      });
    }
    if (maxUsdCents === undefined || usdOf(items, currency) <= maxUsdCents) return items;
  }
  const fallback = PRODUCTS_BY_SKU.get("LIP-001") as Product;
  return [{ sku: fallback.sku, qty: 1, unit_price_minor: priceIn(fallback, currency) }];
}

/** The unit count that brings `product` closest to `usdCents`, at least one. */
function qtyNear(product: Product, usdCents: number): number {
  return Math.max(1, Math.round(usdCents / product.priceUsdCents));
}

function timestampOf(dayStart: number, secondOfDay: number): string {
  return new Date(dayStart + secondOfDay * 1000).toISOString();
}

function dayStartOf(businessDate: string): number {
  return Date.parse(`${businessDate}T00:00:00Z`);
}

function compactDate(businessDate: string): string {
  return businessDate.replaceAll("-", "");
}

function orderId(businessDate: string, sequence: number): string {
  return `ORD-${compactDate(businessDate)}-${String(sequence).padStart(5, "0")}`;
}

function newCustomer(rng: Rng, taken: Set<string>, ageDays?: number, disposable = false): Customer {
  let id = "";
  do id = `C-${rng.int(100000, 999999)}`;
  while (taken.has(id));
  taken.add(id);
  const country = rng.weighted(HOME_COUNTRIES);
  const age =
    ageDays ??
    rng.weighted([
      [rng.int(0, 1), 2],
      [rng.int(2, 14), 6],
      [Math.min(1800, Math.floor(-Math.log(1 - rng.next()) * 360) + 15), 92],
    ] as const);
  const domain = disposable ? rng.pick(DISPOSABLE_DOMAINS) : rng.weighted(EMAIL_DOMAINS);
  const email = `${rng.pick(FIRST_NAMES)}.${rng.pick(LAST_NAMES)}${rng.int(1, 99)}@${domain}`;
  const cards = Array.from({ length: rng.chance(0.15) ? 2 : 1 }, (_, k) =>
    cardFingerprint(`${id}:${k}`),
  );
  return { id, ageDays: age, email, cards, country };
}

function toOrder(
  rng: Rng,
  customer: Customer,
  items: LineItem[],
  currency: Currency,
  shippingCountry: string,
  placedAt: string,
  card = rng.pick(customer.cards),
): Order {
  return {
    order_id: "",
    customer_id: customer.id,
    account_age_days: customer.ageDays,
    email: customer.email,
    card_fingerprint: card,
    billing_country: customer.country.code,
    shipping_country: shippingCountry,
    currency,
    items,
    total_minor: totalOf(items),
    placed_at: placedAt,
  };
}

/** How many orders the day holds, around `targetOrders`. */
export function organicOrderCount(params: BatchParams): number {
  return Math.round(
    params.targetOrders * (0.94 + 0.12 * createRng(`${params.batchId}:count`).next()),
  );
}

/** The day's ordinary orders. Independent of any fraud or stock knob, and numbered 1..n by time. */
export function generateOrganicOrders(params: BatchParams): Order[] {
  const { batchId, businessDate } = params;
  const rng = createRng(`${batchId}:orders`);
  const count = organicOrderCount(params);
  const dayStart = dayStartOf(businessDate);
  const taken = new Set<string>();
  const pool = Array.from({ length: Math.round(count * 0.72) }, () => newCustomer(rng, taken));
  const uses = new Map<string, number>();
  const hours = HOUR_WEIGHTS.map((weight, hour) => [hour, weight] as const);
  const crossBorderCap = CHECKOUT_RULES.crossBorder.minUsdCents * ORGANIC_HEADROOM;
  const newAccountCap = CHECKOUT_RULES.newAccount.minUsdCents * ORGANIC_HEADROOM;

  const orders: Order[] = [];
  for (let i = 0; i < count; i += 1) {
    let customer = rng.pick(pool);
    while ((uses.get(customer.id) ?? 0) >= 3) customer = rng.pick(pool);
    uses.set(customer.id, (uses.get(customer.id) ?? 0) + 1);

    const currency = customer.country.currency;
    const isNew = customer.ageDays < CHECKOUT_RULES.newAccount.maxAgeDays;
    const items = pickItems(rng, currency, isNew ? newAccountCap : undefined);
    let shipping = rng.chance(0.04) ? rng.weighted(HOME_COUNTRIES).code : customer.country.code;
    if (shipping !== customer.country.code && usdOf(items, currency) > crossBorderCap) {
      shipping = customer.country.code;
    }
    const second = rng.weighted(hours) * 3600 + rng.int(0, 3599);
    orders.push(toOrder(rng, customer, items, currency, shipping, timestampOf(dayStart, second)));
  }
  orders.sort((a, b) => a.placed_at.localeCompare(b.placed_at));
  orders.forEach((order, index) => {
    order.order_id = orderId(businessDate, index + 1);
  });
  return orders;
}

/** One legitimate gift to another country, which checkout would flag all the same. */
function baselineNoise(params: BatchParams, sequence: number): Order[] {
  const rng = createRng(`${params.batchId}:noise`);
  if (!rng.chance(BASELINE_NOISE_RATE)) return [];
  const customer = newCustomer(rng, new Set(), rng.int(200, 900));
  customer.country = { code: "US", currency: "USD" };
  const laptop = PRODUCTS_BY_SKU.get("LAP-001") as Product;
  const items: LineItem[] = [{ sku: laptop.sku, qty: 1, unit_price_minor: priceIn(laptop, "USD") }];
  const order = toOrder(
    rng,
    customer,
    items,
    "USD",
    "DE",
    timestampOf(dayStartOf(params.businessDate), rng.int(36000, 64800)),
  );
  order.order_id = orderId(params.businessDate, sequence);
  return [order];
}

/**
 * One organised attack, all from brand-new accounts with disposable emails: a card-testing burst on a
 * stolen card that turns into cash-out orders, bulk laptops shipped to a high-risk corridor, and
 * high-value electronics bought from new UK accounts. Each order is worth enough to matter.
 */
function fraudOrders(params: BatchParams, firstSequence: number): Order[] {
  const rng = createRng(`${params.batchId}:fraud`);
  const dayStart = dayStartOf(params.businessDate);
  const taken = new Set<string>();
  const orders: Order[] = [];
  const product = (sku: string): Product => PRODUCTS_BY_SKU.get(sku) as Product;
  const fraudster = (country: Country): Customer => {
    const customer = newCustomer(rng, taken, rng.int(0, 1), true);
    customer.country = country;
    return customer;
  };
  const push = (order: Order): void => {
    order.order_id = orderId(params.businessDate, firstSequence + orders.length);
    orders.push(order);
  };
  const usd: Country = { code: "US", currency: "USD" };
  const gb: Country = { code: "GB", currency: "GBP" };
  const lineOf = (item: Product, qty: number, currency: Currency): LineItem[] => [
    { sku: item.sku, qty, unit_price_minor: priceIn(item, currency) },
  ];

  const stolenCard = cardFingerprint(`${params.batchId}:stolen`);
  const probes = rng.int(3, 4);
  const cashOuts = rng.int(5, 7);
  const cashOutProducts = ["LAP-001", "TAB-001", "CAM-001", "EAR-002", "WCH-001"].map(product);
  let burstAt = dayStart + rng.int(2 * 3600, 20 * 3600) * 1000;
  for (let i = 0; i < probes + cashOuts; i += 1) {
    const item = i < probes ? product("GFT-025") : rng.pick(cashOutProducts);
    const qty = i < probes ? 1 : qtyNear(item, rng.int(60_000, 140_000));
    burstAt += rng.int(30, 75) * 1000;
    push(
      toOrder(
        rng,
        fraudster(usd),
        lineOf(item, qty, "USD"),
        "USD",
        "US",
        new Date(burstAt).toISOString(),
        stolenCard,
      ),
    );
  }

  const laptop = product("LAP-001");
  for (let i = 0, count = rng.int(3, 5); i < count; i += 1) {
    const qty = qtyNear(laptop, rng.int(150_000, 300_000));
    push(
      toOrder(
        rng,
        fraudster(usd),
        lineOf(laptop, qty, "USD"),
        "USD",
        rng.pick(FRAUD_SHIP_TO),
        timestampOf(dayStart, rng.int(3600, 82800)),
      ),
    );
  }

  const ukProducts = ["LAP-001", "CAM-001", "EAR-002"].map(product);
  for (let i = 0, count = rng.int(2, 3); i < count; i += 1) {
    const item = rng.pick(ukProducts);
    const qty = qtyNear(item, rng.int(120_000, 250_000));
    push(
      toOrder(
        rng,
        fraudster(gb),
        lineOf(item, qty, "GBP"),
        "GBP",
        "GB",
        timestampOf(dayStart, rng.int(3600, 82800)),
      ),
    );
  }
  return orders;
}

export function generateOrders(params: BatchParams & { injectFraud: boolean }): Order[] {
  const organic = generateOrganicOrders(params);
  const noise = baselineNoise(params, organic.length + 1);
  const fraud = params.injectFraud ? fraudOrders(params, organic.length + noise.length + 1) : [];
  return [...organic, ...noise, ...fraud].sort((a, b) => a.placed_at.localeCompare(b.placed_at));
}

export function generateRefunds(params: BatchParams & { refundRate: number }): Refund[] {
  const { batchId, businessDate } = params;
  const rng = createRng(`${batchId}:refunds`);
  const orders = generateOrganicOrders(params);
  const dayStart = dayStartOf(businessDate);
  const count = Math.round(orders.length * params.refundRate * (0.8 + 0.4 * rng.next()));
  const refunds: Refund[] = [];

  const add = (id: string, currency: Currency, amountMinor: number, placedAtMs: number): void => {
    const earliest = Math.max(placedAtMs + 15 * 60_000, dayStart);
    const at = Math.min(earliest + rng.int(0, 6 * 3600) * 1000, dayStart + 86_399_000);
    refunds.push({
      refund_id: `RF-${compactDate(businessDate)}-${String(refunds.length + 1).padStart(4, "0")}`,
      order_id: id,
      currency,
      amount_minor: amountMinor,
      reason: rng.weighted(REFUND_REASONS),
      refunded_at: new Date(at).toISOString(),
    });
  };

  for (const order of rng.shuffle(orders).slice(0, count)) {
    const line = rng.pick(order.items);
    const amount = rng.chance(0.6) ? order.total_minor : line.unit_price_minor * line.qty;
    add(order.order_id, order.currency, amount, Date.parse(order.placed_at));
  }

  const previous = new Date(dayStart - 86_400_000).toISOString().slice(0, 10);
  for (let i = 0, late = rng.int(1, 3); i < late; i += 1) {
    const product = rng.weighted(PRODUCTS.map((p) => [p, p.popularity] as const));
    add(orderId(previous, rng.int(1, 450)), "USD", priceIn(product, "USD"), dayStart);
  }
  return refunds.sort((a, b) => a.refunded_at.localeCompare(b.refunded_at));
}

/** `low` leaves a handful of SKUs about to run out. `stockout` also empties a promoted SKU. */
export type InventoryScenario = "normal" | "low" | "stockout";

export function parseInventoryScenario(value: string | null): InventoryScenario {
  const normalized = value?.trim().toLowerCase();
  return normalized === "low" || normalized === "stockout" ? normalized : "normal";
}

export function parseFlag(value: string | null): boolean {
  return value?.trim().toLowerCase() === "true";
}

export function generateInventory(params: {
  batchId: string;
  lookbackDays: number;
  scenario: InventoryScenario;
}): InventoryItem[] {
  const rng = createRng(`${params.batchId}:inventory`);
  const order = rng.shuffle(PRODUCTS);
  const promoted = new Set(order.slice(0, 6).map((p) => p.sku));
  const low = new Set(
    order
      .filter((p) => !promoted.has(p.sku))
      .slice(0, 4)
      .map((p) => p.sku),
  );
  const emptied = params.scenario === "stockout" ? (order[0] as Product).sku : undefined;

  return PRODUCTS.map((product): InventoryItem => {
    const isPromoted = promoted.has(product.sku);
    const velocity = Number(
      (product.popularity * 2.2 * (0.7 + 0.6 * rng.next()) * (isPromoted ? 1.8 : 1)).toFixed(2),
    );
    let coverDays = 4 + rng.next() * 26;
    if (params.scenario !== "normal" && low.has(product.sku)) coverDays = 0.6 + rng.next() * 2;
    let onHand = Math.max(1, Math.floor(velocity * coverDays));
    if (product.sku === emptied) onHand = 0;
    return {
      sku: product.sku,
      name: product.name,
      category: product.category,
      on_hand: onHand,
      velocity_per_day: velocity,
      promoted: isPromoted,
      unit_cost_usd_cents: Math.round(product.priceUsdCents * 0.42),
      lead_time_days: leadTimeDays(product.category),
    };
  });
}
