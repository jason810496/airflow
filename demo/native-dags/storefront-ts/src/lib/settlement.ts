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

// Turns a day's orders and refunds into the numbers finance and risk read.

import { evaluateRules, type RuleId } from "./checkout-rules.js";
import { PRODUCTS_BY_SKU } from "./catalog.js";
import { toUsdCents, sum, type FxRates } from "./money.js";
import type { Currency, Order, Refund } from "./types.js";

export interface SuspiciousOrder {
  order_id: string;
  customer_id: string;
  card_fingerprint: string;
  billing_country: string;
  shipping_country: string;
  account_age_days: number;
  placed_at: string;
  amount_usd_cents: number;
  rules: RuleId[];
  details: string[];
}

export interface CurrencyTotals {
  orders: number;
  gross_minor: number;
  gross_usd_cents: number;
}

export interface Settlement {
  grossUsdCents: number;
  refundsUsdCents: number;
  netUsdCents: number;
  orderCount: number;
  refundCount: number;
  /** Refunds for orders that are not in this batch, typically from the day before. */
  unmatchedRefundCount: number;
  byCurrency: Partial<Record<Currency, CurrencyTotals>>;
  byCategory: Record<string, number>;
  suspicious: SuspiciousOrder[];
}

export function settleBatch(input: {
  orders: readonly Order[];
  refunds: readonly Refund[];
  rates: FxRates;
}): Settlement {
  const { orders, refunds, rates } = input;
  const priced = orders.map((order) => ({
    order,
    amountUsdCents: toUsdCents(order.total_minor, order.currency, rates),
  }));

  const byCurrency: Partial<Record<Currency, CurrencyTotals>> = {};
  const byCategory: Record<string, number> = {};
  for (const { order, amountUsdCents } of priced) {
    const totals = byCurrency[order.currency] ?? { orders: 0, gross_minor: 0, gross_usd_cents: 0 };
    totals.orders += 1;
    totals.gross_minor += order.total_minor;
    totals.gross_usd_cents += amountUsdCents;
    byCurrency[order.currency] = totals;
    for (const item of order.items) {
      const category = PRODUCTS_BY_SKU.get(item.sku)?.category ?? "other";
      byCategory[category] =
        (byCategory[category] ?? 0) +
        toUsdCents(item.qty * item.unit_price_minor, order.currency, rates);
    }
  }

  const known = new Set(orders.map((order) => order.order_id));
  const grossUsdCents = sum(priced.map((p) => p.amountUsdCents));
  const refundsUsdCents = sum(refunds.map((r) => toUsdCents(r.amount_minor, r.currency, rates)));

  const hits = evaluateRules(priced);
  const suspicious = priced
    .filter(({ order }) => hits.has(order.order_id))
    .map(({ order, amountUsdCents }): SuspiciousOrder => {
      const orderHits = hits.get(order.order_id) ?? [];
      return {
        order_id: order.order_id,
        customer_id: order.customer_id,
        card_fingerprint: order.card_fingerprint,
        billing_country: order.billing_country,
        shipping_country: order.shipping_country,
        account_age_days: order.account_age_days,
        placed_at: order.placed_at,
        amount_usd_cents: amountUsdCents,
        rules: orderHits.map((hit) => hit.rule),
        details: orderHits.map((hit) => hit.detail),
      };
    })
    .sort((a, b) => b.amount_usd_cents - a.amount_usd_cents);

  return {
    grossUsdCents,
    refundsUsdCents,
    netUsdCents: grossUsdCents - refundsUsdCents,
    orderCount: orders.length,
    refundCount: refunds.length,
    unmatchedRefundCount: refunds.filter((r) => !known.has(r.order_id)).length,
    byCurrency,
    byCategory,
    suspicious,
  };
}
