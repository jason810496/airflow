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

// The fraud rules the checkout backend applies, shared so the nightly validation flags exactly what
// checkout would. Change a threshold here and both sides move together.

import type { Order } from "./types.js";

export const CHECKOUT_RULES = {
  /** Billing and shipping countries differ and the order is worth more than this. */
  crossBorder: { minUsdCents: 40_000 },
  /** More than `maxUses` orders on one card fingerprint inside `windowMinutes`. */
  cardVelocity: { maxUses: 4, windowMinutes: 30 },
  /** The account is younger than `maxAgeDays` and the order is worth more than this. */
  newAccount: { maxAgeDays: 2, minUsdCents: 25_000 },
} as const;

export type RuleId = "cross_border_high_value" | "card_velocity" | "new_account_high_value";

export interface RuleHit {
  rule: RuleId;
  detail: string;
}

export interface PricedOrder {
  order: Order;
  amountUsdCents: number;
}

const MINUTE_MS = 60_000;

function dollars(cents: number): string {
  return `$${(cents / 100).toFixed(2)}`;
}

/** Hits per order id; orders that trip no rule are absent. */
export function evaluateRules(priced: readonly PricedOrder[]): Map<string, RuleHit[]> {
  const hits = new Map<string, RuleHit[]>();
  const add = (orderId: string, hit: RuleHit): void => {
    const list = hits.get(orderId) ?? [];
    list.push(hit);
    hits.set(orderId, list);
  };

  const { crossBorder, newAccount, cardVelocity } = CHECKOUT_RULES;
  for (const { order, amountUsdCents } of priced) {
    if (
      order.billing_country !== order.shipping_country &&
      amountUsdCents > crossBorder.minUsdCents
    ) {
      add(order.order_id, {
        rule: "cross_border_high_value",
        detail: `${order.billing_country} billing, ${order.shipping_country} shipping, ${dollars(amountUsdCents)}`,
      });
    }
    if (order.account_age_days < newAccount.maxAgeDays && amountUsdCents > newAccount.minUsdCents) {
      add(order.order_id, {
        rule: "new_account_high_value",
        detail: `account ${order.account_age_days}d old, ${dollars(amountUsdCents)}`,
      });
    }
  }

  const byCard = new Map<string, { id: string; at: number }[]>();
  for (const { order } of priced) {
    const uses = byCard.get(order.card_fingerprint) ?? [];
    uses.push({ id: order.order_id, at: Date.parse(order.placed_at) });
    byCard.set(order.card_fingerprint, uses);
  }
  const windowMs = cardVelocity.windowMinutes * MINUTE_MS;
  for (const [card, uses] of byCard) {
    if (uses.length <= cardVelocity.maxUses) continue;
    uses.sort((a, b) => a.at - b.at);
    const flagged = new Set<string>();
    let left = 0;
    for (let right = 0; right < uses.length; right += 1) {
      while ((uses[right]?.at ?? 0) - (uses[left]?.at ?? 0) > windowMs) left += 1;
      if (right - left + 1 > cardVelocity.maxUses) {
        for (let i = left; i <= right; i += 1) flagged.add(uses[i]?.id ?? "");
      }
    }
    for (const id of flagged) {
      add(id, {
        rule: "card_velocity",
        detail: `card ${card} used more than ${cardVelocity.maxUses} times in ${cardVelocity.windowMinutes} minutes`,
      });
    }
  }
  return hits;
}
