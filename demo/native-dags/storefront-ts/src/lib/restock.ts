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

// Restock policy: which strategy a day's inventory calls for, and the purchase order it produces.

import type { InventoryItem } from "./types.js";

export type RestockStrategy = "restock_standard" | "restock_expedite" | "pause_campaigns";

/** Any SKU with less cover than this needs expedited freight. */
export const EXPEDITE_BELOW_DAYS = 3;
/** Any SKU with less cover than this is reordered. */
export const REORDER_BELOW_DAYS = 14;
const TARGET_COVER_DAYS = 21;
const EXPEDITE_COVER_DAYS = 14;

export function daysOfCover(item: InventoryItem): number {
  return item.velocity_per_day > 0
    ? item.on_hand / item.velocity_per_day
    : Number.POSITIVE_INFINITY;
}

export function stockedOutPromoted(items: readonly InventoryItem[]): InventoryItem[] {
  return items.filter((item) => item.promoted && item.on_hand === 0);
}

export function critical(items: readonly InventoryItem[]): InventoryItem[] {
  return items.filter((item) => daysOfCover(item) < EXPEDITE_BELOW_DAYS);
}

export function chooseStrategy(items: readonly InventoryItem[]): RestockStrategy {
  if (stockedOutPromoted(items).length > 0) return "pause_campaigns";
  if (critical(items).length > 0) return "restock_expedite";
  return "restock_standard";
}

export interface PurchaseLine {
  sku: string;
  name: string;
  on_hand: number;
  days_of_cover: number;
  order_qty: number;
  ship_mode: "ground" | "air";
  lead_time_days: number;
  est_cost_usd_cents: number;
}

export function buildPurchaseOrder(
  items: readonly InventoryItem[],
  expedite: boolean,
): PurchaseLine[] {
  return items
    .filter((item) => daysOfCover(item) < REORDER_BELOW_DAYS)
    .map((item): PurchaseLine => {
      const cover = daysOfCover(item);
      const urgent = expedite && cover < EXPEDITE_BELOW_DAYS;
      const targetDays = urgent ? EXPEDITE_COVER_DAYS : TARGET_COVER_DAYS;
      const qty = Math.max(1, Math.ceil(item.velocity_per_day * targetDays) - item.on_hand);
      return {
        sku: item.sku,
        name: item.name,
        on_hand: item.on_hand,
        days_of_cover: Number(cover.toFixed(1)),
        order_qty: qty,
        ship_mode: urgent ? "air" : "ground",
        lead_time_days: urgent
          ? Math.max(3, Math.ceil(item.lead_time_days / 5))
          : item.lead_time_days,
        est_cost_usd_cents: qty * item.unit_cost_usd_cents * (urgent ? 1.15 : 1),
      };
    })
    .map((line) => ({ ...line, est_cost_usd_cents: Math.round(line.est_cost_usd_cents) }))
    .sort((a, b) => a.days_of_cover - b.days_of_cover);
}

export function orderCostCents(lines: readonly PurchaseLine[]): number {
  return lines.reduce((total, line) => total + line.est_cost_usd_cents, 0);
}

export function campaignPauseDoc(batchId: string, outOfStock: readonly InventoryItem[]) {
  return {
    batch_id: batchId,
    paused: outOfStock.map((item) => ({
      sku: item.sku,
      name: item.name,
      reason: "promoted SKU is out of stock",
      resume_when: `on hand covers ${EXPEDITE_BELOW_DAYS} days of sales`,
    })),
  };
}

export function describeChoice(items: readonly InventoryItem[], strategy: RestockStrategy): string {
  return (
    `${items.length} SKUs: ${stockedOutPromoted(items).length} promoted out of stock, ` +
    `${critical(items).length} under ${EXPEDITE_BELOW_DAYS} days of cover, choosing ${strategy}`
  );
}
