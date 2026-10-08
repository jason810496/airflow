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

// Records the storefront team writes to the lake. Field names are snake_case because the files are
// the data contract that the risk and finance teams read from other languages.

export type Currency = "USD" | "EUR" | "GBP" | "JPY";

export interface LineItem {
  sku: string;
  qty: number;
  /** In the minor unit of the order's currency. */
  unit_price_minor: number;
}

export interface Order {
  order_id: string;
  customer_id: string;
  account_age_days: number;
  email: string;
  card_fingerprint: string;
  billing_country: string;
  shipping_country: string;
  currency: Currency;
  items: LineItem[];
  total_minor: number;
  placed_at: string;
}

export interface Refund {
  refund_id: string;
  order_id: string;
  currency: Currency;
  amount_minor: number;
  reason: string;
  refunded_at: string;
}

export interface InventoryItem {
  sku: string;
  name: string;
  category: string;
  on_hand: number;
  /** Average units sold per day over the export's lookback window. */
  velocity_per_day: number;
  promoted: boolean;
  unit_cost_usd_cents: number;
  lead_time_days: number;
}

interface FeedHeader {
  batch_id: string;
  business_date: string;
  source: string;
}

export interface OrdersFile extends FeedHeader {
  orders: Order[];
}

export interface RefundsFile extends FeedHeader {
  refunds: Refund[];
}

export interface InventoryFile extends FeedHeader {
  lookback_days: number;
  items: InventoryItem[];
}

/** What an export task returns as its XCom: where the feed landed, not the feed itself. */
export interface ExportSummary {
  feed: string;
  path: string;
  records: number;
  source: string;
}

/** What `validate_orders` returns as its XCom. Amounts are whole dollars with cents. */
export interface SalesSummary {
  grossUsd: number;
  refundsUsd: number;
  netUsd: number;
  orderCount: number;
  suspiciousCount: number;
  batchDir: string;
}
