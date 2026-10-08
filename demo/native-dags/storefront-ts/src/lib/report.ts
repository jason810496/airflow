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

// Plain-text tables and reports for the task logs.

import { formatMinor, formatUsd } from "./money.js";
import {
  REORDER_BELOW_DAYS,
  daysOfCover,
  orderCostCents,
  type PurchaseLine,
  type RestockStrategy,
} from "./restock.js";
import type { OutboxItem, Statement } from "./outbox.js";
import type { Settlement } from "./settlement.js";
import type { Currency, InventoryItem } from "./types.js";

export function renderTable(
  headers: readonly string[],
  rows: readonly (readonly string[])[],
  rightAligned: readonly number[] = [],
): string {
  const widths = headers.map((header, column) =>
    Math.max(header.length, ...rows.map((row) => (row[column] ?? "").length)),
  );
  const format = (cells: readonly string[]): string =>
    cells
      .map((cell, column) =>
        rightAligned.includes(column)
          ? cell.padStart(widths[column] ?? 0)
          : cell.padEnd(widths[column] ?? 0),
      )
      .join("  ")
      .trimEnd();
  const rule = widths.map((width) => "-".repeat(width)).join("  ");
  return [format(headers), rule, ...rows.map(format)].join("\n");
}

export function renderSalesReport(businessDate: string, result: Settlement): string[] {
  const currencyRows = Object.entries(result.byCurrency).map(([currency, totals]) => [
    currency,
    String(totals.orders),
    formatMinor(totals.gross_minor, currency as Currency),
    formatUsd(totals.gross_usd_cents),
  ]);
  const lines = [
    `Sales for ${businessDate}\n${renderTable(["currency", "orders", "gross (local)", "gross (usd)"], currencyRows, [1, 2, 3])}`,
    `gross ${formatUsd(result.grossUsdCents)}, refunds ${formatUsd(result.refundsUsdCents)} ` +
      `(${result.refundCount} refunds, ${result.unmatchedRefundCount} for earlier days), net ${formatUsd(result.netUsdCents)}`,
  ];
  if (result.suspicious.length === 0) return [...lines, "No suspicious orders"];
  const rows = result.suspicious
    .slice(0, 10)
    .map((s) => [s.order_id, s.customer_id, formatUsd(s.amount_usd_cents), s.rules.join(",")]);
  return [
    ...lines,
    `${result.suspicious.length} suspicious orders, top ${rows.length}\n` +
      renderTable(["order", "customer", "amount", "rules"], rows, [2]),
  ];
}

export function renderManifest(
  batchDir: string,
  files: readonly { name: string; bytes: number; sha256: string }[],
): string {
  const rows = files.map((f) => [f.name, String(f.bytes), f.sha256.slice(0, 16)]);
  return `Manifest for ${batchDir}\n${renderTable(["file", "bytes", "sha256"], rows, [1])}`;
}

export function renderStockTable(items: readonly InventoryItem[]): string {
  const rows = items.map((item) => [
    item.sku,
    item.name,
    String(item.on_hand),
    item.velocity_per_day.toFixed(1),
    daysOfCover(item).toFixed(1),
    item.promoted ? "yes" : "",
  ]);
  return renderTable(
    ["sku", "name", "on hand", "per day", "cover (d)", "promoted"],
    rows,
    [2, 3, 4],
  );
}

function purchaseOrderHeadline(strategy: RestockStrategy, lines: readonly PurchaseLine[]): string {
  const byAir = lines.filter((line) => line.ship_mode === "air").length;
  switch (strategy) {
    case "restock_standard":
      return `Standard purchase order, ${lines.length} SKUs under ${REORDER_BELOW_DAYS} days of cover`;
    case "restock_expedite":
      return `Expedited purchase order, ${byAir} SKUs by air, ${lines.length - byAir} by ground`;
    case "pause_campaigns":
      return `Emergency purchase order, ${lines.length} SKUs`;
  }
}

/** One purchase order as it reads in the task log, for any restock strategy. */
export function renderPurchaseOrder(
  strategy: RestockStrategy,
  lines: readonly PurchaseLine[],
): string {
  const rows = lines.map((l) => [
    l.sku,
    String(l.days_of_cover),
    String(l.order_qty),
    l.ship_mode,
    String(l.lead_time_days),
    formatUsd(l.est_cost_usd_cents),
  ]);
  const table = renderTable(
    ["sku", "cover (d)", "order qty", "ship", "lead (d)", "cost"],
    rows,
    [1, 2, 4, 5],
  );
  return `${purchaseOrderHeadline(strategy, lines)}, ${formatUsd(orderCostCents(lines))}\n${table}`;
}

export function renderStatementsReport(statements: readonly Statement[]): string {
  const rows = statements
    .slice(0, 5)
    .map((s) => [s.customer_id, String(s.orders), formatUsd(s.balance_usd_cents)]);
  return (
    `Statements for ${statements.length} customers\n` +
    renderTable(["customer", "orders", "balance"], rows, [1, 2])
  );
}

export function renderSentReport(items: readonly OutboxItem[]): string {
  const sent = items.filter((item) => item.status === "sent");
  const byDomain = new Map<string, number>();
  for (const item of sent) {
    const domain = item.email.split("@")[1] ?? "unknown";
    byDomain.set(domain, (byDomain.get(domain) ?? 0) + 1);
  }
  const rows = [...byDomain.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([domain, count]) => [domain, String(count)]);
  return (
    `Sent ${sent.length} invoices (simulated), held ${items.length - sent.length}\n` +
    renderTable(["domain", "sent"], rows, [1])
  );
}
