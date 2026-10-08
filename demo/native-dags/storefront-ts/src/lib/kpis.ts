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

// The daily KPI document that `publish_daily_kpis` writes, and how it reads in the task log.

import { formatUsd } from "./money.js";
import type { Batch } from "./lake.js";
import { renderTable } from "./report.js";
import { REORDER_BELOW_DAYS, daysOfCover } from "./restock.js";
import type { InventoryItem, SalesSummary } from "./types.js";

export function buildKpis(input: {
  batch: Batch;
  summary: SalesSummary;
  items: readonly InventoryItem[];
  flagged: boolean | null;
  strategy: string | null;
  triggeredRunId: string | null;
}) {
  const { batch, summary, items, flagged, strategy, triggeredRunId } = input;
  return {
    batch_id: batch.batchId,
    business_date: batch.businessDate,
    orders: summary.orderCount,
    gross_usd: summary.grossUsd,
    refunds_usd: summary.refundsUsd,
    net_usd: summary.netUsd,
    average_order_usd: Number((summary.grossUsd / Math.max(1, summary.orderCount)).toFixed(2)),
    refund_rate: Number((summary.refundsUsd / Math.max(1, summary.grossUsd)).toFixed(4)),
    suspicious_orders: summary.suspiciousCount,
    handoff: {
      next_dag: flagged ? "risk_fraud_screening" : "finance_revenue_close",
      trigger_run_id: triggeredRunId,
    },
    inventory: {
      skus: items.length,
      under_reorder_point: items.filter((item) => daysOfCover(item) < REORDER_BELOW_DAYS).length,
      restock_strategy: strategy,
    },
  };
}

export type DailyKpis = ReturnType<typeof buildKpis>;

export function renderKpis(kpis: DailyKpis): string {
  const dollars = (value: number): string => formatUsd(Math.round(value * 100));
  return (
    `Daily KPIs for ${kpis.business_date}\n` +
    renderTable(
      ["metric", "value"],
      [
        ["orders", String(kpis.orders)],
        ["gross", dollars(kpis.gross_usd)],
        ["refunds", dollars(kpis.refunds_usd)],
        ["net", dollars(kpis.net_usd)],
        ["average order", `$${kpis.average_order_usd.toFixed(2)}`],
        ["refund rate", `${(kpis.refund_rate * 100).toFixed(2)}%`],
        ["suspicious orders", String(kpis.suspicious_orders)],
        [
          "handed off to",
          `${kpis.handoff.next_dag} (${kpis.handoff.trigger_run_id ?? "no run id"})`,
        ],
        ["restock strategy", kpis.inventory.restock_strategy ?? "unknown"],
      ],
      [1],
    )
  );
}
