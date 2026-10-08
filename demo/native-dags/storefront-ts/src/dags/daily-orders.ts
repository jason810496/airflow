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

// storefront_daily_orders: the storefront team's nightly pipeline.
//
// Three exports land in the lake, the orders are validated against the checkout fraud rules, and the
// batch is announced through the `handoff.storefront.latest_batch` Variable. A suspicious day is
// handed to risk, a clean one to finance. Inventory picks a restock strategy on the side, and a final
// task publishes the day's KPIs once whichever branch ran has finished.

import path from "node:path";

import { Dag, getClient, getContext, triggerDagRun, type TaskRef } from "apache-airflow-ts-sdk";

import { CHECKOUT_RULES } from "../lib/checkout-rules.js";
import {
  LATEST_BATCH_VARIABLE,
  batchFor,
  fileDigest,
  listFiles,
  readJson,
  writeJson,
  type Batch,
} from "../lib/lake.js";
import { formatMinor, formatUsd, parseFxRates, toDollars } from "../lib/money.js";
import { renderTable } from "../lib/report.js";
import {
  EXPEDITE_BELOW_DAYS,
  REORDER_BELOW_DAYS,
  buildPurchaseOrder,
  chooseStrategy,
  critical,
  daysOfCover,
  stockedOutPromoted,
  type RestockStrategy,
} from "../lib/restock.js";
import { settleBatch } from "../lib/settlement.js";
import {
  generateInventory,
  generateOrders,
  generateRefunds,
  parseFlag,
  parseLowStock,
} from "../lib/synthetic.js";
import type {
  ExportSummary,
  InventoryFile,
  InventoryItem,
  OrdersFile,
  RefundsFile,
  SalesSummary,
} from "../lib/types.js";

const ORDERS_PER_DAY = 500;
const REFUND_RATE = 0.04;
const LOOKBACK_DAYS = 7;
const DEFAULT_SOURCE_HOST = "storefront.internal.acme";

const DOC_MD = `
### Storefront daily orders

Exports the day's orders, refunds and inventory to the lake, validates the orders with the
same rules the checkout backend uses, and hands the batch to the next team.

* \`ingest\`: three parallel exports, each writing one file under \`/files/demo/lake/storefront/<batch>/\`.
* \`validate_orders\`: converts to USD, matches refunds, flags suspicious orders.
* \`write_lake_manifest\`: lists every file with its sha256 and sets the Variable
  \`handoff.storefront.latest_batch\` to the batch directory.
* \`has_suspicious_orders\`: triggers \`risk_fraud_screening\` when anything was flagged,
  otherwise \`finance_revenue_close\`.
* \`restock_strategy\`: standard restock, expedited restock, or pause campaigns.
* \`publish_daily_kpis\`: joins both decisions and writes \`ops/kpis.json\`.

Knobs: Variables \`storefront.inject_fraud\` (\`true\`), \`storefront.low_stock\` (\`true\` or \`stockout\`),
\`storefront.fx_rates\` (JSON, USD per unit).
`;

export const dailyOrders = new Dag("storefront_daily_orders", {
  description: "Export, validate and hand off the storefront's daily orders",
  schedule: "@daily",
  startDate: new Date("2026-01-01T00:00:00Z"),
  catchup: false,
  maxActiveRuns: 1,
  tags: ["storefront", "typescript"],
  queue: "typescript",
  isPausedUponCreation: false,
  docMd: DOC_MD,
});

async function sourceHost(): Promise<string> {
  const connection = await getClient().getConnection("storefront_api");
  if (!connection?.host) {
    console.log(`Connection storefront_api has no host, using ${DEFAULT_SOURCE_HOST}`);
    return DEFAULT_SOURCE_HOST;
  }
  return connection.host;
}

async function startExport(feed: string): Promise<{ batch: Batch; source: string }> {
  const batch = batchFor(getContext().runId);
  const source = await sourceHost();
  console.log(`Exporting ${feed} for ${batch.businessDate} from ${source}, batch ${batch.batchId}`);
  return { batch, source };
}

function header(batch: Batch, source: string) {
  return { batch_id: batch.batchId, business_date: batch.businessDate, source };
}

async function exportOrders({ targetOrders }: { targetOrders: number }): Promise<ExportSummary> {
  const { batch, source } = await startExport("orders");
  const injectFraud = parseFlag(await getClient().getVariable("storefront.inject_fraud"));
  const orders = generateOrders({ ...batch, targetOrders, injectFraud });
  const file = await writeJson(batch.dir, "orders.json", { ...header(batch, source), orders });
  console.log(
    `Wrote ${orders.length} orders (${file.bytes} bytes) to ${file.path}, inject_fraud=${injectFraud}`,
  );
  return { feed: "orders", path: file.path, records: orders.length, source };
}

async function exportRefunds(args: {
  targetOrders: number;
  refundRate: number;
}): Promise<ExportSummary> {
  const { batch, source } = await startExport("refunds");
  const refunds = generateRefunds({ ...batch, ...args });
  const file = await writeJson(batch.dir, "refunds.json", { ...header(batch, source), refunds });
  console.log(`Wrote ${refunds.length} refunds (${file.bytes} bytes) to ${file.path}`);
  return { feed: "refunds", path: file.path, records: refunds.length, source };
}

async function exportInventory({ lookbackDays }: { lookbackDays: number }): Promise<ExportSummary> {
  const { batch, source } = await startExport("inventory");
  const lowStock = parseLowStock(await getClient().getVariable("storefront.low_stock"));
  const items = generateInventory({ batchId: batch.batchId, lookbackDays, lowStock });
  const file = await writeJson(batch.dir, "inventory.json", {
    ...header(batch, source),
    lookback_days: lookbackDays,
    items,
  });
  console.log(
    `Wrote ${items.length} SKUs (${file.bytes} bytes) to ${file.path}, low_stock=${lowStock}`,
  );
  return { feed: "inventory", path: file.path, records: items.length, source };
}

async function validateOrders({
  orders,
  refunds,
}: {
  orders: ExportSummary;
  refunds: ExportSummary;
}): Promise<SalesSummary> {
  const batch = batchFor(getContext().runId);
  const rates = parseFxRates(await getClient().getVariable("storefront.fx_rates"));
  const [ordersFile, refundsFile] = await Promise.all([
    readJson<OrdersFile>(orders.path),
    readJson<RefundsFile>(refunds.path),
  ]);
  const result = settleBatch({ orders: ordersFile.orders, refunds: refundsFile.refunds, rates });

  await writeJson(batch.dir, "suspicious_orders.json", {
    batch_id: batch.batchId,
    business_date: batch.businessDate,
    rules: CHECKOUT_RULES,
    count: result.suspicious.length,
    orders: result.suspicious,
  });
  await writeJson(batch.dir, "sales_summary.json", {
    batch_id: batch.batchId,
    business_date: batch.businessDate,
    fx_rates: rates,
    gross_usd_cents: result.grossUsdCents,
    refunds_usd_cents: result.refundsUsdCents,
    net_usd_cents: result.netUsdCents,
    order_count: result.orderCount,
    refund_count: result.refundCount,
    unmatched_refund_count: result.unmatchedRefundCount,
    suspicious_count: result.suspicious.length,
    by_currency: result.byCurrency,
    by_category_usd_cents: result.byCategory,
  });

  const currencyRows = Object.entries(result.byCurrency).map(([currency, totals]) => [
    currency,
    String(totals.orders),
    formatMinor(totals.gross_minor, currency as keyof typeof result.byCurrency),
    formatUsd(totals.gross_usd_cents),
  ]);
  console.log(
    `Sales for ${batch.businessDate}\n${renderTable(["currency", "orders", "gross (local)", "gross (usd)"], currencyRows, [1, 2, 3])}`,
  );
  console.log(
    `gross ${formatUsd(result.grossUsdCents)}, refunds ${formatUsd(result.refundsUsdCents)} ` +
      `(${result.refundCount} refunds, ${result.unmatchedRefundCount} for earlier days), net ${formatUsd(result.netUsdCents)}`,
  );
  if (result.suspicious.length > 0) {
    const rows = result.suspicious
      .slice(0, 10)
      .map((s) => [s.order_id, s.customer_id, formatUsd(s.amount_usd_cents), s.rules.join(",")]);
    console.log(
      `${result.suspicious.length} suspicious orders, top ${rows.length}\n` +
        renderTable(["order", "customer", "amount", "rules"], rows, [2]),
    );
  } else {
    console.log("No suspicious orders");
  }

  return {
    grossUsd: toDollars(result.grossUsdCents),
    refundsUsd: toDollars(result.refundsUsdCents),
    netUsd: toDollars(result.netUsdCents),
    orderCount: result.orderCount,
    suspiciousCount: result.suspicious.length,
    batchDir: batch.dir,
  };
}

async function writeLakeManifest(): Promise<{ batchDir: string; files: number }> {
  const batch = batchFor(getContext().runId);
  const names = (await listFiles(batch.dir)).filter((name) => name !== "_manifest.json");
  const files = await Promise.all(
    names.map(async (name) => {
      const { bytes, sha256 } = await fileDigest(path.join(batch.dir, name));
      return { name, bytes, sha256 };
    }),
  );
  await writeJson(batch.dir, "_manifest.json", {
    team: "storefront",
    batch_id: batch.batchId,
    business_date: batch.businessDate,
    produced_by: getContext().dagId,
    files,
  });
  await getClient().setVariable(
    LATEST_BATCH_VARIABLE,
    batch.dir,
    "Latest storefront batch directory in the lake. Written by storefront_daily_orders.",
  );
  console.log(
    `Manifest for ${batch.dir}\n` +
      renderTable(
        ["file", "bytes", "sha256"],
        files.map((f) => [f.name, String(f.bytes), f.sha256.slice(0, 16)]),
        [1],
      ),
  );
  console.log(`Set Variable ${LATEST_BATCH_VARIABLE}`);
  return { batchDir: batch.dir, files: files.length };
}

async function hasSuspiciousOrders({ summary }: { summary: SalesSummary }): Promise<boolean> {
  console.log(
    `${summary.suspiciousCount} suspicious orders, routing to ${summary.suspiciousCount > 0 ? "risk" : "finance"}`,
  );
  return summary.suspiciousCount > 0;
}

async function loadInventory(summary: ExportSummary): Promise<InventoryItem[]> {
  return (await readJson<InventoryFile>(summary.path)).items;
}

function stockTable(items: readonly InventoryItem[]): string {
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

export interface RestockResult {
  strategy: RestockStrategy;
  lines: number;
  estCostUsd: number;
  path: string;
}

async function restockStrategy({ inventory }: { inventory: ExportSummary }): Promise<TaskRef> {
  const items = await loadInventory(inventory);
  const strategy = chooseStrategy(items);
  console.log(
    `${items.length} SKUs: ${stockedOutPromoted(items).length} promoted out of stock, ` +
      `${critical(items).length} under ${EXPEDITE_BELOW_DAYS} days of cover, choosing ${strategy}`,
  );
  return cases[strategy];
}

async function restockStandard({
  inventory,
}: {
  inventory: ExportSummary;
}): Promise<RestockResult> {
  const items = await loadInventory(inventory);
  const batch = batchFor(getContext().runId);
  const lines = buildPurchaseOrder(items, false);
  const costCents = lines.reduce((total, line) => total + line.est_cost_usd_cents, 0);
  const file = await writeJson(path.join(batch.dir, "ops"), "purchase_order.json", {
    batch_id: batch.batchId,
    strategy: "standard",
    lines,
    est_cost_usd_cents: costCents,
  });
  console.log(
    `Standard purchase order, ${lines.length} SKUs under ${REORDER_BELOW_DAYS} days of cover, ${formatUsd(costCents)}\n` +
      renderTable(
        ["sku", "cover (d)", "order qty", "ship", "lead (d)", "cost"],
        lines.map((l) => [
          l.sku,
          String(l.days_of_cover),
          String(l.order_qty),
          l.ship_mode,
          String(l.lead_time_days),
          formatUsd(l.est_cost_usd_cents),
        ]),
        [1, 2, 4, 5],
      ),
  );
  return {
    strategy: "restock_standard",
    lines: lines.length,
    estCostUsd: toDollars(costCents),
    path: file.path,
  };
}

async function restockExpedite({
  inventory,
}: {
  inventory: ExportSummary;
}): Promise<RestockResult> {
  const items = await loadInventory(inventory);
  const batch = batchFor(getContext().runId);
  const lines = buildPurchaseOrder(items, true);
  const costCents = lines.reduce((total, line) => total + line.est_cost_usd_cents, 0);
  const urgent = lines.filter((line) => line.ship_mode === "air");
  const file = await writeJson(path.join(batch.dir, "ops"), "purchase_order.json", {
    batch_id: batch.batchId,
    strategy: "expedite",
    lines,
    est_cost_usd_cents: costCents,
  });
  console.log(
    `Expedited purchase order, ${urgent.length} SKUs by air, ${lines.length - urgent.length} by ground, ${formatUsd(costCents)}\n` +
      renderTable(
        ["sku", "cover (d)", "order qty", "ship", "lead (d)", "cost"],
        lines.map((l) => [
          l.sku,
          String(l.days_of_cover),
          String(l.order_qty),
          l.ship_mode,
          String(l.lead_time_days),
          formatUsd(l.est_cost_usd_cents),
        ]),
        [1, 2, 4, 5],
      ),
  );
  return {
    strategy: "restock_expedite",
    lines: lines.length,
    estCostUsd: toDollars(costCents),
    path: file.path,
  };
}

async function pauseCampaigns({ inventory }: { inventory: ExportSummary }): Promise<RestockResult> {
  const items = await loadInventory(inventory);
  const batch = batchFor(getContext().runId);
  const outOfStock = stockedOutPromoted(items);
  const ops = path.join(batch.dir, "ops");
  await writeJson(ops, "campaign_pause.json", {
    batch_id: batch.batchId,
    paused: outOfStock.map((item) => ({
      sku: item.sku,
      name: item.name,
      reason: "promoted SKU is out of stock",
      resume_when: `on hand covers ${EXPEDITE_BELOW_DAYS} days of sales`,
    })),
  });
  const lines = buildPurchaseOrder(items, true);
  const costCents = lines.reduce((total, line) => total + line.est_cost_usd_cents, 0);
  const file = await writeJson(ops, "purchase_order.json", {
    batch_id: batch.batchId,
    strategy: "pause_campaigns",
    lines,
    est_cost_usd_cents: costCents,
  });
  console.log(
    `Paused campaigns for ${outOfStock.length} out-of-stock promoted SKUs, emergency order ${formatUsd(costCents)}\n` +
      stockTable(outOfStock),
  );
  return {
    strategy: "pause_campaigns",
    lines: lines.length,
    estCostUsd: toDollars(costCents),
    path: file.path,
  };
}

async function publishDailyKpis({
  summary,
  inventory,
}: {
  summary: SalesSummary;
  inventory: ExportSummary;
}): Promise<{ path: string }> {
  const client = getClient();
  const batch = batchFor(getContext().runId);
  const flagged = await client.getXCom<boolean>({
    key: "return_value",
    taskId: "has_suspicious_orders",
  });
  const strategy = await client.getXCom<string>({
    key: "return_value",
    taskId: "restock_strategy",
  });
  const triggered = flagged ? "request_risk_review" : "close_revenue";
  const triggeredRunId = await client.getXCom<string>({ key: "trigger_run_id", taskId: triggered });
  const items = await loadInventory(inventory);

  const kpis = {
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
  const file = await writeJson(path.join(batch.dir, "ops"), "kpis.json", kpis);
  console.log(
    `Daily KPIs for ${batch.businessDate}\n` +
      renderTable(
        ["metric", "value"],
        [
          ["orders", String(kpis.orders)],
          ["gross", formatUsd(Math.round(kpis.gross_usd * 100))],
          ["refunds", formatUsd(Math.round(kpis.refunds_usd * 100))],
          ["net", formatUsd(Math.round(kpis.net_usd * 100))],
          ["average order", `$${kpis.average_order_usd.toFixed(2)}`],
          ["refund rate", `${(kpis.refund_rate * 100).toFixed(2)}%`],
          ["suspicious orders", String(kpis.suspicious_orders)],
          ["handed off to", `${kpis.handoff.next_dag} (${triggeredRunId ?? "no run id"})`],
          ["restock strategy", strategy ?? "unknown"],
        ],
        [1],
      ),
  );
  return { path: file.path };
}

const ingest = dailyOrders.taskGroup("ingest");
const exportRetries = { retries: 2, retryDelay: 20 };
const ordersExport = ingest.task(
  "export_orders",
  exportOrders,
  exportRetries,
)({
  targetOrders: ORDERS_PER_DAY,
});
const refundsExport = ingest.task(
  "export_refunds",
  exportRefunds,
  exportRetries,
)({
  targetOrders: ORDERS_PER_DAY,
  refundRate: REFUND_RATE,
});
const inventoryExport = ingest.task(
  "export_inventory",
  exportInventory,
  exportRetries,
)({
  lookbackDays: LOOKBACK_DAYS,
});

const validated = dailyOrders.task(
  "validate_orders",
  validateOrders,
)({
  orders: ordersExport,
  refunds: refundsExport,
});

// Order-only: it reads what the tasks above wrote, not what they returned.
const manifest = dailyOrders.task("write_lake_manifest", writeLakeManifest)();
manifest.after(ingest, validated);

const handoffConf = { requested_by: "storefront", contract: LATEST_BATCH_VARIABLE };
const requestRiskReview = dailyOrders.task(
  triggerDagRun({
    dagId: "risk_fraud_screening",
    conf: handoffConf,
    waitForCompletion: false,
    note: "Suspicious orders flagged by storefront_daily_orders",
  }),
  { taskId: "request_risk_review" },
)();
const closeRevenue = dailyOrders.task(
  triggerDagRun({
    dagId: "finance_revenue_close",
    conf: handoffConf,
    waitForCompletion: false,
    note: "Clean batch from storefront_daily_orders",
  }),
  { taskId: "close_revenue" },
)();

// The handoff Variable is set before the decision, so both triggers follow it.
const gate = dailyOrders.if(
  hasSuspiciousOrders,
  { summary: validated },
  { taskId: "has_suspicious_orders" },
);
gate.then(requestRiskReview).else(closeRevenue);
gate.after(manifest);

const restockStandardRef = dailyOrders.task(
  "restock_standard",
  restockStandard,
)({ inventory: inventoryExport });
const restockExpediteRef = dailyOrders.task(
  "restock_expedite",
  restockExpedite,
)({ inventory: inventoryExport });
const pauseCampaignsRef = dailyOrders.task(
  "pause_campaigns",
  pauseCampaigns,
)({ inventory: inventoryExport });
const cases: Record<RestockStrategy, TaskRef> = {
  restock_standard: restockStandardRef,
  restock_expedite: restockExpediteRef,
  pause_campaigns: pauseCampaignsRef,
};
dailyOrders
  .switch(restockStrategy, { inventory: inventoryExport }, { taskId: "restock_strategy" })
  .case(restockStandardRef)
  .case(restockExpediteRef)
  .case(pauseCampaignsRef);

// Skipped branches count as done, so this runs once whichever side of each decision was taken.
const published = dailyOrders.task("publish_daily_kpis", publishDailyKpis, {
  triggerRule: "none_failed_min_one_success",
})({ summary: validated, inventory: inventoryExport });
published.after(
  requestRiskReview,
  closeRevenue,
  restockStandardRef,
  restockExpediteRef,
  pauseCampaignsRef,
);
