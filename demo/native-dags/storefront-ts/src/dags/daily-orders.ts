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

import { Dag, getClient, getContext, triggerDagRun, type TaskRef } from "apache-airflow-ts-sdk";

import { buildKpis, renderKpis } from "../lib/kpis.js";
import * as lake from "../lib/lake.js";
import { parseFxRates } from "../lib/money.js";
import * as report from "../lib/report.js";
import * as restock from "../lib/restock.js";
import * as sales from "../lib/settlement.js";
import * as synthetic from "../lib/synthetic.js";
import { loadInventory, savePurchaseOrder, startExport, writeOps } from "../lib/task-io.js";
import type { ExportSummary, OrdersFile, RefundsFile, SalesSummary } from "../lib/types.js";

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

Knobs: Variables \`storefront.inject_fraud\` (\`true\`), \`storefront.inventory_scenario\`
(\`normal\`, \`low\` or \`stockout\`) and \`storefront.fx_rates\` (JSON, USD per unit).
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

// The graph. The task handlers it refers to are defined below.
const ingest = dailyOrders.taskGroup("ingest");
const retry = { retries: 2, retryDelay: 20 };
const ordersExport = ingest.task("export_orders", exportOrders, retry)({ targetOrders: 500 });
const refundsExport = ingest.task(
  "export_refunds",
  exportRefunds,
  retry,
)({ targetOrders: 500, refundRate: 0.04 });
const inventoryExport = ingest.task(
  "export_inventory",
  exportInventory,
  retry,
)({ lookbackDays: 7 });

const validated = dailyOrders.task(
  "validate_orders",
  validateOrders,
)({ orders: ordersExport, refunds: refundsExport });

// Order-only: it reads what the tasks above wrote, not what they returned.
const manifest = dailyOrders.task("write_lake_manifest", writeLakeManifest)();
manifest.after(ingest, validated);

const handoff = (dagId: string, taskId: string, note: string) =>
  dailyOrders.task(
    triggerDagRun({
      dagId,
      conf: { requested_by: "storefront", contract: lake.LATEST_BATCH_VARIABLE },
      waitForCompletion: false,
      note,
    }),
    { taskId },
  )();
const requestRiskReview = handoff(
  "risk_fraud_screening",
  "request_risk_review",
  "Suspicious orders flagged by storefront_daily_orders",
);
const closeRevenue = handoff(
  "finance_revenue_close",
  "close_revenue",
  "Clean batch from storefront_daily_orders",
);

// The handoff Variable is set before the decision, so both triggers follow it.
const gate = dailyOrders.if(
  hasSuspiciousOrders,
  { summary: validated },
  { taskId: "has_suspicious_orders" },
);
gate.then(requestRiskReview).else(closeRevenue);
gate.after(manifest);

const onInventory = (taskId: string, handler: typeof restockStandard) =>
  dailyOrders.task(taskId, handler)({ inventory: inventoryExport });
const cases: Record<restock.RestockStrategy, TaskRef> = {
  restock_standard: onInventory("restock_standard", restockStandard),
  restock_expedite: onInventory("restock_expedite", restockExpedite),
  pause_campaigns: onInventory("pause_campaigns", pauseCampaigns),
};
const strategy = dailyOrders.switch(
  restockStrategy,
  { inventory: inventoryExport },
  { taskId: "restock_strategy" },
);
Object.values(cases).forEach((branch) => strategy.case(branch));

// Skipped branches count as done, so this runs once whichever side of each decision was taken.
const published = dailyOrders.task("publish_daily_kpis", publishDailyKpis, {
  triggerRule: "none_failed_min_one_success",
})({ summary: validated, inventory: inventoryExport });
published.after(requestRiskReview, closeRevenue, ...Object.values(cases));

async function exportOrders({ targetOrders }: { targetOrders: number }): Promise<ExportSummary> {
  const job = await startExport("orders");
  const injectFraud = synthetic.parseFlag(await getClient().getVariable("storefront.inject_fraud"));
  const orders = synthetic.generateOrders({ ...job.batch, targetOrders, injectFraud });
  return job.write({ orders }, orders.length, `inject_fraud=${injectFraud}`);
}

async function exportRefunds(args: {
  targetOrders: number;
  refundRate: number;
}): Promise<ExportSummary> {
  const job = await startExport("refunds");
  const refunds = synthetic.generateRefunds({ ...job.batch, ...args });
  return job.write({ refunds }, refunds.length, `refund_rate=${args.refundRate}`);
}

async function exportInventory({ lookbackDays }: { lookbackDays: number }): Promise<ExportSummary> {
  const job = await startExport("inventory");
  const knob = await getClient().getVariable("storefront.inventory_scenario");
  const scenario = synthetic.parseInventoryScenario(knob);
  const items = synthetic.generateInventory({ batchId: job.batch.batchId, lookbackDays, scenario });
  return job.write({ lookback_days: lookbackDays, items }, items.length, `scenario=${scenario}`);
}

async function validateOrders(feeds: {
  orders: ExportSummary;
  refunds: ExportSummary;
}): Promise<SalesSummary> {
  const batch = lake.currentBatch();
  const rates = parseFxRates(await getClient().getVariable("storefront.fx_rates"));
  const [orders, refunds] = await Promise.all([
    lake.readJson<OrdersFile>(feeds.orders.path),
    lake.readJson<RefundsFile>(feeds.refunds.path),
  ]);
  const result = sales.settleBatch({ orders: orders.orders, refunds: refunds.refunds, rates });

  await lake.writeJson(
    batch.dir,
    "suspicious_orders.json",
    sales.suspiciousOrdersDoc(batch, result),
  );
  await lake.writeJson(
    batch.dir,
    "sales_summary.json",
    sales.salesSummaryDoc(batch, rates, result),
  );
  report.renderSalesReport(batch.businessDate, result).forEach((text) => console.log(text));
  return sales.toSalesSummary(result, batch.dir);
}

async function writeLakeManifest(): Promise<{ batchDir: string; files: number }> {
  const batch = lake.currentBatch();
  const files = await lake.writeManifest(batch, getContext().dagId);
  await getClient().setVariable(
    lake.LATEST_BATCH_VARIABLE,
    batch.dir,
    "Latest storefront batch directory in the lake. Written by storefront_daily_orders.",
  );
  console.log(
    `${report.renderManifest(batch.dir, files)}\nSet Variable ${lake.LATEST_BATCH_VARIABLE}`,
  );
  return { batchDir: batch.dir, files: files.length };
}

async function hasSuspiciousOrders({ summary }: { summary: SalesSummary }): Promise<boolean> {
  console.log(
    `${summary.suspiciousCount} suspicious orders, routing to ${summary.suspiciousCount > 0 ? "risk" : "finance"}`,
  );
  return summary.suspiciousCount > 0;
}

async function restockStrategy({ inventory }: { inventory: ExportSummary }): Promise<TaskRef> {
  const items = await loadInventory(inventory);
  const choice = restock.chooseStrategy(items);
  console.log(restock.describeChoice(items, choice));
  return cases[choice];
}

async function restockStandard({ inventory }: { inventory: ExportSummary }) {
  const lines = restock.buildPurchaseOrder(await loadInventory(inventory), false);
  return savePurchaseOrder("restock_standard", lines);
}

async function restockExpedite({ inventory }: { inventory: ExportSummary }) {
  const lines = restock.buildPurchaseOrder(await loadInventory(inventory), true);
  return savePurchaseOrder("restock_expedite", lines);
}

async function pauseCampaigns({ inventory }: { inventory: ExportSummary }) {
  const items = await loadInventory(inventory);
  const outOfStock = restock.stockedOutPromoted(items);
  await writeOps(
    "campaign_pause.json",
    restock.campaignPauseDoc(lake.currentBatch().batchId, outOfStock),
  );
  console.log(
    `Paused campaigns for ${outOfStock.length} SKUs\n${report.renderStockTable(outOfStock)}`,
  );
  return savePurchaseOrder("pause_campaigns", restock.buildPurchaseOrder(items, true));
}

async function publishDailyKpis(args: { summary: SalesSummary; inventory: ExportSummary }) {
  const xcom = <T>(taskId: string, key = "return_value") => getClient().getXCom<T>({ key, taskId });
  const flagged = await xcom<boolean>("has_suspicious_orders");
  const kpis = buildKpis({
    batch: lake.currentBatch(),
    summary: args.summary,
    items: await loadInventory(args.inventory),
    flagged,
    strategy: await xcom<string>("restock_strategy"),
    triggeredRunId: await xcom<string>(
      flagged ? "request_risk_review" : "close_revenue",
      "trigger_run_id",
    ),
  });
  console.log(renderKpis(kpis));
  return { path: await writeOps("kpis.json", kpis) };
}
