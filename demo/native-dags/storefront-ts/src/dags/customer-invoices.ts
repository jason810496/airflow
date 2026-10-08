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

// storefront_customer_invoices: renders and sends customer invoices once finance has closed the day.
//
// Triggered by finance, never scheduled. It follows `handoff.finance.latest_close` to the closed
// batch and, until finance publishes one, falls back to the storefront's own latest batch.

import path from "node:path";

import { Dag, getClient } from "apache-airflow-ts-sdk";

import { buildInvoices, renderInvoiceHtml, type Invoice } from "../lib/invoice.js";
import {
  LATEST_BATCH_VARIABLE,
  listFiles,
  outboxRoot,
  readJson,
  sha256Of,
  writeJson,
  writeText,
} from "../lib/lake.js";
import { formatUsd, parseFxRates, sum } from "../lib/money.js";
import { renderTable } from "../lib/report.js";
import type { OrdersFile, RefundsFile } from "../lib/types.js";

const FINANCE_CLOSE_VARIABLE = "handoff.finance.latest_close";

const DOC_MD = `
### Storefront customer invoices

Triggered by \`finance_revenue_close\`. Reads the batch to invoice from the Variable
\`handoff.finance.latest_close\`, or from \`handoff.storefront.latest_batch\` when finance has not
published one. Writes one invoice per customer to \`/files/demo/outbox/invoices/<batch>/\`,
then simulates sending them and writes \`outbox_index.json\`.

Orders flagged by the checkout rules are held, not sent.
`;

export const customerInvoices = new Dag("storefront_customer_invoices", {
  description: "Render and send customer invoices for a closed storefront batch",
  startDate: new Date("2026-01-01T00:00:00Z"),
  tags: ["storefront", "typescript"],
  queue: "typescript",
  isPausedUponCreation: false,
  docMd: DOC_MD,
});

interface BatchRef {
  batchDir: string;
  source: "finance" | "storefront";
}

interface RenderedInvoices {
  dir: string;
  invoices: number;
  heldForReview: number;
}

interface RenderedStatements {
  path: string;
  customers: number;
}

async function resolveBatch(): Promise<BatchRef> {
  const client = getClient();
  const closeDir = await client.getVariable(FINANCE_CLOSE_VARIABLE);
  if (closeDir) {
    const close = await readJson<{ storefront_batch_dir?: string }>(
      path.join(closeDir, "close_summary.json"),
    ).catch(() => ({}) as { storefront_batch_dir?: string });
    const batchDir =
      close.storefront_batch_dir ?? (await client.getVariable(LATEST_BATCH_VARIABLE));
    if (batchDir) {
      console.log(`Finance closed ${closeDir}, invoicing ${batchDir}`);
      return { batchDir, source: "finance" };
    }
  }
  const batchDir = await client.getVariable(LATEST_BATCH_VARIABLE);
  if (!batchDir) {
    throw new Error(
      `Neither ${FINANCE_CLOSE_VARIABLE} nor ${LATEST_BATCH_VARIABLE} is set; nothing to invoice`,
    );
  }
  console.log(
    `Variable ${FINANCE_CLOSE_VARIABLE} is not usable yet, falling back to ${LATEST_BATCH_VARIABLE}: ${batchDir}`,
  );
  return { batchDir, source: "storefront" };
}

async function loadBatch(batchDir: string) {
  const [orders, refunds, suspicious] = await Promise.all([
    readJson<OrdersFile>(path.join(batchDir, "orders.json")),
    readJson<RefundsFile>(path.join(batchDir, "refunds.json")),
    readJson<{ orders: { order_id: string }[] }>(path.join(batchDir, "suspicious_orders.json")),
  ]);
  return { orders, refunds, flagged: new Set(suspicious.orders.map((order) => order.order_id)) };
}

function invoiceDir(batchDir: string): string {
  return path.join(outboxRoot(), "invoices", path.basename(batchDir));
}

async function renderInvoices({ batch }: { batch: BatchRef }): Promise<RenderedInvoices> {
  const { orders, refunds, flagged } = await loadBatch(batch.batchDir);
  const rates = parseFxRates(await getClient().getVariable("storefront.fx_rates"));
  const { invoices, skippedFullyRefunded } = buildInvoices({
    batchId: orders.batch_id,
    businessDate: orders.business_date,
    orders: orders.orders,
    refunds: refunds.refunds,
    flaggedOrderIds: flagged,
    rates,
  });

  const dir = invoiceDir(batch.batchDir);
  for (const invoice of invoices) {
    await writeJson(dir, `${invoice.invoice_no}.json`, invoice);
    await writeText(dir, `${invoice.invoice_no}.html`, renderInvoiceHtml(invoice));
  }
  const held = invoices.filter((invoice) => invoice.review_flagged).length;
  console.log(
    `Rendered ${invoices.length} invoices into ${dir}, ${held} held for review, ` +
      `${skippedFullyRefunded} fully refunded orders skipped, ${formatUsd(sum(invoices.map((i) => i.total_usd_cents)))} billed`,
  );
  return { dir, invoices: invoices.length, heldForReview: held };
}

async function renderStatements({ batch }: { batch: BatchRef }): Promise<RenderedStatements> {
  const { orders, refunds } = await loadBatch(batch.batchDir);
  const rates = parseFxRates(await getClient().getVariable("storefront.fx_rates"));
  const { invoices } = buildInvoices({
    batchId: orders.batch_id,
    businessDate: orders.business_date,
    orders: orders.orders,
    refunds: refunds.refunds,
    flaggedOrderIds: new Set(),
    rates,
  });
  const statements = invoices
    .map((invoice) => ({
      customer_id: invoice.customer_id,
      email: invoice.email,
      orders: new Set(invoice.lines.map((line) => line.order_id)).size,
      balance_usd_cents: invoice.total_usd_cents,
    }))
    .sort((a, b) => b.balance_usd_cents - a.balance_usd_cents);

  const dir = path.join(outboxRoot(), "statements", path.basename(batch.batchDir));
  const file = await writeJson(dir, "statements.json", {
    batch_id: orders.batch_id,
    as_of: orders.business_date,
    statements,
  });
  console.log(
    `Statements for ${statements.length} customers\n` +
      renderTable(
        ["customer", "orders", "balance"],
        statements
          .slice(0, 5)
          .map((s) => [s.customer_id, String(s.orders), formatUsd(s.balance_usd_cents)]),
        [1, 2],
      ),
  );
  return { path: file.path, customers: statements.length };
}

async function sendInvoices({
  invoices,
  statements,
}: {
  invoices: RenderedInvoices;
  statements: RenderedStatements;
}): Promise<{ sent: number; held: number; indexPath: string }> {
  const names = (await listFiles(invoices.dir)).filter(
    (name) => name.startsWith("INV-") && name.endsWith(".json"),
  );
  const items = [];
  for (const name of names) {
    const invoice = await readJson<Invoice>(path.join(invoices.dir, name));
    items.push({
      invoice_no: invoice.invoice_no,
      customer_id: invoice.customer_id,
      email: invoice.email,
      status: invoice.review_flagged ? "held" : "sent",
      message_id: sha256Of(`${invoice.invoice_no}:${invoice.email}`).slice(0, 16),
    });
  }
  const sent = items.filter((item) => item.status === "sent");
  const byDomain = new Map<string, number>();
  for (const item of sent) {
    const domain = item.email.split("@")[1] ?? "unknown";
    byDomain.set(domain, (byDomain.get(domain) ?? 0) + 1);
  }
  const file = await writeJson(invoices.dir, "outbox_index.json", {
    batch_id: path.basename(invoices.dir),
    statements_path: statements.path,
    sent: sent.length,
    held: items.length - sent.length,
    items,
  });
  console.log(
    `Sent ${sent.length} invoices (simulated), held ${items.length - sent.length}\n` +
      renderTable(
        ["domain", "sent"],
        [...byDomain.entries()]
          .sort((a, b) => b[1] - a[1])
          .map(([domain, count]) => [domain, String(count)]),
        [1],
      ),
  );
  return { sent: sent.length, held: items.length - sent.length, indexPath: file.path };
}

const batch = customerInvoices.task("resolve_batch", resolveBatch)();

const render = customerInvoices.taskGroup("render");
const invoices = render.task("render_invoices", renderInvoices)({ batch });
const statements = render.task("render_statements", renderStatements)({ batch });

customerInvoices.task("send_invoices", sendInvoices)({ invoices, statements });
