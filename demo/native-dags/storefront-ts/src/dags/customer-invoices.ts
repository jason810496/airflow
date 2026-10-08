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

import { renderInvoiceHtml } from "../lib/invoice.js";
import { LATEST_BATCH_VARIABLE, readJson, writeJson, writeText } from "../lib/lake.js";
import { formatUsd, sum } from "../lib/money.js";
import {
  buildBatchInvoices,
  buildStatements,
  invoiceDir,
  readOutbox,
  statementsDir,
} from "../lib/outbox.js";
import { renderSentReport, renderStatementsReport } from "../lib/report.js";

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

const batch = customerInvoices.task("resolve_batch", resolveBatch)();

const render = customerInvoices.taskGroup("render");
const invoices = render.task("render_invoices", renderInvoices)({ batch });
const statements = render.task("render_statements", renderStatements)({ batch });

customerInvoices.task("send_invoices", sendInvoices)({ invoices, statements });

interface BatchRef {
  batchDir: string;
  source: "finance" | "storefront";
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

async function renderInvoices({ batch }: { batch: BatchRef }) {
  const { invoices, skippedFullyRefunded } = await buildBatchInvoices(batch.batchDir, true);
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

async function renderStatements({ batch }: { batch: BatchRef }) {
  const { batchId, businessDate, invoices } = await buildBatchInvoices(batch.batchDir, false);
  const statements = buildStatements(invoices);
  const file = await writeJson(statementsDir(batch.batchDir), "statements.json", {
    batch_id: batchId,
    as_of: businessDate,
    statements,
  });
  console.log(renderStatementsReport(statements));
  return { path: file.path, customers: statements.length };
}

async function sendInvoices(args: { invoices: { dir: string }; statements: { path: string } }) {
  const items = await readOutbox(args.invoices.dir);
  const sent = items.filter((item) => item.status === "sent").length;
  const file = await writeJson(args.invoices.dir, "outbox_index.json", {
    batch_id: path.basename(args.invoices.dir),
    statements_path: args.statements.path,
    sent,
    held: items.length - sent,
    items,
  });
  console.log(renderSentReport(items));
  return { sent, held: items.length - sent, indexPath: file.path };
}
