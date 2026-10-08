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

// Reading a closed batch back from the lake and shaping the customer-facing files in the outbox.

import path from "node:path";

import { getClient } from "apache-airflow-ts-sdk";

import { buildInvoices, type Invoice, type InvoiceBuild } from "./invoice.js";
import { listFiles, outboxRoot, readJson, sha256Of } from "./lake.js";
import { parseFxRates } from "./money.js";
import type { OrdersFile, RefundsFile } from "./types.js";

export function invoiceDir(batchDir: string): string {
  return path.join(outboxRoot(), "invoices", path.basename(batchDir));
}

export function statementsDir(batchDir: string): string {
  return path.join(outboxRoot(), "statements", path.basename(batchDir));
}

/** Invoices for a storefront batch. Orders flagged by the checkout rules are marked held only when asked. */
export async function buildBatchInvoices(
  batchDir: string,
  holdFlagged: boolean,
): Promise<InvoiceBuild & { batchId: string; businessDate: string }> {
  const [orders, refunds, suspicious] = await Promise.all([
    readJson<OrdersFile>(path.join(batchDir, "orders.json")),
    readJson<RefundsFile>(path.join(batchDir, "refunds.json")),
    readJson<{ orders: { order_id: string }[] }>(path.join(batchDir, "suspicious_orders.json")),
  ]);
  const rates = parseFxRates(await getClient().getVariable("storefront.fx_rates"));
  const flagged = holdFlagged ? suspicious.orders.map((order) => order.order_id) : [];
  const build = buildInvoices({
    batchId: orders.batch_id,
    businessDate: orders.business_date,
    orders: orders.orders,
    refunds: refunds.refunds,
    flaggedOrderIds: new Set(flagged),
    rates,
  });
  return { ...build, batchId: orders.batch_id, businessDate: orders.business_date };
}

export function buildStatements(invoices: readonly Invoice[]) {
  return invoices
    .map((invoice) => ({
      customer_id: invoice.customer_id,
      email: invoice.email,
      orders: new Set(invoice.lines.map((line) => line.order_id)).size,
      balance_usd_cents: invoice.total_usd_cents,
    }))
    .sort((a, b) => b.balance_usd_cents - a.balance_usd_cents);
}

export type Statement = ReturnType<typeof buildStatements>[number];

/** The simulated send: one index entry per rendered invoice, held ones are not sent. */
export async function readOutbox(dir: string) {
  const names = (await listFiles(dir)).filter(
    (name) => name.startsWith("INV-") && name.endsWith(".json"),
  );
  const items = [];
  for (const name of names) {
    const invoice = await readJson<Invoice>(path.join(dir, name));
    items.push({
      invoice_no: invoice.invoice_no,
      customer_id: invoice.customer_id,
      email: invoice.email,
      status: invoice.review_flagged ? "held" : "sent",
      message_id: sha256Of(`${invoice.invoice_no}:${invoice.email}`).slice(0, 16),
    });
  }
  return items;
}

export type OutboxItem = Awaited<ReturnType<typeof readOutbox>>[number];
