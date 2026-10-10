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

// Customer invoices: grouping a batch's orders per customer and rendering them.

import { PRODUCTS_BY_SKU } from "./catalog.js";
import { formatMinor, formatUsd, toUsdCents, type FxRates } from "./money.js";
import type { Currency, Order, Refund } from "./types.js";

export interface InvoiceLine {
  order_id: string;
  sku: string;
  description: string;
  qty: number;
  unit_price_minor: number;
  currency: Currency;
}

export interface Invoice {
  invoice_no: string;
  customer_id: string;
  email: string;
  issued_on: string;
  batch_id: string;
  lines: InvoiceLine[];
  refunded_minor: Partial<Record<Currency, number>>;
  total_minor: Partial<Record<Currency, number>>;
  total_usd_cents: number;
  review_flagged: boolean;
}

export interface InvoiceBuild {
  invoices: Invoice[];
  skippedFullyRefunded: number;
}

export function buildInvoices(input: {
  batchId: string;
  businessDate: string;
  orders: readonly Order[];
  refunds: readonly Refund[];
  flaggedOrderIds: ReadonlySet<string>;
  rates: FxRates;
}): InvoiceBuild {
  const refundedByOrder = new Map<string, number>();
  for (const refund of input.refunds) {
    refundedByOrder.set(
      refund.order_id,
      (refundedByOrder.get(refund.order_id) ?? 0) + refund.amount_minor,
    );
  }

  const byCustomer = new Map<string, Order[]>();
  let skippedFullyRefunded = 0;
  for (const order of input.orders) {
    if ((refundedByOrder.get(order.order_id) ?? 0) >= order.total_minor) {
      skippedFullyRefunded += 1;
      continue;
    }
    const list = byCustomer.get(order.customer_id) ?? [];
    list.push(order);
    byCustomer.set(order.customer_id, list);
  }

  const date = input.businessDate.replaceAll("-", "");
  const invoices = [...byCustomer.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([customerId, orders], index): Invoice => {
      const total: Partial<Record<Currency, number>> = {};
      const refunded: Partial<Record<Currency, number>> = {};
      let usd = 0;
      const lines: InvoiceLine[] = [];
      for (const order of orders) {
        const refundedMinor = refundedByOrder.get(order.order_id) ?? 0;
        total[order.currency] = (total[order.currency] ?? 0) + order.total_minor - refundedMinor;
        if (refundedMinor > 0)
          refunded[order.currency] = (refunded[order.currency] ?? 0) + refundedMinor;
        usd += toUsdCents(order.total_minor - refundedMinor, order.currency, input.rates);
        for (const item of order.items) {
          lines.push({
            order_id: order.order_id,
            sku: item.sku,
            description: PRODUCTS_BY_SKU.get(item.sku)?.name ?? item.sku,
            qty: item.qty,
            unit_price_minor: item.unit_price_minor,
            currency: order.currency,
          });
        }
      }
      return {
        invoice_no: `INV-${date}-${String(index + 1).padStart(5, "0")}`,
        customer_id: customerId,
        email: orders[0]?.email ?? "",
        issued_on: input.businessDate,
        batch_id: input.batchId,
        lines,
        refunded_minor: refunded,
        total_minor: total,
        total_usd_cents: usd,
        review_flagged: orders.some((order) => input.flaggedOrderIds.has(order.order_id)),
      };
    });
  return { invoices, skippedFullyRefunded };
}

function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

export function renderInvoiceHtml(invoice: Invoice): string {
  const rows = invoice.lines
    .map(
      (line) =>
        `<tr><td>${escapeHtml(line.order_id)}</td><td>${escapeHtml(line.description)}</td>` +
        `<td class="n">${line.qty}</td><td class="n">${formatMinor(line.unit_price_minor, line.currency)}</td>` +
        `<td class="n">${formatMinor(line.qty * line.unit_price_minor, line.currency)}</td></tr>`,
    )
    .join("\n");
  const totals = Object.entries(invoice.total_minor)
    .map(([currency, minor]) => `<li>${formatMinor(minor, currency as Currency)}</li>`)
    .join("");
  return `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>${escapeHtml(invoice.invoice_no)}</title>
<style>body{font:14px system-ui;margin:2rem;max-width:48rem}table{border-collapse:collapse;width:100%}
td,th{border-bottom:1px solid #ddd;padding:.35rem .5rem;text-align:left}.n{text-align:right}</style></head>
<body>
<h1>CoC EU Retail invoice ${escapeHtml(invoice.invoice_no)}</h1>
<p>Customer ${escapeHtml(invoice.customer_id)} (${escapeHtml(invoice.email)})<br>Issued ${escapeHtml(invoice.issued_on)}</p>
<table><thead><tr><th>Order</th><th>Item</th><th class="n">Qty</th><th class="n">Unit</th><th class="n">Amount</th></tr></thead>
<tbody>
${rows}
</tbody></table>
<h2>Total due after refunds</h2><ul>${totals}</ul>
<p>Approx. ${formatUsd(invoice.total_usd_cents)}</p>
</body></html>
`;
}
