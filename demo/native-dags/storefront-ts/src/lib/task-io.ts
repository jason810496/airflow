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

// What the storefront task handlers share: finding the batch, exporting a feed, writing ops files.

import path from "node:path";

import { getClient } from "apache-airflow-ts-sdk";

import { currentBatch, readJson, writeJson } from "./lake.js";
import { orderCostCents, type PurchaseLine, type RestockStrategy } from "./restock.js";
import { toDollars } from "./money.js";
import { renderPurchaseOrder } from "./report.js";
import type { ExportSummary, InventoryFile, InventoryItem } from "./types.js";

const DEFAULT_SOURCE_HOST = "storefront.internal.coceuretail";

async function sourceHost(): Promise<string> {
  const connection = await getClient().getConnection("storefront_api");
  if (!connection?.host) {
    console.log(`Connection storefront_api has no host, using ${DEFAULT_SOURCE_HOST}`);
    return DEFAULT_SOURCE_HOST;
  }
  return connection.host;
}

/** Starts one feed export. `write` stores the feed file and returns the XCom. */
export async function startExport(feed: string) {
  const batch = currentBatch();
  const source = await sourceHost();
  console.log(`Exporting ${feed} for ${batch.businessDate} from ${source}, batch ${batch.batchId}`);
  return {
    batch,
    async write(body: object, records: number, note: string): Promise<ExportSummary> {
      const file = await writeJson(batch.dir, `${feed}.json`, {
        batch_id: batch.batchId,
        business_date: batch.businessDate,
        source,
        ...body,
      });
      console.log(
        `Wrote ${records} ${feed} (${file.bytes} bytes) to ${file.path}, ${note}`,
      );
      return { feed, path: file.path, records, source };
    },
  };
}

export async function loadInventory(summary: ExportSummary): Promise<InventoryItem[]> {
  return (await readJson<InventoryFile>(summary.path)).items;
}

export interface RestockResult {
  strategy: RestockStrategy;
  lines: number;
  estCostUsd: number;
  path: string;
}

function opsDir(): string {
  return path.join(currentBatch().dir, "ops");
}

export async function writeOps(name: string, value: unknown): Promise<string> {
  return (await writeJson(opsDir(), name, value)).path;
}

/** Writes ops/purchase_order.json. The file calls the strategy `standard`, `expedite` or `pause_campaigns`. */
export async function savePurchaseOrder(
  strategy: RestockStrategy,
  lines: readonly PurchaseLine[],
): Promise<RestockResult> {
  console.log(renderPurchaseOrder(strategy, lines));
  const costCents = orderCostCents(lines);
  const file = await writeOps("purchase_order.json", {
    batch_id: currentBatch().batchId,
    strategy: strategy.replace(/^restock_/, ""),
    lines,
    est_cost_usd_cents: costCents,
  });
  return { strategy, lines: lines.length, estCostUsd: toDollars(costCents), path: file };
}
