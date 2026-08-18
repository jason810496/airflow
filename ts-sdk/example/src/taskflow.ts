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

// Handlers for the `typescript_taskflow_example` Dag, one per authoring style.
//
// Both styles produce the same call: the SDK passes one object holding `ctx`,
// `client`, and a key per bound argument. Which way the parameter is typed is
// the author's choice, so this bundle keeps one of each.

import type { TaskHandlerArgs } from "@apache-airflow/ts-sdk";

interface Region {
  code: string;
  name: string;
}

interface Totals {
  orders: number;
  revenue: number;
}

/**
 * Flat style: the bound arguments are annotated inline, alongside `ctx` and
 * `client`. Shortest for a handler that is not reused elsewhere.
 *
 * `dry_run` keeps its Python name on the wire and is renamed while
 * destructuring; the Dag call omits it, so it arrives from the stub's default.
 */
export async function summarize({
  client,
  region,
  totals,
  currency,
  dry_run: dryRun,
}: TaskHandlerArgs & {
  region: Region;
  totals: Totals;
  currency: string;
  dry_run: boolean;
}) {
  const average = totals.orders === 0 ? 0 : totals.revenue / totals.orders;

  if (!dryRun) {
    await client.setXCom({ key: "summary_line", value: `${region.name}: ${totals.orders} orders` });
  }

  return {
    region: region.code,
    orders: totals.orders,
    averageOrder: Number(average.toFixed(2)),
    currency,
    dryRun,
  };
}

/** Every argument the Dag's `report(...)` call binds. */
export interface ReportArgs {
  summary: {
    region: string;
    orders: number;
    averageOrder: number;
    currency: string;
  };
  region_code: string;
  threshold: number;
  retries_used: number;
  label: string;
}

/**
 * Interface style: one named interface carries every bound argument and is
 * intersected with {@link TaskHandlerArgs} to type the whole parameter. Worth
 * the name once a handler has several arguments, is exported for testing, or
 * wants its argument type documented next to it.
 */
export async function report({
  ctx,
  summary,
  region_code: regionCode,
  threshold,
  retries_used: retriesUsed,
  label,
}: ReportArgs & TaskHandlerArgs) {
  const healthy = summary.averageOrder >= threshold;

  return {
    label,
    regionCode,
    matchesSummary: summary.region === regionCode,
    healthy,
    retriesUsed,
    taskId: ctx.taskId,
  };
}
