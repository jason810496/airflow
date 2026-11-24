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

// Task handlers for `dags/typescript_mapping_stub_example.py`, whose mapped `@task.stub` tasks the
// Python Dag declares. Its `map()` callables are Python, so the runtime has Airflow's Python sidecar
// run them; each handler only sees one instance's finished arguments, or a reduce's sequence.

import { getContext, TaskHandler, type XComSequence } from "@apache-airflow/ts-sdk";

const DAG_ID = "typescript_mapping_stub_example";

async function collect<T>(sequence: XComSequence<T>): Promise<T[]> {
  const all: T[] = [];
  for await (const item of sequence) all.push(item);
  return all;
}

export const mappingStubHandlers = [
  new TaskHandler(DAG_ID, "transform", async ({ region, row }: { region: string; row: string }) => {
    return `${region}:${row}`;
  }),
  new TaskHandler(DAG_ID, "report", async ({ totals }: { totals: XComSequence<string> }) =>
    (await collect(totals)).join("|"),
  ),
  new TaskHandler(DAG_ID, "pair", async ({ item }: { item: [string, number] }) => `${item[0].trim()}-${item[1]}`),
  new TaskHandler(DAG_ID, "load", async ({ path, bucket }: { path: string; bucket: string }) => `${bucket}/${path}`),
  new TaskHandler(DAG_ID, "per_region.first", async ({ region }: { region: string }) => region.toUpperCase()),
  new TaskHandler(
    DAG_ID,
    "per_region.second",
    async ({ previous, region }: { previous: string; region: string }) =>
      `${previous}-${region}-${getContext().mapIndex}`,
  ),
  new TaskHandler(DAG_ID, "report_regions", async ({ values }: { values: XComSequence<string> }) =>
    collect(values),
  ),
];
