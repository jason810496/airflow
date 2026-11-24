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

// A Dag declared in TypeScript that maps tasks with every primitive ts-sdk ADR-0004 offers:
// `expand` over an upstream list, a literal list and a record, a cross product, `partial`,
// `map`, `zip` with padding, `concat`, `expandKwargs` over objects and over a `map`, a mapped
// upstream expanded over again, and a reduce. The e2e suite checks what each instance received.

import { Dag, getClient, getContext, SkipError, type XComSequence } from "@apache-airflow/ts-sdk";

export const dag = new Dag("typescript_mapping_example", {
  catchup: false,
  tags: ["typescript", "mapping"],
  description: "Task mapping declared in TypeScript.",
  queue: "typescript",
});

const files = dag.task("list_files", async (): Promise<string[]> => [" a.csv", "bb.csv "])();
const otherFiles = dag.task("list_other_files", async (): Promise<string[]> => ["c.csv"])();
const sizes = dag.task("list_sizes", async (): Promise<number[]> => [10])();

const trim = (path: string) => path.trim();

// One instance per file, each labelled with its path in the UI.
const measured = dag
  .task("measure", async ({ path, bucket }: { path: string; bucket: string }) => {
    await getClient().setRenderedMapIndex(path);
    return path.length + bucket.length;
  })
  .partial({ bucket: "raw" })
  .expand({ path: files.map(trim) });

// A map() callable that skips the last instance.
dag
  .task("picky", async ({ path }: { path: string }) => path)
  .expand({
    path: files.concat(otherFiles).map((path) => {
      if (path === "c.csv") throw new SkipError("c.csv is not wanted");
      return path.trim();
    }),
  });

// A cross product: four instances, the last key varying fastest, as in Python.
dag
  .task("grid", async ({ x, y }: { x: number; y: string }) => `${y}${x}/${getContext().expandedTiCount}`)
  .expand({ x: [1, 2], y: ["a", "b"] });

// One [key, value] pair per instance.
dag
  .task("tag", async ({ entry }: { entry: [string, string] }) => entry.join("="))
  .expand({ entry: { env: "prod", team: "data" } });

// The shorter source is padded with null.
dag
  .task("pair", async ({ p }: { p: [string | null, number | null] }) => `${p[0]?.trim()}:${p[1]}`)
  .expand({ p: files.zip(sizes, { padding: null }) });

// The first instance leaves `min` to the handler's default.
dag
  .task("sized", async ({ path, min = 5 }: { path: string; min?: number }) => path.length >= min)
  .expandKwargs([{ path: "a.csv" }, { path: "bb.csv", min: 10 }]);

// Each instance's arguments built by a map() callable, spread into the handler's arguments.
dag
  .task("locate", async ({ path, bucket }: { path: string; bucket: string }) => `${bucket}/${path}`)
  .partial({ bucket: "raw" })
  .expandKwargs(files.map((path) => ({ path: path.trim() })));

// A mapped task's instances expanded over again.
dag.task("audit", async ({ size }: { size: number }) => size * 2).expand({ size: measured });

// A reduce reads every instance of `measure`, on demand.
dag.task("summarize", async ({ sizes }: { sizes: XComSequence<number> }) => {
  const all: number[] = [];
  for await (const size of sizes) all.push(size);
  return { count: await sizes.length(), total: all.reduce((a, b) => a + b, 0), last: await sizes.at(-1) };
})({ sizes: measured });
