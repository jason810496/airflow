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

import { describe, expect, expectTypeOf, it } from "vitest";

import { serializeDag } from "../../src/coordinator/serde.js";
import { Dag, getDagTaskMapCallables, type TaskRef } from "../../src/sdk/dag.js";
import type { MappedTaskRef, XComSequence } from "../../src/sdk/mapping.js";

type Json = Record<string, unknown>;

function tasksOf(dag: Dag): Record<string, Json> {
  const data = serializeDag(dag, "", ".") as Json;
  return Object.fromEntries(
    (data["tasks"] as { __var: Json }[]).map((t) => [t.__var["task_id"] as string, t.__var]),
  );
}

const ref = (taskId: string) => ({ task_id: taskId, key: "return_value" });
const xcomref = (inner: Json) => ({ __type: "xcomref", __var: inner });

function declare() {
  const dag = new Dag("mapping");
  const listFiles = dag.task("list_files", async (): Promise<string[]> => ["a.csv", "b.csv"]);
  const listOther = dag.task("list_other", async (): Promise<string[]> => ["c.csv"]);
  const measure = dag.task(
    "measure",
    async ({ path, bucket }: { path: string; bucket: string }) => path.length + bucket.length,
  );
  return { dag, files: listFiles(), other: listOther(), measure };
}

describe("expand", () => {
  it("writes a mapped task as Python writes a decorated one", () => {
    const { dag, files, measure } = declare();
    const sizes = measure.partial({ bucket: "raw" }).expand({ path: files });
    expectTypeOf(sizes).toEqualTypeOf<MappedTaskRef<number>>();

    const tasks = tasksOf(dag);

    expect(tasks["measure"]).toMatchObject({
      _is_mapped: true,
      _expand_input_attr: "op_kwargs_expand_input",
      _disallow_kwargs_override: false,
      op_kwargs_expand_input: {
        type: "dict-of-lists",
        value: { __type: "dict", __var: { path: xcomref(ref("list_files")) } },
      },
      partial_kwargs: { op_kwargs: { __type: "dict", __var: { bucket: "raw" } }, op_args: [] },
      _arg_bindings: [
        { name: "bucket", kind: "param" },
        { name: "path", kind: "param" },
      ],
    });
    expect(tasks["list_files"]!["downstream_task_ids"]).toEqual(["measure"]);
  });

  it("writes map, zip and concat as the xcomref Python writes", () => {
    const { dag, files, other, measure } = declare();
    const trim = (path: string) => path.trim();
    measure.expand({
      path: files.concat(other).map(trim),
      bucket: files.zip(other, { padding: null }).map(([a]) => a ?? "none"),
    });

    const expandInput = (tasksOf(dag)["measure"]!["op_kwargs_expand_input"] as Json)[
      "value"
    ] as Json;

    expect((expandInput["__var"] as Json)["path"]).toEqual(
      xcomref({
        type: "map",
        arg: { type: "concat", args: [ref("list_files"), ref("list_other")] },
        callables: [trim.toString()],
      }),
    );
    expect(((expandInput["__var"] as Json)["bucket"] as Json)["__var"]).toMatchObject({
      type: "map",
      arg: { type: "zip", args: [ref("list_files"), ref("list_other")], fillvalue: null },
    });
    expect(tasksOf(dag)["list_other"]!["downstream_task_ids"]).toEqual(["measure"]);
  });

  it("expands over literal lists and records", () => {
    const dag = new Dag("literals");
    const tag = dag.task("tag", async ({ entry, n }: { entry: [string, string]; n: number }) => {
      void entry;
      return n;
    });
    tag.expand({ entry: { env: "prod", team: "data" }, n: [1, 2] });

    const value = (tasksOf(dag)["tag"]!["op_kwargs_expand_input"] as Json)["value"] as Json;

    expect(value["__var"]).toEqual({
      entry: { __type: "dict", __var: { env: "prod", team: "data" } },
      n: [1, 2],
    });
  });

  it("chains map callables into one wrapper, which the runtime finds by the argument's name", () => {
    const { dag, files, measure } = declare();
    const trim = (path: string) => path.trim();
    const upper = (path: string) => path.toUpperCase();
    measure.partial({ bucket: "raw" }).expand({ path: files.map(trim).map(upper) });

    expect(getDagTaskMapCallables(dag, "measure", "path")).toEqual([trim, upper]);
    expect(getDagTaskMapCallables(dag, "measure", "bucket")).toBeUndefined();
    expect(getDagTaskMapCallables(dag, "list_files", "path")).toBeUndefined();
  });
});

describe("expandKwargs", () => {
  it("writes one kwargs entry, which the runtime spreads into the handler's arguments", () => {
    const { dag, files, measure } = declare();
    const toArgs = (path: string) => ({ path });
    measure.partial({ bucket: "raw" }).expandKwargs(files.map(toArgs));

    const task = tasksOf(dag)["measure"]!;

    expect(task["_arg_bindings"]).toEqual([{ name: "bucket", kind: "param" }, { kind: "kwargs" }]);
    expect(task["_disallow_kwargs_override"]).toBe(true);
    expect(task["op_kwargs_expand_input"]).toEqual({
      type: "list-of-dicts",
      value: xcomref({ type: "map", arg: ref("list_files"), callables: [toArgs.toString()] }),
    });
    expect(getDagTaskMapCallables(dag, "measure", "**")).toEqual([toArgs]);
  });

  it("writes a literal list of objects", () => {
    const { dag, measure } = declare();
    measure.partial({ bucket: "raw" }).expandKwargs([{ path: "a.csv" }, { path: "b.csv" }]);

    expect(tasksOf(dag)["measure"]!["op_kwargs_expand_input"]).toEqual({
      type: "list-of-dicts",
      value: [
        { __type: "dict", __var: { path: "a.csv" } },
        { __type: "dict", __var: { path: "b.csv" } },
      ],
    });
  });
});

describe("a reduce", () => {
  it("reads a mapped task whole, as a sequence binding", () => {
    const { dag, files, measure } = declare();
    const sizes = measure.partial({ bucket: "raw" }).expand({ path: files });
    const summarize = dag.task("summarize", async ({ sizes }: { sizes: XComSequence<number> }) => {
      return sizes.length();
    });
    summarize({ sizes });

    const tasks = tasksOf(dag);

    expect(tasks["summarize"]!["_arg_bindings"]).toEqual([
      { name: "sizes", kind: "sequence", task_id: "measure" },
    ]);
    expect(tasks["measure"]!["downstream_task_ids"]).toEqual(["summarize"]);
  });
});

describe("refusals", () => {
  it("keeps map() outermost", () => {
    const { files } = declare();
    const mapped = files.map((path) => path) as unknown as TaskRef<string[]>;

    expect(() => files.zip(mapped)).toThrow(/not a map\(\) or zip\(\) result; call map\(\) last/);
  });

  it("takes no async map callable", () => {
    const { files } = declare();
    // A compile error too: MapCallable rejects a callable returning a promise.
    const asyncCallable = (async (path: string) => path) as never;
    expect(() => files.map(asyncCallable)).toThrow(/cannot be async/);
  });

  it("takes no undefined padding", () => {
    const { files, other } = declare();
    expect(() => files.zip(other, { padding: undefined as never })).toThrow(/pad with null/);
  });

  it("calls a task once, mapped or not", () => {
    const { files, measure } = declare();
    measure.expand({ path: files, bucket: ["raw"] });
    expect(() => measure({ path: "a", bucket: "b" })).toThrow(/was already called/);
  });

  it("takes an argument from partial() or expand(), not both", () => {
    const { files, measure } = declare();
    expect(() =>
      measure.partial({ bucket: "raw" }).expand({ path: files, bucket: ["eu"] } as never),
    ).toThrow(/given by both partial\(\) and expand\(\)/);
  });

  it("takes a source only in expand() and expandKwargs()", () => {
    const { files, measure } = declare();
    expect(() => measure({ path: files.map((p) => p) as never, bucket: "raw" })).toThrow(
      /only expand\(\) and expandKwargs\(\) take/,
    );
  });

  it("needs an argument to expand over", () => {
    const { measure } = declare();
    expect(() => measure.expand({} as never)).toThrow(/names no argument to expand over/);
  });

  it("needs elements to expand over", () => {
    const { measure } = declare();
    expect(() => measure.expand({ path: "a.csv" as never, bucket: ["raw"] })).toThrow(
      /has no elements to expand over/,
    );
  });
});
