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

import { describe, it, expect } from "vitest";
import {
  Dag,
  finalizeDag,
  getDagTaskInputs,
  getDagTaskRecords,
  type DagSpec,
  type TaskInputs,
  type TaskRef,
} from "../../src/sdk/dag.js";
import { brand } from "../../src/sdk/brand.js";
import { DagRegistry } from "../../src/sdk/registry.js";
import type { TaskHandlerArgs } from "../../src/sdk/task.js";

type RowArgs = { rows: number } & TaskHandlerArgs;

/** A `{dagId, taskId}` object carrying the brand a real reference has, as one
 *  from a second resolved copy of the package would. */
function forgeTaskRef(dagId: string, taskId: string): TaskRef {
  const ref = { dagId, taskId };
  brand(ref, "TaskRef");
  return ref;
}

describe("Dag", () => {
  it("returns a factory whose call yields a frozen TaskRef for the task", () => {
    const dag = new Dag("example_dag");
    const myTask = dag.task("my_task", async () => "hello");
    expect(typeof myTask).toBe("function");

    const task = myTask();
    expect(task).toEqual({ dagId: "example_dag", taskId: "my_task" });
    expect(Object.isFrozen(task)).toBe(true);
  });

  it("records the upstream references and literals each task is called with", () => {
    const dag = new Dag("chained_dag");
    const extract = dag.task("extract", async () => ({ rows: 1 }));
    const transform = dag.task(
      "transform",
      async (_args: { extracted: TaskRef; regionCode: string } & TaskHandlerArgs) => undefined,
    );
    const load = dag.task("load", async (_args: { transformed: TaskRef } & TaskHandlerArgs) => {});

    const extracted = extract();
    load({ transformed: transform({ extracted, regionCode: "us" }) });

    const inputs = getDagTaskInputs(dag);
    expect(inputs.get("extract")).toEqual({});
    expect(inputs.get("transform")).toEqual({ extracted, regionCode: "us" });
    expect(inputs.get("load")).toEqual({
      transformed: { dagId: "chained_dag", taskId: "transform" },
    });
  });

  it("records inputs frozen against later mutation of the caller's object", () => {
    const dag = new Dag("example_dag");
    const extract = dag.task("extract", async () => 1);
    const transform = dag.task("transform", async (_args: RowArgs) => undefined);
    const inputs: TaskInputs<RowArgs> = { rows: extract() };
    transform(inputs);

    (inputs as Record<string, unknown>).sneaky = 1;
    const recorded = getDagTaskInputs(dag).get("transform")!;
    expect(recorded).toEqual({ rows: { dagId: "example_dag", taskId: "extract" } });
    expect(Object.isFrozen(recorded)).toBe(true);
  });

  it("rejects a task called a second time", () => {
    const dag = new Dag("example_dag");
    const extract = dag.task("extract", async () => undefined);
    extract();
    expect(() => extract()).toThrowError(
      /Task "extract" of Dag "example_dag" was already called; a task holds one place in a Dag/,
    );
  });

  it("rejects a task that is never called, when the Dag is read", () => {
    const dag = new Dag("example_dag");
    dag.task("extract", async () => undefined)();
    dag.task("orphan", async () => undefined);
    expect(() => finalizeDag(dag)).toThrowError(
      /Task "orphan" of Dag "example_dag" is never called, so it has no place in the Dag/,
    );
  });

  it("accepts a fully called Dag however often it is read", () => {
    const dag = new Dag("example_dag");
    dag.task("extract", async () => undefined)();
    expect(() => {
      finalizeDag(dag);
      finalizeDag(dag);
    }).not.toThrow();
  });

  it("keeps reporting an incomplete Dag every time it is read", () => {
    const dag = new Dag("example_dag");
    dag.task("orphan", async () => undefined);
    expect(() => finalizeDag(dag)).toThrowError(/is never called/);
    expect(() => finalizeDag(dag)).toThrowError(/is never called/);
  });

  it("rejects wiring once the Dag has been read", () => {
    const dag = new Dag("example_dag");
    const extract = dag.task("extract", async () => undefined);
    extract();
    finalizeDag(dag);

    expect(() => extract()).toThrowError(
      /Task "extract" of Dag "example_dag" was called after the Dag was read/,
    );
  });

  it("rejects a task declared once the Dag has been read", () => {
    const dag = new Dag("example_dag");
    dag.task("extract", async () => undefined)();
    finalizeDag(dag);

    expect(() => dag.task("late", async () => undefined)).toThrowError(
      /Task "late" cannot be added to Dag "example_dag" after the Dag was read/,
    );
    expect(dag.taskIds).toEqual(["extract"]);
  });

  it("rejects an input taken from another Dag", () => {
    const first = new Dag("first_dag");
    const second = new Dag("second_dag");
    const extracted = first.task("extract", async () => 1)();
    const transform = second.task("transform", async (_args: RowArgs) => undefined);
    expect(() => transform({ rows: extracted })).toThrowError(
      /Input "rows" of task "transform" comes from Dag "first_dag", not "second_dag"/,
    );
  });

  it("takes an unbranded task-handle look-alike as a literal, not as an upstream", () => {
    const dag = new Dag("example_dag");
    const transform = dag.task(
      "transform",
      async (_args: { config: { dagId: string; taskId: string } } & TaskHandlerArgs) => undefined,
    );
    const config = { dagId: "example_dag", taskId: "extract" };
    transform({ config });

    expect(getDagTaskInputs(dag).get("transform")).toEqual({ config });
  });

  it("rejects a branded reference to a task this Dag never registered", () => {
    const dag = new Dag("example_dag");
    const transform = dag.task("transform", async (_args: RowArgs) => undefined);
    expect(() => transform({ rows: forgeTaskRef("example_dag", "ghost") })).toThrowError(
      /Input "rows" of task "transform" refers to unregistered task "ghost"/,
    );
  });

  it("rejects a reference from another Dag object carrying the same Dag ID", () => {
    const dag = new Dag("example_dag");
    dag.task("extract", async () => undefined);
    const transform = dag.task("transform", async (_args: RowArgs) => undefined);
    // What a second resolved copy of the package hands over: same IDs, same
    // brand, but not a reference this Dag returned.
    expect(() => transform({ rows: forgeTaskRef("example_dag", "extract") })).toThrowError(
      /Input "rows" of task "transform" was not returned by this Dag's "extract"/,
    );
  });

  it("rejects an input keyed by a symbol, which no argument name can be", () => {
    const dag = new Dag("example_dag");
    const extract = dag.task("extract", async () => undefined);
    const transform = dag.task("transform", async (_args: RowArgs) => undefined);
    const inputs = { [Symbol("rows")]: extract() } as unknown as TaskInputs<RowArgs>;
    expect(() => transform(inputs)).toThrowError(
      /Input "Symbol\(rows\)" of task "transform" is keyed by a symbol/,
    );
  });

  it("rejects task inputs that are not a plain object", () => {
    const dag = new Dag("example_dag");
    const transform = dag.task("transform", async (_args: RowArgs) => undefined);
    expect(() => transform(new Date() as unknown as TaskInputs<RowArgs>)).toThrowError(
      /inputs for Dag "example_dag" task "transform" must be an object/,
    );
    expect(getDagTaskInputs(dag).has("transform")).toBe(false);
  });

  it("retains its spec and each task's handler and spec, copied and frozen", () => {
    const dagSpec = {};
    const taskSpec = {};
    const handler = async () => "hello";
    const dag = new Dag("example_dag", dagSpec);
    dag.task("my_task", handler, { spec: taskSpec })();

    expect(dag.dagId).toBe("example_dag");
    expect(dag.spec).toEqual(dagSpec);
    expect(Object.isFrozen(dag.spec)).toBe(true);
    const record = getDagTaskRecords(dag).get("my_task");
    expect(record?.handler).toBe(handler);
    expect(record?.spec).toEqual(taskSpec);
    expect(Object.isFrozen(record!.spec)).toBe(true);
  });

  it.each([
    ["null", null],
    ["an array", []],
    ["a non-plain object", new Date()],
  ])("rejects a Dag spec that is not an options object: %s", (_label, spec) => {
    expect(() => new Dag("example_dag", spec as unknown as DagSpec)).toThrowError(
      /spec for Dag "example_dag" must be an object/,
    );
  });

  it("rejects an unknown key in the Dag spec", () => {
    expect(() => new Dag("example_dag", { schedule: "@daily" } as unknown as DagSpec)).toThrowError(
      /Unknown option "schedule" in the spec for Dag "example_dag"/,
    );
  });

  it("rejects a non-boolean isMixedLanguageDag", () => {
    expect(
      () => new Dag("example_dag", { isMixedLanguageDag: "yes" } as unknown as DagSpec),
    ).toThrowError(/isMixedLanguageDag for Dag "example_dag" must be a boolean/);
  });

  it.each([
    ["a populated object", { retries: 2 }],
    ["null", null],
    ["an array", []],
    ["a non-plain object", new Date()],
  ])("rejects a task spec that is not an empty object: %s", (_label, spec) => {
    const dag = new Dag("example_dag");
    expect(() =>
      dag.task("transform", async () => undefined, {
        spec: spec as unknown as Record<string, never>,
      }),
    ).toThrowError(/spec for Dag "example_dag" task "transform" must be an empty object/);
    expect(dag.taskIds).toEqual([]);
  });

  it("exposes its task IDs in attachment order", () => {
    const dag = new Dag("ordered_dag");
    expect(dag.taskIds).toEqual([]);
    dag.task("extract", async () => undefined);
    dag.task("transform", async () => undefined);
    expect(dag.taskIds).toEqual(["extract", "transform"]);
  });

  it.each([
    ["a misspelled spec key", { specs: {} }],
    ["an upstream handle passed as an option", { upstream: { dagId: "d", taskId: "t" } }],
  ])("rejects %s in the task options", (_label, options) => {
    const dag = new Dag("example_dag");
    expect(() =>
      dag.task("transform", async () => undefined, options as unknown as Record<string, never>),
    ).toThrowError(/Unknown option ".+" for Dag "example_dag" task "transform"/);
    expect(dag.taskIds).toEqual([]);
  });

  it.each([
    ["null", null],
    ["an array", []],
    ["a string", "spec"],
    ["a non-plain object", new Date()],
  ])("rejects task options that are not an options object: %s", (_label, options) => {
    const dag = new Dag("example_dag");
    expect(() =>
      dag.task("transform", async () => undefined, options as unknown as Record<string, never>),
    ).toThrowError(/options for Dag "example_dag" task "transform" must be an object/);
  });

  it("rejects duplicate taskIds within a Dag", () => {
    const dag = new Dag("example_dag");
    dag.task("dup", async () => undefined);
    expect(() => dag.task("dup", async () => undefined)).toThrowError(/already registered/);
  });

  it("allows the same taskId in different Dags", () => {
    const first = async () => "first";
    const second = async () => "second";
    const firstDag = new Dag("first_dag");
    const secondDag = new Dag("second_dag");
    firstDag.task("extract", first)();
    secondDag.task("extract", second)();

    const registry = new DagRegistry();
    registry.register(firstDag, secondDag);
    expect(registry.getTaskHandler("first_dag", "extract")).toBe(first);
    expect(registry.getTaskHandler("second_dag", "extract")).toBe(second);
  });

  it("accepts a Unicode dagId that Python's word-character rule allows", () => {
    const handler = async () => undefined;
    const dag = new Dag("café_dag");
    dag.task("任務", handler)();
    const registry = new DagRegistry();
    registry.register(dag);
    expect(registry.getTaskHandler("café_dag", "任務")).toBe(handler);
  });

  it("rejects non-function handlers", () => {
    const dag = new Dag("example_dag");
    expect(() => dag.task("x", "not a function" as unknown as () => Promise<unknown>)).toThrowError(
      /must be a function/,
    );
  });

  it("treats a dotted TaskGroup taskId as a single taskId (group.task)", () => {
    const dag = new Dag("example_dag");
    dag.task("transforms.normalize", async () => "ok")();
    const registry = new DagRegistry();
    registry.register(dag);
    expect(registry.getTaskHandler("example_dag", "transforms.normalize")).toBeDefined();
    // Should NOT accidentally match the prefix alone
    expect(registry.getTaskHandler("example_dag", "transforms")).toBeUndefined();
    expect(registry.getTaskHandler("example_dag", "normalize")).toBeUndefined();
  });

  describe("bound to a Dag declared in Python", () => {
    it("takes its layout from the Python Dag, so an uncalled task is fine", () => {
      const dag = new Dag("python_dag", { isMixedLanguageDag: true });
      dag.task("extract", async () => undefined);
      expect(() => finalizeDag(dag)).not.toThrow();
    });

    it("rejects wiring, which the Python Dag owns", () => {
      const dag = new Dag("python_dag", { isMixedLanguageDag: true });
      const extract = dag.task("extract", async () => undefined);
      expect(() => extract()).toThrowError(
        /Task "extract" of Dag "python_dag" cannot be called: the Dag is declared in Python/,
      );
    });

    it("rejects a task spec, which the Python Dag owns", () => {
      const dag = new Dag("python_dag", { isMixedLanguageDag: true });
      expect(() =>
        dag.task("extract", async () => undefined, {
          spec: { retries: 2 } as unknown as Record<string, never>,
        }),
      ).toThrowError(
        /Task "extract" of Dag "python_dag" cannot take a spec: the Dag is declared in Python/,
      );
      expect(dag.taskIds).toEqual([]);
    });
  });
});
