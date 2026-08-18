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

import { describe, expect, it } from "vitest";

import {
  computeRelativeFileloc,
  parseDags,
  serializeDag,
  serializeValue,
  unwrapTypeEncoding,
} from "../../src/coordinator/serde.js";
import { Dag, type DagSpec, type TaskRef, type TaskSpec } from "../../src/sdk/dag.js";
import { DagRegistry } from "../../src/sdk/registry.js";
import type { TaskHandlerArgs } from "../../src/sdk/task.js";

type Json = Record<string, unknown>;
type WiredArgs = Record<string, TaskRef> & TaskHandlerArgs;

/** Declare a task and place it, optionally behind some upstreams. */
function place(dag: Dag, taskId: string, upstream: readonly TaskRef[] = [], spec?: TaskSpec) {
  const factory = dag.task(taskId, async (_args: WiredArgs) => undefined, spec ? { spec } : {});
  const inputs: Record<string, TaskRef> = {};
  upstream.forEach((ref, index) => {
    inputs[`in_${index}`] = ref;
  });
  return factory(inputs);
}

/** A one-task Dag, serialized. */
function serializeWith(spec: DagSpec, taskSpec?: TaskSpec): Json {
  const dag = new Dag("d", spec);
  place(dag, "t", [], taskSpec);
  return serializeDag(dag, "/bundles/app/bundle.mjs", "bundle.mjs") as Json;
}

function taskVar(serialized: Json, index = 0): Json {
  const tasks = serialized["tasks"] as { __type: string; __var: Json }[];
  expect(tasks[index]!.__type).toBe("operator");
  return tasks[index]!.__var;
}

describe("serializeDag", () => {
  it("writes the fields Airflow always expects", () => {
    const serialized = serializeWith({});

    expect(serialized).toMatchObject({
      dag_id: "d",
      fileloc: "/bundles/app/bundle.mjs",
      relative_fileloc: "bundle.mjs",
      timezone: "UTC",
      dag_dependencies: [],
      edge_info: {},
      params: [],
      deadline: null,
      allowed_run_types: null,
      max_active_tasks: 16,
      max_active_runs: 16,
      max_consecutive_failed_dag_runs: 0,
      catchup: false,
      disable_bundle_versioning: false,
    });
  });

  it("puts every task in the flat root group", () => {
    const dag = new Dag("d");
    const first = place(dag, "first");
    place(dag, "second", [first]);

    expect(serializeDag(dag, "", ".")["task_group"]).toEqual({
      _group_id: null,
      group_display_name: "",
      prefix_group_id: true,
      tooltip: "",
      ui_color: "CornflowerBlue",
      ui_fgcolor: "#000",
      children: { first: ["operator", "first"], second: ["operator", "second"] },
      upstream_group_ids: [],
      downstream_group_ids: [],
      upstream_task_ids: [],
      downstream_task_ids: [],
    });
  });

  it("identifies a task as TypeScript rather than as a Python operator", () => {
    expect(taskVar(serializeWith({}))).toEqual({
      task_id: "t",
      task_type: "TypeScriptOperator",
      _task_module: "airflow.sdk.coordinators.node",
      language: "typescript",
      template_fields: [],
    });
  });

  describe("timetable", () => {
    it.each([
      ["unset", undefined, { __type: "airflow.timetables.simple.NullTimetable", __var: {} }],
      ["@once", "@once", { __type: "airflow.timetables.simple.OnceTimetable", __var: {} }],
      [
        "@continuous",
        "@continuous",
        { __type: "airflow.timetables.simple.ContinuousTimetable", __var: {} },
      ],
      [
        "a cron expression",
        "0 3 * * *",
        {
          __type: "airflow.timetables.trigger.CronTriggerTimetable",
          __var: {
            expression: "0 3 * * *",
            timezone: "UTC",
            interval: 0,
            run_immediately: false,
          },
        },
      ],
    ])("maps %s onto the matching timetable", (_name, schedule, expected) => {
      expect(serializeWith({ schedule })["timetable"]).toEqual(expected);
    });

    it("passes a cron preset through for the scheduler to expand", () => {
      // Python expands the preset before serializing, so it would record
      // "0 0 * * *" here. Both rebuild the same CronTriggerTimetable, because
      // its constructor resolves a preset either way, so the preset is kept
      // rather than duplicating Airflow's preset table in this SDK.
      const timetable = serializeWith({ schedule: "@daily" })["timetable"] as Json;
      expect(timetable["__type"]).toBe("airflow.timetables.trigger.CronTriggerTimetable");
      expect((timetable["__var"] as Json)["expression"]).toBe("@daily");
    });

    it.each([
      ["an asset expression", { assets: ["s3://bucket/key"] }, /an object schedule names a Python/],
      ["a number", 86400, /a number schedule names a Python/],
      ["an empty string", "", /schedule for Dag "d" is empty/],
      ["a blank string", "   ", /schedule for Dag "d" is empty/],
    ])("rejects %s", (_name, schedule, expected) => {
      expect(() => serializeWith({ schedule } as DagSpec)).toThrowError(expected);
    });
  });

  describe("non-decorated fields", () => {
    it("writes them as bare values, without the type encoding", () => {
      const serialized = serializeWith({
        startDate: new Date("2026-01-01T00:00:00Z"),
        endDate: new Date("2026-12-31T23:30:15Z"),
        dagrunTimeout: 300,
        tags: ["gamma", "alpha"],
        description: "demo",
      });

      expect(serialized).toMatchObject({
        start_date: 1767225600,
        end_date: 1798759815,
        dagrun_timeout: 300,
        // Python holds tags in a set and writes them sorted.
        tags: ["alpha", "gamma"],
        description: "demo",
      });
    });

    it("keeps only what a decorated field would keep", () => {
      // The two halves of the split: everything above went through
      // serializeValue and then lost its wrapper, because no authoring field is
      // in Python's decorated set. A decorated field would stop at the first.
      const wrapped = serializeValue(new Date("2026-01-01T00:00:00Z"));
      expect(wrapped).toEqual({ __type: "datetime", __var: 1767225600 });
      expect(unwrapTypeEncoding(wrapped)).toBe(1767225600);
    });

    it("collapses duplicate tags, as the set Python holds them in does", () => {
      expect(serializeWith({ tags: ["b", "a", "b"] })["tags"]).toEqual(["a", "b"]);
    });
  });

  describe("omit-if-default", () => {
    it("omits a Dag field left at its schema default", () => {
      const serialized = serializeWith({ failFast: false, renderTemplateAsNativeObj: false });
      expect(serialized).not.toHaveProperty("fail_fast");
      expect(serialized).not.toHaveProperty("render_template_as_native_obj");
    });

    it("writes a Dag field that differs from its schema default", () => {
      expect(serializeWith({ failFast: true })).toMatchObject({ fail_fast: true });
    });

    it("omits task fields left at their schema defaults", () => {
      const task = taskVar(
        serializeWith(
          {},
          { retries: 0, queue: "default", pool: "default_pool", retryDelay: 300, owner: "airflow" },
        ),
      );
      for (const key of ["retries", "queue", "pool", "retry_delay", "owner"]) {
        expect(task).not.toHaveProperty(key);
      }
    });

    it("writes task fields that differ from their schema defaults", () => {
      const task = taskVar(
        serializeWith(
          {},
          { retries: 2, queue: "typescript", retryDelay: 600, executionTimeout: 5 },
        ),
      );
      expect(task).toMatchObject({
        retries: 2,
        queue: "typescript",
        retry_delay: 600,
        execution_timeout: 5,
      });
    });

    it("never writes the email flags, which have no recipient to reach", () => {
      const task = taskVar(serializeWith({}, { emailOnFailure: false, emailOnRetry: false }));
      expect(task).not.toHaveProperty("email_on_failure");
      expect(task).not.toHaveProperty("email_on_retry");
    });
  });

  describe("downstream_task_ids", () => {
    it("inverts the recorded wiring, sorted", () => {
      const dag = new Dag("d");
      const root = place(dag, "root");
      const right = place(dag, "right", [root]);
      const left = place(dag, "left", [root]);
      place(dag, "join", [right, left]);

      const serialized = serializeDag(dag, "", ".") as Json;
      expect(taskVar(serialized, 0)["downstream_task_ids"]).toEqual(["left", "right"]);
      expect(taskVar(serialized, 1)["downstream_task_ids"]).toEqual(["join"]);
      expect(taskVar(serialized, 3)).not.toHaveProperty("downstream_task_ids");
    });

    it("counts one edge when two arguments come from the same upstream", () => {
      const dag = new Dag("d");
      const upstream = place(dag, "up");
      place(dag, "down", [upstream, upstream]);

      expect(taskVar(serializeDag(dag, "", ".") as Json, 0)["downstream_task_ids"]).toEqual([
        "down",
      ]);
    });

    it("ignores a literal argument that looks like a reference", () => {
      const dag = new Dag("d");
      place(dag, "up");
      const factory = dag.task("down", async (_args: WiredArgs) => undefined);
      factory({ config: { dagId: "d", taskId: "up" } as unknown as TaskRef });

      expect(taskVar(serializeDag(dag, "", ".") as Json, 0)).not.toHaveProperty(
        "downstream_task_ids",
      );
    });
  });

  describe("rejects a value the schema cannot carry", () => {
    it.each([
      ["startDate", { startDate: "2026-01-01" }, /startDate for Dag "d" must be a valid Date/],
      ["an invalid Date", { startDate: new Date("nope") }, /must be a valid Date/],
      ["description", { description: 7 }, /description for Dag "d" must be a string/],
      ["catchup", { catchup: "yes" }, /catchup for Dag "d" must be a boolean/],
      [
        "maxActiveRuns",
        { maxActiveRuns: "3" },
        /maxActiveRuns for Dag "d" must be a finite number/,
      ],
      ["dagrunTimeout", { dagrunTimeout: Infinity }, /must be a duration in seconds/],
      ["tags", { tags: ["a", 2] }, /tags for Dag "d" must be an array of strings/],
    ])("on %s", (_name, spec, expected) => {
      expect(() => serializeWith(spec as DagSpec)).toThrowError(expected);
    });

    it("names the task a bad task field belongs to", () => {
      expect(() => serializeWith({}, { retries: "two" } as unknown as TaskSpec)).toThrowError(
        /retries for task "t" of Dag "d" must be a finite number/,
      );
    });
  });
});

describe("serializeValue", () => {
  it.each([
    ["a string", "x", "x"],
    ["a boolean", true, true],
    ["a number", 1.5, 1.5],
    ["null", null, null],
    ["undefined", undefined, null],
    ["a list, without a wrapper", [1, "a"], [1, "a"]],
  ])("passes %s through", (_name, value, expected) => {
    expect(serializeValue(value)).toEqual(expected);
  });

  it("encodes a Date as fractional epoch seconds", () => {
    expect(serializeValue(new Date("2026-01-01T00:00:00.500Z"))).toEqual({
      __type: "datetime",
      __var: 1767225600.5,
    });
  });

  it("encodes a Set as a sorted list", () => {
    expect(serializeValue(new Set(["gamma", "alpha", "beta"]))).toEqual({
      __type: "set",
      __var: ["alpha", "beta", "gamma"],
    });
  });

  it.each([
    ["an object", { b: 1, a: "x" }],
    [
      "a Map",
      new Map<string, unknown>([
        ["b", 1],
        ["a", "x"],
      ]),
    ],
  ])("encodes %s as a dict", (_name, value) => {
    expect(serializeValue(value)).toEqual({ __type: "dict", __var: { b: 1, a: "x" } });
  });

  it("recurses into nested values", () => {
    expect(serializeValue({ when: new Date("2026-01-01T00:00:00Z"), items: [{ n: 1 }] })).toEqual({
      __type: "dict",
      __var: {
        when: { __type: "datetime", __var: 1767225600 },
        items: [{ __type: "dict", __var: { n: 1 } }],
      },
    });
  });

  it.each([
    ["a non-finite number", Number.NaN, /non-finite number/],
    ["an invalid Date", new Date("nope"), /invalid Date/],
    ["a function", () => undefined, /Cannot serialize a function/],
  ])("rejects %s", (_name, value, expected) => {
    expect(() => serializeValue(value)).toThrowError(expected);
  });
});

describe("unwrapTypeEncoding", () => {
  it("takes the __var of an encoded value", () => {
    expect(unwrapTypeEncoding({ __type: "timedelta", __var: 300 })).toBe(300);
  });

  it.each([
    ["a primitive", 5],
    ["a list", [1, 2]],
    ["an object that is not encoded", { __var: 1 }],
  ])("leaves %s alone", (_name, value) => {
    expect(unwrapTypeEncoding(value)).toEqual(value);
  });
});

describe("computeRelativeFileloc", () => {
  it.each([
    ["a file inside the bundle", "/bundles/app/dags/bundle.mjs", "/bundles/app", "dags/bundle.mjs"],
    ["a file at the bundle root", "/bundles/app/bundle.mjs", "/bundles/app", "bundle.mjs"],
    ["a file that is the bundle", "/bundles/app", "/bundles/app", "."],
    ["an unknown bundle path", "/bundles/app/bundle.mjs", "", "."],
    ["an unknown file", "", "/bundles/app", ""],
  ])("resolves %s", (_name, fileloc, bundlePath, expected) => {
    expect(computeRelativeFileloc(fileloc, bundlePath)).toBe(expected);
  });
});

describe("parseDags", () => {
  const request = { file: "/bundles/app/bundle.mjs", bundle_path: "/bundles/app" };

  it("wraps each Dag in the envelope the supervisor expects", () => {
    const dag = new Dag("native_dag");
    place(dag, "t");

    const result = parseDags(new DagRegistry(dag), request);

    expect(result.type).toBe("DagFileParsingResult");
    expect(result.fileloc).toBe("/bundles/app/bundle.mjs");
    expect(result.serialized_dags).toHaveLength(1);
    const entry = result.serialized_dags[0]!;
    expect(entry.data["__version"]).toBe(3);
    expect((entry.data["dag"] as Json)["dag_id"]).toBe("native_dag");
    expect((entry.data["dag"] as Json)["relative_fileloc"]).toBe("bundle.mjs");
  });

  it("leaves out a mixed-language Dag, whose graph belongs to a Python file", () => {
    const native = new Dag("native_dag");
    place(native, "t");
    const mixed = new Dag("python_dag", { isMixedLanguageDag: true });
    mixed.task("bound", async () => undefined);

    const result = parseDags(new DagRegistry(native, mixed), request);

    expect(result.serialized_dags.map((entry) => (entry.data["dag"] as Json)["dag_id"])).toEqual([
      "native_dag",
    ]);
  });

  it("serializes nothing for a bundle that only binds handlers to Python Dags", () => {
    const mixed = new Dag("python_dag", { isMixedLanguageDag: true });
    mixed.task("bound", async () => undefined);

    expect(parseDags(new DagRegistry(mixed), request).serialized_dags).toEqual([]);
  });
});
