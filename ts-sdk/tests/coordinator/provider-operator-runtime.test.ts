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

import { fileURLToPath } from "node:url";

import { beforeEach, describe, expect, it } from "vitest";

import { Bundle } from "../../src/sdk/bundle.js";
import { Dag } from "../../src/sdk/dag.js";
import { DescribedOperator, resetSeen, seen, WriteOperator } from "./support/provider-fixture.js";
import {
  drive,
  makeStartupDetails,
  requestsOf,
  type Responder,
} from "./support/supervisor-driver.js";

/** The module the runtime imports to find the implementation, as a provider package is. */
const MODULE = fileURLToPath(new URL("./support/provider-fixture.ts", import.meta.url));
const CLASSPATH = "airflow.providers.demo.operators.file.WriteOperator";
const DOWNSTREAM = { write: ["after_a", "after_b"], after_a: [], after_b: [] };

const literal = (name: string, value: unknown, valueSchema?: Record<string, unknown>) => ({
  name,
  kind: "literal",
  value,
  ...(valueSchema !== undefined && { value_schema: valueSchema }),
});
const DATE_TIME = { type: "string", format: "date-time" };
const DURATION = { type: "number", format: "duration-seconds" };

/** The run context Airflow sends for a task that is a provider operator. */
function providerContext(
  kwargs: unknown[],
  overrides: Record<string, unknown> = {},
  extra: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    ...extra,
    provider_operator: {
      classpath: CLASSPATH,
      module: MODULE,
      kwargs,
      downstream_task_ids: DOWNSTREAM,
      ...overrides,
    },
  };
}

/** Renders by upper-casing every string, and answers `GetXCom` from `xcoms` by task id. */
function supervisor(xcoms: Record<string, unknown> = {}, fallback?: Responder): Responder {
  return (type, body) => {
    if (type === "RenderTemplateFields") {
      const values = body["values"] as Record<string, unknown>;
      return {
        body: {
          type: "RenderedTemplateFieldsResult",
          values: Object.fromEntries(
            Object.entries(values).map(([key, value]) => [
              key,
              typeof value === "string" ? value.toUpperCase() : value,
            ]),
          ),
        },
      };
    }
    if (type === "GetXCom") {
      return {
        body: { type: "XComResult", key: "return_value", value: xcoms[String(body["task_id"])] },
      };
    }
    return fallback?.(type, body);
  };
}

const failureOf = (result: Awaited<ReturnType<typeof drive>>) =>
  String(result.logRecords.find((r) => r["event"] === "[ts-sdk.runtime] Task failed")?.["error"]);

beforeEach(resetSeen);

describe("a provider operator of a Python Dag, which no bundle declares", () => {
  const start = (kwargs: unknown[], overrides = {}, extra = {}) =>
    makeStartupDetails("write", "py_dag", providerContext(kwargs, overrides, extra));

  it("imports the module, renders the template fields, runs it and pushes its result", async () => {
    const result = await drive(
      new Bundle(),
      start([literal("path", "/tmp/a"), literal("content", "hi")]),
      supervisor(),
    );

    expect(result.response).toMatchObject({ type: "SucceedTask" });
    expect(requestsOf(result, "RenderTemplateFields")).toEqual([
      { type: "RenderTemplateFields", values: { path: "/tmp/a", content: "hi" } },
    ]);
    expect(seen.args).toEqual([{ path: "/TMP/A", content: "HI" }]);
    expect(requestsOf(result, "SetXCom")).toEqual([
      expect.objectContaining({
        key: "return_value",
        value: { bytes: 3 },
        dag_id: "py_dag",
        task_id: "write",
        run_id: "run-1",
      }),
    ]);
    expect(seen.contexts[0]).toMatchObject({
      dagId: "py_dag",
      taskId: "write",
      runId: "run-1",
      tryNumber: 1,
      mapIndex: -1,
    });
    expect(seen.contexts[0]!.signal).toBeInstanceOf(AbortSignal);
    expect(seen.scope).toEqual([{ taskId: "write", hasClient: true }]);
  });

  it("keeps the task removed when the run context has no provider operator", async () => {
    const result = await drive(new Bundle(new Dag("other")), makeStartupDetails("write", "py_dag"));

    expect(result.response).toMatchObject({ type: "TaskState", state: "removed" });
    expect(seen.args).toEqual([]);
  });

  it("prefers a handler or Dag the bundle declares over the import", async () => {
    const dag = new Dag("py_dag");
    dag.task("write", async () => "handler")();

    const result = await drive(new Bundle(dag), start([literal("path", "/x")]), supervisor());

    expect(result.response).toMatchObject({ type: "SucceedTask" });
    expect(requestsOf(result, "SetXCom")[0]).toMatchObject({ value: "handler" });
    expect(seen.args).toEqual([]);
  });

  it("sends no render request when the task gives no template field", async () => {
    const result = await drive(
      new Bundle(),
      start([literal("file_mode", 420)], { classpath: classpathOf("PlainOperator") }),
      supervisor(),
    );

    expect(result.response).toMatchObject({ type: "SucceedTask" });
    expect(requestsOf(result, "RenderTemplateFields")).toEqual([]);
    expect(seen.args).toEqual([{ mode: 420 }]);
  });

  it("renders only the template fields the task gives", async () => {
    const result = await drive(
      new Bundle(),
      start([literal("path", "p"), literal("file_mode", 420)]),
      supervisor(),
    );

    expect(result.response).toMatchObject({ type: "SucceedTask" });
    expect(requestsOf(result, "RenderTemplateFields")).toEqual([
      { type: "RenderTemplateFields", values: { path: "p" } },
    ]);
    expect(seen.args).toEqual([{ path: "P", mode: 420 }]);
  });

  it("reads datetimes as dates, keeps durations in seconds and maps Python names to the binding's", async () => {
    const result = await drive(
      new Bundle(),
      start([
        literal("path", "p"),
        literal("file_mode", null),
        literal("timeout", 90, DURATION),
        literal("since", "2026-01-02T03:04:05+00:00", DATE_TIME),
        literal("options", { a: "x", b: [1, 2] }),
      ]),
      supervisor(),
    );

    expect(result.response).toMatchObject({ type: "SucceedTask" });
    expect(seen.args).toEqual([
      {
        path: "P",
        mode: null,
        timeout: 90,
        since: new Date("2026-01-02T03:04:05Z"),
        options: { a: "x", b: [1, 2] },
      },
    ]);
    // Only the template field travels to Python.
    expect(requestsOf(result, "RenderTemplateFields")).toEqual([
      { type: "RenderTemplateFields", values: { path: "p" } },
    ]);
  });

  it("fails the task when a datetime is not a valid date-time", async () => {
    const result = await drive(
      new Bundle(),
      start([literal("path", "p"), literal("since", "not a date", DATE_TIME)]),
      supervisor(),
    );

    expect(result.response).toMatchObject({ type: "TaskState", state: "failed" });
    expect(failureOf(result)).toContain('Argument "since"');
    expect(seen.args).toEqual([]);
  });

  it("pulls a task's result from XCom instead of rendering it", async () => {
    const result = await drive(
      new Bundle(),
      start([literal("path", "p"), { name: "content", kind: "xcom", task_id: "build" }]),
      supervisor({ build: "report body" }),
    );

    expect(result.response).toMatchObject({ type: "SucceedTask" });
    expect(requestsOf(result, "GetXCom")).toEqual([
      expect.objectContaining({ key: "return_value", task_id: "build", dag_id: "py_dag" }),
    ]);
    expect(requestsOf(result, "RenderTemplateFields")).toEqual([
      { type: "RenderTemplateFields", values: { path: "p" } },
    ]);
    expect(seen.args).toEqual([{ path: "P", content: "report body" }]);
  });

  it("fails the task and names the argument when a required one is missing", async () => {
    const result = await drive(new Bundle(), start([literal("content", "x")]), supervisor());

    expect(result.response).toMatchObject({ type: "TaskState", state: "failed" });
    expect(failureOf(result)).toContain('needs the argument "path"');
    expect(seen.args).toEqual([]);
  });

  it("logs an argument it does not know and runs on", async () => {
    const result = await drive(
      new Bundle(),
      start([literal("path", "p"), literal("retries", 2), literal("pool", "x")]),
      supervisor(),
    );

    expect(result.response).toMatchObject({ type: "SucceedTask" });
    const record = result.logRecords.find((r) => String(r["event"]).includes("Ignoring arguments"));
    expect(record).toMatchObject({ arguments: ["retries", "pool"], operator: CLASSPATH });
    expect(seen.args).toEqual([{ path: "P" }]);
  });

  it("pushes no XCom when the operator returns nothing", async () => {
    seen.behavior = () => undefined;

    const result = await drive(new Bundle(), start([literal("path", "p")]), supervisor());

    expect(result.response).toMatchObject({ type: "SucceedTask" });
    expect(requestsOf(result, "SetXCom")).toEqual([]);
  });

  it("fails the task when the operator throws, and retries it when it has retries left", async () => {
    seen.behavior = () => {
      throw new Error("disk full");
    };

    const failed = await drive(new Bundle(), start([literal("path", "p")]), supervisor());
    expect(failed.response).toMatchObject({ type: "TaskState", state: "failed" });

    const retried = await drive(
      new Bundle(),
      start([literal("path", "p")], {}, { should_retry: true }),
      supervisor(),
    );
    expect(retried.response).toMatchObject({ type: "RetryTask", retry_reason: "disk full" });
    expect(requestsOf(retried, "SetXCom")).toEqual([]);
  });

  it("fails the task when rendering fails", async () => {
    const failing: Responder = (type, body) =>
      type === "RenderTemplateFields"
        ? { error: { error: "GENERIC_ERROR", detail: { message: "bad template" } } }
        : supervisor()(type, body);

    const result = await drive(new Bundle(), start([literal("path", "p")]), failing);

    expect(result.response).toMatchObject({ type: "TaskState", state: "failed" });
    expect(seen.args).toEqual([]);
  });

  describe("when the module cannot give an implementation", () => {
    it.each([
      {
        name: "Airflow names no module",
        overrides: { module: null },
        message: "Airflow names no module that implements it",
      },
      {
        name: "the module cannot be imported",
        overrides: { module: "@apache-airflow/providers-missing/operators/file" },
        message: 'Cannot import "@apache-airflow/providers-missing/operators/file"',
      },
      {
        name: "the module has no export named after the class",
        overrides: { classpath: classpathOf("NotThere") },
        message: 'does not export a provider operator named "NotThere"',
      },
      {
        name: "the export is not a provider operator",
        overrides: { classpath: classpathOf("NotABinding") },
        message: 'does not export a provider operator named "NotABinding"',
      },
      {
        name: "the export was built by another copy of the SDK",
        overrides: { classpath: classpathOf("ForeignCopyOperator") },
        message: "built by another copy of @apache-airflow/ts-sdk",
      },
      {
        name: "the export has no execute",
        overrides: { classpath: classpathOf("DescribedOperator") },
        message: "which has no execute to run",
      },
      {
        name: "the export implements another class path",
        overrides: { classpath: classpathOf("MisnamedOperator") },
        message: "which implements airflow.providers.demo.operators.file.SomethingElse",
      },
    ])("fails the task when $name", async ({ overrides, message }) => {
      const result = await drive(
        new Bundle(),
        start([literal("path", "p")], overrides),
        supervisor(),
      );

      expect(result.response).toMatchObject({ type: "TaskState", state: "failed" });
      expect(failureOf(result)).toContain(message);
      expect(requestsOf(result, "RenderTemplateFields")).toEqual([]);
    });

    it("retries the task when it has retries left", async () => {
      const result = await drive(
        new Bundle(),
        start([literal("path", "p")], { module: null }, { should_retry: true }),
        supervisor(),
      );

      expect(result.response).toMatchObject({ type: "RetryTask" });
    });
  });

  describe("skipDownstream", () => {
    it("skips the direct downstream tasks of the task", async () => {
      seen.behavior = (ctx) => ctx.skipDownstream();

      const result = await drive(new Bundle(), start([literal("path", "p")]), supervisor());

      expect(result.response).toMatchObject({ type: "SucceedTask" });
      expect(requestsOf(result, "SkipDownstreamTasks")).toEqual([
        { type: "SkipDownstreamTasks", tasks: ["after_a", "after_b"] },
      ]);
      expect(requestsOf(result, "SetXCom")[0]).toMatchObject({
        key: "skipmixin_key",
        value: { skipped: ["after_a", "after_b"] },
      });
    });

    it("sends nothing for a task with no downstream task", async () => {
      seen.behavior = (ctx) => ctx.skipDownstream();

      const result = await drive(
        new Bundle(),
        makeStartupDetails("after_a", "py_dag", providerContext([literal("path", "p")])),
        supervisor(),
      );

      expect(result.response).toMatchObject({ type: "SucceedTask" });
      expect(requestsOf(result, "SkipDownstreamTasks")).toEqual([]);
    });
  });
});

describe("a provider operator of a TypeScript Dag, which the bundle declares", () => {
  function tsDag() {
    const dag = new Dag("ts_dag", { queue: "typescript" });
    dag.task("write", WriteOperator({ path: "/tmp/{{ run_id }}", mode: 420 }), {
      queue: "typescript",
    })();
    return dag;
  }

  it("runs the implementation of the Dag's record, without importing a module", async () => {
    const context = providerContext(
      [literal("path", "/tmp/{{ run_id }}"), literal("file_mode", 420)],
      { module: null },
    );

    const result = await drive(
      new Bundle(tsDag()),
      makeStartupDetails("write", "ts_dag", context),
      supervisor(),
    );

    expect(result.response).toMatchObject({ type: "SucceedTask" });
    expect(requestsOf(result, "RenderTemplateFields")).toEqual([
      { type: "RenderTemplateFields", values: { path: "/tmp/{{ run_id }}" } },
    ]);
    expect(seen.args).toEqual([{ path: "/TMP/{{ RUN_ID }}", mode: 420 }]);
    expect(requestsOf(result, "SetXCom")[0]).toMatchObject({
      key: "return_value",
      value: { bytes: 3 },
      dag_id: "ts_dag",
    });
  });

  it("fails the task when the run context has no provider operator", async () => {
    const result = await drive(
      new Bundle(tsDag()),
      makeStartupDetails("write", "ts_dag"),
      supervisor(),
    );

    expect(result.response).toMatchObject({ type: "TaskState", state: "failed" });
    expect(failureOf(result)).toContain("has no provider operator in its run context");
    expect(seen.args).toEqual([]);
  });

  it("fails the task when the run context is for another class path", async () => {
    const context = providerContext([literal("path", "p")], {
      classpath: classpathOf("PlainOperator"),
    });

    const result = await drive(
      new Bundle(tsDag()),
      makeStartupDetails("write", "ts_dag", context),
      supervisor(),
    );

    expect(result.response).toMatchObject({ type: "TaskState", state: "failed" });
    expect(failureOf(result)).toContain(`not a ${CLASSPATH}`);
    expect(seen.args).toEqual([]);
  });

  it("keeps failing a binding without execute, which only a Python worker can run", async () => {
    const dag = new Dag("ts_dag");
    dag.task("read", DescribedOperator({ path: "p" }))();

    const result = await drive(new Bundle(dag), makeStartupDetails("read", "ts_dag"));

    expect(result.response).toMatchObject({ type: "TaskState", state: "failed" });
    expect(failureOf(result)).toContain("only a Python worker can run");
  });
});

function classpathOf(className: string): string {
  return `airflow.providers.demo.operators.file.${className}`;
}
