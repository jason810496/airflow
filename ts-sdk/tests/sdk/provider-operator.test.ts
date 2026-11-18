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

import { serializeDag } from "../../src/coordinator/serde.js";
import { Dag, type TaskRef } from "../../src/sdk/dag.js";
import type { OperatorContext } from "../../src/sdk/operator.js";
import { defineProviderOperator, type Templated } from "../../src/sdk/provider-operator.js";

type Json = Record<string, unknown>;

interface NotifyArgs {
  subject: Templated<string>;
  recipients?: Templated<readonly string[]>;
  channel?: string;
  responseTimeout?: number | null;
  notBefore?: Date | null;
  extra?: Readonly<Record<string, unknown>>;
}

const Notify = defineProviderOperator<NotifyArgs>({
  module: "airflow.providers.demo.operators.notify",
  className: "NotifyOperator",
  templateFields: ["subject", "recipients"],
  params: {
    subject: { name: "subject", required: true },
    recipients: { name: "recipients" },
    channel: { name: "channel" },
    responseTimeout: { name: "response_timeout", encoding: "timedelta" },
    notBefore: { name: "not_before", encoding: "datetime" },
    extra: { name: "extra" },
  },
});

const WaitSensor = defineProviderOperator<{ path: string }>({
  module: "airflow.providers.demo.sensors.wait",
  className: "WaitSensor",
  templateFields: ["path"],
  templateExt: [".txt"],
  templateFieldsRenderers: { path: "bash" },
  uiColor: "#e6f1f2",
  uiFgcolor: "#111",
  operatorName: "Wait",
  isSensor: true,
  canSkipDownstream: true,
  params: { path: { name: "path", required: true } },
});

function serializeTasks(dag: Dag): Map<string, Json> {
  const serialized = serializeDag(dag, "/bundles/app/bundle.min.mjs", "bundle.min.mjs") as Json;
  const tasks = serialized["tasks"] as { __var: Json }[];
  return new Map(tasks.map((task) => [task.__var["task_id"] as string, task.__var]));
}

describe("defineProviderOperator", () => {
  it("serializes the task as the Python operator class with its constructor arguments", () => {
    const dag = new Dag("d");
    dag.task(
      "notify",
      Notify({
        subject: "Run {{ ds }}",
        channel: "ops",
        responseTimeout: 90,
        notBefore: new Date("2026-01-02T03:04:05Z"),
        extra: { tags: ["a", "b"], nested: { level: 1 } },
      }),
      { retries: 2, retryDelay: 30, pool: "notifications" },
    )();

    expect(serializeTasks(dag).get("notify")).toEqual({
      task_id: "notify",
      task_type: "NotifyOperator",
      _task_module: "airflow.providers.demo.operators.notify",
      language: "typescript",
      is_stub: true,
      stub_kind: "provider_operator",
      template_fields: ["subject", "recipients"],
      retries: 2,
      retry_delay: 30,
      pool: "notifications",
      // In the order the operator declares its parameters, then the task options.
      _operator_kwargs: [
        { name: "subject", kind: "literal", value: "Run {{ ds }}" },
        { name: "channel", kind: "literal", value: "ops" },
        {
          name: "response_timeout",
          kind: "literal",
          value: 90,
          value_schema: { type: "number", format: "duration-seconds" },
        },
        {
          name: "not_before",
          kind: "literal",
          value: "2026-01-02T03:04:05.000Z",
          value_schema: { type: "string", format: "date-time" },
        },
        { name: "extra", kind: "literal", value: { tags: ["a", "b"], nested: { level: 1 } } },
        { name: "retries", kind: "literal", value: 2 },
        { name: "pool", kind: "literal", value: "notifications" },
        {
          name: "retry_delay",
          kind: "literal",
          value: 30,
          value_schema: { type: "number", format: "duration-seconds" },
        },
      ],
    });
  });

  it("writes the module that implements the operator, when the binding names one", () => {
    const implemented = defineProviderOperator<{ path: string }>({
      module: "airflow.providers.demo.operators.file",
      className: "ReadOperator",
      implementationModule: "@demo/providers-demo/operators/file",
      templateFields: [],
      params: { path: { name: "path", required: true } },
      execute: () => undefined,
    });
    const dag = new Dag("d", { queue: "typescript" });
    dag.task("read", implemented({ path: "p" }))();
    dag.task("notify", Notify({ subject: "s" }))();

    const tasks = serializeTasks(dag);

    expect(tasks.get("read")).toMatchObject({
      is_stub: true,
      stub_kind: "provider_operator",
      _operator_module: "@demo/providers-demo/operators/file",
    });
    expect(tasks.get("notify")).not.toHaveProperty("_operator_module");
  });

  it("writes the arguments in the operator's parameter order, whatever order they were given in", () => {
    const dag = new Dag("d");
    dag.task("notify", Notify({ extra: { a: 1 }, channel: "ops", subject: "s" }))();

    expect(serializeTasks(dag).get("notify")!["_operator_kwargs"]).toEqual([
      { name: "subject", kind: "literal", value: "s" },
      { name: "channel", kind: "literal", value: "ops" },
      { name: "extra", kind: "literal", value: { a: 1 } },
    ]);
  });

  it("writes an unset optional argument nowhere and a null one as a null literal", () => {
    const dag = new Dag("d");
    dag.task("notify", Notify({ subject: "s", responseTimeout: null, notBefore: null }))();

    expect(serializeTasks(dag).get("notify")!["_operator_kwargs"]).toEqual([
      { name: "subject", kind: "literal", value: "s" },
      { name: "response_timeout", kind: "literal", value: null },
      { name: "not_before", kind: "literal", value: null },
    ]);
  });

  it("writes a task option that is a date as an ISO 8601 string", () => {
    const dag = new Dag("d");
    dag.task("notify", Notify({ subject: "s" }), {
      startDate: new Date("2026-03-04T05:06:07Z"),
      maxRetryDelay: 600,
    })();

    expect(serializeTasks(dag).get("notify")!["_operator_kwargs"]).toEqual([
      { name: "subject", kind: "literal", value: "s" },
      {
        name: "start_date",
        kind: "literal",
        value: "2026-03-04T05:06:07.000Z",
        value_schema: { type: "string", format: "date-time" },
      },
      {
        name: "max_retry_delay",
        kind: "literal",
        value: 600,
        value_schema: { type: "number", format: "duration-seconds" },
      },
    ]);
  });

  it("gives a provider task the Dag's queue like any other task, unless it names its own", () => {
    const dag = new Dag("d", { queue: "typescript" });
    dag.task("native", async () => 1)();
    dag.task("python", Notify({ subject: "s" }))();
    dag.task("routed", Notify({ subject: "s" }), { queue: "python-gpu" })();

    const tasks = serializeTasks(dag);
    expect(tasks.get("native")!["queue"]).toBe("typescript");
    expect(tasks.get("python")!["queue"]).toBe("typescript");
    expect(tasks.get("python")!["is_stub"]).toBe(true);
    expect(tasks.get("routed")!["queue"]).toBe("python-gpu");
    expect(tasks.get("routed")!["_operator_kwargs"]).toContainEqual({
      name: "queue",
      kind: "literal",
      value: "python-gpu",
    });
  });

  it("marks a task that a TypeScript handler runs as a Dag definition and a provider task as a provider operator", () => {
    const dag = new Dag("d");
    dag.task("native", async () => 1)();
    dag.task("python", Notify({ subject: "s" }))();

    const tasks = serializeTasks(dag);
    expect(tasks.get("native")!["stub_kind"]).toBe("dag_definition");
    expect(tasks.get("python")!["stub_kind"]).toBe("provider_operator");
  });

  it("writes what the operator class fixes: sensor, skip, colours, templates and display name", () => {
    const dag = new Dag("d");
    dag.task("wait", WaitSensor({ path: "/tmp/x" }))();

    expect(serializeTasks(dag).get("wait")).toMatchObject({
      task_type: "WaitSensor",
      _operator_name: "Wait",
      _is_sensor: true,
      _can_skip_downstream: true,
      template_ext: [".txt"],
      template_fields_renderers: { path: "bash" },
      ui_color: "#e6f1f2",
      ui_fgcolor: "#111",
    });
  });

  it("writes a task's result given as a whole argument as an xcom binding and an upstream edge", () => {
    const dag = new Dag("d");
    const extracted = dag.task("extract", async () => "subject")();
    const notified = dag.task("notify", Notify({ subject: extracted }))();
    dag.task("report", async ({ sent }: { sent: unknown }) => sent)({ sent: notified });

    const tasks = serializeTasks(dag);
    expect(tasks.get("extract")!["downstream_task_ids"]).toEqual(["notify"]);
    expect(tasks.get("notify")!["_operator_kwargs"]).toEqual([
      { name: "subject", kind: "xcom", task_id: "extract" },
    ]);
    // The provider task's own result feeds a TypeScript task the usual way.
    expect(tasks.get("report")!["_arg_bindings"]).toEqual([
      { name: "sent", kind: "xcom", task_id: "notify" },
    ]);
  });

  it.each([
    ["an array", (ref: TaskRef) => ({ recipients: ["ops@example.com", ref] as never })],
    ["an object", (ref: TaskRef) => ({ recipients: { first: ref } as never })],
  ])("rejects a task's result inside %s when the Dag is serialized", (_, nest) => {
    const dag = new Dag("d");
    const listed = dag.task("list", async () => ["a@example.com"])();
    dag.task("notify", Notify({ subject: "s", ...nest(listed) }))();

    expect(() => serializeTasks(dag)).toThrow(
      'Argument "recipients" of task "notify" of Dag "d" holds the result of task "list" inside ' +
        "an array or an object; only a whole argument can take a task's result",
    );
  });

  it("copies its arguments, so a later change to them does not change the Dag", () => {
    const extra = { tags: ["a"] };
    const dag = new Dag("d");
    dag.task("notify", Notify({ subject: "s", extra }))();
    extra.tags.push("b");

    expect(serializeTasks(dag).get("notify")!["_operator_kwargs"]).toContainEqual({
      name: "extra",
      kind: "literal",
      value: { tags: ["a"] },
    });
  });

  it.each([
    [() => Notify({} as NotifyArgs), 'NotifyOperator(...) needs the argument "subject"'],
    [
      () => Notify({ subject: "s", channelName: "ops" } as NotifyArgs),
      'Unknown argument "channelName" for NotifyOperator(...)',
    ],
    [() => Notify("s" as unknown as NotifyArgs), "takes an object of its arguments"],
    [
      () => Notify({ subject: "s", responseTimeout: "5m" as unknown as number }),
      'Argument "responseTimeout" of NotifyOperator(...) is a duration in seconds, not a string',
    ],
    [
      () => Notify({ subject: "s", notBefore: "2026-01-01" as unknown as Date }),
      'Argument "notBefore" of NotifyOperator(...) is a valid Date, not a string',
    ],
    [
      () => Notify({ subject: "s", extra: { when: new Date(0) } }),
      'Argument "extra" of NotifyOperator(...) holds a Date, which JSON cannot carry',
    ],
  ])("rejects arguments the operator cannot take", (build, message) => {
    expect(build).toThrow(message);
  });

  it("rejects a task's result for an argument the worker does not render", () => {
    const dag = new Dag("d");
    const extracted = dag.task("extract", async () => "ops")() as TaskRef;

    expect(() => Notify({ subject: "s", channel: extracted as unknown as string })).toThrow(
      'Argument "channel" of NotifyOperator(...) was given the result of task "extract", but it ' +
        "is not a templated argument",
    );
  });

  it("needs a task id and takes no inputs when placed", () => {
    const dag = new Dag("d");

    expect(() => dag.task(Notify({ subject: "s" }), {})).toThrow(
      'A NotifyOperator task of Dag "d" has no taskId; name it with ' +
        'dag.task("my_task", NotifyOperator({ ... }))',
    );
    const place = dag.task("notify", Notify({ subject: "s" })) as (inputs?: object) => TaskRef;
    expect(() => place({ subject: "other" })).toThrow("which takes no inputs");
  });

  it("fails rather than runs when its queue routes it to the Node coordinator", async () => {
    const operator = Notify({ subject: "s" });
    const failed = { type: "TaskState", state: "failed" };
    const messages: string[] = [];
    const op = {
      ctx: { taskId: "notify" },
      fail: (message: string) => {
        messages.push(message);
        return failed;
      },
    } as unknown as OperatorContext;

    await expect(operator.execute(op)).resolves.toBe(failed);
    expect(messages).toEqual([
      'Task "notify" is a NotifyOperator, which only a Python worker can run; give it a queue ' +
        "that is not routed to the Node coordinator",
    ]);
  });
});
