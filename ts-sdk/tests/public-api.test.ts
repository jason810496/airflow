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

import { afterEach, describe, expect, expectTypeOf, it, vi } from "vitest";
import { AIRFLOW_METADATA_FLAG } from "../src/coordinator/manifest.js";
import type {
  ConnectionResult,
  DagSpec,
  GetXComOpts,
  SetXComOpts,
  TaskClient,
  TaskContext,
  TaskFactory,
  TaskHandler,
  TaskHandlerArgs,
  TaskInputs,
  TaskOptions,
  TaskRef,
  TaskSpec,
} from "../src/index.js";
import * as sdk from "../src/index.js";
import {
  ConnectionNotFoundError,
  Dag,
  DagRegistry,
  serveDags,
  SUPERVISOR_API_VERSION,
  VariableNotFoundError,
} from "../src/index.js";

/** An `interface`, which unlike a type literal has no implicit index signature,
 *  so wiring it as a literal only works if `TaskInputs` maps it structurally. */
interface ExtractedRows {
  rowCount: number;
}

describe("public API", () => {
  it("exports the Dag authoring surface", async () => {
    const dag = new Dag("public_api_dag");
    const extract = dag.task("public_api_task", async () => ({ rows: 12 }));
    const load = dag.task(
      "public_api_downstream",
      async ({ extracted, regionCode }: { extracted: TaskRef; regionCode: string }) => {
        void extracted;
        void regionCode;
      },
    );
    const extracted = extract();
    expect(load({ extracted, regionCode: "us" })).toEqual({
      dagId: "public_api_dag",
      taskId: "public_api_downstream",
    });
    expect(extracted).toEqual({ dagId: "public_api_dag", taskId: "public_api_task" });
    expect(dag.taskIds).toEqual(["public_api_task", "public_api_downstream"]);
    // serveDags hands the registry to the runtime, which needs the supervisor's
    // socket addresses that Airflow puts on argv.
    await expect(serveDags(new DagRegistry(dag))).rejects.toThrow("Missing --comm");
  });

  // The registry guard runs before the already-served latch, so this holds
  // regardless of whether another test in this file already served a registry.
  it.each([
    ["a bare Dag", new Dag("not_a_registry_dag")],
    ["a plain object", { register: () => {} }],
    ["null", null],
  ])("rejects a %s in place of a registry", async (_label, value) => {
    await expect(serveDags(value as unknown as DagRegistry)).rejects.toThrow(
      /serveDags\(\.\.\.\) takes a DagRegistry/,
    );
  });

  it("names the duplicate-copy cause for a registry built by another copy", async () => {
    // Stands in for a registry from a second resolved copy: same brand, other
    // class. It still cannot be served, so the point is only that it says why.
    const foreign = {};
    Object.defineProperty(foreign, Symbol.for("airflow.ts-sdk.DagRegistry"), { value: true });
    await expect(serveDags(foreign as unknown as DagRegistry)).rejects.toThrow(
      /different copy of @apache-airflow\/ts-sdk/,
    );
  });

  describe("the one-shot serve latch", () => {
    // Global by design, so it outlives the test that trips it.
    afterEach(() => {
      delete (globalThis as unknown as Record<symbol, unknown>)[
        Symbol.for("airflow.ts-sdk.served")
      ];
    });

    it("rejects a second call once a serve has completed", async () => {
      const argv = process.argv;
      // The one path that completes without sockets.
      process.argv = [...argv, AIRFLOW_METADATA_FLAG];
      vi.spyOn(process.stdout, "write").mockReturnValue(true);
      try {
        await serveDags(new DagRegistry(new Dag("served_dag")));
        await expect(serveDags(new DagRegistry(new Dag("second_call_dag")))).rejects.toThrow(
          /serveDags\(\.\.\.\) was already called/,
        );
      } finally {
        process.argv = argv;
        vi.restoreAllMocks();
      }
    });

    it("releases the latch when a serve fails, so the call can be retried", async () => {
      await expect(serveDags(new DagRegistry(new Dag("first_try")))).rejects.toThrow(
        "Missing --comm",
      );
      // The retry reports why it actually failed, not "already called".
      await expect(serveDags(new DagRegistry(new Dag("second_try")))).rejects.toThrow(
        "Missing --comm",
      );
    });
  });

  it("exports DagRegistry as the Dag collection a bundle serves", () => {
    const dag = new Dag("registry_api_dag");
    const handler = async () => "hello";
    dag.task("extract", handler);
    // Building a registry starts nothing, so a test can dispatch through it
    // exactly as the runtime does, with no sockets in scope.
    const registry = new DagRegistry(dag);
    expect(registry.getTaskHandler("registry_api_dag", "extract")).toBe(handler);
    registry.register(new Dag("late_dag"));
    expect(registry.getTaskHandler("late_dag", "extract")).toBeUndefined();
  });

  it("keeps registry enumeration out of the public surface", () => {
    const registry = new DagRegistry();
    for (const name of ["listTasks", "listDags"]) {
      expect(name in registry).toBe(false);
    }
    expectTypeOf<keyof DagRegistry>().toEqualTypeOf<"register" | "getTaskHandler">();
  });

  it("does not export the removed registerTask surface or the coordinator itself", () => {
    for (const name of [
      "registerTask",
      "listRegisteredTasks",
      "registerDags",
      "defaultRegistry",
      "startCoordinator",
    ]) {
      expect(name in sdk).toBe(false);
    }
    expectTypeOf<typeof sdk>().not.toHaveProperty("registerTask");
    expectTypeOf<typeof sdk>().not.toHaveProperty("listRegisteredTasks");
    expectTypeOf<typeof sdk>().not.toHaveProperty("registerDags");
    // The runtime reads the registry it is handed, so there is no process-wide
    // registry for a Dag constructor to write into.
    expectTypeOf<typeof sdk>().not.toHaveProperty("defaultRegistry");
    expectTypeOf<typeof sdk>().not.toHaveProperty("startCoordinator");
  });

  it("exports public error classes", () => {
    const err = new VariableNotFoundError("missing");
    expect(err).toBeInstanceOf(Error);
    expect(err.name).toBe("VariableNotFoundError");
    expect(err.key).toBe("missing");

    const connErr = new ConnectionNotFoundError("missing_conn");
    expect(connErr).toBeInstanceOf(Error);
    expect(connErr.name).toBe("ConnectionNotFoundError");
    expect(connErr.connId).toBe("missing_conn");
  });

  it("reaches the runtime only through serveDags, which takes a registry", () => {
    expectTypeOf<typeof serveDags>().toEqualTypeOf<(registry: DagRegistry) => Promise<void>>();
    expectTypeOf<ConstructorParameters<typeof DagRegistry>>().toEqualTypeOf<Dag[]>();
    expectTypeOf(SUPERVISOR_API_VERSION).toMatchTypeOf<string>();
  });

  it("keeps the Dag authoring signatures extensible via trailing specs", () => {
    expectTypeOf<TaskRef>().toEqualTypeOf<{
      readonly dagId: string;
      readonly taskId: string;
    }>();
    // A handler that takes only the runtime's own arguments is called with none;
    // every other argument it declares has to be supplied, as an upstream
    // reference or as a literal of that argument's own type.
    expectTypeOf<TaskFactory<TaskHandlerArgs>>().toEqualTypeOf<() => TaskRef>();
    expectTypeOf<TaskInputs<{ rows: number } & TaskHandlerArgs>>().toEqualTypeOf<{
      rows: TaskRef | number;
    }>();
    // An optional argument stays optional, and an argument typed by an
    // interface can still be given a literal.
    expectTypeOf<TaskInputs<{ rows?: number } & TaskHandlerArgs>>().toEqualTypeOf<{
      rows?: TaskRef | number;
    }>();
    expectTypeOf<TaskInputs<{ extracted: ExtractedRows } & TaskHandlerArgs>>().toEqualTypeOf<{
      extracted: TaskRef | { rowCount: number };
    }>();
    // A value JSON cannot carry can only come from an upstream task.
    expectTypeOf<TaskInputs<{ at: Date } & TaskHandlerArgs>>().toEqualTypeOf<{ at: TaskRef }>();
    expectTypeOf<TaskFactory<{ rows: number } & TaskHandlerArgs>>().toEqualTypeOf<
      (inputs: { rows: TaskRef | number }) => TaskRef
    >();
    expectTypeOf<TaskOptions>().toEqualTypeOf<{
      readonly spec?: TaskSpec;
    }>();
    expectTypeOf<ConstructorParameters<typeof Dag>>().toEqualTypeOf<[string, DagSpec?]>();
    expectTypeOf<Dag["task"]>().toEqualTypeOf<
      <TArgs extends object = TaskHandlerArgs, TReturn = unknown>(
        taskId: string,
        handler: TaskHandler<TReturn, TArgs>,
        options?: TaskOptions,
      ) => TaskFactory<TArgs>
    >();
    expectTypeOf<Dag["taskIds"]>().toEqualTypeOf<readonly string[]>();
    // Both specs are all-optional, so `{}` stays assignable and a field the
    // schema gains later cannot break a call site.
    const emptyDagSpec: DagSpec = {};
    const emptyTaskSpec: TaskSpec = {};
    expect([emptyDagSpec, emptyTaskSpec]).toEqual([{}, {}]);
    // The one Dag-level field that is not part of the serialized Dag; every
    // other field of either spec comes from the schema.
    expectTypeOf<DagSpec["isMixedLanguageDag"]>().toEqualTypeOf<boolean | undefined>();
    expectTypeOf<DagSpec["schedule"]>().toEqualTypeOf<string | undefined>();
    expectTypeOf<DagSpec["tags"]>().toEqualTypeOf<readonly string[] | undefined>();
    expectTypeOf<DagSpec["startDate"]>().toEqualTypeOf<Date | undefined>();
    expectTypeOf<TaskSpec["retries"]>().toEqualTypeOf<number | undefined>();
    // A timedelta is seconds, not a Date, and a bool defaulting to true is
    // still a plain optional: `undefined` already means "unset".
    expectTypeOf<TaskSpec["retryDelay"]>().toEqualTypeOf<number | undefined>();
    expectTypeOf<TaskSpec["doXcomPush"]>().toEqualTypeOf<boolean | undefined>();
    // Identity is positional, so it is not restated in either spec.
    expectTypeOf<DagSpec>().not.toHaveProperty("dagId");
    expectTypeOf<TaskSpec>().not.toHaveProperty("taskId");
  });

  it("uses idiomatic TypeScript names for public client types", () => {
    expectTypeOf<TaskContext>().toEqualTypeOf<{
      readonly dagId: string;
      readonly taskId: string;
      readonly runId: string;
      readonly tryNumber: number;
      readonly mapIndex: number;
      readonly signal: AbortSignal;
    }>();
    expectTypeOf<GetXComOpts>().toEqualTypeOf<{
      key: string;
      dagId?: string;
      runId?: string;
      taskId?: string;
      mapIndex?: number | null;
      includePriorDates?: boolean;
    }>();
    expectTypeOf<SetXComOpts>().toEqualTypeOf<{
      key: string;
      value: SetXComOpts["value"];
      dagId?: string;
      runId?: string;
      taskId?: string;
      mapIndex?: number | null;
    }>();
    expectTypeOf<ConnectionResult>().toEqualTypeOf<{
      id: string;
      type: string;
      host?: string | null;
      schema?: string | null;
      login?: string | null;
      password?: string | null;
      port?: number | null;
      extra?: string | null;
    }>();
    expectTypeOf<TaskClient["getConnection"]>().toEqualTypeOf<
      (connId: string) => Promise<ConnectionResult | null>
    >();
    expectTypeOf<TaskClient["getConnectionOrThrow"]>().toEqualTypeOf<
      (connId: string) => Promise<ConnectionResult>
    >();
    expectTypeOf<TaskClient["getXCom"]>().toEqualTypeOf<
      <T = unknown>(opts: GetXComOpts) => Promise<T | null>
    >();
  });

  it("rejects wire-format names and non-JSON XCom values", () => {
    function acceptsGetXComOpts(_opts: GetXComOpts): void {}
    function acceptsSetXComOpts(_opts: SetXComOpts): void {}

    acceptsGetXComOpts({
      key: "result",
      dagId: "example",
      runId: "manual__2026-01-01T00:00:00+00:00",
      taskId: "extract",
      mapIndex: 0,
      includePriorDates: true,
    });
    acceptsSetXComOpts({
      key: "result",
      value: { count: 1 },
      dagId: "example",
      runId: "manual__2026-01-01T00:00:00+00:00",
      taskId: "extract",
      mapIndex: null,
    });

    // @ts-expect-error public options use dagId, not dag_id.
    acceptsGetXComOpts({ key: "result", dag_id: "example" });
    // @ts-expect-error public options use includePriorDates, not include_prior_dates.
    acceptsGetXComOpts({ key: "result", include_prior_dates: true });
    // @ts-expect-error public ConnectionResult uses id/type, not wire-format names.
    expectTypeOf<ConnectionResult>().toEqualTypeOf<{ conn_id: string; conn_type: string }>();
    // @ts-expect-error public ConnectionResult uses id/type, not connId/connType.
    expectTypeOf<ConnectionResult>().toEqualTypeOf<{ connId: string; connType: string }>();
    // @ts-expect-error public TaskContext does not expose the raw task-instance id.
    expectTypeOf<TaskContext>().toHaveProperty("taskInstanceId");
    // Never invoked: these constructor/method misuses also throw at runtime.
    const rejectsPositionalMisuse = () => {
      // @ts-expect-error dagId is positional, not an options object.
      new Dag({ dagId: "example" });
      // @ts-expect-error a task handler is required.
      new Dag("example").task("extract");
      const dag = new Dag("example");
      const extract = dag.task("extract", async () => undefined);
      // @ts-expect-error spec is keyword-only, not positional.
      dag.task("transform2", async () => undefined, { extract });
      // @ts-expect-error a Dag spec is an options object, not a primitive.
      new Dag("spec_dag", 42);
      new Dag("scheduled_dag", { schedule: "@daily", tags: ["team-a"] });
      new Dag("python_dag", { isMixedLanguageDag: true });
      dag.task("transform3", async () => undefined, { spec: { retries: 2 } });
      // @ts-expect-error specs use the camelCased field name, not the schema key.
      new Dag("snake_case_dag", { dag_display_name: "Example" });
      // @ts-expect-error a field the schema does not define is a typo.
      new Dag("typo_dag", { scheduled: "@daily" });
      // @ts-expect-error a timedelta field is seconds, not a Date.
      dag.task("transform4", async () => undefined, { spec: { retryDelay: new Date() } });
      // @ts-expect-error the reference a task call returns is data, not callable.
      extract()();
      // @ts-expect-error serveDags takes the registry, not a bare Dag.
      serveDags(dag);
      // @ts-expect-error a registry is built from Dags, not from task factories.
      new DagRegistry(extract);
    };
    void rejectsPositionalMisuse;

    // The wiring the compiler has to reject: an argument the task declares is
    // missing, is of the wrong type, or is not one of its arguments at all.
    const rejectsMiswiredTasks = () => {
      const dag = new Dag("wiring_example");
      const extract = dag.task("extract", async () => ({ rows: 12 }));
      const transform = dag.task(
        "transform",
        async (_args: { extracted: TaskRef; regionCode: string } & TaskHandlerArgs) => undefined,
      );
      transform({ extracted: extract(), regionCode: "us" });
      const interfaceArgs = dag.task(
        "interface_args",
        async (_args: { extracted: ExtractedRows; at?: string } & TaskHandlerArgs) => undefined,
      );
      // An interface-typed argument takes a literal, and an optional one may be
      // left out entirely.
      interfaceArgs({ extracted: { rowCount: 1 } });
      // @ts-expect-error every declared argument has to be supplied.
      transform({ extracted: extract() });
      // @ts-expect-error a literal has to match the argument's own type.
      transform({ extracted: extract(), regionCode: 7 });
      // @ts-expect-error an argument the task does not declare is a typo.
      transform({ extracted: extract(), regionCode: "us", region: "us" });
      // @ts-expect-error a task that declares no arguments is called with none.
      extract({ regionCode: "us" });
      // @ts-expect-error an upstream reference cannot be replaced by any value.
      transform({ extracted: "extract", regionCode: "us" });
      const withDate = dag.task(
        "with_date",
        async (_args: { at: Date } & TaskHandlerArgs) => undefined,
      );
      withDate({ at: extract() });
      // @ts-expect-error a value JSON cannot carry has to come from a task.
      withDate({ at: new Date() });
    };
    void rejectsMiswiredTasks;
    // @ts-expect-error the TaskRef handle is opaque and does not expose the handler.
    expectTypeOf<TaskRef>().toHaveProperty("handler");
    // @ts-expect-error XCom values must be JSON-compatible.
    acceptsSetXComOpts({ key: "result", value: new Date() });
  });
});
