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

import { describe, expect, it, vi } from "vitest";

import { resolveArgBindings, type ArgBindingDeps } from "../../src/coordinator/arg-binding.js";
import type { XComEntry } from "../../src/coordinator/client.js";
import type { ArgBindings } from "../../src/generated/supervisor.js";
import type { GetXComOpts, JsonValue } from "../../src/sdk/client-types.js";

/** A spec no generated type describes, as a newer or malformed Airflow could send. */
function unknownSpec(binding: Record<string, unknown>): ArgBindings {
  return [binding] as unknown as ArgBindings;
}

/** Records what was pulled, answering each upstream task from `stored`. */
function xcomClient(stored: Record<string, XComEntry>): {
  deps: ArgBindingDeps;
  pulls: GetXComOpts[];
} {
  const pulls: GetXComOpts[] = [];
  return {
    pulls,
    deps: {
      signal: new AbortController().signal,
      client: {
        getXComEntry: async (opts) => {
          pulls.push(opts);
          return stored[opts.taskId ?? ""] ?? { found: false, value: null };
        },
      },
    },
  };
}

function found(value: JsonValue): XComEntry {
  return { found: true, value };
}

/** Deps whose pulls all block until `expected` of them are in flight, so a
 *  resolution that pulls one at a time never finishes. */
function concurrentClient(expected: number): { deps: ArgBindingDeps; peak: () => number } {
  let inFlight = 0;
  let peak = 0;
  let releaseAll = (): void => {};
  const allStarted = new Promise<void>((resolve) => (releaseAll = resolve));
  return {
    peak: () => peak,
    deps: {
      signal: new AbortController().signal,
      client: {
        getXComEntry: async (opts) => {
          inFlight += 1;
          peak = Math.max(peak, inFlight);
          if (inFlight === expected) releaseAll();
          await allStarted;
          inFlight -= 1;
          return found(`from ${opts.taskId}`);
        },
      },
    },
  };
}

/** Deps that never answer, aborting the signal once the first pull is in flight. */
function abortingClient(reason?: unknown): { deps: ArgBindingDeps; pullCount: () => number } {
  const controller = new AbortController();
  let pullCount = 0;
  return {
    pullCount: () => pullCount,
    deps: {
      signal: controller.signal,
      client: {
        getXComEntry: () => {
          pullCount += 1;
          setTimeout(() => controller.abort(reason), 0);
          return new Promise<XComEntry>(() => {});
        },
      },
    },
  };
}

const NO_PULLS: ArgBindingDeps = {
  signal: new AbortController().signal,
  client: {
    getXComEntry: () => {
      throw new Error("resolution pulled an XCom it should not have");
    },
  },
};

describe("resolveArgBindings", () => {
  it.each<[string, ArgBindings | undefined]>([
    ["absent, as on an Airflow too old to send one", undefined],
    ["null, as for a task called with no arguments", null],
  ])("binds nothing when the spec is %s", async (_case, bindings) => {
    await expect(resolveArgBindings(bindings, NO_PULLS)).resolves.toEqual({
      names: null,
      values: {},
    });
  });

  it("reports a spec that bound nothing as empty, not as absent", async () => {
    await expect(resolveArgBindings([], NO_PULLS)).resolves.toEqual({ names: [], values: {} });
  });

  it("binds literals under their names, keeping declaration order", async () => {
    const bound = await resolveArgBindings(
      [
        { name: "country", kind: "literal", value: "uk", value_schema: { type: "string" } },
        { name: "threshold", kind: "literal", value: 0.5 },
        { name: "retries_num", kind: "literal", value: 3, from_default: true },
      ],
      NO_PULLS,
    );

    expect(bound.names).toEqual(["country", "threshold", "retries_num"]);
    expect(bound.values).toEqual({ country: "uk", threshold: 0.5, retries_num: 3 });
  });

  it.each<[string, unknown]>([
    ["a string", "uk"],
    ["a number", 3],
    ["false", false],
    ["null", null],
    ["an array", [1, "two", null]],
    ["an object", { nested: { ok: true } }],
  ])("binds %s literal unchanged", async (_case, value) => {
    const bound = await resolveArgBindings([{ name: "arg", kind: "literal", value }], NO_PULLS);
    expect(bound.values).toEqual({ arg: value });
  });

  it("binds a literal Airflow sent without a value as null", async () => {
    const bound = await resolveArgBindings([{ name: "arg", kind: "literal" }], NO_PULLS);
    expect(bound.values).toEqual({ arg: null });
  });

  it("binds an argument named __proto__ as an own key, leaving the prototype alone", async () => {
    const { values } = await resolveArgBindings(
      [{ name: "__proto__", kind: "literal", value: "polluted" }],
      NO_PULLS,
    );

    expect(Object.getOwnPropertyDescriptor(values, "__proto__")?.value).toBe("polluted");
    expect(Object.getPrototypeOf(values)).toBe(Object.prototype);
  });

  it.each<[string, Record<string, unknown>, string]>([
    ["a kind from a newer Airflow", { name: "arg", kind: "asset" }, '"asset"'],
    ["a binding with no kind at all", { name: "arg" }, "undefined"],
  ])("refuses %s rather than leaving the argument unbound", async (_case, binding, rendered) => {
    await expect(resolveArgBindings(unknownSpec(binding), NO_PULLS)).rejects.toThrowError(
      `Task argument "arg" has binding kind ${rendered}, which this version of the TypeScript ` +
        "SDK cannot bind; upgrade @apache-airflow/ts-sdk to match this Airflow release",
    );
  });

  it("refuses a name bound twice", async () => {
    await expect(
      resolveArgBindings(
        [
          { name: "country", kind: "literal", value: "uk" },
          { name: "country", kind: "literal", value: "de" },
        ],
        NO_PULLS,
      ),
    ).rejects.toThrowError(
      'Task argument "country" is bound more than once by the call to this task',
    );
  });

  it.each(["ctx", "client"])("refuses an argument named %s, which the SDK owns", async (name) => {
    await expect(
      resolveArgBindings([{ name, kind: "literal", value: "shadowed" }], NO_PULLS),
    ).rejects.toThrowError(
      `Task argument "${name}" collides with an argument the TypeScript SDK passes to every ` +
        "handler (ctx, client); rename the parameter in the @task.stub signature",
    );
  });

  it("pulls no upstream output for a spec it refuses", async () => {
    const { deps, pulls } = xcomClient({ fn_extract: found("never read") });

    await expect(
      resolveArgBindings(
        [
          { name: "extracted", kind: "xcom", task_id: "fn_extract" },
          { name: "ctx", kind: "literal", value: "shadowed" },
        ],
        deps,
      ),
    ).rejects.toThrowError(/collides with an argument/);
    expect(pulls).toEqual([]);
  });
});

describe("resolveArgBindings with upstream outputs", () => {
  it("binds an upstream task's return value under the argument's name", async () => {
    const { deps, pulls } = xcomClient({ fn_extract: found({ rows: [1, 2] }) });

    const bound = await resolveArgBindings(
      [{ name: "extracted", kind: "xcom", task_id: "fn_extract" }],
      deps,
    );

    expect(bound).toEqual({ names: ["extracted"], values: { extracted: { rows: [1, 2] } } });
    expect(pulls).toEqual([{ key: "return_value", taskId: "fn_extract" }]);
  });

  it("keeps declaration order when literals and upstream outputs are interleaved", async () => {
    const { deps } = xcomClient({
      fn_extract: found("extracted"),
      fn_enrich: found("enriched"),
    });

    const bound = await resolveArgBindings(
      [
        { name: "country", kind: "literal", value: "uk" },
        { name: "extracted", kind: "xcom", task_id: "fn_extract" },
        { name: "threshold", kind: "literal", value: 0.5 },
        { name: "enriched", kind: "xcom", task_id: "fn_enrich" },
      ],
      deps,
    );

    expect(bound.names).toEqual(["country", "extracted", "threshold", "enriched"]);
    expect(Object.keys(bound.values)).toEqual(["country", "extracted", "threshold", "enriched"]);
    expect(bound.values).toEqual({
      country: "uk",
      extracted: "extracted",
      threshold: 0.5,
      enriched: "enriched",
    });
  });

  it("pulls every upstream output at once rather than one after another", async () => {
    const { deps, peak } = concurrentClient(4);

    // A serial resolution never gets past the first pull: each one blocks until
    // all four are in flight, so this test times out rather than fails.
    const bound = await resolveArgBindings(
      [
        { name: "a", kind: "xcom", task_id: "fn_a" },
        { name: "b", kind: "xcom", task_id: "fn_b" },
        { name: "c", kind: "xcom", task_id: "fn_c" },
        { name: "d", kind: "xcom", task_id: "fn_d" },
      ],
      deps,
    );

    expect(peak()).toBe(4);
    expect(bound.values).toEqual({
      a: "from fn_a",
      b: "from fn_b",
      c: "from fn_c",
      d: "from fn_d",
    });
  });

  it("binds an upstream output stored as null, which is a value and not an absence", async () => {
    const { deps } = xcomClient({ fn_extract: found(null) });

    const bound = await resolveArgBindings(
      [{ name: "extracted", kind: "xcom", task_id: "fn_extract" }],
      deps,
    );

    expect(bound.values).toEqual({ extracted: null });
  });

  it("fails the task when the upstream task pushed no output, naming both", async () => {
    const { deps } = xcomClient({});

    await expect(
      resolveArgBindings([{ name: "extracted", kind: "xcom", task_id: "fn_extract" }], deps),
    ).rejects.toThrowError(
      'Task argument "extracted" takes the output of upstream task "fn_extract", which pushed no ' +
        "return_value XCom; a task that returns nothing pushes none",
    );
  });
});

describe("resolveArgBindings precision guard", () => {
  const INT64_SCHEMA = { type: "integer", format: "int64" };

  it("refuses an upstream int64 beyond what a JavaScript number holds exactly", async () => {
    const { deps } = xcomClient({ fn_count: found(2 ** 63) });

    await expect(
      resolveArgBindings(
        [{ name: "total", kind: "xcom", task_id: "fn_count", value_schema: INT64_SCHEMA }],
        deps,
      ),
    ).rejects.toThrowError(
      `Task argument "total" is a 64-bit integer of ${2 ** 63}, beyond the ±9007199254740991 a ` +
        "JavaScript number holds exactly, so its low digits are already lost; carry it across the " +
        "language boundary as a string instead",
    );
  });

  it("refuses a literal int64 the Dag passed beyond that range", async () => {
    await expect(
      resolveArgBindings(
        [{ name: "total", kind: "literal", value: 2 ** 63, value_schema: INT64_SCHEMA }],
        NO_PULLS,
      ),
    ).rejects.toThrowError(/is a 64-bit integer of/);
  });

  it.each<[string, JsonValue, Record<string, unknown> | undefined]>([
    ["an int64 inside the exact range", Number.MAX_SAFE_INTEGER, { format: "int64" }],
    ["a huge number the Dag did not declare as an int", 2 ** 63, { type: "number" }],
    ["a huge number with no schema at all", 2 ** 63, undefined],
    ["a non-numeric value under an int64 schema", "9223372036854775807", { format: "int64" }],
  ])("binds %s", async (_case, value, value_schema) => {
    const bound = await resolveArgBindings(
      [{ name: "total", kind: "literal", value, value_schema }],
      NO_PULLS,
    );

    expect(bound.values).toEqual({ total: value });
  });
});

describe("resolveArgBindings abort handling", () => {
  it("stops waiting for an upstream pull when the task is terminated", async () => {
    const { deps, pullCount } = abortingClient(new Error("Task aborted by SIGTERM"));

    await expect(
      resolveArgBindings([{ name: "extracted", kind: "xcom", task_id: "fn_extract" }], deps),
    ).rejects.toThrowError(
      "Aborted while resolving this task's arguments from its upstream tasks: Task aborted by SIGTERM",
    );
    expect(pullCount()).toBe(1);
  });

  it.each<[string, unknown, RegExp]>([
    ["no reason at all", undefined, /This operation was aborted/],
    ["a reason that is not an error", "worker shutting down", /worker shutting down/],
  ])("reports an abort with %s", async (_case, reason, expected) => {
    const { deps } = abortingClient(reason);

    await expect(
      resolveArgBindings([{ name: "extracted", kind: "xcom", task_id: "fn_extract" }], deps),
    ).rejects.toThrowError(expected);
  });

  it("issues no pull at all when the task was already terminated", async () => {
    const { deps, pullCount } = abortingClient();
    const controller = new AbortController();
    controller.abort(new Error("Task aborted by SIGTERM"));

    await expect(
      resolveArgBindings([{ name: "extracted", kind: "xcom", task_id: "fn_extract" }], {
        ...deps,
        signal: controller.signal,
      }),
    ).rejects.toThrowError(/Aborted while resolving this task's arguments/);
    expect(pullCount()).toBe(0);
  });

  it("leaves no abort listener behind once resolution finishes", async () => {
    const { deps } = xcomClient({ fn_extract: found("extracted") });
    const controller = new AbortController();
    const removeListener = vi.spyOn(controller.signal, "removeEventListener");

    await resolveArgBindings([{ name: "extracted", kind: "xcom", task_id: "fn_extract" }], {
      ...deps,
      signal: controller.signal,
    });

    // The handler still runs on this signal; a listener left behind would reject
    // a promise nobody is awaiting any more.
    expect(removeListener).toHaveBeenCalledWith("abort", expect.any(Function));
  });

  it("ignores the signal for a spec with nothing to pull", async () => {
    const controller = new AbortController();
    controller.abort(new Error("Task aborted by SIGTERM"));

    const bound = await resolveArgBindings([{ name: "country", kind: "literal", value: "uk" }], {
      ...NO_PULLS,
      signal: controller.signal,
    });

    expect(bound.values).toEqual({ country: "uk" });
  });
});
