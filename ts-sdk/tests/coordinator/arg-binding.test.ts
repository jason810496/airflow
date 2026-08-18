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

import { decodeArgBindings } from "../../src/coordinator/arg-binding.js";
import type { ArgBindings } from "../../src/generated/supervisor.js";

/** A spec no generated type describes, as a newer or malformed Airflow could send. */
function unknownSpec(binding: Record<string, unknown>): ArgBindings {
  return [binding] as unknown as ArgBindings;
}

describe("decodeArgBindings", () => {
  it.each<[string, ArgBindings | undefined]>([
    ["absent, as on an Airflow too old to send one", undefined],
    ["null, as for a task called with no arguments", null],
  ])("binds nothing when the spec is %s", (_case, bindings) => {
    expect(decodeArgBindings(bindings)).toEqual({ names: null, values: {} });
  });

  it("reports a spec that bound nothing as empty, not as absent", () => {
    expect(decodeArgBindings([])).toEqual({ names: [], values: {} });
  });

  it("binds literals under their names, keeping declaration order", () => {
    const bound = decodeArgBindings([
      { name: "country", kind: "literal", value: "uk", value_schema: { type: "string" } },
      { name: "threshold", kind: "literal", value: 0.5 },
      { name: "retries_num", kind: "literal", value: 3, from_default: true },
    ]);

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
  ])("binds %s literal unchanged", (_case, value) => {
    expect(decodeArgBindings([{ name: "arg", kind: "literal", value }]).values).toEqual({
      arg: value,
    });
  });

  it("binds a literal Airflow sent without a value as null", () => {
    expect(decodeArgBindings([{ name: "arg", kind: "literal" }]).values).toEqual({ arg: null });
  });

  it("binds an argument named __proto__ as an own key, leaving the prototype alone", () => {
    const { values } = decodeArgBindings([
      { name: "__proto__", kind: "literal", value: "polluted" },
    ]);

    expect(Object.getOwnPropertyDescriptor(values, "__proto__")?.value).toBe("polluted");
    expect(Object.getPrototypeOf(values)).toBe(Object.prototype);
  });

  it("refuses an argument taking an upstream task's output", () => {
    expect(() =>
      decodeArgBindings([{ name: "extracted", kind: "xcom", task_id: "fn_extract" }]),
    ).toThrowError(
      'Task argument "extracted" takes the output of upstream task "fn_extract", but XCom-backed ' +
        "arguments are not supported yet; read the value inside the handler instead — " +
        'client.getXCom({ key: "return_value", taskId: "fn_extract" })',
    );
  });

  it.each<[string, Record<string, unknown>, string]>([
    ["a kind from a newer Airflow", { name: "arg", kind: "asset" }, '"asset"'],
    ["a binding with no kind at all", { name: "arg" }, "undefined"],
  ])("refuses %s rather than leaving the argument unbound", (_case, binding, rendered) => {
    expect(() => decodeArgBindings(unknownSpec(binding))).toThrowError(
      `Task argument "arg" has binding kind ${rendered}, which this version of the TypeScript ` +
        "SDK cannot bind; upgrade @apache-airflow/ts-sdk to match this Airflow release",
    );
  });

  it("refuses a name bound twice", () => {
    expect(() =>
      decodeArgBindings([
        { name: "country", kind: "literal", value: "uk" },
        { name: "country", kind: "literal", value: "de" },
      ]),
    ).toThrowError('Task argument "country" is bound more than once by the call to this task');
  });

  it.each(["ctx", "client"])("refuses an argument named %s, which the SDK owns", (name) => {
    expect(() => decodeArgBindings([{ name, kind: "literal", value: "shadowed" }])).toThrowError(
      `Task argument "${name}" collides with an argument the TypeScript SDK passes to every ` +
        "handler (ctx, client); rename the parameter in the @task.stub signature",
    );
  });
});
