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

// The TaskFlow call arguments Airflow captured for a task, decoded.
//
// A Python Dag declares a task that runs in another language with
// `@task.stub` and calls it TaskFlow-style — `transform("uk", extract())`.
// Airflow materializes that call site into an ordered, named spec and
// delivers it as `StartupDetails.ti_context.arg_bindings`; this turns it
// back into the extra keys the task handler is called with.

import type { ArgBindings } from "../generated/supervisor.js";
import type { JsonValue } from "../sdk/client-types.js";

/**
 * Handler argument names the SDK owns.
 *
 * Reserved permanently: these two are the entire top-level surface the SDK
 * injects, and whatever it gains later belongs inside `ctx` rather than
 * becoming a third key. Every other key is a Dag author's own, so the set
 * never grows and a Dag cannot be broken by a future release claiming a name.
 */
const RESERVED_ARG_NAMES: readonly string[] = ["ctx", "client"];

/** A task's TaskFlow call arguments, decoded from its run context. */
export interface BoundArgs {
  /**
   * Bound names in the calling signature's declaration order, or `null` when
   * the run context carried no spec — an Airflow too old to send one, or a task
   * called with no arguments. An empty array means a spec that bound nothing.
   */
  readonly names: readonly string[] | null;
  /** Bound values keyed by name, to spread onto the handler's argument object. */
  readonly values: Readonly<Record<string, JsonValue>>;
}

/**
 * Decode `ti_context.arg_bindings` into the extra keys a task handler receives.
 *
 * Every rejection here fails the task. A binding this SDK cannot honour would
 * otherwise reach the handler as a missing key, and a missing key destructures
 * to `undefined` — the task would corrupt its output rather than stop.
 */
export function decodeArgBindings(bindings: ArgBindings | undefined): BoundArgs {
  if (bindings == null) return { names: null, values: {} };

  const names: string[] = [];
  const entries: [string, JsonValue][] = [];
  for (const binding of bindings) {
    const name = binding.name;
    if (RESERVED_ARG_NAMES.includes(name)) {
      throw new Error(
        `Task argument "${name}" collides with an argument the TypeScript SDK passes to every ` +
          `handler (${RESERVED_ARG_NAMES.join(", ")}); rename the parameter in the @task.stub signature`,
      );
    }
    if (names.includes(name)) {
      throw new Error(`Task argument "${name}" is bound more than once by the call to this task`);
    }
    if (binding.kind === "xcom") {
      throw new Error(
        `Task argument "${name}" takes the output of upstream task "${binding.task_id}", but ` +
          "XCom-backed arguments are not supported yet; read the value inside the handler " +
          `instead — client.getXCom({ key: "return_value", taskId: "${binding.task_id}" })`,
      );
    }
    if (binding.kind !== "literal") {
      // Unreachable for the wire union as generated, but a newer Airflow can add
      // a kind, and skipping it would silently leave the argument unbound.
      const kind: unknown = (binding as { kind?: unknown }).kind;
      throw new Error(
        `Task argument "${name}" has binding kind ${JSON.stringify(kind)}, which this version of ` +
          "the TypeScript SDK cannot bind; upgrade @apache-airflow/ts-sdk to match this Airflow release",
      );
    }
    names.push(name);
    // Airflow omits `value` for a literal whose value is null.
    entries.push([name, (binding.value ?? null) as JsonValue]);
  }
  // fromEntries, not assignment: it defines own properties, so an argument
  // named `__proto__` becomes a key instead of reaching the prototype.
  return { names, values: Object.fromEntries(entries) };
}
