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

// The TaskFlow call arguments Airflow captured for a task, resolved.
//
// A Python Dag declares a task that runs in another language with
// `@task.stub` and calls it TaskFlow-style — `transform("uk", extract())`.
// Airflow materializes that call site into an ordered, named spec and
// delivers it as `StartupDetails.ti_context.arg_bindings`; this turns it
// back into the extra keys the task handler is called with, pulling the
// upstream outputs the call passed along the way.

import type {
  ArgBindings,
  ArgValueSchema,
  TaskArgBinding,
  XComArgBinding,
} from "../generated/supervisor.js";
import type { CoordinatorClient } from "./client.js";
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

/** The XCom key an upstream TaskFlow task's output is stored under. */
const RETURN_VALUE_KEY = "return_value";

/** Schema `format` Airflow stamps on a value the Dag declared as a Python `int`. */
const INT64_FORMAT = "int64";

/** A task's TaskFlow call arguments, resolved from its run context. */
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

/** What resolving a spec needs from the runtime. */
export interface ArgBindingDeps {
  /** Pulls the upstream outputs the spec references. */
  readonly client: Pick<CoordinatorClient, "getXComEntry">;
  /** The task's abort signal; see {@link abortable}. */
  readonly signal: AbortSignal;
}

/**
 * Resolve `ti_context.arg_bindings` into the extra keys a task handler receives.
 *
 * Every rejection here fails the task. A binding this SDK cannot honour would
 * otherwise reach the handler as a missing key, and a missing key destructures
 * to `undefined` — the task would corrupt its output rather than stop.
 */
export async function resolveArgBindings(
  bindings: ArgBindings | undefined,
  deps: ArgBindingDeps,
): Promise<BoundArgs> {
  if (bindings == null) return { names: null, values: {} };

  // Validate the whole spec before pulling anything, so a spec this SDK cannot
  // honour fails the task without spending a round-trip on it first.
  const names: string[] = [];
  let pullsUpstream = false;
  for (const binding of bindings) {
    checkBindable(binding, names);
    names.push(binding.name);
    pullsUpstream ||= binding.kind === "xcom";
  }

  // One pass over the spec, every upstream pull in flight at once: a task
  // called with four upstream outputs waits for one round-trip, not four.
  const resolveAll = (): Promise<[string, JsonValue][]> =>
    Promise.all(
      bindings.map(
        async (binding): Promise<[string, JsonValue]> => [
          binding.name,
          binding.kind === "xcom"
            ? await pullXComArg(binding, deps.client)
            : ((binding.value ?? null) as JsonValue),
        ],
      ),
    );
  const entries = pullsUpstream ? await abortable(resolveAll, deps.signal) : await resolveAll();

  // fromEntries, not assignment: it defines own properties, so an argument
  // named `__proto__` becomes a key instead of reaching the prototype.
  return { names, values: Object.fromEntries(entries) };
}

/** Refuse a binding this SDK cannot honour, given the names bound before it. */
function checkBindable(binding: TaskArgBinding, boundSoFar: readonly string[]): void {
  const name = binding.name;
  if (RESERVED_ARG_NAMES.includes(name)) {
    throw new Error(
      `Task argument "${name}" collides with an argument the TypeScript SDK passes to every ` +
        `handler (${RESERVED_ARG_NAMES.join(", ")}); rename the parameter in the @task.stub signature`,
    );
  }
  if (boundSoFar.includes(name)) {
    throw new Error(`Task argument "${name}" is bound more than once by the call to this task`);
  }
  if (binding.kind === "xcom") return;
  if (binding.kind !== "literal") {
    // Unreachable for the wire union as generated, but a newer Airflow can add
    // a kind, and skipping it would silently leave the argument unbound.
    const kind: unknown = (binding as { kind?: unknown }).kind;
    throw new Error(
      `Task argument "${name}" has binding kind ${JSON.stringify(kind)}, which this version of ` +
        "the TypeScript SDK cannot bind; upgrade @apache-airflow/ts-sdk to match this Airflow release",
    );
  }
  // Airflow omits `value` for a literal whose value is null.
  checkExactInteger(name, (binding.value ?? null) as JsonValue, binding.value_schema);
}

/** Pull the upstream task's output this argument was called with. */
async function pullXComArg(
  binding: XComArgBinding,
  client: ArgBindingDeps["client"],
): Promise<JsonValue> {
  const entry = await client.getXComEntry({ key: RETURN_VALUE_KEY, taskId: binding.task_id });
  if (!entry.found) {
    throw new Error(
      `Task argument "${binding.name}" takes the output of upstream task "${binding.task_id}", ` +
        `which pushed no ${RETURN_VALUE_KEY} XCom; a task that returns nothing pushes none`,
    );
  }
  checkExactInteger(binding.name, entry.value, binding.value_schema);
  return entry.value;
}

/**
 * Refuse an integer JavaScript already failed to hold.
 *
 * Airflow stamps `format: "int64"` on an argument the Dag declared as a Python
 * `int`, whose range is wider than the one a JavaScript `number` represents
 * exactly. Binding such a value silently hands the handler a different number
 * from the one the Dag produced, which nothing downstream can detect.
 */
function checkExactInteger(
  name: string,
  value: JsonValue,
  schema: ArgValueSchema | null | undefined,
): void {
  if (schema?.["format"] !== INT64_FORMAT) return;
  if (typeof value !== "number" || Math.abs(value) <= Number.MAX_SAFE_INTEGER) return;
  throw new Error(
    `Task argument "${name}" is a 64-bit integer of ${value}, beyond the ±${Number.MAX_SAFE_INTEGER} ` +
      "a JavaScript number holds exactly, so its low digits are already lost; carry it across the " +
      "language boundary as a string instead",
  );
}

/**
 * Run `work`, giving up as soon as `signal` aborts.
 *
 * The comm channel cannot cancel a request in flight, so this abandons the
 * reply rather than the pull. It still matters: arguments resolve before the
 * handler runs, so a task terminated mid-pull has nothing else listening to the
 * signal, and would sit until the runtime's force-exit grace period expires.
 * `work` is a thunk so an already-aborted task issues no request at all.
 */
async function abortable<T>(work: () => Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) throw abortError(signal);
  let onAbort!: () => void;
  const aborted = new Promise<never>((_resolve, reject) => {
    onAbort = () => reject(abortError(signal));
  });
  signal.addEventListener("abort", onAbort, { once: true });
  try {
    return await Promise.race([work(), aborted]);
  } finally {
    signal.removeEventListener("abort", onAbort);
  }
}

function abortError(signal: AbortSignal): Error {
  const reason: unknown = signal.reason;
  return new Error(
    "Aborted while resolving this task's arguments from its upstream tasks: " +
      (reason instanceof Error ? reason.message : String(reason)),
  );
}
