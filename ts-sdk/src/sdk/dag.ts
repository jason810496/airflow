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

// The Dag authoring surface: `new Dag(dagId)`, `dag.task(taskId, handler)`, and
// the factory it returns. Calling a factory declares the task's place in the
// Dag, the way calling a TaskFlow function does in Python.

import { brand, DUPLICATE_COPY_HINT, hasBrand } from "./brand.js";
import type { JsonValue } from "./client-types.js";
import type { TaskHandler, TaskHandlerArgs } from "./task.js";

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

/**
 * Dag-level options.
 *
 * Every field here but {@link DagSpec.isMixedLanguageDag} describes the Dag
 * Airflow schedules, and native Dag declaration will add them — schedule,
 * catchup, tags, ... — generated from the serialized-Dag JSON schema as
 * `src/generated/supervisor.ts` is. They are all optional, so `{}` stays valid
 * and adding one cannot break a call site. Unknown keys are rejected, so a
 * misspelled field is an error rather than a Dag that quietly ignores it.
 */
export interface DagSpec {
  /**
   * Whether this Dag only binds TypeScript handlers to a Dag declared in Python.
   *
   * A mixed-language Dag takes its structure — tasks, order, arguments — from
   * the Python Dag file, so this SDK never serializes it and wiring its tasks
   * here is an error; what a handler is called with comes from the Python call,
   * delivered by the supervisor. Leave it unset for a Dag declared in
   * TypeScript.
   *
   * Local to this SDK and never serialized, unlike every other field of this
   * type: it says where the Dag *comes from*, which is not something the
   * serialized Dag has an opinion about.
   */
  readonly isMixedLanguageDag?: boolean;
}

/**
 * Task-level options.
 *
 * No fields yet, so only `{}` is accepted. Future task fields (retries, queue,
 * ...) will land here, generated as {@link DagSpec}'s are.
 */
export type TaskSpec = Record<string, never>;

/**
 * A reference to the result of one task, returned by calling that task.
 *
 * Identity only: the handler and the value are deliberately not exposed. Pass a
 * reference as an input of a downstream task to make that task depend on it.
 *
 * A reference carries a hidden brand, so a hand-written `{dagId, taskId}`
 * object is not one: wiring inputs also take plain JSON values, and without the
 * brand such an object could not be told apart from an upstream reference.
 */
export interface TaskRef {
  /** Identifier of the Dag this task belongs to. */
  readonly dagId: string;
  /** Airflow task ID, including any TaskGroup prefix. */
  readonly taskId: string;
}

/** Whether `value` is a TaskRef returned by any copy of this package. */
function isTaskRef(value: unknown): value is TaskRef {
  return hasBrand(value, "TaskRef");
}

/** The handler arguments a caller has to supply: everything the runtime does
 *  not pass on its own. `Pick` keeps the `?` and `readonly` modifiers, so an
 *  optional argument stays optional in the call. */
type WiredArgs<TArgs> = Pick<TArgs, Exclude<keyof TArgs, keyof TaskHandlerArgs>>;

/**
 * The part of `T` a literal can express, matched structurally.
 *
 * `Extract<T, JsonValue>` would do for a type literal but collapses an
 * `interface` to `never`, since an interface gets no implicit index signature
 * and so never matches `JsonValue`'s object arm. An object survives only if
 * every property does, which leaves types JSON cannot carry — a `Date`, a
 * method — as `never`, so such an argument can only be given a reference.
 */
type JsonCompatible<T> = T extends JsonValue
  ? T
  : // A function is an object with no keys, so it would otherwise map to `{}`
    // and take every method of a class along with it.
    T extends (...args: never[]) => unknown
    ? never
    : T extends readonly (infer TElement)[]
      ? readonly JsonCompatible<TElement>[]
      : T extends object
        ? T extends { [K in keyof T]: JsonCompatible<T[K]> }
          ? { [K in keyof T]: JsonCompatible<T[K]> }
          : never
        : never;

/**
 * The inputs a task is called with: an upstream {@link TaskRef} or a literal
 * value, per handler argument.
 *
 * A literal is restricted to the JSON-compatible part of the argument's type,
 * because it has to survive the trip through the serialized Dag. An argument
 * that cannot be expressed as JSON at all, such as a `Date`, can only be given
 * a reference.
 */
export type TaskInputs<TArgs> = {
  [K in keyof WiredArgs<TArgs>]: TaskRef | JsonCompatible<WiredArgs<TArgs>[K]>;
};

/**
 * What `dag.task(...)` returns: call it to declare where the task sits in the Dag.
 *
 * A handler that takes only `{ctx, client}` is called with no arguments; one
 * that declares further arguments is called with a value for each:
 *
 * ```ts
 * load({ transformed: transform({ extracted: extract(), regionCode: "us" }) });
 * ```
 *
 * The compiler checks that every argument is supplied and that each literal
 * matches its argument's type. It does not check an upstream's return type
 * against the argument it feeds: a {@link TaskRef} carries no value type, and
 * matching the two is left to the decode side, as `value_schema` is in
 * `airflow-core/adr/lang-sdk/0007-taskflow-across-language-boundary.md`.
 *
 * The factory is not itself a {@link TaskRef}: a reference exists only once the
 * task producing it has been called, which is what makes a cycle unwritable.
 */
export type TaskFactory<TArgs> = [keyof WiredArgs<TArgs>] extends [never]
  ? () => TaskRef
  : (inputs: TaskInputs<TArgs>) => TaskRef;

/**
 * Named options for `dag.task()`.
 *
 * Keyword-only so future fields can be added without a new parameter. Unknown
 * keys are rejected, so a typo fails at import time rather than being ignored.
 */
export interface TaskOptions {
  /** Task-level options. Stored, but not used yet — see {@link TaskSpec}. */
  readonly spec?: TaskSpec;
}

/** Per-task record a Dag retains: the reference, the handler, and its spec. */
export interface TaskRecord {
  readonly task: TaskRef;
  readonly handler: TaskHandler;
  readonly spec: TaskSpec;
}

/**
 * Internal: what one call to a task factory recorded, by argument name.
 *
 * A {@link TaskRef} is an edge from the upstream task; anything else is a
 * constant argument.
 *
 * Recorded for the serializer that will turn a native Dag into serialized Dag
 * JSON; nothing reads them at execution time. What a running task is called
 * with comes from the supervisor's `arg_bindings`, which are derived from the
 * serialized Dag, resolved per task instance, and are the one binding contract
 * every language SDK implements — see decision G of
 * `airflow-core/adr/lang-sdk/0007-taskflow-across-language-boundary.md`. Those
 * bindings therefore win over anything recorded here, and the two never have to
 * be reconciled: a mixed-language Dag records no wiring at all, and a native
 * Dag's wiring is what produced its bindings.
 */
export type RecordedInputs = Readonly<Record<string, TaskRef | JsonValue>>;

const NO_INPUTS: RecordedInputs = Object.freeze({});

// Assigned inside Dag's static block: gives package-internal code access to
// Dag's private state without public accessors on the class.
let taskRecordsOf: (dag: Dag) => ReadonlyMap<string, TaskRecord>;
let inputsOf: (dag: Dag) => ReadonlyMap<string, RecordedInputs>;
let finalizeOf: (dag: Dag) => void;

/** Internal: whether `value` is a Dag built by any copy of this package. */
export function isDag(value: unknown): value is Dag {
  return hasBrand(value, "Dag");
}

/**
 * A Dag declared in TypeScript.
 *
 * Declare the tasks with `dag.task(taskId, handler)`, then call what each
 * returns to lay out the Dag:
 *
 * ```ts
 * const extract = dag.task("extract", async ({ client }) => ({ rows: 12 }));
 * const load = dag.task("load", async ({ rows }: { rows: number } & TaskHandlerArgs) => {});
 * load({ rows: extract() });
 * ```
 *
 * Every task has to be called exactly once, so a task cannot be left out of the
 * Dag by accident. A Dag that instead binds handlers to a Dag declared in
 * Python sets {@link DagSpec.isMixedLanguageDag} and wires nothing.
 *
 * Constructing a Dag has no effect beyond the instance itself. Collect the ones
 * a bundle should serve in a `DagRegistry` and pass it to `serveDags(...)`.
 */
export class Dag {
  /** Identifier of this Dag. Must match the Python Dag's `dag_id` when this Dag
   *  is mixed-language. */
  readonly dagId: string;
  /** Dag-level options this instance was constructed with, copied and frozen. */
  readonly spec: DagSpec;
  readonly #tasks = new Map<string, TaskRecord>();
  readonly #inputs = new Map<string, RecordedInputs>();
  #finalized = false;

  static {
    taskRecordsOf = (dag) => dag.#tasks;
    inputsOf = (dag) => dag.#inputs;
    finalizeOf = (dag) => dag.#finalize();
  }

  constructor(dagId: string, spec: DagSpec = {}) {
    validateDagSpec(dagId, spec);
    brand(this, "Dag");
    this.dagId = dagId;
    // Copied and frozen, as task specs are: nothing reads a spec until the
    // bundle manifest is built, long after the user's module has run, so a
    // later mutation of their object would silently change what is packed.
    // Shallow — a nested value in a future generated spec stays mutable.
    this.spec = Object.freeze({ ...spec });
  }

  /** Task IDs attached to this Dag, in attachment order. */
  get taskIds(): readonly string[] {
    return [...this.#tasks.keys()];
  }

  /**
   * Declare a task of this Dag, and return the factory that places it.
   *
   * The handler's `{ctx, client}` arguments come from the runtime; every other
   * argument it declares becomes an input of the returned {@link TaskFactory}.
   * A handler that needs neither can declare only its own arguments. On a
   * mixed-language Dag, `taskId` must match the Python operator's `task_id`
   * exactly, including any TaskGroup prefix.
   */
  task<TArgs extends object = TaskHandlerArgs, TReturn = unknown>(
    taskId: string,
    handler: TaskHandler<TReturn, TArgs>,
    options: TaskOptions = {},
  ): TaskFactory<TArgs> {
    if (typeof handler !== "function") {
      throw new Error(`handler for Dag "${this.dagId}" task "${taskId}" must be a function`);
    }
    if (this.#tasks.has(taskId)) {
      throw new Error(`Task "${taskId}" is already registered for Dag "${this.dagId}"`);
    }
    // A task added to a native Dag after it was read could no longer be wired
    // into it, and would sit in the Dag unplaced and unreported. A
    // mixed-language Dag has no wiring to miss, and the registry looks its
    // tasks up live, so adding one there stays allowed.
    if (this.#finalized && !this.spec.isMixedLanguageDag) {
      throw new Error(
        `Task "${taskId}" cannot be added to Dag "${this.dagId}" after the Dag was read; declare every task while the module is loading`,
      );
    }
    this.#validateOptions(taskId, options);
    const { spec = {} } = options;
    this.#validateTaskSpec(taskId, spec);
    const task = createTaskRef(this.dagId, taskId);
    this.#tasks.set(taskId, {
      task,
      handler: handler as TaskHandler,
      spec: Object.freeze({ ...spec }),
    });
    return ((inputs?: unknown) => {
      this.#wire(taskId, inputs);
      return task;
    }) as TaskFactory<TArgs>;
  }

  // TypeScript is bypassable — from plain JavaScript, or an `as TaskOptions`
  // cast — so an unknown key is rejected rather than silently ignored.
  #validateOptions(taskId: string, options: TaskOptions): void {
    const value: unknown = options;
    if (!isPlainRecord(value)) {
      throw new Error(`options for Dag "${this.dagId}" task "${taskId}" must be an object`);
    }
    for (const key of Object.keys(value)) {
      if (key !== "spec") {
        throw new Error(`Unknown option "${key}" for Dag "${this.dagId}" task "${taskId}"`);
      }
    }
  }

  #validateTaskSpec(taskId: string, spec: TaskSpec): void {
    if (this.spec.isMixedLanguageDag && isPlainRecord(spec) && Reflect.ownKeys(spec).length > 0) {
      throw new Error(
        `Task "${taskId}" of Dag "${this.dagId}" cannot take a spec: the Dag is declared in Python, which is where its task options come from`,
      );
    }
    if (!isPlainRecord(spec) || Reflect.ownKeys(spec).length > 0) {
      throw new Error(`spec for Dag "${this.dagId}" task "${taskId}" must be an empty object`);
    }
  }

  #wire(taskId: string, inputs: unknown): void {
    if (this.spec.isMixedLanguageDag) {
      throw new Error(
        `Task "${taskId}" of Dag "${this.dagId}" cannot be called: the Dag is declared in Python, which is where its dependencies come from`,
      );
    }
    if (this.#finalized) {
      throw new Error(
        `Task "${taskId}" of Dag "${this.dagId}" was called after the Dag was read; call every task while the module is loading`,
      );
    }
    if (this.#inputs.has(taskId)) {
      throw new Error(
        `Task "${taskId}" of Dag "${this.dagId}" was already called; a task holds one place in a Dag, so call it once and reuse the reference`,
      );
    }
    this.#inputs.set(taskId, this.#collectInputs(taskId, inputs));
  }

  #collectInputs(taskId: string, inputs: unknown): RecordedInputs {
    if (inputs === undefined) return NO_INPUTS;
    if (!isPlainRecord(inputs)) {
      throw new Error(`inputs for Dag "${this.dagId}" task "${taskId}" must be an object`);
    }
    // Every own key, not just the enumerable string ones: a symbol key would be
    // copied by the spread below and so has to be checked, not stepped over.
    for (const key of Reflect.ownKeys(inputs)) {
      if (typeof key === "symbol") {
        throw new Error(
          `Input "${String(key)}" of task "${taskId}" is keyed by a symbol; an argument name is a string`,
        );
      }
      const value = inputs[key];
      // Anything unbranded is a literal argument, including a look-alike
      // `{dagId, taskId}` object: only a real reference makes an edge.
      if (!isTaskRef(value)) continue;
      if (value.dagId !== this.dagId) {
        throw new Error(
          `Input "${key}" of task "${taskId}" comes from Dag "${value.dagId}", not "${this.dagId}"`,
        );
      }
      const upstream = this.#tasks.get(value.taskId);
      if (!upstream) {
        throw new Error(
          `Input "${key}" of task "${taskId}" refers to unregistered task "${value.taskId}"`,
        );
      }
      // Identity, not just the ID pair: two Dag objects can carry the same
      // dagId, and a second resolved copy of this package brands its own
      // references, so matching IDs do not make a reference this Dag handed out.
      if (upstream.task !== value) {
        throw new Error(
          `Input "${key}" of task "${taskId}" was not returned by this Dag's "${value.taskId}"; it comes from another Dag object with the same ID, or ${DUPLICATE_COPY_HINT}`,
        );
      }
    }
    return Object.freeze({ ...inputs }) as RecordedInputs;
  }

  #finalize(): void {
    if (this.#finalized) return;
    // A mixed-language Dag is laid out by its Python file, so an uncalled task
    // there is the normal case rather than a missing edge.
    if (!this.spec.isMixedLanguageDag) {
      for (const taskId of this.#tasks.keys()) {
        if (!this.#inputs.has(taskId)) {
          throw new Error(
            `Task "${taskId}" of Dag "${this.dagId}" is never called, so it has no place in the Dag; call it, or set isMixedLanguageDag if the Dag is declared in Python`,
          );
        }
      }
    }
    // Last, so a Dag that failed the check reports that same failure again
    // rather than reporting itself as already read.
    this.#finalized = true;
  }
}

function createTaskRef(dagId: string, taskId: string): TaskRef {
  const task: TaskRef = { dagId, taskId };
  brand(task, "TaskRef");
  return Object.freeze(task);
}

function validateDagSpec(dagId: string, spec: DagSpec): void {
  const value: unknown = spec;
  if (!isPlainRecord(value)) {
    throw new Error(`spec for Dag "${dagId}" must be an object`);
  }
  for (const key of Reflect.ownKeys(value)) {
    if (key !== "isMixedLanguageDag") {
      throw new Error(`Unknown option "${String(key)}" in the spec for Dag "${dagId}"`);
    }
  }
  if (value.isMixedLanguageDag !== undefined && typeof value.isMixedLanguageDag !== "boolean") {
    throw new Error(`isMixedLanguageDag for Dag "${dagId}" must be a boolean`);
  }
}

/**
 * Internal: the task records of a Dag, for registry lookups.
 *
 * Not re-exported from the package root, and the package `"exports"` map
 * blocks deep imports, so this is unreachable from outside the SDK.
 */
export function getDagTaskRecords(dag: Dag): ReadonlyMap<string, TaskRecord> {
  return taskRecordsOf(dag);
}

/** Internal: what each task of a Dag was called with, keyed by task ID.
 *  A task that has not been called is absent. */
export function getDagTaskInputs(dag: Dag): ReadonlyMap<string, RecordedInputs> {
  return inputsOf(dag);
}

/**
 * Internal: check that `dag` is fully laid out, and close it to further wiring.
 *
 * Idempotent, and never part of the public surface: a Dag is finished when its
 * module has finished loading, so there is nothing for an author to call.
 */
export function finalizeDag(dag: Dag): void {
  finalizeOf(dag);
}
