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

// What a generated provider binding is built from. A binding carries no task body: it records
// the Python operator class and its constructor arguments, the serializer writes them into the
// Dag, and a Python worker builds and runs the operator from there. See
// `airflow-core/adr/lang-sdk/0009-provider-operators-as-generated-dsl.md`.
//
// A binding that also passes `execute` is the implementation of the operator too. A task of it runs
// in this runtime, whichever language its Dag is written in. See
// `airflow-core/adr/lang-sdk/0018-provider-operators-implemented-in-lang-sdks.md`.

import { brand } from "./brand.js";
import { isPlainRecord, isTaskRef, type TaskRef } from "./dag.js";
import { brandOperator, type Operator, type PythonOperatorArgument } from "./operator.js";
import { describeType } from "./plain-json.js";
import { runProviderImplementation } from "./provider-implementation.js";

/**
 * A templated argument: its value, or the task whose result it takes.
 *
 * A string value may hold a Jinja template, which the Python worker renders as it does for a
 * Python Dag. A task reference makes that task an upstream of this one.
 */
export type Templated<TValue> = TValue | TaskRef;

/** One constructor argument of a provider operator. */
export interface ProviderParamDefinition {
  /** The argument's name in the Python constructor. */
  readonly name: string;
  readonly required?: boolean;
  /** How a value JSON has no type for is given: seconds for a duration, a `Date` for a datetime. */
  readonly encoding?: "timedelta" | "datetime";
}

/** `TArgs` as `execute` receives it: every {@link Templated} argument is its rendered value. */
export type ResolvedArgs<TArgs extends object> = {
  [TKey in keyof TArgs]: Exclude<TArgs[TKey], TaskRef>;
};

/** A logger an operator's `execute` writes to. The fields are structured log context. */
export interface ProviderOperatorLogger {
  debug(message: string, fields?: Record<string, unknown>): void;
  info(message: string, fields?: Record<string, unknown>): void;
  warning(message: string, fields?: Record<string, unknown>): void;
  error(message: string, fields?: Record<string, unknown>): void;
}

/** What an operator's `execute` is told about the task it runs. */
export interface ProviderOperatorContext {
  readonly dagId: string;
  readonly taskId: string;
  readonly runId: string;
  readonly tryNumber: number;
  /** -1 for a task that is not mapped. */
  readonly mapIndex: number;
  /** Fires when Airflow terminates the task. Pass it to anything that takes an abort signal. */
  readonly signal: AbortSignal;
  readonly logger: ProviderOperatorLogger;
  /** Skip the direct downstream tasks of this task, as a branch operator does. */
  skipDownstream(): Promise<void>;
}

/** A provider operator class, as its generated binding describes it. */
export interface ProviderOperatorDefinition<TArgs extends object = object, TResult = unknown> {
  /** Python module of the class, such as `airflow.providers.standard.operators.bash`. */
  readonly module: string;
  readonly className: string;
  /**
   * The module that exports this binding under `className`, such as
   * `@apache-airflow/providers-localfile/operators/file`. It is written to the task, as the generated
   * Python wrapper writes it, so that a runtime that does not declare the task can import it. The spec
   * generator fails when it is not the package name and the source file's path.
   */
  readonly implementationModule?: string;
  /** The constructor arguments, keyed by the name the binding gives each one. */
  readonly params: Readonly<Record<string, ProviderParamDefinition>>;
  /** The Python names of the arguments the worker renders as templates. */
  readonly templateFields: readonly string[];
  readonly templateExt?: readonly string[];
  readonly templateFieldsRenderers?: Readonly<Record<string, string>>;
  readonly uiColor?: string;
  readonly uiFgcolor?: string;
  /** The name the UI shows, when it is not the class name. */
  readonly operatorName?: string;
  readonly isSensor?: boolean;
  /** Whether the operator may skip its downstream tasks, as `SkipMixin` marks it in Python. */
  readonly canSkipDownstream?: boolean;
  /**
   * Implements the operator `${module}.${className}`, so that a task of this class runs here
   * instead of on a Python worker. A provider package exports the binding under `className`.
   *
   * It takes the arguments of the task, rendered, and returns the task's `return_value` XCom,
   * which must be JSON. Throwing fails the task, or retries it when it has retries left.
   */
  readonly execute?: (
    args: ResolvedArgs<TArgs>,
    ctx: ProviderOperatorContext,
  ) => TResult | Promise<TResult>;
}

/** What {@link defineProviderOperator} returns: the operator's factory. */
export type ProviderOperatorBinding<TArgs extends object = never, TResult = unknown> = (
  args: TArgs,
) => Operator<void, TResult>;

/** Internal: an operator a binding implements. */
export interface ProviderImplementation {
  /** The Python class path, `${module}.${className}`. */
  readonly classpath: string;
  readonly definition: ProviderOperatorDefinition;
  readonly execute: (args: object, ctx: ProviderOperatorContext) => unknown;
}

interface BindingRecord {
  readonly classpath: string;
  readonly implementation?: ProviderImplementation;
}

const BINDINGS = new WeakMap<object, BindingRecord>();
const BINDING_BRAND = "ProviderOperatorBinding";

/** Internal: whether `value` is a binding built by any copy of this package. */
export function isProviderOperatorBinding(value: unknown): value is ProviderOperatorBinding {
  return (
    typeof value === "function" &&
    (BINDINGS.has(value) || Symbol.for(`airflow.ts-sdk.${BINDING_BRAND}`) in value)
  );
}

/**
 * Internal: what the binding `value` implements, `null` for a binding that is no implementation,
 * and `undefined` when `value` is not a binding built by this copy of the package.
 */
export function getProviderImplementation(
  value: unknown,
): { classpath: string; implementation: ProviderImplementation | null } | undefined {
  const record = typeof value === "function" ? BINDINGS.get(value) : undefined;
  return record === undefined
    ? undefined
    : { classpath: record.classpath, implementation: record.implementation ?? null };
}

/**
 * Build the binding of one provider operator: a function that takes the operator's arguments
 * and returns what `dag.task` places in a Dag.
 *
 * ```ts
 * dag.task("create_bucket", S3CreateBucketOperator({ bucketName: "raw" }))();
 * ```
 *
 * The task runs on a Python worker with the provider installed, so it does not take the Dag's
 * queue: give it one in its task options when the default queue is not served by Python workers.
 *
 * With `execute`, the binding implements the operator. A task of it runs in this runtime when its
 * queue is routed to the Node coordinator: in the bundle that declares the task's Dag, or else in
 * the provider runner, which imports the package that exports the binding.
 */
export function defineProviderOperator<TArgs extends object, TResult = unknown>(
  definition: ProviderOperatorDefinition<TArgs, TResult>,
): ProviderOperatorBinding<TArgs, TResult> {
  const { className } = definition;
  const classpath = `${definition.module}.${className}`;
  const templated = new Set(definition.templateFields);
  const implementation: ProviderImplementation | undefined =
    definition.execute === undefined
      ? undefined
      : {
          classpath,
          definition: definition as unknown as ProviderOperatorDefinition,
          execute: definition.execute as ProviderImplementation["execute"],
        };
  const binding: ProviderOperatorBinding<TArgs, TResult> = (args) => {
    const given: unknown = args;
    if (!isPlainRecord(given)) {
      throw new Error(`${className}(...) takes an object of its arguments`);
    }
    for (const key of Object.keys(given)) {
      if (!Object.hasOwn(definition.params, key)) {
        throw new Error(`Unknown argument "${key}" for ${className}(...)`);
      }
    }
    // In the order the operator declares its parameters, whatever order the caller named them in.
    const kwargs = new Map<string, PythonOperatorArgument>();
    const upstream = new Set<TaskRef>();
    for (const [key, param] of Object.entries(definition.params)) {
      const value = given[key];
      if (value === undefined) {
        if (param.required) throw new Error(`${className}(...) needs the argument "${key}"`);
        continue;
      }
      const label = `Argument "${key}" of ${className}(...)`;
      kwargs.set(param.name, {
        value: copyArgument(value, param, label, templated.has(param.name), upstream),
        ...(param.encoding !== undefined && { encoding: param.encoding }),
      });
    }
    const upstreamTasks = Object.freeze([...upstream]);
    const operator: Operator<void, TResult> = {
      operatorName: definition.operatorName ?? className,
      canSkipDownstream: definition.canSkipDownstream === true,
      requiresTaskId: true,
      label: className,
      taskIdExample: `dag.task("my_task", ${className}({ ... }))`,
      takesNoInputs: true,
      pythonOperator: {
        module: definition.module,
        className,
        kwargs,
        templateFields: definition.templateFields,
        templateExt: definition.templateExt,
        templateFieldsRenderers: definition.templateFieldsRenderers,
        uiColor: definition.uiColor,
        uiFgcolor: definition.uiFgcolor,
        isSensor: definition.isSensor,
        implementationModule: definition.implementationModule,
      },
      getUpstreamTasks: () => upstreamTasks,
      // Reached only when the task's queue is routed to the Node coordinator.
      execute:
        implementation === undefined
          ? (op) =>
              Promise.resolve(
                op.fail(
                  `Task "${op.ctx.taskId}" is a ${className}, which only a Python worker can run; ` +
                    "give it a queue that is not routed to the Node coordinator",
                ),
              )
          : (op) => runProviderImplementation(implementation, op),
    };
    return brandOperator(operator);
  };
  BINDINGS.set(binding, { classpath, implementation });
  brand(binding, BINDING_BRAND);
  return binding;
}

/**
 * Copy one argument, so that a later change to what the author passed does not change the Dag.
 *
 * A task reference is kept as it is and added to `upstream`. Only a templated argument can take
 * one: the worker resolves a reference when it renders templates, and nothing else would. It has
 * to be the whole argument, which the serializer checks when it writes the Dag.
 */
function copyArgument(
  value: unknown,
  param: ProviderParamDefinition,
  label: string,
  isTemplated: boolean,
  upstream: Set<TaskRef>,
): unknown {
  if (value === null) return null;
  if (param.encoding === "timedelta") {
    if (typeof value !== "number" || !Number.isFinite(value)) {
      throw new Error(`${label} is a duration in seconds, not ${describeType(value)}`);
    }
    return value;
  }
  if (param.encoding === "datetime") {
    if (!(value instanceof Date) || Number.isNaN(value.getTime())) {
      throw new Error(`${label} is a valid Date, not ${describeType(value)}`);
    }
    return new Date(value.getTime());
  }
  const copy = (item: unknown): unknown => {
    if (item === null || typeof item === "string" || typeof item === "boolean") return item;
    if (typeof item === "number" && Number.isFinite(item)) return item;
    if (isTaskRef(item)) {
      if (!isTemplated) {
        throw new Error(
          `${label} was given the result of task "${item.taskId}", but it is not a templated ` +
            "argument, so the worker would not resolve it",
        );
      }
      upstream.add(item);
      return item;
    }
    if (Array.isArray(item)) return Object.freeze(item.map(copy));
    if (isPlainRecord(item)) {
      return Object.freeze(
        Object.fromEntries(
          Object.entries(item)
            .filter(([, nested]) => nested !== undefined)
            .map(([key, nested]) => [key, copy(nested)]),
        ),
      );
    }
    throw new Error(
      `${label} holds ${describeType(item)}, which JSON cannot carry; pass a string, a finite ` +
        "number, a boolean, null, an array, a plain object or a task's result",
    );
  };
  return copy(value);
}
