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

import { isPlainRecord, isTaskRef, type TaskRef } from "./dag.js";
import { brandOperator, type Operator, type PythonOperatorArgument } from "./operator.js";
import { describeType } from "./plain-json.js";

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

/** A provider operator class, as its generated binding describes it. */
export interface ProviderOperatorDefinition {
  /** Python module of the class, such as `airflow.providers.standard.operators.bash`. */
  readonly module: string;
  readonly className: string;
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
 */
export function defineProviderOperator<TArgs extends object, TResult = unknown>(
  definition: ProviderOperatorDefinition,
): (args: TArgs) => Operator<void, TResult> {
  const { className } = definition;
  const templated = new Set(definition.templateFields);
  return (args) => {
    const given: unknown = args;
    if (!isPlainRecord(given)) {
      throw new Error(`${className}(...) takes an object of its arguments`);
    }
    const kwargs = new Map<string, PythonOperatorArgument>();
    const upstream = new Set<TaskRef>();
    for (const [key, value] of Object.entries(given)) {
      const param = Object.hasOwn(definition.params, key) ? definition.params[key] : undefined;
      if (param === undefined) {
        throw new Error(`Unknown argument "${key}" for ${className}(...)`);
      }
      if (value === undefined) continue;
      const label = `Argument "${key}" of ${className}(...)`;
      kwargs.set(param.name, {
        value: copyArgument(value, param, label, templated.has(param.name), upstream),
        ...(param.encoding !== undefined && { encoding: param.encoding }),
      });
    }
    for (const [key, param] of Object.entries(definition.params)) {
      if (param.required && !kwargs.has(param.name)) {
        throw new Error(`${className}(...) needs the argument "${key}"`);
      }
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
      },
      getUpstreamTasks: () => upstreamTasks,
      // Reached only when the task's queue is routed to the Node coordinator.
      execute: (op) =>
        Promise.resolve(
          op.fail(
            `Task "${op.ctx.taskId}" is a ${className}, which only a Python worker can run; ` +
              "give it a queue that is not routed to the Node coordinator",
          ),
        ),
    };
    return brandOperator(operator);
  };
}

/**
 * Copy one argument, so that a later change to what the author passed does not change the Dag.
 *
 * A task reference is kept as it is and added to `upstream`. Only a templated argument can take
 * one: the worker resolves a reference when it renders templates, and nothing else would.
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
