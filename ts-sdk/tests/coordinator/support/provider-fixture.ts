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

// A provider operator package for the runtime to import, as the provider runner imports one. A
// test sets `behavior` to change what the operator does, and reads what it was run with from `seen`.

import {
  defineProviderOperator,
  type ProviderOperatorContext,
  type Templated,
} from "../../../src/sdk/provider-operator.js";
import { getClient, getContext } from "../../../src/sdk/task.js";

export interface WriteArgs {
  path: Templated<string>;
  content?: Templated<string | null>;
  mode?: number | null;
  timeout?: number | null;
  since?: Date;
  options?: Readonly<Record<string, string | readonly number[]>>;
}

export const seen: {
  args: unknown[];
  contexts: ProviderOperatorContext[];
  scope: { taskId: string; hasClient: boolean }[];
  behavior?: (ctx: ProviderOperatorContext) => unknown;
} = { args: [], contexts: [], scope: [] };

export function resetSeen(): void {
  seen.args = [];
  seen.contexts = [];
  seen.scope = [];
  seen.behavior = undefined;
}

export const WriteOperator = defineProviderOperator<WriteArgs, unknown>({
  module: "airflow.providers.demo.operators.file",
  className: "WriteOperator",
  templateFields: ["path", "content"],
  params: {
    path: { name: "path", required: true },
    content: { name: "content" },
    mode: { name: "file_mode" },
    timeout: { name: "timeout", encoding: "timedelta" },
    since: { name: "since", encoding: "datetime" },
    options: { name: "options" },
  },
  execute(args, ctx) {
    seen.args.push(args);
    seen.contexts.push(ctx);
    seen.scope.push({ taskId: getContext().taskId, hasClient: getClient() !== undefined });
    return seen.behavior === undefined ? { bytes: 3 } : seen.behavior(ctx);
  },
});

export const PlainOperator = defineProviderOperator<{ mode: number; text?: Templated<string> }>({
  module: "airflow.providers.demo.operators.file",
  className: "PlainOperator",
  templateFields: ["text"],
  params: { mode: { name: "file_mode", required: true }, text: { name: "text" } },
  execute(args) {
    seen.args.push(args);
  },
});

/** Describes a Python operator only, so it has no `execute`. */
export const DescribedOperator = defineProviderOperator<{ path: string }>({
  module: "airflow.providers.demo.operators.file",
  className: "DescribedOperator",
  templateFields: [],
  params: { path: { name: "path", required: true } },
});

/** Exported under a name that is not its class name's, so it does not match the class path. */
export const MisnamedOperator = defineProviderOperator<{ path: string }>({
  module: "airflow.providers.demo.operators.file",
  className: "SomethingElse",
  templateFields: [],
  params: { path: { name: "path", required: true } },
  execute() {},
});

export const NotABinding = { path: "x" };

/** Built by another copy of the SDK: it carries the shared brand, but this copy has no record of it. */
export const ForeignCopyOperator = Object.assign(() => undefined, {
  [Symbol.for("airflow.ts-sdk.ProviderOperatorBinding")]: true,
});
