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

// Runs the implementation of a provider operator: reads the task's arguments from the run context
// Airflow sent, has the runtime render the template fields in Python, and calls the operator's
// `execute`. The path is the same for a task of a TypeScript Dag and for a task of a Python Dag, since
// both get `ti_context.provider_operator`.
//
// The coordinator types are imported as types only: the authoring layer must not load the
// coordinator at run time.

import type {
  RuntimeRetryTask,
  RuntimeSucceedTask,
  RuntimeTaskState,
} from "../coordinator/protocol.js";
import type { JsonValue } from "./client-types.js";
import { skipTasks } from "./dag.js";
import type { ProviderRunContext } from "./operator.js";
import type { ProviderImplementation, ProviderOperatorContext } from "./provider-operator.js";
import { runInTaskScope } from "./task.js";

/** How the task ended. */
export type ProviderRunOutcome = RuntimeSucceedTask | RuntimeRetryTask | RuntimeTaskState;

/**
 * Run `implementation` for the task `op` describes.
 *
 * Any error ends the task as a failure, or as a retry when it has retries left.
 */
export async function runProviderImplementation(
  implementation: ProviderImplementation,
  op: ProviderRunContext,
): Promise<ProviderRunOutcome> {
  try {
    const provider = op.details.ti_context.provider_operator;
    if (provider == null) {
      return op.fail(
        `Task "${op.ctx.taskId}" has no provider operator in its run context, so ` +
          `${implementation.classpath} cannot run it; check that Airflow sends one`,
      );
    }
    if (provider.classpath !== implementation.classpath) {
      return op.fail(
        `Task "${op.ctx.taskId}" is a ${provider.classpath}, not a ${implementation.classpath}`,
      );
    }
    const args = await op.resolveProviderArgs(provider.kwargs, implementation.definition);
    const downstream = provider.downstream_task_ids[op.ctx.taskId] ?? [];
    const { client } = op;
    const context: ProviderOperatorContext = {
      dagId: op.ctx.dagId,
      taskId: op.ctx.taskId,
      runId: op.ctx.runId,
      tryNumber: op.ctx.tryNumber,
      mapIndex: op.ctx.mapIndex,
      signal: op.ctx.signal,
      logger: op.logs,
      skipDownstream: async () => {
        if (downstream.length > 0) await skipTasks(client, downstream);
      },
    };
    const result = await runInTaskScope({ ctx: op.ctx, client }, () =>
      implementation.execute(args, context),
    );
    return await op.succeed(result as JsonValue | undefined);
  } catch (err) {
    return op.fail(err instanceof Error ? err.message : String(err));
  }
}
