<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# ADR-0005: TypeScript Authoring Interface for Callbacks and Deadline Alerts

## Status

Proposed.

## Context

The shared contract for callbacks, deadline alerts and listeners is in
[`airflow-core/adr/lang-sdk/0020`](../../airflow-core/adr/lang-sdk/0020-callbacks-deadline-alerts-and-listeners-across-lang-sdks.md).
This ADR records the TypeScript spelling of it. Listeners have no TypeScript API.

## Decision

1. **Callbacks are `DagSpec` and `TaskSpec` fields.** `DagSpec` gets `onSuccessCallback` and
   `onFailureCallback`; `TaskSpec` gets `onExecuteCallback`, `onSuccessCallback`,
   `onFailureCallback`, `onRetryCallback` and `onSkippedCallback`. Each takes one callback or an
   array. They are hand-written fields, since the schema keys are the `has_on_*_callback` flags,
   which the serializer sets when a field is non-empty.
2. **A callback is a function or a `PythonCallbackRef`.** The function takes a `CallbackContext` and
   may be async, since it runs in the runtime. `getClient()` works inside it.
3. **`DagSpec.deadline` takes `DeadlineAlert` objects.** Built-in references come from the
   `deadlines` module, and a custom reference is `python.deadlineReferenceRef`. `interval` is seconds. An alert whose callback is a function must have a `name` unique
   in the Dag.
4. **The `python` module adds `callbackRef`, `asyncCallbackRef` and `deadlineReferenceRef`.** `asyncCallbackRef` and the
   `executor` option of `callbackRef` are accepted only in a deadline alert, and the types enforce
   it.
5. **`TaskSpec` gets an `email` field.** It takes one address or an array. The existing
   `emailOnFailure` and `emailOnRetry` fields take effect only when `email` is set, as in Python.

## Signatures

```ts
// @apache-airflow/ts-sdk
type CallbackFn = (ctx: CallbackContext) => void | Promise<void>;
type Callback = CallbackFn | PythonCallbackRef;
type Callbacks = Callback | Callback[];

interface DagSpec {
  readonly onSuccessCallback?: Callbacks;
  readonly onFailureCallback?: Callbacks;
  readonly deadline?: DeadlineAlert[];
  /* ... */
}
interface TaskSpec {
  readonly onExecuteCallback?: Callbacks;
  readonly onSuccessCallback?: Callbacks;
  readonly onFailureCallback?: Callbacks;
  readonly onRetryCallback?: Callbacks;
  readonly onSkippedCallback?: Callbacks;
  readonly email?: string | string[];
  /* ... */
}

interface CallbackContext {
  readonly source: "task" | "dag" | "deadline";
  readonly dagId: string;
  readonly runId: string;
  readonly taskId: string | null;       // task callbacks
  readonly tryNumber: number | null;    // task callbacks
  readonly mapIndex: number | null;     // task callbacks
  readonly error: string | null;        // failure and retry callbacks
  readonly reason: string | null;       // Dag callbacks
  readonly deadline: { id: string; deadlineTime: Date } | null;   // deadline callbacks
}

type DeadlineAlert =
  | { name: string; reference: DeadlineReference; interval: number; callback: CallbackFn }
  | { name?: string; reference: DeadlineReference; interval: number;
      callback: PythonCallbackRef | PythonAsyncCallbackRef };

// @apache-airflow/ts-sdk/deadlines
function dagRunLogicalDate(): DeadlineReference;
function dagRunQueuedAt(): DeadlineReference;
function fixedDatetime(at: Date | string): DeadlineReference;   // a string needs an offset
function averageRuntime(options?: { maxRuns?: number; minRuns?: number }): DeadlineReference;

// @apache-airflow/ts-sdk/python
function callbackRef(path: string, kwargs?: JsonObject, options?: { executor?: string }): PythonCallbackRef;
function asyncCallbackRef(path: string, kwargs?: JsonObject, options?: { queue?: string }): PythonAsyncCallbackRef;
function deadlineReferenceRef(path: string, data?: JsonObject): DeadlineReference;
```

## Example

```ts
import { Dag, getClient, type CallbackContext } from "@apache-airflow/ts-sdk";
import * as deadlines from "@apache-airflow/ts-sdk/deadlines";
import { asyncCallbackRef, callbackRef, deadlineReferenceRef } from "@apache-airflow/ts-sdk/python";

async function notifyTeam(ctx: CallbackContext) {
  await getClient().setVariable("last_notified_run", ctx.runId);
}
const page = (ctx: CallbackContext) => console.error(`${ctx.dagId} failed: ${ctx.error}`);
const escalate = (ctx: CallbackContext) => console.error(`${ctx.dagId} missed ${ctx.deadline?.deadlineTime}`);

const dag = new Dag("daily_report", {
  queue: "typescript",
  onSuccessCallback: [notifyTeam, callbackRef("my_plugin.alerts.notify", { channel: "#ops" })],
  onFailureCallback: page,
  deadline: [
    { name: "sla", reference: deadlines.dagRunQueuedAt(), interval: 3600, callback: escalate },
    { reference: deadlines.dagRunLogicalDate(), interval: 7200,
      callback: callbackRef("my_plugin.alerts.page", {}, { executor: "CeleryExecutor" }) },
    { reference: deadlines.fixedDatetime("2026-12-31T00:00:00+01:00"), interval: 0,
      callback: asyncCallbackRef("my_plugin.alerts.slack", { channel: "#ops" }, { queue: "default" }) },
    { name: "slow", reference: deadlines.averageRuntime({ maxRuns: 10, minRuns: 3 }), interval: 600,
      callback: escalate },
    { name: "batch", reference: deadlineReferenceRef("my_plugin.deadlines.UpstreamBatchLanded", { batch: "eu" }),
      interval: 1800, callback: escalate },
  ],
});

dag.task(async function load() { /* ... */ }, {
  onExecuteCallback: (ctx) => console.log(`starting try ${ctx.tryNumber}`),
  onSuccessCallback: notifyTeam,
  onFailureCallback: [page, callbackRef("my_plugin.alerts.notify", { channel: "#oncall" })],
  onRetryCallback: page,
  onSkippedCallback: (ctx) => console.log(`${ctx.taskId} skipped`),
  email: ["oncall@example.com"],
  emailOnRetry: false,
});
```

## Consequences

- The serializer writes `has_on_*_callback` from these fields and the deadline alerts into
  `deadline`, which it writes as `null` today.
- The runtime handles the callback startup messages of core ADR-0020 beside `StartupDetails`, and
  sends `RunPythonCallback` and `SendErrorEmail` to the supervisor.
- `capabilities.yaml` marks callbacks and deadline alerts as supported, and listeners as supported
  through Python listeners.
