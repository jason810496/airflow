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

# Airflow TypeScript SDK

Public TypeScript interfaces for writing Apache Airflow task handlers.

**Status:** alpha · API will change · Node 22+ · ESM-only

This package defines the user-facing task handler contract and the coordinator
runtime used to execute registered TypeScript handlers from Airflow.

> **Note:** This package is not yet published to a public npm registry. Until an
> official Apache Airflow release is available, build and use it from source (see
> [Development](#development)).

## Task Handlers

```ts
import { registerTask, type TaskHandlerArgs } from "@apache-airflow/ts-sdk";

export async function sayHello({ ctx, client }: TaskHandlerArgs) {
  const greeting = await client.getVariable("greeting");
  return { message: `Hello from ${ctx.taskId}: ${greeting}` };
}

registerTask({ dagId: "example_dag", taskId: "say_hello" }, sayHello);
```

Non-`undefined` return values are pushed to XCom under the `"return_value"`
key by the active runtime, matching Python `@task` behavior.

## Coordinator Usage

Airflow runs TypeScript task bundles through the Python-side
`airflow.sdk.coordinators.node.NodeCoordinator`. Declaring Airflow Dags in
TypeScript is not supported yet; the Dag is still declared in Python. The
intended authoring shape matches the other non-Python SDKs: a Python Dag
declares the scheduling shape with stub tasks, and the TypeScript module
registers handlers with matching task IDs.

Python Dag:

```python
from airflow.sdk import dag, task


@dag
def sales_pipeline():
    @task.stub(queue="typescript")
    def extract(): ...

    @task.stub(queue="typescript")
    def transform(extracted): ...

    transform(extract())


sales_pipeline()
```

Airflow coordinator config:

```ini
[sdk]
coordinators = {
  "ts": {
    "classpath": "airflow.sdk.coordinators.node.NodeCoordinator",
    "kwargs": {"bundles_root": ["/opt/airflow/ts-bundles"]}
  }
}
queue_to_coordinator = {"typescript": "ts"}
```

Each configured bundle directory must contain a `bundle.mjs` built with
`airflow-ts-pack` (see [Packing bundles](#packing-bundles)), which embeds the
Airflow metadata in the bundle itself.

TypeScript entrypoint:

```ts
import { registerTask, startCoordinator, type TaskHandlerArgs } from "@apache-airflow/ts-sdk";

export async function extract({ client }: TaskHandlerArgs) {
  const connection = await client.getConnection("sales_db");
  const rowCount = Number((await client.getVariable("daily_row_count")) ?? "0");

  return {
    connectionId: connection?.id ?? null,
    rowCount,
  };
}

export async function transform({ client }: TaskHandlerArgs) {
  const extracted = await client.getXCom<{ rowCount: number }>({
    key: "return_value",
    taskId: "extract",
  });

  return {
    transformedRows: extracted?.rowCount ?? 0,
  };
}

registerTask({ dagId: "sales_pipeline", taskId: "extract" }, extract);
registerTask({ dagId: "sales_pipeline", taskId: "transform" }, transform);

await startCoordinator();
```

The Python stub defines the Dag dependency graph. The TypeScript handler does
the work and uses `TaskClient` for task-time Airflow data access. Register each
handler with the Python Dag's `dag_id` and the stub task's `task_id`. The
handler function is the reusable task implementation; `registerTask` binds that
handler to a Python stub Dag/task identity for coordinator mode.

For larger projects, keep one Airflow entrypoint that imports every module that
registers tasks, then starts the coordinator:

```ts
import "./sales/tasks";
import "./billing/tasks";
import { startCoordinator } from "@apache-airflow/ts-sdk";

await startCoordinator();
```

Airflow launches the bundled entrypoint with `--comm=host:port` and
`--logs=host:port`. `startCoordinator()` connects to those sockets, receives
the task startup message, finds the registered handler for the Dag/task pair,
and reports the terminal task state back to Airflow.

See [`example/`](example/) for a coordinator-runtime example that packs a
bundle with `airflow-ts-pack` and uses a Python stub Dag.

## Packing bundles

`airflow-ts-pack` produces everything `NodeCoordinator` needs in one command.
Packing is build-time only, so `esbuild` is an optional peer dependency the
runtime install skips:

```bash
npm install --save-dev esbuild
airflow-ts-pack src/main.ts --outdir dist
```

It bundles the entrypoint into `dist/bundle.mjs` with esbuild, runs the
bundle with `--airflow-metadata` so the bundle reports its own registered
Dag/task pairs and supervisor schema version, and embeds that manifest in the
bundle as a leading `//# airflowMetadata=<base64>` comment. The result is a
single deployable file whose metadata cannot drift from its code; no
hand-written sidecar is needed.

Options:

- `--outdir <dir>` — output directory (default `dist`)
- `--source <name>` — display name of the primary source file shown in the
  Airflow UI (default: entry basename)

## TaskClient

Every task handler receives a `TaskClient` for task-time Airflow data access:

| Method                                    | Description         |
| ----------------------------------------- | ------------------- |
| `getVariable(key)` / `getVariableOrThrow` | Airflow Variables   |
| `getXCom(opts)` / `setXCom(opts)`         | XCom read/write     |
| `getConnection(connId)`                   | Airflow Connections |

Locator fields such as `dagId`, `runId`, and `taskId` default to the
current task context when omitted.

## Cancellation

`ctx.signal` is an `AbortSignal` controlled by the active runtime. Pass it to
`fetch()`, timers, database clients, child processes, or other abortable APIs
so tasks can clean up cooperatively when Airflow terminates the task attempt.

## Compatibility matrix

Which Airflow TaskInstance states and capabilities this SDK supports. This table is generated from
[`capabilities.json`](capabilities.json) (the source of truth is `capabilities` in
[`src/conformance/capabilities.ts`](src/conformance/capabilities.ts)); the conformance dimensions are
defined in the
[Language SDK conformance spec](https://github.com/apache/airflow/blob/main/contributing-docs/30_new_language_sdk.rst).
Do not edit the table by hand — update the constant and regenerate.

<!-- BEGIN AUTO-GENERATED LANG-SDK COMPAT MATRIX -->

*Min. Airflow version: 3.3 · supervisor schema: 2026-06-16*

| Dimension | Tier | Supported | Since | Notes |
|---|---|---|---|---|
| **TaskInstance states** |  |  |  |  |
| state: `success` | MUST | ✓ | 3.3 |  |
| state: `failed` | MUST | ✓ | 3.3 |  |
| state: `up_for_retry` | MUST | ✓ | 3.3 | RetryTask |
| state: `skipped` | SHOULD | ✗ | – | runtime does not emit TaskState skipped yet |
| state: `deferred` | MAY | ✗ | – | runtime does not emit DeferTask yet |
| state: `up_for_reschedule` | MAY | ✗ | – | runtime does not emit RescheduleTask yet |
| state: `awaiting_input` | MAY | ✗ | – | runtime does not emit AwaitInputTask yet |
| state: `removed` | MAY | ✓ | 3.3 |  |
| **Runtime capabilities** |  |  |  |  |
| capability: `mixed-lang-stub-target` | MUST | ✓ | 3.3 | @task.stub |
| capability: `task-logging` | MUST | ✓ | 3.3 | structured records over the log socket |
| capability: `xcom-read-write` | MUST | ✓ | 3.3 | getXCom / setXCom |
| capability: `connection-read` | MUST | ✓ | 3.3 | getConnection |
| capability: `variable-read-write` | MUST | ✗ | – | getVariable only; no write over the comm socket yet |
| capability: `self-contained-bundle` | MUST | ✓ | 3.3 | Airflow metadata embedded in the bundle |
| capability: `task-state-store` | MAY | ✗ | – | no task-facing state-store API yet |
| capability: `asset-state-store` | MAY | ✗ | – | no task-facing state-store API yet |
| capability: `asset-event-emit` | MAY | ✗ | – | runtime does not emit asset events yet |
| capability: `asset-event-read` | MAY | ✗ | – | no task-facing asset-event API yet |
| **Native-Dag authoring** |  |  |  |  |
| capability: `native-dag-authoring` | SHOULD | ✗ | – | native Dag authoring not implemented yet |
| capability: `task-args` | MUST † | n/a | – |  |
| capability: `dag-params` | MUST † | n/a | – |  |
| capability: `taskflow-dependencies` | MUST † | n/a | – |  |
| capability: `branching` | SHOULD † | n/a | – |  |
| capability: `dag-test` | SHOULD † | n/a | – |  |
| capability: `task-group` | MAY † | n/a | – |  |
| capability: `dynamic-task-mapping` | MAY † | n/a | – |  |
| capability: `asset-inlets-outlets` | MAY † | n/a | – |  |
| capability: `asset-scheduling` | MAY † | n/a | – |  |
| capability: `object-store` | MAY | ✗ | – | no object-storage API yet |

*Marks: ✓ supported · ✗ not supported · n/a not applicable · – not published. A tier marked † applies only when `native-dag-authoring` is supported.*

<!-- END AUTO-GENERATED LANG-SDK COMPAT MATRIX -->

## Development

```bash
pnpm install
pnpm test
pnpm run typecheck
pnpm run build
```

Without a local pnpm install, [prek](https://prek.j178.dev) can compile the SDK
with its own managed node + pnpm toolchain:

```bash
prek run compile-ts-sdk
```
