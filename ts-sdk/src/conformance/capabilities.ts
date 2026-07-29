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

/** Support for a single Language SDK conformance dimension. */
export interface CapabilityEntry {
  supported: boolean;
  since: string | null;
  note: string;
}

/** The machine-readable capability manifest serialised to ts-sdk/capabilities.json. */
export interface CapabilitiesDoc {
  sdk: string;
  supervisor_schema_version: string;
  min_airflow_version: string;
  states: Record<string, CapabilityEntry>;
  capabilities: Record<string, CapabilityEntry>;
}

const MIN_AIRFLOW_VERSION = "3.3";

// Keep in sync with the supervisor schema the ts-sdk is generated against
// (ts-sdk/src/generated/supervisor.ts).
const SUPERVISOR_SCHEMA_VERSION = "2026-06-16";

const yes = (note = ""): CapabilityEntry => ({ supported: true, since: MIN_AIRFLOW_VERSION, note });
const no = (note = ""): CapabilityEntry => ({ supported: false, since: null, note });

/**
 * Single source of truth for what the TypeScript SDK supports. Update it when the runtime gains or
 * loses a conformance dimension, then regenerate capabilities.json. The normative meaning of each
 * dimension is defined in contributing-docs/30_new_language_sdk.rst.
 *
 * The runtime terminates a task with SucceedTask, RetryTask, or TaskState (failed/removed); it does
 * not yet emit skipped, DeferTask, RescheduleTask, or AwaitInputTask.
 */
export const capabilities: CapabilitiesDoc = {
  sdk: "ts",
  supervisor_schema_version: SUPERVISOR_SCHEMA_VERSION,
  min_airflow_version: MIN_AIRFLOW_VERSION,
  states: {
    success: yes(),
    failed: yes(),
    up_for_retry: yes("RetryTask"),
    skipped: no("runtime does not emit TaskState skipped yet"),
    deferred: no("runtime does not emit DeferTask yet"),
    up_for_reschedule: no("runtime does not emit RescheduleTask yet"),
    awaiting_input: no("runtime does not emit AwaitInputTask yet"),
    removed: yes(),
  },
  // Runtime capabilities reflect the task-facing coordinator client surface; native-Dag authoring
  // is not implemented yet, so every native capability is unsupported.
  capabilities: {
    "mixed-lang-stub-target": yes("@task.stub"),
    "task-logging": yes("structured records over the log socket"),
    "xcom-read-write": yes("getXCom / setXCom"),
    "connection-read": yes("getConnection"),
    "variable-read-write": no("getVariable only; no write over the comm socket yet"),
    "self-contained-bundle": yes("Airflow metadata embedded in the bundle"),
    "task-state-store": no("no task-facing state-store API yet"),
    "asset-state-store": no("no task-facing state-store API yet"),
    "asset-event-emit": no("runtime does not emit asset events yet"),
    "asset-event-read": no("no task-facing asset-event API yet"),
    "native-dag-authoring": no("native Dag authoring not implemented yet"),
    "task-args": no(),
    "dag-params": no(),
    "taskflow-dependencies": no(),
    "branching": no(),
    "dag-test": no(),
    "task-group": no(),
    "dynamic-task-mapping": no(),
    "asset-inlets-outlets": no(),
    "asset-scheduling": no(),
    "object-store": no("no object-storage API yet"),
  },
};
