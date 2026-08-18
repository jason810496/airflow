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

// Builds the Dags of test_dags.json with this SDK's authoring API, the way
// serialize_python.py builds them with Python's. Shared by conformance.test.ts
// and serialize_typescript.ts so both describe the same Dags.

import {
  DAG_SCHEMA_FIELDS,
  TASK_SCHEMA_FIELDS,
  type SchemaField,
} from "../../src/generated/dag-schema-fields.js";
import { Dag, type DagSpec, type TaskRef, type TaskSpec } from "../../src/sdk/dag.js";
import type { TaskHandlerArgs } from "../../src/sdk/task.js";

import fixtures from "./test_dags.json" with { type: "json" };

export interface TaskFixture {
  readonly task_id: string;
  readonly upstream: readonly string[];
  readonly spec: Readonly<Record<string, unknown>>;
}

export interface DagFixture {
  readonly dag_id: string;
  readonly spec: Readonly<Record<string, unknown>>;
  readonly tasks: readonly TaskFixture[];
}

export const FIXTURE_DAGS: readonly DagFixture[] = (fixtures as unknown as { dags: DagFixture[] })
  .dags;

/**
 * Invert a generated table into the map the fixtures are written against.
 *
 * Keyed by schema key, except for a virtual field, whose schema key names what
 * the serializer derives rather than what a Dag author sets — `schedule` is
 * spelled that way in both Python's `DAG()` and this SDK, but its schema key is
 * `timetable`.
 */
function bySchemaKey(
  table: Readonly<Record<string, SchemaField>>,
): Map<string, [string, SchemaField]> {
  return new Map(
    Object.entries(table).map(([name, field]) => [field.virtual ? name : field.key, [name, field]]),
  );
}

export const DAG_FIELDS_BY_KEY = bySchemaKey(DAG_SCHEMA_FIELDS);
export const TASK_FIELDS_BY_KEY = bySchemaKey(TASK_SCHEMA_FIELDS);

/** Decode the fixture's tagged scalars, mirroring serialize_python.py's `decode`. */
function decode(value: unknown): unknown {
  if (value !== null && typeof value === "object" && !Array.isArray(value)) {
    const tagged = value as Record<string, unknown>;
    if ("__datetime" in tagged) return new Date(String(tagged["__datetime"]));
    // A duration is already a number of seconds in this SDK's authoring API.
    if ("__timedelta" in tagged) return tagged["__timedelta"];
  }
  return value;
}

/** Rewrite a fixture spec, keyed by schema name, as this SDK's authoring spec. */
function toAuthoringSpec(
  spec: Readonly<Record<string, unknown>>,
  fieldsByKey: Map<string, [string, SchemaField]>,
  label: string,
): Record<string, unknown> {
  const authoring: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(spec)) {
    const entry = fieldsByKey.get(key);
    if (!entry) throw new Error(`${label}: fixture sets "${key}", which is not an authoring field`);
    authoring[entry[0]] = decode(value);
  }
  return authoring;
}

export function buildFixtureDag(fixture: DagFixture): Dag {
  const dag = new Dag(
    fixture.dag_id,
    toAuthoringSpec(fixture.spec, DAG_FIELDS_BY_KEY, fixture.dag_id) as DagSpec,
  );
  const factories = new Map<string, (inputs: Record<string, TaskRef>) => TaskRef>();
  for (const task of fixture.tasks) {
    const spec = toAuthoringSpec(
      task.spec,
      TASK_FIELDS_BY_KEY,
      `${fixture.dag_id}.${task.task_id}`,
    ) as TaskSpec;
    factories.set(
      task.task_id,
      dag.task(
        task.task_id,
        async (_args: Record<string, TaskRef> & TaskHandlerArgs) => undefined,
        {
          spec,
        },
      ),
    );
  }
  // Fixtures list a task after the tasks it depends on, so every reference a
  // task needs already exists by the time it is placed.
  const refs = new Map<string, TaskRef>();
  for (const task of fixture.tasks) {
    const inputs: Record<string, TaskRef> = {};
    for (const upstream of task.upstream) {
      const ref = refs.get(upstream);
      if (!ref) throw new Error(`${fixture.dag_id}: "${upstream}" is placed after its downstream`);
      inputs[`from_${upstream}`] = ref;
    }
    refs.set(task.task_id, factories.get(task.task_id)!(inputs));
  }
  return dag;
}

export function buildFixtureDags(all: readonly DagFixture[]): Dag[] {
  return all.map(buildFixtureDag);
}
