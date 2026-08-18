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

// Turns a Dag declared in TypeScript into Airflow's DagSerialization v3 JSON —
// what the Dag processor stores and the scheduler reads. The format is
// Airflow-internal rather than an SDK schema, so it is reimplemented per
// language against `airflow-core/src/airflow/serialization/schema.json`; see
// `airflow-core/adr/lang-sdk/0004-dag-parsing.md` for the field table this
// follows, and the Java SDK's `Serde.kt` for the same job in another language.
//
// Byte-parity with Python's serializer is not the goal — Python omits fields
// against a `client_defaults` table this SDK does not receive. What has to hold
// is that `DagSerialization.from_dict` rebuilds the same Dag, which
// tests/coordinator/conformance.test.ts pins against recorded Python output.

import { relative as relativePath } from "node:path";

import {
  DAG_SCHEMA_FIELDS,
  SERIALIZATION_VERSION,
  TASK_SCHEMA_FIELDS,
  type SchemaField,
} from "../generated/dag-schema-fields.js";
import type { JsonValue } from "../sdk/client-types.js";
import { getDagTaskInputs, getDagTaskRecords, isTaskRef, type Dag } from "../sdk/dag.js";
import { listRegistryDagObjects, type DagRegistry } from "../sdk/registry.js";
import type { RuntimeDagFileParsingResult } from "./protocol.js";

/** A serialized Dag: JSON, by the time it reaches the supervisor as msgpack. */
type SerializedValue = JsonValue;

/** Airflow's type/var encoding, as `BaseSerialization.serialize()` emits it. */
interface TypeEncoded {
  readonly __type: string;
  readonly __var: SerializedValue;
}

/**
 * Identity every TypeScript task carries, in place of the Python operator class
 * a Python Dag would name.
 *
 * Fixed rather than derived: nothing on the Airflow side imports `_task_module`
 * — `SerializedBaseOperator.populate_operator` only compares the pair as
 * strings when matching plugin extra links — so the pair is free to name the
 * coordinator that actually runs the task, which makes every TypeScript task
 * greppable in the UI and the metadata DB.
 */
const TASK_TYPE = "TypeScriptOperator";
const TASK_MODULE = "airflow.sdk.coordinators.node";

/**
 * Marks the tasks this SDK serialized, as the Java SDK marks its own.
 *
 * Nothing in airflow-core reads it today; the `operator` schema definition
 * allows additional properties, so it rides along as a marker for tooling that
 * wants to tell language-native tasks apart without parsing `_task_module`.
 */
const TASK_LANGUAGE = "typescript";

// Python resolves these from [core]/[scheduler] config when the Dag leaves them
// unset, and its serializer always writes the resolved value — there is no
// schema default to omit against. A bundle cannot read airflow.cfg, so the
// stock defaults stand in.
const DAG_CONFIG_FALLBACKS: Readonly<Record<string, SerializedValue>> = {
  max_active_tasks: 16, // [core] max_active_tasks_per_dag
  max_active_runs: 16, // [core] max_active_runs_per_dag
  max_consecutive_failed_dag_runs: 0,
  catchup: false, // [scheduler] catchup_by_default
  disable_bundle_versioning: false,
};

/** How one set of authoring fields is written into a serialized object. */
interface FieldRules {
  readonly fields: Readonly<Record<string, SchemaField>>;
  /**
   * Fields whose {__type, __var} wrapper survives; every other field is
   * unwrapped to the bare __var, as Python's `serialize_to_json` does.
   *
   * Neither set overlaps the generated authoring fields today, so in practice
   * everything is unwrapped. They are named so that a decorated field added to
   * the schema later takes the right path rather than silently losing its
   * wrapper. Source: `DagSerialization._decorated_fields` and
   * `OperatorSerialization._decorated_fields`.
   */
  readonly decorated: ReadonlySet<string>;
  /** Fields never written, whatever the author set. */
  readonly omitted: ReadonlySet<string>;
}

const DAG_FIELD_RULES: FieldRules = {
  fields: DAG_SCHEMA_FIELDS,
  decorated: new Set(["default_args", "access_control"]),
  omitted: new Set(),
};

const TASK_FIELD_RULES: FieldRules = {
  fields: TASK_SCHEMA_FIELDS,
  decorated: new Set(["executor_config"]),
  // Python drops both unless the operator names an email recipient
  // (`OperatorSerialization._serialize_node`). A TypeScript task has no `email`
  // field to name one, so writing them would describe a notification that can
  // never be sent.
  omitted: new Set(["email_on_failure", "email_on_retry"]),
};

const NULL_TIMETABLE = "airflow.timetables.simple.NullTimetable";
const ONCE_TIMETABLE = "airflow.timetables.simple.OnceTimetable";
const CONTINUOUS_TIMETABLE = "airflow.timetables.simple.ContinuousTimetable";
const CRON_TIMETABLE = "airflow.timetables.trigger.CronTriggerTimetable";

/**
 * Answer a `DagFileParseRequest` with every Dag the bundle declared natively.
 *
 * A mixed-language Dag is left out: its graph belongs to the Python Dag file
 * that declares it, and emitting it here would register a second Dag with the
 * same `dag_id` from a different `fileloc`.
 */
export function parseDags(
  registry: DagRegistry,
  request: { file: string; bundle_path: string },
): RuntimeDagFileParsingResult {
  const fileloc = request.file;
  const relativeFileloc = computeRelativeFileloc(fileloc, request.bundle_path);
  const serializedDags = listSerializableDags(registry).map((dag) => ({
    data: {
      __version: SERIALIZATION_VERSION,
      dag: serializeDag(dag, fileloc, relativeFileloc),
    },
  }));
  return {
    type: "DagFileParsingResult",
    fileloc,
    serialized_dags: serializedDags,
  };
}

/**
 * Internal: the Dags of `registry` this SDK serializes.
 *
 * A mixed-language Dag is not one of them — see {@link parseDags} — and the
 * filter lives here so the parse response and what the runtime logs cannot
 * disagree about which Dags the bundle served.
 */
export function listSerializableDags(registry: DagRegistry): Dag[] {
  return listRegistryDagObjects(registry).filter((dag) => !dag.spec.isMixedLanguageDag);
}

/** Serialize one Dag to the `dag` object of a DagSerialization v3 payload. */
export function serializeDag(
  dag: Dag,
  fileloc: string,
  relativeFileloc: string,
): Record<string, SerializedValue> {
  const downstream = collectDownstreamTaskIds(dag);
  const taskIds = [...getDagTaskRecords(dag).keys()];
  const data: Record<string, SerializedValue> = {
    dag_id: dag.dagId,
    fileloc,
    relative_fileloc: relativeFileloc,
    timezone: "UTC",
    timetable: serializeTimetable(dag.spec.schedule, dag.dagId),
    tasks: [...getDagTaskRecords(dag)].map(([taskId, record]) =>
      serializeTask(dag.dagId, taskId, record.spec, downstream.get(taskId)),
    ),
    dag_dependencies: [],
    task_group: serializeTaskGroup(taskIds),
    edge_info: {},
    params: [],
    // Always written by Python's serializer, so a Dag without either still
    // round-trips to the same object.
    deadline: null,
    allowed_run_types: null,
  };
  applySchemaFields(data, dag.spec, DAG_FIELD_RULES, `Dag "${dag.dagId}"`);
  for (const [key, fallback] of Object.entries(DAG_CONFIG_FALLBACKS)) {
    data[key] ??= fallback;
  }
  return data;
}

/** Serialize one task, with its downstream edges sorted for a stable payload. */
function serializeTask(
  dagId: string,
  taskId: string,
  spec: object,
  downstream: ReadonlySet<string> | undefined,
): SerializedValue {
  const data: Record<string, SerializedValue> = {
    task_id: taskId,
    task_type: TASK_TYPE,
    _task_module: TASK_MODULE,
    language: TASK_LANGUAGE,
    // Python's operator serializer always emits this — its list value never
    // matches the tuple default it is compared against. A TypeScript task has
    // no Jinja templating, so the list is empty rather than absent.
    template_fields: [],
  };
  applySchemaFields(data, spec, TASK_FIELD_RULES, `task "${taskId}" of Dag "${dagId}"`);
  if (downstream?.size) {
    data["downstream_task_ids"] = [...downstream].sort();
  }
  return { __type: "operator", __var: data };
}

/** The flat root group holding every task. Nested TaskGroups are not authorable
 *  in TypeScript yet, so the group is always the root one Python builds for a
 *  Dag whose tasks all sit at the top level. */
function serializeTaskGroup(taskIds: readonly string[]): SerializedValue {
  const children: Record<string, SerializedValue> = {};
  for (const taskId of taskIds) {
    children[taskId] = ["operator", taskId];
  }
  return {
    _group_id: null,
    group_display_name: "",
    prefix_group_id: true,
    tooltip: "",
    ui_color: "CornflowerBlue",
    ui_fgcolor: "#000",
    children,
    upstream_group_ids: [],
    downstream_group_ids: [],
    upstream_task_ids: [],
    downstream_task_ids: [],
  };
}

/**
 * Lower a `schedule` onto the timetable the scheduler reconstructs.
 *
 * Only the four schedules that map to a stock timetable are accepted. Anything
 * else — an asset expression, a custom timetable — is a Python object the
 * scheduler has to import, which a TypeScript bundle cannot name, so it is
 * rejected here rather than serialized into a Dag that fails to deserialize.
 */
function serializeTimetable(schedule: unknown, dagId: string): SerializedValue {
  if (schedule === undefined || schedule === null) {
    return simpleTimetable(NULL_TIMETABLE);
  }
  if (typeof schedule !== "string") {
    throw new Error(
      `schedule for Dag "${dagId}" must be "@once", "@continuous", or a cron expression; ` +
        `${describeType(schedule)} schedule names a Python object this SDK cannot serialize`,
    );
  }
  if (schedule.trim() === "") {
    throw new Error(
      `schedule for Dag "${dagId}" is empty; leave it unset for a Dag with no schedule`,
    );
  }
  if (schedule === "@once") return simpleTimetable(ONCE_TIMETABLE);
  if (schedule === "@continuous") return simpleTimetable(CONTINUOUS_TIMETABLE);
  // Every other string is a cron expression, including a preset such as
  // "@daily"; the scheduler parses it when it rebuilds the timetable.
  //
  // TODO: honour [scheduler] create_cron_data_intervals, which switches Python
  // to CronDataIntervalTimetable. A bundle cannot read airflow.cfg, so the
  // supervisor has to send the flag first; tracked at
  // https://github.com/apache/airflow/issues/67938
  return {
    __type: CRON_TIMETABLE,
    __var: { expression: schedule, timezone: "UTC", interval: 0, run_immediately: false },
  };
}

function simpleTimetable(type: string): SerializedValue {
  return { __type: type, __var: {} };
}

/**
 * Write the fields a spec set onto `data`, skipping any left at its schema
 * default — Python's serializer omits what the scheduler re-derives.
 */
function applySchemaFields(
  data: Record<string, SerializedValue>,
  spec: object,
  rules: FieldRules,
  label: string,
): void {
  const values = spec as Record<string, unknown>;
  for (const [name, field] of Object.entries(rules.fields)) {
    // A virtual field names a key the serializer derives rather than writes;
    // `schedule` becomes `timetable`.
    if (field.virtual || rules.omitted.has(field.key)) continue;
    const value = values[name];
    if (value === undefined || value === field.default) continue;
    const encoded = encodeField(field, value, `${name} for ${label}`);
    data[field.key] = rules.decorated.has(field.key) ? encoded : unwrapTypeEncoding(encoded);
  }
}

/** Encode one authoring value as the schema's type for that field. Rejects a
 *  value of the wrong type: this is the last point before the scheduler, and a
 *  mistyped field would otherwise surface as an unreadable Dag. */
function encodeField(field: SchemaField, value: unknown, label: string): SerializedValue {
  switch (field.type) {
    case "string":
      if (typeof value !== "string") throw typeError(label, "a string", value);
      return value;
    case "boolean":
      if (typeof value !== "boolean") throw typeError(label, "a boolean", value);
      return value;
    case "number":
      if (typeof value !== "number" || !Number.isFinite(value)) {
        throw typeError(label, "a finite number", value);
      }
      return value;
    case "timedelta":
      if (typeof value !== "number" || !Number.isFinite(value)) {
        throw typeError(label, "a duration in seconds", value);
      }
      return { __type: "timedelta", __var: value };
    case "datetime":
      if (!(value instanceof Date) || Number.isNaN(value.getTime())) {
        throw typeError(label, "a valid Date", value);
      }
      return serializeValue(value);
    case "string[]": {
      if (!Array.isArray(value) || value.some((item) => typeof item !== "string")) {
        throw typeError(label, "an array of strings", value);
      }
      // Python holds these in a set, so duplicates collapse and the order is
      // the sorted one that keeps a Dag's hash stable across runs.
      return serializeValue(new Set(value as string[]));
    }
  }
}

function typeError(label: string, expected: string, value: unknown): Error {
  return new Error(`${label} must be ${expected}, not ${describeType(value)}`);
}

function describeType(value: unknown): string {
  if (value === null) return "null";
  if (Array.isArray(value)) return "an array";
  if (value instanceof Date) return "a Date";
  if (value instanceof Set) return "a Set";
  const noun = typeof value;
  return `${/^[aeiou]/.test(noun) ? "an" : "a"} ${noun}`;
}

/**
 * Encode a value the way `BaseSerialization.serialize()` does.
 *
 * A duration has no distinct runtime type in TypeScript — it is a number of
 * seconds — so `timedelta` is applied by {@link encodeField} from the schema
 * rather than inferred here.
 */
export function serializeValue(value: unknown): SerializedValue {
  if (value === null || value === undefined) return null;
  if (typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new Error(`Cannot serialize the non-finite number ${String(value)}`);
    }
    return value;
  }
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) throw new Error("Cannot serialize an invalid Date");
    return { __type: "datetime", __var: value.getTime() / 1000 };
  }
  if (value instanceof Set) {
    return {
      __type: "set",
      __var: [...value].map(serializeValue).sort(compareSerialized),
    };
  }
  if (Array.isArray(value)) return value.map(serializeValue);
  if (value instanceof Map) {
    return { __type: "dict", __var: serializeEntries(value.entries()) };
  }
  if (typeof value === "object") {
    return { __type: "dict", __var: serializeEntries(Object.entries(value)) };
  }
  throw new Error(`Cannot serialize a ${typeof value}`);
}

function serializeEntries(entries: Iterable<[unknown, unknown]>): Record<string, SerializedValue> {
  const encoded: Record<string, SerializedValue> = {};
  for (const [key, item] of entries) {
    encoded[String(key)] = serializeValue(item);
  }
  return encoded;
}

// Python sorts a set's members before writing them; JSON's default sort is
// lexicographic on the string form, which matches for the string sets this
// SDK produces and stays total for anything else.
function compareSerialized(left: SerializedValue, right: SerializedValue): number {
  const a = typeof left === "string" ? left : JSON.stringify(left);
  const b = typeof right === "string" ? right : JSON.stringify(right);
  return a < b ? -1 : a > b ? 1 : 0;
}

/**
 * Strip the type encoding from a non-decorated field, as Python's
 * `serialize_to_json` does: it serializes every field, then keeps only the
 * `__var` of the ones outside its decorated set.
 */
export function unwrapTypeEncoding(value: SerializedValue): SerializedValue {
  if (!isTypeEncoded(value)) return value;
  return value.__var;
}

function isTypeEncoded(value: SerializedValue): value is TypeEncoded & SerializedValue {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    "__type" in value &&
    "__var" in value
  );
}

/** Where the Dag file sits inside its bundle, as Airflow records it. */
export function computeRelativeFileloc(fileloc: string, bundlePath: string): string {
  if (!fileloc) return "";
  if (!bundlePath) return ".";
  const result = relativePath(bundlePath, fileloc);
  return result === "" ? "." : result;
}

/** Invert the recorded inputs into each task's set of downstream task IDs. */
function collectDownstreamTaskIds(dag: Dag): Map<string, Set<string>> {
  const downstream = new Map<string, Set<string>>();
  for (const [taskId, inputs] of getDagTaskInputs(dag)) {
    for (const value of Object.values(inputs)) {
      if (!isTaskRef(value)) continue;
      // Two arguments fed by the same upstream are one edge.
      const edges = downstream.get(value.taskId) ?? new Set<string>();
      edges.add(taskId);
      downstream.set(value.taskId, edges);
    }
  }
  return downstream;
}
