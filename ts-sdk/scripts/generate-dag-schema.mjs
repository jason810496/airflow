#!/usr/bin/env node
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

// Codegen for the Dag authoring fields of `DagSpec` and `TaskSpec`.
//
// Reads the vendored Dag serialization schema (`schema/dag-schema.json`, kept
// in sync with `airflow-core/src/airflow/serialization/schema.json` by the
// `sync-ts-sdk-dag-schema` prek hook) and emits
// `src/generated/dag-schema-fields.ts`: the two field interfaces plus the
// table of schema keys and defaults the serializer needs.
//
// Field selection mirrors the Go SDK's `TaskSpec` generator
// (`go-sdk/bundle/bundlev1/gen`), as decision "Generated from the
// serialization schema" of
// `airflow-core/adr/lang-sdk/0008-taskflow-dag-dsl.md` requires. Every scalar
// property of the "operator" definition becomes a task field, in schema
// order, unless one of these rules skips it:
//
//   - "_"-prefixed keys are serializer internals (_task_module, _is_mapped, ...)
//   - keys in the definition's "required" list are written by the serializer
//     itself (task_type, ui_color, ...)
//   - "has_on_*" keys are flags derived from Python callbacks, not settable
//   - non-scalar keys (arrays, objects, anyOf, refs other than timedelta and
//     datetime) cannot be expressed as a scalar authoring field
//   - EXCLUDED_TASK_FIELDS entries are Python-only concerns, documented per key
//
// A new scalar key added to the schema therefore shows up in the regenerated
// file — visible in its diff — or must gain an EXCLUDED_TASK_FIELDS entry,
// instead of going silently missing. Dag-level fields come from the curated
// DAG_FIELDS allowlist instead, matching Go's hand-written `DagSpec`, but
// their types and defaults are still read from the schema so an allowlist
// entry cannot outlive the key it names.
//
// Three things differ from the Go generator, because TypeScript expresses
// them directly and Go cannot:
//
//   - Optionality is `?`, so a boolean whose schema default is true needs no
//     pointer to tell "unset" from an explicit false.
//   - `number` covers both integer and floating-point schema types, so
//     retry_exponential_backoff needs no per-key type override.
//   - Identity is positional — `new Dag(dagId)` and `dag.task(taskId, ...)` —
//     so dag_id and task_id stay serializer-owned here rather than being
//     re-exposed as spec fields the way Go's `TaskId` is.
//
// Names are the schema key in camelCase, matching the SDK's existing `dagId` /
// `taskId` spelling. Unlike Go, no initialism table is needed: TypeScript
// camelCase lowercases an acronym tail (`docMd`, `doXcomPush`).

import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const ROOT = join(HERE, "..");
const SCHEMA_PATH = join(ROOT, "schema/dag-schema.json");
const OUT_PATH = join(ROOT, "src/generated/dag-schema-fields.ts");

/**
 * Version the serializer stamps into a serialized Dag's `__version`.
 *
 * The schema cannot supply it: its root `allOf` only constrains `__version` to
 * a positive integer. A core bump therefore has to be mirrored here by hand,
 * and `airflow-core/tests/unit/serialization/test_ts_sdk_serialization_version.py`
 * fails until it is.
 */
export const SERIALIZATION_VERSION = 3;

/**
 * Scalar "operator" keys deliberately kept off `TaskSpec`, each with the
 * reason. Every other eligible scalar key becomes a field, so an entry that no
 * longer matches an eligible schema key fails generation and the list cannot
 * go stale.
 */
export const EXCLUDED_TASK_FIELDS = {
  doc: "legacy doc attribute; the UI renders Markdown, so only doc_md is exposed",
  doc_json: "legacy doc attribute; the UI renders Markdown, so only doc_md is exposed",
  doc_yaml: "legacy doc attribute; the UI renders Markdown, so only doc_md is exposed",
  doc_rst: "legacy doc attribute; the UI renders Markdown, so only doc_md is exposed",
  allow_nested_operators:
    "Python runtime concern: warns when an operator executes inside another operator",
  multiple_outputs: "TaskFlow (@task) dict-unpacking concern, meaningless outside Python",
  start_from_trigger:
    "deferrable-operator machinery; its start_trigger_args counterpart is an object the spec cannot express",
  is_setup: "setup/teardown flags carry trigger-rule invariants the SDK does not model yet",
  is_teardown: "setup/teardown flags carry trigger-rule invariants the SDK does not model yet",
  on_failure_fail_dagrun: "only valid on teardown tasks, which the SDK does not model yet",
};

/**
 * The Dag-level allowlist, in the order the fields are emitted.
 *
 * Hand-curated rather than derived, matching Go's `DagSpec`: the "dag"
 * definition mixes authoring options with bundle bookkeeping (fileloc,
 * bundle_name, ...) and structure the SDK builds itself (tasks, task_group,
 * edge_info), and no mechanical rule separates the two. Types and defaults
 * still come from the schema, so an entry naming a key the schema dropped
 * fails generation.
 *
 * `tsType` overrides the type for a key the schema types too loosely to map.
 * `mapsTo` marks a virtual field: one the authoring surface offers and the
 * serializer lowers onto a schema key of a different shape.
 */
export const DAG_FIELDS = [
  {
    name: "schedule",
    mapsTo: "timetable",
    tsType: "string",
    doc: 'When the Dag runs: `"@once"`, `"@continuous"`, a cron expression, or unset for no schedule. The serializer lowers it onto the schema\'s `timetable` object.',
  },
  { key: "description" },
  { key: "start_date" },
  { key: "end_date" },
  // The schema declares a bare array with no item type, so it cannot say this.
  { key: "tags", tsType: "readonly string[]", schemaType: "string[]" },
  { key: "dag_display_name" },
  { key: "doc_md" },
  { key: "max_active_tasks" },
  { key: "max_active_runs" },
  { key: "max_consecutive_failed_dag_runs" },
  { key: "dagrun_timeout" },
  { key: "catchup" },
  { key: "fail_fast" },
  { key: "render_template_as_native_obj" },
  { key: "disable_bundle_versioning" },
  { key: "is_paused_upon_creation" },
];

/** Whether the serializer, not the author, owns `key` in `definition`. */
function isSerializerOwned(key, required) {
  return key.startsWith("_") || required.has(key) || key.startsWith("has_on_");
}

/**
 * Map one schema property onto an authoring field, or return undefined when it
 * is not a scalar the spec can express (array, object, anyOf, or a `$ref`
 * other than timedelta/datetime).
 */
export function resolveField(key, prop) {
  const ref =
    typeof prop.$ref === "string" && prop.$ref.startsWith("#/definitions/")
      ? prop.$ref.slice("#/definitions/".length)
      : undefined;
  // A `"default": null` says "absent by default", which is what leaving the
  // field unset already means.
  const hasDefault = Object.hasOwn(prop, "default") && prop.default !== null;
  const base = { key, name: toCamelCase(key), ...(hasDefault && { default: prop.default }) };

  if (ref === "timedelta") return { ...base, tsType: "number", schemaType: "timedelta" };
  if (ref === "datetime") return { ...base, tsType: "Date", schemaType: "datetime" };
  if (ref !== undefined) return undefined;

  switch (prop.type) {
    case "string":
      return { ...base, tsType: "string", schemaType: "string" };
    case "integer":
    case "number":
      return { ...base, tsType: "number", schemaType: "number" };
    case "boolean":
      return { ...base, tsType: "boolean", schemaType: "boolean" };
    default:
      return undefined;
  }
}

function getDefinition(schema, name) {
  const definition = schema.definitions?.[name];
  if (!definition?.properties) {
    throw new Error(`schema has no "${name}" definition with properties`);
  }
  return definition;
}

/** Derive the `TaskSpec` fields from the "operator" definition, in schema order. */
export function selectTaskFields(schema) {
  const operator = getDefinition(schema, "operator");
  const required = new Set(operator.required ?? []);
  const matchedExclusions = new Set();
  const fields = [];

  for (const [key, prop] of Object.entries(operator.properties)) {
    if (isSerializerOwned(key, required)) continue;
    if (Object.hasOwn(EXCLUDED_TASK_FIELDS, key)) {
      matchedExclusions.add(key);
      continue;
    }
    const field = resolveField(key, prop);
    if (field) fields.push(field);
  }

  for (const key of Object.keys(EXCLUDED_TASK_FIELDS)) {
    if (!matchedExclusions.has(key)) {
      throw new Error(
        `EXCLUDED_TASK_FIELDS entry "${key}" matches no eligible schema property; remove or fix it`,
      );
    }
  }
  return fields;
}

/** Resolve the `DAG_FIELDS` allowlist against the "dag" definition, in allowlist order. */
export function selectDagFields(schema) {
  const dag = getDefinition(schema, "dag");
  const required = new Set(dag.required ?? []);

  return DAG_FIELDS.map((entry) => {
    if (entry.mapsTo !== undefined) {
      if (!Object.hasOwn(dag.properties, entry.mapsTo)) {
        throw new Error(
          `virtual Dag field "${entry.name}" lowers onto schema key "${entry.mapsTo}", which the "dag" definition no longer declares`,
        );
      }
      return {
        key: entry.mapsTo,
        name: entry.name,
        tsType: entry.tsType,
        schemaType: entry.tsType,
        doc: entry.doc,
        virtual: true,
      };
    }

    const prop = dag.properties[entry.key];
    if (prop === undefined) {
      throw new Error(
        `DAG_FIELDS entry "${entry.key}" is not a property of the "dag" definition; remove or fix it`,
      );
    }
    if (isSerializerOwned(entry.key, required)) {
      throw new Error(`DAG_FIELDS entry "${entry.key}" is serializer-owned and cannot be authored`);
    }

    const field = resolveField(entry.key, prop);
    if (entry.tsType) {
      return {
        ...(field ?? { key: entry.key, name: toCamelCase(entry.key) }),
        tsType: entry.tsType,
        schemaType: entry.schemaType ?? entry.tsType,
      };
    }
    if (!field) {
      throw new Error(
        `DAG_FIELDS entry "${entry.key}" is not a scalar the spec can express; give it a tsType override or remove it`,
      );
    }
    return field;
  });
}

/** `max_active_tis_per_dag` -> `maxActiveTisPerDag`. */
export function toCamelCase(key) {
  return key.replace(/_(.)/g, (_match, char) => char.toUpperCase());
}

const LICENSE_HEADER = `/*!
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

// AUTO-GENERATED by scripts/generate-dag-schema.mjs — do not edit by hand.
// Source: schema/dag-schema.json, vendored from
// airflow-core/src/airflow/serialization/schema.json.
//
// Re-run with: pnpm run generate:dag-schema
`;

/** Render one field's doc comment: what it maps to, and its schema default. */
function renderFieldDoc(field) {
  if (field.doc) return `  /** ${field.doc} */`;
  const unit = field.schemaType === "timedelta" ? ", in seconds" : "";
  const suffix =
    field.default === undefined ? "" : ` (schema default \`${JSON.stringify(field.default)}\`)`;
  return `  /** Maps to the schema key \`${field.key}\`${unit}${suffix}. */`;
}

function renderInterface(name, doc, fields) {
  const members = fields.flatMap((field) => [
    renderFieldDoc(field),
    `  readonly ${field.name}?: ${field.tsType};`,
  ]);
  return `${doc}\nexport interface ${name} {\n${members.join("\n")}\n}\n`;
}

function renderTable(name, doc, typeName, fields) {
  const entries = fields.map((field) => {
    const parts = [
      `key: ${JSON.stringify(field.key)}`,
      `type: ${JSON.stringify(field.schemaType)}`,
    ];
    if (field.default !== undefined) parts.push(`default: ${JSON.stringify(field.default)}`);
    if (field.virtual) parts.push("virtual: true");
    return `  ${field.name}: { ${parts.join(", ")} },`;
  });
  return `${doc}\nexport const ${name} = {\n${entries.join(
    "\n",
  )}\n} as const satisfies Readonly<Record<keyof ${typeName}, SchemaField>>;\n`;
}

export function renderModule(dagFields, taskFields) {
  return [
    LICENSE_HEADER,
    renderInterface(
      "GeneratedDagFields",
      `/**
 * Dag-level authoring fields, from the curated allowlist in
 * scripts/generate-dag-schema.mjs resolved against the "dag" definition.
 *
 * Every field is optional: leaving one unset means the scheduler applies its
 * own default, so \`{}\` stays a valid spec and a field added here later cannot
 * break an existing call site.
 */`,
      dagFields,
    ),
    renderInterface(
      "GeneratedTaskFields",
      `/**
 * Task-level authoring fields, from the scalar properties of the "operator"
 * definition that the author rather than the serializer owns.
 *
 * Optional on the same terms as {@link GeneratedDagFields}.
 */`,
      taskFields,
    ),
    `/** How a field is carried in the serialized Dag. \`datetime\` is fractional
 *  seconds since the epoch and \`timedelta\` is seconds; both are written as
 *  numbers. */
export type SchemaFieldType = "string" | "number" | "boolean" | "string[]" | "datetime" | "timedelta";

/** What the serializer needs to know about one authoring field. */
export interface SchemaField {
  /** The key this field takes in the serialized Dag. */
  readonly key: string;
  readonly type: SchemaFieldType;
  /** The schema's default, absent when it declares none. A value equal to it
   *  can be omitted, as Python's BaseSerialization omits what the scheduler
   *  re-derives. */
  readonly default?: string | number | boolean;
  /** Set when the serializer derives \`key\` from this field rather than
   *  writing the value straight through. */
  readonly virtual?: boolean;
}
`,
    renderTable(
      "DAG_SCHEMA_FIELDS",
      "/** Schema key, wire type and default of every {@link GeneratedDagFields} field. */",
      "GeneratedDagFields",
      dagFields,
    ),
    renderTable(
      "TASK_SCHEMA_FIELDS",
      "/** Schema key, wire type and default of every {@link GeneratedTaskFields} field. */",
      "GeneratedTaskFields",
      taskFields,
    ),
    `/** The \`__version\` the serializer stamps into a serialized Dag. The schema
 *  constrains it only to a positive integer, so it is pinned here and guarded
 *  by a test in airflow-core. */
export const SERIALIZATION_VERSION = ${SERIALIZATION_VERSION};
`,
  ].join("\n");
}

function main() {
  const schema = JSON.parse(readFileSync(SCHEMA_PATH, "utf8"));
  const dagFields = selectDagFields(schema);
  const taskFields = selectTaskFields(schema);

  mkdirSync(dirname(OUT_PATH), { recursive: true });
  writeFileSync(OUT_PATH, renderModule(dagFields, taskFields), "utf8");

  console.log(`wrote ${OUT_PATH}`);
  console.log(`  dag fields=${dagFields.length}, task fields=${taskFields.length}`);
}

// Importable for tests; runs only when invoked as a script.
if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) {
  main();
}
