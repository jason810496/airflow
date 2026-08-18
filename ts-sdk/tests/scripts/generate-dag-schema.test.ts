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

import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import {
  DAG_SCHEMA_FIELDS,
  SERIALIZATION_VERSION,
  TASK_SCHEMA_FIELDS,
} from "../../src/generated/dag-schema-fields.js";
import {
  DAG_FIELDS,
  EXCLUDED_TASK_FIELDS,
  renderModule,
  resolveField,
  selectDagFields,
  selectTaskFields,
  toCamelCase,
} from "../../scripts/generate-dag-schema.mjs";

const SCHEMA_PATH = join(dirname(fileURLToPath(import.meta.url)), "../../schema/dag-schema.json");

interface SchemaDefinition {
  required?: string[];
  properties: Record<string, Record<string, unknown>>;
}

/** The vendored schema the checked-in `src/generated/dag-schema-fields.ts` was built from. */
function readSchema(): { definitions: { dag: SchemaDefinition; operator: SchemaDefinition } } {
  return JSON.parse(readFileSync(SCHEMA_PATH, "utf8"));
}

/** A minimal schema shaped like the real one, for the drift cases. */
function buildSchema(overrides: {
  operator?: Record<string, unknown>;
  dag?: Record<string, unknown>;
}) {
  return {
    definitions: {
      operator: { required: ["task_id"], properties: { task_id: { type: "string" } } },
      dag: { required: ["dag_id"], properties: { timetable: { type: "object" } } },
      ...overrides,
    },
  };
}

describe("task field selection", () => {
  it("exposes every author-owned scalar of the operator definition, in schema order", () => {
    expect(selectTaskFields(readSchema()).map((field) => field.name)).toEqual([
      "owner",
      "startDate",
      "endDate",
      "triggerRule",
      "dependsOnPast",
      "ignoreFirstDependsOnPast",
      "waitForPastDependsBeforeSkipping",
      "waitForDownstream",
      "retries",
      "queue",
      "pool",
      "poolSlots",
      "executionTimeout",
      "retryDelay",
      "retryExponentialBackoff",
      "maxRetryDelay",
      "priorityWeight",
      "weightRule",
      "executor",
      "doXcomPush",
      "emailOnFailure",
      "emailOnRetry",
      "docMd",
      "mapIndexTemplate",
      "maxActiveTisPerDag",
      "maxActiveTisPerDagrun",
    ]);
  });

  it.each([
    ["a schema-required key the serializer writes", "task_type"],
    ["the positionally supplied task id", "task_id"],
    ["a serializer internal", "_task_module"],
    ["a callback-derived flag", "has_on_failure_callback"],
    ["an array", "inlets"],
    ["an object ref", "executor_config"],
    ["an anyOf", "render_template_as_native_obj"],
    ["an excluded Python-only concern", "is_setup"],
  ])("skips %s", (_label, schemaKey) => {
    const keys = selectTaskFields(readSchema()).map((field) => field.key);
    expect(keys).not.toContain(schemaKey);
  });

  it("fails when an exclusion no longer matches an eligible schema key", () => {
    const [excluded] = Object.keys(EXCLUDED_TASK_FIELDS);
    const schema = buildSchema({
      operator: {
        required: [],
        properties: Object.fromEntries(
          Object.keys(EXCLUDED_TASK_FIELDS)
            .filter((key) => key !== excluded)
            .map((key) => [key, { type: "boolean" }]),
        ),
      },
    });
    expect(() => selectTaskFields(schema)).toThrowError(
      new RegExp(`EXCLUDED_TASK_FIELDS entry "${excluded}" matches no eligible schema property`),
    );
  });

  it("fails when the operator definition is gone", () => {
    expect(() => selectTaskFields({ definitions: {} })).toThrowError(
      /schema has no "operator" definition/,
    );
  });
});

describe("Dag field selection", () => {
  it("resolves the curated allowlist, in allowlist order", () => {
    expect(selectDagFields(readSchema()).map((field) => field.name)).toEqual([
      "schedule",
      "description",
      "startDate",
      "endDate",
      "tags",
      "dagDisplayName",
      "docMd",
      "maxActiveTasks",
      "maxActiveRuns",
      "maxConsecutiveFailedDagRuns",
      "dagrunTimeout",
      "catchup",
      "failFast",
      "renderTemplateAsNativeObj",
      "disableBundleVersioning",
      "isPausedUponCreation",
    ]);
  });

  it("keeps the allowlist within what the dag definition actually declares", () => {
    const declared = Object.keys(readSchema().definitions.dag.properties);
    for (const entry of DAG_FIELDS) {
      expect(declared).toContain(entry.mapsTo ?? entry.key);
    }
  });

  it("lowers the virtual schedule field onto the timetable key", () => {
    const schedule = selectDagFields(readSchema()).find((field) => field.name === "schedule");
    expect(schedule).toMatchObject({ key: "timetable", tsType: "string", virtual: true });
  });

  it("types tags from the override, since the schema declares a bare array", () => {
    const tags = selectDagFields(readSchema()).find((field) => field.name === "tags");
    expect(tags).toMatchObject({ tsType: "readonly string[]", schemaType: "string[]" });
  });

  it.each([
    [
      "the virtual field's target is gone",
      { dag: { properties: { catchup: { type: "boolean" } } } },
      /virtual Dag field "schedule" lowers onto schema key "timetable"/,
    ],
    [
      "an allowlisted key is gone",
      { dag: { properties: { timetable: { type: "object" } } } },
      /DAG_FIELDS entry "description" is not a property of the "dag" definition/,
    ],
    [
      "an allowlisted key became serializer-owned",
      {
        dag: {
          required: ["description"],
          properties: { timetable: { type: "object" }, description: { type: "string" } },
        },
      },
      /DAG_FIELDS entry "description" is serializer-owned/,
    ],
    [
      "an allowlisted key stopped being expressible and has no override",
      {
        dag: {
          properties: { timetable: { type: "object" }, description: { type: "array" } },
        },
      },
      /DAG_FIELDS entry "description" is not a scalar the spec can express/,
    ],
  ])("fails when %s", (_label, overrides, expected) => {
    expect(() => selectDagFields(buildSchema(overrides))).toThrowError(expected);
  });

  it("fails when the dag definition is gone", () => {
    expect(() => selectDagFields({ definitions: {} })).toThrowError(
      /schema has no "dag" definition/,
    );
  });
});

describe("property resolution", () => {
  it.each([
    ["a string", { type: "string" }, "string", "string"],
    ["an integer", { type: "integer" }, "number", "number"],
    ["a number", { type: "number" }, "number", "number"],
    ["a boolean", { type: "boolean" }, "boolean", "boolean"],
    ["a timedelta ref", { $ref: "#/definitions/timedelta" }, "number", "timedelta"],
    ["a datetime ref", { $ref: "#/definitions/datetime" }, "Date", "datetime"],
  ])("maps %s", (_label, prop, tsType, schemaType) => {
    expect(resolveField("some_key", prop)).toMatchObject({ tsType, schemaType });
  });

  it.each([
    ["an array", { type: "array" }],
    ["an object", { type: "object" }],
    ["an anyOf", { anyOf: [{ type: "string" }] }],
    ["a ref the spec cannot express", { $ref: "#/definitions/dict" }],
  ])("cannot express %s", (_label, prop) => {
    expect(resolveField("some_key", prop)).toBeUndefined();
  });

  it("carries a declared default, but reads an explicit null as no default", () => {
    expect(resolveField("k", { type: "string", default: "x" })).toHaveProperty("default", "x");
    expect(resolveField("k", { type: "string", default: null })).not.toHaveProperty("default");
    expect(resolveField("k", { type: "string" })).not.toHaveProperty("default");
  });

  it("camel-cases schema keys without special-casing acronyms", () => {
    expect(toCamelCase("max_active_tis_per_dag")).toBe("maxActiveTisPerDag");
    expect(toCamelCase("do_xcom_push")).toBe("doXcomPush");
    expect(toCamelCase("owner")).toBe("owner");
  });
});

describe("the emitted module", () => {
  it("documents the schema key, the seconds unit and the default of each field", () => {
    const rendered = renderModule(
      [{ key: "d", name: "d", tsType: "string", schemaType: "string", doc: "Custom." }],
      [
        {
          key: "retry_delay",
          name: "retryDelay",
          tsType: "number",
          schemaType: "timedelta",
          default: 300,
        },
        { key: "executor", name: "executor", tsType: "string", schemaType: "string" },
      ],
    );
    expect(rendered).toContain("/** Custom. */");
    expect(rendered).toContain(
      "/** Maps to the schema key `retry_delay`, in seconds (schema default `300`). */",
    );
    expect(rendered).toContain("/** Maps to the schema key `executor`. */");
    expect(rendered).toContain("readonly executor?: string;");
  });

  it("pins the serialization version the schema cannot express", () => {
    expect(SERIALIZATION_VERSION).toBe(3);
  });
});

describe("the field table", () => {
  it("gives every authoring field its schema key and wire type", () => {
    for (const table of [DAG_SCHEMA_FIELDS, TASK_SCHEMA_FIELDS]) {
      for (const [name, field] of Object.entries(table)) {
        expect(field.key, name).toBeTruthy();
        expect(
          ["string", "number", "boolean", "string[]", "datetime", "timedelta"],
          name,
        ).toContain(field.type);
      }
    }
  });

  it("stays in step with the generator's own selection", () => {
    const schema = readSchema();
    expect(Object.keys(DAG_SCHEMA_FIELDS)).toEqual(selectDagFields(schema).map((f) => f.name));
    expect(Object.keys(TASK_SCHEMA_FIELDS)).toEqual(selectTaskFields(schema).map((f) => f.name));
  });

  it("carries the schema defaults the serializer omits against", () => {
    expect(TASK_SCHEMA_FIELDS.owner).toEqual({ key: "owner", type: "string", default: "airflow" });
    expect(TASK_SCHEMA_FIELDS.retryDelay).toEqual({
      key: "retry_delay",
      type: "timedelta",
      default: 300,
    });
    expect(TASK_SCHEMA_FIELDS.doXcomPush).toEqual({
      key: "do_xcom_push",
      type: "boolean",
      default: true,
    });
    expect(DAG_SCHEMA_FIELDS.failFast).toEqual({
      key: "fail_fast",
      type: "boolean",
      default: false,
    });
    // No schema default, so the scheduler's own default stands.
    expect(DAG_SCHEMA_FIELDS.catchup).toEqual({ key: "catchup", type: "boolean" });
  });

  it("marks schedule as derived rather than written straight through", () => {
    expect(DAG_SCHEMA_FIELDS.schedule).toEqual({
      key: "timetable",
      type: "string",
      virtual: true,
    });
  });
});
