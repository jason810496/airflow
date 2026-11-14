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

import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
  generate,
  getEncoding,
  getModulePath,
  renderPackage,
  renderType,
  toCamelCase,
  toPackageSlug,
} from "../../scripts/generate-providers.mjs";

const STRING = { kind: "string" };
const NULL = { kind: "null" };

const SDK_PACKAGE = {
  name: "@apache-airflow/ts-sdk",
  engines: { node: ">=22" },
  devDependencies: { typescript: "^6.0.2" },
};

const SPEC = {
  spec_version: "1.0",
  provider: { id: "demo.mail", package: "apache-airflow-providers-demo-mail", version: "2.3.4" },
  operators: [
    {
      name: "SendOperator",
      module: "airflow.providers.demo.mail.operators.send",
      kind: "operator",
      description: "Send a message. A comment end */ in the text cannot close the comment.",
      template_fields: ["subject"],
      template_ext: [".txt"],
      template_fields_renderers: { subject: "md" },
      ui_color: "#abc",
      operator_name: "Send",
      can_skip_downstream: true,
      params: [
        {
          name: "subject",
          type: STRING,
          required: true,
          templated: true,
          description: "What it is about.",
        },
        { name: "retry_after", type: { kind: "union", options: [{ kind: "timedelta" }, NULL] } },
        { name: "extra", type: { kind: "any" }, default: "none" },
      ],
      omitted_params: [{ name: "callback", annotation: "Callable" }],
    },
    {
      name: "InboxSensor",
      module: "airflow.providers.demo.mail.sensors.inbox",
      kind: "sensor",
      template_fields: [],
      params: [],
    },
  ],
  skipped: [
    { module: "airflow.providers.demo.mail.operators.base", name: "Base", reason: "base class" },
    {
      module: "airflow.providers.demo.mail.operators.send",
      name: "CallOperator",
      reason: "required argument 'fn' (Callable | None) is not JSON-serializable",
    },
  ],
};

describe("renderType", () => {
  it.each([
    [STRING, "string"],
    [{ kind: "integer" }, "number"],
    [{ kind: "timedelta" }, "number"],
    [{ kind: "datetime" }, "Date"],
    [{ kind: "any" }, "JsonValue"],
    [{ kind: "literal", literals: ["a", 1, null] }, '"a" | 1 | null'],
    [{ kind: "array", items: STRING }, "readonly string[]"],
    [
      { kind: "array", items: { kind: "union", options: [STRING, NULL] } },
      "readonly (string | null)[]",
    ],
    [{ kind: "tuple", items: [STRING, { kind: "boolean" }] }, "readonly [string, boolean]"],
    [{ kind: "object", values: { kind: "any" } }, "Readonly<Record<string, JsonValue>>"],
    [
      {
        kind: "struct",
        fields: [
          { name: "id", type: STRING, required: true },
          { name: "name", type: STRING, required: false },
        ],
      },
      "{ readonly id: string; readonly name?: string }",
    ],
    // A union of types that share a TypeScript type names it once.
    [{ kind: "union", options: [{ kind: "integer" }, { kind: "number" }, NULL] }, "number | null"],
  ])("renders %j as %s", (type, expected) => {
    expect(renderType(type)).toBe(expected);
  });

  it("rejects a kind this generator does not know", () => {
    expect(() => renderType({ kind: "bytes" })).toThrow('Unknown type kind "bytes"');
  });
});

describe("naming", () => {
  it("spells arguments, packages and modules the TypeScript way", () => {
    expect(toCamelCase("aws_conn_id")).toBe("awsConnId");
    expect(toCamelCase("s3_bucket")).toBe("s3Bucket");
    expect(toPackageSlug("common.ai")).toBe("common-ai");
    expect(getModulePath("airflow.providers.common.ai.operators.llm", "common.ai")).toBe(
      "operators/llm",
    );
    expect(() => getModulePath("airflow.providers.amazon.aws.x", "common.ai")).toThrow(
      "is not part of provider common.ai",
    );
  });

  it("encodes a duration or a datetime only at the top of a type", () => {
    expect(getEncoding({ kind: "union", options: [{ kind: "timedelta" }, NULL] })).toBe(
      "timedelta",
    );
    expect(getEncoding({ kind: "datetime" })).toBe("datetime");
    expect(getEncoding(STRING)).toBeUndefined();
  });
});

describe("renderPackage", () => {
  const files = renderPackage(SPEC, SDK_PACKAGE);

  it("writes one module per Python module, plus the package files", () => {
    expect([...files.keys()]).toEqual([
      "package.json",
      "tsconfig.build.json",
      "README.md",
      "src/operators/send.ts",
      "src/sensors/inbox.ts",
    ]);
  });

  it("names the package after the provider and versions it with the provider", () => {
    expect(JSON.parse(files.get("package.json")!)).toMatchObject({
      name: "@apache-airflow/providers-demo-mail",
      version: "2.3.4",
      exports: { "./*": { types: "./dist/*.d.ts", import: "./dist/*.js" } },
      peerDependencies: { "@apache-airflow/ts-sdk": "workspace:^" },
    });
  });

  it("renders a typed binding for each operator", () => {
    const module = files.get("src/operators/send.ts");

    expect(module).toContain(
      'import type { JsonValue } from "@apache-airflow/ts-sdk";\n' +
        'import { defineProviderOperator, type Templated } from "@apache-airflow/ts-sdk/provider";',
    );
    expect(module).toContain(
      "export interface SendOperatorArgs {\n" +
        "  /** What it is about. Templated. */\n" +
        "  readonly subject: Templated<string>;\n" +
        "  /** A duration in seconds. */\n" +
        "  readonly retryAfter?: number | null;\n" +
        '  /** Defaults to `"none"`. */\n' +
        "  readonly extra?: JsonValue;\n" +
        "}\n",
    );
    expect(module).toContain(
      "export const SendOperator = defineProviderOperator<SendOperatorArgs>({\n" +
        '  module: "airflow.providers.demo.mail.operators.send",\n' +
        '  className: "SendOperator",\n' +
        '  templateFields: ["subject"],\n' +
        '  templateExt: [".txt"],\n' +
        '  templateFieldsRenderers: { subject: "md" },\n' +
        '  uiColor: "#abc",\n' +
        '  operatorName: "Send",\n' +
        "  canSkipDownstream: true,\n" +
        "  params: {\n" +
        '    subject: { name: "subject", required: true },\n' +
        '    retryAfter: { name: "retry_after", encoding: "timedelta" },\n' +
        '    extra: { name: "extra" },\n' +
        "  },\n" +
        "});\n",
    );
    expect(module).toContain("A comment end *\\/ in the text cannot close the comment.");
    expect(module).toContain("because JSON cannot carry them: `callback`.");
  });

  it("imports only what a module uses, and marks a sensor", () => {
    const module = files.get("src/sensors/inbox.ts");

    expect(module).toContain(
      'import { defineProviderOperator } from "@apache-airflow/ts-sdk/provider";',
    );
    expect(module).not.toContain("JsonValue");
    expect(module).toContain("  isSensor: true,\n  params: {},\n");
  });

  it("lists in the README what is available and what is not, without the base classes", () => {
    const readme = files.get("README.md");

    expect(readme).toContain(
      "| `SendOperator` | `@apache-airflow/providers-demo-mail/operators/send` |",
    );
    expect(readme).toContain(
      "| `CallOperator` | required argument 'fn' (Callable \\| None) is not JSON-serializable |",
    );
    expect(readme).not.toContain("`Base`");
  });

  it("rejects two arguments that become one name", () => {
    const [first, ...rest] = SPEC.operators;
    const params = [...first!.params, { name: "retryAfter", type: STRING }];
    const clash = { ...SPEC, operators: [{ ...first, params }, ...rest] };

    expect(() => renderPackage(clash, SDK_PACKAGE)).toThrow(
      'arguments "retry_after" and "retryAfter" both become "retryAfter"',
    );
  });

  it("rejects a spec of another major version", () => {
    expect(() => renderPackage({ ...SPEC, spec_version: "2.0" }, SDK_PACKAGE)).toThrow(
      "has version 2.0; this generator reads version 1.x",
    );
  });
});

describe("generate", () => {
  let root: string;
  let options: { specsDir: string; providersDir: string; sdkPackage: typeof SDK_PACKAGE };

  beforeEach(() => {
    root = mkdtempSync(join(tmpdir(), "generate-providers-"));
    const specsDir = join(root, "specs");
    mkdirSync(specsDir);
    writeFileSync(join(specsDir, "demo.mail.json"), JSON.stringify(SPEC));
    options = { specsDir, providersDir: join(root, "providers"), sdkPackage: SDK_PACKAGE };
  });

  afterEach(() => {
    rmSync(root, { recursive: true, force: true });
  });

  it("writes the packages, and then finds nothing to change", () => {
    expect(generate({ ...options, check: true })).toHaveLength(5);
    expect(generate({ ...options, check: false })).toHaveLength(5);
    expect(generate({ ...options, check: true })).toEqual([]);
    expect(
      readFileSync(join(options.providersDir, "demo-mail", "src", "sensors", "inbox.ts"), "utf8"),
    ).toContain("export const InboxSensor");
  });

  it("reports an edited file, and removes one the specs no longer produce", () => {
    generate({ ...options, check: false });
    const edited = join(options.providersDir, "demo-mail", "src", "operators", "send.ts");
    const stale = join(options.providersDir, "demo-mail", "src", "operators", "gone.ts");
    writeFileSync(edited, "// edited by hand\n");
    writeFileSync(stale, "// left behind\n");

    expect(generate({ ...options, check: true }).sort()).toEqual([stale, edited].sort());
    generate({ ...options, check: false });
    expect(generate({ ...options, check: true })).toEqual([]);
    expect(() => readFileSync(stale)).toThrow();
  });
});
