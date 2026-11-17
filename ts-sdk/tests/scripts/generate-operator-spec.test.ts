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

import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
  PROVIDER_SOURCES_DIR,
  PROVIDERS_DIR,
  TS_SDK_ROOT,
  buildSpec,
  findImplementedPackages,
  generate,
  getSpecPath,
  renderSpec,
} from "../../scripts/generate-operator-spec.mjs";

const OPERATORS_SOURCE = `
import { defineProviderOperator, type Templated } from "@apache-airflow/ts-sdk/provider";

type JsonValue = unknown;

/** Arguments of {@link WriteFileOperator}. */
export interface WriteFileOperatorArgs {
  /** Where to write. */
  readonly path: Templated<string>;
  /**
   * The text to write,
   * which may be long.
   */
  readonly content: Templated<string>;
  /** Overwrite what is there. @default false */
  readonly overwrite?: boolean;
  /** How the text is encoded. @default \`"utf-8"\` */
  readonly encoding?: "utf-8" | "ascii";
  /** Retry delay. */
  readonly delay?: number | null;
  /** Not before. */
  readonly notBefore?: Date;
  /** Labels. */
  readonly labels?: readonly string[];
  /** Headers. */
  readonly headers?: Templated<Readonly<Record<string, string>> | null>;
  /** Anything. */
  readonly extra?: JsonValue;
  /** Mode. */
  readonly mode?: 1 | 2 | true | null;
}

/**
 * Write a file.
 *
 * Longer text that is not part of the summary.
 */
export const WriteFileOperator = defineProviderOperator<WriteFileOperatorArgs>({
  module: "airflow.providers.demo.operators.file",
  className: "WriteFileOperator",
  templateFields: ["path", "content", "headers"],
  templateExt: [".txt"],
  templateFieldsRenderers: { content: "md" },
  uiColor: "#abc",
  operatorName: "Write",
  params: {
    path: { name: "path", required: true },
    content: { name: "content", required: true },
    overwrite: { name: "overwrite" },
    encoding: { name: "output_encoding" },
    delay: { name: "delay", encoding: "timedelta" },
    notBefore: { name: "not_before", encoding: "datetime" },
    labels: { name: "labels" },
    headers: { name: "headers" },
    extra: { name: "extra" },
    mode: { name: "mode" },
  },
  async execute() {
    return undefined;
  },
});
`;

const SENSOR_SOURCE = `
import { defineProviderOperator } from "@apache-airflow/ts-sdk/provider";

export interface FileExistsSensorArgs {
  readonly path: string;
}

// Describes a Python operator only, so it is left out of the spec.
export const Described = defineProviderOperator<{ path: string }>({
  module: "airflow.providers.demo.operators.described",
  className: "Described",
  templateFields: [],
  params: { path: { name: "path", required: true } },
});

export const FileExistsSensor = defineProviderOperator<FileExistsSensorArgs>({
  module: "airflow.providers.demo.sensors.file",
  className: "FileExistsSensor",
  isSensor: true,
  canSkipDownstream: true,
  templateFields: [],
  params: { path: { name: "path", required: true } },
  execute: ({ path }) => path.length,
});
`;

let root: string;
let packageDir: string;
let specDir: string;

function writePackage(sources: Record<string, string>, airflow: unknown = {}): void {
  rmSync(packageDir, { recursive: true, force: true });
  mkdirSync(join(packageDir, "src"), { recursive: true });
  writeFileSync(
    join(packageDir, "package.json"),
    JSON.stringify({ name: "@demo/providers-demo", version: "2.3.4", airflow }),
  );
  for (const [file, text] of Object.entries(sources)) {
    mkdirSync(join(packageDir, "src", ...file.split("/").slice(0, -1)), { recursive: true });
    writeFileSync(join(packageDir, "src", file), text);
  }
}

const AIRFLOW = { providerId: "demo.mail", pythonPackage: "apache-airflow-providers-demo-mail" };

beforeEach(() => {
  root = mkdtempSync(join(tmpdir(), "operator-spec-"));
  packageDir = join(root, "package");
  specDir = join(root, "providers");
});

afterEach(() => {
  rmSync(root, { recursive: true, force: true });
});

describe("buildSpec", () => {
  it("reads the operators, their arguments and their documentation from the source", () => {
    writePackage(
      { "operators/file.ts": OPERATORS_SOURCE, "sensors/file.ts": SENSOR_SOURCE },
      AIRFLOW,
    );

    const spec = buildSpec(packageDir);

    expect(spec.provider).toEqual({
      id: "demo.mail",
      package: "apache-airflow-providers-demo-mail",
      version: "2.3.4",
    });
    expect(spec.spec_version).toBe("1.0");
    expect(spec.implemented_in).toEqual(["TypeScript"]);
    expect(spec.skipped).toEqual([]);
    expect(spec.operators.map((o: { name: string }) => o.name)).toEqual([
      "WriteFileOperator",
      "FileExistsSensor",
    ]);
    const [write, sensor] = spec.operators as [{ params: unknown }, unknown];
    expect(write).toMatchObject({
      name: "WriteFileOperator",
      module: "airflow.providers.demo.operators.file",
      implementation_module: "@demo/providers-demo/operators/file",
      kind: "operator",
      description: "Write a file.",
      template_fields: ["path", "content", "headers"],
      template_ext: [".txt"],
      template_fields_renderers: { content: "md" },
      ui_color: "#abc",
      operator_name: "Write",
    });
    expect(sensor).toEqual({
      name: "FileExistsSensor",
      module: "airflow.providers.demo.sensors.file",
      implementation_module: "@demo/providers-demo/sensors/file",
      kind: "sensor",
      template_fields: [],
      can_skip_downstream: true,
      params: [{ name: "path", type: { kind: "string" }, required: true }],
    });
    expect(write.params).toEqual([
      {
        name: "path",
        type: { kind: "string" },
        required: true,
        templated: true,
        description: "Where to write.",
      },
      {
        name: "content",
        type: { kind: "string" },
        required: true,
        templated: true,
        description: "The text to write, which may be long.",
      },
      {
        name: "overwrite",
        type: { kind: "boolean" },
        default: false,
        description: "Overwrite what is there.",
      },
      {
        name: "output_encoding",
        type: { kind: "literal", literals: ["utf-8", "ascii"] },
        default: "utf-8",
        description: "How the text is encoded.",
      },
      {
        name: "delay",
        type: { kind: "union", options: [{ kind: "timedelta" }, { kind: "null" }] },
        description: "Retry delay.",
      },
      { name: "not_before", type: { kind: "datetime" }, description: "Not before." },
      {
        name: "labels",
        type: { kind: "array", items: { kind: "string" } },
        description: "Labels.",
      },
      {
        name: "headers",
        type: {
          kind: "union",
          options: [{ kind: "object", values: { kind: "string" } }, { kind: "null" }],
        },
        templated: true,
        description: "Headers.",
      },
      { name: "extra", type: { kind: "any" }, description: "Anything." },
      {
        name: "mode",
        type: {
          kind: "union",
          options: [{ kind: "literal", literals: [1, 2, true] }, { kind: "null" }],
        },
        description: "Mode.",
      },
    ]);
  });

  it("is deterministic across the order of the files", () => {
    writePackage({ "a.ts": SENSOR_SOURCE, "b.ts": OPERATORS_SOURCE }, AIRFLOW);
    const first = renderSpec(buildSpec(packageDir));
    writePackage({ "b.ts": OPERATORS_SOURCE, "a.ts": SENSOR_SOURCE }, AIRFLOW);
    expect(renderSpec(buildSpec(packageDir))).toBe(first);
  });

  it("needs the airflow field of package.json", () => {
    writePackage({ "a.ts": SENSOR_SOURCE }, { providerId: "demo" });
    expect(() => buildSpec(packageDir)).toThrowError(/needs "providerId" and "pythonPackage"/);
  });

  it.each([
    {
      name: "is not exported",
      source: SENSOR_SOURCE.replace("export const FileExistsSensor", "const FileExistsSensor"),
    },
    {
      name: "is exported under another name",
      source: SENSOR_SOURCE.replace("export const FileExistsSensor", "export const Sensor"),
    },
  ])(
    "fails when a binding $name, which a runtime could not import by its class name",
    ({ source }) => {
      writePackage({ "a.ts": source }, AIRFLOW);
      expect(() => buildSpec(packageDir)).toThrowError(
        /must be declared as `export const FileExistsSensor = defineProviderOperator\(\.\.\.\)`/,
      );
    },
  );

  it("accepts an implementationModule that is the package name and the source path", () => {
    writePackage(
      {
        "a.ts": SENSOR_SOURCE.replace(
          'className: "FileExistsSensor",',
          'className: "FileExistsSensor", implementationModule: "@demo/providers-demo/a",',
        ),
      },
      AIRFLOW,
    );
    expect(buildSpec(packageDir).operators[0]?.implementation_module).toBe(
      "@demo/providers-demo/a",
    );
  });

  it("fails when an implementationModule is not the package name and the source path", () => {
    writePackage(
      {
        "a.ts": SENSOR_SOURCE.replace(
          'className: "FileExistsSensor",',
          'className: "FileExistsSensor", implementationModule: "@demo/providers-demo/b",',
        ),
      },
      AIRFLOW,
    );
    expect(() => buildSpec(packageDir)).toThrowError(
      /"implementationModule" must be "@demo\/providers-demo\/a", not "@demo\/providers-demo\/b"/,
    );
  });

  it("fails when nothing is implemented", () => {
    writePackage({ "a.ts": "export const x = 1;\n" }, AIRFLOW);
    expect(() => buildSpec(packageDir)).toThrowError(/no defineProviderOperator call/);
  });

  it("fails when two bindings implement one class", () => {
    writePackage({ "a.ts": SENSOR_SOURCE, "b.ts": SENSOR_SOURCE }, AIRFLOW);
    expect(() => buildSpec(packageDir)).toThrowError(
      /airflow.providers.demo.sensors.file.FileExistsSensor is implemented twice/,
    );
  });

  const withArg = (arg: string, params = '{ name: "a" }', templateFields = "[]") => `
import { defineProviderOperator, type Templated } from "@apache-airflow/ts-sdk/provider";
export interface OpArgs {
  ${arg}
}
export const Op = defineProviderOperator<OpArgs>({
  module: "m",
  className: "Op",
  templateFields: ${templateFields},
  params: { a: ${params} },
  execute: () => 1,
});
`;

  it.each([
    ["a type with no spec kind", "readonly a?: Map<string, string>;", /type `Map<string, string>`/],
    ["a tuple", "readonly a?: readonly [string, number];", /no spec type kind/],
    ["a Date without its encoding", "readonly a?: Date;", /needs `encoding: "datetime"`/],
    [
      "a required property that is optional",
      "readonly a?: string;",
      /optional in OpArgs/,
      '{ name: "a", required: true }',
    ],
    ["an optional property that is required", "readonly a: string;", /required in OpArgs/],
    ["a missing property", "readonly b?: string;", /no typed property "a"/],
    [
      "a Templated type that is no template field",
      "readonly a?: Templated<string>;",
      /not in `templateFields`/,
    ],
    [
      "a template field that is not Templated",
      "readonly a?: string;",
      /must be `Templated<...>`/,
      '{ name: "a" }',
      '["a"]',
    ],
    [
      "a default that is not JSON",
      "/** @default nope */ readonly a?: string;",
      /@default must be a JSON value/,
    ],
    ["a Record with other keys", "readonly a?: Record<number, string>;", /keys must be `string`/],
    [
      "a nested Templated",
      "readonly a?: readonly Templated<string>[];",
      /may only wrap the whole type/,
    ],
  ] as [string, string, RegExp, string?, string?][])(
    "rejects %s",
    (_name, arg, message, params, templateFields) => {
      writePackage({ "a.ts": withArg(arg, params, templateFields) }, AIRFLOW);
      expect(() => buildSpec(packageDir)).toThrowError(message);
    },
  );
});

describe("renderSpec", () => {
  it("writes one argument per line, as the Python generator does", () => {
    writePackage({ "a.ts": SENSOR_SOURCE }, AIRFLOW);
    expect(renderSpec(buildSpec(packageDir))).toBe(
      `{
  "spec_version": "1.0",
  "provider": {
    "id": "demo.mail",
    "package": "apache-airflow-providers-demo-mail",
    "version": "2.3.4"
  },
  "implemented_in": [
    "TypeScript"
  ],
  "operators": [
    {
      "name": "FileExistsSensor",
      "module": "airflow.providers.demo.sensors.file",
      "implementation_module": "@demo/providers-demo/a",
      "kind": "sensor",
      "template_fields": [],
      "can_skip_downstream": true,
      "params": [
        {"name": "path", "type": {"kind": "string"}, "required": true}
      ]
    }
  ],
  "skipped": []
}
`,
    );
  });
});

describe("generate", () => {
  it("writes the spec under the provider id path, and reports what is out of date", () => {
    writePackage({ "a.ts": SENSOR_SOURCE }, AIRFLOW);
    const options = { packageDirs: [packageDir], providerSourcesDir: specDir };
    const target = getSpecPath(specDir, "demo.mail");
    expect(target).toBe(join(specDir, "demo", "mail", "_generated", "lang_sdk_operators.json"));

    expect(generate({ ...options, check: true })).toEqual([target]);
    expect(generate({ ...options, check: false })).toEqual([target]);
    expect(readFileSync(target, "utf8")).toBe(renderSpec(buildSpec(packageDir)));
    expect(generate({ ...options, check: true })).toEqual([]);

    writeFileSync(target, "{}\n");
    expect(generate({ ...options, check: true })).toEqual([target]);
  });
});

describe("the provider packages", () => {
  it("have up-to-date operator specs", () => {
    const packageDirs = findImplementedPackages(PROVIDERS_DIR);
    expect(
      generate({ packageDirs, providerSourcesDir: PROVIDER_SOURCES_DIR, check: true }),
    ).toEqual([]);
  });

  it("pass --check from the command line", () => {
    const output = execFileSync(
      process.execPath,
      [join(TS_SDK_ROOT, "scripts", "generate-operator-spec.mjs"), "--check"],
      { encoding: "utf8" },
    );
    expect(output).toMatch(/no provider package implements operators|up to date/);
  });
});
