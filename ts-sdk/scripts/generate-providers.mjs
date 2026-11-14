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

// Generates one npm package per provider under providers/, from the operator specs that
// scripts/ci/lang_sdk_providers/generate_operator_specs.py writes to
// generated/lang_sdk_provider_operators/. Each Python operator module becomes one TypeScript
// module of the same path, holding a typed binding per operator class.
//
// Re-run with: pnpm run generate:providers
// Check only:  pnpm run generate:providers --check

import console from "node:console";
import { mkdirSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from "node:fs";
import { dirname, join, relative, resolve, sep } from "node:path";
import process from "node:process";
import { fileURLToPath, pathToFileURL } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const TS_SDK_ROOT = resolve(HERE, "..");
const SPECS_DIR = resolve(TS_SDK_ROOT, "..", "generated", "lang_sdk_provider_operators");
const PROVIDERS_DIR = join(TS_SDK_ROOT, "providers");
const SPEC_SCRIPT = "scripts/ci/lang_sdk_providers/generate_operator_specs.py";
const SUPPORTED_SPEC_MAJOR = "1";

const LICENSE = [
  "Licensed to the Apache Software Foundation (ASF) under one",
  "or more contributor license agreements.  See the NOTICE file",
  "distributed with this work for additional information",
  "regarding copyright ownership.  The ASF licenses this file",
  "to you under the Apache License, Version 2.0 (the",
  '"License"); you may not use this file except in compliance',
  "with the License.  You may obtain a copy of the License at",
  "",
  "  http://www.apache.org/licenses/LICENSE-2.0",
  "",
  "Unless required by applicable law or agreed to in writing,",
  "software distributed under the License is distributed on an",
  '"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY',
  "KIND, either express or implied.  See the License for the",
  "specific language governing permissions and limitations",
  "under the License.",
];

const TS_LICENSE = ["/*!", ...LICENSE.map((line) => (line ? ` * ${line}` : " *")), " */", ""].join(
  "\n",
);
const MD_LICENSE = ["<!--", ...LICENSE.map((line) => (line ? ` ${line}` : "")), " -->", ""].join(
  "\n",
);

const DOC_WIDTH = 92;

/** `bucket_name` -> `bucketName`. */
export function toCamelCase(name) {
  return name.replace(/_+([a-zA-Z0-9])/g, (_, letter) => letter.toUpperCase());
}

/** The directory and npm name of a provider: `common.ai` -> `common-ai`. */
export function toPackageSlug(providerId) {
  return providerId.replaceAll(".", "-");
}

/** Whether a type takes the given kind at its top level, alone or in a union. */
function hasKind(type, kind) {
  return type.kind === kind || (type.kind === "union" && type.options.some((o) => o.kind === kind));
}

/** How a value of this type is encoded, when JSON has no type for it. */
export function getEncoding(type) {
  if (hasKind(type, "timedelta")) return "timedelta";
  if (hasKind(type, "datetime")) return "datetime";
  return undefined;
}

/** Render a spec type as a TypeScript type. */
export function renderType(type) {
  switch (type.kind) {
    case "string":
    case "boolean":
    case "null":
      return type.kind;
    case "integer":
    case "number":
    case "timedelta":
      return "number";
    case "datetime":
      return "Date";
    case "any":
      return "JsonValue";
    case "literal":
      return type.literals.map((value) => JSON.stringify(value)).join(" | ");
    case "array": {
      const items = renderType(type.items);
      return `readonly ${needsParens(type.items) ? `(${items})` : items}[]`;
    }
    case "tuple":
      return `readonly [${type.items.map(renderType).join(", ")}]`;
    case "object":
      return `Readonly<Record<string, ${renderType(type.values)}>>`;
    case "struct":
      return `{ ${type.fields
        .map((f) => `readonly ${f.name}${f.required ? "" : "?"}: ${renderType(f.type)}`)
        .join("; ")} }`;
    case "union":
      return [...new Set(type.options.map(renderType))].join(" | ");
    default:
      throw new Error(`Unknown type kind ${JSON.stringify(type.kind)} in an operator spec`);
  }
}

function needsParens(type) {
  return type.kind === "union" || (type.kind === "literal" && type.literals.length > 1);
}

function usesJsonValue(type) {
  if (type.kind === "any") return true;
  const nested = [type.items, type.values, ...(type.options ?? [])].flat().filter(Boolean);
  return [...nested, ...(type.fields ?? []).map((f) => f.type)].some(usesJsonValue);
}

function wrap(text, width) {
  const lines = [];
  let line = "";
  for (const word of text.split(/\s+/).filter(Boolean)) {
    if (line && line.length + 1 + word.length > width) {
      lines.push(line);
      line = word;
    } else {
      line = line ? `${line} ${word}` : word;
    }
  }
  if (line) lines.push(line);
  return lines;
}

/** A doc comment of `paragraphs`, indented by `indent`. `*\/` cannot end it early. */
function renderDoc(paragraphs, indent) {
  const lines = paragraphs
    .filter(Boolean)
    .flatMap((paragraph, index) => [
      ...(index > 0 ? [""] : []),
      ...wrap(paragraph.replaceAll("*/", "*\\/"), DOC_WIDTH - indent.length),
    ]);
  if (lines.length === 0) return "";
  if (lines.length === 1) return `${indent}/** ${lines[0]} */\n`;
  return `${indent}/**\n${lines.map((l) => `${indent} *${l ? ` ${l}` : ""}`).join("\n")}\n${indent} */\n`;
}

function renderParamDoc(param) {
  const notes = [];
  if (getEncoding(param.type) === "timedelta") notes.push("A duration in seconds.");
  if (param.templated) notes.push("Templated.");
  if (param.default !== undefined) notes.push(`Defaults to \`${JSON.stringify(param.default)}\`.`);
  return renderDoc([[param.description, ...notes].filter(Boolean).join(" ")], "  ");
}

/** A JSON literal on one line, spaced as Prettier would space it. */
function renderLiteral(value) {
  if (Array.isArray(value)) return `[${value.map(renderLiteral).join(", ")}]`;
  if (typeof value === "object" && value !== null) {
    const entries = Object.entries(value).map(
      ([key, item]) =>
        `${/^[A-Za-z_$][\w$]*$/.test(key) ? key : JSON.stringify(key)}: ${renderLiteral(item)}`,
    );
    return entries.length > 0 ? `{ ${entries.join(", ")} }` : "{}";
  }
  return JSON.stringify(value);
}

function renderOperator(operator, provider) {
  const names = new Map();
  for (const param of operator.params) {
    const key = toCamelCase(param.name);
    if (names.has(key)) {
      throw new Error(
        `${operator.module}.${operator.name}: arguments "${names.get(key)}" and "${param.name}" ` +
          `both become "${key}"`,
      );
    }
    names.set(key, param.name);
  }
  const argsName = `${operator.name}Args`;
  const fields = operator.params
    .map((param) => {
      const type = renderType(param.type);
      const optional = param.required ? "" : "?";
      return (
        renderParamDoc(param) +
        `  readonly ${toCamelCase(param.name)}${optional}: ${param.templated ? `Templated<${type}>` : type};\n`
      );
    })
    .join("");
  const params = operator.params
    .map((param) => {
      const encoding = getEncoding(param.type);
      const entries = [
        `name: ${JSON.stringify(param.name)}`,
        ...(param.required ? ["required: true"] : []),
        ...(encoding ? [`encoding: ${JSON.stringify(encoding)}`] : []),
      ];
      return `    ${toCamelCase(param.name)}: { ${entries.join(", ")} },\n`;
    })
    .join("");
  const definition = [
    `  module: ${JSON.stringify(operator.module)},`,
    `  className: ${JSON.stringify(operator.name)},`,
    `  templateFields: ${renderLiteral(operator.template_fields)},`,
    operator.template_ext && `  templateExt: ${renderLiteral(operator.template_ext)},`,
    operator.template_fields_renderers &&
      `  templateFieldsRenderers: ${renderLiteral(operator.template_fields_renderers)},`,
    operator.ui_color && `  uiColor: ${JSON.stringify(operator.ui_color)},`,
    operator.ui_fgcolor && `  uiFgcolor: ${JSON.stringify(operator.ui_fgcolor)},`,
    operator.operator_name && `  operatorName: ${JSON.stringify(operator.operator_name)},`,
    operator.kind === "sensor" && "  isSensor: true,",
    operator.can_skip_downstream && "  canSkipDownstream: true,",
    params ? `  params: {\n${params}  },` : "  params: {},",
  ].filter(Boolean);
  const omitted = (operator.omitted_params ?? []).map((p) => `\`${p.name}\``);
  return (
    renderDoc([`Arguments of {@link ${operator.name}}.`], "") +
    `export interface ${argsName} {\n${fields}}\n\n` +
    renderDoc(
      [
        operator.description,
        `Runs \`${operator.module}.${operator.name}\` of \`${provider.package}\` on a Python worker.`,
        omitted.length > 0 &&
          `Not available from TypeScript, because JSON cannot carry them: ${omitted.join(", ")}.`,
      ],
      "",
    ) +
    `export const ${operator.name} = defineProviderOperator<${argsName}>({\n${definition.join("\n")}\n});\n`
  );
}

/** The path of a Python module inside its provider package: `aws/operators/s3`. */
export function getModulePath(moduleName, providerId) {
  const prefix = `airflow.providers.${providerId}.`;
  if (!moduleName.startsWith(prefix)) {
    throw new Error(`Module ${moduleName} is not part of provider ${providerId}`);
  }
  return moduleName.slice(prefix.length).replaceAll(".", "/");
}

/** Render the TypeScript module holding the operators of one Python module. */
export function renderModule(moduleName, operators, provider) {
  const usesTemplated = operators.some((op) => op.params.some((param) => param.templated));
  const usesJson = operators.some((op) => op.params.some((param) => usesJsonValue(param.type)));
  const providerImport = usesTemplated
    ? 'import { defineProviderOperator, type Templated } from "@apache-airflow/ts-sdk/provider";'
    : 'import { defineProviderOperator } from "@apache-airflow/ts-sdk/provider";';
  return [
    TS_LICENSE,
    `// AUTO-GENERATED by ts-sdk/scripts/generate-providers.mjs. Do not edit by hand.`,
    `// Source: ${moduleName} of ${provider.package} ${provider.version}.`,
    "",
    ...(usesJson ? ['import type { JsonValue } from "@apache-airflow/ts-sdk";'] : []),
    providerImport,
    "",
    operators.map((operator) => renderOperator(operator, provider)).join("\n"),
  ].join("\n");
}

function getPackageName(provider) {
  return `@apache-airflow/providers-${toPackageSlug(provider.id)}`;
}

function renderPackageJson(provider, sdkPackage) {
  const manifest = {
    name: getPackageName(provider),
    version: provider.version,
    description: `Apache Airflow ${provider.package} operators for Dags written with the TypeScript SDK`,
    license: "Apache-2.0",
    author: "Apache Software Foundation",
    type: "module",
    homepage: "https://airflow.apache.org",
    repository: {
      type: "git",
      url: "https://github.com/apache/airflow.git",
      directory: `ts-sdk/providers/${toPackageSlug(provider.id)}`,
    },
    bugs: { url: "https://github.com/apache/airflow/issues" },
    publishConfig: { access: "public" },
    exports: { "./*": { types: "./dist/*.d.ts", import: "./dist/*.js" } },
    files: ["dist", "README.md"],
    scripts: {
      clean: "node -e \"require('node:fs').rmSync('dist', { recursive: true, force: true })\"",
      build: "pnpm run clean && tsc -p tsconfig.build.json",
      prepack: "pnpm run build",
    },
    keywords: ["airflow", "apache-airflow", "typescript", "aip-108", provider.id],
    engines: sdkPackage.engines,
    peerDependencies: { [sdkPackage.name]: "workspace:^" },
    devDependencies: {
      [sdkPackage.name]: "workspace:*",
      typescript: sdkPackage.devDependencies.typescript,
    },
  };
  return `${JSON.stringify(manifest, null, 2)}\n`;
}

const TSCONFIG_BUILD = `${JSON.stringify(
  {
    extends: "../../tsconfig.json",
    compilerOptions: {
      noEmit: false,
      outDir: "./dist",
      rootDir: "./src",
      sourceMap: false,
      declarationMap: false,
      // The SDK is resolved as the package it is published as, not as the sources next door.
      paths: {},
    },
    include: ["src/**/*.ts"],
  },
  null,
  2,
)}\n`;

function renderReadme(spec, modules) {
  const { provider } = spec;
  const [firstModule, firstOperators] = modules[0] ?? [];
  const example = firstOperators?.[0];
  const rows = modules.flatMap(([moduleName, operators]) =>
    operators.map(
      (op) =>
        `| \`${op.name}\` | \`${getPackageName(provider)}/${getModulePath(moduleName, provider.id)}\` |`,
    ),
  );
  const skipped = spec.skipped
    .filter((entry) => entry.name && entry.reason !== "base class")
    .map((entry) => `| \`${entry.name}\` | ${entry.reason.replaceAll("|", "\\|")} |`);
  return [
    MD_LICENSE,
    `# ${getPackageName(provider)}`,
    "",
    `The operators and sensors of \`${provider.package}\` ${provider.version}, for a Dag written with`,
    "[`@apache-airflow/ts-sdk`](https://www.npmjs.com/package/@apache-airflow/ts-sdk).",
    "",
    "A binding has no task body. It records the Python operator class and its arguments in the",
    "Dag, and a Python worker that has the provider installed runs the operator. Such a task does",
    "not take the Dag's queue: give it one in its task options when the default queue is not",
    "served by Python workers.",
    "",
    ...(example
      ? [
          "```ts",
          'import { Dag } from "@apache-airflow/ts-sdk";',
          `import { ${example.name} } from "${getPackageName(provider)}/${getModulePath(firstModule, provider.id)}";`,
          "",
          'const dag = new Dag("example");',
          `dag.task("my_task", ${example.name}({ /* ... */ }))();`,
          "```",
          "",
        ]
      : []),
    "A module mirrors the Python module of the same path, and an argument is the Python",
    "argument in camel case. A templated argument takes a Jinja template, or the result of",
    "another task. A duration is a number of seconds.",
    "",
    "This package is generated. To change it, regenerate the operator specs and the packages:",
    "",
    "```bash",
    `python3 ${SPEC_SCRIPT} --allow-list ${provider.id}`,
    "```",
    "",
    "```bash",
    "pnpm --dir ts-sdk run generate:providers",
    "```",
    "",
    "## Operators",
    "",
    "| Operator | Module |",
    "| --- | --- |",
    ...rows,
    "",
    ...(skipped.length > 0
      ? ["## Not available", "", "| Operator | Reason |", "| --- | --- |", ...skipped, ""]
      : []),
  ].join("\n");
}

/** Every file of one provider package, keyed by its path inside the package. */
export function renderPackage(spec, sdkPackage) {
  const [major] = String(spec.spec_version).split(".");
  if (major !== SUPPORTED_SPEC_MAJOR) {
    throw new Error(
      `Operator spec of ${spec.provider.id} has version ${spec.spec_version}; this generator ` +
        `reads version ${SUPPORTED_SPEC_MAJOR}.x`,
    );
  }
  const byModule = new Map();
  for (const operator of spec.operators) {
    if (!byModule.has(operator.module)) byModule.set(operator.module, []);
    byModule.get(operator.module).push(operator);
  }
  const modules = [...byModule].sort(([a], [b]) => (a < b ? -1 : 1));
  const files = new Map([
    ["package.json", renderPackageJson(spec.provider, sdkPackage)],
    ["tsconfig.build.json", TSCONFIG_BUILD],
    ["README.md", renderReadme(spec, modules)],
  ]);
  for (const [moduleName, operators] of modules) {
    files.set(
      `src/${getModulePath(moduleName, spec.provider.id)}.ts`,
      renderModule(moduleName, operators, spec.provider),
    );
  }
  return files;
}

function listFiles(directory) {
  let entries;
  try {
    entries = readdirSync(directory, { withFileTypes: true });
  } catch {
    return [];
  }
  return entries.flatMap((entry) => {
    if (entry.name === "node_modules" || entry.name === "dist") return [];
    const path = join(directory, entry.name);
    return entry.isDirectory() ? listFiles(path) : [path];
  });
}

/**
 * Write the packages of every spec in `specsDir` under `providersDir`, or with `check` only
 * report what differs. Returns the paths that were, or would be, written or removed.
 */
export function generate({ specsDir, providersDir, sdkPackage, check }) {
  const changed = [];
  const expected = new Set();
  const specFiles = readdirSync(specsDir)
    .filter((name) => name.endsWith(".json"))
    .sort();
  for (const specFile of specFiles) {
    const spec = JSON.parse(readFileSync(join(specsDir, specFile), "utf8"));
    const packageDir = join(providersDir, toPackageSlug(spec.provider.id));
    for (const [path, content] of renderPackage(spec, sdkPackage)) {
      const target = join(packageDir, ...path.split("/"));
      expected.add(target);
      let current;
      try {
        current = readFileSync(target, "utf8");
      } catch {
        current = undefined;
      }
      if (current === content) continue;
      changed.push(target);
      if (!check) {
        mkdirSync(dirname(target), { recursive: true });
        writeFileSync(target, content, "utf8");
      }
    }
  }
  // A module whose operators all left the spec, or a provider that left the allow list.
  for (const stale of listFiles(providersDir).filter((path) => !expected.has(path))) {
    changed.push(stale);
    if (!check) rmSync(stale);
  }
  return changed;
}

function main() {
  const check = process.argv.includes("--check");
  if (!statSync(SPECS_DIR, { throwIfNoEntry: false })?.isDirectory()) {
    throw new Error(`No operator specs in ${SPECS_DIR}; run ${SPEC_SCRIPT} first`);
  }
  const sdkPackage = JSON.parse(readFileSync(join(TS_SDK_ROOT, "package.json"), "utf8"));
  const changed = generate({
    specsDir: SPECS_DIR,
    providersDir: PROVIDERS_DIR,
    sdkPackage,
    check,
  });
  const paths = changed.map((path) => relative(TS_SDK_ROOT, path).split(sep).join("/"));
  if (check && paths.length > 0) {
    console.error(
      "The provider packages are out of date with generated/lang_sdk_provider_operators/. " +
        "Run `pnpm run generate:providers` in ts-sdk/ and commit the result:",
    );
    for (const path of paths) console.error(`  ${path}`);
    process.exitCode = 1;
    return;
  }
  console.log(check ? "provider packages are up to date" : `wrote ${paths.length} files`);
}

// Importable for tests; runs only when invoked as a script.
if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) {
  main();
}
