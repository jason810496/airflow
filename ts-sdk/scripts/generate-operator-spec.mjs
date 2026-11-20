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

// Writes the operator spec of a provider that is implemented in TypeScript. The spec is read from the
// package's source: each `defineProviderOperator` call that passes `execute`, and the
// `<ClassName>Args` interface that types its arguments. Every other SDK generates its binding from
// the spec, as the Python wrapper does. See
// airflow-core/adr/lang-sdk/0018-provider-operators-implemented-in-lang-sdks.md.
//
// A package takes part when its package.json has an `airflow` field:
//
//     "airflow": { "providerId": "ts.localfile", "pythonPackage": "apache-airflow-providers-ts-localfile" }
//
// The spec goes to providers/<provider id as a path>/_generated/lang_sdk_operators.json at the
// repository root, beside the specs the Python generator writes. tests/scripts/generate-operator-spec.test.ts
// fails when a spec is out of date.
//
// Re-run with: pnpm run generate:operator-spec [-- --check] [package directory...]

import console from "node:console";
import { existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, relative, resolve, sep } from "node:path";
import process from "node:process";
import { fileURLToPath, pathToFileURL } from "node:url";

import ts from "typescript";

const HERE = dirname(fileURLToPath(import.meta.url));
export const TS_SDK_ROOT = resolve(HERE, "..");
export const PROVIDER_SOURCES_DIR = resolve(TS_SDK_ROOT, "..", "providers");
export const PROVIDERS_DIR = join(TS_SDK_ROOT, "providers");
export const SPEC_VERSION = "1.0";
const SPEC_PATH = join("_generated", "lang_sdk_operators.json");
const DEFINE = "defineProviderOperator";

/** The packages under `providersDir` that declare an `airflow.providerId`, sorted. */
export function findImplementedPackages(providersDir) {
  let entries;
  try {
    entries = readdirSync(providersDir, { withFileTypes: true });
  } catch {
    return [];
  }
  return entries
    .filter((entry) => entry.isDirectory() && !entry.name.startsWith("."))
    .map((entry) => join(providersDir, entry.name))
    .filter((dir) => {
      const manifest = join(dir, "package.json");
      return existsSync(manifest) && readPackageJson(dir).airflow?.providerId !== undefined;
    })
    .sort();
}

function readPackageJson(packageDir) {
  return JSON.parse(readFileSync(join(packageDir, "package.json"), "utf8"));
}

/** Where the spec of a provider goes: `common.ai` is `common/ai` under `providerSourcesDir`. */
export function getSpecPath(providerSourcesDir, providerId) {
  return join(providerSourcesDir, ...providerId.split("."), SPEC_PATH);
}

function listSourceFiles(directory) {
  return readdirSync(directory, { withFileTypes: true })
    .flatMap((entry) => {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) return entry.name === "node_modules" ? [] : listSourceFiles(path);
      return entry.name.endsWith(".ts") && !entry.name.endsWith(".d.ts") ? [path] : [];
    })
    .sort();
}

class SpecError extends Error {}

function fail(where, message) {
  throw new SpecError(`${where}: ${message}`);
}

function getPropertyName(property) {
  const name = property.name;
  if (name && (ts.isIdentifier(name) || ts.isStringLiteralLike(name))) return name.text;
  return undefined;
}

function getProperty(literal, name) {
  return literal.properties.find((p) => getPropertyName(p) === name);
}

function readString(literal, name, where, { optional = false } = {}) {
  const property = getProperty(literal, name);
  if (property === undefined) {
    if (optional) return undefined;
    fail(where, `"${name}" is missing`);
  }
  const value = ts.isPropertyAssignment(property) ? property.initializer : undefined;
  if (value === undefined || !ts.isStringLiteralLike(value)) {
    fail(where, `"${name}" must be a string literal`);
  }
  return value.text;
}

function readBoolean(literal, name, where) {
  const property = getProperty(literal, name);
  if (property === undefined) return undefined;
  const value = ts.isPropertyAssignment(property) ? property.initializer : undefined;
  if (value?.kind === ts.SyntaxKind.TrueKeyword) return true;
  if (value?.kind === ts.SyntaxKind.FalseKeyword) return false;
  return fail(where, `"${name}" must be true or false`);
}

function readStringArray(literal, name, where) {
  const property = getProperty(literal, name);
  if (property === undefined) return undefined;
  const value = ts.isPropertyAssignment(property) ? property.initializer : undefined;
  if (value === undefined || !ts.isArrayLiteralExpression(value)) {
    fail(where, `"${name}" must be an array of string literals`);
  }
  return value.elements.map((element) => {
    if (!ts.isStringLiteralLike(element)) fail(where, `"${name}" must hold string literals only`);
    return element.text;
  });
}

function readObject(literal, name, where) {
  const property = getProperty(literal, name);
  if (property === undefined) return undefined;
  const value = ts.isPropertyAssignment(property) ? property.initializer : undefined;
  if (value === undefined || !ts.isObjectLiteralExpression(value)) {
    fail(where, `"${name}" must be an object literal`);
  }
  return value;
}

function readStringMap(literal, name, where) {
  const object = readObject(literal, name, where);
  if (object === undefined) return undefined;
  return Object.fromEntries(
    object.properties.map((property) => {
      const key = getPropertyName(property);
      const value = ts.isPropertyAssignment(property) ? property.initializer : undefined;
      if (key === undefined || value === undefined || !ts.isStringLiteralLike(value)) {
        fail(where, `"${name}" must map names to string literals`);
      }
      return [key, value.text];
    }),
  );
}

function collapse(text) {
  return text.split(/\s+/).filter(Boolean).join(" ");
}

function getJsDoc(node) {
  const doc = node.jsDoc?.at(-1);
  return doc === undefined ? undefined : doc;
}

function getDocText(doc) {
  return doc?.comment === undefined ? "" : (ts.getTextOfJSDocComment(doc.comment) ?? "");
}

/** Every `defineProviderOperator` call of `sourceFile` that passes `execute`. */
function findImplementations(sourceFile) {
  const found = [];
  const visit = (node) => {
    if (
      ts.isCallExpression(node) &&
      ts.isIdentifier(node.expression) &&
      node.expression.text === DEFINE &&
      node.arguments[0] !== undefined &&
      ts.isObjectLiteralExpression(node.arguments[0]) &&
      getProperty(node.arguments[0], "execute") !== undefined
    ) {
      found.push(node);
    }
    ts.forEachChild(node, visit);
  };
  visit(sourceFile);
  return found;
}

function findDeclaration(call) {
  let node = call.parent;
  while (node !== undefined && !ts.isVariableStatement(node)) node = node.parent;
  return node;
}

function findArgsInterface(sourceFile, name, where) {
  const found = sourceFile.statements.find(
    (statement) => ts.isInterfaceDeclaration(statement) && statement.name.text === name,
  );
  if (found === undefined) fail(where, `interface ${name} is not declared in the same file`);
  if (found.heritageClauses !== undefined) fail(where, `interface ${name} must not extend another`);
  return found;
}

function unwrapType(type) {
  let current = type;
  for (;;) {
    if (ts.isParenthesizedTypeNode(current)) current = current.type;
    else if (ts.isTypeOperatorNode(current) && current.operator === ts.SyntaxKind.ReadonlyKeyword)
      current = current.type;
    else if (
      ts.isTypeReferenceNode(current) &&
      ts.isIdentifier(current.typeName) &&
      current.typeName.text === "Readonly" &&
      current.typeArguments?.length === 1
    )
      current = current.typeArguments[0];
    else return current;
  }
}

function isTemplatedType(type) {
  return (
    ts.isTypeReferenceNode(type) &&
    ts.isIdentifier(type.typeName) &&
    type.typeName.text === "Templated" &&
    type.typeArguments?.length === 1
  );
}

/** A TypeScript type as the spec type kind it stands for. */
function describeType(typeNode, encoding, where) {
  const type = unwrapType(typeNode);
  const unsupported = () =>
    fail(
      where,
      `type \`${type.getText()}\` has no spec type kind; use a string, boolean, number, ` +
        "literal union, `T | null`, array, `Record<string, T>` or `JsonValue`",
    );
  switch (type.kind) {
    case ts.SyntaxKind.StringKeyword:
      return { kind: "string" };
    case ts.SyntaxKind.BooleanKeyword:
      return { kind: "boolean" };
    case ts.SyntaxKind.NumberKeyword:
      return { kind: encoding === "timedelta" ? "timedelta" : "number" };
    case ts.SyntaxKind.UnknownKeyword:
      return { kind: "any" };
    case ts.SyntaxKind.LiteralType: {
      const literal = type.literal;
      if (literal.kind === ts.SyntaxKind.NullKeyword) return { kind: "null" };
      return { kind: "literal", literals: [readLiteral(literal, unsupported)] };
    }
    case ts.SyntaxKind.ArrayType:
      return { kind: "array", items: describeType(type.elementType, undefined, where) };
    case ts.SyntaxKind.UnionType:
      return describeUnion(type, encoding, where);
    case ts.SyntaxKind.TypeReference:
      return describeReference(type, encoding, where, unsupported);
    default:
      return unsupported();
  }
}

function readLiteral(literal, unsupported) {
  if (ts.isStringLiteralLike(literal)) return literal.text;
  if (literal.kind === ts.SyntaxKind.TrueKeyword) return true;
  if (literal.kind === ts.SyntaxKind.FalseKeyword) return false;
  if (ts.isNumericLiteral(literal) && Number.isInteger(Number(literal.text))) {
    return Number(literal.text);
  }
  if (
    ts.isPrefixUnaryExpression(literal) &&
    literal.operator === ts.SyntaxKind.MinusToken &&
    ts.isNumericLiteral(literal.operand) &&
    Number.isInteger(Number(literal.operand.text))
  ) {
    return -Number(literal.operand.text);
  }
  return unsupported();
}

function describeReference(type, encoding, where, unsupported) {
  const name = ts.isIdentifier(type.typeName) ? type.typeName.text : undefined;
  const args = type.typeArguments ?? [];
  if (name === "Date" && args.length === 0) {
    if (encoding !== "datetime") {
      fail(where, 'a `Date` argument needs `encoding: "datetime"` in its `params` entry');
    }
    return { kind: "datetime" };
  }
  if (name === "JsonValue" && args.length === 0) return { kind: "any" };
  if ((name === "Array" || name === "ReadonlyArray") && args.length === 1) {
    return { kind: "array", items: describeType(args[0], undefined, where) };
  }
  if (name === "Record" && args.length === 2) {
    if (unwrapType(args[0]).kind !== ts.SyntaxKind.StringKeyword) {
      fail(where, "`Record` keys must be `string`");
    }
    return { kind: "object", values: describeType(args[1], undefined, where) };
  }
  if (name === "Templated") {
    fail(where, "`Templated` may only wrap the whole type of an argument");
  }
  return unsupported();
}

function describeUnion(type, encoding, where) {
  const options = [];
  let literals = null;
  for (const member of type.types) {
    const described = describeType(member, encoding, where);
    if (described.kind === "literal") {
      if (literals === null) {
        literals = { kind: "literal", literals: [] };
        options.push(literals);
      }
      literals.literals.push(...described.literals);
    } else if (!options.some((o) => JSON.stringify(o) === JSON.stringify(described))) {
      options.push(described);
    }
  }
  // `boolean` is `true | false` to the compiler, but the spec has a kind for it.
  return options.length === 1 ? options[0] : { kind: "union", options };
}

function readDefault(property, where) {
  const tag = ts.getJSDocTags(property).find((t) => t.tagName.text === "default");
  if (tag === undefined) return undefined;
  const text = collapse(ts.getTextOfJSDocComment(tag.comment) ?? "").replace(/^`(.*)`$/, "$1");
  let value;
  try {
    value = JSON.parse(text);
  } catch {
    return fail(
      where,
      `@default must be a JSON value, such as \`false\` or \`"utf-8"\`, not ${text}`,
    );
  }
  if (value === null || !["string", "number", "boolean", "object"].includes(typeof value)) {
    fail(where, "@default must be a string, number, boolean or array");
  }
  if (typeof value === "object" && !Array.isArray(value)) {
    fail(where, "@default must be a string, number, boolean or array");
  }
  return value;
}

function buildParam(key, definition, args, templateFields, className, relativeFile) {
  const where = `${relativeFile}: ${className}.params.${key}`;
  const name = readString(definition, "name", where);
  const encoding = readString(definition, "encoding", where, { optional: true });
  const required = readBoolean(definition, "required", where) === true;
  const property = args.members.find(
    (member) => ts.isPropertySignature(member) && getPropertyName(member) === key,
  );
  if (property === undefined || property.type === undefined) {
    fail(where, `interface ${className}Args has no typed property "${key}"`);
  }
  if (required === (property.questionToken !== undefined)) {
    fail(
      where,
      required
        ? `"${key}" is required in \`params\` but optional in ${className}Args`
        : `"${key}" is optional in \`params\` but required in ${className}Args`,
    );
  }
  const isTemplateField = templateFields.includes(name);
  const wrapped = isTemplatedType(unwrapType(property.type));
  if (isTemplateField !== wrapped) {
    fail(
      where,
      isTemplateField
        ? `"${name}" is a template field, so its type must be \`Templated<...>\``
        : `"${key}" has a \`Templated<...>\` type but "${name}" is not in \`templateFields\``,
    );
  }
  const inner = wrapped ? unwrapType(property.type).typeArguments[0] : property.type;
  const entry = { name, type: describeType(inner, encoding, where) };
  if (required) entry.required = true;
  if (isTemplateField) entry.templated = true;
  const defaultValue = readDefault(property, where);
  if (defaultValue !== undefined) {
    if (required) fail(where, "a required argument has no default");
    entry.default = defaultValue;
  }
  const description = collapse(getDocText(getJsDoc(property)));
  if (description) entry.description = description;
  return entry;
}

/** Whether `statement` is `export const <name> = ...`. */
function isExportedConst(statement, name) {
  return (
    statement !== undefined &&
    statement.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.ExportKeyword) ===
      true &&
    statement.declarationList.declarations.some(
      (declaration) => ts.isIdentifier(declaration.name) && declaration.name.text === name,
    )
  );
}

/**
 * The module a runtime imports to get the operator: the package name and the path of the source file
 * under `src/`, which the package's `exports` map to its built file.
 */
function getImplementationModule(packageName, relativeFile) {
  return `${packageName}/${relativeFile.replace(/^src\//, "").replace(/\.ts$/, "")}`;
}

function buildOperator(call, sourceFile, relativeFile, packageName) {
  const literal = call.arguments[0];
  const where = `${relativeFile}`;
  const module = readString(literal, "module", where);
  const className = readString(literal, "className", where);
  const scope = `${where}: ${className}`;
  if (!isExportedConst(findDeclaration(call), className)) {
    fail(
      scope,
      `must be declared as \`export const ${className} = ${DEFINE}(...)\`, the export a runtime imports`,
    );
  }
  const templateFields = readStringArray(literal, "templateFields", scope);
  if (templateFields === undefined) fail(scope, '"templateFields" is missing');
  const paramsLiteral = readObject(literal, "params", scope);
  if (paramsLiteral === undefined) fail(scope, '"params" is missing');
  const args = findArgsInterface(sourceFile, `${className}Args`, scope);

  const implementationModule = getImplementationModule(packageName, relativeFile);
  const declaredModule = readString(literal, "implementationModule", scope, { optional: true });
  if (declaredModule !== undefined && declaredModule !== implementationModule) {
    fail(
      scope,
      `"implementationModule" must be "${implementationModule}", not "${declaredModule}"`,
    );
  }
  const operator = {
    name: className,
    module,
    implementation_module: implementationModule,
    kind: readBoolean(literal, "isSensor", scope) === true ? "sensor" : "operator",
  };
  const description = collapse(
    getDocText(getJsDoc(findDeclaration(call) ?? call)).split(/\n\s*\n/)[0] ?? "",
  );
  if (description) operator.description = description;
  operator.template_fields = templateFields;
  const templateExt = readStringArray(literal, "templateExt", scope);
  if (templateExt?.length) operator.template_ext = templateExt;
  const renderers = readStringMap(literal, "templateFieldsRenderers", scope);
  if (renderers && Object.keys(renderers).length > 0)
    operator.template_fields_renderers = renderers;
  const uiColor = readString(literal, "uiColor", scope, { optional: true });
  if (uiColor !== undefined) operator.ui_color = uiColor;
  const uiFgcolor = readString(literal, "uiFgcolor", scope, { optional: true });
  if (uiFgcolor !== undefined) operator.ui_fgcolor = uiFgcolor;
  const operatorName = readString(literal, "operatorName", scope, { optional: true });
  if (operatorName !== undefined) operator.operator_name = operatorName;
  if (readBoolean(literal, "canSkipDownstream", scope) === true)
    operator.can_skip_downstream = true;

  const params = [];
  for (const property of paramsLiteral.properties) {
    const key = getPropertyName(property);
    const value = ts.isPropertyAssignment(property) ? property.initializer : undefined;
    if (key === undefined || value === undefined || !ts.isObjectLiteralExpression(value)) {
      fail(scope, "each entry of `params` must be `key: { name: ... }`");
    }
    params.push(buildParam(key, value, args, templateFields, className, relativeFile));
  }
  const names = params.map((p) => p.name);
  for (const field of templateFields) {
    if (!names.includes(field)) fail(scope, `template field "${field}" is not in \`params\``);
  }
  operator.params = params;
  return operator;
}

/** The spec of the TypeScript package in `packageDir`. */
export function buildSpec(packageDir) {
  const manifest = readPackageJson(packageDir);
  const { providerId, pythonPackage } = manifest.airflow ?? {};
  if (typeof providerId !== "string" || typeof pythonPackage !== "string") {
    throw new SpecError(
      `${join(packageDir, "package.json")}: "airflow" needs "providerId" and "pythonPackage"`,
    );
  }
  if (typeof manifest.version !== "string") {
    throw new SpecError(`${join(packageDir, "package.json")}: "version" is missing`);
  }
  const operators = [];
  const seen = new Set();
  for (const file of listSourceFiles(join(packageDir, "src"))) {
    const text = readFileSync(file, "utf8");
    const sourceFile = ts.createSourceFile(
      file,
      text,
      ts.ScriptTarget.Latest,
      true,
      ts.ScriptKind.TS,
    );
    const relativeFile = relative(packageDir, file).split(sep).join("/");
    for (const call of findImplementations(sourceFile)) {
      const operator = buildOperator(call, sourceFile, relativeFile, manifest.name);
      const classpath = `${operator.module}.${operator.name}`;
      if (seen.has(classpath)) throw new SpecError(`${classpath} is implemented twice`);
      seen.add(classpath);
      operators.push(operator);
    }
  }
  if (operators.length === 0) {
    throw new SpecError(`${packageDir}: no ${DEFINE} call with an \`execute\` found under src/`);
  }
  operators.sort((a, b) => compare(a.module, b.module) || compare(a.name, b.name));
  return {
    spec_version: SPEC_VERSION,
    provider: { id: providerId, package: pythonPackage, version: manifest.version },
    implemented_in: ["TypeScript"],
    operators,
    skipped: [],
  };
}

function compare(a, b) {
  return a < b ? -1 : a > b ? 1 : 0;
}

/** JSON as Python's `json.dumps` writes it on one line: `{"a": 1, "b": [2, 3]}`. */
function inlineJson(value) {
  if (Array.isArray(value)) return `[${value.map(inlineJson).join(", ")}]`;
  if (value !== null && typeof value === "object") {
    return `{${Object.entries(value)
      .map(([key, item]) => `${JSON.stringify(key)}: ${inlineJson(item)}`)
      .join(", ")}}`;
  }
  return JSON.stringify(value);
}

class Inline {
  constructor(value) {
    this.value = value;
  }
}

function prettyJson(value, depth = 0) {
  if (value instanceof Inline) return inlineJson(value.value);
  const pad = "  ".repeat(depth + 1);
  const close = "  ".repeat(depth);
  if (Array.isArray(value)) {
    if (value.length === 0) return "[]";
    return `[\n${value.map((item) => pad + prettyJson(item, depth + 1)).join(",\n")}\n${close}]`;
  }
  if (value !== null && typeof value === "object") {
    const entries = Object.entries(value);
    if (entries.length === 0) return "{}";
    return `{\n${entries
      .map(([key, item]) => `${pad}${JSON.stringify(key)}: ${prettyJson(item, depth + 1)}`)
      .join(",\n")}\n${close}}`;
  }
  return JSON.stringify(value);
}

/**
 * The text of a spec file: one argument per line, as `generate_operator_specs.py` writes it, so a
 * changed argument is a one-line diff.
 */
export function renderSpec(spec) {
  const shaped = {
    ...spec,
    operators: spec.operators.map((operator) => ({
      ...operator,
      params: operator.params.map((param) => new Inline(param)),
    })),
    skipped: spec.skipped.map((entry) => new Inline(entry)),
  };
  return `${prettyJson(shaped)}\n`;
}

/**
 * Write the spec of each package in `packageDirs` under `providerSourcesDir`, or with `check` only
 * report which differ. Returns the spec paths that were, or would be, written.
 */
export function generate({ packageDirs, providerSourcesDir, check }) {
  const changed = [];
  for (const packageDir of packageDirs) {
    const spec = buildSpec(packageDir);
    const target = getSpecPath(providerSourcesDir, spec.provider.id);
    const content = renderSpec(spec);
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
  return changed;
}

function main(argv) {
  const check = argv.includes("--check");
  const given = argv.filter((arg) => !arg.startsWith("--")).map((arg) => resolve(arg));
  const packageDirs = given.length > 0 ? given : findImplementedPackages(PROVIDERS_DIR);
  if (packageDirs.length === 0) {
    console.log("no provider package implements operators");
    return 0;
  }
  let changed;
  try {
    changed = generate({ packageDirs, providerSourcesDir: PROVIDER_SOURCES_DIR, check });
  } catch (error) {
    if (!(error instanceof SpecError)) throw error;
    console.error(error.message);
    return 1;
  }
  const paths = changed.map((path) =>
    relative(resolve(TS_SDK_ROOT, ".."), path).split(sep).join("/"),
  );
  if (check) {
    if (paths.length > 0) {
      console.error(`out of date, run \`pnpm run generate:operator-spec\`:\n${paths.join("\n")}`);
      return 1;
    }
    console.log("operator specs are up to date");
    return 0;
  }
  for (const path of paths) console.log(path);
  console.log(`wrote ${paths.length} specs`);
  return 0;
}

// Importable for tests; runs only when invoked as a script.
if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) {
  process.exitCode = main(process.argv.slice(2));
}
