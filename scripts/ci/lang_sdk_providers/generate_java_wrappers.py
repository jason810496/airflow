# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
r"""
Generate the typed Java wrappers of provider operators.

A spec at ``providers/<provider>/_generated/lang_sdk_operators.json`` describes the operators of one
provider, and a Dag written in Java uses an operator through a wrapper generated from it. Each Python
operator class becomes one Java class of the same name, in the package of its Python module under
``org.apache.airflow.providers``, with a ``builder()`` whose methods are the operator's arguments.
``build()`` returns the task to pass to ``DagDef.task``. The specs follow
``airflow-core/src/airflow/lang_sdk_provider_operators.schema.json``. See
``airflow-core/adr/lang-sdk/0018-provider-operators-across-sdks.md``::

    python3 scripts/ci/lang_sdk_providers/generate_java_wrappers.py

Which providers get a wrapper, and in which Gradle project of ``java-sdk``, is listed in
``JAVA_SDK_PROVIDERS``. A wrapper of an operator that Python implements writes the Python class path and
the arguments, and a Python worker runs it. A wrapper of an operator that a Java provider implements also
names the module that holds it, so the Java runtime runs it. Everything under a provider's package that
the specs do not generate is removed.

``--check`` writes nothing and fails when a wrapper is out of date.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import textwrap
from pathlib import Path
from typing import Any

SUPPORTED_SPEC_MAJOR = "1"
REPO_ROOT = Path(__file__).resolve().parents[3]
PROVIDERS_DIR = REPO_ROOT / "providers"
JAVA_SDK_DIR = REPO_ROOT / "java-sdk"
SPEC_FILE_NAME = "lang_sdk_operators.json"
SCRIPT = "scripts/ci/lang_sdk_providers/generate_java_wrappers.py"

# The providers that have wrappers, and the Gradle project of the Java SDK that holds each one.
JAVA_SDK_PROVIDERS = {
    "standard": "providers/standard",
    "amazon": "providers/amazon",
    "java.localfile": "providers/localfile",
}
BASE_PACKAGE = "org.apache.airflow.providers"
SOURCE_ROOT = Path("src") / "main" / "java"

LICENSE = """\
/*
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
"""

DOC_WIDTH = 92
LINE_LENGTH = 100
INDENT = "  "

JAVA_KEYWORDS = frozenset(
    "abstract assert boolean break byte case catch char class const continue default do double else "
    "enum extends final finally float for goto if implements import instanceof int interface long native "
    "new package private protected public return short static strictfp super switch synchronized this "
    "throw throws transient try void volatile while true false null var".split()
)
# Methods of the builder that an argument must not take the name of.
RESERVED_METHODS = frozenset(
    "build param getClass hashCode equals toString notify notifyAll wait clone finalize".split()
)

_PRIMITIVES = {"string": "String", "boolean": "boolean", "integer": "long", "number": "double"}
_BOXED = {"boolean": "Boolean", "long": "Long", "double": "Double"}

TypeSpec = dict[str, Any]


class UnsupportedSpecError(ValueError):
    """The spec has something a Java wrapper cannot express."""


def to_camel_case(name: str) -> str:
    """`bucket_name` -> `bucketName`, with `_` added when the result is not a usable method name."""
    camel = re.sub(r"_+([a-zA-Z0-9])", lambda match: match.group(1).upper(), name.strip("_"))
    return camel + "_" if camel in JAVA_KEYWORDS | RESERVED_METHODS else camel


def java_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=True)


def escape_javadoc(text: str) -> str:
    """Return *text* as it can stand in a Javadoc comment: HTML-escaped, ASCII, and with no ``*/``."""
    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    text = text.replace("\\", "&#92;").replace("*/", "*&#47;").replace("{@", "{&#64;")
    text = re.sub(r"(?m)^@", "&#64;", text)
    return "".join(char if ord(char) < 128 else f"&#{ord(char)};" for char in text)


def render_doc(paragraphs: list[str], indent: str) -> list[str]:
    """
    Render *paragraphs* as a Javadoc comment, one wrapped paragraph after another.

    A paragraph is Javadoc already, so text that comes from a spec goes through :func:`escape_javadoc` first.
    """
    lines: list[str] = []
    for paragraph in (" ".join(p.split()) for p in paragraphs):
        if not paragraph:
            continue
        if lines:
            lines.append("")
        lines += textwrap.wrap(
            paragraph, DOC_WIDTH - len(indent), break_on_hyphens=False, break_long_words=False
        )
    if not lines:
        return []
    if len(lines) == 1:
        return [f"{indent}/** {lines[0]} */"]
    return [f"{indent}/**", *(f"{indent} * {line}".rstrip() for line in lines), f"{indent} */"]


def _type(spec: TypeSpec, *, boxed: bool, used: set[str]) -> str:
    kind = spec["kind"]
    if kind in _PRIMITIVES:
        name = _PRIMITIVES[kind]
        return _BOXED.get(name, name) if boxed else name
    if kind == "null":
        return "Object"
    if kind == "any":
        return "Object"
    if kind == "timedelta":
        used.add("java.time.Duration")
        return "Duration"
    if kind == "datetime":
        used.add("java.time.OffsetDateTime")
        return "OffsetDateTime"
    if kind == "literal":
        return "String" if all(isinstance(value, str) for value in spec["literals"]) else "Object"
    if kind == "array":
        used.add("java.util.List")
        return f"List<{_type(spec['items'], boxed=True, used=used)}>"
    if kind == "tuple":
        used.add("java.util.List")
        return "List<Object>"
    if kind == "object":
        used.add("java.util.Map")
        return f"Map<String, {_type(spec['values'], boxed=True, used=used)}>"
    if kind == "struct":
        used.add("java.util.Map")
        return "Map<String, Object>"
    if kind == "union":
        options = [option for option in spec["options"] if option["kind"] != "null"]
        rendered = list(dict.fromkeys(_type(option, boxed=boxed, used=used) for option in options))
        if len(rendered) == 1:
            return rendered[0]
        if {option["kind"] for option in options} <= {"integer", "number"}:
            return "double" if not boxed else "Double"
        return "Object"
    raise UnsupportedSpecError(f"the type kind {kind!r} is not supported in a Java wrapper")


def render_type(spec: TypeSpec, *, used: set[str]) -> str:
    """Return the Java type of the method parameter for *spec*, adding what it imports to *used*."""
    return _type(spec, boxed=False, used=used)


def render_param_doc(param: dict[str, Any]) -> list[str]:
    notes = [param.get("description", "")]
    if param["type"]["kind"] == "literal":
        notes.append("One of " + ", ".join(json.dumps(value) for value in param["type"]["literals"]) + ".")
    if "default" in param:
        notes.append(f"Defaults to {json.dumps(param['default'])}.")
    if param.get("templated"):
        notes.append("Templated.")
    return render_doc([escape_javadoc(" ".join(note for note in notes if note))], INDENT * 2)


def render_methods(operator: dict[str, Any], *, used: set[str]) -> str:
    names: dict[str, str] = {}
    chunks = []
    for param in operator["params"]:
        method = to_camel_case(param["name"])
        if method in names:
            raise UnsupportedSpecError(
                f"{operator['module']}.{operator['name']}: arguments {names[method]!r} and "
                f"{param['name']!r} both become {method!r}"
            )
        names[method] = param["name"]
        java_type = render_type(param["type"], used=used)
        doc = render_param_doc(param)
        signatures = [java_type]
        if param.get("templated") and java_type != "Object":
            # The result of a task, which the worker resolves when it renders the templates. An
            # Object argument takes it already.
            used.add("org.apache.airflow.sdk.TaskRef")
            signatures.append("TaskRef<?>")
        for signature in signatures:
            body = [
                *doc,
                f"{INDENT * 2}public Builder {method}({signature} value) {{",
                f"{INDENT * 3}return param({java_string(param['name'])}, value);",
                f"{INDENT * 2}}}",
            ]
            chunks.append("\n".join(body))
    return "\n\n".join(chunks)


def render_call(method: str, values: list[str], indent: str) -> list[str]:
    """Render ``.method("a", "b")``, one argument to a line when it does not fit."""
    items = [java_string(value) for value in values]
    inline = f"{indent}.{method}({', '.join(items)})"
    if len(inline) <= LINE_LENGTH:
        return [inline]
    arguments = [f"{indent}{INDENT * 2}{item}," for item in items]
    arguments[-1] = arguments[-1].removesuffix(",") + ")"
    return [f"{indent}.{method}(", *arguments]


def render_info(operator: dict[str, Any]) -> list[str]:
    def strings(values: list[str]) -> str:
        return ", ".join(java_string(value) for value in values)

    lines = [
        f"{INDENT}private static final OperatorInfo INFO =",
        f"{INDENT * 3}OperatorInfo.of({java_string(operator['module'])}, {java_string(operator['name'])})",
        *render_call("params", [param["name"] for param in operator["params"]], INDENT * 5),
    ]
    required = [param["name"] for param in operator["params"] if param.get("required")]
    if required:
        lines += render_call("required", required, INDENT * 5)
    if operator["template_fields"]:
        lines += render_call("templated", operator["template_fields"], INDENT * 5)
    if "implementation_module" in operator:
        lines.append(f"{INDENT * 5}.implementationModule({java_string(operator['implementation_module'])})")
    if operator.get("template_ext"):
        lines += render_call("templateExt", operator["template_ext"], INDENT * 5)
    if operator.get("template_fields_renderers"):
        pairs = ", ".join(
            f"{java_string(key)}, {java_string(value)}"
            for key, value in operator["template_fields_renderers"].items()
        )
        lines.append(f"{INDENT * 5}.templateFieldsRenderers(Map.of({pairs}))")
    for key, method in (
        ("ui_color", "uiColor"),
        ("ui_fgcolor", "uiFgcolor"),
        ("operator_name", "operatorName"),
    ):
        if key in operator:
            lines.append(f"{INDENT * 5}.{method}({java_string(operator[key])})")
    if operator["kind"] == "sensor":
        lines.append(f"{INDENT * 5}.sensor()")
    if operator.get("can_skip_downstream"):
        lines.append(f"{INDENT * 5}.canSkipDownstream()")
    lines[-1] += ";"
    return lines


def render_class(operator: dict[str, Any], package: str, *, spec_path: str, provider: dict[str, Any]) -> str:
    name = operator["name"]
    used = {
        "org.apache.airflow.sdk.provider.OperatorBuilder",
        "org.apache.airflow.sdk.provider.OperatorInfo",
    }
    if operator.get("template_fields_renderers"):
        used.add("java.util.Map")
    methods = render_methods(operator, used=used)
    implemented = "implementation_module" in operator
    runs_on = (
        f"Runs {{@code {operator['module']}.{name}}} of {{@code {provider['package']}}} in the Java "
        "runtime, which a Java coordinator or a Dag in Java provides."
        if implemented
        else f"Runs {{@code {operator['module']}.{name}}} of {{@code {provider['package']}}} on a Python "
        "worker, so give the task a queue that a Python worker serves."
    )
    omitted = [f"{{@code {item['name']}}}" for item in operator.get("omitted_params", [])]
    doc = render_doc(
        [
            escape_javadoc(operator.get("description", "")),
            runs_on,
            f"Not available from Java, because JSON cannot carry them: {', '.join(omitted)}."
            if omitted
            else "",
        ],
        "",
    )
    imports = "\n".join(f"import {name};" for name in sorted(used))
    lines = [
        LICENSE,
        f"// Generated from {spec_path} by",
        f"// {SCRIPT}. Do not edit by hand.",
        "",
        f"package {package};",
        "",
        imports,
        "",
        *doc,
        f"public final class {name} {{",
        f"{INDENT}/** The Python class path of the operator. */",
        f'{INDENT}public static final String CLASSPATH = "{operator["module"]}.{name}";',
        "",
        *render_info(operator),
        "",
        f"{INDENT}private {name}() {{}}",
        "",
        f"{INDENT}/** Starts building a task of this operator. */",
        f"{INDENT}public static Builder builder() {{",
        f"{INDENT * 2}return new Builder();",
        f"{INDENT}}}",
        "",
        f"{INDENT}/** Builds a task of {{@code {name}}}, to pass to {{@code DagDef.task}}. */",
        f"{INDENT}public static final class Builder extends OperatorBuilder<Builder, Object> {{",
        f"{INDENT * 2}private Builder() {{",
        f"{INDENT * 3}super(INFO);",
        f"{INDENT * 2}}}",
    ]
    if methods:
        lines += ["", methods]
    lines += [f"{INDENT}}}", "}", ""]
    return "\n".join(lines)


def get_package(module: str, provider_id: str) -> str:
    """Return the Java package of the Python *module*: ``org.apache.airflow.providers.amazon.aws.operators.s3``."""
    prefix = f"airflow.providers.{provider_id}."
    if not module.startswith(prefix) and module != prefix.removesuffix("."):
        raise UnsupportedSpecError(f"Module {module} is not part of provider {provider_id}")
    return module.replace("airflow.providers.", f"{BASE_PACKAGE}.", 1)


def build_files(spec: dict[str, Any]) -> dict[str, str]:
    """Return the text of each wrapper of *spec*, by its path under the Gradle project's ``src/main/java``."""
    major = spec["spec_version"].split(".")[0]
    if major != SUPPORTED_SPEC_MAJOR:
        raise UnsupportedSpecError(
            f"The spec has version {spec['spec_version']}; this generator reads version {SUPPORTED_SPEC_MAJOR}.x"
        )
    provider = spec["provider"]
    provider_id = provider["id"]
    spec_path = f"providers/{provider_id.replace('.', '/')}/_generated/{SPEC_FILE_NAME}"
    files: dict[str, str] = {}
    for operator in spec["operators"]:
        package = get_package(operator["module"], provider_id)
        path = f"{package.replace('.', '/')}/{operator['name']}.java"
        if path in files:
            raise UnsupportedSpecError(f"{operator['module']}.{operator['name']} is in the spec twice")
        files[path] = render_class(operator, package, spec_path=spec_path, provider=provider)
    return files


def validate_spec(spec: dict[str, Any]) -> None:
    """Raise ``jsonschema.ValidationError`` when *spec* does not follow the spec schema."""
    import jsonschema

    schema_path = REPO_ROOT / "airflow-core" / "src" / "airflow" / "lang_sdk_provider_operators.schema.json"
    jsonschema.validate(spec, json.loads(schema_path.read_text()))


def find_spec_files(providers_dir: Path) -> list[Path]:
    return sorted(providers_dir.glob(f"**/_generated/{SPEC_FILE_NAME}"))


def get_generated_root(spec: dict[str, Any]) -> str:
    """Return the directory, under ``src/main/java``, that holds everything generated from *spec*."""
    return f"{BASE_PACKAGE}.{spec['provider']['id']}".replace(".", "/")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument(
        "--providers-dir", type=Path, default=PROVIDERS_DIR, help="Directory of the providers."
    )
    parser.add_argument("--java-sdk-dir", type=Path, default=JAVA_SDK_DIR, help="Directory of the Java SDK.")
    parser.add_argument("--check", action="store_true", help="Fail when a wrapper is out of date.")
    args = parser.parse_args(argv)

    status = 0
    found = set()
    for spec_file in find_spec_files(args.providers_dir):
        spec = json.loads(spec_file.read_text())
        provider_id = spec["provider"]["id"]
        if provider_id not in JAVA_SDK_PROVIDERS:
            continue
        found.add(provider_id)
        validate_spec(spec)
        source_root = args.java_sdk_dir / JAVA_SDK_PROVIDERS[provider_id] / SOURCE_ROOT
        expected = build_files(spec)
        generated_root = source_root / get_generated_root(spec)
        stale = [
            path
            for path in sorted(generated_root.rglob("*.java"))
            if path.relative_to(source_root).as_posix() not in expected
        ]
        written = 0
        for relative_path, content in expected.items():
            path = source_root / relative_path
            if path.is_file() and path.read_text() == content:
                continue
            if args.check:
                print(f"{path} is out of date. Re-run {SCRIPT}", file=sys.stderr)
                status = 1
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
            written += 1
        for path in stale:
            if args.check:
                print(f"{path} is not generated from {provider_id}'s spec. Re-run {SCRIPT}", file=sys.stderr)
                status = 1
            else:
                path.unlink()
                written += 1
        if not args.check:
            print(f"{provider_id}: {written} file(s) written or removed", file=sys.stderr)
    if missing := sorted(set(JAVA_SDK_PROVIDERS) - found):
        print(f"No spec found for {', '.join(missing)} under {args.providers_dir}", file=sys.stderr)
        status = 1
    return status


if __name__ == "__main__":
    sys.exit(main())
