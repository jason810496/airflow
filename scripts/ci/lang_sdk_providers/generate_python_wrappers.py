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
Generate the Python wrappers of the providers that a language SDK implements.

A spec at ``providers/<provider>/_generated/lang_sdk_operators.json`` that names ``implemented_in`` is
written by the SDK that implements the provider. This script turns it into Python classes that
only describe the operators, so a Python Dag can use them. Each class subclasses
``LangSdkProviderOperator``, serializes its arguments as argument bindings and names the module that
implements it, and the runtime of the task's queue runs the operator. One module is written for each ``module`` of the spec, under
``providers/<provider>/src/airflow/providers/...``. Specs without ``implemented_in`` belong to
providers implemented in Python and are left alone. The specs follow
``airflow-core/src/airflow/lang_sdk_provider_operators.schema.json``. See
``airflow-core/adr/lang-sdk/0018-provider-operators-implemented-in-lang-sdks.md``::

    python3 scripts/ci/lang_sdk_providers/generate_python_wrappers.py

``--check`` writes nothing and fails when a wrapper is out of date.
"""

from __future__ import annotations

import argparse
import json
import keyword
import sys
import textwrap
from pathlib import Path
from typing import Any

SUPPORTED_SPEC_MAJOR = "1"
REPO_ROOT = Path(__file__).resolve().parents[3]
PROVIDERS_DIR = REPO_ROOT / "providers"
SPEC_FILE_NAME = "lang_sdk_operators.json"
SCHEMA_PATH = REPO_ROOT / "airflow-core" / "src" / "airflow" / "lang_sdk_provider_operators.schema.json"
SCRIPT = "scripts/ci/lang_sdk_providers/generate_python_wrappers.py"

LICENSE = """\
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
"""

LINE_LENGTH = 110
DOC_WIDTH = 100
INDENT = "    "
_PRIMITIVES = {"string": "str", "boolean": "bool", "integer": "int", "number": "float", "null": "None"}

TypeSpec = dict[str, Any]


class UnsupportedSpecError(ValueError):
    """The spec has something a Python wrapper cannot express."""


def quote(value: str) -> str:
    """Return *value* as a double-quoted Python string, the way ``ruff format`` writes it."""
    text = json.dumps(value, ensure_ascii=False)
    if '\\"' in text and "'" not in value and "\\" not in value:
        return "'" + text[1:-1].replace('\\"', '"') + "'"
    return text


def render_value(value: Any) -> str:
    """Render a literal or default value of a spec."""
    if isinstance(value, str):
        return quote(value)
    if isinstance(value, list):
        return "[" + ", ".join(render_value(item) for item in value) + "]"
    return repr(value)


def render_type(spec: TypeSpec, *, used: set[str], where: str) -> str:
    """
    Return the annotation of the spec type *spec*.

    The names the annotation needs are added to *used*. *where* names the operator argument, for errors.
    """
    kind = spec["kind"]
    if kind in _PRIMITIVES:
        return _PRIMITIVES[kind]
    if kind == "any":
        used.add("Any")
        return "Any"
    if kind == "literal":
        used.add("Literal")
        return f"Literal[{', '.join(render_value(value) for value in spec['literals'])}]"
    if kind == "array":
        used.add("Sequence")
        return f"Sequence[{render_type(spec['items'], used=used, where=where)}]"
    if kind == "object":
        used.add("Mapping")
        return f"Mapping[str, {render_type(spec['values'], used=used, where=where)}]"
    if kind == "union":
        options = [render_type(option, used=used, where=where) for option in spec["options"]]
        return " | ".join(dict.fromkeys(options))
    raise UnsupportedSpecError(f"{where}: the type kind {kind!r} is not supported in a Python wrapper")


def render_class_tuple(name: str, values: list[str]) -> list[str]:
    """Render ``name = (...)`` as a class attribute, one item to a line when it does not fit."""
    items = [quote(value) for value in values]
    inline = f"{INDENT}{name} = ({', '.join(items)}{',' if len(items) == 1 else ''})"
    if len(inline) <= LINE_LENGTH:
        return [inline]
    return [f"{INDENT}{name} = (", *(f"{INDENT * 2}{item}," for item in items), f"{INDENT})"]


def escape_docstring(text: str) -> str:
    return text.replace("\\", "\\\\").replace('"""', '\\"\\"\\"')


def render_docstring(operator: dict[str, Any]) -> list[str]:
    """Render the class docstring: the description and a ``:param:`` line for each documented argument."""
    description = " ".join(operator.get("description", "").split())
    if not description:
        runtimes = " or ".join(operator["implemented_in"])
        description = f"The ``{operator['name']}`` operator, implemented in {runtimes}."
    summary, _, rest = description.partition(". ")
    summary = summary if summary.endswith((".", "!", "?")) else summary + "."
    body = [""] + textwrap.wrap(rest, DOC_WIDTH) if rest else []
    params = [
        textwrap.fill(
            f":param {param['name']}: {' '.join(param['description'].split())}",
            DOC_WIDTH,
            subsequent_indent=INDENT,
        ).splitlines()
        for param in operator["params"]
        if param.get("description")
    ]
    if params:
        body += [""] + [line for lines in params for line in lines]
    lines = [escape_docstring(line) for line in [summary, *body]]
    if not body:
        return [f'{INDENT}"""{lines[0]}"""']
    return [f'{INDENT}"""', *(f"{INDENT}{line}".rstrip() for line in lines), f'{INDENT}"""']


def render_parameter(param: dict[str, Any], *, used: set[str], where: str) -> str:
    name = param["name"]
    if not name.isidentifier() or keyword.iskeyword(name):
        raise UnsupportedSpecError(f"{where}: {name!r} is not a valid Python argument name")
    annotation = render_type(param["type"], used=used, where=f"{where}.{name}")
    if param.get("required"):
        return f"{name}: {annotation}"
    # A list default would be shared between instances, so the constructor builds it.
    if "default" in param and not isinstance(param["default"], list):
        return f"{name}: {annotation} = {render_value(param['default'])}"
    if not _accepts_none(param["type"]):
        annotation += " | None"
    return f"{name}: {annotation} = None"


def _accepts_none(spec: TypeSpec) -> bool:
    if spec["kind"] in ("null", "any"):
        return True
    if spec["kind"] == "literal":
        return None in spec["literals"]
    return spec["kind"] == "union" and any(_accepts_none(option) for option in spec["options"])


def render_class(operator: dict[str, Any], *, used: set[str]) -> str:
    where = operator["name"]
    params = operator["params"]
    parameters = [render_parameter(param, used=used, where=where) for param in params]
    lines = [f"class {operator['name']}(LangSdkProviderOperator):", *render_docstring(operator), ""]
    lines += render_class_tuple("template_fields", operator["template_fields"])
    lines += render_class_tuple("operator_params", [param["name"] for param in params])
    lines += render_class_tuple("implemented_in", operator["implemented_in"])
    if "implementation_module" not in operator:
        raise UnsupportedSpecError(
            f"{where}: an operator implemented in a Lang SDK needs an implementation_module"
        )
    lines.append(f"{INDENT}implementation_module = {quote(operator['implementation_module'])}")
    for attribute in ("ui_color", "ui_fgcolor"):
        if attribute in operator:
            lines.append(f"{INDENT}{attribute} = {quote(operator[attribute])}")
    lines += ["", f"{INDENT}def __init__(", f"{INDENT * 2}self,", f"{INDENT * 2}*,"]
    lines += [f"{INDENT * 2}{parameter}," for parameter in parameters]
    lines += [f"{INDENT * 2}**kwargs,", f"{INDENT}) -> None:", f"{INDENT * 2}super().__init__(**kwargs)"]
    for param in params:
        name = param["name"]
        if isinstance(param.get("default"), list):
            lines.append(
                f"{INDENT * 2}self.{name} = {render_value(param['default'])} if {name} is None else {name}"
            )
        else:
            lines.append(f"{INDENT * 2}self.{name} = {name}")
    return "\n".join(lines) + "\n"


def get_module_path(module: str, provider_id: str) -> str:
    """Return the path of *module* under the provider's ``src``, such as ``airflow/providers/x/a.py``."""
    prefix = f"airflow.providers.{provider_id}."
    if not module.startswith(prefix):
        raise UnsupportedSpecError(f"Module {module} is not part of provider {provider_id}")
    return module.replace(".", "/") + ".py"


def build_modules(spec: dict[str, Any]) -> dict[str, str]:
    """Return the text of each wrapper module of *spec*, by its path under the provider's ``src``."""
    major = spec["spec_version"].split(".")[0]
    if major != SUPPORTED_SPEC_MAJOR:
        raise UnsupportedSpecError(
            f"The spec has version {spec['spec_version']}; this generator reads version {SUPPORTED_SPEC_MAJOR}.x"
        )
    provider_id = spec["provider"]["id"]
    spec_path = f"providers/{provider_id.replace('.', '/')}/_generated/{SPEC_FILE_NAME}"
    by_module: dict[str, list[dict[str, Any]]] = {}
    for operator in spec["operators"]:
        by_module.setdefault(operator["module"], []).append(
            {**operator, "implemented_in": spec["implemented_in"]}
        )

    modules: dict[str, str] = {}
    for module, operators in by_module.items():
        used: set[str] = set()
        classes = [render_class(operator, used=used) for operator in operators]
        stdlib = []
        if collections_names := [name for name in ("Mapping", "Sequence") if name in used]:
            stdlib.append(f"from collections.abc import {', '.join(collections_names)}")
        if typing_names := [name for name in ("Any", "Literal") if name in used]:
            stdlib.append(f"from typing import {', '.join(typing_names)}")
        groups = [
            ["from __future__ import annotations"],
            stdlib,
            ["from airflow.sdk import LangSdkProviderOperator"],
        ]
        imports = "\n\n".join("\n".join(group) for group in groups if group)
        header = f"# Generated from {spec_path} by\n# {SCRIPT}. Do not edit by hand.\n"
        modules[get_module_path(module, provider_id)] = f"{LICENSE}\n{header}\n{imports}\n\n\n" + "\n\n".join(
            classes
        )
    return modules


def validate_spec(spec: dict[str, Any]) -> None:
    """Raise ``jsonschema.ValidationError`` when *spec* does not follow the spec schema."""
    import jsonschema

    jsonschema.validate(spec, json.loads(SCHEMA_PATH.read_text()))


def find_spec_files(providers_dir: Path) -> list[Path]:
    return sorted(providers_dir.glob(f"**/_generated/{SPEC_FILE_NAME}"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument(
        "--providers-dir",
        type=Path,
        default=PROVIDERS_DIR,
        help="Directory that holds the providers, each with its _generated/ spec.",
    )
    parser.add_argument("--check", action="store_true", help="Fail when a wrapper is out of date.")
    args = parser.parse_args(argv)

    status = 0
    for spec_file in find_spec_files(args.providers_dir):
        spec = json.loads(spec_file.read_text())
        validate_spec(spec)
        if "implemented_in" not in spec:
            continue
        provider_dir = spec_file.parent.parent
        written = 0
        for relative_path, content in build_modules(spec).items():
            path = provider_dir / "src" / relative_path
            if path.is_file() and path.read_text() == content:
                continue
            if args.check:
                print(f"{path} is out of date. Re-run {SCRIPT}", file=sys.stderr)
                status = 1
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
            written += 1
            print(f"{spec['provider']['id']}: wrote {path}", file=sys.stderr)
        if not args.check and not written:
            print(f"{spec['provider']['id']}: up to date", file=sys.stderr)
    return status


if __name__ == "__main__":
    sys.exit(main())
