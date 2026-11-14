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
Generate the operator specs a language SDK builds its provider bindings from.

For each provider in ``--allow-list``, every operator and sensor its ``provider.yaml`` lists is
imported and its constructor read across the class hierarchy. The result is one JSON file per
provider, ``generated/lang_sdk_provider_operators/<provider-id>.json``, which names each operator's
Python class and the constructor arguments a language SDK can express as JSON. See
``airflow-core/adr/lang-sdk/0009-provider-operators-as-generated-dsl.md``.

Each provider is read in its own environment, so the host needs no provider installed::

    python3 scripts/ci/lang_sdk_providers/generate_operator_specs.py --allow-list amazon,common.ai,standard

``--no-uv`` reads the providers in the running interpreter instead, for an environment that already
has them, such as Breeze. ``--check`` writes nothing and fails when a spec is out of date.
"""

from __future__ import annotations

import argparse
import ast
import collections.abc
import datetime
import enum
import importlib
import inspect
import json
import re
import subprocess
import sys
import textwrap
import types
import typing
from pathlib import Path
from typing import Any

SPEC_VERSION = "1.0"
REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "generated" / "lang_sdk_provider_operators"

TypeSpec = dict[str, Any]

_PRIMITIVES: dict[Any, str] = {str: "string", bool: "boolean", int: "integer", float: "number"}
_ARRAY_ORIGINS = (
    list,
    set,
    frozenset,
    collections.abc.Sequence,
    collections.abc.MutableSequence,
    collections.abc.Iterable,
    collections.abc.Collection,
    collections.abc.Set,
)
_OBJECT_ORIGINS = (dict, collections.abc.Mapping, collections.abc.MutableMapping)
_TEMPORAL_KINDS = ("timedelta", "datetime")
_MAX_TYPE_DEPTH = 8


def describe_type(annotation: Any, *, _depth: int = 0) -> TypeSpec | None:
    """
    Return the language-neutral type of *annotation*, or ``None`` when JSON cannot carry it.

    A union keeps the members JSON can carry, so ``str | Callable`` is a string. A duration or a
    datetime is only kept at the top of a type: a Lang SDK encodes those per argument, not per
    element.
    """
    if _depth > _MAX_TYPE_DEPTH:
        return {"kind": "any"}
    if annotation is Any or annotation is object or annotation is inspect.Parameter.empty:
        return {"kind": "any"}
    if annotation is None or annotation is type(None):
        return {"kind": "null"}
    if annotation in _PRIMITIVES:
        return {"kind": _PRIMITIVES[annotation]}
    if isinstance(annotation, typing.NewType):
        return describe_type(annotation.__supertype__, _depth=_depth + 1)

    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)
    if origin is typing.Annotated:
        return describe_type(args[0], _depth=_depth + 1)
    if origin is typing.Union or origin is types.UnionType:
        return _describe_union(args, _depth)
    if origin is typing.Literal:
        if all(isinstance(value, (str, int, bool)) or value is None for value in args):
            return {"kind": "literal", "literals": list(args)}
        return None
    if origin is tuple:
        if len(args) == 2 and args[1] is Ellipsis:
            return _describe_array(args[:1], _depth)
        items = [_describe_nested(arg, _depth) for arg in args]
        if not items or any(item is None for item in items):
            return None
        return {"kind": "tuple", "items": items}
    if origin in _ARRAY_ORIGINS:
        return _describe_array(args, _depth)
    if origin in _OBJECT_ORIGINS:
        return _describe_object(args, _depth)
    if annotation in _ARRAY_ORIGINS or annotation is tuple:
        return {"kind": "array", "items": {"kind": "any"}}
    if annotation in _OBJECT_ORIGINS:
        return {"kind": "object", "values": {"kind": "any"}}

    if _is_typed_dict(annotation):
        return _describe_typed_dict(annotation, _depth)
    if isinstance(annotation, type):
        if issubclass(annotation, datetime.datetime):
            return {"kind": "datetime"}
        if issubclass(annotation, datetime.timedelta):
            return {"kind": "timedelta"}
        # Only an enum whose members are their values can be given as that value.
        if issubclass(annotation, enum.Enum) and issubclass(annotation, (str, int)):
            return {"kind": "literal", "literals": [member.value for member in annotation]}
    return None


def _is_typed_dict(annotation: Any) -> bool:
    # ``typing.is_typeddict`` does not recognize a ``typing_extensions.TypedDict``.
    return (
        isinstance(annotation, type)
        and issubclass(annotation, dict)
        and hasattr(annotation, "__required_keys__")
    )


def _describe_nested(annotation: Any, depth: int) -> TypeSpec | None:
    spec = describe_type(annotation, _depth=depth + 1)
    return None if spec is None or _has_temporal(spec) else spec


def _describe_array(args: tuple[Any, ...], depth: int) -> TypeSpec | None:
    items = _describe_nested(args[0], depth) if args else {"kind": "any"}
    return None if items is None else {"kind": "array", "items": items}


def _describe_object(args: tuple[Any, ...], depth: int) -> TypeSpec | None:
    if not args:
        return {"kind": "object", "values": {"kind": "any"}}
    if args[0] is not str and args[0] is not Any:
        return None
    values = _describe_nested(args[1], depth)
    return None if values is None else {"kind": "object", "values": values}


def _describe_typed_dict(annotation: Any, depth: int) -> TypeSpec | None:
    try:
        hints = typing.get_type_hints(annotation)
    except Exception:
        return {"kind": "object", "values": {"kind": "any"}}
    fields = []
    for name, hint in hints.items():
        if (spec := _describe_nested(hint, depth)) is None:
            return {"kind": "object", "values": {"kind": "any"}}
        fields.append({"name": name, "type": spec, "required": name in annotation.__required_keys__})
    return {"kind": "struct", "fields": fields}


def _describe_union(args: tuple[Any, ...], depth: int) -> TypeSpec | None:
    options: list[TypeSpec] = []
    for arg in args:
        spec = describe_type(arg, _depth=depth + 1)
        if spec is None:
            continue
        for option in spec["options"] if spec["kind"] == "union" else [spec]:
            if option not in options:
                options.append(option)
    kinds = {option["kind"] for option in options}
    # A number already carries a duration and a string a datetime, so the plain JSON form wins.
    if kinds & {"number", "integer"}:
        options = [option for option in options if option["kind"] != "timedelta"]
    if "string" in kinds:
        options = [option for option in options if option["kind"] != "datetime"]
    if len([option for option in options if option["kind"] in _TEMPORAL_KINDS]) > 1:
        return None
    if any(option["kind"] == "any" for option in options):
        return {"kind": "any"}
    if not options:
        return None
    if len(options) == 1:
        return options[0]
    return {"kind": "union", "options": options}


def _has_temporal(spec: TypeSpec) -> bool:
    if spec["kind"] in _TEMPORAL_KINDS:
        return True
    nested: list[TypeSpec] = [*spec.get("options", []), *(field["type"] for field in spec.get("fields", []))]
    if spec["kind"] == "tuple":
        nested += spec["items"]
    elif spec["kind"] == "array":
        nested.append(spec["items"])
    elif spec["kind"] == "object":
        nested.append(spec["values"])
    return any(_has_temporal(item) for item in nested)


def build_annotation_namespace(module: types.ModuleType) -> dict[str, Any]:
    """
    Return the names an annotation of *module* may use, including its ``TYPE_CHECKING`` imports.

    Provider modules postpone annotations and import many of their types only for type checkers, so
    those imports are run here. One that fails leaves its names out, which marks the arguments that
    use them as not expressible.
    """
    namespace: dict[str, Any] = {**vars(typing), **vars(module)}
    try:
        tree = ast.parse(inspect.getsource(module))
    except (OSError, TypeError, SyntaxError):
        return namespace
    for node in tree.body:
        if not isinstance(node, ast.If) or "TYPE_CHECKING" not in ast.unparse(node.test):
            continue
        for statement in node.body:
            if isinstance(statement, (ast.Import, ast.ImportFrom)):
                code = compile(ast.Module([statement], []), getattr(module, "__file__", "<module>"), "exec")
                try:
                    exec(code, namespace)
                except Exception:
                    continue
    return namespace


def resolve_annotation(annotation: Any, namespace: dict[str, Any]) -> Any:
    """Evaluate a postponed *annotation*; return ``NotImplemented`` when it cannot be resolved."""
    if not isinstance(annotation, str):
        return annotation
    try:
        return eval(annotation, namespace)
    except Exception:
        return NotImplemented


def read_super_init_overrides(klass: type) -> tuple[set[str], set[str]]:
    """
    Return the inherited arguments *klass* fixes, and the ones it supplies a default for.

    ``super().__init__(options=[...], **kwargs)`` fixes ``options``: passing it again raises. An
    assignment into ``kwargs`` before that call gives an inherited required argument a default.
    """
    try:
        tree = ast.parse(textwrap.dedent(inspect.getsource(klass.__init__)))
    except (OSError, TypeError, SyntaxError):
        return set(), set()
    own = set(inspect.signature(klass.__init__).parameters)
    fixed: set[str] = set()
    defaulted: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            target = ast.unparse(node.func)
            if target == "super().__init__":
                fixed.update(kw.arg for kw in node.keywords if kw.arg and kw.arg not in own)
            elif target == "kwargs.setdefault" and node.args and isinstance(node.args[0], ast.Constant):
                defaulted.add(str(node.args[0].value))
        elif isinstance(node, ast.Subscript) and isinstance(node.ctx, ast.Store):
            if ast.unparse(node.value) == "kwargs" and isinstance(node.slice, ast.Constant):
                defaulted.add(str(node.slice.value))
    return fixed, defaulted


def collect_constructor_params(cls: type, base: type) -> list[tuple[inspect.Parameter, type]]:
    """
    Return the keyword arguments *cls* takes, with the class that declares each, down to *base*.

    A constructor that forwards ``**kwargs`` passes them to the next class, so the walk continues
    there. The arguments of *base* and of the classes it inherits from are left out.
    """
    params: dict[str, tuple[inspect.Parameter, type]] = {}
    fixed: set[str] = set()
    defaulted: set[str] = set()
    for klass in cls.__mro__:
        # *base* takes no arguments for the classes after it, so the chain of ``**kwargs`` ends there.
        if klass is base:
            break
        if "__init__" not in vars(klass):
            continue
        signature = inspect.signature(klass.__init__)
        for name, param in list(signature.parameters.items())[1:]:
            if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD) or name in params or name in fixed:
                continue
            given_default = name in defaulted and param.default is param.empty
            params[name] = (param.replace(default=None) if given_default else param, klass)
        class_fixed, class_defaulted = read_super_init_overrides(klass)
        fixed |= class_fixed
        defaulted |= class_defaulted
        if not any(p.kind is p.VAR_KEYWORD for p in signature.parameters.values()):
            break
    return list(params.values())


_ROLE = re.compile(r":[\w:.+-]+:`~?([^`<]+?)(?:\s*<[^`>]*>)?`")
_PARAM = re.compile(r"^:param\s+(?:[^:]+\s+)?(\w+):\s*(.*)$")


def clean_doc(text: str) -> str:
    """Turn a reStructuredText fragment into one line of plain text."""
    text = _ROLE.sub(r"\1", text).replace("``", "`")
    return " ".join(text.split())


def read_class_docs(cls: type, base: type) -> tuple[str, dict[str, str]]:
    """Return the first paragraph of the docstring of *cls* and its ``:param:`` descriptions."""
    summary = ""
    params: dict[str, str] = {}
    for klass in cls.__mro__:
        if klass in base.__mro__:
            continue
        doc = inspect.cleandoc(vars(klass).get("__doc__") or "")
        init_doc = inspect.cleandoc(getattr(vars(klass).get("__init__"), "__doc__", None) or "")
        if not summary and klass is cls:
            summary = clean_doc(doc.split("\n\n", 1)[0])
        current: str | None = None
        for line in f"{doc}\n{init_doc}".splitlines():
            if match := _PARAM.match(line.strip()):
                # A subclass documents an argument it redeclares, so the first description wins.
                current = match.group(1) if match.group(1) not in params else None
                if current:
                    params[current] = match.group(2)
            elif current and line.startswith((" ", "\t")) and line.strip():
                params[current] += " " + line.strip()
            else:
                current = None
    return summary, {name: clean_doc(text) for name, text in params.items()}


def _to_json_default(value: Any) -> tuple[bool, Any]:
    if value is None or isinstance(value, (str, bool, int, float)):
        return True, value
    if isinstance(value, (list, tuple)) and all(_to_json_default(item)[0] for item in value):
        return True, list(value)
    return False, None


def build_operator_spec(cls: type, base: type) -> dict[str, Any]:
    """
    Return the spec of one operator class, or ``{"skipped": reason}`` when it cannot be expressed.

    An optional argument JSON cannot carry is listed under ``omitted_params`` and left to its
    default. A required one makes the whole operator unusable from a Lang SDK.
    """
    summary, param_docs = read_class_docs(cls, base)
    template_fields = list(getattr(cls, "template_fields", ()))
    namespaces: dict[type, dict[str, Any]] = {}
    params: list[dict[str, Any]] = []
    omitted: list[dict[str, str]] = []
    for param, owner in collect_constructor_params(cls, base):
        if owner not in namespaces:
            namespaces[owner] = build_annotation_namespace(sys.modules[owner.__module__])
        annotation = resolve_annotation(param.annotation, namespaces[owner])
        spec = None if annotation is NotImplemented else describe_type(annotation)
        required = param.default is param.empty
        # An argument that could only be given ``null`` has nothing left to set.
        if spec is None or spec == {"kind": "null"}:
            if required:
                return {
                    "skipped": f"required argument {param.name!r} ({param.annotation}) is not "
                    "JSON-serializable"
                }
            omitted.append({"name": param.name, "annotation": str(param.annotation)})
            continue
        entry: dict[str, Any] = {"name": param.name, "type": spec}
        if required:
            entry["required"] = True
        if param.name in template_fields:
            entry["templated"] = True
        if not required:
            is_json, default = _to_json_default(param.default)
            if is_json and default is not None:
                entry["default"] = default
        if doc := param_docs.get(param.name):
            entry["description"] = doc
        params.append(entry)

    spec_out: dict[str, Any] = {
        "name": cls.__name__,
        "module": cls.__module__,
        "kind": "sensor" if getattr(cls, "_is_sensor", False) is True else "operator",
    }
    if summary:
        spec_out["description"] = summary
    spec_out["template_fields"] = template_fields
    if template_ext := list(getattr(cls, "template_ext", ())):
        spec_out["template_ext"] = template_ext
    if renderers := dict(getattr(cls, "template_fields_renderers", {})):
        spec_out["template_fields_renderers"] = renderers
    if (ui_color := getattr(cls, "ui_color", "#fff")) != "#fff":
        spec_out["ui_color"] = ui_color
    if (ui_fgcolor := getattr(cls, "ui_fgcolor", "#000")) != "#000":
        spec_out["ui_fgcolor"] = ui_fgcolor
    if (custom_name := getattr(cls, "custom_operator_name", None)) and isinstance(custom_name, str):
        spec_out["operator_name"] = custom_name
    # The same class attribute Dag serialization reads; on other operators it is a property.
    if getattr(cls, "inherits_from_skipmixin", False) is True:
        spec_out["can_skip_downstream"] = True
    spec_out["params"] = params
    if omitted:
        spec_out["omitted_params"] = omitted
    return spec_out


def build_provider_spec(provider_id: str) -> dict[str, Any]:
    """Import every operator and sensor module of the provider *provider_id* and describe its classes."""
    import yaml

    from airflow.sdk.bases.operator import BaseOperator
    from airflow.sdk.bases.sensor import BaseSensorOperator

    provider_yaml = REPO_ROOT / "providers" / provider_id.replace(".", "/") / "provider.yaml"
    info = yaml.safe_load(provider_yaml.read_text())
    modules = sorted(
        {
            module
            for section in ("operators", "sensors")
            for entry in info.get(section, [])
            for module in entry.get("python-modules", [])
        }
    )
    operators: list[dict[str, Any]] = []
    skipped: list[dict[str, str]] = []
    for module_name in modules:
        try:
            module = importlib.import_module(module_name)
        except Exception as exc:
            skipped.append({"module": module_name, "reason": f"cannot be imported: {type(exc).__name__}"})
            continue
        for name, cls in sorted(vars(module).items()):
            if (
                not inspect.isclass(cls)
                or cls.__module__ != module_name
                or name.startswith("_")
                or not issubclass(cls, BaseOperator)
            ):
                continue
            if not _implements_execution(cls, BaseOperator, BaseSensorOperator):
                skipped.append({"module": module_name, "name": name, "reason": "base class"})
                continue
            spec = build_operator_spec(cls, BaseOperator)
            if reason := spec.get("skipped"):
                skipped.append({"module": module_name, "name": name, "reason": reason})
            else:
                operators.append(spec)
    return {
        "spec_version": SPEC_VERSION,
        "provider": {
            "id": provider_id,
            "package": info["package-name"],
            "version": info["versions"][0],
        },
        "operators": operators,
        "skipped": skipped,
    }


def _implements_execution(cls: type, base: type, sensor_base: type) -> bool:
    if inspect.isabstract(cls):
        return False
    if issubclass(cls, sensor_base):
        return cls.poke is not sensor_base.poke or cls.execute is not sensor_base.execute  # type: ignore[attr-defined]
    return cls.execute is not base.execute  # type: ignore[attr-defined]


def render_spec(spec: dict[str, Any]) -> str:
    """Render *spec* with one constructor argument per line, so a changed argument is a one-line diff."""
    placeholders: dict[str, str] = {}

    def inline(value: Any) -> str:
        key = f"@@{len(placeholders)}@@"
        placeholders[json.dumps(key)] = json.dumps(value, ensure_ascii=False)
        return key

    shaped = {
        **spec,
        "operators": [
            {
                **operator,
                "params": [inline(param) for param in operator["params"]],
                **(
                    {"omitted_params": [inline(param) for param in operator["omitted_params"]]}
                    if "omitted_params" in operator
                    else {}
                ),
            }
            for operator in spec["operators"]
        ],
        "skipped": [inline(entry) for entry in spec["skipped"]],
    }
    rendered = json.dumps(shaped, indent=2, ensure_ascii=False)
    for key, value in placeholders.items():
        rendered = rendered.replace(key, value)
    return rendered + "\n"


def get_spec_path(output_dir: Path, provider_id: str) -> Path:
    return output_dir / f"{provider_id}.json"


def run_in_provider_environment(provider_id: str, output_dir: Path, *, check: bool) -> int:
    """Run this script for *provider_id* in the environment of its own distribution."""
    project = REPO_ROOT / "providers" / provider_id.replace(".", "/")
    command = ["uv", "run", "--project", str(project), "python", __file__, "--no-uv"]
    command += ["--allow-list", provider_id, "--output-dir", str(output_dir)]
    if check:
        command.append("--check")
    return subprocess.run(command, check=False, cwd=REPO_ROOT).returncode


def parse_allow_list(values: list[str]) -> list[str]:
    providers = [item.strip() for value in values for item in value.split(",") if item.strip()]
    for provider_id in providers:
        if not (REPO_ROOT / "providers" / provider_id.replace(".", "/") / "provider.yaml").is_file():
            raise SystemExit(f"Unknown provider {provider_id!r}: no providers/.../provider.yaml for it")
    return sorted(set(providers))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument(
        "--allow-list",
        action="append",
        required=True,
        metavar="PROVIDER[,PROVIDER...]",
        help="Provider ids to generate specs for, such as amazon,common.ai. May be repeated.",
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--no-uv", action="store_true", help="Read the providers in this interpreter.")
    parser.add_argument("--check", action="store_true", help="Fail when a spec file is out of date.")
    args = parser.parse_args(argv)

    providers = parse_allow_list(args.allow_list)
    if not args.no_uv:
        results = [
            run_in_provider_environment(provider_id, args.output_dir, check=args.check)
            for provider_id in providers
        ]
        return max(results, default=0)

    script = Path(__file__).relative_to(REPO_ROOT)
    status = 0
    for provider_id in providers:
        rendered = render_spec(spec := build_provider_spec(provider_id))
        path = get_spec_path(args.output_dir, provider_id)
        if args.check:
            if not path.is_file() or path.read_text() != rendered:
                print(
                    f"{path} is out of date. Re-run {script} --allow-list {provider_id}",
                    file=sys.stderr,
                )
                status = 1
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered)
        print(
            f"{provider_id}: {len(spec['operators'])} operators, {len(spec['skipped'])} skipped -> {path}",
            file=sys.stderr,
        )
    return status


if __name__ == "__main__":
    sys.exit(main())
