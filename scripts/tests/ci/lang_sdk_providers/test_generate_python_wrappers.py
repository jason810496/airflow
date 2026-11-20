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
from __future__ import annotations

import importlib.util
import json
import shutil
import subprocess
from typing import Any

import jsonschema
import pytest
from ci.lang_sdk_providers import generate_python_wrappers as generator

STRING = {"kind": "string"}
NULL = {"kind": "null"}
MODULE = "airflow.providers.ts.localfile.operators.file"
IMPLEMENTATION_MODULE = "@scope/providers-ts-localfile/operators/file"

SPEC: dict[str, Any] = {
    "spec_version": "1.0",
    "provider": {
        "id": "ts.localfile",
        "package": "apache-airflow-providers-ts-localfile",
        "version": "0.1.0",
    },
    "implemented_in": ["TypeScript"],
    "operators": [
        {
            "name": "WriteFileOperator",
            "module": MODULE,
            "implementation_module": IMPLEMENTATION_MODULE,
            "kind": "operator",
            "description": "Write a file. The file is replaced when it exists.",
            "template_fields": ["path", "content"],
            "ui_color": "#ffefeb",
            "params": [
                {
                    "name": "path",
                    "type": STRING,
                    "required": True,
                    "templated": True,
                    "description": "Where to write the file.",
                },
                {"name": "content", "type": STRING, "required": True, "templated": True},
                {
                    "name": "mode",
                    "type": {"kind": "literal", "literals": ["overwrite", "append"]},
                    "default": "overwrite",
                },
                {"name": "tags", "type": {"kind": "array", "items": STRING}, "default": ["a"]},
                {"name": "extra", "type": {"kind": "object", "values": {"kind": "any"}}},
                {"name": "limit", "type": {"kind": "union", "options": [{"kind": "integer"}, NULL]}},
                {"name": "ratio", "type": {"kind": "number"}, "default": 0.5},
                {"name": "flag", "type": {"kind": "boolean"}, "default": False},
            ],
        },
        {
            "name": "HasFileOperator",
            "module": MODULE,
            "implementation_module": IMPLEMENTATION_MODULE,
            "kind": "operator",
            "template_fields": ["path"],
            "params": [{"name": "path", "type": STRING, "required": True}],
        },
    ],
    "skipped": [],
}

WRITE_FILE_CLASS = '''\
class WriteFileOperator(LangSdkProviderOperator):
    """
    Write a file.

    The file is replaced when it exists.

    :param path: Where to write the file.
    """

    template_fields = ("path", "content")
    operator_params = ("path", "content", "mode", "tags", "extra", "limit", "ratio", "flag")
    implemented_in = ("TypeScript",)
    implementation_module = "@scope/providers-ts-localfile/operators/file"
    ui_color = "#ffefeb"

    def __init__(
        self,
        *,
        path: str,
        content: str,
        mode: Literal["overwrite", "append"] = "overwrite",
        tags: Sequence[str] | None = None,
        extra: Mapping[str, Any] | None = None,
        limit: int | None = None,
        ratio: float = 0.5,
        flag: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.path = path
        self.content = content
        self.mode = mode
        self.tags = ["a"] if tags is None else tags
        self.extra = extra
        self.limit = limit
        self.ratio = ratio
        self.flag = flag
'''


def write_spec(providers_dir, spec=SPEC, provider_path="ts/localfile"):
    spec_file = providers_dir / provider_path / "_generated" / generator.SPEC_FILE_NAME
    spec_file.parent.mkdir(parents=True)
    spec_file.write_text(json.dumps(spec))
    return providers_dir / provider_path / "src" / MODULE.replace(".", "/")


def test_build_modules_writes_one_module_per_spec_module():
    modules = generator.build_modules(SPEC)

    assert list(modules) == ["airflow/providers/ts/localfile/operators/file.py"]
    module = modules["airflow/providers/ts/localfile/operators/file.py"]
    assert module.startswith(generator.LICENSE)
    assert (
        "# Generated from providers/ts/localfile/_generated/lang_sdk_operators.json by\n"
        "# scripts/ci/lang_sdk_providers/generate_python_wrappers.py. Do not edit by hand.\n"
    ) in module
    assert (
        "from __future__ import annotations\n\n"
        "from collections.abc import Mapping, Sequence\n"
        "from typing import Any, Literal\n\n"
        "from airflow.sdk import LangSdkProviderOperator\n\n\n"
    ) in module
    assert f"\n\n\n{WRITE_FILE_CLASS}\n\nclass HasFileOperator(LangSdkProviderOperator):\n" in module
    assert module.endswith("        self.path = path\n")


def test_build_modules_imports_only_what_a_module_uses():
    spec = {**SPEC, "operators": SPEC["operators"][1:]}

    module = generator.build_modules(spec)["airflow/providers/ts/localfile/operators/file.py"]

    assert "from __future__ import annotations\n\nfrom airflow.sdk import LangSdkProviderOperator\n" in module
    assert "collections.abc" not in module
    assert "typing" not in module


def test_build_modules_splits_the_spec_by_module():
    operators = [SPEC["operators"][0], {**SPEC["operators"][1], "module": "airflow.providers.ts.localfile.x"}]

    modules = generator.build_modules({**SPEC, "operators": operators})

    assert list(modules) == [
        "airflow/providers/ts/localfile/operators/file.py",
        "airflow/providers/ts/localfile/x.py",
    ]


@pytest.mark.parametrize(
    ("type_spec", "expected", "used"),
    [
        (STRING, "str", set()),
        ({"kind": "boolean"}, "bool", set()),
        ({"kind": "integer"}, "int", set()),
        ({"kind": "number"}, "float", set()),
        (NULL, "None", set()),
        ({"kind": "any"}, "Any", {"Any"}),
        ({"kind": "literal", "literals": ["a", 1, True, None]}, 'Literal["a", 1, True, None]', {"Literal"}),
        ({"kind": "array", "items": STRING}, "Sequence[str]", {"Sequence"}),
        ({"kind": "object", "values": STRING}, "Mapping[str, str]", {"Mapping"}),
        ({"kind": "union", "options": [STRING, NULL]}, "str | None", set()),
        (
            {"kind": "array", "items": {"kind": "union", "options": [STRING, {"kind": "integer"}]}},
            "Sequence[str | int]",
            {"Sequence"},
        ),
        ({"kind": "union", "options": [{"kind": "integer"}, {"kind": "number"}]}, "int | float", set()),
    ],
)
def test_render_type_maps_the_spec_kinds(type_spec, expected, used):
    names: set[str] = set()

    assert generator.render_type(type_spec, used=names, where="Op.arg") == expected
    assert names == used


@pytest.mark.parametrize(
    "type_spec",
    [
        {"kind": "timedelta"},
        {"kind": "datetime"},
        {"kind": "tuple", "items": [STRING]},
        {"kind": "struct", "fields": []},
        {"kind": "array", "items": {"kind": "timedelta"}},
    ],
)
def test_render_type_rejects_a_kind_it_does_not_support(type_spec):
    with pytest.raises(
        generator.UnsupportedSpecError, match=r"Op\.arg: the type kind '\w+' is not supported"
    ):
        generator.render_type(type_spec, used=set(), where="Op.arg")


def test_build_modules_names_the_operator_of_an_unsupported_argument():
    operator = {**SPEC["operators"][1], "params": [{"name": "wait", "type": {"kind": "timedelta"}}]}

    with pytest.raises(
        generator.UnsupportedSpecError, match=r"HasFileOperator\.wait: the type kind 'timedelta'"
    ):
        generator.build_modules({**SPEC, "operators": [operator]})


@pytest.mark.parametrize(
    ("spec", "message"),
    [
        ({**SPEC, "spec_version": "2.0"}, "has version 2.0; this generator reads version 1.x"),
        (
            {**SPEC, "operators": [{**SPEC["operators"][1], "module": "airflow.providers.other.file"}]},
            "Module airflow.providers.other.file is not part of provider ts.localfile",
        ),
        (
            {**SPEC, "operators": [{**SPEC["operators"][1], "params": [{"name": "class", "type": STRING}]}]},
            "'class' is not a valid Python argument name",
        ),
        (
            {
                **SPEC,
                "operators": [
                    {k: v for k, v in SPEC["operators"][1].items() if k != "implementation_module"}
                ],
            },
            "HasFileOperator: an operator implemented in a Lang SDK needs an implementation_module",
        ),
    ],
)
def test_build_modules_rejects_a_spec_it_cannot_express(spec, message):
    with pytest.raises(generator.UnsupportedSpecError, match=message):
        generator.build_modules(spec)


@pytest.mark.parametrize(
    ("value", "expected"),
    [("a", '"a"'), ('say "hi"', "'say \"hi\"'"), ("it's", '"it\'s"'), ("a\nb", '"a\\nb"'), (True, "True")],
)
def test_render_value_quotes_the_way_ruff_does(value, expected):
    assert generator.render_value(value) == expected


def test_render_class_explodes_a_tuple_that_does_not_fit_one_line():
    operator = {**SPEC["operators"][1], "implemented_in": ["TypeScript"]}
    operator["params"] = [
        {"name": f"argument_number_{i}", "type": STRING, "required": True} for i in range(6)
    ]

    rendered = generator.render_class(operator, used=set())

    assert '    operator_params = (\n        "argument_number_0",\n' in rendered
    assert '        "argument_number_5",\n    )\n    implemented_in' in rendered


def test_main_writes_the_wrappers_and_check_finds_them_up_to_date(tmp_path):
    target = write_spec(tmp_path)
    args = ["--providers-dir", str(tmp_path)]

    assert generator.main([*args, "--check"]) == 1
    assert not target.with_suffix(".py").exists()
    assert generator.main(args) == 0
    assert (
        target.with_suffix(".py").read_text()
        == generator.build_modules(SPEC)["airflow/providers/ts/localfile/operators/file.py"]
    )
    assert generator.main([*args, "--check"]) == 0


def test_main_check_reports_a_wrapper_edited_by_hand(tmp_path, capsys):
    target = write_spec(tmp_path).with_suffix(".py")
    generator.main(["--providers-dir", str(tmp_path)])
    target.write_text(target.read_text() + "# edited\n")

    assert generator.main(["--providers-dir", str(tmp_path), "--check"]) == 1

    assert f"{target} is out of date" in capsys.readouterr().err
    assert target.read_text().endswith("# edited\n")


def test_main_leaves_a_provider_implemented_in_python_alone(tmp_path):
    spec = {key: value for key, value in SPEC.items() if key != "implemented_in"}
    write_spec(tmp_path, spec)

    assert generator.main(["--providers-dir", str(tmp_path)]) == 0
    assert not (tmp_path / "ts" / "localfile" / "src").exists()


def test_main_finds_a_provider_nested_under_a_namespace_directory(tmp_path):
    spec = {
        **SPEC,
        "provider": {**SPEC["provider"], "id": "common.files"},
        "operators": [{**SPEC["operators"][1], "module": "airflow.providers.common.files.file"}],
    }
    spec_file = tmp_path / "common" / "files" / "_generated" / generator.SPEC_FILE_NAME
    spec_file.parent.mkdir(parents=True)
    spec_file.write_text(json.dumps(spec))

    assert generator.main(["--providers-dir", str(tmp_path)]) == 0
    assert (
        tmp_path / "common" / "files" / "src" / "airflow" / "providers" / "common" / "files" / "file.py"
    ).is_file()


@pytest.mark.parametrize(
    "spec",
    [
        pytest.param({**SPEC, "implemented_in": []}, id="empty-implemented-in"),
        pytest.param({**SPEC, "implemented_in": ["TypeScript", "TypeScript"]}, id="duplicate-runtime"),
        pytest.param({**SPEC, "implemented_in": ["Rust"]}, id="unknown-runtime"),
        pytest.param({**SPEC, "operators": [{"name": "NoModuleOperator"}]}, id="operator-without-module"),
    ],
)
def test_main_rejects_a_spec_that_breaks_the_schema(tmp_path, spec):
    write_spec(tmp_path, spec)

    with pytest.raises(jsonschema.ValidationError):
        generator.main(["--providers-dir", str(tmp_path)])
    assert not (tmp_path / "ts" / "localfile" / "src").exists()


def test_the_spec_schema_takes_the_languages_of_the_sdks():
    for language in ("TypeScript", "Go", "Java"):
        generator.validate_spec({**SPEC, "implemented_in": [language]})
    generator.validate_spec({key: value for key, value in SPEC.items() if key != "implemented_in"})


@pytest.mark.skipif(shutil.which("ruff") is None, reason="ruff is not installed")
def test_generated_modules_follow_the_repo_ruff_config(tmp_path):
    target = write_spec(tmp_path).with_suffix(".py")
    generator.main(["--providers-dir", str(tmp_path)])
    config = ["--config", str(generator.REPO_ROOT / "pyproject.toml")]

    format_result = subprocess.run(
        ["ruff", "format", "--check", *config, str(target)], capture_output=True, text=True, check=False
    )
    check_result = subprocess.run(
        ["ruff", "check", *config, str(target)], capture_output=True, text=True, check=False
    )

    assert format_result.returncode == 0, format_result.stdout + format_result.stderr
    assert check_result.returncode == 0, check_result.stdout + check_result.stderr


def test_a_generated_wrapper_instantiates_and_serializes(tmp_path):
    from airflow.sdk import DAG, BaseOperator, XComArg
    from airflow.serialization.enums import Encoding
    from airflow.serialization.serialized_objects import DagSerialization

    target = write_spec(tmp_path).with_suffix(".py")
    generator.main(["--providers-dir", str(tmp_path)])
    # Loaded from its file: the generated package would have to join the installed airflow.providers.
    module_spec = importlib.util.spec_from_file_location(MODULE, target)
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)

    with DAG("wrapper_dag", schedule=None) as dag:
        build = BaseOperator(task_id="build")
        write = module.WriteFileOperator(
            task_id="write",
            path="/tmp/{{ run_id }}.txt",
            content=XComArg(build),
            queue="typescript",
        )
    tasks = {
        task[Encoding.VAR]["task_id"]: task[Encoding.VAR]
        for task in DagSerialization.to_dict(dag)["dag"]["tasks"]
    }

    assert write.ui_color == "#ffefeb"
    assert write.tags == ["a"]
    assert tasks["write"]["_task_module"] == MODULE
    assert tasks["write"]["task_type"] == "WriteFileOperator"
    assert tasks["write"]["queue"] == "typescript"
    assert tasks["write"]["is_stub"] is True
    assert tasks["write"]["_operator_module"] == IMPLEMENTATION_MODULE
    assert tasks["write"]["_operator_kwargs"] == [
        {"name": "path", "kind": "literal", "value": "/tmp/{{ run_id }}.txt"},
        {"name": "content", "kind": "xcom", "task_id": "build"},
        {"name": "mode", "kind": "literal", "value": "overwrite"},
        {"name": "tags", "kind": "literal", "value": ["a"]},
        {"name": "ratio", "kind": "literal", "value": 0.5},
        {"name": "flag", "kind": "literal", "value": False},
    ]
    assert tasks["build"]["downstream_task_ids"] == ["write"]


def test_committed_wrappers_are_what_the_committed_specs_generate(capsys):
    """Fails when a spec with ``implemented_in`` and its committed wrapper have drifted apart."""
    assert generator.main(["--check"]) == 0, capsys.readouterr().err


def test_every_committed_spec_with_implemented_in_has_a_wrapper():
    specs = [json.loads(path.read_text()) for path in generator.find_spec_files(generator.PROVIDERS_DIR)]
    implemented = [spec for spec in specs if "implemented_in" in spec]
    assert implemented, "no committed spec names implemented_in"
    for spec in implemented:
        provider_dir = generator.PROVIDERS_DIR.joinpath(*spec["provider"]["id"].split("."))
        for relative_path in generator.build_modules(spec):
            assert (provider_dir / "src" / relative_path).is_file()
