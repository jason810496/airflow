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

import datetime
import enum
import json
import subprocess
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Literal, TypedDict
from unittest import mock

import pytest
from ci.lang_sdk_providers import generate_operator_specs as generator

STRING = {"kind": "string"}
NULL = {"kind": "null"}


class Color(str, enum.Enum):
    RED = "red"
    BLUE = "blue"


class Opaque(enum.Enum):
    ONE = object()


class User(TypedDict):
    id: str
    name: str


@pytest.mark.parametrize(
    ("annotation", "expected"),
    [
        (str, STRING),
        (bool, {"kind": "boolean"}),
        (int, {"kind": "integer"}),
        (float, {"kind": "number"}),
        (Any, {"kind": "any"}),
        (str | None, {"kind": "union", "options": [STRING, NULL]}),
        (list[str], {"kind": "array", "items": STRING}),
        (Sequence[int], {"kind": "array", "items": {"kind": "integer"}}),
        (tuple[str, ...], {"kind": "array", "items": STRING}),
        (tuple[str, int], {"kind": "tuple", "items": [STRING, {"kind": "integer"}]}),
        (dict, {"kind": "object", "values": {"kind": "any"}}),
        (Mapping[str, str], {"kind": "object", "values": STRING}),
        (Literal["a", "b"], {"kind": "literal", "literals": ["a", "b"]}),
        (Color, {"kind": "literal", "literals": ["red", "blue"]}),
        (datetime.timedelta | None, {"kind": "union", "options": [{"kind": "timedelta"}, NULL]}),
        (datetime.datetime, {"kind": "datetime"}),
        (
            User,
            {
                "kind": "struct",
                "fields": [
                    {"name": "id", "type": STRING, "required": True},
                    {"name": "name", "type": STRING, "required": True},
                ],
            },
        ),
        # The JSON form wins over the temporal one a union also allows.
        (datetime.timedelta | float, {"kind": "number"}),
        (datetime.datetime | str, STRING),
        # Members JSON cannot carry are dropped from a union.
        (str | Callable[[], str], STRING),
    ],
)
def test_describe_type_maps_json_serializable_annotations(annotation, expected):
    assert generator.describe_type(annotation) == expected


@pytest.mark.parametrize(
    "annotation",
    [
        Callable[[], str],
        bytes,
        Opaque,
        dict[int, str],
        list[Callable],
        # A duration inside a container has no per-argument encoding.
        list[datetime.timedelta],
        datetime.datetime | datetime.timedelta,
    ],
)
def test_describe_type_rejects_annotations_json_cannot_carry(annotation):
    assert generator.describe_type(annotation) is None


class Base:
    def __init__(self, *, task_id: str, retries: int = 0):
        pass

    def execute(self):
        raise NotImplementedError


class Sender(Base):
    """
    Send a message.

    A longer explanation that is not part of the summary.

    :param subject: The subject of the :class:`~some.module.Message`,
        which continues on a second line.
    :param options: What to choose from.
    """

    template_fields = ("subject",)
    ui_color = "#abc"

    def __init__(
        self,
        *,
        subject: str,
        options: list[str],
        timeout: datetime.timedelta | None = None,
        callback: Callable[[], None] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)


class FixedSender(Sender):
    inherits_from_skipmixin = True

    def __init__(self, *, urgent: bool = False, **kwargs):
        kwargs.setdefault("subject", "fixed")
        super().__init__(options=["yes", "no"], **kwargs)


class NeedsCallable(Base):
    def __init__(self, *, python_callable: Callable[[], None], **kwargs):
        super().__init__(**kwargs)


def test_collect_constructor_params_follows_kwargs_down_to_the_base():
    params = generator.collect_constructor_params(FixedSender, Base)

    names = {param.name: (param.default, owner) for param, owner in params}
    # ``options`` is fixed by the subclass, ``subject`` gets a default from it, and the base's
    # own arguments are left to the Lang SDK's task options.
    assert list(names) == ["urgent", "subject", "timeout", "callback"]
    assert names["subject"] == (None, Sender)


def test_build_operator_spec_describes_arguments_and_class_attributes():
    spec = generator.build_operator_spec(Sender, Base)

    assert spec == {
        "name": "Sender",
        "module": __name__,
        "kind": "operator",
        "description": "Send a message.",
        "template_fields": ["subject"],
        "ui_color": "#abc",
        "params": [
            {
                "name": "subject",
                "type": STRING,
                "required": True,
                "templated": True,
                "description": "The subject of the some.module.Message, which continues on a second line.",
            },
            {
                "name": "options",
                "type": {"kind": "array", "items": STRING},
                "required": True,
                "description": "What to choose from.",
            },
            {"name": "timeout", "type": {"kind": "union", "options": [{"kind": "timedelta"}, NULL]}},
        ],
        "omitted_params": [{"name": "callback", "annotation": "Callable[[], None] | None"}],
    }


def test_build_operator_spec_marks_skip_capable_operators():
    assert generator.build_operator_spec(FixedSender, Base)["can_skip_downstream"] is True


def test_build_operator_spec_skips_an_operator_requiring_a_non_json_argument():
    spec = generator.build_operator_spec(NeedsCallable, Base)

    assert spec == {
        "skipped": "required argument 'python_callable' (Callable[[], None]) is not JSON-serializable"
    }


def test_render_spec_writes_one_argument_per_line():
    spec = {
        "spec_version": "1.0",
        "provider": {"id": "demo"},
        "operators": [generator.build_operator_spec(Sender, Base)],
        "skipped": [{"module": "demo", "reason": "base class"}],
    }

    rendered = generator.render_spec(spec)

    assert json.loads(rendered) == spec
    assert '      {"name": "options", "type": {"kind": "array", "items": {"kind": "string"}}' in rendered


def test_parse_allow_list_splits_and_validates_provider_ids():
    assert generator.parse_allow_list(["standard,amazon", "common.ai"]) == ["amazon", "common.ai", "standard"]
    with pytest.raises(SystemExit, match="Unknown provider 'nope'"):
        generator.parse_allow_list(["nope"])


@mock.patch("ci.lang_sdk_providers.generate_operator_specs.subprocess.run", autospec=True)
def test_main_reads_each_provider_in_its_own_environment(mock_run, tmp_path):
    mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0)

    assert generator.main(["--allow-list", "common.ai", "--output-dir", str(tmp_path), "--check"]) == 0

    assert mock_run.call_args.args[0] == [
        "uv",
        "run",
        "--project",
        str(generator.REPO_ROOT / "providers" / "common" / "ai"),
        "python",
        generator.__file__,
        "--no-uv",
        "--allow-list",
        "common.ai",
        "--output-dir",
        str(tmp_path),
        "--check",
    ]


@mock.patch("ci.lang_sdk_providers.generate_operator_specs.build_provider_spec", autospec=True)
def test_main_check_fails_on_an_out_of_date_spec(mock_build, tmp_path):
    mock_build.return_value = {"provider": {"id": "standard"}, "operators": [], "skipped": []}
    args = ["--no-uv", "--allow-list", "standard", "--output-dir", str(tmp_path)]

    assert generator.main([*args, "--check"]) == 1
    assert generator.main(args) == 0
    assert generator.main([*args, "--check"]) == 0
