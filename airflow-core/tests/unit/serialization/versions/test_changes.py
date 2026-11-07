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

import copy
from typing import Any

import pytest

from airflow.serialization import versions
from airflow.serialization.versions.v2 import ConvertAirflow2Dag
from airflow.serialization.versions.v3 import (
    AddClientDefaults,
    AddDagDeadline,
    ExcludeDagSchemaDefaults,
    RenamePythonCallableToName,
    ReplaceTaskCallbacksWithFlags,
)
from airflow.serialization.versions.v2025_10_27 import OmitDefaultRetryDelay
from airflow.serialization.versions.v2025_11_05 import ReplaceDefaultArgsCallbacksWithFlags
from airflow.serialization.versions.v2026_04_06 import (
    AddAllowedRunTypes,
    AddParamSource,
    AddPartitionedAssetTimetable,
    AddTaskRenderTemplateAsNativeObj,
    AllowRetryBackoffMultiplier,
    AlwaysWriteDagLimits,
    DropEventsTimetableSummary,
    EncodeMappedArgument,
)
from airflow.serialization.versions.v2026_06_30 import (
    AddRerunWithLatestVersion,
    AddTaskGroupDocMd,
    ReplaceRetryPolicyWithFlag,
)
from airflow.serialization.versions.v2026_10_30 import (
    AddBundleName,
    AddReturnsDagResult,
    AddStubTaskBindings,
    AddTaskGroupLoop,
)

PARTITIONED = "airflow.timetables.simple.PartitionedAssetTimetable"
ASSET_TRIGGERED = "airflow.timetables.simple.AssetTriggeredTimetable"
EVENTS = "airflow.timetables.events.EventsTimetable"
STUB_MODULE = "airflow.providers.standard.decorators.stub"
ASSET_CONDITION = {"__type": "asset", "name": "a", "uri": "s3://a", "group": "asset", "extra": {}}
MAPPED_ARGUMENT = {
    "__type": "mapped_argument",
    "__var": {"input": {"type": "list-of-dicts", "value": [{"a": 1}]}, "key": "a"},
}


def _param(default: Any, source: str | None = None) -> dict[str, Any]:
    param = {
        "__class": "airflow.sdk.definitions.param.Param",
        "default": default,
        "description": None,
        "schema": {"__type": "dict", "__var": {}},
    }
    if source is not None:
        param["source"] = source
    return param


def _build_head_blob() -> dict[str, Any]:
    """A blob using every feature added since 3.0, as the current serializer writes it."""
    return {
        "__version": "2027-02-28",
        "client_defaults": {"tasks": {"retries": 2, "retry_exponential_backoff": 3.0}},
        "dag": {
            "dag_id": "kitchen_sink",
            "fileloc": "/dags/kitchen_sink.py",
            "bundle_name": "dags-folder",
            "catchup": True,
            "max_active_tasks": 4,
            "max_active_runs": 1,
            "max_consecutive_failed_dag_runs": 3,
            "disable_bundle_versioning": False,
            "allowed_run_types": ["manual"],
            "rerun_with_latest_version": True,
            "deadline": [{"__type": "deadline_alert", "__var": {"interval": 60.0}}],
            "params": [["p", _param(1, source="dag")]],
            "default_args": {
                "__type": "dict",
                "__var": {"retries": 1, "has_on_failure_callback": True, "has_retry_policy": True},
            },
            "timetable": {
                "__type": PARTITIONED,
                "__var": {
                    "asset_condition": ASSET_CONDITION,
                    "batch_asset_events": False,
                    "default_partition_mapper": {"__type": "identity", "__var": {}},
                    "partition_mapper_config": [],
                },
            },
            "task_group": {
                "_group_id": None,
                "children": {
                    "stub": ["operator", "stub"],
                    "loop": [
                        "taskgroup",
                        {
                            "_group_id": "loop",
                            "doc_md": "Loops",
                            "loop": {
                                "max_iterations": 3,
                                "has_until": False,
                                "terminal_task_id": "loop.end",
                                "gate_task_id": "loop.gate",
                            },
                            "children": {"mapped": ["operator", "mapped"]},
                        },
                    ],
                },
            },
            "tasks": [
                {
                    "__type": "operator",
                    "__var": {
                        "task_id": "stub",
                        "task_type": "_StubOperator",
                        "_task_module": STUB_MODULE,
                        "is_stub": True,
                        "_arg_bindings": [{"name": "x", "kind": "literal", "value": 1}],
                        "returns_dag_result": True,
                        "render_template_as_native_obj": True,
                        "retry_exponential_backoff": 1.5,
                        "has_on_failure_callback": True,
                        "params": [["tp", _param("v", source="task")]],
                    },
                },
                {
                    "__type": "operator",
                    "__var": {
                        "task_id": "mapped",
                        "task_type": "PythonOperator",
                        "_task_module": "airflow.providers.standard.operators.python",
                        "_is_mapped": True,
                        "expand_input": {
                            "type": "dict-of-lists",
                            "value": {"__type": "dict", "__var": {"x": MAPPED_ARGUMENT}},
                        },
                        "partial_kwargs": {
                            "has_on_execute_callback": True,
                            "has_retry_policy": True,
                            "python_callable_name": "do_it",
                            "retry_exponential_backoff": 2,
                            "op_kwargs": {"__type": "dict", "__var": {"y": MAPPED_ARGUMENT}},
                        },
                    },
                },
            ],
        },
    }


def _build_events_blob() -> dict[str, Any]:
    return {
        "__version": "2027-02-28",
        "dag": {
            "dag_id": "events",
            "fileloc": "/dags/events.py",
            "tasks": [],
            "timetable": {
                "__type": EVENTS,
                "__var": {
                    "event_dates": ["2026-01-01T00:00:00+00:00", "2026-02-01T00:00:00+00:00"],
                    "restrict_to_events": False,
                    "description": None,
                },
            },
        },
    }


def _build_v3_blob() -> dict[str, Any]:
    """A blob as Airflow 3.1.0 wrote it, before any later change."""
    return {
        "__version": 3,
        "dag": {
            "dag_id": "v3",
            "fileloc": "/dags/v3.py",
            "params": [["p", _param(1)]],
            "default_args": {
                "__type": "dict",
                "__var": {"on_failure_callback": "<function cb>", "on_retry_callback": None},
            },
            "tasks": [
                {
                    "__type": "operator",
                    "__var": {
                        "task_id": "stub",
                        "task_type": "_StubOperator",
                        "_task_module": STUB_MODULE,
                        "params": [["tp", _param("v")]],
                    },
                },
                {
                    "__type": "operator",
                    "__var": {"task_id": "plain", "task_type": "BashOperator", "_task_module": "m"},
                },
            ],
        },
    }


ALL_CHANGES = [
    change
    for version in versions.VERSIONS
    for change in version.changes
    if change is not ConvertAirflow2Dag and not change.requires_migration
]
BLOBS = [
    pytest.param(_build_head_blob, id="head"),
    pytest.param(_build_events_blob, id="events"),
    pytest.param(_build_v3_blob, id="v3"),
]


@pytest.mark.parametrize("build_blob", BLOBS)
@pytest.mark.parametrize("change", ALL_CHANGES, ids=lambda c: c.__name__)
class TestSafeToApplyTwice:
    def test_upgrade(self, change, build_blob):
        once = build_blob()
        change().upgrade(once)
        twice = copy.deepcopy(once)
        change().upgrade(twice)

        assert twice == once

    def test_downgrade(self, change, build_blob):
        once = build_blob()
        change().downgrade(once)
        twice = copy.deepcopy(once)
        change().downgrade(twice)

        assert twice == once


def test_every_change_has_a_description():
    for version in versions.VERSIONS:
        for change in version.changes:
            assert change.description, change.__name__


def test_convert_airflow_2_dag_refuses_downgrade():
    with pytest.raises(NotImplementedError):
        ConvertAirflow2Dag().downgrade({"__version": 2, "dag": {}})


class TestAddClientDefaults:
    def test_downgrade_copies_defaults_into_tasks_and_partial_kwargs(self):
        data = {
            "__version": 3,
            "client_defaults": {"tasks": {"retries": 2, "retry_delay": 60.0, "pool": "p"}},
            "dag": {
                "tasks": [
                    {"__type": "operator", "__var": {"task_id": "a", "pool": "own"}},
                    {
                        "__type": "operator",
                        "__var": {"task_id": "m", "_is_mapped": True, "partial_kwargs": {"retries": 5}},
                    },
                ]
            },
        }

        AddClientDefaults().downgrade(data)

        assert data == {
            "__version": 3,
            "dag": {
                "tasks": [
                    {
                        "__type": "operator",
                        "__var": {"task_id": "a", "pool": "own", "retries": 2, "retry_delay": 60.0},
                    },
                    {
                        "__type": "operator",
                        "__var": {
                            "task_id": "m",
                            "_is_mapped": True,
                            "partial_kwargs": {
                                "retries": 5,
                                "retry_delay": {"__type": "timedelta", "__var": 60.0},
                                "pool": "p",
                            },
                        },
                    },
                ]
            },
        }

    def test_downgrade_drops_empty_client_defaults(self):
        data = {"__version": 3, "client_defaults": {}, "dag": {"tasks": []}}

        AddClientDefaults().downgrade(data)

        assert data == {"__version": 3, "dag": {"tasks": []}}


def test_replace_task_callbacks_with_flags_downgrade():
    data = _build_head_blob()

    ReplaceTaskCallbacksWithFlags().downgrade(data)

    stub, mapped = (task["__var"] for task in data["dag"]["tasks"])
    assert stub["on_failure_callback"] is True
    assert "has_on_failure_callback" not in stub
    assert mapped["partial_kwargs"]["on_execute_callback"] is True
    assert "has_on_execute_callback" not in mapped["partial_kwargs"]


def test_exclude_dag_schema_defaults_downgrade_writes_defaults_but_keeps_values():
    data = {"__version": 3, "dag": {"dag_id": "d", "catchup": True}}

    ExcludeDagSchemaDefaults().downgrade(data)

    assert data["dag"] == {
        "dag_id": "d",
        "catchup": True,
        "fail_fast": False,
        "render_template_as_native_obj": False,
        "disable_bundle_versioning": False,
        "max_active_tasks": 16,
        "max_active_runs": 16,
        "max_consecutive_failed_dag_runs": 0,
    }
    # Airflow 3.0 reads a has_on_*_callback key's presence as true.
    assert "has_on_success_callback" not in data["dag"]


def test_rename_python_callable_to_name_downgrade():
    data = _build_head_blob()

    RenamePythonCallableToName().downgrade(data)

    partial_kwargs = data["dag"]["tasks"][1]["__var"]["partial_kwargs"]
    assert partial_kwargs["python_callable"] == "do_it"
    assert "python_callable_name" not in partial_kwargs


@pytest.mark.parametrize(
    ("client_defaults", "expected"),
    [
        pytest.param(None, {"tasks": {"retry_delay": 300.0}}, id="no-client-defaults"),
        pytest.param({}, {"tasks": {"retry_delay": 300.0}}, id="empty-client-defaults"),
        pytest.param(
            {"tasks": {"retries": 1}}, {"tasks": {"retries": 1, "retry_delay": 300.0}}, id="other-defaults"
        ),
        pytest.param({"tasks": {"retry_delay": 60.0}}, {"tasks": {"retry_delay": 60.0}}, id="already-set"),
    ],
)
def test_omit_default_retry_delay_downgrade(client_defaults, expected):
    data: dict[str, Any] = {"__version": 3, "dag": {"tasks": []}}
    if client_defaults is not None:
        data["client_defaults"] = client_defaults

    OmitDefaultRetryDelay().downgrade(data)

    assert data["client_defaults"] == expected


class TestReplaceDefaultArgsCallbacksWithFlags:
    def test_upgrade_turns_repr_into_flag(self):
        data = _build_v3_blob()

        ReplaceDefaultArgsCallbacksWithFlags().upgrade(data)

        assert data["dag"]["default_args"]["__var"] == {"has_on_failure_callback": True}

    def test_downgrade_drops_flags(self):
        data = _build_head_blob()

        ReplaceDefaultArgsCallbacksWithFlags().downgrade(data)

        assert data["dag"]["default_args"]["__var"] == {"retries": 1, "has_retry_policy": True}


def test_allow_retry_backoff_multiplier_downgrade():
    data = _build_head_blob()

    AllowRetryBackoffMultiplier().downgrade(data)

    stub, mapped = (task["__var"] for task in data["dag"]["tasks"])
    assert stub["retry_exponential_backoff"] is True
    assert mapped["partial_kwargs"]["retry_exponential_backoff"] is True
    assert data["client_defaults"]["tasks"]["retry_exponential_backoff"] is True


@pytest.mark.parametrize(
    ("value", "expected", "lossy"),
    [(0, False, False), (2.0, True, False), (1.5, True, True), (True, True, False)],
)
def test_allow_retry_backoff_multiplier_downgrade_warns_only_when_lossy(value, expected, lossy, caplog):
    data = {
        "__version": 3,
        "dag": {"tasks": [{"__type": "operator", "__var": {"retry_exponential_backoff": value}}]},
    }

    AllowRetryBackoffMultiplier().downgrade(data)

    assert data["dag"]["tasks"][0]["__var"]["retry_exponential_backoff"] is expected
    assert ("Serialized Dag downgrade dropped data" in caplog) is lossy


class TestAddParamSource:
    def test_upgrade_marks_only_dag_params(self):
        data = _build_v3_blob()

        AddParamSource().upgrade(data)

        assert data["dag"]["params"][0][1]["source"] == "dag"
        assert "source" not in data["dag"]["tasks"][0]["__var"]["params"][0][1]

    def test_downgrade_drops_source_everywhere(self):
        data = _build_head_blob()

        AddParamSource().downgrade(data)

        assert "source" not in data["dag"]["params"][0][1]
        assert "source" not in data["dag"]["tasks"][0]["__var"]["params"][0][1]


def test_add_partitioned_asset_timetable_downgrade():
    data = _build_head_blob()

    AddPartitionedAssetTimetable().downgrade(data)

    assert data["dag"]["timetable"] == {
        "__type": ASSET_TRIGGERED,
        "__var": {"asset_condition": ASSET_CONDITION, "batch_asset_events": False},
    }


@pytest.mark.parametrize(
    ("description", "expected"),
    [("Payday", "Payday"), (None, "2 events")],
)
def test_drop_events_timetable_summary_downgrade(description, expected):
    data = _build_events_blob()
    data["dag"]["timetable"]["__var"]["description"] = description

    DropEventsTimetableSummary().downgrade(data)

    assert data["dag"]["timetable"]["__var"]["_summary"] == expected


def test_drop_events_timetable_summary_downgrade_keeps_existing_summary():
    data = _build_events_blob()
    data["dag"]["timetable"]["__var"]["_summary"] = "kept"

    DropEventsTimetableSummary().downgrade(data)

    assert data["dag"]["timetable"]["__var"]["_summary"] == "kept"


def test_encode_mapped_argument_downgrade():
    data = _build_head_blob()

    EncodeMappedArgument().downgrade(data)

    mapped = data["dag"]["tasks"][1]["__var"]
    expected = "MappedArgument(_input=<list-of-dicts>, _key='a')"
    assert mapped["expand_input"]["value"]["__var"]["x"] == expected
    assert mapped["partial_kwargs"]["op_kwargs"]["__var"]["y"] == expected


class TestAlwaysWriteDagLimits:
    def test_upgrade_fills_old_defaults(self):
        data = _build_v3_blob()

        AlwaysWriteDagLimits().upgrade(data)

        assert {key: data["dag"][key] for key in AlwaysWriteDagLimits.OLD_DEFAULTS} == {
            "catchup": False,
            "max_active_tasks": 16,
            "max_active_runs": 16,
            "max_consecutive_failed_dag_runs": 0,
            "disable_bundle_versioning": False,
        }

    def test_upgrade_keeps_written_values(self):
        data = _build_head_blob()
        expected = copy.deepcopy(data)

        AlwaysWriteDagLimits().upgrade(data)

        assert data == expected


def test_add_allowed_run_types_upgrade_writes_null():
    data = _build_v3_blob()

    AddAllowedRunTypes().upgrade(data)

    assert data["dag"]["allowed_run_types"] is None


class TestAddStubTaskBindings:
    def test_upgrade_marks_stub_tasks(self):
        data = _build_v3_blob()

        AddStubTaskBindings().upgrade(data)

        stub, plain = (task["__var"] for task in data["dag"]["tasks"])
        assert stub["is_stub"] is True
        assert "is_stub" not in plain

    def test_downgrade_drops_both_keys(self):
        data = _build_head_blob()

        AddStubTaskBindings().downgrade(data)

        stub = data["dag"]["tasks"][0]["__var"]
        assert "is_stub" not in stub
        assert "_arg_bindings" not in stub


def _get_loop_group(data: dict[str, Any]) -> dict[str, Any]:
    return data["dag"]["task_group"]["children"]["loop"][1]


@pytest.mark.parametrize(
    ("change", "get_container", "key"),
    [
        pytest.param(AddDagDeadline, lambda d: d["dag"], "deadline", id="AddDagDeadline"),
        pytest.param(
            AddTaskRenderTemplateAsNativeObj,
            lambda d: d["dag"]["tasks"][0]["__var"],
            "render_template_as_native_obj",
            id="AddTaskRenderTemplateAsNativeObj",
        ),
        pytest.param(AddAllowedRunTypes, lambda d: d["dag"], "allowed_run_types", id="AddAllowedRunTypes"),
        pytest.param(
            AddRerunWithLatestVersion,
            lambda d: d["dag"],
            "rerun_with_latest_version",
            id="AddRerunWithLatestVersion",
        ),
        pytest.param(AddTaskGroupDocMd, _get_loop_group, "doc_md", id="AddTaskGroupDocMd"),
        pytest.param(
            ReplaceRetryPolicyWithFlag,
            lambda d: d["dag"]["tasks"][1]["__var"]["partial_kwargs"],
            "has_retry_policy",
            id="ReplaceRetryPolicyWithFlag-partial_kwargs",
        ),
        pytest.param(
            ReplaceRetryPolicyWithFlag,
            lambda d: d["dag"]["default_args"]["__var"],
            "has_retry_policy",
            id="ReplaceRetryPolicyWithFlag-default_args",
        ),
        pytest.param(AddBundleName, lambda d: d["dag"], "bundle_name", id="AddBundleName"),
        pytest.param(AddTaskGroupLoop, _get_loop_group, "loop", id="AddTaskGroupLoop"),
        pytest.param(
            AddReturnsDagResult,
            lambda d: d["dag"]["tasks"][0]["__var"],
            "returns_dag_result",
            id="AddReturnsDagResult",
        ),
    ],
)
def test_downgrade_drops_key(change, get_container, key):
    data = _build_head_blob()
    assert key in get_container(data)

    change().downgrade(data)

    assert key not in get_container(data)


def test_head_blob_downgrades_to_3_1_0_shape():
    data = versions.downgrade(_build_head_blob(), to=3)

    dag = data["dag"]
    assert data["__version"] == 3
    assert data["client_defaults"] == {
        "tasks": {"retries": 2, "retry_exponential_backoff": True, "retry_delay": 300.0}
    }
    assert dag["timetable"]["__type"] == ASSET_TRIGGERED
    assert {"bundle_name", "allowed_run_types", "rerun_with_latest_version"}.isdisjoint(dag)
    # Migration 0101 moves deadline alerts; the engine leaves them to it.
    assert dag["deadline"] == [{"__type": "deadline_alert", "__var": {"interval": 60.0}}]


def test_head_blob_downgrades_to_3_0_shape():
    data = versions.downgrade(_build_head_blob(), to=2)

    dag = data["dag"]
    assert data["__version"] == 2
    assert "client_defaults" not in data
    assert "deadline" not in dag
    stub, mapped = (task["__var"] for task in dag["tasks"])
    assert stub["retries"] == 2
    assert stub["retry_delay"] == 300.0
    assert stub["on_failure_callback"] is True
    assert mapped["partial_kwargs"]["retry_delay"] == {"__type": "timedelta", "__var": 300.0}
    assert mapped["partial_kwargs"]["python_callable"] == "do_it"
