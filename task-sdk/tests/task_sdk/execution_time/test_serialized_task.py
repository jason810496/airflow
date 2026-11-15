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

import pytest

from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk.definitions.dag import DAG
from airflow.sdk.definitions.xcom_arg import PlainXComArg
from airflow.sdk.execution_time.serialized_task import SerializedTaskError, build_task, decode_value

FILELOC = "/bundles/app/dag.min.mjs"


class RecordingOperator(BaseOperator):
    """An operator that keeps what it was built with."""

    template_fields = ("message",)

    def __init__(self, *, message=None, timeout=None, not_before=None, extra=None, **kwargs):
        super().__init__(**kwargs)
        self.message = message
        self.timeout = timeout
        self.not_before = not_before
        self.extra = extra

    def execute(self, context):
        return self.message


def encoded_task(task_id, *, downstream=(), **fields):
    return {
        "__type": "operator",
        "__var": {"task_id": task_id, "downstream_task_ids": list(downstream), **fields},
    }


def provider_task(task_id, kwargs, *, downstream=()):
    return encoded_task(
        task_id,
        downstream=downstream,
        task_type="RecordingOperator",
        _task_module=__name__,
        _operator_kwargs={"__type": "dict", "__var": kwargs},
    )


def stub_task(task_id, *, downstream=(), **fields):
    return encoded_task(
        task_id,
        downstream=downstream,
        task_type="TypeScriptOperator",
        _task_module="airflow.sdk.coordinators.node",
        is_stub=True,
        **fields,
    )


def serialized_dag(*tasks):
    return {"__version": 3, "dag": {"dag_id": "native_dag", "tasks": list(tasks)}}


@pytest.mark.parametrize(
    ("encoded", "expected"),
    [
        (None, None),
        ("text", "text"),
        (3, 3),
        ([1, "a"], [1, "a"]),
        ({"__type": "dict", "__var": {"a": {"__type": "dict", "__var": {"b": 1}}}}, {"a": {"b": 1}}),
        ({"__type": "timedelta", "__var": 90}, datetime.timedelta(seconds=90)),
        (
            {"__type": "datetime", "__var": 1767323045},
            datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.UTC),
        ),
        ({"__type": "set", "__var": ["a", "b"]}, {"a", "b"}),
        ({"__type": "tuple", "__var": [1, 2]}, (1, 2)),
    ],
)
def test_decode_value(encoded, expected):
    assert decode_value(encoded, DAG("d", schedule=None)) == expected


def test_decode_value_turns_a_reference_into_the_xcom_arg_of_that_task():
    with DAG("d", schedule=None) as dag:
        upstream = RecordingOperator(task_id="extract")

    decoded = decode_value({"__type": "xcomref", "__var": {"task_id": "extract", "key": "rows"}}, dag)

    assert isinstance(decoded, PlainXComArg)
    assert decoded.operator is upstream
    assert decoded.key == "rows"


@pytest.mark.parametrize(
    ("encoded", "message"),
    [
        ({"a": 1}, "it is not a type-encoded value"),
        ({"__type": "pod", "__var": {}}, "Cannot decode a value of type 'pod'"),
        ({"__type": "xcomref", "__var": {"task_id": "missing", "key": "x"}}, "unknown task 'missing'"),
    ],
)
def test_decode_value_rejects_what_it_cannot_decode(encoded, message):
    with pytest.raises(SerializedTaskError, match=message):
        decode_value(encoded, DAG("d", schedule=None))


def test_build_task_builds_the_operator_with_its_dag_around_it():
    data = serialized_dag(
        stub_task("extract", downstream=["notify"]),
        provider_task(
            "notify",
            {
                "message": {"__type": "xcomref", "__var": {"task_id": "extract", "key": "return_value"}},
                "timeout": {"__type": "timedelta", "__var": 30},
                "extra": {"__type": "dict", "__var": {"tags": ["a"]}},
                "retries": 2,
            },
            downstream=["report", "cleanup"],
        ),
        stub_task("report"),
        stub_task("cleanup", is_teardown=True, trigger_rule="all_done"),
    )

    task = build_task(data, "notify", fileloc=FILELOC)

    assert isinstance(task, RecordingOperator)
    assert task.dag.dag_id == "native_dag"
    assert task.dag.fileloc == FILELOC
    assert task.retries == 2
    assert task.timeout == datetime.timedelta(seconds=30)
    assert task.extra == {"tags": ["a"]}
    assert isinstance(task.message, PlainXComArg)
    assert task.message.operator.task_id == "extract"
    assert task.upstream_task_ids == {"extract"}
    assert task.downstream_task_ids == {"report", "cleanup"}
    cleanup = task.dag.task_dict["cleanup"]
    assert cleanup.is_teardown is True
    assert cleanup.trigger_rule == "all_done"


def test_a_placeholder_task_does_not_run():
    data = serialized_dag(stub_task("extract"), provider_task("notify", {}))
    placeholder = build_task(data, "notify", fileloc=FILELOC).dag.task_dict["extract"]

    with pytest.raises(RuntimeError, match="'extract' is a placeholder"):
        placeholder.execute({})


@pytest.mark.parametrize(
    "task_id",
    [
        pytest.param("stub", id="a task the language runtime runs"),
        pytest.param("plain", id="a task without operator arguments"),
        pytest.param("missing", id="a task the Dag does not have"),
    ],
)
def test_build_task_returns_none_for_a_task_no_python_operator_runs(task_id):
    data = serialized_dag(
        stub_task("stub"),
        encoded_task("plain", task_type="RecordingOperator", _task_module=__name__),
    )

    assert build_task(data, task_id, fileloc=FILELOC) is None


@pytest.mark.parametrize(
    ("module", "task_type", "message"),
    [
        ("no.such.module", "Operator", "Cannot import operator no.such.module.Operator"),
        ("datetime", "timedelta", "datetime.timedelta is not an operator class"),
    ],
)
def test_build_task_rejects_a_class_it_cannot_build(module, task_type, message):
    task = provider_task("notify", {})
    task["__var"].update(task_type=task_type, _task_module=module)

    with pytest.raises(SerializedTaskError, match=message):
        build_task(serialized_dag(task), "notify", fileloc=FILELOC)
