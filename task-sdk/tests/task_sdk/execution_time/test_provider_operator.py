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

from airflow.sdk.api.datamodels._generated import TIProviderOperatorResponse
from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk.definitions.dag import DAG
from airflow.sdk.definitions.xcom_arg import PlainXComArg
from airflow.sdk.execution_time.provider_operator import (
    ProviderOperatorError,
    build_provider_operator,
    decode_value,
)

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


def provider_operator(kwargs, *, classpath=f"{__name__}.RecordingOperator", downstream_task_ids=None):
    return TIProviderOperatorResponse(
        classpath=classpath,
        kwargs=kwargs,
        downstream_task_ids=downstream_task_ids or {"notify": []},
    )


def build(operator):
    return build_provider_operator(operator, dag_id="native_dag", task_id="notify", fileloc=FILELOC)


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
    with pytest.raises(ProviderOperatorError, match=message):
        decode_value(encoded, DAG("d", schedule=None))


def test_build_provider_operator_builds_the_operator_with_its_dag_around_it():
    operator = provider_operator(
        {
            "message": {"__type": "xcomref", "__var": {"task_id": "extract", "key": "return_value"}},
            "timeout": {"__type": "timedelta", "__var": 30},
            "extra": {"__type": "dict", "__var": {"tags": ["a"]}},
            "retries": 2,
        },
        downstream_task_ids={
            "extract": ["notify"],
            "notify": ["report", "cleanup"],
            "report": [],
            "cleanup": [],
        },
    )

    task = build(operator)

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
    assert set(task.dag.task_dict) == {"extract", "notify", "report", "cleanup"}


def test_a_placeholder_task_does_not_run():
    operator = provider_operator({}, downstream_task_ids={"extract": ["notify"], "notify": []})
    placeholder = build(operator).dag.task_dict["extract"]

    with pytest.raises(RuntimeError, match="'extract' is a placeholder"):
        placeholder.execute({})


@pytest.mark.parametrize(
    ("classpath", "message"),
    [
        ("no.such.module.Operator", "Cannot import operator no.such.module.Operator"),
        ("datetime.timedelta", "datetime.timedelta is not an operator class"),
    ],
)
def test_build_provider_operator_rejects_a_class_it_cannot_build(classpath, message):
    with pytest.raises(ProviderOperatorError, match=message):
        build(provider_operator({}, classpath=classpath))
