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

from airflow.sdk.api.datamodels._generated import TaskArgBinding, TIProviderOperator
from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk.definitions.dag import DAG
from airflow.sdk.definitions.xcom_arg import PlainXComArg
from airflow.sdk.execution_time.provider_operator import (
    ProviderOperatorError,
    build_provider_operator,
    resolve_binding,
)

FILELOC = "/bundles/app/dag.min.mjs"
DATE_TIME = {"type": "string", "format": "date-time"}
DURATION_SECONDS = {"type": "number", "format": "duration-seconds"}


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


def literal(name, value, value_schema=None):
    return {"name": name, "kind": "literal", "value": value, "value_schema": value_schema}


def xcom(name, task_id):
    return {"name": name, "kind": "xcom", "task_id": task_id}


def provider_operator(kwargs, *, classpath=f"{__name__}.RecordingOperator", downstream_task_ids=None):
    return TIProviderOperator.model_validate(
        {
            "classpath": classpath,
            "kwargs": kwargs,
            "downstream_task_ids": downstream_task_ids or {"notify": []},
        }
    )


def build(operator):
    return build_provider_operator(operator, dag_id="native_dag", task_id="notify", fileloc=FILELOC)


@pytest.mark.parametrize(
    ("binding", "expected"),
    [
        pytest.param(literal("a", None), None, id="null"),
        pytest.param(literal("a", "text"), "text", id="string"),
        pytest.param(literal("a", 3), 3, id="number"),
        pytest.param(literal("a", {"tags": ["a", 1]}), {"tags": ["a", 1]}, id="json"),
        pytest.param(literal("a", "10:00", {"type": "string"}), "10:00", id="schema-without-format"),
        pytest.param(literal("a", None, DATE_TIME), None, id="null-date-time"),
        pytest.param(literal("a", None, DURATION_SECONDS), None, id="null-duration"),
        pytest.param(literal("a", 90, DURATION_SECONDS), datetime.timedelta(seconds=90), id="duration"),
        pytest.param(
            literal("a", 1.5, DURATION_SECONDS), datetime.timedelta(seconds=1.5), id="fractional-duration"
        ),
        pytest.param(
            literal("a", "2026-01-02T03:04:05.000Z", DATE_TIME),
            datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.UTC),
            id="utc-date-time",
        ),
        pytest.param(
            literal("a", "2026-01-02T03:04:05+02:00", DATE_TIME),
            datetime.datetime(2026, 1, 2, 1, 4, 5, tzinfo=datetime.UTC),
            id="offset-date-time",
        ),
        pytest.param(
            literal("a", "2026-01-02T03:04:05", DATE_TIME),
            datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.UTC),
            id="date-time-without-offset-is-utc",
        ),
    ],
)
def test_resolve_binding_reads_a_literal_as_the_type_its_schema_names(binding, expected):
    resolved = resolve_binding(TaskArgBinding.model_validate(binding), DAG("d", schedule=None))

    assert resolved == expected
    assert type(resolved) is type(expected)
    if isinstance(resolved, datetime.datetime):
        assert resolved.tzinfo is not None


@pytest.mark.parametrize(
    ("value", "value_schema"),
    [
        pytest.param("tomorrow", DATE_TIME, id="date-time-not-a-date"),
        pytest.param(5, DATE_TIME, id="date-time-not-a-string"),
        pytest.param("a minute", DURATION_SECONDS, id="duration-not-a-number"),
    ],
)
def test_resolve_binding_rejects_a_literal_its_schema_does_not_fit(value, value_schema):
    binding = TaskArgBinding.model_validate(literal("when", value, value_schema))

    with pytest.raises(ProviderOperatorError, match=f"Argument 'when' is not a valid .*: {value!r}"):
        resolve_binding(binding, DAG("d", schedule=None))


def test_resolve_binding_turns_an_xcom_binding_into_the_xcom_arg_of_that_task():
    with DAG("d", schedule=None) as dag:
        upstream = RecordingOperator(task_id="extract")

    resolved = resolve_binding(TaskArgBinding.model_validate(xcom("message", "extract")), dag)

    assert isinstance(resolved, PlainXComArg)
    assert resolved.operator is upstream
    assert resolved.key == "return_value"


def test_resolve_binding_rejects_an_xcom_binding_of_an_unknown_task():
    with pytest.raises(ProviderOperatorError, match="unknown task 'missing'"):
        resolve_binding(TaskArgBinding.model_validate(xcom("message", "missing")), DAG("d", schedule=None))


def test_build_provider_operator_builds_the_operator_with_its_dag_around_it():
    operator = provider_operator(
        [
            xcom("message", "extract"),
            literal("timeout", 30, DURATION_SECONDS),
            literal("not_before", "2026-01-02T03:04:05.000Z", DATE_TIME),
            literal("extra", {"tags": ["a"]}),
            literal("retries", 2),
        ],
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
    assert task.not_before == datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.UTC)
    assert task.extra == {"tags": ["a"]}
    assert isinstance(task.message, PlainXComArg)
    assert task.message.operator.task_id == "extract"
    assert task.upstream_task_ids == {"extract"}
    assert task.downstream_task_ids == {"report", "cleanup"}
    assert set(task.dag.task_dict) == {"extract", "notify", "report", "cleanup"}


def test_the_built_operator_is_not_a_stub_so_the_task_runner_executes_it():
    assert build(provider_operator([literal("message", "hi")])).is_stub is False


def test_a_placeholder_task_does_not_run():
    operator = provider_operator([], downstream_task_ids={"extract": ["notify"], "notify": []})
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
        build(provider_operator([], classpath=classpath))
