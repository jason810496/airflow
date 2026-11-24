#
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
"""Resolution paths the conformance test does not reach: when to resolve, skips and errors."""

from __future__ import annotations

from unittest import mock

import pytest
from uuid6 import uuid7

from airflow.sdk import DAG, task
from airflow.sdk.api.datamodels._generated import TaskArgBinding, TaskInstance
from airflow.sdk.coordinators._stub_mapping import (
    ArgResolutionError,
    needs_resolution,
    resolve_arg_bindings,
)
from airflow.sdk.exceptions import AirflowSkipException
from airflow.sdk.execution_time.comms import GetXCom, XComResult


def _ti(task_id: str, map_index: int) -> TaskInstance:
    return TaskInstance(
        id=uuid7(),
        task_id=task_id,
        dag_id="d",
        run_id="r",
        try_number=1,
        dag_version_id=uuid7(),
        map_index=map_index,
    )


def _bindings(*kinds: str) -> list[TaskArgBinding]:
    entries = {
        "literal": {"name": "a", "kind": "literal", "value": 1},
        "xcom": {"name": "b", "kind": "xcom", "task_id": "t"},
        "sequence": {"name": "c", "kind": "sequence", "task_id": "t"},
        "param": {"name": "d", "kind": "param"},
        "kwargs": {"kind": "kwargs"},
    }
    return [TaskArgBinding.model_validate(entries[kind]) for kind in kinds]


@pytest.mark.parametrize(
    ("kinds", "map_index", "expected"),
    [
        pytest.param(("literal", "xcom"), 0, False, id="unmapped-kinds"),
        pytest.param(("param",), 0, True, id="param"),
        pytest.param(("kwargs",), 0, True, id="kwargs"),
        pytest.param(("sequence",), -1, False, id="reduce"),
        pytest.param(("sequence",), 2, True, id="sequence-in-a-mapped-group"),
    ],
)
def test_needs_resolution(kinds, map_index, expected):
    assert needs_resolution(_ti("t", map_index), _bindings(*kinds)) is expected


@pytest.fixture
def rows(mock_supervisor_comms):
    def respond(msg):
        if isinstance(msg, GetXCom) and msg.task_id == "rows":
            return XComResult(key=msg.key, value=["a.csv", "skip"])
        return mock.DEFAULT

    mock_supervisor_comms.send.side_effect = respond


def _skip(path):
    if path == "skip":
        raise AirflowSkipException("nothing to load")
    return path.upper()


def _mapped_task():
    with DAG("d"):

        @task
        def rows():
            return []

        @task
        def load(path): ...

        return load.expand(path=rows().map(_skip)).operator


def _resolve(make_ti_context, op, map_index: int, bindings: list[dict]):
    ti_context = make_ti_context(dag_id="d", run_id="r").model_copy(
        update={"arg_bindings": [TaskArgBinding.model_validate(b) for b in bindings]}
    )
    return resolve_arg_bindings(_ti(op.task_id, map_index), ti_context, op)


def test_the_python_map_callables_run_while_resolving(rows, make_ti_context):
    assert _resolve(make_ti_context, _mapped_task(), 0, [{"name": "path", "kind": "param"}]) == [
        {"name": "path", "kind": "literal", "value": "A.CSV"}
    ]


def test_a_skip_from_a_map_callable_reaches_the_caller(rows, make_ti_context):
    with pytest.raises(AirflowSkipException, match="nothing to load"):
        _resolve(make_ti_context, _mapped_task(), 1, [{"name": "path", "kind": "param"}])


def test_an_argument_the_task_does_not_take_fails(rows, make_ti_context):
    with pytest.raises(ArgResolutionError, match="'other' has no value at map index 0"):
        _resolve(make_ti_context, _mapped_task(), 0, [{"name": "other", "kind": "param"}])
