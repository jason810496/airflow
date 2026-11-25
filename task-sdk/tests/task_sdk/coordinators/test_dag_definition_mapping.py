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
"""Resolution paths of a Dag definition the conformance test cannot reach: its own spread, and errors."""

from __future__ import annotations

from unittest import mock

import pytest
from uuid6 import uuid7

from airflow.sdk.api.datamodels._generated import TaskArgBinding, TaskInstance
from airflow.sdk.coordinators._dag_definition_mapping import resolve_dag_definition_bindings
from airflow.sdk.coordinators._stub_mapping import ArgResolutionError
from airflow.sdk.execution_time.comms import GetXCom, XComResult

ROWS = {"__type": "xcomref", "__var": {"task_id": "rows", "key": "return_value"}}


def _ti(map_index: int) -> TaskInstance:
    return TaskInstance(
        id=uuid7(),
        task_id="load",
        dag_id="d",
        run_id="r",
        try_number=1,
        dag_version_id=uuid7(),
        map_index=map_index,
    )


def _dag(expand_input: dict, **task) -> dict:
    """A serialized Dag whose task ``load`` expands over *expand_input*, as a Dag definition writes it."""
    load = {
        "task_id": "load",
        "_is_mapped": True,
        "_expand_input_attr": "op_kwargs_expand_input",
        "op_kwargs_expand_input": expand_input,
        **task,
    }
    return {
        "tasks": [
            {"__type": "operator", "__var": {"task_id": "rows"}},
            {"__type": "operator", "__var": load},
        ],
        "task_group": {
            "_group_id": None,
            "children": {"rows": ["operator", "rows"], "load": ["operator", "load"]},
        },
    }


@pytest.fixture
def rows(mock_supervisor_comms):
    def respond(msg):
        if isinstance(msg, GetXCom) and msg.task_id == "rows":
            return XComResult(key=msg.key, value=[{"path": "a.csv"}, {"path": "b.csv", "min": 10}])
        return mock.DEFAULT

    mock_supervisor_comms.send.side_effect = respond


def _resolve(make_ti_context, map_index: int, bindings: list[dict], dag: dict):
    ti_context = make_ti_context(dag_id="d", run_id="r").model_copy(
        update={"arg_bindings": [TaskArgBinding.model_validate(b) for b in bindings]}
    )
    return resolve_dag_definition_bindings(_ti(map_index), ti_context, dag)


def test_a_kwargs_entry_spreads_this_indexs_dict(rows, make_ti_context):
    dag = _dag(
        {"type": "list-of-dicts", "value": ROWS},
        partial_kwargs={"op_kwargs": {"__type": "dict", "__var": {"bucket": "raw"}}, "op_args": []},
        _disallow_kwargs_override=True,
    )
    bindings = [{"name": "bucket", "kind": "param"}, {"kind": "kwargs"}]

    assert _resolve(make_ti_context, 1, bindings, dag) == [
        {"name": "bucket", "kind": "literal", "value": "raw"},
        {"name": "**", "kind": "literal", "value": {"path": "b.csv", "min": 10}, "spread": True},
    ]


def test_a_kwargs_entry_over_map_leaves_the_callables_to_the_runtime(rows, make_ti_context):
    mapped = {"__type": "xcomref", "__var": {"type": "map", "arg": ROWS["__var"], "callables": ["(r) => r"]}}
    dag = _dag({"type": "list-of-dicts", "value": mapped})

    assert _resolve(make_ti_context, 0, [{"kind": "kwargs"}], dag) == [
        {"name": "**", "kind": "literal", "value": {"path": "a.csv"}, "spread": True, "apply_map": True}
    ]


@pytest.mark.parametrize(
    ("map_index", "bindings", "expand_input", "error"),
    [
        pytest.param(
            5,
            [{"kind": "kwargs"}],
            {
                "type": "list-of-dicts",
                "value": {
                    "__type": "xcomref",
                    "__var": {"type": "map", "arg": ROWS["__var"], "callables": ["f"]},
                },
            },
            "has no item at map index 5",
            id="index-past-the-end",
        ),
        pytest.param(
            0,
            [{"name": "x", "kind": "param"}],
            {
                "type": "dict-of-lists",
                "value": {
                    "__type": "dict",
                    "__var": {
                        "x": {
                            "__type": "xcomref",
                            "__var": {
                                "type": "zip",
                                "args": [{"type": "map", "arg": ROWS["__var"], "callables": ["f"]}],
                            },
                        }
                    },
                },
            },
            "'map' that is not outermost",
            id="inner-map",
        ),
        pytest.param(
            0,
            [{"name": "x", "kind": "param"}],
            {
                "type": "dict-of-lists",
                "value": {
                    "__type": "dict",
                    "__var": {
                        "x": {"__type": "xcomref", "__var": {"task_id": "gone", "key": "return_value"}}
                    },
                },
            },
            "Task 'gone' is not in the Dag",
            id="missing-upstream",
        ),
    ],
)
def test_an_argument_with_no_value_fails(rows, make_ti_context, map_index, bindings, expand_input, error):
    with pytest.raises(ArgResolutionError, match=error):
        _resolve(make_ti_context, map_index, bindings, _dag(expand_input))
