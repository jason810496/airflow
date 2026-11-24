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
"""
Conformance of mapped stub-task resolution (ADR-0016).

Every mapping form is resolved twice with the same supervisor responses: as a Python task resolves
itself, and as the supervisor's sidecar resolves a Python Dag's stub task for a language SDK runtime,
from the serialized bindings. Each instance must receive the same arguments.
"""

from __future__ import annotations

import inspect
from collections.abc import Mapping, Sequence
from typing import Any
from unittest import mock

import pytest

from airflow.providers.standard.decorators.stub import stub
from airflow.sdk import DAG, task, task_group
from airflow.sdk.api.datamodels._generated import TaskArgBinding, TaskInstance
from airflow.sdk.execution_time.comms import (
    ErrorResponse,
    GetTICount,
    GetXCom,
    GetXComCount,
    GetXComSequenceItem,
    GetXComSequenceSlice,
    TICount,
    XComCountResponse,
    XComResult,
    XComSequenceIndexResult,
    XComSequenceSliceResult,
)
from airflow.sdk.execution_time.lazy_sequence import LazyXComSequence
from airflow.serialization.enums import Encoding
from airflow.serialization.serialized_objects import DagSerialization

from tests_common.test_utils.version_compat import AIRFLOW_V_3_5_PLUS

pytestmark = pytest.mark.skipif(
    not AIRFLOW_V_3_5_PLUS, reason="mapped stub resolution was added in Airflow 3.5"
)

if AIRFLOW_V_3_5_PLUS:
    from airflow.sdk.coordinators._stub_mapping import needs_resolution, resolve_arg_bindings


def inc(x):
    return x + 1


def to_kwargs(pair):
    return {"x": pair[0], "y": pair[1]}


@task
def nums():
    return [1, 2]


@task
def letters():
    return ["a", "b", "c"]


@task
def produce(v):
    return v


@task
def dicts():
    return [{"x": 1, "y": "a"}, {"x": 2}]


def fn_xy(x: Any, y: Any = "dflt"): ...


def fn_xyz(x: Any, y: Any, z: int = 3): ...


def fn_v(v: int): ...


def fn_wv(w: int, v: int): ...


def fn_all(values: list): ...


def cross_product():
    with DAG("d") as dag:
        stub(fn_xyz).partial(z=5).expand(x=nums(), y=["a", "b"])
    return dag, "fn_xyz", {("nums", -1): [1, 2]}, {"fn_xyz": 4}


def literal_with_default():
    with DAG("d") as dag:
        stub(fn_xy).expand(x=[10, 20, 30])
    return dag, "fn_xy", {}, {"fn_xy": 3}


def mapped_upstream():
    with DAG("d") as dag:
        stub(fn_xy).expand(x=produce.expand(v=[1, 2, 3]))
    xcoms = {("produce", 0): 1, ("produce", 1): 2, ("produce", 2): 3}
    return dag, "fn_xy", xcoms, {"fn_xy": 3, "produce": 3}


def zip_concat_map():
    with DAG("d") as dag:
        n = nums()
        stub(fn_xyz).expand(x=n.zip(letters(), fillvalue="?"), y=n.concat(n), z=n.map(inc))
    return dag, "fn_xyz", {("nums", -1): [1, 2], ("letters", -1): ["a", "b", "c"]}, {"fn_xyz": 24}


def zip_under_map():
    with DAG("d") as dag:
        stub(fn_xy).expand(x=nums().zip(letters()).map(to_kwargs))
    return dag, "fn_xy", {("nums", -1): [1, 2], ("letters", -1): ["a", "b", "c"]}, {"fn_xy": 2}


def expand_kwargs_literal():
    with DAG("d") as dag:
        stub(fn_xy).partial(y="part").expand_kwargs([{"x": 1}, {"x": 7, "y": "own"}], strict=False)
    return dag, "fn_xy", {}, {"fn_xy": 2}


def expand_kwargs_upstream():
    with DAG("d") as dag:
        stub(fn_xy).expand_kwargs(dicts())
    return dag, "fn_xy", {("dicts", -1): [{"x": 1, "y": "a"}, {"x": 2}]}, {"fn_xy": 2}


def expand_kwargs_map():
    with DAG("d") as dag:
        stub(fn_xy).expand_kwargs(nums().zip(letters()).map(to_kwargs))
    return dag, "fn_xy", {("nums", -1): [1, 2], ("letters", -1): ["a", "b", "c"]}, {"fn_xy": 2}


def _group_dag():
    with DAG("d") as dag:

        @task_group
        def tg(v):
            return stub(fn_wv)(w=stub(fn_v)(v), v=v)

        stub(fn_all)(tg.expand(v=nums().map(inc)))
    xcoms = {
        ("nums", -1): [1, 2, 3],
        ("tg.fn_v", 0): 20,
        ("tg.fn_v", 1): 30,
        ("tg.fn_v", 2): 40,
        ("tg.fn_wv", 0): 200,
        ("tg.fn_wv", 1): 300,
        ("tg.fn_wv", 2): 400,
    }
    return dag, xcoms, {"tg.fn_v": 3, "tg.fn_wv": 3, "tg": 3}


def group_arg():
    dag, xcoms, counts = _group_dag()
    return dag, "tg.fn_v", xcoms, counts


def group_same_group_upstream():
    dag, xcoms, counts = _group_dag()
    return dag, "tg.fn_wv", xcoms, counts


def group_reduce():
    dag, xcoms, counts = _group_dag()
    return dag, "fn_all", xcoms, counts


def reduce():
    with DAG("d") as dag:
        stub(fn_all)(produce.expand(v=[1, 2]))
    return dag, "fn_all", {("produce", 0): 1, ("produce", 1): 2}, {"produce": 2}


SCENARIOS = [
    cross_product,
    literal_with_default,
    mapped_upstream,
    zip_concat_map,
    zip_under_map,
    expand_kwargs_literal,
    expand_kwargs_upstream,
    expand_kwargs_map,
    group_arg,
    group_same_group_upstream,
    group_reduce,
    reduce,
]


def _make_responder(xcoms: dict[tuple[str, int], Any], counts: dict[str, int]):
    def rows(task_id):
        return [v for (t, i), v in sorted(xcoms.items(), key=lambda kv: kv[0][1]) if t == task_id and i >= 0]

    def respond(msg):
        if isinstance(msg, GetXCom):
            return XComResult(
                key=msg.key, value=xcoms.get((msg.task_id, -1 if msg.map_index is None else msg.map_index))
            )
        if isinstance(msg, GetXComCount):
            return XComCountResponse(len=len(rows(msg.task_id)))
        if isinstance(msg, GetXComSequenceItem):
            values = rows(msg.task_id)
            if -len(values) <= msg.offset < len(values):
                return XComSequenceIndexResult(root=values[msg.offset])
            return ErrorResponse()
        if isinstance(msg, GetXComSequenceSlice):
            return XComSequenceSliceResult(root=rows(msg.task_id)[msg.start : msg.stop : msg.step])
        if isinstance(msg, GetTICount):
            if msg.task_group_id:
                return TICount(count=counts.get(msg.task_group_id, 0))
            return TICount(count=counts.get(msg.task_ids[0], 1))
        return mock.DEFAULT

    return respond


def _normalize(value: Any) -> Any:
    if isinstance(value, LazyXComSequence):
        return ("sequence", value._xcom_arg.operator.task_id)
    if isinstance(value, Mapping):
        return {k: _normalize(v) for k, v in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str):
        return [_normalize(v) for v in value]
    return value


def _resolve_in_python(create_runtime_ti, op, map_index: int) -> dict[str, Any]:
    ti = create_runtime_ti(
        task=op if op.is_mapped else op.prepare_for_execution(), dag_id="d", map_index=map_index
    )
    ti.task.render_template_fields(context=ti.get_template_context())
    bound = inspect.signature(ti.task.python_callable).bind(*ti.task.op_args, **ti.task.op_kwargs)
    bound.apply_defaults()
    return _normalize(dict(bound.arguments))


def _receive(bindings: list[dict]) -> dict[str, Any]:
    """What the runtime binds from resolved bindings: each literal's value, or a reduce's lazy sequence."""
    return {
        binding["name"]: ("sequence", binding["task_id"])
        if binding["kind"] == "sequence"
        else _normalize(binding.get("value"))
        for binding in bindings
    }


@pytest.mark.parametrize("scenario", SCENARIOS, ids=lambda s: s.__name__)
def test_sidecar_resolves_like_python(scenario, create_runtime_ti, mock_supervisor_comms):
    dag, task_id, xcoms, counts = scenario()
    encoded = {
        t[Encoding.VAR]["task_id"]: t[Encoding.VAR] for t in DagSerialization.to_dict(dag)["dag"]["tasks"]
    }[task_id]
    op = dag.task_dict[task_id]
    mock_supervisor_comms.send.side_effect = _make_responder(xcoms, counts)
    arg_bindings = [TaskArgBinding.model_validate(e) for e in encoded["_arg_bindings"]]
    for map_index in range(counts[task_id]) if op.get_needs_expansion() else [-1]:
        expected = _resolve_in_python(create_runtime_ti, op, map_index)
        runtime_ti = create_runtime_ti(task=op, dag_id="d", map_index=map_index)
        ti = TaskInstance.model_validate(
            {field: getattr(runtime_ti, field) for field in TaskInstance.model_fields}
        )

        if not needs_resolution(ti, arg_bindings):
            # Nothing varies per map index, so the supervisor passes the bindings on as they are.
            assert _receive(encoded["_arg_bindings"]) == expected
            continue
        ti_context = runtime_ti._ti_context_from_server.model_copy(update={"arg_bindings": arg_bindings})

        assert _receive(resolve_arg_bindings(ti, ti_context, op)) == expected, f"map_index={map_index}"
