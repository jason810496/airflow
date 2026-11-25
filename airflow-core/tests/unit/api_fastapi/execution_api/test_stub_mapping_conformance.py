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

Every mapping form is resolved twice from the same upstream values: as a Python task resolves itself, and
as a language SDK task receives it, from the API server's bindings, the supervisor's reads, and the
runtime steps, with the ``map()`` callables applied by a Dag definition's runtime or by the sidecar for a
stub TaskHandler. Each instance must receive the same arguments.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping, Sequence
from typing import Any
from unittest import mock

import pytest
from sqlalchemy import select

from airflow.api_fastapi.execution_api.services.task_instances import get_arg_bindings, get_task
from airflow.api_fastapi.execution_api.versions import bundle
from airflow.models.dagbag import DBDagBag
from airflow.models.taskinstance import TaskInstance
from airflow.models.xcom import XCOM_RETURN_KEY, XComModel
from airflow.sdk import task, task_group
from airflow.sdk.api.datamodels._generated import TaskArgBinding
from airflow.sdk.coordinators._apply_map import apply_map, find_map_argument
from airflow.sdk.coordinators._arg_bindings import read_arg_bindings
from airflow.sdk.definitions.xcom_arg import MapXComArg
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
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.db import clear_db_runs
from tests_common.test_utils.mapping import expand_mapped_task_instances

pytestmark = pytest.mark.db_test


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
    task.stub(fn_xyz).partial(z=5).expand(x=nums(), y=["a", "b"])
    return "fn_xyz", {("nums", -1): [1, 2]}, {"fn_xyz": 4}


def literal_with_default():
    task.stub(fn_xy).expand(x=[10, 20, 30])
    return "fn_xy", {}, {"fn_xy": 3}


def dict_upstream():
    task.stub(fn_xy).expand(x=produce({"uk": 1, "fr": 2}))
    return "fn_xy", {("produce", -1): {"uk": 1, "fr": 2}}, {"fn_xy": 2}


def mapped_upstream():
    task.stub(fn_xy).expand(x=produce.expand(v=[1, 2, 3]))
    xcoms = {("produce", 0): 1, ("produce", 1): 2, ("produce", 2): 3}
    return "fn_xy", xcoms, {"fn_xy": 3, "produce": 3}


def zip_concat_map():
    n = nums()
    task.stub(fn_xyz).expand(x=n.zip(letters(), fillvalue="?"), y=n.concat(n), z=n.map(inc))
    return "fn_xyz", {("nums", -1): [1, 2], ("letters", -1): ["a", "b", "c"]}, {"fn_xyz": 24}


def zip_under_map():
    task.stub(fn_xy).expand(x=nums().zip(letters()).map(to_kwargs))
    return "fn_xy", {("nums", -1): [1, 2], ("letters", -1): ["a", "b", "c"]}, {"fn_xy": 2}


def expand_kwargs_literal():
    task.stub(fn_xy).partial(y="part").expand_kwargs([{"x": 1}, {"x": 7, "y": "own"}], strict=False)
    return "fn_xy", {}, {"fn_xy": 2}


def expand_kwargs_upstream():
    task.stub(fn_xy).expand_kwargs(dicts())
    return "fn_xy", {("dicts", -1): [{"x": 1, "y": "a"}, {"x": 2}]}, {"fn_xy": 2}


def expand_kwargs_map():
    task.stub(fn_xy).expand_kwargs(nums().zip(letters()).map(to_kwargs))
    return "fn_xy", {("nums", -1): [1, 2], ("letters", -1): ["a", "b", "c"]}, {"fn_xy": 2}


def _group():
    @task_group
    def tg(v):
        return task.stub(fn_wv)(w=task.stub(fn_v)(v), v=v)

    task.stub(fn_all)(tg.expand(v=nums().map(inc)))
    xcoms = {
        ("nums", -1): [1, 2, 3],
        ("tg.fn_v", 0): 20,
        ("tg.fn_v", 1): 30,
        ("tg.fn_v", 2): 40,
        ("tg.fn_wv", 0): 200,
        ("tg.fn_wv", 1): 300,
        ("tg.fn_wv", 2): 400,
    }
    return xcoms, {"tg.fn_v": 3, "tg.fn_wv": 3, "tg": 3}


def group_arg():
    return "tg.fn_v", *_group()


def group_same_group_upstream():
    return "tg.fn_wv", *_group()


def group_reduce():
    return "fn_all", *_group()


def reduce():
    task.stub(fn_all)(produce.expand(v=[1, 2]))
    return "fn_all", {("produce", 0): 1, ("produce", 1): 2}, {"produce": 2}


SCENARIOS = [
    cross_product,
    literal_with_default,
    dict_upstream,
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


def _make_responder(xcoms: dict[tuple[str, int], Any], counts: dict[str, int]) -> Callable[[Any], Any]:
    """Answer a Python task's requests from *xcoms*, as the supervisor would."""

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


def _find_callables(op, name: str) -> Sequence[Callable[[Any], Any]]:
    """Find the map() callables of argument *name*, as a Dag definition's runtime finds its own."""
    if op.is_mapped:
        value = op._get_specified_expand_input().value
    else:
        # The scenarios name each group member's parameter after the group key it reads.
        value = op.get_closest_mapped_task_group()._expand_input.value
    source = value.get(name) if isinstance(value, Mapping) else value
    return source.callables if isinstance(source, MapXComArg) else []


def _apply_own_callables(op, name: str, value: Any) -> Any:
    """Apply the map() callables as a Dag definition's runtime applies its own."""
    for fn in _find_callables(op, name):
        # JSON has no tuple, so a zip() element reaches a runtime as a list.
        value = fn(tuple(value) if isinstance(value, list) else value)
    return value


def _send_apply_map(op, name: str, value: Any) -> Any:
    """Have the callables applied as the sidecar answers a stub TaskHandler's ApplyMap, from the same Python Dag."""
    return apply_map(find_map_argument(op, name), value)


def _apply_runtime_steps(bindings: list[dict], op, apply: Callable[[Any, str, Any], Any]) -> dict[str, Any]:
    """Do what ADR-0016's runtime steps do: get map() callables applied, then take kwarg, else fallback."""
    args: dict[str, Any] = {}
    for binding in bindings:
        if binding["kind"] == "sequence":
            args[binding["name"]] = ("sequence", binding["task_id"])
            continue
        value: Any = binding.get("value")
        if binding.get("apply_map"):
            value = apply(op, binding["name"], value)
        if (kwarg := binding.get("kwarg")) is not None:
            value = value[kwarg] if kwarg in value else binding["fallback"]["value"]
        args[binding["name"]] = _normalize(value)
    return args


def _push_upstream_values(xcoms: dict[tuple[str, int], Any], dag, run_id: str, *, session) -> None:
    for (task_id, map_index), value in sorted(xcoms.items(), key=lambda kv: kv[0][1]):
        if map_index >= 0 and not _get_tis(dag, run_id, task_id, session=session)[0].map_index >= 0:
            expand_mapped_task_instances(dag.task_dict[task_id], run_id, session=session)
        ti = next(t for t in _get_tis(dag, run_id, task_id, session=session) if t.map_index == map_index)
        # A mapped upstream's length counts its successful instances.
        ti.state = TaskInstanceState.SUCCESS
        XComModel.set(
            key=XCOM_RETURN_KEY,
            value=value,
            dag_id=ti.dag_id,
            task_id=task_id,
            run_id=run_id,
            map_index=map_index,
            mapped_length=len(value) if isinstance(value, (list, dict)) else None,
            session=session,
        )
    session.flush()


def _get_tis(dag, run_id: str, task_id: str, *, session) -> list[TaskInstance]:
    return list(
        session.scalars(
            select(TaskInstance)
            .where(
                TaskInstance.dag_id == dag.dag_id,
                TaskInstance.run_id == run_id,
                TaskInstance.task_id == task_id,
            )
            .order_by(TaskInstance.map_index)
        )
    )


@pytest.fixture(autouse=True)
def _clean_db():
    clear_db_runs()
    yield
    clear_db_runs()


@pytest.fixture(autouse=True)
def _head_api_version():
    """Derive bindings for a client at the head Execution API version, as outside a request."""
    token = bundle.api_version_var.set(None)
    yield
    bundle.api_version_var.reset(token)


@pytest.mark.parametrize(
    "apply",
    [
        pytest.param(_apply_own_callables, id="dag-definition"),
        pytest.param(_send_apply_map, id="stub-task-handler"),
    ],
)
@pytest.mark.parametrize("scenario", SCENARIOS, ids=lambda s: s.__name__)
def test_server_and_supervisor_resolve_like_python(
    scenario, apply, dag_maker, session, create_runtime_ti, mock_supervisor_comms
):
    with dag_maker("d", serialized=True, session=session) as dag:
        task_id, xcoms, counts = scenario()
    dr = dag_maker.create_dagrun()
    _push_upstream_values(xcoms, dag, dr.run_id, session=session)
    op = dag_maker.dag.task_dict[task_id]
    if op.get_needs_expansion() and _get_tis(dag, dr.run_id, task_id, session=session)[0].map_index < 0:
        expand_mapped_task_instances(dag.task_dict[task_id], dr.run_id, session=session)
    mock_supervisor_comms.send.side_effect = _make_responder(xcoms, counts)
    dag_bag = DBDagBag()

    tis = _get_tis(dag, dr.run_id, task_id, session=session)
    assert len(tis) == counts.get(task_id, 1)
    for ti in tis:
        expected = _resolve_in_python(create_runtime_ti, op, ti.map_index)
        serialized_task = get_task(dag_bag, ti, session=session)
        sent = get_arg_bindings(serialized_task, ti, dag_bag=dag_bag, session=session) or []
        received = read_arg_bindings(
            [TaskArgBinding.model_validate(b if isinstance(b, dict) else b.model_dump()) for b in sent],
            lambda ref: xcoms[ref.task_id, -1 if ref.map_index is None else ref.map_index],
        )
        bindings = [b.model_dump(mode="json", exclude_unset=True) for b in received]

        assert _apply_runtime_steps(bindings, op, apply) == expected, f"map_index={ti.map_index}"
