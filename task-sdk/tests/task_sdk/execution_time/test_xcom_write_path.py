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
"""Tests for the experimental supervisor write path of a Python task's XComs."""

from __future__ import annotations

import json
import os
import socket
from collections import namedtuple
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar
from unittest import mock

import attrs
import msgspec
import pytest
import structlog
from uuid6 import uuid7

from airflow.sdk import DAG, BaseOperator, task, task_group
from airflow.sdk.api import client as sdk_client
from airflow.sdk.api.datamodels._generated import TaskInstance, TaskInstanceState
from airflow.sdk.bases.xcom import BaseXCom
from airflow.sdk.coordinators._xcom_backend import XComBackendCall, _HookRunner
from airflow.sdk.exceptions import (
    AirflowRuntimeError,
    ErrorType,
    UnmappableXComTypePushed,
    XComForMappingNotPushed,
)
from airflow.sdk.execution_time import task_runner, xcom as xcom_module, xcom_write_path
from airflow.sdk.execution_time.comms import BundleInfo, CommsDecoder, ErrorResponse, SetXCom
from airflow.sdk.execution_time.supervisor import ActivitySubprocess, ProcessTracker, supervise_task
from airflow.sdk.execution_time.task_runner import _push_xcom_if_needed
from airflow.sdk.execution_time.xcom_write_path import (
    ENV_FLAG,
    SupervisorXComWriter,
    is_raw_value,
    measure_mapped_length,
    unwrap_raw_value,
)
from airflow.sdk.serde import serialize

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

if not AIRFLOW_V_3_3_PLUS:
    pytest.skip("The supervisor write path builds on the coordinator XCom backend", allow_module_level=True)

log = structlog.get_logger()

Point = namedtuple("Point", ["x", "y"])
_TI_ID = uuid7()


@dataclass
class Order:
    id: int


class _PointerXCom(BaseXCom):
    """Keeps values in ``stored`` and a pointer to each in the XCom row, as an object storage backend does."""

    stored: ClassVar[dict[str, Any]] = {}

    @classmethod
    def store_value(cls, coordinate, key, value):
        pointer = f"ptr://{coordinate.task_id}/{coordinate.ti_id}/{key}"
        cls.stored[pointer] = value
        return pointer


@attrs.define
class _InProcessHelper:
    """Runs XCom backend calls in the test process, the way the helper process runs them."""

    xcom: type[BaseXCom]
    calls: list[XComBackendCall] = attrs.field(factory=list)
    can_run_a_call: bool = True

    def send_call(self, call, finish):
        self.calls.append(call)
        finish(_HookRunner(self.xcom).run(call))

    def stop(self):
        pass


@attrs.define
class _DeferredHelper:
    """Holds each call until the test answers it, the way the helper process runs one call at a time."""

    xcom: type[BaseXCom]
    pending: list = attrs.field(factory=list)
    can_run_a_call: bool = True

    def send_call(self, call, finish):
        self.pending.append((call, finish))

    def answer_next(self):
        call, finish = self.pending.pop(0)
        finish(_HookRunner(self.xcom).run(call))

    def stop(self):
        pass


def local_dag_bundle_cfg(path) -> dict[str, str]:
    bundle = {
        "name": "my-bundle",
        "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
        "kwargs": {"path": str(path), "refresh_interval": 1},
    }
    return {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps([bundle])}


@pytest.fixture(autouse=True)
def _clear_stored():
    yield
    _PointerXCom.stored.clear()


def _make_dto(ti_id, task_id: str, map_index: int = -1) -> TaskInstance:
    return TaskInstance(
        id=ti_id,
        dag_version_id=uuid7(),
        task_id=task_id,
        dag_id="exp_dag",
        run_id="test_run",
        try_number=1,
        map_index=map_index,
        queue="default",
    )


@pytest.fixture
def make_supervisor(mocker, monkeypatch):
    """Build an ``ActivitySubprocess`` for a task instance, with a mock API client and no real process."""

    def make(xcom: type[BaseXCom], ti_id, task_id: str, map_index: int = -1, helper=None):
        monkeypatch.setattr("airflow.sdk.execution_time.xcom.XCom", xcom)
        client = mocker.Mock(spec=sdk_client.Client)
        client.xcoms = mocker.create_autospec(sdk_client.XComOperations, instance=True)
        proc = ActivitySubprocess(
            id=ti_id,
            pid=12345,
            process=mock.MagicMock(spec=ProcessTracker),
            process_log=structlog.get_logger(logger_name="task"),
            stdin=mock.MagicMock(spec=socket.socket),
            client=client,
        )
        proc.ti = _make_dto(ti_id, task_id, map_index)  # type: ignore[assignment]
        proc._open_sockets[proc.stdin] = "requests"
        proc._xcom_writer = SupervisorXComWriter(proc)
        proc._xcom_writer._helper = helper if helper is not None else _InProcessHelper(xcom)  # type: ignore[assignment]
        return proc

    return make


def _sent_error(proc: ActivitySubprocess) -> dict | None:
    """The error of the frame most recently sent to the task process."""
    frame = proc.stdin.sendall.call_args.args[0]  # type: ignore[attr-defined]
    return msgspec.msgpack.decode(frame[4:])[2]


@attrs.define
class _SupervisorLoop:
    """Hands each request of the task process to the supervisor's handler, and raises what it answers."""

    proc: ActivitySubprocess

    def send(self, msg):
        self.proc.stdin.sendall.reset_mock()  # type: ignore[attr-defined]
        self.proc._handle_request(msg, log, 1)  # type: ignore[arg-type]
        if (error := _sent_error(self.proc)) is not None:
            raise AirflowRuntimeError(ErrorResponse.model_validate(error))
        return None

    async def asend(self, msg):
        return self.send(msg)


@pytest.fixture
def connect(monkeypatch, mocker):
    """Make the task process talk to ``proc``, optionally with the supervisor write path on."""

    def _connect(proc: ActivitySubprocess, *, flag: bool) -> None:
        comms = mocker.create_autospec(CommsDecoder, instance=True)
        loop = _SupervisorLoop(proc)
        comms.send.side_effect = loop.send
        comms.asend.side_effect = loop.asend
        monkeypatch.setattr(task_runner, "SUPERVISOR_COMMS", comms, raising=False)
        monkeypatch.setattr(task_runner, "XCom", xcom_module.XCom)
        if flag:
            monkeypatch.setenv(ENV_FLAG, "1")
        else:
            monkeypatch.delenv(ENV_FLAG, raising=False)

    return _connect


@pytest.mark.parametrize(
    ("env", "expected"),
    [
        pytest.param(None, False, id="unset"),
        pytest.param("0", False, id="zero"),
        pytest.param("1", True, id="one"),
    ],
)
def test_is_enabled(monkeypatch, env, expected):
    monkeypatch.delenv(ENV_FLAG, raising=False)
    if env is not None:
        monkeypatch.setenv(ENV_FLAG, env)
    assert xcom_write_path.is_enabled() is expected


def test_is_active_needs_a_task_process_comms(monkeypatch, mock_supervisor_comms):
    monkeypatch.setenv(ENV_FLAG, "1")
    assert xcom_write_path.is_active()

    monkeypatch.setattr(task_runner, "SUPERVISOR_COMMS", object())
    assert not xcom_write_path.is_active()


class TestMeasureMappedLength:
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param([1, 2, 3], id="list"),
            pytest.param([], id="empty-list"),
            pytest.param({"a": 1, "b": 2}, id="dict"),
            pytest.param((1, 2, 3, 4), id="tuple"),
            pytest.param(Point(1, 2), id="namedtuple"),
            pytest.param([[1], [2, 3]], id="nested-list"),
            pytest.param([datetime(2024, 1, 1, tzinfo=UTC)] * 3, id="list-of-encoded-elements"),
            pytest.param((Order(1), Order(2)), id="tuple-of-encoded-elements"),
        ],
    )
    def test_matches_len_of_the_value(self, value):
        assert measure_mapped_length(serialize(value)) == len(value)

    @pytest.mark.parametrize(
        ("value", "type_name"),
        [
            pytest.param("abc", "str", id="str"),
            pytest.param(7, "int", id="int"),
            pytest.param(1.5, "float", id="float"),
            pytest.param({1, 2}, "set", id="set"),
            pytest.param(datetime(2024, 1, 1, tzinfo=UTC), "datetime", id="datetime"),
            pytest.param(Order(1), "Order", id="dataclass"),
        ],
    )
    def test_rejects_what_is_not_mappable(self, value, type_name):
        with pytest.raises(xcom_write_path._Unmappable, match=type_name):
            measure_mapped_length(serialize(value))

    def test_rejects_no_value(self):
        with pytest.raises(XComForMappingNotPushed):
            measure_mapped_length(None)


def test_unwrap_raw_value_reads_what_the_task_sent():
    msg = xcom_write_path._build_set_message(
        mock.Mock(
            dag_id="d", task_id="t", run_id="r", map_index=None, task=mock.Mock(returns_dag_result=True)
        ),
        "key",
        (1, 2),
        True,
    )

    raw = unwrap_raw_value(msg.value)

    assert is_raw_value(msg.value)
    assert raw.value == serialize((1, 2))
    assert raw.needs_mapped_length is True
    assert (msg.map_index, msg.dag_result) == (-1, True)


@pytest.mark.parametrize(
    "value",
    [
        None,
        1,
        "x",
        [1],
        {"a": 1},
        {"__airflow_exp_supervisor_write__": 1, "b": 2},
        {"__airflow_exp_supervisor_write__": 1},
        {"__airflow_exp_supervisor_write__": {"value": 1}},
        {"__airflow_exp_supervisor_write__": {"value": 1, "needs_mapped_length": "yes"}},
    ],
)
def test_is_raw_value_ignores_what_a_task_stored_itself(value):
    assert not is_raw_value(value)


def _build_dag(topology: str) -> DAG:
    with DAG(dag_id="exp_dag", start_date=datetime(2024, 1, 1, tzinfo=UTC)) as dag:

        @task
        def extract():
            return [1, 2, 3]

        @task
        def other():
            return [4, 5]

        @task
        def transform(x):
            return x

        @task
        def reduce(values):
            return len(list(values))

        match topology:
            case "plain":
                transform(extract())
            case "expand":
                transform.expand(x=extract())
            case "expand_kwargs":
                transform.expand_kwargs(extract())
            case "map":
                transform.expand(x=extract().map(lambda v: v))
            case "zip":
                transform.expand(x=extract().zip(other()))
            case "concat":
                transform.expand(x=extract().concat(other()))
            case "mapped_task_group":

                @task_group
                def group(x):
                    transform(x)

                group.expand(x=extract())
            case "reduce":
                reduce(transform.expand(x=extract()))
    return dag


_TOPOLOGIES = [
    "plain",
    "expand",
    "expand_kwargs",
    "map",
    "zip",
    "concat",
    "mapped_task_group",
    "reduce",
]

_VALUES = {
    "list": [1, "a", {"k": 2}],
    "empty-list": [],
    "dict": {"a": 1, "b": 2},
    "tuple": (1, 2, 3),
    "namedtuple": Point(1, 2),
    "nested": [[1, 2], [3]],
    "datetimes": [datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 2, tzinfo=UTC)],
    "tuple-of-datetimes": (datetime(2024, 1, 1, tzinfo=UTC),),
    "str": "abc",
    "int": 5,
    "none": None,
    "set": {1, 2},
    "datetime": datetime(2024, 1, 1, tzinfo=UTC),
}


_MAPPABLE_VALUES = {
    "list",
    "empty-list",
    "dict",
    "tuple",
    "namedtuple",
    "nested",
    "datetimes",
    "tuple-of-datetimes",
}


def _run_push(
    create_runtime_ti, make_supervisor, connect, *, dag: DAG, task_id: str, value, xcom, flag: bool
) -> dict:
    """Run ``_push_xcom_if_needed`` for ``task_id`` end to end, and report what reached the API server."""
    operator = dag.task_dict[task_id]
    is_mapped = bool(operator.is_mapped)
    ti = create_runtime_ti(task=operator, dag_id=dag.dag_id, map_index=0 if is_mapped else -1, ti_id=_TI_ID)
    ti.is_mapped = is_mapped
    if is_mapped:
        # The runtime replaces a mapped operator with the unmapped one it renders before the task runs.
        ti.task = operator.unmap({"op_kwargs": {"x": 1}})
    proc = make_supervisor(xcom, ti.id, task_id, map_index=ti.map_index)
    connect(proc, flag=flag)

    error = None
    try:
        _push_xcom_if_needed(value, ti, mock.MagicMock())
    except Exception as e:
        error = (type(e).__name__, str(e))
    return {
        "error": error,
        "api": [(c.args, {k: v for k, v in c.kwargs.items()}) for c in proc.client.xcoms.set.call_args_list],
        "stored": dict(_PointerXCom.stored),
    }


@pytest.mark.parametrize("xcom", [BaseXCom, _PointerXCom], ids=["default-backend", "custom-backend"])
@pytest.mark.parametrize("value_id", list(_VALUES))
@pytest.mark.parametrize("topology", _TOPOLOGIES)
def test_supervisor_path_writes_what_the_task_process_writes(
    create_runtime_ti, make_supervisor, connect, topology, value_id, xcom
):
    """The API server sees the same XCom row and map length, and the backend the same value."""
    results = {}
    for flag in (False, True):
        dag = _build_dag(topology)
        for task_id in dag.task_dict:
            _PointerXCom.stored.clear()
            results[flag, task_id] = _run_push(
                create_runtime_ti,
                make_supervisor,
                connect,
                dag=dag,
                task_id=task_id,
                value=_VALUES[value_id],
                xcom=xcom,
                flag=flag,
            )

    dag = _build_dag(topology)
    for task_id, operator in dag.task_dict.items():
        assert results[True, task_id] == results[False, task_id], task_id
        # Both modes must not agree on a harness failure: only a producer fails, and only on a bad value.
        is_producer = not operator.is_mapped and next(operator.iter_mapped_dependants(), None) is not None
        rejected = is_producer and value_id not in _MAPPABLE_VALUES
        assert (results[False, task_id]["error"] is not None) is rejected, task_id
        assert len(results[False, task_id]["api"]) == (0 if rejected or _VALUES[value_id] is None else 1)


@pytest.mark.parametrize(
    ("topology", "producers"),
    [
        pytest.param("plain", set(), id="plain"),
        pytest.param("expand", {"extract"}, id="expand"),
        pytest.param("expand_kwargs", {"extract"}, id="expand_kwargs"),
        pytest.param("map", {"extract"}, id="map"),
        pytest.param("zip", {"extract", "other"}, id="zip"),
        pytest.param("concat", {"extract", "other"}, id="concat"),
        pytest.param("mapped_task_group", {"extract"}, id="mapped_task_group"),
        pytest.param("reduce", {"extract"}, id="reduce"),
    ],
)
def test_supervisor_path_records_the_map_length_for_the_producers_only(
    create_runtime_ti, make_supervisor, connect, topology, producers
):
    dag = _build_dag(topology)
    lengths = {}
    for task_id in dag.task_dict:
        result = _run_push(
            create_runtime_ti,
            make_supervisor,
            connect,
            dag=dag,
            task_id=task_id,
            value=[1, 2, 3],
            xcom=BaseXCom,
            flag=True,
        )
        (call,) = result["api"]
        lengths[task_id] = call[1]["mapped_length"]

    assert {t for t, length in lengths.items() if length is not None} == producers
    assert all(length == 3 for length in lengths.values() if length is not None)


class TestTaskProcess:
    @pytest.fixture
    def push_ti(self, create_runtime_ti, mock_supervisor_comms):
        def make(**kwargs):
            return create_runtime_ti(task=BaseOperator(task_id="push_task", **kwargs))

        return make

    def test_flag_off_runs_the_backend_in_the_task_process(self, push_ti, monkeypatch, mock_supervisor_comms):
        monkeypatch.delenv(ENV_FLAG, raising=False)
        ti = push_ti()

        with mock.patch.object(task_runner.XCom, "_push", autospec=True) as push:
            _push_xcom_if_needed({"a": 1}, ti, mock.MagicMock())

        push.assert_called_once_with(ti, "return_value", {"a": 1}, dag_result=False, mapped_length=None)
        mock_supervisor_comms.send.assert_not_called()

    def test_flag_on_sends_the_value_without_running_the_backend(
        self, push_ti, monkeypatch, mock_supervisor_comms
    ):
        monkeypatch.setenv(ENV_FLAG, "1")
        ti = push_ti()

        with mock.patch.object(task_runner.XCom, "_push", autospec=True) as push:
            _push_xcom_if_needed({"a": 1}, ti, mock.MagicMock())

        push.assert_not_called()
        (sent,) = [c.args[0] for c in mock_supervisor_comms.send.call_args_list]
        assert isinstance(sent, SetXCom)
        assert is_raw_value(sent.value)
        assert unwrap_raw_value(sent.value) == xcom_write_path.RawXCom({"a": 1}, False)
        assert sent.mapped_length is None

    def test_flag_on_does_not_measure_in_the_task_process(
        self, create_runtime_ti, mock_supervisor_comms, monkeypatch
    ):
        monkeypatch.setenv(ENV_FLAG, "1")
        dag = _build_dag("expand")
        ti = create_runtime_ti(task=dag.task_dict["extract"], dag_id=dag.dag_id)

        with mock.patch("airflow.sdk.definitions.mappedoperator.is_mappable_value") as is_mappable:
            _push_xcom_if_needed([1, 2, 3], ti, mock.MagicMock())

        is_mappable.assert_not_called()
        sent = mock_supervisor_comms.send.call_args.args[0]
        assert sent.mapped_length is None
        assert unwrap_raw_value(sent.value).needs_mapped_length is True

    def test_flag_on_pushes_each_multiple_output_and_the_return_value(
        self, push_ti, monkeypatch, mock_supervisor_comms
    ):
        monkeypatch.setenv(ENV_FLAG, "1")
        ti = push_ti(multiple_outputs=True)

        _push_xcom_if_needed({"k1": 1, "k2": 2}, ti, mock.MagicMock())

        sent = {
            c.args[0].key: unwrap_raw_value(c.args[0].value).value
            for c in mock_supervisor_comms.send.call_args_list
        }
        assert sent == {"k1": 1, "k2": 2, "return_value": {"k1": 1, "k2": 2}}

    def test_flag_on_sends_xcom_push_of_a_task_for_the_supervisor_to_store(
        self, push_ti, monkeypatch, mock_supervisor_comms
    ):
        monkeypatch.setenv(ENV_FLAG, "1")
        ti = push_ti()

        ti.xcom_push("custom", [1, 2])

        sent = mock_supervisor_comms.send.call_args.args[0]
        assert (sent.key, sent.task_id) == ("custom", "push_task")
        assert unwrap_raw_value(sent.value) == xcom_write_path.RawXCom([1, 2], False)

    def test_flag_on_keeps_control_plane_keys_off_the_backend(
        self, push_ti, monkeypatch, mock_supervisor_comms
    ):
        monkeypatch.setenv(ENV_FLAG, "1")
        ti = push_ti()

        task_runner._xcom_push(ti, "skipmixin_key", {"skipped": ["a"]})

        sent = mock_supervisor_comms.send.call_args.args[0]
        assert not is_raw_value(sent.value)
        assert sent.value == {"skipped": ["a"]}

    @pytest.mark.asyncio
    async def test_flag_on_sends_an_async_push_for_the_supervisor_to_store(
        self, push_ti, monkeypatch, mock_supervisor_comms
    ):
        monkeypatch.setenv(ENV_FLAG, "1")
        ti = push_ti()

        await task_runner._axcom_push(ti, "custom", [1, 2])

        sent = mock_supervisor_comms.asend.call_args.args[0]
        assert unwrap_raw_value(sent.value) == xcom_write_path.RawXCom([1, 2], False)

    def test_flag_on_does_not_hide_other_supervisor_errors(self, push_ti, monkeypatch, mock_supervisor_comms):
        monkeypatch.setenv(ENV_FLAG, "1")
        error = AirflowRuntimeError(
            ErrorResponse(error=ErrorType.XCOM_BACKEND_ERROR, detail={"message": "boom"})
        )
        mock_supervisor_comms.send.side_effect = error

        with pytest.raises(AirflowRuntimeError) as raised:
            _push_xcom_if_needed(1, push_ti(), mock.MagicMock())

        assert raised.value is error


class TestSupervisor:
    @staticmethod
    def _raw_set_xcom(value, *, needs_mapped_length=False, task_id="task_1", map_index=None) -> SetXCom:
        body = {"value": serialize(value), "needs_mapped_length": needs_mapped_length}
        return SetXCom(
            key="return_value",
            value={"__airflow_exp_supervisor_write__": body},  # type: ignore[dict-item]
            dag_id="exp_dag",
            run_id="test_run",
            task_id=task_id,
            map_index=map_index,
        )

    @staticmethod
    def _api_calls(proc) -> list:
        return [(c.args, c.kwargs) for c in proc.client.xcoms.set.call_args_list]

    def test_a_plain_set_xcom_is_forwarded_as_it_is(self, make_supervisor):
        ti_id = uuid7()
        proc = make_supervisor(_PointerXCom, ti_id, "task_1")
        msg = SetXCom(key="k", value="ptr://elsewhere", dag_id="exp_dag", run_id="test_run", task_id="task_1")

        proc._handle_request(msg, log, 1)

        assert self._api_calls(proc) == [
            (
                ("exp_dag", "test_run", "task_1", "k", "ptr://elsewhere", None),
                {"dag_result": False, "mapped_length": None},
            )
        ]
        assert proc._xcom_writer._helper.calls == []

    def test_the_default_backend_stores_the_encoded_value_and_the_length(self, make_supervisor):
        proc = make_supervisor(BaseXCom, uuid7(), "task_1")

        proc._handle_request(self._raw_set_xcom((1, 2, 3), needs_mapped_length=True), log, 1)

        assert self._api_calls(proc) == [
            (
                ("exp_dag", "test_run", "task_1", "return_value", serialize((1, 2, 3)), None),
                {"dag_result": False, "mapped_length": 3},
            )
        ]
        assert proc._xcom_writer._helper.calls == []

    def test_a_custom_backend_stores_the_value_and_the_row_holds_what_it_returned(self, make_supervisor):
        ti_id = uuid7()
        proc = make_supervisor(_PointerXCom, ti_id, "task_1")

        proc._handle_request(self._raw_set_xcom([1, 2, 3], needs_mapped_length=True), log, 1)

        pointer = f"ptr://task_1/{ti_id}/return_value"
        assert _PointerXCom.stored == {pointer: [1, 2, 3]}
        assert self._api_calls(proc) == [
            (
                ("exp_dag", "test_run", "task_1", "return_value", pointer, None),
                {"dag_result": False, "mapped_length": 3},
            )
        ]

    def test_the_length_is_that_of_the_value_and_not_of_the_pointer(self, make_supervisor):
        proc = make_supervisor(_PointerXCom, uuid7(), "task_1")

        proc._handle_request(self._raw_set_xcom(["a"] * 40, needs_mapped_length=True), log, 1)

        assert self._api_calls(proc)[0][1]["mapped_length"] == 40

    def test_no_length_is_recorded_when_nothing_maps_over_the_value(self, make_supervisor):
        proc = make_supervisor(BaseXCom, uuid7(), "task_1")

        proc._handle_request(self._raw_set_xcom([1, 2, 3]), log, 1)

        assert self._api_calls(proc)[0][1]["mapped_length"] is None

    def test_another_task_instances_xcom_has_no_task_instance_id(self, make_supervisor):
        proc = make_supervisor(_PointerXCom, uuid7(), "task_1")

        proc._handle_request(self._raw_set_xcom(1, task_id="other"), log, 1)

        assert list(_PointerXCom.stored) == ["ptr://other/None/return_value"]

    @pytest.mark.parametrize("xcom", [BaseXCom, _PointerXCom], ids=["default-backend", "custom-backend"])
    @pytest.mark.parametrize(
        ("value", "exception_type", "message"),
        [
            pytest.param(None, "XComForMappingNotPushed", "did not push XCom for task mapping", id="absent"),
            pytest.param("abc", "UnmappableXComTypePushed", "unmappable return type 'str'", id="str"),
            pytest.param({1, 2}, "UnmappableXComTypePushed", "unmappable return type 'set'", id="set"),
        ],
    )
    def test_a_value_that_cannot_be_mapped_over_fails_the_task_and_is_not_stored(
        self, make_supervisor, xcom, value, exception_type, message
    ):
        proc = make_supervisor(xcom, uuid7(), "task_1")

        proc._handle_request(self._raw_set_xcom(value, needs_mapped_length=True), log, 1)

        error = _sent_error(proc)
        assert error["detail"]["exception_type"] == exception_type
        assert error["detail"]["message"] == message
        assert self._api_calls(proc) == []
        assert _PointerXCom.stored == {}
        assert proc._xcom_writer._helper.calls == []

    def test_a_value_the_backend_cannot_take_fails_the_request_and_is_not_sent_to_the_api(
        self, make_supervisor
    ):
        proc = make_supervisor(_PointerXCom, uuid7(), "task_1")
        not_allowed = {"__classname__": "not.importable.Thing", "__version__": 1, "__data__": {}}
        msg = self._raw_set_xcom(1)
        msg = msg.model_copy(
            update={
                "value": {
                    "__airflow_exp_supervisor_write__": {"value": not_allowed, "needs_mapped_length": False}
                }
            }
        )

        proc._handle_request(msg, log, 1)

        assert self._api_calls(proc) == []
        error = _sent_error(proc)
        assert error["error"] == ErrorType.XCOM_BACKEND_ERROR.value
        assert "not.importable.Thing" in error["detail"]["message"]

    def test_the_task_waits_for_the_helper_before_it_gets_an_answer(self, make_supervisor):
        helper = _DeferredHelper(_PointerXCom)
        proc = make_supervisor(_PointerXCom, uuid7(), "task_1", helper=helper)

        proc._handle_request(self._raw_set_xcom([1, 2], needs_mapped_length=True), log, 1)

        proc.stdin.sendall.assert_not_called()  # type: ignore[attr-defined]
        assert self._api_calls(proc) == []

        helper.answer_next()

        assert len(self._api_calls(proc)) == 1
        proc.stdin.sendall.assert_called_once()  # type: ignore[attr-defined]

    def test_wait_stops_the_helper(self, make_supervisor, mocker):
        helper = mocker.create_autospec(_InProcessHelper, instance=True)
        proc = make_supervisor(_PointerXCom, uuid7(), "task_1", helper=helper)
        mocker.patch.object(ActivitySubprocess, "_monitor_subprocess")
        mocker.patch.object(ActivitySubprocess, "update_task_state_if_needed")
        mocker.patch.object(ActivitySubprocess, "_upload_logs")

        proc.wait()

        helper.stop.assert_called_once()
        assert proc._xcom_writer._helper is None


@pytest.mark.parametrize("flag", [False, True], ids=["flag-off", "flag-on"])
def test_a_custom_backend_in_both_modes_stores_a_dataclass_it_can_import(
    create_runtime_ti, make_supervisor, connect, flag
):
    """Both modes agree on a class that the supervisor can import and is allowed to deserialize."""
    result = _run_push(
        create_runtime_ti,
        make_supervisor,
        connect,
        dag=_build_dag("plain"),
        task_id="extract",
        value={"when": datetime(2024, 1, 1, tzinfo=UTC)},
        xcom=_PointerXCom,
        flag=flag,
    )

    assert result["error"] is None
    assert list(result["stored"].values()) == [{"when": datetime(2024, 1, 1, tzinfo=UTC)}]


@pytest.fixture
def deployment_allow_list():
    """Allow only SDK classes to be deserialized, as a deployment that has not listed its own classes."""
    from airflow.sdk import serde

    def clear_caches():
        for cached in (
            serde._get_patterns,
            serde._get_regexp_patterns,
            serde._match_glob,
            serde._match_regexp,
        ):
            cached.cache_clear()

    with conf_vars({("core", "allowed_deserialization_classes"): "airflow.sdk.*"}):
        clear_caches()
        yield
    clear_caches()


class TestKnownDivergences:
    """Where the supervisor path cannot match the task process, kept as tests so that they stay visible."""

    @pytest.mark.usefixtures("deployment_allow_list")
    def test_a_class_the_supervisor_may_not_deserialize_fails_a_custom_backend(
        self, create_runtime_ti, make_supervisor, connect
    ):
        """The task process hands its backend the object, the supervisor only has the encoded value."""
        outcomes = {}
        for flag in (False, True):
            outcomes[flag] = _run_push(
                create_runtime_ti,
                make_supervisor,
                connect,
                dag=_build_dag("plain"),
                task_id="extract",
                value=Order(7),
                xcom=_PointerXCom,
                flag=flag,
            )

        assert outcomes[False]["error"] is None
        assert list(outcomes[False]["stored"].values()) == [Order(7)]
        assert outcomes[True]["error"] is not None
        assert outcomes[True]["error"][0] == "AirflowRuntimeError"
        assert "allow list" in outcomes[True]["error"][1]
        assert outcomes[True]["api"] == []

    def test_an_unserializable_value_fails_before_the_supervisor_can_say_it_is_unmappable(
        self, create_runtime_ti, make_supervisor, connect
    ):
        outcomes = {}
        for flag in (False, True):
            outcomes[flag] = _run_push(
                create_runtime_ti,
                make_supervisor,
                connect,
                dag=_build_dag("expand"),
                task_id="extract",
                value=b"bytes",
                xcom=BaseXCom,
                flag=flag,
            )

        assert outcomes[False]["error"] == ("UnmappableXComTypePushed", "unmappable return type 'bytes'")
        assert outcomes[True]["error"][0] == "TypeError"
        assert "Cannot serialize" in outcomes[True]["error"][1]

    def test_the_unmappable_error_restores_the_exception_with_the_value(
        self, mock_supervisor_comms, monkeypatch, create_runtime_ti
    ):
        monkeypatch.setenv(ENV_FLAG, "1")
        mock_supervisor_comms.send.side_effect = AirflowRuntimeError(
            ErrorResponse(detail={"exception_type": "UnmappableXComTypePushed", "message": "x"})
        )
        ti = create_runtime_ti(task=BaseOperator(task_id="t"))

        with pytest.raises(UnmappableXComTypePushed) as raised:
            xcom_write_path.push(ti, "return_value", "abc", needs_mapped_length=True)

        assert raised.value.args == ("abc",)


class _FileXCom(BaseXCom):
    """Writes each value to a file and keeps the path in the XCom row, as an object storage backend does."""

    directory: ClassVar[Path]

    @classmethod
    def store_value(cls, coordinate, key, value):
        path = cls.directory / f"{coordinate.task_id}.{key}.{os.getpid()}.json"
        path.write_text(json.dumps(value))
        return str(path)


_EXP_DAG = """
from airflow.sdk import DAG, task

with DAG(dag_id="exp_write_path", schedule=None):

    @task
    def extract():
        return [10, 20, 30]

    @task
    def bad_extract():
        return "not a list"

    @task
    def transform(x):
        return x

    transform.expand(x=extract())
    transform.expand(x=bad_extract())
"""


@pytest.mark.usefixtures("disable_capturing")
class TestSuperviseWithProcesses:
    """Run the task and the XCom backend in real processes, with the API server mocked."""

    @pytest.fixture
    def run_task(self, tmp_path, monkeypatch, make_ti_context):
        (tmp_path / "exp_dag.py").write_text(_EXP_DAG)
        bundle_info = BundleInfo(name="my-bundle", version=None)

        def run(task_id: str, *, flag: bool, xcom: type[BaseXCom]):
            _FileXCom.directory = tmp_path
            monkeypatch.setattr(xcom_module, "XCom", xcom)
            monkeypatch.setattr(task_runner, "XCom", xcom)
            client = mock.MagicMock(spec=sdk_client.Client)
            client.task_instances.start.return_value = make_ti_context()
            ti = TaskInstance(
                id=uuid7(),
                task_id=task_id,
                dag_id="exp_write_path",
                run_id="run_1",
                try_number=1,
                dag_version_id=uuid7(),
                queue="default",
            )
            env = {**local_dag_bundle_cfg(tmp_path), ENV_FLAG: "1" if flag else "0"}
            with mock.patch.dict(os.environ, env):
                exit_code = supervise_task(
                    ti=ti,
                    dag_rel_path="exp_dag.py",
                    token="",
                    server="",
                    dry_run=True,
                    client=client,
                    bundle_info=bundle_info,
                )
            return exit_code, client

        return run

    @pytest.mark.parametrize("flag", [False, True], ids=["flag-off", "flag-on"])
    def test_default_backend_records_the_value_and_length(self, run_task, flag):
        exit_code, client = run_task("extract", flag=flag, xcom=BaseXCom)

        assert exit_code == 0
        client.xcoms.set.assert_called_once_with(
            "exp_write_path",
            "run_1",
            "extract",
            "return_value",
            [10, 20, 30],
            -1,
            dag_result=False,
            mapped_length=3,
        )

    @pytest.mark.parametrize("flag", [False, True], ids=["flag-off", "flag-on"])
    def test_custom_backend_stores_the_value_and_records_the_pointer_and_length(
        self, run_task, tmp_path, flag
    ):
        exit_code, client = run_task("extract", flag=flag, xcom=_FileXCom)

        assert exit_code == 0
        ((args, kwargs),) = [(c.args, c.kwargs) for c in client.xcoms.set.call_args_list]
        pointer = Path(args[4])
        assert pointer.parent == tmp_path
        assert json.loads(pointer.read_text()) == [10, 20, 30]
        assert args[:4] == ("exp_write_path", "run_1", "extract", "return_value")
        assert kwargs == {"dag_result": False, "mapped_length": 3}
        task_pid = client.task_instances.start.call_args.args[1]
        writer_pid = int(pointer.name.split(".")[-2])
        assert (writer_pid == task_pid) is (not flag)
        assert writer_pid != os.getpid()

    @pytest.mark.parametrize("flag", [False, True], ids=["flag-off", "flag-on"])
    def test_an_unmappable_value_fails_the_task_and_is_not_stored(self, run_task, flag):
        _, client = run_task("bad_extract", flag=flag, xcom=_FileXCom)

        client.xcoms.set.assert_not_called()
        client.task_instances.finish.assert_called_once()
        assert client.task_instances.finish.call_args.kwargs["state"] == TaskInstanceState.FAILED
