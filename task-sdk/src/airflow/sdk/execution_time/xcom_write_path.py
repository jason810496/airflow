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
Experiment: write a Python task's XComs on the supervisor, as a language SDK task's are.

With ``AIRFLOW_EXP_SUPERVISOR_WRITE_PATH=1`` the task process sends the value it would have stored, encoded
only so it can cross the wire, and the supervisor runs the XCom backend and measures the map length. Without
the flag nothing in this module is used and the task process keeps its own write path.

The wire marker is an envelope inside ``SetXCom.value`` instead of a new field, so that the experiment needs
no supervisor schema version. A real change would add a field to ``SetXCom``.
"""

from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING, Any, cast

import attrs
import structlog

from airflow.sdk.exceptions import (
    AirflowRuntimeError,
    ErrorType,
    UnmappableXComTypePushed,
    XComForMappingNotPushed,
)
from airflow.sdk.execution_time.comms import ErrorResponse, SetXCom

if TYPE_CHECKING:
    from pydantic import JsonValue
    from structlog.typing import FilteringBoundLogger

    from airflow.sdk.api.client import Client
    from airflow.sdk.coordinators._xcom_backend import XComBackendProcess, XComBackendResult
    from airflow.sdk.execution_time.supervisor import ActivitySubprocess, RequestResult, ResponseSent
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

ENV_FLAG = "AIRFLOW_EXP_SUPERVISOR_WRITE_PATH"

_ENVELOPE_KEY = "__airflow_exp_supervisor_write__"
_CLASSNAME = "__classname__"
_DATA = "__data__"
_TUPLE_CLASSNAME = "builtins.tuple"

log: FilteringBoundLogger = structlog.get_logger(logger_name="xcom_write_path")


@attrs.frozen
class RawXCom:
    """What the task process sent: its value as ``serialize`` encodes it, and whether to measure it."""

    value: JsonValue
    needs_mapped_length: bool


def is_enabled() -> bool:
    return os.environ.get(ENV_FLAG) == "1"


def is_active() -> bool:
    """
    Whether this task process pushes through the supervisor.

    An in-process supervisor (``dag.test()``) has no process boundary and no helper process to run the XCom
    backend in, so it stays on the task's own write path.
    """
    if not is_enabled():
        return False
    from airflow.sdk.execution_time import task_runner
    from airflow.sdk.execution_time.comms import CommsDecoder

    return isinstance(getattr(task_runner, "SUPERVISOR_COMMS", None), CommsDecoder)


def is_raw_value(value: JsonValue) -> bool:
    if not (isinstance(value, dict) and len(value) == 1):
        return False
    body = value.get(_ENVELOPE_KEY)
    return (
        isinstance(body, dict)
        and body.keys() == {"value", "needs_mapped_length"}
        and isinstance(body["needs_mapped_length"], bool)
    )


def unwrap_raw_value(value: JsonValue) -> RawXCom:
    body: Any = value[_ENVELOPE_KEY]  # type: ignore[index,call-overload]
    return RawXCom(value=body["value"], needs_mapped_length=body["needs_mapped_length"])


def _build_set_message(ti: RuntimeTaskInstance, key: str, value: Any, needs_mapped_length: bool) -> SetXCom:
    from airflow.sdk.serde import serialize

    body: dict[str, JsonValue] = {
        "value": cast("JsonValue", serialize(value)),
        "needs_mapped_length": needs_mapped_length,
    }
    envelope: dict[str, JsonValue] = {_ENVELOPE_KEY: body}
    return SetXCom(
        key=key,
        value=envelope,
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        run_id=ti.run_id,
        map_index=ti.map_index if ti.map_index is not None else -1,
        dag_result=ti.task.returns_dag_result,
    )


def _restore_error(error: AirflowRuntimeError, value: Any) -> BaseException:
    """Return the exception a Python task raises for what the supervisor rejected, or ``error`` itself."""
    match (error.error.detail or {}).get("exception_type"):
        case "XComForMappingNotPushed":
            return XComForMappingNotPushed()
        case "UnmappableXComTypePushed":
            return UnmappableXComTypePushed(value)
    return error


def push(ti: RuntimeTaskInstance, key: str, value: Any, *, needs_mapped_length: bool = False) -> None:
    """Send ``value`` to the supervisor to store through the XCom backend."""
    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    msg = _build_set_message(ti, key, value, needs_mapped_length)
    try:
        SUPERVISOR_COMMS.send(msg)
    except AirflowRuntimeError as e:
        restored = _restore_error(e, value)
        if restored is e:
            raise
        raise restored from None


async def apush(ti: RuntimeTaskInstance, key: str, value: Any, *, needs_mapped_length: bool = False) -> None:
    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    msg = await asyncio.to_thread(_build_set_message, ti, key, value, needs_mapped_length)
    try:
        await SUPERVISOR_COMMS.asend(msg)
    except AirflowRuntimeError as e:
        restored = _restore_error(e, value)
        if restored is e:
            raise
        raise restored from None


class _Unmappable(Exception):
    """The supervisor's verdict on a value it cannot measure."""


def measure_mapped_length(encoded: JsonValue) -> int:
    """
    Return the length ``len(value)`` of the value that ``serialize`` encoded as ``encoded``.

    The same rule as ``is_mappable_value``: a list, a dict or a tuple, and nothing else. ``serialize`` keeps
    a list a list and a dict a dict, and encodes a tuple as a ``builtins.tuple`` dict, so the length is read
    off the encoding without importing the classes of the elements.

    :raises XComForMappingNotPushed: when there is no value.
    :raises _Unmappable: when the value is not a list, a dict or a tuple.
    """
    if encoded is None:
        raise XComForMappingNotPushed()
    if isinstance(encoded, list):
        return len(encoded)
    if isinstance(encoded, dict):
        if _CLASSNAME not in encoded:
            return len(encoded)
        if encoded[_CLASSNAME] == _TUPLE_CLASSNAME:
            return len(encoded[_DATA])  # type: ignore[arg-type]
        raise _Unmappable(str(encoded[_CLASSNAME]).rpartition(".")[2])
    raise _Unmappable(type(encoded).__qualname__)


def _build_error(exception_type: str, message: str) -> ErrorResponse:
    return ErrorResponse(
        error=ErrorType.GENERIC_ERROR,
        detail={"status_code": None, "message": message, "exception_type": exception_type},
    )


def _build_backend_error(exc: Exception) -> ErrorResponse:
    from airflow.sdk.api.client import ServerResponseError

    if isinstance(exc, ServerResponseError):
        return ErrorResponse.from_server_error(exc)
    return ErrorResponse(
        error=ErrorType.XCOM_BACKEND_ERROR,
        detail={"status_code": None, "message": str(exc), "exception_type": type(exc).__name__},
    )


@attrs.define
class SupervisorXComWriter:
    """
    Stores the XComs that a Python task process sent unstored, from its supervisor.

    A custom XCom backend runs in the helper process that language SDK tasks use, so the supervisor itself
    keeps none of the backend's state. The default backend stores what ``serialize`` encoded, which is what
    the task process sent, so it needs no process of its own.
    """

    proc: ActivitySubprocess
    _helper: XComBackendProcess | None = attrs.field(default=None, init=False)

    @property
    def _client(self) -> Client:
        return self.proc.client

    def write(self, msg: SetXCom, req_id: int) -> RequestResult | ResponseSent:
        from airflow.sdk.execution_time.request_handlers import handle_set_xcom
        from airflow.sdk.execution_time.supervisor import ResponseSent

        raw = unwrap_raw_value(msg.value)
        mapped_length = None
        if raw.needs_mapped_length:
            try:
                mapped_length = measure_mapped_length(raw.value)
            except XComForMappingNotPushed as e:
                self.proc.send_msg(None, request_id=req_id, error=_build_error(type(e).__name__, str(e)))
                return ResponseSent.ALREADY_SENT
            except _Unmappable as e:
                message = f"unmappable return type {str(e)!r}"
                self.proc.send_msg(
                    None, request_id=req_id, error=_build_error("UnmappableXComTypePushed", message)
                )
                return ResponseSent.ALREADY_SENT
        msg = msg.model_copy(update={"value": raw.value, "mapped_length": mapped_length})

        from airflow.sdk.coordinators._xcom_backend import XComBackendCall, runs_in_helper
        from airflow.sdk.execution_time import xcom

        if not runs_in_helper(xcom.XCom):
            return handle_set_xcom(self._client, msg)

        coordinate = self._build_coordinate(msg)
        return self._call_helper(
            XComBackendCall.store(coordinate, msg.resolved_map_index, msg.key, msg.value),
            lambda stored: handle_set_xcom(self._client, msg.model_copy(update={"value": stored.value})),
            req_id,
        )

    def _build_coordinate(self, msg: SetXCom):
        from airflow.sdk.bases.xcom import XComCoordinate, build_own_coordinate, is_own_xcom

        ti: Any = self.proc.ti
        if is_own_xcom(ti, msg.dag_id, msg.run_id, msg.task_id, msg.resolved_map_index):
            return build_own_coordinate(ti)
        return XComCoordinate(msg.dag_id, msg.run_id, msg.task_id)

    def _call_helper(
        self,
        call: Any,
        then: Any,
        req_id: int,
    ) -> ResponseSent:
        """Answer request ``req_id`` with ``then(result)`` once the helper has run ``call``."""
        from airflow.sdk.coordinators._xcom_backend import XComBackendError, send_unless_exited
        from airflow.sdk.execution_time.supervisor import ResponseSent

        def answer(resp, error, **dump_opts: bool) -> None:
            if not send_unless_exited(self.proc, resp, req_id, error, **dump_opts):
                self.proc.process_log.warning(
                    "The task exited before its XCom request was answered", request_id=req_id
                )

        def finish(result: XComBackendResult) -> None:
            try:
                if result.error is not None:
                    raise XComBackendError(result.error)
                resp, dump_opts = then(result)
            except Exception as e:
                self.proc.process_log.exception("Failed to handle the XCom backend result", request_id=req_id)
                answer(None, _build_backend_error(e))
            else:
                answer(resp, None, **dump_opts)

        self._start_helper_call(call, finish)
        return ResponseSent.ALREADY_SENT

    def _start_helper_call(self, call: Any, finish: Any) -> None:
        from airflow.sdk.coordinators._xcom_backend import XComBackendProcess

        if self._helper is not None and not self._helper.can_run_a_call:
            self.stop()
        if self._helper is None:
            self._helper = XComBackendProcess.start(
                id=self.proc.id,
                client=self._client,
                logger=self.proc.process_log,
                selector=self.proc.selector,
            )
        self._helper.send_call(call, finish)

    def stop(self) -> None:
        """Stop the helper process, so that the next call starts one that can answer it."""
        if (helper := self._helper) is not None:
            self._helper = None
            helper.stop()
