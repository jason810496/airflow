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
The XCom backend and the template rendering for the tasks of a language SDK runtime.

The supervisor runs the configured XCom backend on a runtime's XCom requests, and renders the template
fields of its task, in a helper process per task, so the long-lived supervisor process keeps none of the
backend's state. The process starts on the first call of either kind. A language SDK runtime cannot
render Jinja, so the helper builds the task's template context from the task's start-up details and a
placeholder task, and services ``var``, ``conn`` and ``xcom_pull`` through the supervisor. The Dag's
params, ``template_ext`` files and user-defined macros do not reach the helper, since the Execution API
sends none of them.

The supervisor services the helper from the task's selector, so a call never blocks the task's request
loop.
"""

from __future__ import annotations

import contextlib
import json
import selectors
import signal
import time
from collections import deque
from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, Literal, Union, get_args
from uuid import UUID

import attrs
import structlog
from pydantic import BaseModel, Field, JsonValue, TypeAdapter

from airflow.sdk.api.datamodels._generated import TaskInstance, TIRunContext  # noqa: TC001
from airflow.sdk.bases.xcom import BaseXCom, XComCoordinate
from airflow.sdk.execution_time.comms import (
    ErrorResponse,
    GetConnection,
    GetDagRunState,
    GetDRCount,
    GetPreviousDagRun,
    GetPreviousTI,
    GetPrevSuccessfulDagRun,
    GetTaskStates,
    GetTICount,
    GetVariable,
    GetVariableKeys,
    GetXCom,
    GetXComCount,
    GetXComSequenceItem,
    GetXComSequenceSlice,
    MaskSecret,
    ToSupervisor,
    ToTask,
    XComResult,
)
from airflow.sdk.execution_time.supervisor import (
    ResponseSent,
    WatchedSubprocess,
    register_request_method,
    task_process_uses_exec,
)

if TYPE_CHECKING:
    from socket import socket
    from typing import Self

    from structlog.typing import FilteringBoundLogger

    from airflow.sdk.api.client import Client
    from airflow.sdk.definitions.dag import DAG

log: FilteringBoundLogger = structlog.get_logger(logger_name="xcom_backend")

_EXIT_TIMEOUT = 10.0


class TemplateRenderError(RuntimeError):
    """
    Rendering the template fields failed.

    For an exception raised in the helper process, the message holds its type and message, since the
    exception itself does not cross the process boundary.
    """


class XComBackendError(RuntimeError):
    """
    An XCom backend call failed.

    For an exception raised in the helper process, the message holds its type and message, since the
    exception itself does not cross the process boundary.
    """


class _Coordinate(BaseModel):
    dag_id: str
    run_id: str
    task_id: str
    ti_id: UUID | None = None
    region_id: UUID | None = None
    region_index: int | None = None


class XComBackendCall(BaseModel):
    """
    A call of an XCom backend hook, sent to the helper process.

    ``value`` is a value as the runtime sends it for ``store``, the XCom row for ``load`` and ``purge``,
    and a list of rows for ``load_many``.
    """

    hook: Literal["store", "load", "load_many", "purge", "exit"]
    key: str = ""
    value: JsonValue = None
    coordinate: _Coordinate | None = None
    map_index: int = -1
    type: Literal["XComBackendCall"] = "XComBackendCall"

    @classmethod
    def store(cls, coordinate: XComCoordinate, map_index: int, key: str, value: JsonValue) -> Self:
        return cls(
            hook="store",
            key=key,
            value=value,
            coordinate=_Coordinate.model_validate(attrs.asdict(coordinate)),
            map_index=map_index,
        )


class XComBackendResult(BaseModel):
    """The outcome of an XCom backend call, sent by the helper process."""

    value: JsonValue = None
    error: str | None = None
    type: Literal["XComBackendResult"] = "XComBackendResult"


class RenderCall(BaseModel):
    """
    Template field values for the helper process to render, with what it needs to build the task's context.

    ``values`` are the plain JSON literals of the operator's template fields, by their Python argument name.
    """

    ti: TaskInstance
    ti_context: TIRunContext
    start_date: datetime
    values: dict[str, JsonValue]
    type: Literal["RenderCall"] = "RenderCall"


class RenderResult(BaseModel):
    """
    The outcome of a render, sent by the helper process.

    ``values`` are what the runtime receives. ``rendered_fields`` are the same values prepared for the
    task's rendered fields, which truncates and redacts them as a Python task does, with the secrets the
    helper process has seen.
    """

    values: dict[str, JsonValue] = {}
    rendered_fields: dict[str, JsonValue] = {}
    error: str | None = None
    type: Literal["RenderResult"] = "RenderResult"


class AwaitXComBackendCall(BaseModel):
    """Sent by the helper process when it is ready for its first call."""

    type: Literal["AwaitXComBackendCall"] = "AwaitXComBackendCall"


# The helper services the lazy accessors of a template context, which send any request a task process can.
# Built from the unions at run time, which mypy cannot follow, so it sees them as Any.
_ToXComBackendProcess: Any = Annotated[
    Union[(*get_args(get_args(ToTask)[0]), XComBackendCall, RenderCall)],
    Field(discriminator="type"),
]
_FromXComBackendProcess: Any = Annotated[
    Union[(*get_args(get_args(ToSupervisor)[0]), AwaitXComBackendCall, XComBackendResult, RenderResult)],
    Field(discriminator="type"),
]


def runs_in_helper(xcom: type[BaseXCom]) -> bool:
    """Whether task XComs go through the helper process, which runs every backend but the default one."""
    return xcom is not BaseXCom


def send_unless_exited(
    process: WatchedSubprocess,
    msg: BaseModel | None,
    request_id: int,
    error: ErrorResponse | None = None,
    **dump_opts: bool,
) -> bool:
    """Send ``msg`` to ``process``, and return False instead once the process has exited."""
    if process.stdin not in process._open_sockets:
        return False
    try:
        process.send_msg(msg, request_id=request_id, error=error, **dump_opts)
    except (BrokenPipeError, ConnectionResetError):
        return False
    return True


_PendingCall = tuple[XComBackendCall | RenderCall, Callable[[Any], None]]


def _build_error_result(call: XComBackendCall | RenderCall, error: str) -> XComBackendResult | RenderResult:
    """Return the result ``call`` gets when it fails with ``error``."""
    return RenderResult(error=error) if isinstance(call, RenderCall) else XComBackendResult(error=error)


@attrs.define(kw_only=True)
class XComBackendProcess(WatchedSubprocess):
    """
    The helper process of a language SDK runtime's task, which runs XCom backend calls and renders template fields.

    The process asks for a call, runs it, and answers with its result, which is also how it asks for the
    next call. Its lookups, such as the backend's Connection and Variable lookups, reach the API server
    through ``client``.
    """

    client: Client
    decoder: ClassVar[TypeAdapter] = TypeAdapter(_FromXComBackendProcess)

    _call_request_id: int | None = attrs.field(default=None, init=False)
    _queued_calls: deque[_PendingCall] = attrs.field(factory=deque, init=False)
    _running_call: _PendingCall | None = attrs.field(default=None, init=False)
    _stopping: bool = attrs.field(default=False, init=False)

    @classmethod
    def start(  # type: ignore[override]
        cls,
        *,
        id: UUID,
        client: Client,
        logger: FilteringBoundLogger,
        selector: selectors.BaseSelector,
    ) -> Self:
        return super().start(
            id=id,
            client=client,
            # The selector of the task this process serves, so that the task's request loop services both.
            selector=selector,
            logger=logger,
            target=run_xcom_backend,
            use_exec=task_process_uses_exec(),
        )

    @property
    def can_run_a_call(self) -> bool:
        """Whether the process can still be sent a call, which a process that has exited cannot."""
        return self.stdin in self._open_sockets

    def send_call(self, call: XComBackendCall, finish: Callable[[XComBackendResult], None]) -> None:
        """
        Send ``call`` to the process, which answers it by calling ``finish`` with its result.

        The process runs one call at a time, so a call waits its turn. ``finish`` also takes the error of
        a call the process never answers, such as when it exits while the call is waiting.
        """
        self._queued_calls.append((call, finish))
        self._send_next_call()

    def send_render(self, call: RenderCall, finish: Callable[[RenderResult], None]) -> None:
        """Send ``call`` to the process, which answers it like :meth:`send_call`, with a render result."""
        self._queued_calls.append((call, finish))
        self._send_next_call()

    def _send_next_call(self) -> None:
        """Hand the process its oldest waiting call, or ``exit`` once stopping, if it asks and runs none."""
        if self._call_request_id is None or self._running_call is not None:
            return
        if self._stopping:
            request_id, self._call_request_id = self._call_request_id, None
            send_unless_exited(self, XComBackendCall(hook="exit"), request_id)
            return
        if not self._queued_calls:
            return
        self._running_call = self._queued_calls.popleft()
        call = self._running_call[0]
        request_id, self._call_request_id = self._call_request_id, None
        if not send_unless_exited(self, call, request_id):
            self._fail_waiting_calls("The XCom backend process exited before it was sent a call")

    def _fail_waiting_calls(self, error: str) -> None:
        """Answer every call the process has not answered with ``error``."""
        running, self._running_call = self._running_call, None
        if running is not None:
            running[1](_build_error_result(running[0], error))
        self._fail_queued_calls(error)

    def _fail_queued_calls(self, error: str) -> None:
        """Answer every call that has not been sent to the process with ``error``."""
        queued = list(self._queued_calls)
        self._queued_calls.clear()
        for call, finish in queued:
            finish(_build_error_result(call, error))

    def stop(self) -> None:
        """Let the helper process exit, and kill it if it does not exit in time."""
        self._stopping = True
        # A queued call must not run after its task is over.
        self._fail_queued_calls("The XCom backend process was stopped")
        deadline = time.monotonic() + _EXIT_TIMEOUT
        if self.can_run_a_call:
            if self._exit_code is None:
                self._send_next_call()
            # The selector is shared with the task, so it is serviced only while this process is alive.
            while self._open_sockets and (now := time.monotonic()) < deadline:
                self._service_subprocess(max_wait_time=deadline - now)
        if self._exit_code is None:
            with contextlib.suppress(self._process.TimeoutExpired):
                self._exit_code = self._process.wait(timeout=max(0.0, deadline - time.monotonic()))
        if self._exit_code is None:
            self.kill(signal.SIGTERM, force=True)
        # The selector belongs to the task.
        for sock in list(self._open_sockets):
            self._on_socket_closed(sock)
            sock.close()
        self._fail_waiting_calls("The XCom backend process was stopped")

    def _on_socket_closed(self, sock: socket) -> None:
        super()._on_socket_closed(sock)
        if sock is self.stdin:
            # The process has exited, or is exiting.
            self._fail_waiting_calls("The XCom backend process exited before it answered")

    def _handle_await_call(
        self, msg: AwaitXComBackendCall, log: FilteringBoundLogger, req_id: int
    ) -> ResponseSent:
        self._call_request_id = req_id
        self._send_next_call()
        return ResponseSent.ALREADY_SENT

    def _handle_result(
        self, msg: XComBackendResult | RenderResult, log: FilteringBoundLogger, req_id: int
    ) -> ResponseSent:
        self._call_request_id = req_id
        running, self._running_call = self._running_call, None
        self._send_next_call()
        if running is not None:
            running[1](msg)
        return ResponseSent.ALREADY_SENT

    _request_handlers = {
        **WatchedSubprocess._get_shared_request_handlers(
            GetConnection,
            GetDRCount,
            GetDagRunState,
            GetPrevSuccessfulDagRun,
            GetPreviousDagRun,
            GetPreviousTI,
            GetTICount,
            GetTaskStates,
            GetVariable,
            GetVariableKeys,
            GetXCom,
            GetXComCount,
            GetXComSequenceItem,
            GetXComSequenceSlice,
            MaskSecret,
        ),
        **dict(
            [
                register_request_method(AwaitXComBackendCall, _handle_await_call),
                register_request_method(XComBackendResult, _handle_result),
                register_request_method(RenderResult, _handle_result),
            ]
        ),
    }


def run_xcom_backend() -> None:
    """Run the XCom backend calls and renders that the supervisor sends, until it sends ``exit``."""
    from airflow.sdk.execution_time import task_runner
    from airflow.sdk.execution_time.comms import CommsDecoder
    from airflow.sdk.execution_time.xcom import XCom

    comms = CommsDecoder[BaseModel, BaseModel](log=log, body_decoder=TypeAdapter(_ToXComBackendProcess))
    # The secrets backends and the accessors of a template context look up Connections, Variables and
    # XComs through SUPERVISOR_COMMS.
    task_runner.SUPERVISOR_COMMS = comms  # type: ignore[assignment]
    runner = _HookRunner(XCom)
    call = comms.send(AwaitXComBackendCall())
    while True:
        if isinstance(call, RenderCall):
            result: BaseModel = render_call(call)
        elif isinstance(call, XComBackendCall) and call.hook != "exit":
            result = runner.run(call)
        else:
            break
        call = comms.send(result)


@attrs.define
class _HookRunner:
    """Runs the hooks of an XCom backend that the supervisor's calls name, one method per hook."""

    xcom: type[BaseXCom]

    def run(self, call: XComBackendCall) -> XComBackendResult:
        try:
            if (hook := getattr(self, call.hook, None)) is None:
                raise ValueError(f"Unknown XCom backend hook {call.hook!r}")
            return hook(call)
        except Exception as e:
            log.exception("XCom backend call failed", hook=call.hook, key=call.key)
            return XComBackendResult(error=f"{type(e).__name__}: {e}")

    def store(self, call: XComBackendCall) -> XComBackendResult:
        from airflow.sdk.serde import deserialize

        if call.coordinate is None:
            raise ValueError("A store call needs the coordinate of the XCom")
        # No fallback to the XCom row, which would put the payload in the database.
        value = deserialize(call.value)  # type: ignore[type-var]
        coordinate = XComCoordinate(**call.coordinate.model_dump())
        return XComBackendResult(value=self.xcom._store(coordinate, call.map_index, call.key, value))

    def load(self, call: XComBackendCall) -> XComBackendResult:
        return XComBackendResult(value=self._load_row(call.key, call.value))

    def load_many(self, call: XComBackendCall) -> XComBackendResult:
        rows = call.value if isinstance(call.value, list) else []
        return XComBackendResult(value=[self._load_row(call.key, row) for row in rows])

    def purge(self, call: XComBackendCall) -> XComBackendResult:
        self.xcom.purge(XComResult(key=call.key, value=call.value))
        return XComBackendResult()

    def _load_row(self, key: str, row: JsonValue) -> JsonValue:
        """Return what the runtime receives for ``row``: what the default backend would keep in the row."""
        from airflow.sdk.serde import serialize

        # No fallback to the raw row, which would hide that the backend is unusable.
        return serialize(self.xcom._load(XComResult(key=key, value=row)))  # type: ignore[return-value]


def build_placeholder_dag(dag_id: str, task_id: str) -> DAG:
    """Build a Dag of one placeholder task, which is what the context of ``task_id`` is built from."""
    from airflow.sdk.definitions.dag import DAG
    from airflow.sdk.execution_time.provider_operator import _PlaceholderTask

    dag = DAG(dag_id, schedule=None)
    with dag:
        _PlaceholderTask(task_id=task_id)
    return dag


def render_call(call: RenderCall) -> RenderResult:
    """Render the values of ``call``, and report a failure as the error of the result."""
    try:
        return _render(call)
    except Exception as e:
        log.exception("Rendering the template fields failed", task_id=call.ti.task_id)
        return RenderResult(error=f"{type(e).__name__}: {e}")


def _render(call: RenderCall) -> RenderResult:
    from airflow.sdk._shared.secrets_masker import redact
    from airflow.sdk.execution_time.context import set_current_team_name
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance, _serialize_template_field

    set_current_team_name(call.ti_context.dag_run.team_name)
    ti = call.ti
    dag = build_placeholder_dag(ti.dag_id, ti.task_id)
    task = dag.task_dict[ti.task_id]
    runtime_ti = RuntimeTaskInstance.model_construct(
        **ti.model_dump(exclude_unset=True),
        task=task,
        _ti_context_from_server=call.ti_context,
        max_tries=call.ti_context.max_tries,
        start_date=call.start_date,
        state=None,
    )
    context = runtime_ti.get_template_context()

    values: dict[str, JsonValue] = {}
    rendered_fields: dict[str, JsonValue] = {}
    for name, value in call.values.items():
        rendered = task.render_template(value, context)
        values[name] = _to_json(name, rendered)
        serialized = _serialize_template_field(rendered, name)
        rendered_fields[name] = TypeAdapter(JsonValue).validate_python(redact(serialized, name))
    return RenderResult(values=values, rendered_fields=rendered_fields)


def _to_json(name: str, value: Any) -> JsonValue:
    """Return ``value`` as plain JSON, since that is all the runtime can read."""
    try:
        return json.loads(json.dumps(value, allow_nan=False))
    except (TypeError, ValueError) as e:
        raise TemplateRenderError(
            f"Template field {name!r} rendered to a {type(value).__name__} that is not JSON-serializable: {e}"
        ) from e
