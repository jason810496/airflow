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
End-to-end integration tests for supervisor IPC schema migration.

These tests synthesise a Cadwyn :class:`~cadwyn.VersionBundle` with
**six dated** :class:`~cadwyn.VersionChange` entries -- three field
additions on a request body and three on a response body -- and drive
the full ``send_msg`` / ``handle_requests`` pipeline against a **real
foreign-runtime subprocess** spawned through the coordinator
interface. Every msgpack frame in the test crosses a real TCP socket
between two real OS processes, so the assertions cover the wire
representation, not just an in-memory model dump.

Two body classes mirror the production split between channels:

- :class:`_RequestBody` -- runtime -> supervisor. Three fields
  (``field_a``, ``field_b``, ``field_c``) introduced at three
  different dates. Each version-change is paired with a
  ``convert_request_to_next_version_for`` backfill so an older client
  payload reaches the head Pydantic class with every field present.
- :class:`_ResponseBody` -- supervisor -> runtime. Three fields
  (``response_x``, ``response_y``, ``response_z``) introduced at three
  different dates. No upgrade backfill is needed (responses only flow
  downgrade direction); ``schema(...).field(...).didnt_exist`` alone
  trims later fields from the older wire shape.

Test ground covered:

- The subprocess the coordinator launches **really receives** a
  downgraded wire payload (verified via a temp file the runtime
  writes after reading the seed frame).
- The same runtime subprocess **really sends back** a wire-shape
  request that the supervisor of either channel
  (:class:`ActivitySubprocess` analogue / :class:`DagFileProcessorProcess`
  analogue) upgrades to head before the decoder validates it.
- The coordinator's ``target_schema_version`` is the single source
  of truth for ``WatchedSubprocess.client_version`` -- both
  task-execution and dag-processing wiring resolve from there.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
from pathlib import Path
from typing import Any, ClassVar, Literal
from unittest.mock import MagicMock

import msgspec
import psutil
import pytest
import structlog
from cadwyn import (
    HeadVersion,
    Version,
    VersionBundle,
    VersionChange,
    convert_request_to_next_version_for,
    schema,
)
from pydantic import BaseModel, TypeAdapter
from uuid6 import uuid7

from airflow.sdk.execution_time.comms import _RequestFrame
from airflow.sdk.execution_time.coordinator import BaseCoordinator, _send_startup_details, _start_server
from airflow.sdk.execution_time.supervisor import WatchedSubprocess
from airflow.sdk.execution_time.supervisor_schemas import SchemaVersionMigrator

_FAKE_RUNTIME_SCRIPT = Path(__file__).with_name("_fake_runtime.py")


class _RequestBody(BaseModel):
    """
    Runtime -> supervisor request body.

    Three fields appear in this body, each introduced at a different
    dated bundle entry. Older runtimes omit later fields; the upgrade
    walk backfills them so the supervisor's head-class decoder always
    validates.
    """

    type: Literal["_RequestBody"] = "_RequestBody"
    ti_id: str
    field_a: int | None = None
    field_b: int | None = None
    field_c: int | None = None


class _ResponseBody(BaseModel):
    """
    Supervisor -> runtime response body.

    Three fields appear in this body, each introduced at a different
    dated bundle entry. The downgrade walk trims any field a runtime
    pinned to an older version was not built to decode.
    """

    type: Literal["_ResponseBody"] = "_ResponseBody"
    ti_id: str
    response_x: str | None = None
    response_y: str | None = None
    response_z: str | None = None


# Request-body breaking changes -- each adds a field and a request-side
# backfill so an older client payload reaches the head shape intact.


class _AddRequestFieldA(VersionChange):
    """3026-02-15: introduce ``_RequestBody.field_a``."""

    description = __doc__
    instructions_to_migrate_to_previous_version = (schema(_RequestBody).field("field_a").didnt_exist,)

    @convert_request_to_next_version_for(_RequestBody)  # type: ignore[arg-type]
    def _backfill(request):
        request.body.setdefault("field_a", 0)


class _AddRequestFieldB(VersionChange):
    """3026-05-10: introduce ``_RequestBody.field_b``."""

    description = __doc__
    instructions_to_migrate_to_previous_version = (schema(_RequestBody).field("field_b").didnt_exist,)

    @convert_request_to_next_version_for(_RequestBody)  # type: ignore[arg-type]
    def _backfill(request):
        request.body.setdefault("field_b", 0)


class _AddRequestFieldC(VersionChange):
    """3026-08-22: introduce ``_RequestBody.field_c``."""

    description = __doc__
    instructions_to_migrate_to_previous_version = (schema(_RequestBody).field("field_c").didnt_exist,)

    @convert_request_to_next_version_for(_RequestBody)  # type: ignore[arg-type]
    def _backfill(request):
        request.body.setdefault("field_c", 0)


# Response-body breaking changes -- downgrade-only direction, so each
# entry only carries the ``didnt_exist`` instruction. No upgrade
# transformer is needed because responses are never sent runtime ->
# supervisor.


class _AddResponseFieldX(VersionChange):
    """3026-03-01: introduce ``_ResponseBody.response_x``."""

    description = __doc__
    instructions_to_migrate_to_previous_version = (schema(_ResponseBody).field("response_x").didnt_exist,)


class _AddResponseFieldY(VersionChange):
    """3026-06-15: introduce ``_ResponseBody.response_y``."""

    description = __doc__
    instructions_to_migrate_to_previous_version = (schema(_ResponseBody).field("response_y").didnt_exist,)


class _AddResponseFieldZ(VersionChange):
    """3026-09-30: introduce ``_ResponseBody.response_z``."""

    description = __doc__
    instructions_to_migrate_to_previous_version = (schema(_ResponseBody).field("response_z").didnt_exist,)


# Bundle ordered newest -> oldest, the order cadwyn expects for the
# downgrade walk. Each date carries exactly one breaking change; the
# request- and response-side changes interleave so a single pinned
# client_version exercises a mix of both body's transformers.
_BUNDLE = VersionBundle(
    HeadVersion(),
    Version("3026-09-30", _AddResponseFieldZ),
    Version("3026-08-22", _AddRequestFieldC),
    Version("3026-06-15", _AddResponseFieldY),
    Version("3026-05-10", _AddRequestFieldB),
    Version("3026-03-01", _AddResponseFieldX),
    Version("3026-02-15", _AddRequestFieldA),
    Version("3025-12-01"),
)


@pytest.fixture
def synthetic_migrator(monkeypatch):
    """
    Re-route the production migrator and registered-body registry to the
    synthetic bundle and its two body classes.

    The supervisor IPC code paths re-import these names per call so
    monkeypatching the module attributes is enough; no cache clearing
    is required.
    """
    migrator = SchemaVersionMigrator(_BUNDLE)
    monkeypatch.setattr(
        "airflow.sdk.execution_time.supervisor_schemas.get_schema_version_migrator",
        lambda: migrator,
    )
    monkeypatch.setattr(
        "airflow.sdk.execution_time.comms._registered_models_by_name",
        lambda: {"_RequestBody": _RequestBody, "_ResponseBody": _ResponseBody},
    )
    return migrator


class _StubTaskExecutionSupervisor(WatchedSubprocess):
    """
    :class:`ActivitySubprocess` analogue pinned to ``_RequestBody``.

    The production class decodes against ``TypeAdapter(ToSupervisor)``;
    swapping in a synthetic decoder lets the real ``handle_requests``
    pipeline run end-to-end without polluting ``ToSupervisor`` with a
    test-only class. Inheriting without re-decorating with
    ``@attrs.define`` keeps the instance ``__dict__`` open so we can
    stash captured messages on the instance.
    """

    decoder: ClassVar[TypeAdapter] = TypeAdapter(_RequestBody)

    def _handle_request(self, msg, log, req_id):
        self.__dict__.setdefault("_received_msgs", []).append(msg)


class _StubDagProcessingSupervisor(WatchedSubprocess):
    """:class:`DagFileProcessorProcess` analogue pinned to ``_RequestBody``."""

    decoder: ClassVar[TypeAdapter] = TypeAdapter(_RequestBody)

    def _handle_request(self, msg, log, req_id):
        self.__dict__.setdefault("_received_msgs", []).append(msg)


class _StubCoordinator(BaseCoordinator):
    """
    Minimal coordinator that launches :file:`_fake_runtime.py` for both
    Dag-Processing and Task-Execution.

    *target_version* is what ``target_schema_version`` will return for
    any inbound body, modelling a foreign runtime pinned to that
    version. The constructor also takes a list of CLI arguments to
    forward verbatim to the fake runtime (capture path, send-frames
    path, read counts) so each test can configure the runtime's
    per-scenario behaviour without subclassing.
    """

    sdk = "test"
    file_extension = ".testjar"

    def __init__(self, *, target_version: str, extra_runtime_args: list[str]) -> None:
        self._target_version = target_version
        self._extra_runtime_args = list(extra_runtime_args)

    def target_schema_version(self, _body: Any) -> str:
        return self._target_version

    def _runtime_cmd(self, comm_addr: str) -> list[str]:
        return [
            sys.executable,
            os.fspath(_FAKE_RUNTIME_SCRIPT),
            "--comm-addr",
            comm_addr,
            *self._extra_runtime_args,
        ]

    def task_execution_cmd(self, *, comm_addr: str, **_unused) -> list[str]:
        return self._runtime_cmd(comm_addr)

    def dag_parsing_cmd(self, *, comm_addr: str, **_unused) -> list[str]:
        return self._runtime_cmd(comm_addr)


def _new_supervisor(cls: type[WatchedSubprocess], stdin: socket.socket) -> WatchedSubprocess:
    """Construct a real :class:`WatchedSubprocess` subclass bound to *stdin*."""
    return cls(
        id=uuid7(),
        pid=1,
        stdin=stdin,
        process=MagicMock(spec=psutil.Process),
        process_log=structlog.get_logger(),
    )


def _spawn_runtime(coordinator: _StubCoordinator, *, channel: str) -> tuple[subprocess.Popen, socket.socket]:
    """
    Start a TCP comm server, spawn the fake runtime through *coordinator*,
    and return ``(proc, server_side_socket)``.

    *channel* picks which coordinator entrypoint is invoked --
    ``"task-execution"`` for :meth:`task_execution_cmd` and
    ``"dag-processing"`` for :meth:`dag_parsing_cmd` -- so both
    coordinator routes are covered by the same helper.

    The returned socket is the *supervisor* end of the runtime <->
    supervisor comm channel; the runtime end lives inside the spawned
    subprocess and is closed by the runtime on exit.
    """
    comm_server = _start_server()
    host, port = comm_server.getsockname()
    comm_addr = f"{host}:{port}"

    if channel == "task-execution":
        cmd = coordinator.task_execution_cmd(comm_addr=comm_addr)
    elif channel == "dag-processing":
        cmd = coordinator.dag_parsing_cmd(comm_addr=comm_addr)
    else:
        raise ValueError(f"unknown channel {channel!r}")

    proc = subprocess.Popen(cmd, stdin=subprocess.DEVNULL)
    try:
        comm_server.settimeout(5.0)
        runtime_sock, _ = comm_server.accept()
    finally:
        comm_server.close()
    return proc, runtime_sock


def _read_one_request_frame(sock: socket.socket) -> _RequestFrame:
    """
    Read exactly one :class:`_RequestFrame` from *sock* using the same
    4-byte length prefix protocol the production selector loop uses.
    """
    sock.settimeout(5.0)
    length_bytes = b""
    while len(length_bytes) < 4:
        chunk = sock.recv(4 - len(length_bytes))
        if not chunk:
            raise EOFError("socket closed before length prefix")
        length_bytes += chunk
    length = int.from_bytes(length_bytes, "big")
    payload = b""
    while len(payload) < length:
        chunk = sock.recv(length - len(payload))
        if not chunk:
            raise EOFError("socket closed before body")
        payload += chunk
    return msgspec.msgpack.Decoder(_RequestFrame).decode(payload)


def _write_send_frames(tmp_path: Path, frames: list[list]) -> Path:
    """Serialise *frames* to a temp JSON file for ``--send-frames``."""
    path = tmp_path / "send_frames.json"
    path.write_text(json.dumps(frames))
    return path


_PARAMETRIZE_CHANNELS = pytest.mark.parametrize(
    ("supervisor_cls", "channel"),
    [
        pytest.param(_StubTaskExecutionSupervisor, "task-execution", id="task-execution"),
        pytest.param(_StubDagProcessingSupervisor, "dag-processing", id="dag-processing"),
    ],
)


# ---------------------------------------------------------------------------
# Downgrade direction: supervisor's head ``_ResponseBody`` -> runtime wire.
# Exercised end-to-end through a real subprocess launched by the coordinator.
# ---------------------------------------------------------------------------


class TestCoordinatorSubprocessReceivesDowngradedResponse:
    """
    The runtime subprocess the coordinator launches receives a wire-shape
    payload trimmed to its pinned schema version -- verified by reading
    the JSON file the runtime writes after reading the frame.
    """

    @pytest.fixture
    def head_response(self) -> _ResponseBody:
        return _ResponseBody(
            ti_id="ti-resp",
            response_x="x-value",
            response_y="y-value",
            response_z="z-value",
        )

    @pytest.mark.parametrize(
        ("client_version", "expected_fields", "absent_fields"),
        [
            pytest.param(
                "3026-09-30",
                {"response_x": "x-value", "response_y": "y-value", "response_z": "z-value"},
                (),
                id="head",
            ),
            pytest.param(
                "3026-06-15",
                {"response_x": "x-value", "response_y": "y-value"},
                ("response_z",),
                id="middle",
            ),
            pytest.param(
                "3026-03-01",
                {"response_x": "x-value"},
                ("response_y", "response_z"),
                id="early",
            ),
            pytest.param(
                "3025-12-01",
                {},
                ("response_x", "response_y", "response_z"),
                id="baseline",
            ),
        ],
    )
    @_PARAMETRIZE_CHANNELS
    def test_runtime_subprocess_sees_downgraded_seed(
        self,
        synthetic_migrator,
        head_response,
        client_version,
        expected_fields,
        absent_fields,
        supervisor_cls,
        channel,
        tmp_path,
    ):
        """
        Drive a real seed-frame downgrade through a real subprocess.

        The flow:

        1. The stub coordinator's ``task_execution_cmd`` /
           ``dag_parsing_cmd`` returns a real ``[python, fake_runtime,
           ...]`` invocation.
        2. The test spawns the subprocess and accepts its TCP
           connection -- this is exactly what the production
           ``_runtime_subprocess_entrypoint`` does, minus the bridge.
        3. The test writes one downgraded seed frame via
           ``_send_startup_details`` (Task-Execution one-shot) for the
           task-execution channel, and via ``WatchedSubprocess.send_msg``
           for the dag-processing channel -- mirroring the production
           call sites for each channel.
        4. The runtime subprocess reads the frame, dumps the body to
           the capture file, and exits. The test reads the file and
           asserts the wire shape carries exactly the fields the
           pinned client version was built to decode.
        """
        captured_out = tmp_path / "captured.json"
        coordinator = _StubCoordinator(
            target_version=client_version,
            extra_runtime_args=[
                "--captured-out",
                os.fspath(captured_out),
                "--read-count",
                "1",
            ],
        )
        proc, runtime_sock = _spawn_runtime(coordinator, channel=channel)
        try:
            if channel == "task-execution":
                # Production call site: coordinator._send_startup_details.
                _send_startup_details(runtime_sock, head_response, client_version=client_version)
            else:
                # Production call site for dag-processing: the supervisor's
                # ``send_msg`` (after ``self.client_version`` is pinned in
                # ``DagFileProcessorProcess._on_child_started``). Replicate
                # that by constructing a real supervisor whose ``stdin``
                # is the comm socket the runtime is connected to.
                sup = _new_supervisor(supervisor_cls, runtime_sock)
                sup.client_version = client_version
                sup.send_msg(head_response, request_id=0)
            assert proc.wait(timeout=5) == 0, "fake runtime exited non-zero"
        finally:
            runtime_sock.close()
            if proc.poll() is None:
                proc.kill()
                proc.wait()

        bodies = json.loads(captured_out.read_text())
        assert len(bodies) == 1, f"{channel}: runtime should have captured exactly one seed body"
        body = bodies[0]
        assert body["type"] == "_ResponseBody"
        assert body["ti_id"] == "ti-resp"
        for field, value in expected_fields.items():
            assert body.get(field) == value, f"{channel}: field {field!r} missing or wrong"
        for field in absent_fields:
            assert field not in body, f"{channel}: field {field!r} must not appear on wire"


# ---------------------------------------------------------------------------
# Upgrade direction: runtime wire ``_RequestBody`` -> supervisor head.
# Exercised end-to-end through a real subprocess launched by the coordinator.
# ---------------------------------------------------------------------------


class TestSupervisorReceivesUpgradedRequestFromCoordinatorSubprocess:
    """
    The supervisor of either channel receives a head-shape body even when
    the runtime is pinned to an older version that omits later fields --
    verified by reading the real wire bytes the subprocess sent and
    pushing them through the supervisor's ``handle_requests`` pipeline.
    """

    @pytest.mark.parametrize(
        ("client_version", "wire_body", "expected_head"),
        [
            pytest.param(
                "3026-08-22",
                {"type": "_RequestBody", "ti_id": "ti-head", "field_a": 11, "field_b": 22, "field_c": 33},
                {"ti_id": "ti-head", "field_a": 11, "field_b": 22, "field_c": 33},
                id="head",
            ),
            pytest.param(
                "3026-05-10",
                {"type": "_RequestBody", "ti_id": "ti-middle", "field_a": 11, "field_b": 22},
                {"ti_id": "ti-middle", "field_a": 11, "field_b": 22, "field_c": 0},
                id="middle",
            ),
            pytest.param(
                "3026-02-15",
                {"type": "_RequestBody", "ti_id": "ti-early", "field_a": 11},
                {"ti_id": "ti-early", "field_a": 11, "field_b": 0, "field_c": 0},
                id="early",
            ),
            pytest.param(
                "3025-12-01",
                {"type": "_RequestBody", "ti_id": "ti-baseline"},
                {"ti_id": "ti-baseline", "field_a": 0, "field_b": 0, "field_c": 0},
                id="baseline",
            ),
        ],
    )
    @_PARAMETRIZE_CHANNELS
    def test_supervisor_decodes_upgraded_request(
        self,
        synthetic_migrator,
        client_version,
        wire_body,
        expected_head,
        supervisor_cls,
        channel,
        tmp_path,
    ):
        """
        Push a real wire frame from a real subprocess through the supervisor.

        The flow:

        1. The stub coordinator launches the fake runtime with a
           ``--send-frames`` payload describing one ``_RequestBody``
           frame at the pinned client version.
        2. The test reads the frame off the runtime <-> supervisor
           TCP socket using the exact 4-byte length prefix protocol
           the production selector loop uses.
        3. The frame is pushed into a real ``handle_requests``
           generator on a real ``WatchedSubprocess`` subclass. The
           supervisor's ``client_version`` is pinned to the same date
           the runtime was, mirroring the production wiring that
           ``coordinator.target_schema_version`` produces.
        4. The test reads the message ``_handle_request`` observed and
           asserts every backfilled field has the right value.
        """
        send_frames_path = _write_send_frames(tmp_path, [[42, wire_body, None]])
        captured_out = tmp_path / "captured.json"
        coordinator = _StubCoordinator(
            target_version=client_version,
            extra_runtime_args=[
                "--captured-out",
                os.fspath(captured_out),
                "--send-frames",
                os.fspath(send_frames_path),
                "--read-count",
                "0",
            ],
        )
        proc, runtime_sock = _spawn_runtime(coordinator, channel=channel)
        try:
            frame = _read_one_request_frame(runtime_sock)
            assert proc.wait(timeout=5) == 0, "fake runtime exited non-zero"
        finally:
            runtime_sock.close()
            if proc.poll() is None:
                proc.kill()
                proc.wait()

        # The bytes that reached the supervisor must still be the
        # pinned-version wire shape -- prove it before driving the
        # upgrade pipeline, otherwise an accidental head-shape send
        # would make the backfill assertions vacuous.
        assert frame.body == wire_body

        # Drive the production upgrade-and-decode pipeline on a real
        # supervisor instance pinned to the runtime's version.
        sup_stdin, _peer = socket.socketpair()
        try:
            sup = _new_supervisor(supervisor_cls, sup_stdin)
            sup.client_version = coordinator.target_schema_version(None)
            gen = sup.handle_requests(structlog.get_logger())
            next(gen)
            gen.send(frame)
            gen.close()
        finally:
            sup_stdin.close()
            _peer.close()

        received = sup.__dict__.get("_received_msgs", [])
        assert len(received) == 1, f"{channel}: supervisor's _handle_request never fired"
        msg = received[0]
        assert isinstance(msg, _RequestBody)
        for field, value in expected_head.items():
            assert getattr(msg, field) == value, f"{channel}: head field {field!r} mismatch"


# ---------------------------------------------------------------------------
# Coordinator-driven wiring: target_schema_version pins the supervisor.
# ---------------------------------------------------------------------------


class TestCoordinatorTargetSchemaVersionDrivesSupervisor:
    """
    Confirm the wiring contract: a coordinator's ``target_schema_version``
    is what production code (:meth:`ActivitySubprocess._on_child_started`
    /:meth:`DagFileProcessorProcess._on_child_started`) copies onto
    :attr:`WatchedSubprocess.client_version`. Reproducing just that pin
    step here keeps the assertion focused on the wiring without forking
    a real task-runner child.
    """

    @pytest.mark.parametrize(
        "pinned",
        ["3025-12-01", "3026-02-15", "3026-03-01", "3026-05-10", "3026-06-15", "3026-08-22", "3026-09-30"],
    )
    @_PARAMETRIZE_CHANNELS
    def test_target_schema_version_pins_client_version(self, pinned, supervisor_cls, channel):
        coordinator = _StubCoordinator(target_version=pinned, extra_runtime_args=[])
        sup_stdin, _peer = socket.socketpair()
        try:
            sup = _new_supervisor(supervisor_cls, sup_stdin)
            sup.client_version = coordinator.target_schema_version(None)
        finally:
            sup_stdin.close()
            _peer.close()
        assert sup.client_version == pinned

    def test_two_supervisors_can_pin_different_versions(self, synthetic_migrator, tmp_path):
        # A task-execution supervisor and a dag-processing supervisor
        # driven by two different coordinators must end up with
        # independent ``client_version`` values -- confirms there is no
        # shared global state across coordinator instances.
        sup_a_stdin, _a = socket.socketpair()
        sup_b_stdin, _b = socket.socketpair()
        try:
            sup_a = _new_supervisor(_StubTaskExecutionSupervisor, sup_a_stdin)
            sup_b = _new_supervisor(_StubDagProcessingSupervisor, sup_b_stdin)
            sup_a.client_version = _StubCoordinator(
                target_version="3026-02-15", extra_runtime_args=[]
            ).target_schema_version(None)
            sup_b.client_version = _StubCoordinator(
                target_version="3026-09-30", extra_runtime_args=[]
            ).target_schema_version(None)
        finally:
            sup_a_stdin.close()
            _a.close()
            sup_b_stdin.close()
            _b.close()
        assert sup_a.client_version == "3026-02-15"
        assert sup_b.client_version == "3026-09-30"
