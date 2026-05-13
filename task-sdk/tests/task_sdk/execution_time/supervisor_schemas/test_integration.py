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
In-process integration tests for supervisor IPC schema migration.

What is being verified -- the wiring between the supervisor and the
schema-version migrator -- is independent of OS-level IPC. So these
tests drive the production seams (``WatchedSubprocess.send_msg``,
``WatchedSubprocess.handle_requests``, ``_send_startup_details``)
directly with a ``MagicMock`` socket, then decode the bytes the
production code wrote and assert on the wire shape.

The migrator runs for real against a synthetic Cadwyn bundle defined
in :mod:`_synthetic_bundle`. The ``synthetic_migrator`` fixture swaps
in that bundle via ``monkeypatch`` for the duration of one test, so
the real ``supervisor_schemas`` registry is restored automatically on
teardown -- no module-level mutation outlives the test.

The synthetic bundle has six dated :class:`~cadwyn.VersionChange`
entries -- three on ``_RequestBody`` (runtime -> supervisor) and three
on ``_ResponseBody`` (supervisor -> runtime). Tests parameterise the
pinned client version across all seven defined dates plus the
baseline, so every transformer in the chain runs in at least one
scenario for each channel.
"""

from __future__ import annotations

from typing import Any, ClassVar
from unittest.mock import MagicMock

import msgspec
import psutil
import pytest
import structlog
from pydantic import TypeAdapter
from task_sdk.execution_time.supervisor_schemas._synthetic_bundle import (
    ALL_VERSIONS,
    SYNTHETIC_BUNDLE,
    SYNTHETIC_REGISTRY,
    _RequestBody,
    _ResponseBody,
)
from uuid6 import uuid7

from airflow.sdk.execution_time.comms import _RequestFrame, _ResponseFrame
from airflow.sdk.execution_time.coordinator import _send_startup_details
from airflow.sdk.execution_time.supervisor import WatchedSubprocess
from airflow.sdk.execution_time.supervisor_schemas import SchemaVersionMigrator


@pytest.fixture
def synthetic_migrator(monkeypatch) -> SchemaVersionMigrator:
    """
    Bind the production migrator factory and registry to :data:`SYNTHETIC_BUNDLE`.

    Production call sites do a deferred ``from airflow.sdk.execution_time.supervisor_schemas
    import get_schema_version_migrator`` per call, so swapping the
    module attribute via ``monkeypatch.setattr`` redirects every
    downgrade and upgrade through the synthetic bundle until pytest
    tears the fixture down.
    """
    migrator = SchemaVersionMigrator(SYNTHETIC_BUNDLE)
    monkeypatch.setattr(
        "airflow.sdk.execution_time.supervisor_schemas.get_schema_version_migrator",
        lambda: migrator,
    )
    monkeypatch.setattr(
        "airflow.sdk.execution_time.supervisor_schemas.registered_models_by_name",
        lambda: SYNTHETIC_REGISTRY,
    )
    return migrator


class _StubTaskExecutionSupervisor(WatchedSubprocess):
    """``ActivitySubprocess`` analogue pinned to the synthetic ``_RequestBody`` decoder."""

    decoder: ClassVar[TypeAdapter] = TypeAdapter(_RequestBody)

    def _handle_request(self, msg, log, req_id):
        self.__dict__.setdefault("_received_msgs", []).append(msg)


class _StubDagProcessingSupervisor(WatchedSubprocess):
    """``DagFileProcessorProcess`` analogue pinned to the synthetic ``_RequestBody`` decoder."""

    decoder: ClassVar[TypeAdapter] = TypeAdapter(_RequestBody)

    def _handle_request(self, msg, log, req_id):
        self.__dict__.setdefault("_received_msgs", []).append(msg)


_SUPERVISOR_BY_MODE: dict[str, type[WatchedSubprocess]] = {
    "task-execution": _StubTaskExecutionSupervisor,
    "dag-processing": _StubDagProcessingSupervisor,
}


def _new_supervisor(mode: str, pinned_version: str) -> WatchedSubprocess:
    """Build a :class:`WatchedSubprocess` with a mock stdin and a pinned migrator version."""
    cls = _SUPERVISOR_BY_MODE[mode]
    ws = cls(
        id=uuid7(),
        pid=1,
        stdin=MagicMock(),
        process=MagicMock(spec=psutil.Process),
        process_log=structlog.get_logger(),
    )
    ws.lang_sdk_msg_schema_version = pinned_version
    return ws


def _decode_response_frame(raw: bytes) -> dict[str, Any]:
    """Pull the body dict out of the on-wire ``_ResponseFrame`` bytes."""
    # 4-byte big-endian length prefix, then a msgpack-encoded ``_ResponseFrame``
    # tuple (id, body, error). Production framing -- exactly what the
    # foreign-language runtime decodes against.
    length = int.from_bytes(raw[:4], "big")
    payload = raw[4 : 4 + length]
    frame = msgspec.msgpack.Decoder(_ResponseFrame).decode(payload)
    assert frame.body is not None, "downgraded frame must carry a body"
    return frame.body


_DOWNGRADE_EXPECTATIONS: dict[str, dict[str, Any]] = {
    # For each pinned client version, the wire shape the runtime must
    # see (every key present + value) and the head fields it must NOT
    # see (later additions, trimmed by the downgrade walk).
    "3025-12-01": {
        "present": {"type": "_ResponseBody", "ti_id": "ti-resp"},
        "absent": ("response_x", "response_y", "response_z"),
    },
    "3026-02-15": {
        "present": {"type": "_ResponseBody", "ti_id": "ti-resp"},
        "absent": ("response_x", "response_y", "response_z"),
    },
    "3026-03-01": {
        "present": {
            "type": "_ResponseBody",
            "ti_id": "ti-resp",
            "response_x": "x-value",
        },
        "absent": ("response_y", "response_z"),
    },
    "3026-05-10": {
        "present": {
            "type": "_ResponseBody",
            "ti_id": "ti-resp",
            "response_x": "x-value",
        },
        "absent": ("response_y", "response_z"),
    },
    "3026-06-15": {
        "present": {
            "type": "_ResponseBody",
            "ti_id": "ti-resp",
            "response_x": "x-value",
            "response_y": "y-value",
        },
        "absent": ("response_z",),
    },
    "3026-08-22": {
        "present": {
            "type": "_ResponseBody",
            "ti_id": "ti-resp",
            "response_x": "x-value",
            "response_y": "y-value",
        },
        "absent": ("response_z",),
    },
    "3026-09-30": {
        "present": {
            "type": "_ResponseBody",
            "ti_id": "ti-resp",
            "response_x": "x-value",
            "response_y": "y-value",
            "response_z": "z-value",
        },
        "absent": (),
    },
}


_HEAD_RESPONSE_BODY = _ResponseBody(
    ti_id="ti-resp",
    response_x="x-value",
    response_y="y-value",
    response_z="z-value",
)


def _wire_request_for(pinned_version: str, ti_id: str) -> dict[str, Any]:
    """
    Build a wire-shape ``_RequestBody`` dict containing exactly the fields a
    runtime pinned to *pinned_version* was built to send.
    """
    wire: dict[str, Any] = {"type": "_RequestBody", "ti_id": ti_id}
    if pinned_version >= "3026-02-15":
        wire["field_a"] = 11
    if pinned_version >= "3026-05-10":
        wire["field_b"] = 22
    if pinned_version >= "3026-08-22":
        wire["field_c"] = 33
    return wire


def _expected_head_request_for(pinned_version: str, ti_id: str) -> _RequestBody:
    """
    Build the head Pydantic shape the supervisor must see after upgrade for a runtime
    pinned to *pinned_version*. Fields the runtime did not send are backfilled to ``0``.
    """
    return _RequestBody(
        ti_id=ti_id,
        field_a=11 if pinned_version >= "3026-02-15" else 0,
        field_b=22 if pinned_version >= "3026-05-10" else 0,
        field_c=33 if pinned_version >= "3026-08-22" else 0,
    )


_PARAMETRIZE_MODE = pytest.mark.parametrize(
    "mode",
    [
        pytest.param("task-execution", id="task-execution"),
        pytest.param("dag-processing", id="dag-processing"),
    ],
)


@_PARAMETRIZE_MODE
@pytest.mark.parametrize("pinned_version", ALL_VERSIONS)
def test_send_msg_downgrades_to_pinned_wire_shape(synthetic_migrator, mode, pinned_version):
    """
    Drive ``send_msg`` with the synthetic migrator bound and confirm the bytes that hit
    stdin decode to the expected wire-version dict.
    """
    ws = _new_supervisor(mode, pinned_version)
    ws.send_msg(_HEAD_RESPONSE_BODY, request_id=0)

    ws.stdin.sendall.assert_called_once()
    raw = ws.stdin.sendall.call_args[0][0]
    wire_body = _decode_response_frame(bytes(raw))

    expectations = _DOWNGRADE_EXPECTATIONS[pinned_version]
    for field, value in expectations["present"].items():
        assert wire_body.get(field) == value, (
            f"{mode} @ {pinned_version}: wire field {field!r} mismatch -- got {wire_body!r}"
        )
    for field in expectations["absent"]:
        assert field not in wire_body, (
            f"{mode} @ {pinned_version}: wire field {field!r} must be stripped by downgrade"
        )


@pytest.mark.parametrize("pinned_version", ALL_VERSIONS)
def test_send_startup_details_downgrades_seed_frame(synthetic_migrator, pinned_version):
    """
    The task-execution seed handoff goes through ``_send_startup_details``, not ``send_msg``.

    Same downgrade contract though: the bytes written to the runtime
    socket must carry the wire shape the runtime was built against.
    """
    sock = MagicMock()
    _send_startup_details(sock, _HEAD_RESPONSE_BODY, lang_sdk_msg_schema_version=pinned_version)  # type: ignore[arg-type]

    sock.sendall.assert_called_once()
    raw = sock.sendall.call_args[0][0]
    wire_body = _decode_response_frame(bytes(raw))

    expectations = _DOWNGRADE_EXPECTATIONS[pinned_version]
    for field, value in expectations["present"].items():
        assert wire_body.get(field) == value, (
            f"seed @ {pinned_version}: wire field {field!r} mismatch -- got {wire_body!r}"
        )
    for field in expectations["absent"]:
        assert field not in wire_body, (
            f"seed @ {pinned_version}: wire field {field!r} must be stripped by downgrade"
        )


@_PARAMETRIZE_MODE
@pytest.mark.parametrize("pinned_version", ALL_VERSIONS)
def test_handle_requests_upgrades_wire_to_head_shape(synthetic_migrator, mode, pinned_version):
    """
    Hand-build a ``_RequestFrame`` at *pinned_version*'s wire shape, drive ``handle_requests``,
    and confirm the head Pydantic body the decoder produced has every later-version field
    backfilled.
    """
    ws = _new_supervisor(mode, pinned_version)
    wire = _wire_request_for(pinned_version, ti_id="ti-up")

    gen = ws.handle_requests(structlog.get_logger())
    next(gen)
    try:
        gen.send(_RequestFrame(id=1, body=wire))
    finally:
        gen.close()

    received = ws.__dict__.get("_received_msgs", [])
    assert len(received) == 1, f"{mode} @ {pinned_version}: expected exactly one upgraded message"
    assert received[0] == _expected_head_request_for(pinned_version, ti_id="ti-up")


def test_round_trip_preserves_state_across_multiple_frames(synthetic_migrator):
    """
    Send three responses and two requests at the middle pinned version to confirm neither
    direction drops state between frames.
    """
    pinned_version = "3026-05-10"
    ws = _new_supervisor("task-execution", pinned_version)

    responses = [
        _ResponseBody(
            ti_id=f"ti-{i}",
            response_x="x-value",
            response_y="y-value",
            response_z="z-value",
        )
        for i in range(3)
    ]
    for index, response in enumerate(responses):
        ws.send_msg(response, request_id=index)

    wire_bodies = [_decode_response_frame(bytes(call.args[0])) for call in ws.stdin.sendall.call_args_list]
    assert [b["ti_id"] for b in wire_bodies] == ["ti-0", "ti-1", "ti-2"]
    for body in wire_bodies:
        # 3026-05-10 trims response_y and response_z (both introduced later)
        # while keeping response_x (introduced at 3026-03-01).
        assert body.get("response_x") == "x-value"
        assert "response_y" not in body
        assert "response_z" not in body

    request_wires = [_wire_request_for(pinned_version, ti_id=f"ti-up-{i}") for i in range(2)]
    expected_heads = [_expected_head_request_for(pinned_version, ti_id=f"ti-up-{i}") for i in range(2)]

    gen = ws.handle_requests(structlog.get_logger())
    next(gen)
    try:
        for index, wire in enumerate(request_wires):
            gen.send(_RequestFrame(id=index + 1, body=wire))
    finally:
        gen.close()

    received = ws.__dict__.get("_received_msgs", [])
    assert received == expected_heads
