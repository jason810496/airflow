#!/usr/bin/env python3
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
Supervisor-side harness for the supervisor IPC integration test.

Plays the role ``airflow dag-processor`` and ``airflow worker`` play
in production: a real OS process that runs the supervisor side of the
IPC contract -- forwards head-shape responses through
:meth:`WatchedSubprocess.send_msg` (downgraded to the runtime's pinned
version) and decodes upstream requests through
:meth:`WatchedSubprocess.handle_requests` (upgraded back to head).

Running the actual ``airflow dag-processor`` or ``airflow worker``
CLI in a unit-test environment is impractical: both require a
configured Airflow deployment (database, DAG bundles, queue broker,
API server). This harness stops one layer earlier -- it constructs
the same :class:`WatchedSubprocess` subclass the CLI would, configures
the synthetic schema bundle, launches the runtime through the stub
coordinator's ``task_execution_cmd`` / ``dag_parsing_cmd``, and
records every wire body it sees to a JSON file the test parent
inspects. The Python code under test (downgrade in ``send_msg``,
upgrade in ``handle_requests``, coordinator cmd resolution) is byte
for byte the same as what the CLI processes execute.

CLI:

  --config PATH    JSON file with the full scenario configuration.

The single-file config keeps the harness invocation hermetic: every
parameter the test wants to vary lives in the JSON, so the harness's
argv is identical across scenarios. Required keys:

  mode                          "dag-processing" | "task-execution"
  client_version                pinned schema version (ISO date)
  runtime_script                absolute path to ``_fake_runtime.py``
  runtime_capture_out           where the runtime writes captures
  supervisor_capture_out        where this harness writes captures
  send_response_bodies          list of head-shape ``_ResponseBody``
                                dicts to forward to the runtime
  runtime_send_frames           list of ``[id, body, ctx]`` triples
                                the runtime should send upstream
  runtime_read_count            frames the runtime reads before
                                sending upstream
  runtime_final_read_count      frames the runtime reads after
                                sending upstream

The harness records its observations as a JSON object::

    {
        "sent": [body_dict, ...],  # head-shape values fed into send_msg
        "received": [body_dict, ...],  # head-shape values after handle_requests upgrade
        "client_version": "3026-...",
    }
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import socket
import subprocess
import sys
from pathlib import Path
from typing import Any, ClassVar
from unittest.mock import MagicMock

import msgspec
import psutil
import structlog
from pydantic import TypeAdapter
from uuid6 import uuid7

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from _synthetic_bundle import (  # noqa: E402  -- sys.path tweak above is intentional
    _RequestBody,
    _ResponseBody,
    install_synthetic_migrator,
)

# Installing the synthetic migrator must happen before any import that
# might cache the head migrator (the production call sites re-import
# per call, so this is belt-and-suspenders).
install_synthetic_migrator()

from airflow.sdk.execution_time.comms import _RequestFrame  # noqa: E402
from airflow.sdk.execution_time.coordinator import (  # noqa: E402
    BaseCoordinator,
    _send_startup_details,
    _start_server,
)
from airflow.sdk.execution_time.supervisor import WatchedSubprocess  # noqa: E402


class _StubTaskExecutionSupervisor(WatchedSubprocess):
    """:class:`ActivitySubprocess` analogue pinned to ``_RequestBody`` decoder."""

    decoder: ClassVar[TypeAdapter] = TypeAdapter(_RequestBody)

    def _handle_request(self, msg, log, req_id):
        self.__dict__.setdefault("_received_msgs", []).append(msg)


class _StubDagProcessingSupervisor(WatchedSubprocess):
    """:class:`DagFileProcessorProcess` analogue pinned to ``_RequestBody`` decoder."""

    decoder: ClassVar[TypeAdapter] = TypeAdapter(_RequestBody)

    def _handle_request(self, msg, log, req_id):
        self.__dict__.setdefault("_received_msgs", []).append(msg)


_SUPERVISOR_FOR_MODE: dict[str, type[WatchedSubprocess]] = {
    "task-execution": _StubTaskExecutionSupervisor,
    "dag-processing": _StubDagProcessingSupervisor,
}


class _StubCoordinator(BaseCoordinator):
    """
    Coordinator that returns the fake-runtime invocation for both
    channels and pins its ``target_schema_version`` to a constant.
    """

    sdk = "test"
    file_extension = ".testjar"

    def __init__(self, *, target_version: str, runtime_cmd: list[str]) -> None:
        self._target_version = target_version
        self._runtime_cmd = list(runtime_cmd)

    def target_schema_version(self, _body: Any) -> str:
        return self._target_version

    def task_execution_cmd(self, *, comm_addr: str, **_unused) -> list[str]:
        return [*self._runtime_cmd, "--comm-addr", comm_addr]

    def dag_parsing_cmd(self, *, comm_addr: str, **_unused) -> list[str]:
        return [*self._runtime_cmd, "--comm-addr", comm_addr]


def _read_one_request_frame(sock: socket.socket, *, timeout: float = 5.0) -> _RequestFrame:
    """
    Read exactly one :class:`_RequestFrame` from *sock*.

    Uses the same 4-byte big-endian length-prefix protocol the
    production selector loop's ``length_prefixed_frame_reader`` uses,
    so a frame the runtime wrote with msgspec on its side parses
    cleanly here without any version-specific decoder logic.
    """
    sock.settimeout(timeout)
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


def _build_supervisor(mode: str, stdin: socket.socket) -> WatchedSubprocess:
    cls = _SUPERVISOR_FOR_MODE[mode]
    return cls(
        id=uuid7(),
        pid=os.getpid(),
        stdin=stdin,
        process=MagicMock(spec=psutil.Process),
        process_log=structlog.get_logger(),
    )


def _run(config: dict) -> int:
    mode: str = config["mode"]
    if mode not in _SUPERVISOR_FOR_MODE:
        raise ValueError(f"unknown mode {mode!r}")

    client_version: str = config["client_version"]
    runtime_script: str = config["runtime_script"]
    runtime_capture_out: str = config["runtime_capture_out"]
    supervisor_capture_out: str = config["supervisor_capture_out"]
    response_bodies_to_send: list[dict] = config.get("send_response_bodies") or []
    runtime_send_frames: list[list] = config.get("runtime_send_frames") or []
    runtime_read_count: int = int(config.get("runtime_read_count", len(response_bodies_to_send)))
    runtime_final_read_count: int = int(config.get("runtime_final_read_count", 0))

    log = structlog.get_logger()

    # Stage the runtime's outbound queue as a JSON file the fake
    # runtime reads via --send-frames. Anchored next to the supervisor
    # capture file so cleanup is trivially the test's tmp_path.
    runtime_send_frames_path = Path(supervisor_capture_out).with_suffix(".runtime_send.json")
    runtime_send_frames_path.write_text(json.dumps(runtime_send_frames))

    runtime_cmd = [
        sys.executable,
        runtime_script,
        "--captured-out",
        runtime_capture_out,
        "--read-count",
        str(runtime_read_count),
        "--final-read-count",
        str(runtime_final_read_count),
        "--send-frames",
        os.fspath(runtime_send_frames_path),
    ]

    coordinator = _StubCoordinator(target_version=client_version, runtime_cmd=runtime_cmd)

    comm_server = _start_server()
    host, port = comm_server.getsockname()
    comm_addr = f"{host}:{port}"

    if mode == "task-execution":
        cmd = coordinator.task_execution_cmd(comm_addr=comm_addr)
    else:
        cmd = coordinator.dag_parsing_cmd(comm_addr=comm_addr)

    proc = subprocess.Popen(cmd, stdin=subprocess.DEVNULL)
    try:
        comm_server.settimeout(10.0)
        runtime_sock, _ = comm_server.accept()
    finally:
        comm_server.close()

    captured_sent: list[dict] = []
    captured_received: list[dict] = []

    try:
        supervisor = _build_supervisor(mode, runtime_sock)
        supervisor.client_version = coordinator.target_schema_version(None)

        # Phase 1 -- supervisor -> runtime. The task-execution mode
        # starts with the special one-shot ``_send_startup_details``
        # downgrade (the production coordinator's seed handoff); the
        # rest of the responses (and every dag-processing response)
        # flow through ``send_msg`` exactly as production does after
        # the seed.
        for index, body_dict in enumerate(response_bodies_to_send):
            response_body = _ResponseBody(**body_dict)
            captured_sent.append(response_body.model_dump(mode="json"))
            if mode == "task-execution" and index == 0:
                _send_startup_details(runtime_sock, response_body, client_version=client_version)
            else:
                supervisor.send_msg(response_body, request_id=index)

        # Phase 2 -- runtime -> supervisor. Read the upstream frames
        # the runtime is sending, push each through the supervisor's
        # production ``handle_requests`` pipeline so the upgrade walk
        # actually runs, and record the decoded head shape.
        if runtime_send_frames:
            gen = supervisor.handle_requests(log)
            next(gen)
            try:
                for _ in range(len(runtime_send_frames)):
                    frame = _read_one_request_frame(runtime_sock)
                    gen.send(frame)
            finally:
                gen.close()
            for msg in supervisor.__dict__.get("_received_msgs", []):
                captured_received.append(msg.model_dump(mode="json"))

        exit_code = proc.wait(timeout=10)
        if exit_code != 0:
            log.warning("fake runtime exited with non-zero status", code=exit_code)
    finally:
        runtime_sock.close()
        if proc.poll() is None:
            proc.kill()
            proc.wait()

    Path(supervisor_capture_out).write_text(
        json.dumps(
            {
                "mode": mode,
                "client_version": client_version,
                "sent": captured_sent,
                "received": captured_received,
            }
        )
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, help="JSON config file")
    args = parser.parse_args(argv)

    # Quiet structlog so the harness's stdout / stderr stay readable
    # when the test prints them on failure.
    logging.basicConfig(level=logging.WARNING)

    config = json.loads(Path(args.config).read_text())
    return _run(config)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
