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

Three OS processes participate in every test:

- The test parent (pytest) -- orchestrator.
- :file:`_supervisor_subprocess.py` -- a real Python subprocess that
  plays the role ``airflow dag-processor`` and ``airflow worker``
  play in production. It constructs the same
  :class:`WatchedSubprocess` subclass the CLIs do and drives
  :meth:`send_msg` / :meth:`handle_requests` against the runtime
  process. (The actual CLIs need a full Airflow deployment to run --
  database, DAG bundles, queue broker -- which is out of scope for a
  task-sdk unit test; the harness exercises the same Python code
  paths those CLIs ultimately execute.)
- :file:`_fake_runtime.py` -- a real Python subprocess that plays the
  foreign-language runtime (Java, Go, Rust). Spawned by the
  supervisor harness through the stub coordinator's
  ``task_execution_cmd`` / ``dag_parsing_cmd``, just as a production
  coordinator spawns its runtime.

Bytes flow over real TCP sockets between the supervisor harness and
the runtime. Both subprocesses record every wire body they observe
to a JSON tempfile, and the test parent validates *both* files: the
downgrade direction (supervisor -> runtime) is checked against the
runtime's capture; the upgrade direction (runtime -> supervisor) is
checked against the supervisor harness's capture.

The synthetic bundle the harnesses share has **six** dated
:class:`~cadwyn.VersionChange` entries -- three on ``_RequestBody``
(runtime -> supervisor) and three on ``_ResponseBody`` (supervisor
-> runtime). Tests parameterise the runtime's pinned client version
across all seven defined dates plus the baseline, so every transformer
in the chain runs in at least one scenario for each channel.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from task_sdk.execution_time.supervisor_schemas._synthetic_bundle import ALL_VERSIONS

HERE = Path(__file__).resolve().parent
SUPERVISOR_SUBPROCESS = HERE / "_supervisor_subprocess.py"
FAKE_RUNTIME_SCRIPT = HERE / "_fake_runtime.py"


def _run_harness(config: dict[str, Any], tmp_path: Path) -> tuple[dict[str, Any], list[Any]]:
    """
    Spawn the supervisor harness as a real subprocess and return both
    sides' captures.

    The harness writes its own observations to ``supervisor_capture_out``
    and the runtime (spawned in turn by the harness through the stub
    coordinator) writes its observations to ``runtime_capture_out``.
    Returning both lets the test assert on the downgrade direction
    (runtime capture) and the upgrade direction (supervisor capture)
    in the same parameterisation.

    Surfaces the harness's stdout / stderr on non-zero exit so a CI
    failure points straight at the misbehaving subprocess rather than
    at a silent ``pytest.fail``.
    """
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))

    proc = subprocess.run(
        [sys.executable, os.fspath(SUPERVISOR_SUBPROCESS), "--config", os.fspath(config_path)],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if proc.returncode != 0:
        raise AssertionError(
            f"supervisor harness exited {proc.returncode}\n"
            f"--- stdout ---\n{proc.stdout}\n"
            f"--- stderr ---\n{proc.stderr}\n"
        )

    supervisor_capture = json.loads(Path(config["supervisor_capture_out"]).read_text())
    runtime_capture = json.loads(Path(config["runtime_capture_out"]).read_text())
    return supervisor_capture, runtime_capture


def _make_config(
    *,
    mode: str,
    lang_sdk_msg_schema_version: str,
    tmp_path: Path,
    send_response_bodies: list[dict],
    runtime_send_frames: list[list],
    runtime_final_read_count: int = 0,
) -> dict[str, Any]:
    """Materialise a JSON-serialisable harness config under *tmp_path*."""
    return {
        "mode": mode,
        "lang_sdk_msg_schema_version": lang_sdk_msg_schema_version,
        "runtime_script": os.fspath(FAKE_RUNTIME_SCRIPT),
        "runtime_capture_out": os.fspath(tmp_path / "runtime_capture.json"),
        "supervisor_capture_out": os.fspath(tmp_path / "supervisor_capture.json"),
        "send_response_bodies": send_response_bodies,
        "runtime_send_frames": runtime_send_frames,
        "runtime_read_count": len(send_response_bodies),
        "runtime_final_read_count": runtime_final_read_count,
    }


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

_HEAD_RESPONSE_BODY: dict[str, Any] = {
    "ti_id": "ti-resp",
    "response_x": "x-value",
    "response_y": "y-value",
    "response_z": "z-value",
}


def _wire_request_for(lang_sdk_msg_schema_version: str, ti_id: str) -> dict[str, Any]:
    """
    Build a wire-shape ``_RequestBody`` dict containing exactly the
    fields a runtime pinned to *lang_sdk_msg_schema_version* was built to send.
    """
    wire: dict[str, Any] = {"type": "_RequestBody", "ti_id": ti_id}
    if lang_sdk_msg_schema_version >= "3026-02-15":
        wire["field_a"] = 11
    if lang_sdk_msg_schema_version >= "3026-05-10":
        wire["field_b"] = 22
    if lang_sdk_msg_schema_version >= "3026-08-22":
        wire["field_c"] = 33
    return wire


def _expected_head_request_for(lang_sdk_msg_schema_version: str, ti_id: str) -> dict[str, Any]:
    """
    Build the head Pydantic shape the supervisor must see after upgrade,
    for a runtime pinned to *lang_sdk_msg_schema_version*. Fields the runtime did
    not send are backfilled by the request transformers to ``0``.
    """
    return {
        "type": "_RequestBody",
        "ti_id": ti_id,
        "field_a": 11 if lang_sdk_msg_schema_version >= "3026-02-15" else 0,
        "field_b": 22 if lang_sdk_msg_schema_version >= "3026-05-10" else 0,
        "field_c": 33 if lang_sdk_msg_schema_version >= "3026-08-22" else 0,
    }


_PARAMETRIZE_MODE_AND_VERSION = pytest.mark.parametrize(
    "mode",
    [
        pytest.param("task-execution", id="task-execution"),
        pytest.param("dag-processing", id="dag-processing"),
    ],
)


@_PARAMETRIZE_MODE_AND_VERSION
@pytest.mark.parametrize("lang_sdk_msg_schema_version", ALL_VERSIONS)
def test_supervisor_and_runtime_subprocesses_round_trip(mode, lang_sdk_msg_schema_version, tmp_path):
    """
    Drive a full request/response round-trip across two real subprocesses.

    Sequence -- mirrors what production does for the channel under test:

    1. The supervisor harness sends one head-shape ``_ResponseBody`` to
       the runtime through the production code path
       (``_send_startup_details`` for the task-execution seed, then
       ``send_msg``; ``send_msg`` only for dag-processing). The
       runtime captures the wire body it received and writes it to
       its capture file.
    2. The runtime then sends one wire-shape ``_RequestBody`` upstream
       at *lang_sdk_msg_schema_version*'s wire shape. The supervisor harness reads
       the bytes, runs them through ``handle_requests`` (which calls
       upgrade), and records the head Pydantic model the decoder
       produced.

    The test then validates **both** capture files: the runtime's
    capture proves the downgrade direction matches the pinned version,
    and the supervisor's capture proves the upgrade direction
    backfills every later-version field.
    """
    wire_request = _wire_request_for(lang_sdk_msg_schema_version, ti_id="ti-up")
    expected_head_request = _expected_head_request_for(lang_sdk_msg_schema_version, ti_id="ti-up")
    expected_wire_response = _DOWNGRADE_EXPECTATIONS[lang_sdk_msg_schema_version]

    config = _make_config(
        mode=mode,
        lang_sdk_msg_schema_version=lang_sdk_msg_schema_version,
        tmp_path=tmp_path,
        send_response_bodies=[_HEAD_RESPONSE_BODY],
        runtime_send_frames=[[1, wire_request, None]],
    )
    supervisor_capture, runtime_capture = _run_harness(config, tmp_path)

    # -----------------------------------------------------------------
    # Downgrade direction -- the runtime's capture is the source of truth.
    # -----------------------------------------------------------------
    assert len(runtime_capture) == 1, "runtime should have captured exactly one frame"
    wire_body = runtime_capture[0]
    for field, value in expected_wire_response["present"].items():
        assert wire_body.get(field) == value, (
            f"{mode} @ {lang_sdk_msg_schema_version}: wire field {field!r} mismatch -- got {wire_body!r}"
        )
    for field in expected_wire_response["absent"]:
        assert field not in wire_body, (
            f"{mode} @ {lang_sdk_msg_schema_version}: wire field {field!r} must be stripped by downgrade"
        )

    # -----------------------------------------------------------------
    # Upgrade direction -- the supervisor's capture is the source of truth.
    # -----------------------------------------------------------------
    assert supervisor_capture["mode"] == mode
    assert supervisor_capture["lang_sdk_msg_schema_version"] == lang_sdk_msg_schema_version
    assert supervisor_capture["sent"] == [_HEAD_RESPONSE_BODY | {"type": "_ResponseBody"}], (
        "supervisor harness must record what it fed into send_msg in head shape"
    )
    assert len(supervisor_capture["received"]) == 1, (
        f"{mode} @ {lang_sdk_msg_schema_version}: supervisor should have observed exactly one upgraded request"
    )
    assert supervisor_capture["received"][0] == expected_head_request, (
        f"{mode} @ {lang_sdk_msg_schema_version}: head fields after upgrade must be backfilled"
    )


@_PARAMETRIZE_MODE_AND_VERSION
def test_multiple_responses_and_requests_round_trip(mode, tmp_path):
    """
    Cross a multi-frame exchange to confirm neither direction drops
    state between frames.

    Three responses are sent supervisor -> runtime; two requests are
    sent runtime -> supervisor. Picking the middle pinned version
    (``3026-05-10``) keeps both directions interesting -- at least one
    response field is stripped on downgrade, and at least one request
    field is backfilled on upgrade.
    """
    lang_sdk_msg_schema_version = "3026-05-10"
    responses = [
        _HEAD_RESPONSE_BODY | {"ti_id": "ti-0"},
        _HEAD_RESPONSE_BODY | {"ti_id": "ti-1", "response_x": "x1"},
        _HEAD_RESPONSE_BODY | {"ti_id": "ti-2", "response_z": "z-only"},
    ]
    request_wires = [
        _wire_request_for(lang_sdk_msg_schema_version, ti_id="ti-up-0"),
        _wire_request_for(lang_sdk_msg_schema_version, ti_id="ti-up-1"),
    ]
    expected_head_requests = [
        _expected_head_request_for(lang_sdk_msg_schema_version, ti_id="ti-up-0"),
        _expected_head_request_for(lang_sdk_msg_schema_version, ti_id="ti-up-1"),
    ]
    expected_wire = _DOWNGRADE_EXPECTATIONS[lang_sdk_msg_schema_version]

    config = _make_config(
        mode=mode,
        lang_sdk_msg_schema_version=lang_sdk_msg_schema_version,
        tmp_path=tmp_path,
        send_response_bodies=responses,
        runtime_send_frames=[[i + 1, body, None] for i, body in enumerate(request_wires)],
    )
    supervisor_capture, runtime_capture = _run_harness(config, tmp_path)

    assert len(runtime_capture) == len(responses)
    for wire_body, head_body in zip(runtime_capture, responses):
        assert wire_body["ti_id"] == head_body["ti_id"]
        for field in expected_wire["absent"]:
            assert field not in wire_body, f"{mode}: field {field!r} must be trimmed"

    assert len(supervisor_capture["received"]) == len(expected_head_requests)
    assert supervisor_capture["received"] == expected_head_requests
    assert [b["ti_id"] for b in supervisor_capture["sent"]] == [b["ti_id"] for b in responses]


def test_harness_surfaces_non_zero_exit(tmp_path):
    """
    Smoke-test the harness wrapper: a malformed config must fail loud
    enough that CI failures point at the misbehaving subprocess. The
    JSON file points at a non-existent runtime script, so the harness
    has nothing to spawn and exits non-zero.
    """
    bad_config = {
        "mode": "task-execution",
        "lang_sdk_msg_schema_version": "3026-05-10",
        "runtime_script": os.fspath(tmp_path / "does_not_exist.py"),
        "runtime_capture_out": os.fspath(tmp_path / "runtime_capture.json"),
        "supervisor_capture_out": os.fspath(tmp_path / "supervisor_capture.json"),
        "send_response_bodies": [_HEAD_RESPONSE_BODY],
        "runtime_send_frames": [],
        "runtime_read_count": 1,
    }
    with pytest.raises(AssertionError, match="supervisor harness exited"):
        _run_harness(bad_config, tmp_path)
