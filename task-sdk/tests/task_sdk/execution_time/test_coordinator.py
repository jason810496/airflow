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
from __future__ import annotations

import contextlib
import json
import os
import socket
import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import ANY, MagicMock, patch

import msgpack
import pytest

from airflow.sdk.execution_time.comms import StartupDetails
from airflow.sdk.execution_time.coordinator import (
    BaseCoordinator,
    CoordinatorManager,
    _bridge,
    _send_startup_details,
    _start_server,
    get_coordinator_manager,
    reset_coordinator_manager,
)

MIGRATOR_PATH = "airflow.sdk.execution_time.supervisor_schemas.get_schema_version_migrator"
VERSION = "2026-04-17"


class TestStartServer:
    def test_binds_to_localhost(self):
        server = _start_server()
        try:
            host, port = server.getsockname()
            assert host == "127.0.0.1"
            assert port > 0
        finally:
            server.close()

    def test_assigns_random_port(self):
        s1 = _start_server()
        s2 = _start_server()
        try:
            _, port1 = s1.getsockname()
            _, port2 = s2.getsockname()
            assert port1 != port2
        finally:
            s1.close()
            s2.close()

    def test_accepts_connection(self):
        server = _start_server()
        try:
            addr = server.getsockname()
            client = socket.socket()
            client.connect(addr)
            conn, _ = server.accept()
            conn.sendall(b"ping")
            assert client.recv(4) == b"ping"
            conn.close()
            client.close()
        finally:
            server.close()


class _WireFrame:
    """
    Mock argument matcher for the length-prefixed msgpack frame bytes
    that :func:`_send_startup_details` hands to ``socket.sendall``.

    Using a matcher (rather than reaching into ``mock.call_args``) lets
    the tests stay on the high-level ``assert_called_once_with`` API
    while still asserting on the decoded ``(response_id, body)`` pair
    rather than the opaque on-wire bytes. ``__eq__`` is invoked by mock
    when comparing recorded call arguments against the expectation.
    """

    def __init__(self, *, response_id: Any = ANY, body: Any = ANY) -> None:
        self.response_id = response_id
        self.body = body

    __hash__ = None  # type: ignore[assignment]  # matcher is value-compared, never hashed

    def __eq__(self, raw: object) -> bool:
        if not isinstance(raw, (bytes, bytearray)):
            return NotImplemented
        length = int.from_bytes(raw[:4], "big")
        if length != len(raw) - 4:
            return False
        frame = msgpack.unpackb(bytes(raw[4:]))
        return frame[0] == self.response_id and frame[1] == self.body

    def __repr__(self) -> str:
        return f"_WireFrame(response_id={self.response_id!r}, body={self.body!r})"


class TestSendStartupDetails:
    @pytest.fixture
    def startup_msg(self):
        msg = MagicMock(spec=StartupDetails)
        msg.model_dump.return_value = {"type": "StartupDetails", "ti": {}}
        return msg

    @pytest.fixture
    def mock_socket(self):
        return MagicMock(spec=socket.socket)

    @pytest.fixture
    def mock_migrator(self, mocker):
        return mocker.patch(MIGRATOR_PATH).return_value

    def test_sends_length_prefixed_frame_bytes(self, startup_msg, mock_socket, mock_migrator):
        mock_migrator.downgrade.return_value = {"type": "StartupDetails"}

        _send_startup_details(mock_socket, startup_msg, lang_sdk_msg_schema_version=VERSION)

        # The matcher's ``__eq__`` verifies the 4-byte big-endian length
        # prefix matches the msgpack payload length.
        mock_socket.sendall.assert_called_once_with(_WireFrame())

    def test_frame_carries_response_id_zero(self, startup_msg, mock_socket, mock_migrator):
        mock_migrator.downgrade.return_value = {"type": "StartupDetails"}

        _send_startup_details(mock_socket, startup_msg, lang_sdk_msg_schema_version=VERSION)

        mock_socket.sendall.assert_called_once_with(_WireFrame(response_id=0))

    def test_routes_through_migrator_with_json_mode(self, startup_msg, mock_socket, mock_migrator):
        downgraded = {"type": "StartupDetails", "ti": {"task_id": "t1"}}
        mock_migrator.downgrade.return_value = downgraded

        _send_startup_details(mock_socket, startup_msg, lang_sdk_msg_schema_version=VERSION)

        # The seed StartupDetails is always downgraded through the
        # migrator (json mode forced) before being framed.
        mock_migrator.downgrade.assert_called_once_with(startup_msg, VERSION, dump_kwargs={"mode": "json"})
        startup_msg.model_dump.assert_not_called()
        mock_socket.sendall.assert_called_once_with(_WireFrame(response_id=0, body=downgraded))


class TestBaseCoordinatorDefaults:
    def test_get_code_from_file_raises_not_implemented(self):
        with pytest.raises(NotImplementedError):
            BaseCoordinator().get_code_from_file("/path/to/dag.jar")

    def test_target_msg_schema_version_is_abstract(self):
        # The base class refuses to guess: concrete coordinators
        # (JavaCoordinator, etc.) must read their bundle artifact's
        # pinned schema version. Any default would silently migrate
        # every foreign-runtime payload to the wrong version.
        with pytest.raises(NotImplementedError):
            BaseCoordinator().target_msg_schema_version(MagicMock())

    def test_task_execution_cmd_raises_not_implemented(self):
        with pytest.raises(NotImplementedError):
            BaseCoordinator().task_execution_cmd(
                what=MagicMock(),
                dag_file_path="/dag.jar",
                bundle_path="/path",
                bundle_info=MagicMock(),
                comm_addr="127.0.0.1:1234",
                logs_addr="127.0.0.1:1235",
            )


class TestCoordinatorNamedTuples:
    def test_dag_parsing_info_defaults(self):
        info = BaseCoordinator.DagParsingInfo(
            dag_file_path="/dag.jar",
            bundle_name="my-bundle",
            bundle_path="/bundles/my-bundle",
        )
        assert info.mode == "dag-parsing"
        assert info.dag_file_path == "/dag.jar"
        assert info.bundle_name == "my-bundle"
        assert info.bundle_path == "/bundles/my-bundle"

    def test_task_execution_info_defaults(self):
        mock_ti = MagicMock()
        mock_bundle = MagicMock()
        mock_startup = MagicMock()
        info = BaseCoordinator.TaskExecutionInfo(
            what=mock_ti,
            dag_rel_path="dags/example.jar",
            bundle_info=mock_bundle,
            startup_details=mock_startup,
            lang_sdk_msg_schema_version="2026-04-17",
        )
        assert info.mode == "task-execution"
        assert info.what is mock_ti
        assert info.dag_rel_path == "dags/example.jar"
        assert info.lang_sdk_msg_schema_version == "2026-04-17"


class TestBridge:
    def test_bridge_forwards_comm_bidirectionally(self):
        sup_send, sup_recv = socket.socketpair()
        rt_send, rt_recv = socket.socketpair()
        log_send, log_recv = socket.socketpair()
        stderr_send, stderr_recv = socket.socketpair()

        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.poll.return_value = 0
        mock_log = MagicMock()

        try:
            sup_send.sendall(b"from_supervisor")
            rt_send.sendall(b"from_runtime")
            log_send.sendall(b'{"event":"hello","level":"info"}\n')
            stderr_send.sendall(b"stderr line\n")

            sup_send.close()
            rt_send.close()
            log_send.close()
            stderr_send.close()

            _bridge(sup_recv, rt_recv, log_recv, stderr_recv, mock_proc, mock_log)
        finally:
            for s in (sup_send, rt_send, log_send, stderr_send, sup_recv, rt_recv, log_recv, stderr_recv):
                with contextlib.suppress(OSError):
                    s.close()

    def test_bridge_drains_after_process_exit(self):
        sup_local, sup_remote = socket.socketpair()
        rt_local, rt_remote = socket.socketpair()
        log_local, log_remote = socket.socketpair()
        stderr_local, stderr_remote = socket.socketpair()

        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.poll.side_effect = [None, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        mock_log = MagicMock()

        try:
            stderr_local.sendall(b"error output\n")
            stderr_local.close()
            sup_local.close()
            rt_local.close()
            log_local.close()

            _bridge(sup_remote, rt_remote, log_remote, stderr_remote, mock_proc, mock_log)
        finally:
            for s in (
                sup_local,
                sup_remote,
                rt_local,
                rt_remote,
                log_local,
                log_remote,
                stderr_local,
                stderr_remote,
            ):
                with contextlib.suppress(OSError):
                    s.close()

    def test_bridge_closes_all_sockets(self):
        sup = MagicMock(spec=socket.socket)
        rt = MagicMock(spec=socket.socket)
        logs = MagicMock(spec=socket.socket)
        stderr = MagicMock(spec=socket.socket)

        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.poll.return_value = 0
        mock_log = MagicMock()

        with (
            patch("airflow.sdk.execution_time.coordinator.selectors.DefaultSelector") as mock_sel_cls,
            patch("airflow.sdk.execution_time.selector_loop.service_selector"),
        ):
            mock_sel = MagicMock()
            mock_sel_cls.return_value = mock_sel
            mock_sel.get_map.return_value = {}

            _bridge(sup, rt, logs, stderr, mock_proc, mock_log)

        sup.close.assert_called()
        rt.close.assert_called()
        logs.close.assert_called()
        stderr.close.assert_called()
        mock_sel.close.assert_called_once()


class _StubCoordinator(BaseCoordinator):
    sdk = "test"
    file_extension = ".test"

    def __init__(self, *, exec_cmd: list[str] | None = None):
        self._exec_cmd = exec_cmd or ["test-runtime", "--execute"]

    def task_execution_cmd(self, *, dag_file_path, **_):
        return [*self._exec_cmd, dag_file_path]


class TestRunTaskExecution:
    @patch.object(BaseCoordinator, "_runtime_subprocess_entrypoint")
    def test_run_task_execution_creates_task_execution_info(self, mock_entrypoint):
        mock_ti = MagicMock()
        mock_bundle_info = MagicMock()
        mock_startup = MagicMock()

        coordinator = _StubCoordinator()
        coordinator.run_task_execution(
            what=mock_ti,
            dag_rel_path="dags/example.jar",
            bundle_info=mock_bundle_info,
            startup_details=mock_startup,
            lang_sdk_msg_schema_version="2026-04-17",
        )

        mock_entrypoint.assert_called_once()
        info = mock_entrypoint.call_args[0][0]
        assert isinstance(info, BaseCoordinator.TaskExecutionInfo)
        assert info.what is mock_ti
        assert info.dag_rel_path == "dags/example.jar"
        assert info.bundle_info is mock_bundle_info
        assert info.startup_details is mock_startup
        assert info.lang_sdk_msg_schema_version == "2026-04-17"
        assert info.mode == "task-execution"


class TestRuntimeSubprocessEntrypoint:
    @pytest.fixture(autouse=True)
    def _restore_process_context_env(self):
        old = os.environ.get("_AIRFLOW_PROCESS_CONTEXT")
        try:
            yield
        finally:
            if old is None:
                os.environ.pop("_AIRFLOW_PROCESS_CONTEXT", None)
            else:
                os.environ["_AIRFLOW_PROCESS_CONTEXT"] = old

    def test_unknown_entrypoint_info_type_raises(self):
        coordinator = _StubCoordinator()
        fake_info = MagicMock()
        fake_info.mode = "unknown"

        with pytest.raises(ValueError, match="Unknown entrypoint_info type"):
            coordinator._runtime_subprocess_entrypoint(fake_info)  # type: ignore[arg-type]

    @patch("airflow.sdk.execution_time.coordinator._bridge")
    @patch("airflow.sdk.execution_time.coordinator._send_startup_details")
    @patch("subprocess.Popen", autospec=True)
    @patch("airflow.sdk.execution_time.coordinator._start_server")
    @patch("os.dup", return_value=99)
    @patch("airflow.sdk.execution_time.task_runner.resolve_bundle")
    @patch("airflow.dag_processing.bundles.base.BundleVersionLock", autospec=True)
    def test_task_execution_flow(
        self,
        mock_bundle_lock,
        mock_resolve_bundle,
        mock_dup,
        mock_start_server,
        mock_popen,
        mock_send_startup,
        mock_bridge,
    ):
        comm_server = MagicMock(spec=socket.socket)
        comm_server.getsockname.return_value = ("127.0.0.1", 6000)
        logs_server = MagicMock(spec=socket.socket)
        logs_server.getsockname.return_value = ("127.0.0.1", 6001)
        mock_start_server.side_effect = [comm_server, logs_server]

        runtime_comm = MagicMock(spec=socket.socket)
        runtime_logs = MagicMock(spec=socket.socket)
        comm_server.accept.return_value = (runtime_comm, ("127.0.0.1", 9000))
        logs_server.accept.return_value = (runtime_logs, ("127.0.0.1", 9001))

        child_stderr = MagicMock(spec=socket.socket)
        read_stderr = MagicMock(spec=socket.socket)
        child_stderr.fileno.return_value = 10

        mock_bundle_instance = MagicMock()
        mock_bundle_instance.path = Path("/resolved/bundles/test-bundle")
        mock_resolve_bundle.return_value = mock_bundle_instance

        mock_lock_instance = MagicMock()
        mock_bundle_lock.return_value = mock_lock_instance
        mock_lock_instance.__enter__ = MagicMock(return_value=mock_lock_instance)
        mock_lock_instance.__exit__ = MagicMock(return_value=False)

        startup_payload = {"type": "StartupDetails", "ti": {"task_id": "t1"}}
        mock_ti = MagicMock()
        mock_bundle_info = MagicMock()
        mock_bundle_info.name = "test-bundle"
        mock_bundle_info.version = "v1"
        mock_startup = MagicMock()
        mock_startup.model_dump.return_value = startup_payload

        coordinator = _StubCoordinator(exec_cmd=["test-runtime", "--execute"])
        info = BaseCoordinator.TaskExecutionInfo(
            what=mock_ti,
            dag_rel_path="dags/example.test",
            bundle_info=mock_bundle_info,
            startup_details=mock_startup,
            lang_sdk_msg_schema_version="2026-04-17",
        )

        supervisor_comm = MagicMock(spec=socket.socket)

        with (
            patch("socket.socketpair", return_value=(child_stderr, read_stderr)),
            patch("airflow.sdk.execution_time.coordinator.socket.socket", return_value=supervisor_comm),
        ):
            coordinator._runtime_subprocess_entrypoint(info)

        mock_resolve_bundle.assert_called_once()
        mock_bundle_lock.assert_called_once_with(bundle_name="test-bundle", bundle_version="v1")

        mock_popen.assert_called_once()
        cmd = mock_popen.call_args[0][0]
        assert cmd == ["test-runtime", "--execute", "/resolved/bundles/test-bundle/dags/example.test"]

        # Seed StartupDetails is downgraded once before being written to
        # the runtime socket. The head Pydantic model and the resolved
        # lang_sdk_msg_schema_version both reach ``_send_startup_details``.
        mock_send_startup.assert_called_once_with(runtime_comm, mock_startup, "2026-04-17")
        mock_bridge.assert_called_once()


class _CoordinatorA(BaseCoordinator):
    sdk = "a"
    file_extension = ".a"

    def __init__(self, *, label: str = "a"):
        self.label = label


class _CoordinatorB(BaseCoordinator):
    sdk = "b"
    file_extension = ".b"


class TestCoordinatorManager:
    @pytest.fixture(autouse=True)
    def _reset_cache(self):
        reset_coordinator_manager()
        yield
        reset_coordinator_manager()

    def test_from_config_loads_instances(self, monkeypatch):
        coordinators_json = json.dumps(
            [
                {
                    "name": "alpha",
                    "classpath": f"{_CoordinatorA.__module__}._CoordinatorA",
                    "kwargs": {"label": "alpha-label"},
                },
                {
                    "name": "beta",
                    "classpath": f"{_CoordinatorB.__module__}._CoordinatorB",
                },
            ]
        )
        queue_json = json.dumps({"queue-a": "alpha"})

        monkeypatch.setenv("AIRFLOW__SDK__COORDINATORS", coordinators_json)
        monkeypatch.setenv("AIRFLOW__SDK__QUEUE_TO_COORDINATOR", queue_json)

        from airflow.sdk.configuration import conf

        conf.invalidate_cache()

        manager = CoordinatorManager.from_config()

        alpha = manager.get("alpha")
        beta = manager.get("beta")
        assert isinstance(alpha, _CoordinatorA)
        assert isinstance(beta, _CoordinatorB)
        assert alpha.label == "alpha-label"
        assert {type(c) for c in manager.all()} == {_CoordinatorA, _CoordinatorB}

    def test_from_config_empty(self, monkeypatch):
        monkeypatch.delenv("AIRFLOW__SDK__COORDINATORS", raising=False)
        monkeypatch.delenv("AIRFLOW__SDK__QUEUE_TO_COORDINATOR", raising=False)

        from airflow.sdk.configuration import conf

        conf.invalidate_cache()

        manager = CoordinatorManager.from_config()
        assert manager.all() == []
        assert manager.get("missing") is None

    def test_for_queue_resolves_via_mapping(self):
        coordinator_a = _CoordinatorA()
        coordinator_b = _CoordinatorB()
        manager = CoordinatorManager(
            {"alpha": coordinator_a, "beta": coordinator_b},
            {"queue-a": "alpha", "queue-b": "beta"},
        )

        assert manager.for_queue("queue-a") is coordinator_a
        assert manager.for_queue("queue-b") is coordinator_b
        assert manager.for_queue("queue-missing") is None

    def test_for_dag_file_picks_first_match(self):
        coordinator_a = _CoordinatorA()
        coordinator_b = _CoordinatorB()
        manager = CoordinatorManager({"alpha": coordinator_a, "beta": coordinator_b}, {})

        assert manager.for_dag_file("bundle", "dag.a") is coordinator_a
        assert manager.for_dag_file("bundle", "dag.b") is coordinator_b
        assert manager.for_dag_file("bundle", "dag.py") is None

    def test_for_task_prefers_queue_mapping_over_extension(self):
        # Queue mapping wins even when the dag_rel_path extension matches a different coordinator.
        coordinator_a = _CoordinatorA()
        coordinator_b = _CoordinatorB()
        manager = CoordinatorManager(
            {"alpha": coordinator_a, "beta": coordinator_b},
            {"queue-a": "alpha"},
        )

        assert manager.for_task("queue-a", "dag.b") is coordinator_a

    def test_for_task_falls_back_to_file_extension(self):
        coordinator_a = _CoordinatorA()
        coordinator_b = _CoordinatorB()
        manager = CoordinatorManager({"alpha": coordinator_a, "beta": coordinator_b}, {})

        assert manager.for_task("queue-unmapped", "dag.a") is coordinator_a
        assert manager.for_task("queue-unmapped", "dag.b") is coordinator_b

    def test_for_task_returns_none_when_nothing_matches(self):
        manager = CoordinatorManager({"alpha": _CoordinatorA()}, {})

        assert manager.for_task("queue-unmapped", "dag.py") is None

    def test_file_extensions(self):
        manager = CoordinatorManager({"a": _CoordinatorA(), "b": _CoordinatorB()}, {})
        assert set(manager.file_extensions()) == {".a", ".b"}

    def test_get_coordinator_manager_is_cached(self, monkeypatch):
        monkeypatch.delenv("AIRFLOW__SDK__COORDINATORS", raising=False)

        from airflow.sdk.configuration import conf

        conf.invalidate_cache()

        m1 = get_coordinator_manager()
        m2 = get_coordinator_manager()
        assert m1 is m2
