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
Unit tests for the ``client_version`` attribute on ``WatchedSubprocess``.

Pins:

1. Default ``client_version`` is ``None`` -- existing behaviour is
   unchanged for Python-runtime children.
2. With ``client_version`` set, ``send_msg`` routes head Pydantic
   bodies through the migrator's ``downgrade`` (forwarding
   ``dump_opts`` via ``dump_kwargs``).
3. With ``client_version`` set, ``handle_requests`` upgrades incoming
   wire-shape dicts before validating via ``self.decoder``.
4. Unknown or missing body-type discriminators fall through to the
   underlying ``TypeAdapter`` -- same contract as ``CommsDecoder``.
"""

from __future__ import annotations

import contextlib
from typing import Any, ClassVar, cast
from unittest.mock import MagicMock, patch

import psutil
import pytest
import structlog
from pydantic import BaseModel, TypeAdapter
from uuid6 import uuid7

from airflow.sdk.execution_time.comms import _RequestFrame
from airflow.sdk.execution_time.supervisor import WatchedSubprocess


class _FakeBody(BaseModel):
    """Synthetic Pydantic body for testing send_msg downgrade routing."""

    type: str = "_FakeBody"
    payload: str = "x"


class _StubWatchedSubprocess(WatchedSubprocess):
    """
    Concrete subclass for tests.

    ``WatchedSubprocess.decoder`` is a ``ClassVar`` that real subclasses
    (``ActivitySubprocess``, ``DagFileProcessorProcess``) override with
    a ``TypeAdapter`` for their request union. Tests patch
    ``cls.decoder`` to a ``MagicMock`` to assert on validate calls.
    """

    decoder: ClassVar[TypeAdapter] = TypeAdapter(int)  # placeholder; tests patch on the instance

    def _handle_request(self, msg, log, req_id):
        # No-op: tests assert on the pre-dispatch decode pipeline only.
        return


def _new_watched_subprocess() -> _StubWatchedSubprocess:
    """Construct a WatchedSubprocess with mocked dependencies for unit tests."""
    return _StubWatchedSubprocess(
        id=uuid7(),
        pid=1,
        stdin=MagicMock(),
        process=MagicMock(spec=psutil.Process),
        process_log=structlog.get_logger(),
    )


class TestClientVersionDefault:
    def test_default_is_none(self):
        ws = _new_watched_subprocess()
        assert ws.client_version is None


class TestSendMsgDowngradesWhenBound:
    """When ``client_version`` is set, ``send_msg`` routes BaseModel bodies through downgrade."""

    def test_basemodel_body_is_downgraded(self):
        ws = _new_watched_subprocess()
        ws.client_version = "2026-04-17"
        msg = _FakeBody(payload="hello")
        wire = {"type": "_FakeBody", "payload": "downgraded"}
        with patch("airflow.sdk.execution_time.supervisor_schemas.get_schema_version_migrator") as mock_get:
            mock_get.return_value.downgrade.return_value = wire
            ws.send_msg(msg, request_id=0)
        mock_get.return_value.downgrade.assert_called_once_with(msg, "2026-04-17", dump_kwargs=None)
        # The wire bytes hit stdin.sendall once.
        ws.stdin.sendall.assert_called_once()

    def test_dump_opts_are_forwarded_to_migrator(self):
        ws = _new_watched_subprocess()
        ws.client_version = "2026-04-17"
        msg = _FakeBody()
        with patch("airflow.sdk.execution_time.supervisor_schemas.get_schema_version_migrator") as mock_get:
            mock_get.return_value.downgrade.return_value = {"type": "_FakeBody"}
            ws.send_msg(msg, request_id=0, exclude_unset=True, by_alias=True)
        mock_get.return_value.downgrade.assert_called_once_with(
            msg, "2026-04-17", dump_kwargs={"exclude_unset": True, "by_alias": True}
        )

    def test_unbound_uses_model_dump_directly(self):
        ws = _new_watched_subprocess()  # client_version is None
        msg = MagicMock(spec=BaseModel)
        msg.model_dump.return_value = {"type": "Whatever"}
        with patch("airflow.sdk.execution_time.supervisor_schemas.get_schema_version_migrator") as mock_get:
            ws.send_msg(msg, request_id=0, exclude_unset=True)
        mock_get.return_value.downgrade.assert_not_called()
        msg.model_dump.assert_called_once_with(exclude_unset=True)


class TestHandleRequestsUpgradesWhenBound:
    """When ``client_version`` is set, ``handle_requests`` upgrades dicts before decoding."""

    def _drive_one_request(self, ws: WatchedSubprocess, body) -> Any:
        """Feed one request into the handle_requests generator and return what self.decoder saw."""
        log = structlog.get_logger()
        gen = ws.handle_requests(log)
        next(gen)  # prime the generator
        with contextlib.suppress(StopIteration):
            gen.send(_RequestFrame(id=1, body=body))
        decoder = cast("MagicMock", ws.decoder)
        return decoder.validate_python.call_args

    def test_upgrade_called_for_known_body_type(self):
        ws = _new_watched_subprocess()
        ws.client_version = "2026-04-17"
        ws.decoder = MagicMock(spec=TypeAdapter)
        ws.decoder.validate_python.return_value = "validated"
        wire = {"type": "GetConnection", "conn_id": "c1"}
        upgraded = {"type": "GetConnection", "conn_id": "c1", "new_field": True}
        with patch("airflow.sdk.execution_time.supervisor_schemas.get_schema_version_migrator") as mock_get:
            mock_get.return_value.upgrade.return_value = upgraded
            self._drive_one_request(ws, wire)
        # Migrator was called with the resolved head class.
        mock_get.return_value.upgrade.assert_called_once()
        body_arg, type_arg, version_arg = mock_get.return_value.upgrade.call_args[0]
        assert body_arg == wire
        assert type_arg.__name__ == "GetConnection"
        assert version_arg == "2026-04-17"
        ws.decoder.validate_python.assert_called_once_with(upgraded)

    def test_unknown_body_type_falls_through(self):
        ws = _new_watched_subprocess()
        ws.client_version = "2026-04-17"
        ws.decoder = MagicMock(spec=TypeAdapter)
        ws.decoder.validate_python.return_value = "validated"
        wire = {"type": "DefinitelyNotAType"}
        with patch("airflow.sdk.execution_time.supervisor_schemas.get_schema_version_migrator") as mock_get:
            self._drive_one_request(ws, wire)
        mock_get.return_value.upgrade.assert_not_called()
        ws.decoder.validate_python.assert_called_once_with(wire)

    def test_missing_type_falls_through(self):
        ws = _new_watched_subprocess()
        ws.client_version = "2026-04-17"
        ws.decoder = MagicMock(spec=TypeAdapter)
        ws.decoder.validate_python.return_value = "validated"
        wire = {"payload": "x"}
        with patch("airflow.sdk.execution_time.supervisor_schemas.get_schema_version_migrator") as mock_get:
            self._drive_one_request(ws, wire)
        mock_get.return_value.upgrade.assert_not_called()
        ws.decoder.validate_python.assert_called_once_with(wire)

    def test_unbound_does_not_call_upgrade(self):
        ws = _new_watched_subprocess()  # client_version is None
        ws.decoder = MagicMock(spec=TypeAdapter)
        ws.decoder.validate_python.return_value = "validated"
        wire = {"type": "GetConnection", "conn_id": "c1"}
        with patch("airflow.sdk.execution_time.supervisor_schemas.get_schema_version_migrator") as mock_get:
            self._drive_one_request(ws, wire)
        mock_get.return_value.upgrade.assert_not_called()
        ws.decoder.validate_python.assert_called_once_with(wire)


class TestClientVersionIsolation:
    """Two WatchedSubprocess instances do not share client_version state."""

    def test_two_instances_with_different_versions(self):
        a = _new_watched_subprocess()
        b = _new_watched_subprocess()
        a.client_version = "2026-04-17"
        b.client_version = "2026-06-16"
        assert a.client_version == "2026-04-17"
        assert b.client_version == "2026-06-16"

    @pytest.mark.parametrize("version", ["2026-04-17", "2026-06-16", None])
    def test_assigning_does_not_leak_to_class(self, version):
        ws = _new_watched_subprocess()
        ws.client_version = version
        # A fresh instance still defaults to None.
        ws2 = _new_watched_subprocess()
        assert ws2.client_version is None
