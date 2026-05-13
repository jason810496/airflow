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
Unit tests for the ``lang_sdk_msg_schema_version`` attribute on ``WatchedSubprocess``.

Pins:

1. Default ``lang_sdk_msg_schema_version`` is ``None`` -- existing behaviour is
   unchanged for Python-runtime children.
2. With ``lang_sdk_msg_schema_version`` set, ``send_msg`` routes head Pydantic
   bodies through the migrator's ``downgrade`` (forwarding
   ``dump_opts`` via ``dump_kwargs``).
3. With ``lang_sdk_msg_schema_version`` set, ``handle_requests`` upgrades incoming
   wire-shape dicts before validating via ``self.decoder``.
4. Unknown or missing body-type discriminators fall through to the
   underlying ``TypeAdapter`` -- same contract as ``CommsDecoder``.
"""

from __future__ import annotations

import contextlib
from typing import ClassVar
from unittest.mock import MagicMock

import psutil
import pytest
import structlog
from pydantic import BaseModel, TypeAdapter
from uuid6 import uuid7

from airflow.sdk.execution_time.comms import _RequestFrame
from airflow.sdk.execution_time.supervisor import WatchedSubprocess

VERSION = "2026-04-17"
OTHER_VERSION = "2026-06-16"
MIGRATOR_PATH = "airflow.sdk.execution_time.supervisor_schemas.get_schema_version_migrator"


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


def _make_ws() -> _StubWatchedSubprocess:
    return _StubWatchedSubprocess(
        id=uuid7(),
        pid=1,
        stdin=MagicMock(),
        process=MagicMock(spec=psutil.Process),
        process_log=structlog.get_logger(),
    )


@pytest.fixture
def ws() -> _StubWatchedSubprocess:
    return _make_ws()


@pytest.fixture
def bound_ws(ws) -> _StubWatchedSubprocess:
    ws.lang_sdk_msg_schema_version = VERSION
    return ws


@pytest.fixture
def mock_migrator(mocker):
    return mocker.patch(MIGRATOR_PATH).return_value


@pytest.fixture
def stub_decoder(ws) -> MagicMock:
    """Replace ``ws.decoder`` with a MagicMock and return it."""
    ws.decoder = MagicMock(spec=TypeAdapter)
    ws.decoder.validate_python.return_value = "validated"
    return ws.decoder


class TestClientVersionDefault:
    def test_default_is_none(self, ws):
        assert ws.lang_sdk_msg_schema_version is None


class TestSendMsgDowngradesWhenBound:
    """When ``lang_sdk_msg_schema_version`` is set, ``send_msg`` routes BaseModel bodies through downgrade."""

    def test_basemodel_body_is_downgraded(self, bound_ws, mock_migrator):
        msg = _FakeBody(payload="hello")
        mock_migrator.downgrade.return_value = {"type": "_FakeBody", "payload": "downgraded"}

        bound_ws.send_msg(msg, request_id=0)

        mock_migrator.downgrade.assert_called_once_with(msg, VERSION, dump_kwargs=None)
        # The wire bytes hit stdin.sendall once.
        bound_ws.stdin.sendall.assert_called_once()

    def test_dump_opts_are_forwarded_to_migrator(self, bound_ws, mock_migrator):
        msg = _FakeBody()
        mock_migrator.downgrade.return_value = {"type": "_FakeBody"}

        bound_ws.send_msg(msg, request_id=0, exclude_unset=True, by_alias=True)

        mock_migrator.downgrade.assert_called_once_with(
            msg, VERSION, dump_kwargs={"exclude_unset": True, "by_alias": True}
        )

    def test_unbound_uses_model_dump_directly(self, ws, mock_migrator):
        msg = MagicMock(spec=BaseModel)
        msg.model_dump.return_value = {"type": "Whatever"}

        ws.send_msg(msg, request_id=0, exclude_unset=True)

        mock_migrator.downgrade.assert_not_called()
        msg.model_dump.assert_called_once_with(exclude_unset=True)


class TestHandleRequestsUpgradesWhenBound:
    """When ``lang_sdk_msg_schema_version`` is set, ``handle_requests`` upgrades dicts before decoding."""

    @staticmethod
    def _drive_one_request(ws: WatchedSubprocess, body) -> None:
        """Feed one request into the ``handle_requests`` generator."""
        gen = ws.handle_requests(structlog.get_logger())
        next(gen)  # prime the generator
        with contextlib.suppress(StopIteration):
            gen.send(_RequestFrame(id=1, body=body))

    def test_upgrade_called_for_known_body_type(self, bound_ws, stub_decoder, mock_migrator):
        wire = {"type": "GetConnection", "conn_id": "c1"}
        upgraded = {**wire, "new_field": True}
        mock_migrator.upgrade.return_value = upgraded

        self._drive_one_request(bound_ws, wire)

        from airflow.sdk.execution_time.comms import GetConnection

        mock_migrator.upgrade.assert_called_once_with(wire, GetConnection, VERSION)
        stub_decoder.validate_python.assert_called_once_with(upgraded)

    @pytest.mark.parametrize(
        ("bind", "wire"),
        [
            pytest.param(True, {"type": "DefinitelyNotAType"}, id="bound-unknown-type"),
            pytest.param(True, {"payload": "x"}, id="bound-missing-type"),
            pytest.param(False, {"type": "GetConnection", "conn_id": "c1"}, id="unbound-known-type"),
        ],
    )
    def test_fall_through_does_not_call_upgrade(self, ws, stub_decoder, mock_migrator, bind, wire):
        if bind:
            ws.lang_sdk_msg_schema_version = VERSION

        self._drive_one_request(ws, wire)

        mock_migrator.upgrade.assert_not_called()
        stub_decoder.validate_python.assert_called_once_with(wire)


class TestClientVersionIsolation:
    """Two WatchedSubprocess instances do not share lang_sdk_msg_schema_version state."""

    def test_two_instances_with_different_versions(self):
        a, b = _make_ws(), _make_ws()
        a.lang_sdk_msg_schema_version = VERSION
        b.lang_sdk_msg_schema_version = OTHER_VERSION
        assert a.lang_sdk_msg_schema_version == VERSION
        assert b.lang_sdk_msg_schema_version == OTHER_VERSION

    @pytest.mark.parametrize("version", [VERSION, OTHER_VERSION, None])
    def test_assigning_does_not_leak_to_class(self, version):
        first = _make_ws()
        first.lang_sdk_msg_schema_version = version
        # A fresh instance still defaults to None.
        assert _make_ws().lang_sdk_msg_schema_version is None
