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
Unit tests for :meth:`CommsDecoder.bind` and the migrator-gated frame
paths in :mod:`airflow.sdk.execution_time.comms`.

Pin three contracts:

1. ``bind`` is logger-style immutable -- the original instance keeps
   ``lang_sdk_msg_schema_version is None`` and the returned copy shares the same
   socket / lock / counter identity.
2. When ``lang_sdk_msg_schema_version`` is set, ``_make_frame`` routes the body
   through ``SchemaVersionMigrator.downgrade`` and ``_from_frame``
   routes through ``.upgrade``. When ``None`` (unbound), ``_make_frame``
   uses ``model_dump(mode="json")`` -- the wire shape must be stable
   across bound / unbound paths.
3. Unknown / missing ``type`` discriminators fall through to the
   default ``TypeAdapter`` validation (matches the migrator's
   "unregistered body" contract).
"""

from __future__ import annotations

from socket import socket as _sock
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel, TypeAdapter

from airflow.sdk.execution_time.comms import (
    CommsDecoder,
    ConnectionResult,
    StartupDetails,
    ToSupervisor,
    ToTask,
    _RequestFrame,
    _ResponseFrame,
)
from airflow.sdk.execution_time.supervisor_schemas import resolve_body_class

VERSION = "2026-04-17"
OTHER_VERSION = "2026-06-16"
MIGRATOR_PATH = "airflow.sdk.execution_time.supervisor_schemas.get_schema_version_migrator"


def _make_decoder() -> CommsDecoder[ToTask, ToSupervisor]:
    """Build a decoder with a mock socket so attrs.evolve has something to copy by reference."""
    return CommsDecoder[ToTask, ToSupervisor](socket=MagicMock(spec=_sock))


@pytest.fixture
def decoder() -> CommsDecoder[ToTask, ToSupervisor]:
    return _make_decoder()


@pytest.fixture
def bound_decoder(decoder) -> CommsDecoder[ToTask, ToSupervisor]:
    return decoder.bind(lang_sdk_msg_schema_version=VERSION)


@pytest.fixture
def mock_migrator(mocker):
    """Patch ``get_schema_version_migrator`` and return the migrator instance."""
    return mocker.patch(MIGRATOR_PATH).return_value


class TestBindReturnsNewInstance:
    """``.bind`` is logger-style: returns a new instance, leaves the original alone."""

    def test_unbound_decoder_has_no_lang_sdk_msg_schema_version(self, decoder):
        assert decoder.lang_sdk_msg_schema_version is None

    def test_bind_does_not_mutate_original(self, decoder):
        bound = decoder.bind(lang_sdk_msg_schema_version=VERSION)
        assert decoder.lang_sdk_msg_schema_version is None
        assert bound.lang_sdk_msg_schema_version == VERSION
        assert bound is not decoder

    def test_bind_to_none_drops_the_pin(self, bound_decoder):
        cleared = bound_decoder.bind(lang_sdk_msg_schema_version=None)
        assert cleared.lang_sdk_msg_schema_version is None


class TestBoundDecoderSharesMutableState:
    """
    The new instance shares the socket / locks / counter by reference.

    Documents the constraint: callers MUST discard the unbound
    instance after rebinding. If a caller held both, sends on the
    unbound copy would race the bound copy on the shared socket.
    """

    @pytest.mark.parametrize(
        "attr",
        [
            "socket",
            "_thread_lock",
            "_async_lock",
            "id_counter",
            "body_decoder",
            "resp_decoder",
            "err_decoder",
        ],
    )
    def test_attribute_is_shared_by_reference(self, decoder, bound_decoder, attr):
        assert getattr(bound_decoder, attr) is getattr(decoder, attr)


class TestResolveBodyClass:
    """``resolve_body_class`` reads the ``type`` discriminator into the head class."""

    def test_known_type_resolves_to_class(self):
        assert resolve_body_class({"type": "StartupDetails"}) is StartupDetails

    @pytest.mark.parametrize(
        "body",
        [
            pytest.param({"type": "DefinitelyNotAType"}, id="unknown-type"),
            pytest.param({"payload": "x"}, id="missing-type"),
            pytest.param({"type": 123}, id="non-string-type"),
            pytest.param("hello", id="string-body"),
            pytest.param(None, id="none-body"),
            pytest.param(42, id="int-body"),
        ],
    )
    def test_unresolved_body_returns_none(self, body):
        assert resolve_body_class(body) is None


class TestMakeFrameDowngradesWhenBound:
    """When ``lang_sdk_msg_schema_version`` is set, ``_make_frame`` routes through the migrator."""

    def test_downgrade_called_with_pinned_version(self, bound_decoder, mock_migrator):
        msg = MagicMock(spec=BaseModel)
        wire = {"type": "Whatever", "payload": "downgraded"}
        mock_migrator.downgrade.return_value = wire

        frame = bound_decoder._make_frame(msg)

        mock_migrator.downgrade.assert_called_once_with(msg, VERSION)
        assert isinstance(frame, _RequestFrame)
        assert frame.body == wire

    def test_unbound_uses_model_dump_json_mode(self, decoder):
        # The mode="json" switch makes datetime / UUID / Path serialise
        # as JSON-safe primitives even on the unbound (head-shape) path,
        # so the wire shape is identical regardless of bind state.
        msg = MagicMock(spec=BaseModel)
        msg.model_dump.return_value = {"sentinel": True}

        decoder._make_frame(msg)

        msg.model_dump.assert_called_once_with(mode="json")


class TestFromFrameUpgradesWhenBound:
    """When ``lang_sdk_msg_schema_version`` is set, ``_from_frame`` routes through the migrator."""

    @staticmethod
    def _stub_body_decoder(decoder) -> MagicMock:
        decoder.body_decoder = MagicMock(spec=TypeAdapter)
        decoder.body_decoder.validate_python.return_value = "validated"
        return decoder.body_decoder

    def test_upgrade_called_for_known_body_type(self, bound_decoder, mock_migrator):
        body_decoder = self._stub_body_decoder(bound_decoder)
        wire = {"type": "ConnectionResult", "conn_id": "x", "conn_type": "aws"}
        upgraded = {**wire, "extra_field": True}
        mock_migrator.upgrade.return_value = upgraded

        result = bound_decoder._from_frame(_ResponseFrame(id=1, body=wire))

        mock_migrator.upgrade.assert_called_once_with(wire, ConnectionResult, VERSION)
        # The decoder received the upgraded (head-shape) body, not the wire dict.
        body_decoder.validate_python.assert_called_once_with(upgraded)
        assert result == "validated"

    def test_unbound_known_type_does_not_call_upgrade(self, decoder, mock_migrator):
        # Without a pin the migrator must not run; the raw wire dict is
        # handed straight to the head decoder.
        wire = {"type": "ConnectionResult", "conn_id": "x", "conn_type": "aws"}
        body_decoder = self._stub_body_decoder(decoder)

        decoder._from_frame(_ResponseFrame(id=1, body=wire))

        mock_migrator.upgrade.assert_not_called()
        body_decoder.validate_python.assert_called_once_with(wire)

    @pytest.mark.parametrize(
        "wire",
        [
            pytest.param({"type": "Bogus", "x": 1}, id="unknown-type"),
            pytest.param({"x": 1}, id="missing-type"),
            pytest.param({"type": "_NoTypeBody", "payload": "x"}, id="unregistered-class"),
        ],
    )
    def test_bound_unresolved_body_type_raises(self, decoder, mock_migrator, wire):
        # With the lang-SDK pin active, an unresolvable wire ``type`` is a
        # protocol-contract violation: the runtime and supervisor disagree
        # on which Pydantic class the body should validate against. The
        # migrator must not run, and the head decoder must not be reached
        # with a body of unknown shape.
        decoder = decoder.bind(lang_sdk_msg_schema_version=VERSION)
        body_decoder = self._stub_body_decoder(decoder)

        with pytest.raises(ValueError, match="Cannot resolve head Pydantic class"):
            decoder._from_frame(_ResponseFrame(id=1, body=wire))

        mock_migrator.upgrade.assert_not_called()
        body_decoder.validate_python.assert_not_called()

    def test_none_body_returns_none_without_calling_upgrade(self, bound_decoder, mock_migrator):
        result = bound_decoder._from_frame(_ResponseFrame(id=1, body=None))

        mock_migrator.upgrade.assert_not_called()
        assert result is None


class TestConcurrentBindsDoNotStomp:
    """
    Two independent ``CommsDecoder`` instances with different bindings
    are fully isolated -- ``lang_sdk_msg_schema_version`` is per-instance, not class-level.
    """

    def test_two_instances_with_different_versions(self):
        a = _make_decoder().bind(lang_sdk_msg_schema_version=VERSION)
        b = _make_decoder().bind(lang_sdk_msg_schema_version=OTHER_VERSION)
        assert a.lang_sdk_msg_schema_version == VERSION
        assert b.lang_sdk_msg_schema_version == OTHER_VERSION
        # They also do not share sockets (each was constructed with its own MagicMock).
        assert a.socket is not b.socket

    def test_rebinding_one_does_not_affect_the_other(self):
        a = _make_decoder().bind(lang_sdk_msg_schema_version=VERSION)
        b = _make_decoder().bind(lang_sdk_msg_schema_version=OTHER_VERSION)
        a_cleared = a.bind(lang_sdk_msg_schema_version=None)
        assert a_cleared.lang_sdk_msg_schema_version is None
        assert a.lang_sdk_msg_schema_version == VERSION  # original untouched
        assert b.lang_sdk_msg_schema_version == OTHER_VERSION  # other instance untouched


@pytest.mark.parametrize("version", [VERSION, OTHER_VERSION, None])
def test_bind_round_trips_through_attrs_evolve(decoder, version):
    """``.bind`` accepts any string or None and round-trips identity correctly."""
    bound = decoder.bind(lang_sdk_msg_schema_version=version)
    assert bound.lang_sdk_msg_schema_version == version
    # The chain ``c.bind(None).bind("X").bind(None)`` ends with no pin.
    chained = (
        decoder.bind(lang_sdk_msg_schema_version=None)
        .bind(lang_sdk_msg_schema_version="X")
        .bind(lang_sdk_msg_schema_version=None)
    )
    assert chained.lang_sdk_msg_schema_version is None
