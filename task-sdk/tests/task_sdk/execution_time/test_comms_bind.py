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
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel, TypeAdapter

from airflow.sdk.execution_time.comms import (
    CommsDecoder,
    ToSupervisor,
    ToTask,
    _RequestFrame,
    _ResponseFrame,
)
from airflow.sdk.execution_time.supervisor_schemas import resolve_body_class


def _new_decoder() -> CommsDecoder[ToTask, ToSupervisor]:
    """Build a decoder with a mock socket so attrs.evolve has something to copy by reference."""
    return CommsDecoder[ToTask, ToSupervisor](socket=MagicMock(spec=_sock))


class _NoTypeBody(BaseModel):
    """Plain body with no ``type`` discriminator -- exercises the unregistered fall-through."""

    payload: str = "x"


class TestBindReturnsNewInstance:
    """``.bind`` is logger-style: returns a new instance, leaves the original alone."""

    def test_unbound_decoder_has_no_lang_sdk_msg_schema_version(self):
        c = _new_decoder()
        assert c.lang_sdk_msg_schema_version is None

    def test_bind_does_not_mutate_original(self):
        c = _new_decoder()
        bound = c.bind(lang_sdk_msg_schema_version="2026-04-17")
        assert c.lang_sdk_msg_schema_version is None
        assert bound.lang_sdk_msg_schema_version == "2026-04-17"
        assert bound is not c

    def test_bind_to_none_drops_the_pin(self):
        c = _new_decoder().bind(lang_sdk_msg_schema_version="2026-04-17")
        cleared = c.bind(lang_sdk_msg_schema_version=None)
        assert cleared.lang_sdk_msg_schema_version is None


class TestBoundDecoderSharesMutableState:
    """
    The new instance shares the socket / locks / counter by reference.

    Documents the constraint: callers MUST discard the unbound
    instance after rebinding. If a caller held both, sends on the
    unbound copy would race the bound copy on the shared socket.
    """

    def test_socket_is_shared(self):
        c = _new_decoder()
        bound = c.bind(lang_sdk_msg_schema_version="2026-04-17")
        assert bound.socket is c.socket

    def test_locks_are_shared(self):
        c = _new_decoder()
        bound = c.bind(lang_sdk_msg_schema_version="2026-04-17")
        assert bound._thread_lock is c._thread_lock
        assert bound._async_lock is c._async_lock

    def test_id_counter_is_shared(self):
        c = _new_decoder()
        bound = c.bind(lang_sdk_msg_schema_version="2026-04-17")
        assert bound.id_counter is c.id_counter

    def test_decoders_are_shared(self):
        c = _new_decoder()
        bound = c.bind(lang_sdk_msg_schema_version="2026-04-17")
        assert bound.body_decoder is c.body_decoder
        assert bound.resp_decoder is c.resp_decoder
        assert bound.err_decoder is c.err_decoder


class TestResolveBodyClass:
    """``resolve_body_class`` reads the ``type`` discriminator into the head class."""

    def test_known_type_resolves_to_class(self):
        cls = resolve_body_class({"type": "StartupDetails"})
        assert cls is not None
        assert cls.__name__ == "StartupDetails"

    def test_unknown_type_returns_none(self):
        assert resolve_body_class({"type": "DefinitelyNotAType"}) is None

    def test_missing_type_returns_none(self):
        assert resolve_body_class({"payload": "x"}) is None

    def test_non_dict_returns_none(self):
        assert resolve_body_class("hello") is None
        assert resolve_body_class(None) is None
        assert resolve_body_class(42) is None

    def test_non_string_type_returns_none(self):
        assert resolve_body_class({"type": 123}) is None


class TestMakeFrameDowngradesWhenBound:
    """When ``lang_sdk_msg_schema_version`` is set, ``_make_frame`` routes through the migrator."""

    def test_downgrade_called_with_pinned_version(self):
        c = _new_decoder().bind(lang_sdk_msg_schema_version="2026-04-17")
        msg = MagicMock(spec=BaseModel)
        wire = {"type": "Whatever", "payload": "downgraded"}
        with patch("airflow.sdk.execution_time.supervisor_schemas.get_schema_version_migrator") as mock_get:
            mock_get.return_value.downgrade.return_value = wire
            frame = c._make_frame(msg)
        mock_get.return_value.downgrade.assert_called_once_with(msg, "2026-04-17")
        assert isinstance(frame, _RequestFrame)
        assert frame.body == wire

    def test_unbound_uses_model_dump_json_mode(self):
        # The mode="json" switch makes datetime / UUID / Path serialise
        # as JSON-safe primitives even on the unbound (head-shape) path,
        # so the wire shape is identical regardless of bind state.
        c = _new_decoder()
        msg = MagicMock(spec=BaseModel)
        msg.model_dump.return_value = {"sentinel": True}
        c._make_frame(msg)
        msg.model_dump.assert_called_once_with(mode="json")


class TestFromFrameUpgradesWhenBound:
    """When ``lang_sdk_msg_schema_version`` is set, ``_from_frame`` routes through the migrator."""

    def test_upgrade_called_for_known_body_type(self):
        c = _new_decoder().bind(lang_sdk_msg_schema_version="2026-04-17")
        c.body_decoder = MagicMock(spec=TypeAdapter)
        c.body_decoder.validate_python.return_value = "validated"
        wire = {"type": "ConnectionResult", "conn_id": "x", "conn_type": "aws"}
        upgraded = {"type": "ConnectionResult", "conn_id": "x", "conn_type": "aws", "extra_field": True}
        frame = _ResponseFrame(id=1, body=wire)
        with patch("airflow.sdk.execution_time.supervisor_schemas.get_schema_version_migrator") as mock_get:
            mock_get.return_value.upgrade.return_value = upgraded
            result = c._from_frame(frame)
        # The upgrade was called with the resolved head class and the pinned version.
        mock_get.return_value.upgrade.assert_called_once()
        body_arg, type_arg, version_arg = mock_get.return_value.upgrade.call_args[0]
        assert body_arg == wire
        assert type_arg.__name__ == "ConnectionResult"
        assert version_arg == "2026-04-17"
        # The decoder received the upgraded (head-shape) body, not the wire dict.
        c.body_decoder.validate_python.assert_called_once_with(upgraded)
        assert result == "validated"

    def test_unknown_type_falls_through_without_calling_upgrade(self):
        c = _new_decoder().bind(lang_sdk_msg_schema_version="2026-04-17")
        c.body_decoder = MagicMock(spec=TypeAdapter)
        c.body_decoder.validate_python.return_value = "validated"
        wire = {"type": "Bogus", "x": 1}
        frame = _ResponseFrame(id=1, body=wire)
        with patch("airflow.sdk.execution_time.supervisor_schemas.get_schema_version_migrator") as mock_get:
            c._from_frame(frame)
        mock_get.return_value.upgrade.assert_not_called()
        # The decoder still got the original wire dict and is left to fail/succeed.
        c.body_decoder.validate_python.assert_called_once_with(wire)

    def test_missing_type_falls_through_without_calling_upgrade(self):
        c = _new_decoder().bind(lang_sdk_msg_schema_version="2026-04-17")
        c.body_decoder = MagicMock(spec=TypeAdapter)
        c.body_decoder.validate_python.return_value = "validated"
        wire = {"x": 1}
        frame = _ResponseFrame(id=1, body=wire)
        with patch("airflow.sdk.execution_time.supervisor_schemas.get_schema_version_migrator") as mock_get:
            c._from_frame(frame)
        mock_get.return_value.upgrade.assert_not_called()
        c.body_decoder.validate_python.assert_called_once_with(wire)

    def test_unbound_decoder_does_not_call_upgrade(self):
        c = _new_decoder()  # lang_sdk_msg_schema_version is None
        c.body_decoder = MagicMock(spec=TypeAdapter)
        c.body_decoder.validate_python.return_value = "validated"
        wire = {"type": "ConnectionResult", "conn_id": "x", "conn_type": "aws"}
        frame = _ResponseFrame(id=1, body=wire)
        with patch("airflow.sdk.execution_time.supervisor_schemas.get_schema_version_migrator") as mock_get:
            c._from_frame(frame)
        mock_get.return_value.upgrade.assert_not_called()
        c.body_decoder.validate_python.assert_called_once_with(wire)

    def test_none_body_returns_none_without_calling_upgrade(self):
        c = _new_decoder().bind(lang_sdk_msg_schema_version="2026-04-17")
        frame = _ResponseFrame(id=1, body=None)
        with patch("airflow.sdk.execution_time.supervisor_schemas.get_schema_version_migrator") as mock_get:
            result = c._from_frame(frame)
        mock_get.return_value.upgrade.assert_not_called()
        assert result is None


class TestConcurrentBindsDoNotStomp:
    """
    Two independent ``CommsDecoder`` instances with different bindings
    are fully isolated -- ``lang_sdk_msg_schema_version`` is per-instance, not class-level.
    """

    def test_two_instances_with_different_versions(self):
        a = _new_decoder().bind(lang_sdk_msg_schema_version="2026-04-17")
        b = _new_decoder().bind(lang_sdk_msg_schema_version="2026-06-16")
        assert a.lang_sdk_msg_schema_version == "2026-04-17"
        assert b.lang_sdk_msg_schema_version == "2026-06-16"
        # They also do not share sockets (each was constructed with its own MagicMock).
        assert a.socket is not b.socket

    def test_rebinding_one_does_not_affect_the_other(self):
        a = _new_decoder().bind(lang_sdk_msg_schema_version="2026-04-17")
        b = _new_decoder().bind(lang_sdk_msg_schema_version="2026-06-16")
        a2 = a.bind(lang_sdk_msg_schema_version=None)
        assert a2.lang_sdk_msg_schema_version is None
        assert a.lang_sdk_msg_schema_version == "2026-04-17"  # original untouched
        assert b.lang_sdk_msg_schema_version == "2026-06-16"  # other instance untouched


class TestUnregisteredFallThroughIsConsistentWithMigrator:
    """
    The same "unregistered body passes through" contract the migrator
    advertises: a body whose ``type`` is not in ``registered_models``
    must reach the underlying ``TypeAdapter`` unmodified.
    """

    def test_unregistered_body_class_unmodified(self):
        c = _new_decoder().bind(lang_sdk_msg_schema_version="2026-04-17")
        c.body_decoder = MagicMock(spec=TypeAdapter)
        c.body_decoder.validate_python.return_value = "validated"
        frame = _ResponseFrame(id=1, body={"type": "_NoTypeBody", "payload": "x"})
        with patch("airflow.sdk.execution_time.supervisor_schemas.get_schema_version_migrator") as mock_get:
            c._from_frame(frame)
        mock_get.return_value.upgrade.assert_not_called()


@pytest.mark.parametrize(
    "version",
    ["2026-04-17", "2026-06-16", None],
)
def test_bind_round_trips_through_attrs_evolve(version):
    """``.bind`` accepts any string or None and round-trips identity correctly."""
    c = _new_decoder()
    bound = c.bind(lang_sdk_msg_schema_version=version)
    assert bound.lang_sdk_msg_schema_version == version
    # The chain ``c.bind(None).bind("X").bind(None)`` ends with no pin.
    assert (
        c.bind(lang_sdk_msg_schema_version=None)
        .bind(lang_sdk_msg_schema_version="X")
        .bind(lang_sdk_msg_schema_version=None)
        .lang_sdk_msg_schema_version
        is None
    )
