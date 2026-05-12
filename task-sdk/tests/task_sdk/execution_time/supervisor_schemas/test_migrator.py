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
Unit tests for :mod:`airflow.sdk.execution_time.supervisor_schemas.migrator`.

These pin the in-process supervisor schema migration path -- both
directions: ``downgrade`` (supervisor head -> foreign-runtime client
version) and ``upgrade`` (foreign-runtime client version -> supervisor
head). The downgrade direction is what coordinators use to hand a
runtime a body shaped for its build; the upgrade direction is what the
supervisor will use to decode runtime-originated frames once the wire
schema diverges from head.
"""

from __future__ import annotations

from datetime import date
from typing import Literal

import pytest
from cadwyn import (
    HeadVersion,
    Version,
    VersionBundle,
    VersionChange,
    convert_request_to_next_version_for,
    schema,
)
from pydantic import BaseModel

from airflow.sdk.execution_time.supervisor_schemas import (
    SchemaVersionMigrator,
    get_schema_version_migrator,
)


class _SyntheticBody(BaseModel):
    """Synthetic mirror of an IPC body to drive bundle-level migration tests."""

    type: Literal["SyntheticBody"] = "SyntheticBody"
    ti_id: str
    queue_capacity: int | None = None
    sentry_trace_id: str | None = None


class _IntroduceQueueCapacity(VersionChange):
    """3026-04-17: introduce queue_capacity."""

    description = __doc__
    instructions_to_migrate_to_previous_version = (
        schema(_SyntheticBody).field("queue_capacity").didnt_exist,
    )

    # Upgrade direction: a client on the pre-04-17 wire shape sends no
    # ``queue_capacity``; once the body crosses into 04-17 we backfill
    # the field with a sentinel so the head Pydantic class can validate.
    @convert_request_to_next_version_for(_SyntheticBody)  # type: ignore[arg-type]
    def _backfill_queue_capacity(request):
        request.body.setdefault("queue_capacity", 0)


class _IntroduceSentryTrace(VersionChange):
    """3026-06-16: introduce sentry_trace_id."""

    description = __doc__
    instructions_to_migrate_to_previous_version = (
        schema(_SyntheticBody).field("sentry_trace_id").didnt_exist,
    )

    @convert_request_to_next_version_for(_SyntheticBody)  # type: ignore[arg-type]
    def _backfill_sentry_trace(request):
        request.body.setdefault("sentry_trace_id", "")


_BUNDLE = VersionBundle(
    HeadVersion(),
    Version("3026-06-16", _IntroduceSentryTrace),
    Version("3026-04-17", _IntroduceQueueCapacity),
    Version("3025-01-01"),
)


class TestSchemaVersionMigratorDowngradeAgainstSyntheticBundle:
    """
    Drive the downgrade direction against a synthetic bundle so we can
    pin *field-level* migration behaviour. The real supervisor bundle
    has no schema-level migrations on the IPC bodies yet, so it would
    no-op every version -- which proves nothing about the migration
    chain. The synthetic bundle's mechanism is identical to the real
    one, so what we prove about it applies to the real bundle the
    moment a ``schema(...)`` instruction lands.
    """

    @pytest.fixture
    def migrator(self) -> SchemaVersionMigrator:
        # Default server_version picks up the latest dated entry
        # (``3026-06-16``) -- the same anchor the real supervisor uses.
        return SchemaVersionMigrator(_BUNDLE)

    def _body(self) -> _SyntheticBody:
        return _SyntheticBody(
            ti_id="t1",
            queue_capacity=8,
            sentry_trace_id="00-trace-span-00",
        )

    def test_server_version_defaults_to_latest_dated_entry(self, migrator):
        # ``bundle.versions`` is newest -> oldest; the supervisor's
        # head wire shape is the latest released date.
        assert migrator.server_version == "3026-06-16"

    def test_head_version_returns_every_field(self, migrator):
        out = migrator.downgrade(self._body(), "3026-06-16")
        assert out["ti_id"] == "t1"
        assert out["queue_capacity"] == 8
        assert out["sentry_trace_id"] == "00-trace-span-00"

    def test_middle_version_strips_only_later_fields(self, migrator):
        # 3026-04-17 predates sentry_trace_id but knows about queue_capacity.
        out = migrator.downgrade(self._body(), "3026-04-17")
        assert out["queue_capacity"] == 8
        assert "sentry_trace_id" not in out

    def test_baseline_strips_every_later_field(self, migrator):
        out = migrator.downgrade(self._body(), "3025-01-01")
        assert out["ti_id"] == "t1"
        assert "queue_capacity" not in out
        assert "sentry_trace_id" not in out

    def test_accepts_python_date_target(self, migrator):
        # A ``date`` instance must be mapped to the closest lesser version.
        out = migrator.downgrade(self._body(), date(3026, 5, 1))
        # Between 04-17 and 06-16, so closest-lesser is 04-17:
        # queue_capacity stays, sentry_trace_id is stripped.
        assert "queue_capacity" in out
        assert "sentry_trace_id" not in out

    def test_rejects_non_basemodel_input(self, migrator):
        with pytest.raises(TypeError, match="pydantic BaseModel"):
            migrator.downgrade({"not": "a model"}, "3025-01-01")  # type: ignore[arg-type]

    def test_passes_through_models_with_no_registered_instructions(self, migrator):
        # A model that is *not* mentioned by any ``schema(...)``
        # instruction in the bundle is still a legal argument: the
        # by-type lookup misses on every version, so the body is
        # returned verbatim. This matches the current state of the
        # real IPC bodies (StartupDetails, DagFileParseRequest),
        # which have no field-level migrations registered yet.
        class _Unregistered(BaseModel):
            value: int

        out = migrator.downgrade(_Unregistered(value=42), "3025-01-01")
        assert out == {"value": 42}


class TestSchemaVersionMigratorUpgradeAgainstSyntheticBundle:
    """
    Mirror of the downgrade suite for the upgrade direction. The
    synthetic bundle's ``convert_request_to_next_version_for`` hooks
    backfill the new field at the version that introduces it, so a
    body off an older wire reaches the head with every field present.
    """

    @pytest.fixture
    def migrator(self) -> SchemaVersionMigrator:
        return SchemaVersionMigrator(_BUNDLE)

    def test_baseline_client_payload_is_filled_up_to_head(self, migrator):
        # A client on the very first defined version sends only the
        # always-present field. Both 04-17 and 06-16 must run, each
        # backfilling its own newly-introduced field.
        out = migrator.upgrade({"ti_id": "t1"}, _SyntheticBody, "3025-01-01")
        assert out["ti_id"] == "t1"
        assert out["queue_capacity"] == 0
        assert out["sentry_trace_id"] == ""

    def test_middle_client_payload_only_runs_later_versions(self, migrator):
        # Client built against 04-17 already provides queue_capacity;
        # only the 06-16 backfill should run on top.
        out = migrator.upgrade(
            {"ti_id": "t1", "queue_capacity": 8},
            _SyntheticBody,
            "3026-04-17",
        )
        assert out["queue_capacity"] == 8  # the existing value is preserved
        assert out["sentry_trace_id"] == ""  # backfilled by 06-16

    def test_head_client_payload_is_returned_verbatim(self, migrator):
        # A client already on head needs no upgrade.
        original = {"ti_id": "t1", "queue_capacity": 8, "sentry_trace_id": "00"}
        out = migrator.upgrade(dict(original), _SyntheticBody, "3026-06-16")
        assert out == original

    def test_accepts_python_date_target(self, migrator):
        # 3026-05-01 -> closest-lesser is 3026-04-17, so only the
        # 06-16 backfill should run.
        out = migrator.upgrade(
            {"ti_id": "t1", "queue_capacity": 8},
            _SyntheticBody,
            date(3026, 5, 1),
        )
        assert out["queue_capacity"] == 8
        assert out["sentry_trace_id"] == ""

    def test_rejects_non_dict_input(self, migrator):
        with pytest.raises(TypeError, match="dict payload"):
            migrator.upgrade("not a dict", _SyntheticBody, "3025-01-01")  # type: ignore[arg-type]

    def test_passes_through_unregistered_body_types(self, migrator):
        # An unknown body type misses the by-type lookup on every
        # version change, so the payload is returned unchanged --
        # the same passthrough semantics as the downgrade path.
        class _Unregistered(BaseModel):
            value: int

        out = migrator.upgrade({"value": 42}, _Unregistered, "3025-01-01")
        assert out == {"value": 42}


class TestSchemaVersionMigratorRespectsExplicitServerVersion:
    """
    A migrator pinned to an older ``server_version`` must stop walking
    once the chain reaches that anchor. This is the knob a coordinator
    on a non-head build would use to clamp the upgrade walk so that
    transformers above its own version are not applied.

    Only the upgrade direction is asserted here: the downgrade walk
    delegates the final field-shape to ``generate_versioned_models``
    keyed by *client_version*, which is independent of the server
    anchor, so the anchor has no observable effect when the inbound
    body is already shaped for *server_version*.
    """

    def test_upgrade_does_not_apply_changes_above_server_anchor(self):
        migrator = SchemaVersionMigrator(_BUNDLE, server_version="3026-04-17")
        out = migrator.upgrade({"ti_id": "t1"}, _SyntheticBody, "3025-01-01")
        # The 04-17 backfill ran; the 06-16 backfill did not.
        assert out["queue_capacity"] == 0
        assert "sentry_trace_id" not in out

    def test_unknown_version_string_raises(self):
        migrator = SchemaVersionMigrator(_BUNDLE)
        with pytest.raises(ValueError, match="not found in bundle"):
            migrator.downgrade(_SyntheticBody(ti_id="t1"), "2999-01-01")


class TestGetSchemaVersionMigrator:
    def test_returns_singleton(self):
        # The cached factory must return the same instance across calls
        # so callers can share state-free migrator instances cheaply.
        assert get_schema_version_migrator() is get_schema_version_migrator()

    def test_is_bound_to_supervisor_bundle(self):
        # Sanity check: the singleton uses the real supervisor IPC
        # bundle, not a synthetic one and not the execution-API HTTP
        # bundle. A regression here would silently detach the
        # supervisor from its versioning source of truth.
        from airflow.sdk.execution_time.supervisor_schemas.versions import bundle

        assert get_schema_version_migrator()._bundle is bundle

    def test_server_version_defaults_to_real_bundle_head(self):
        # The supervisor anchor must be the latest dated entry in the
        # real bundle -- never the head sentinel, never silently older.
        from airflow.sdk.execution_time.supervisor_schemas.versions import bundle

        assert get_schema_version_migrator().server_version == bundle.versions[0].value
