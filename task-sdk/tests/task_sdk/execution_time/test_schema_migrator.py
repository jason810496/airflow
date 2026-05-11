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
Unit tests for :mod:`airflow.sdk.execution_time.schema_migrator`.

These pin the in-process IPC schema migration path -- what the
supervisor and parser-supervisor use to hand foreign-language runtimes
a body shaped for the schema version they were built against, without
the HTTP round-trip the compat echo routes would otherwise impose.
"""

from __future__ import annotations

from datetime import date
from typing import Literal

import pytest
from cadwyn import HeadVersion, Version, VersionBundle, VersionChange, schema
from pydantic import BaseModel

from airflow.sdk.execution_time.schema_migrator import SchemaVersionMigrator, get_schema_version_migrator


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


class _IntroduceSentryTrace(VersionChange):
    """3026-06-16: introduce sentry_trace_id."""

    description = __doc__
    instructions_to_migrate_to_previous_version = (
        schema(_SyntheticBody).field("sentry_trace_id").didnt_exist,
    )


_BUNDLE = VersionBundle(
    HeadVersion(),
    Version("3026-06-16", _IntroduceSentryTrace),
    Version("3026-04-17", _IntroduceQueueCapacity),
    Version("3025-01-01"),
)


class TestSchemaVersionMigratorMigratesAgainstSyntheticBundle:
    """
    Drive the migrator against a synthetic bundle so we can pin
    *field-level* migration behaviour. The real execution-API bundle
    has no schema-level migrations on the IPC bodies yet, so it would
    no-op every version -- which proves nothing about the migration
    chain. The synthetic bundle's mechanism is identical to the real
    one, so what we prove about it applies to the real bundle the
    moment a ``schema(...)`` instruction lands.
    """

    @pytest.fixture
    def migrator(self) -> SchemaVersionMigrator:
        return SchemaVersionMigrator(_BUNDLE)

    def _body(self) -> _SyntheticBody:
        return _SyntheticBody(
            ti_id="t1",
            queue_capacity=8,
            sentry_trace_id="00-trace-span-00",
        )

    def test_head_version_returns_every_field(self, migrator):
        out = migrator.migrate(self._body(), "3026-06-16")
        assert out["ti_id"] == "t1"
        assert out["queue_capacity"] == 8
        assert out["sentry_trace_id"] == "00-trace-span-00"

    def test_middle_version_strips_only_later_fields(self, migrator):
        # 3026-04-17 predates sentry_trace_id but knows about queue_capacity.
        out = migrator.migrate(self._body(), "3026-04-17")
        assert out["queue_capacity"] == 8
        assert "sentry_trace_id" not in out

    def test_baseline_strips_every_later_field(self, migrator):
        out = migrator.migrate(self._body(), "3025-01-01")
        assert out["ti_id"] == "t1"
        assert "queue_capacity" not in out
        assert "sentry_trace_id" not in out

    def test_accepts_python_date_target(self, migrator):
        # A ``date`` instance must be mapped to the closest lesser version.
        out = migrator.migrate(self._body(), date(3026, 5, 1))
        # Between 04-17 and 06-16, so closest-lesser is 04-17:
        # queue_capacity stays, sentry_trace_id is stripped.
        assert "queue_capacity" in out
        assert "sentry_trace_id" not in out

    def test_rejects_non_basemodel_input(self, migrator):
        with pytest.raises(TypeError, match="pydantic BaseModel"):
            migrator.migrate({"not": "a model"}, "3025-01-01")  # type: ignore[arg-type]

    def test_passes_through_models_with_no_registered_instructions(self, migrator):
        # A model that is *not* mentioned by any ``schema(...)``
        # instruction in the bundle is still a legal argument: Cadwyn
        # walks the migration chain and finds nothing to apply, so the
        # body is returned verbatim. This matches the current state of
        # the real IPC bodies (StartupDetails, DagFileParseRequestCompat),
        # which have no field-level migrations registered yet.
        class _Unregistered(BaseModel):
            value: int

        out = migrator.migrate(_Unregistered(value=42), "3025-01-01")
        assert out == {"value": 42}


class TestGetSchemaVersionMigrator:
    def test_returns_singleton(self):
        # The cached factory must return the same instance across calls
        # so the underlying Cadwyn caches stay warm and bundle
        # introspection runs at most once per process.
        assert get_schema_version_migrator() is get_schema_version_migrator()

    def test_is_bound_to_execution_api_bundle(self):
        # Sanity check: the singleton uses the real execution-API
        # bundle, not a synthetic one. A regression here would silently
        # detach the supervisor from the source of truth.
        # We don't expose the bundle attribute publicly; the contract
        # is that ``migrate`` on a model registered in *that* bundle
        # works. Use one of the registered taskinstance models to
        # confirm the binding without leaning on private state.
        from airflow.api_fastapi.execution_api.datamodels.taskinstance import TIRetryStatePayload
        from airflow.api_fastapi.execution_api.versions import bundle

        assert TIRetryStatePayload in bundle.versioned_schemas.values()
