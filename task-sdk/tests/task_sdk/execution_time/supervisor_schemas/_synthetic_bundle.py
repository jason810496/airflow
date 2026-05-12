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
Shared synthetic IPC bundle for the supervisor-schemas integration tests.

Lives next to the test, the supervisor harness, and the runtime
harness so all three processes see identical body classes,
version-change registrations and bundle layout when the test parent
spawns the harnesses as real OS subprocesses.

Two body classes mirror the production split between channels:

- :class:`_RequestBody` -- runtime -> supervisor. Three fields with
  three dated breaking changes; each entry pairs a
  ``schema(...).didnt_exist`` instruction with a
  ``convert_request_to_next_version_for`` backfill so a wire payload
  from an older runtime reaches the head Pydantic class with every
  field present.
- :class:`_ResponseBody` -- supervisor -> runtime. Three fields with
  three dated breaking changes; each entry carries only
  ``schema(...).didnt_exist`` (responses never flow upstream, so no
  upgrade transformer is needed).

:func:`install_synthetic_migrator` re-binds the production migrator
factory and the cached registered-bodies index to the synthetic
bundle. It is the single seam every process (test parent and both
harnesses) calls so the migration plumbing routes through this
bundle without mutating the real ``ToSupervisor`` / ``ToManager`` /
``ToTask`` / ``ToDagProcessor`` discriminated unions.
"""

from __future__ import annotations

from typing import Literal

from cadwyn import (
    HeadVersion,
    Version,
    VersionBundle,
    VersionChange,
    convert_request_to_next_version_for,
    schema,
)
from pydantic import BaseModel


class _RequestBody(BaseModel):
    """
    Runtime -> supervisor request body.

    Three fields appear here; an older runtime omits later fields and
    the upgrade walk backfills them so the supervisor's head decoder
    always validates.
    """

    type: Literal["_RequestBody"] = "_RequestBody"
    ti_id: str
    field_a: int | None = None
    field_b: int | None = None
    field_c: int | None = None


class _ResponseBody(BaseModel):
    """
    Supervisor -> runtime response body.

    Three fields appear here; the downgrade walk trims any field
    introduced after the runtime's pinned version.
    """

    type: Literal["_ResponseBody"] = "_ResponseBody"
    ti_id: str
    response_x: str | None = None
    response_y: str | None = None
    response_z: str | None = None


# Request-body breaking changes -- each adds a field and a request-side
# backfill so an older client payload reaches the head shape intact.


class _AddRequestFieldA(VersionChange):
    """3026-02-15: introduce ``_RequestBody.field_a``."""

    description = __doc__
    instructions_to_migrate_to_previous_version = (schema(_RequestBody).field("field_a").didnt_exist,)

    @convert_request_to_next_version_for(_RequestBody)  # type: ignore[arg-type]
    def _backfill(request):
        request.body.setdefault("field_a", 0)


class _AddRequestFieldB(VersionChange):
    """3026-05-10: introduce ``_RequestBody.field_b``."""

    description = __doc__
    instructions_to_migrate_to_previous_version = (schema(_RequestBody).field("field_b").didnt_exist,)

    @convert_request_to_next_version_for(_RequestBody)  # type: ignore[arg-type]
    def _backfill(request):
        request.body.setdefault("field_b", 0)


class _AddRequestFieldC(VersionChange):
    """3026-08-22: introduce ``_RequestBody.field_c``."""

    description = __doc__
    instructions_to_migrate_to_previous_version = (schema(_RequestBody).field("field_c").didnt_exist,)

    @convert_request_to_next_version_for(_RequestBody)  # type: ignore[arg-type]
    def _backfill(request):
        request.body.setdefault("field_c", 0)


# Response-body breaking changes -- downgrade-only direction, no
# upgrade transformer because responses are never sent runtime ->
# supervisor.


class _AddResponseFieldX(VersionChange):
    """3026-03-01: introduce ``_ResponseBody.response_x``."""

    description = __doc__
    instructions_to_migrate_to_previous_version = (schema(_ResponseBody).field("response_x").didnt_exist,)


class _AddResponseFieldY(VersionChange):
    """3026-06-15: introduce ``_ResponseBody.response_y``."""

    description = __doc__
    instructions_to_migrate_to_previous_version = (schema(_ResponseBody).field("response_y").didnt_exist,)


class _AddResponseFieldZ(VersionChange):
    """3026-09-30: introduce ``_ResponseBody.response_z``."""

    description = __doc__
    instructions_to_migrate_to_previous_version = (schema(_ResponseBody).field("response_z").didnt_exist,)


SYNTHETIC_BUNDLE = VersionBundle(
    HeadVersion(),
    Version("3026-09-30", _AddResponseFieldZ),
    Version("3026-08-22", _AddRequestFieldC),
    Version("3026-06-15", _AddResponseFieldY),
    Version("3026-05-10", _AddRequestFieldB),
    Version("3026-03-01", _AddResponseFieldX),
    Version("3026-02-15", _AddRequestFieldA),
    Version("3025-12-01"),
)


ALL_VERSIONS: tuple[str, ...] = (
    "3025-12-01",
    "3026-02-15",
    "3026-03-01",
    "3026-05-10",
    "3026-06-15",
    "3026-08-22",
    "3026-09-30",
)


def install_synthetic_migrator() -> None:
    """
    Replace the production migrator factory and registered-body
    registry with ones backed by :data:`SYNTHETIC_BUNDLE`.

    Every production call site re-imports
    ``get_schema_version_migrator`` and ``registered_models_by_name``
    per call, so re-binding the module-level attributes is enough to
    redirect every downgrade and upgrade through the synthetic bundle.
    Idempotent: calling twice leaves the rebound state unchanged.
    """
    from airflow.sdk.execution_time import supervisor_schemas as ss_mod
    from airflow.sdk.execution_time.supervisor_schemas import SchemaVersionMigrator

    migrator = SchemaVersionMigrator(SYNTHETIC_BUNDLE)
    ss_mod.get_schema_version_migrator = lambda: migrator  # type: ignore[assignment]
    ss_mod.registered_models_by_name = lambda: {  # type: ignore[assignment]
        "_RequestBody": _RequestBody,
        "_ResponseBody": _ResponseBody,
    }
