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

The bundle and its body classes live in their own module (rather than
inline in the test file) so that any helper or fixture that needs them
imports a single canonical definition. The integration test installs
them via the ``synthetic_migrator`` pytest fixture, which uses
``monkeypatch`` to swap the production registry for the duration of a
single test and tears down automatically.

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


SYNTHETIC_REGISTRY: dict[str, type] = {
    "_RequestBody": _RequestBody,
    "_ResponseBody": _ResponseBody,
}
"""
Wire-discriminator -> head class map for the synthetic bundle.

Production lookups in ``resolve_body_class`` go through
``supervisor_schemas.registered_models_by_name``. The ``synthetic_migrator``
fixture swaps that lookup for this dict so the upgrade path can resolve
``_RequestBody`` / ``_ResponseBody`` discriminators against the synthetic
classes without touching the real registry.
"""
