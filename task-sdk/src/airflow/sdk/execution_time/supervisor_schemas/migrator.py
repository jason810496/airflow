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
In-process schema-version migration for supervisor IPC payloads.

The supervisor and the parser-supervisor need to hand foreign-language
SDK runtimes (Java, etc.) payloads shaped for the schema version those
runtimes were built against. Cadwyn already knows how to apply a chain
of ``VersionChange`` entries against a :class:`~cadwyn.VersionBundle`;
this module exposes that capability as a single in-process call so
neither the supervisor nor the parser has to round-trip the body
through a network endpoint just to perform a pure schema
transformation.

The bundle used here is :data:`.versions.bundle` -- the supervisor-IPC
``VersionBundle``, independent of the execution-API HTTP bundle in
``airflow.api_fastapi.execution_api.versions``. See the package
docstring on :mod:`airflow.sdk.execution_time.supervisor_schemas` for
the boundary between the two.
"""

from __future__ import annotations

import functools
from datetime import date
from typing import TYPE_CHECKING, Any

from cadwyn import migrate_response_body
from pydantic import BaseModel

if TYPE_CHECKING:
    from cadwyn import VersionBundle


class SchemaVersionMigrator:
    """
    In-process migrator from a head-shape Pydantic body to an older schema version.

    Wraps :func:`cadwyn.migrate_response_body` around a single
    :class:`~cadwyn.VersionBundle` so callers do not have to know about
    Cadwyn's encoding requirements (the body must be dumped to a dict
    before migration because the migrated value is validated against a
    *different* generated class per version).

    A model does not need an explicit ``schema(...)`` instruction in
    the bundle for :meth:`migrate` to accept it -- Cadwyn looks the
    model up lazily and returns the body verbatim if no field-level
    instructions apply. That matches the current state of the IPC
    bodies (``StartupDetails``, ``DagFileParseRequest``): the
    supervisor bundle has no field-level migrations on those schemas
    today, so calling :meth:`migrate` at every supported target version
    is a no-op until the first breaking field change ships. Once that
    happens, this migrator picks the migration up automatically without
    any change at the call site.
    """

    __slots__ = ("_bundle",)

    def __init__(self, bundle: VersionBundle) -> None:
        self._bundle = bundle

    def migrate(self, body: BaseModel, target_version: str | date) -> dict[str, Any]:
        """
        Migrate *body* from its head shape to *target_version*.

        :param body: A Pydantic instance shaped according to the head
            (latest) version of the bundle. Must be a
            :class:`~pydantic.BaseModel` so Cadwyn can generate the
            corresponding versioned class.
        :param target_version: Either an ISO-format date string (e.g.
            ``"2026-04-17"``) or a :class:`datetime.date`. Cadwyn maps
            it to the closest lesser version in the bundle.
        :returns: A plain dict shaped for *target_version*. Returning a
            dict (rather than a versioned Pydantic instance) is
            deliberate: the migrated class is a Cadwyn-generated copy
            distinct from the head class and would only confuse call
            sites that forward the result onto an IPC frame.
        """
        if not isinstance(body, BaseModel):
            raise TypeError(
                f"SchemaVersionMigrator.migrate expects a pydantic BaseModel instance, got {type(body)!r}"
            )
        # ``mode="json"`` so datetime/UUID/Path serialise to primitives
        # the versioned model's validators can accept. Cadwyn re-validates
        # the migrated dict against the versioned class, which would
        # otherwise reject native Python objects.
        migrated = migrate_response_body(
            self._bundle,
            type(body),
            latest_body=body.model_dump(mode="json"),
            version=target_version,
        )
        return migrated.model_dump(mode="json")


@functools.cache
def get_schema_version_migrator() -> SchemaVersionMigrator:
    """
    Return the process-wide :class:`SchemaVersionMigrator` bound to the supervisor bundle.

    Cached so the bundle is bound once per process. Cadwyn itself also
    caches the generated versioned model classes behind
    :func:`cadwyn.generate_versioned_models`, so per-call cost after
    the first migration is just one ``model_dump`` + dict copy.
    """
    from airflow.sdk.execution_time.supervisor_schemas.versions import bundle

    return SchemaVersionMigrator(bundle)


__all__ = ["SchemaVersionMigrator", "get_schema_version_migrator"]
