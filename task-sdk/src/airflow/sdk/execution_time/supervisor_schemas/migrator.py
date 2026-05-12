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
runtimes were built against -- and vice versa, decode payloads the
runtime sends back in *its* schema version into the supervisor's head
shape. Cadwyn knows how to apply a chain of ``VersionChange`` entries
against a :class:`~cadwyn.VersionBundle`; this module exposes that
capability as two in-process calls so neither the supervisor nor the
parser has to round-trip the body through a network endpoint just to
perform a pure schema transformation.

Cadwyn publishes ``migrate_response_body`` for the head-to-older
(downgrade) direction. There is no symmetric public helper for the
older-to-head (upgrade) direction: ``VersionBundle._migrate_request``
exists but is async and HTTP-coupled (FastAPI ``Request`` +
``Dependent`` + ``solve_dependencies``). We therefore drive both
directions ourselves over the bundle's public ``versions`` /
``reversed_versions`` tuples and ``VersionChange``'s public
``alter_request_by_schema_instructions`` /
``alter_response_by_schema_instructions`` ClassVar dicts -- the same
fields cadwyn's HTTP runner walks. Each transformer is a callable
that only reads/writes ``info.body``, so a body-only ``_BodyInfo``
duck-type lets us skip the HTTP plumbing entirely.

The downgrade path additionally validates the walked body against
the cadwyn-generated versioned class for *client_version*. That is
how a declarative ``schema(X).field(Y).didnt_exist`` (no companion
``convert_response_to_previous_version_for``) actually drops the
field on the wire -- the field is removed from the class shape, not
from any data transformer. Skipping the validate step would silently
let new-only fields leak to older clients. The upgrade path has no
equivalent declarative shortcut: filling in a field the client did
not send always requires an explicit
``convert_request_to_next_version_for`` transformer, so no final
re-validate is needed there.

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

from cadwyn import generate_versioned_models
from pydantic import BaseModel

if TYPE_CHECKING:
    from cadwyn import VersionBundle


class _BodyInfo:
    """
    Duck-type stand-in for Cadwyn's ``RequestInfo`` / ``ResponseInfo``.

    ``cadwyn.structure.data._AlterDataInstruction.__call__`` only
    reads and writes ``info.body``; the by-schema transformers we
    drive never touch FastAPI's Request/Response. Passing this
    minimal object lets us run cadwyn's migrations from a pure IPC
    code path with no HTTP stack.
    """

    __slots__ = ("body",)

    def __init__(self, body: dict[str, Any]) -> None:
        # Copy so the caller's mapping survives intact when the
        # instruction chain mutates ``info.body`` in place.
        self.body = dict(body)


class SchemaVersionMigrator:
    """
    Bidirectional in-process migrator for supervisor IPC bodies.

    The supervisor is always on the head shape of *bundle* (its
    canonical Pydantic models). Each foreign runtime is pinned to a
    specific dated client version; this class walks Cadwyn's
    ``VersionChange`` chain in-process to bridge the two::

        head shape  --- downgrade(body, client) --->  client wire
        head shape  <-- upgrade(body, client)   ---   client wire

    *server_version* is fixed at construction time. It defaults to
    ``bundle.versions[0].value`` -- cadwyn keeps that tuple in
    newest-to-oldest order, and the very last entry is asserted to
    carry no migrations (it is the baseline), so [0] is the natural
    supervisor anchor. Tests that drive a synthetic bundle can pin
    a different anchor.

    A body whose Pydantic type is not mentioned by any ``schema(...)``
    instruction in the bundle is passed through verbatim: cadwyn keys
    its instruction dicts by body type, so the lookup misses and no
    transformer runs. That matches the current state of the real IPC
    bodies (``StartupDetails``, ``DagFileParseRequest``) until the
    first breaking field change ships.
    """

    __slots__ = ("_bundle", "_server_version")

    def __init__(self, bundle: VersionBundle, server_version: str | None = None) -> None:
        self._bundle = bundle
        self._server_version = server_version if server_version is not None else bundle.versions[0].value

    @property
    def server_version(self) -> str:
        return self._server_version

    def downgrade(
        self,
        body: BaseModel,
        client_version: str | date,
        *,
        dump_kwargs: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Migrate *body* from the server (head) shape down to *client_version*.

        Used on the supervisor -> foreign-runtime path: *body* is a
        head-shape Pydantic instance, and the returned dict matches
        the wire shape the client SDK was built against.

        :param body: A Pydantic instance shaped according to the head
            (latest) version of the bundle.
        :param client_version: Either an ISO-format date string (e.g.
            ``"2026-04-17"``) or a :class:`datetime.date`. Cadwyn maps
            a ``date`` to the closest lesser defined version.
        :param dump_kwargs: Optional keyword arguments forwarded to
            both ``model_dump`` calls (the initial dump that feeds the
            migration walk and the final dump that produces the wire
            shape). Used to preserve OpenAPI-generated response options
            like ``exclude_unset=True`` / ``by_alias=True``. ``mode``
            is always forced to ``"json"`` and cannot be overridden.
            Caveat: ``by_alias=True`` makes the initial dump emit
            alias-keyed fields, which cadwyn's ``schema(...)``
            instructions (keyed by Python field name) will not match.
            Safe for models without registered field-level migrations
            today; revisit if an aliased model ever ships migrations.
        :returns: A plain dict shaped for *client_version*. Returning
            a dict (rather than a versioned Pydantic instance) is
            deliberate: call sites forward the result straight onto an
            IPC frame.
        """
        if not isinstance(body, BaseModel):
            raise TypeError(
                f"SchemaVersionMigrator.downgrade expects a pydantic BaseModel instance, got {type(body)!r}"
            )
        client_value = self._resolve(client_version)
        body_type = type(body)
        merged_dump_kwargs: dict[str, Any] = {**(dump_kwargs or {}), "mode": "json"}
        # ``mode="json"`` so datetime/UUID/Path serialise to primitives
        # the versioned-model validators inside the chain accept.
        info = _BodyInfo(body.model_dump(**merged_dump_kwargs))
        for version in self._bundle.versions:
            if version.value > self._server_version:
                continue
            if version.value <= client_value:
                break
            for change in version.changes:
                for instr in change.alter_response_by_schema_instructions.get(body_type, ()):
                    instr(info)  # type: ignore[arg-type]
        # Re-validate against the versioned class so declarative
        # ``schema(X).field(Y).didnt_exist`` instructions take effect:
        # those alter the class shape, not the dict, so without this
        # round-trip the dropped field would still appear on the wire.
        versioned_class: type[BaseModel] = generate_versioned_models(self._bundle)[client_value][body_type]
        return versioned_class.model_validate(info.body).model_dump(**merged_dump_kwargs)

    def upgrade(
        self,
        body: dict[str, Any],
        body_type: type[BaseModel],
        client_version: str | date,
    ) -> dict[str, Any]:
        """
        Migrate *body* from *client_version* up to the server (head) shape.

        Used on the foreign-runtime -> supervisor path: *body* is the
        already-deserialised payload off the wire (still in the
        client's schema), and the returned dict is shaped for
        ``model_validate`` against the head Pydantic class.

        *body_type* must be supplied because a dict carries no Python
        type information; the caller resolves it from the IPC
        discriminator (``body["type"]``) and the registered-models
        index. This mirrors cadwyn's HTTP runner, which dispatches
        request instructions keyed by the body's Pydantic type.

        :param body: The wire payload as a dict.
        :param body_type: The head Pydantic class *body* should
            validate against once migrated.
        :param client_version: Either an ISO-format date string or a
            :class:`datetime.date` (mapped to the closest lesser
            defined version).
        """
        if not isinstance(body, dict):
            raise TypeError(f"SchemaVersionMigrator.upgrade expects a dict payload, got {type(body)!r}")
        client_value = self._resolve(client_version)
        info = _BodyInfo(body)
        for version in self._bundle.reversed_versions:
            if version.value <= client_value:
                continue
            if version.value > self._server_version:
                continue
            for change in version.changes:
                for instr in change.alter_request_by_schema_instructions.get(body_type, ()):
                    instr(info)  # type: ignore[arg-type]
        return info.body

    def _resolve(self, v: str | date) -> str:
        # A ``date`` instance maps to the closest lesser defined
        # version, matching ``cadwyn.migrate_response_body``. A string
        # must be an exact value in the bundle.
        if isinstance(v, date):
            return self._bundle._get_closest_lesser_version(v.isoformat())
        if v not in self._bundle._version_values_set:
            raise ValueError(f"Version {v!r} not found in bundle")
        return v


@functools.cache
def get_schema_version_migrator() -> SchemaVersionMigrator:
    """
    Return the process-wide :class:`SchemaVersionMigrator` bound to the supervisor bundle.

    Cached so the bundle is bound once per process. The migrator
    holds no per-call state (versioned-model lookup is replaced by
    direct walks over the public bundle attributes), so concurrent
    callers can share a single instance safely.
    """
    from airflow.sdk.execution_time.supervisor_schemas.versions import bundle

    return SchemaVersionMigrator(bundle)


__all__ = ["SchemaVersionMigrator", "get_schema_version_migrator"]
