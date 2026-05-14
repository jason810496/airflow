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
In-process bidirectional migration for supervisor schema bodies.

:class:`SchemaVersionMigrator` walks a :class:`~cadwyn.VersionBundle`
itself rather than going through Cadwyn's HTTP runner so the supervisor
can downgrade outgoing bodies and upgrade incoming bodies without a
network round-trip. The downgrade path additionally re-validates against
the cadwyn-generated versioned class so declarative
``schema(X).field(Y).didnt_exist`` instructions actually drop fields on
the wire.
"""

from __future__ import annotations

import functools
import re
from typing import TYPE_CHECKING, Any

from cadwyn import generate_versioned_models
from pydantic import BaseModel

_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

if TYPE_CHECKING:
    from cadwyn import VersionBundle
    from cadwyn.schema_generation import SchemaGenerator


class _BodyInfo:
    """
    Duck-type stand-in for Cadwyn's ``RequestInfo`` / ``ResponseInfo``.

    ``cadwyn.structure.data._AlterDataInstruction.__call__`` only reads
    and writes ``info.body``; the by-schema transformers we drive never
    touch FastAPI's Request/Response. Passing this minimal object lets
    us run cadwyn's migrations from a pure in-process code path with no
    HTTP stack.
    """

    __slots__ = ("body",)

    def __init__(self, body: dict[str, Any]) -> None:
        # Copy so the caller's mapping survives intact when the
        # instruction chain mutates ``info.body`` in place.
        self.body = dict(body)


class SchemaVersionMigrator:
    """
    Bidirectional in-process migrator for supervisor schema bodies.

    The supervisor is always on the head shape of *bundle* (its canonical
    Pydantic models). Each foreign runtime is pinned to a specific dated
    lang-SDK supervisor schema version; this class walks Cadwyn's
    ``VersionChange`` chain in-process to bridge the two::

        head shape  --- downgrade(body, lang_sdk) --->  lang-SDK wire
        head shape  <-- upgrade(body, lang_sdk)   ---   lang-SDK wire

    *supervisor_version* is fixed at construction time and defaults to
    ``bundle.versions[0].value`` -- the latest dated entry, since cadwyn
    keeps that tuple in newest-to-oldest order. Tests can pin a
    different anchor.

    A body whose Pydantic type is not mentioned by any ``schema(...)``
    instruction in the bundle is passed through as-is: cadwyn keys its
    instruction dicts by body type, so the lookup misses and no
    transformer runs.
    """

    __slots__ = ("_bundle", "_supervisor_version", "_versioned_models", "_version_values")

    def __init__(self, bundle: VersionBundle, supervisor_version: str | None = None) -> None:
        self._bundle = bundle
        self._supervisor_version = (
            supervisor_version if supervisor_version is not None else bundle.versions[0].value
        )
        # Caches over the bundle (which is immutable for the migrator's
        # lifetime). ``generate_versioned_models`` walks the full version
        # graph; ``_version_values`` mirrors cadwyn's internal lookup set
        # without reaching into its private attribute.
        self._versioned_models: dict[str, SchemaGenerator] | None = None
        self._version_values: frozenset[str] = frozenset(v.value for v in bundle.versions)

    @property
    def supervisor_version(self) -> str:
        return self._supervisor_version

    def _versioned_class(self, lang_sdk_value: str, body_type: type[BaseModel]) -> type[BaseModel]:
        """Return the cadwyn-generated class for *body_type* at *lang_sdk_value*."""
        if self._versioned_models is None:
            self._versioned_models = generate_versioned_models(self._bundle)
        return self._versioned_models[lang_sdk_value][body_type]

    def downgrade(
        self,
        body: BaseModel,
        lang_sdk_msg_schema_version: str,
        *,
        dump_kwargs: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Migrate *body* from the server (head) shape down to *lang_sdk_msg_schema_version*.

        Used on the supervisor -> foreign-runtime path: *body* is a
        head-shape Pydantic instance, and the returned dict matches the
        wire shape the lang-SDK was built against.

        :param body: A Pydantic instance shaped according to the head
            (latest) version of the bundle.
        :param lang_sdk_msg_schema_version: Dated supervisor schema
            version string in ``YYYY-MM-DD`` format (e.g.
            ``"2026-04-17"``). Must be an exact value in the bundle.
        :param dump_kwargs: Optional keyword arguments forwarded to both
            ``model_dump`` calls. ``mode`` is always forced to
            ``"json"`` and cannot be overridden.
        :returns: A plain dict shaped for *lang_sdk_msg_schema_version*.
        """
        if not isinstance(body, BaseModel):
            raise TypeError(
                f"SchemaVersionMigrator.downgrade expects a pydantic BaseModel instance, got {type(body)!r}"
            )
        lang_sdk_value = self._resolve(lang_sdk_msg_schema_version)
        body_type = type(body)
        merged_dump_kwargs: dict[str, Any] = {**(dump_kwargs or {}), "mode": "json"}
        # ``mode="json"`` so datetime/UUID/Path serialise to primitives
        # the versioned-model validators inside the chain accept.
        info = _BodyInfo(body.model_dump(**merged_dump_kwargs))
        for version in self._bundle.versions:
            if version.value > self._supervisor_version:
                continue
            if version.value <= lang_sdk_value:
                break
            for change in version.changes:
                for instr in change.alter_response_by_schema_instructions.get(body_type, ()):
                    instr(info)  # type: ignore[arg-type]
        # Re-validate against the versioned class so declarative
        # ``schema(X).field(Y).didnt_exist`` instructions take effect:
        # those alter the class shape, not the dict, so without this
        # round-trip the dropped field would still appear on the wire.
        versioned_class = self._versioned_class(lang_sdk_value, body_type)
        return versioned_class.model_validate(info.body).model_dump(**merged_dump_kwargs)

    def upgrade(
        self,
        body: dict[str, Any],
        body_type: type[BaseModel],
        lang_sdk_msg_schema_version: str,
    ) -> dict[str, Any]:
        """
        Migrate *body* from *lang_sdk_msg_schema_version* up to the server (head) shape.

        Used on the foreign-runtime -> supervisor path: *body* is the
        already-deserialised payload off the wire (still in the lang-SDK's
        schema), and the returned dict is shaped for ``model_validate``
        against the head Pydantic class.

        *body_type* must be supplied because a dict carries no Python
        type information; the caller resolves it from the
        discriminator (``body["type"]``) and the registered-models index.

        :param body: The wire payload as a dict.
        :param body_type: The head Pydantic class *body* should validate
            against once migrated.
        :param lang_sdk_msg_schema_version: Dated supervisor schema
            version string in ``YYYY-MM-DD`` format.
        """
        if not isinstance(body, dict):
            raise TypeError(f"SchemaVersionMigrator.upgrade expects a dict payload, got {type(body)!r}")
        lang_sdk_value = self._resolve(lang_sdk_msg_schema_version)
        info = _BodyInfo(body)
        for version in self._bundle.reversed_versions:
            if version.value <= lang_sdk_value:
                continue
            if version.value > self._supervisor_version:
                continue
            for change in version.changes:
                for instr in change.alter_request_by_schema_instructions.get(body_type, ()):
                    instr(info)  # type: ignore[arg-type]

        versioned_class = self._versioned_class(self._supervisor_version, body_type)
        return versioned_class.model_validate(info.body).model_dump(mode="json")

    def _resolve(self, v: str) -> str:
        """Validate *v* is a ``YYYY-MM-DD`` string present in the bundle."""
        if not isinstance(v, str) or not _ISO_DATE_RE.match(v):
            raise ValueError(f"Version {v!r} must be a string in YYYY-MM-DD format")
        if v not in self._version_values:
            raise ValueError(
                f"Version {v!r} not found in bundle "
                "airflow.sdk.execution_time.supervisor_schemas.versions.bundle"
            )
        return v


@functools.cache
def get_schema_version_migrator() -> SchemaVersionMigrator:
    """
    Return the process-wide :class:`SchemaVersionMigrator` bound to the supervisor bundle.

    Cached so the bundle is bound once per process. The migrator holds
    no per-call state, so concurrent callers can share a single
    instance safely.
    """
    from airflow.sdk.execution_time.supervisor_schemas.versions import bundle

    return SchemaVersionMigrator(bundle)


__all__ = ["SchemaVersionMigrator", "get_schema_version_migrator"]
