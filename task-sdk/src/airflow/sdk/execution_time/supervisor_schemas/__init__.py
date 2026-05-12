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
Cadwyn versioning and in-process migration for the supervisor IPC contract.

Two distinct Cadwyn ``VersionBundle`` instances coexist in the codebase:

* :data:`.versions.bundle` (this package) — versions the wire shapes the
  Task SDK supervisor exchanges with a lang-SDK runtime subprocess
  launched by a coordinator (Java, Go, Rust, ...). The bodies it
  references live in their semantic homes
  (``airflow.sdk.execution_time.comms`` for task execution,
  ``airflow.dag_processing.processor`` for Dag parsing); this package
  only owns the versioning machinery, not the model definitions.
* :data:`airflow.api_fastapi.execution_api.versions.bundle` — versions
  the HTTP contract between Task SDK clients and the API server.
  Unaffected by this package.

:func:`registered_models` returns every Pydantic class on the supervisor
IPC wire. It is computed dynamically from the four discriminated
unions ``ToTask``, ``ToSupervisor`` (task-execution channel) and
``ToManager``, ``ToDagProcessor`` (dag-processing channel) so the
registry is always in sync with the actual unions ``CommsDecoder``
decodes against -- no hand-maintained list to drift. Triggerer unions
are intentionally excluded (the Triggerer IPC channel is not handled
by lang-SDK coordinators today).

The ``generate-supervisor-schemas-snapshot`` prek hook walks
:func:`registered_models` to write ``supervisor_schemas/schema.json``,
which is the head-version JSON Schema artefact lang-SDK builders
consume for codegen.
"""

from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Annotated, Any, get_args, get_origin

from airflow.sdk.execution_time.supervisor_schemas.migrator import (
    SchemaVersionMigrator,
    get_schema_version_migrator,
)
from airflow.sdk.execution_time.supervisor_schemas.versions import bundle

if TYPE_CHECKING:
    from pydantic import BaseModel


def _members_of_discriminated_union(union_type: object) -> tuple[type, ...]:
    """Return the BaseModel classes in an ``Annotated[A | B | ..., Field(...)]``."""
    # ``Annotated[X | Y, Field(...)]`` -> the first ``get_args`` arg is the union.
    if get_origin(union_type) is Annotated:
        union_type = get_args(union_type)[0]
    members = get_args(union_type)
    return tuple(m for m in members if isinstance(m, type))


@functools.cache
def registered_models_by_name() -> dict[str, type[BaseModel]]:
    """
    Map every supervisor-IPC body's class name to the head Pydantic class.

    Used by :func:`resolve_body_class` to resolve a wire-shape
    discriminator (``body["type"]``) to the head class the migrator's
    ``upgrade`` path needs. Cached per-process; the registry only
    changes when a union member is added in ``comms.py`` or
    ``processor.py``, which requires a restart anyway.
    """
    return {cls.__name__: cls for cls in registered_models()}


def resolve_body_class(body: Any) -> type[BaseModel] | None:
    """Resolve a wire-body dict's ``type`` discriminator to its head Pydantic class."""
    if not isinstance(body, dict):
        return None
    name = body.get("type")
    if not isinstance(name, str):
        return None
    return registered_models_by_name().get(name)


def registered_models() -> tuple[type[BaseModel], ...]:
    """
    Return every Pydantic class on the supervisor IPC wire, deduplicated by name.

    Derived live from the four discriminated unions the supervisor
    actually decodes against. Adding a new message type to any of those
    unions automatically pulls it into the snapshot the next time the
    ``generate-supervisor-schemas-snapshot`` prek hook runs.

    Imports are deferred so this package stays cheap to import for
    callers that only need the bundle or migrator (e.g. the migrator
    singleton factory); pulling in ``processor`` eagerly would drag the
    whole DAG-processor import graph into every consumer.
    """
    from pydantic import BaseModel

    from airflow.dag_processing.processor import ToDagProcessor, ToManager
    from airflow.sdk.execution_time.comms import ToSupervisor, ToTask

    seen: dict[str, type[BaseModel]] = {}
    for union in (ToTask, ToSupervisor, ToManager, ToDagProcessor):
        for member in _members_of_discriminated_union(union):
            if not issubclass(member, BaseModel):
                continue
            seen.setdefault(member.__name__, member)
    return tuple(seen[name] for name in sorted(seen))


__all__ = [
    "SchemaVersionMigrator",
    "bundle",
    "get_schema_version_migrator",
    "registered_models",
    "registered_models_by_name",
    "resolve_body_class",
]
