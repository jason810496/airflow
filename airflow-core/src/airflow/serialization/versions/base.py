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
"""Building blocks of the serialized Dag format versions."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, ClassVar

import structlog

log = structlog.get_logger(__name__)


class SerializedDagChange:
    """
    One change to the serialized Dag format.

    ``upgrade`` turns a blob of the previous version into this one, and ``downgrade`` turns it back.
    Both work on the whole blob (``{"__version": ..., "dag": ...}``) in place, must not touch
    ``__version`` (the engine writes it), and must be safe to apply twice: blobs stamped ``3`` were
    written by any release from 3.1.0 to 3.4, so every change of those releases runs on them.
    """

    description: ClassVar[str]
    requires_migration: ClassVar[bool] = False
    """
    The change needs other tables, so an alembic migration does the work instead of this class.

    The engine skips ``upgrade`` and ``downgrade`` of such a change.
    """

    def upgrade(self, data: dict[str, Any]) -> None:
        """Turn a blob of the previous version into this version, in place."""

    def downgrade(self, data: dict[str, Any]) -> None:
        """Turn a blob of this version into the previous version, in place."""

    def warn_lossy(self, data: dict[str, Any], detail: str) -> None:
        """Log that a downgrade dropped information the older release cannot hold."""
        dag = data.get("dag")
        log.warning(
            "Serialized Dag downgrade dropped data",
            dag_id=dag.get("dag_id") if isinstance(dag, dict) else None,
            change=type(self).__name__,
            detail=detail,
        )


@dataclass(frozen=True)
class Version:
    """A version of the serialized Dag format: the changes one release made to it."""

    id: int | str
    """``1``, ``2``, ``3`` or a ``YYYY-MM-DD`` date."""
    changes: tuple[type[SerializedDagChange], ...]
    airflow_version: str
    """The first Airflow release that wrote this version."""
    stamp: int | str = field(default=None)  # type: ignore[assignment]
    """The value written to ``__version``; defaults to ``id``."""

    def __post_init__(self) -> None:
        if self.stamp is None:
            object.__setattr__(self, "stamp", self.id)


def iter_tasks(data: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """Yield the ``__var`` dict of each task in the blob."""
    dag = data.get("dag")
    if not isinstance(dag, dict):
        return
    for task in dag.get("tasks") or ():
        if isinstance(task, dict) and isinstance(task.get("__var"), dict):
            yield task["__var"]


def iter_partial_kwargs(data: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """Yield the ``partial_kwargs`` dict of each mapped task in the blob."""
    for task in iter_tasks(data):
        if isinstance(partial_kwargs := task.get("partial_kwargs"), dict):
            yield partial_kwargs


def get_default_args(data: dict[str, Any]) -> dict[str, Any] | None:
    """Return the Dag's ``default_args`` values, or None if it has none."""
    dag = data.get("dag")
    if not isinstance(dag, dict):
        return None
    default_args = dag.get("default_args")
    if isinstance(default_args, dict) and isinstance(default_args.get("__var"), dict):
        return default_args["__var"]
    return None


def iter_task_groups(data: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """Yield every task group of the Dag, the root group first."""
    dag = data.get("dag")
    if not isinstance(dag, dict) or not isinstance(root := dag.get("task_group"), dict):
        return
    pending = [root]
    while pending:
        group = pending.pop()
        yield group
        for child in (group.get("children") or {}).values():
            # A child is a ("taskgroup", group) or ("operator", task_id) pair.
            if isinstance(child, (list, tuple)) and len(child) == 2 and isinstance(child[1], dict):
                pending.append(child[1])


def pop_keys(container: dict[str, Any], keys: tuple[str, ...]) -> list[str]:
    """Remove *keys* from *container* and return the ones that held a value other than None."""
    return [key for key in keys if container.pop(key, None) is not None]
