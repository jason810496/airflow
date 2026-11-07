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
Versions of the serialized Dag format, and the engine that moves a blob between them.

See ``AGENTS.md`` in this folder for when and how to add a change.
"""

from __future__ import annotations

from typing import Any

from packaging.version import Version as PackagingVersion

from airflow.serialization.versions import (
    v2,
    v3,
    v2025_10_27,
    v2025_11_05,
    v2026_04_06,
    v2026_06_30,
    v2026_10_30,
    v2027_02_28,
)
from airflow.serialization.versions.base import SerializedDagChange, Version

__all__ = [
    "HEAD",
    "MIN_DOWNGRADE_VERSION",
    "VERSIONS",
    "SerializedDagChange",
    "Version",
    "downgrade",
    "get_known_stamps",
    "get_version",
    "get_version_for_airflow",
    "upgrade",
]

# Ordered oldest first. Order comes from the position here, never from comparing ids.
VERSIONS: tuple[Version, ...] = (
    Version(id=1, changes=(), airflow_version="1.10.7"),
    v2.VERSION,
    v3.VERSION,
    v2025_10_27.VERSION,
    v2025_11_05.VERSION,
    v2026_04_06.VERSION,
    v2026_06_30.VERSION,
    v2026_10_30.VERSION,
    v2027_02_28.VERSION,
)
HEAD = VERSIONS[-1]

# Version 1 is Airflow 2, and migration 0047 deletes every serialized Dag before going below 3.0.0.
MIN_DOWNGRADE_VERSION = 2


def _is_valid_stamp(stamp: Any) -> bool:
    return isinstance(stamp, (int, str)) and not isinstance(stamp, bool)


def get_known_stamps() -> frozenset[int | str]:
    """Return every ``__version`` value this core can read."""
    return frozenset(version.stamp for version in VERSIONS)


def _find_index(predicate, *, last: bool) -> int | None:
    indices = [i for i, version in enumerate(VERSIONS) if predicate(version)]
    if not indices:
        return None
    return indices[-1] if last else indices[0]


def get_version(version_id: int | str) -> Version:
    """Return the version with id *version_id*."""
    if _is_valid_stamp(version_id):
        for version in VERSIONS:
            if version.id == version_id:
                return version
    raise ValueError(f"Unknown serialized Dag version {version_id!r}")


def upgrade(data: dict[str, Any]) -> dict[str, Any]:
    """
    Upgrade a serialized Dag blob to ``HEAD``, in place.

    A stamp shared by several versions is read as the oldest of them, so every later change runs.

    :raises ValueError: if the blob's ``__version`` is unknown.
    """
    stamp = data.get("__version", "<not present>")
    start = _find_index(lambda v: v.stamp == stamp, last=False) if _is_valid_stamp(stamp) else None
    if start is None:
        raise ValueError(f"Unsure how to deserialize version {stamp!r}")
    for version in VERSIONS[start + 1 :]:
        for change in version.changes:
            if not change.requires_migration:
                change().upgrade(data)
    data["__version"] = HEAD.stamp
    return data


def downgrade(data: dict[str, Any], *, to: int | str) -> dict[str, Any]:
    """
    Downgrade a serialized Dag blob to version *to*, in place.

    A stamp shared by several versions is read as the newest of them, so every change down to *to*
    runs. Changes that need an alembic migration are skipped. A blob already at or below *to* is
    left as it is.

    :raises ValueError: if the blob's ``__version`` or *to* is unknown, or *to* is below version 2.
    """
    target = VERSIONS.index(get_version(to))
    if target < VERSIONS.index(get_version(MIN_DOWNGRADE_VERSION)):
        raise ValueError(
            f"Cannot downgrade a serialized Dag below version {MIN_DOWNGRADE_VERSION}: "
            "Airflow 2 does not read serialized Dags written by Airflow 3"
        )
    stamp = data.get("__version", "<not present>")
    start = _find_index(lambda v: v.stamp == stamp, last=True) if _is_valid_stamp(stamp) else None
    if start is None:
        raise ValueError(f"Unsure how to downgrade serialized Dag version {stamp!r}")
    if start <= target:
        return data
    for version in reversed(VERSIONS[target + 1 : start + 1]):
        for change in reversed(version.changes):
            if not change.requires_migration:
                change().downgrade(data)
    data["__version"] = VERSIONS[target].stamp
    return data


def get_version_for_airflow(airflow_version: str) -> Version:
    """
    Return the newest version the given Airflow release writes.

    Pre-release and dev suffixes are ignored, so ``3.5.0.dev0`` maps like ``3.5.0``.
    """
    release = PackagingVersion(PackagingVersion(airflow_version).base_version)
    matching = [v for v in VERSIONS if PackagingVersion(v.airflow_version) <= release]
    if not matching:
        raise ValueError(f"Airflow {airflow_version} predates serialized Dags")
    return matching[-1]
