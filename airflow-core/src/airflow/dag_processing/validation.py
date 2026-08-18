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

"""Structural checks applied to serialized Dags before they are written to the metadata DB."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow._shared.dagnode.cycle import detect_cycle
from airflow.serialization.enums import Encoding

if TYPE_CHECKING:
    from collections.abc import Collection

    from airflow.serialization.serialized_objects import LazyDeserializedDAG

__all__ = ["validate_serialized_dags"]


def _extract_downstream_edges(dag: LazyDeserializedDAG) -> dict[str, list[str]]:
    """Map each task id to the task ids directly downstream of it, straight from the raw payload."""
    edges: dict[str, list[str]] = {}
    for task in dag.data.get("dag", {}).get("tasks") or []:
        encoded: dict[str, Any] = task.get(Encoding.VAR) or {}
        task_id = encoded.get("task_id")
        if task_id is not None:
            edges[task_id] = encoded.get("downstream_task_ids") or []
    return edges


def _find_dag_structure_error(dag: LazyDeserializedDAG) -> str | None:
    """Return a description of the first structural defect found, or ``None`` when the Dag is sound."""
    edges = _extract_downstream_edges(dag)

    # Dangling ids are rejected first because detect_cycle assumes every downstream id resolves.
    for task_id, downstream_ids in edges.items():
        for downstream_id in downstream_ids:
            if downstream_id not in edges:
                return f"task {task_id!r} lists unknown downstream task {downstream_id!r}"

    if (cycle_task_id := detect_cycle(edges, edges.__getitem__)) is not None:
        return f"a dependency cycle is closed by task {cycle_task_id!r}"

    return None


def validate_serialized_dags(
    dags: Collection[LazyDeserializedDAG],
) -> tuple[list[LazyDeserializedDAG], list[str]]:
    """
    Split serialized Dags into the ones safe to persist and messages for the ones that are not.

    Dags authored in Python are checked by :meth:`~airflow.sdk.DAG.check_cycle` before they are
    serialized, but a language SDK hands core an already-serialized Dag that never passes through
    ``DagBag.bag_dag``, so this is the only structural check standing between it and the metadata
    DB. Corrupt structure is not merely cosmetic: a cycle makes ``topological_sort`` raise, and a
    dangling downstream id raises ``KeyError`` while deserializing.

    The scan reads the raw payload instead of hydrating each :class:`LazyDeserializedDAG`, keeping
    the laziness that class exists for. It is an O(V+E) walk over dicts already in memory, which is
    negligible next to the database write it guards.

    :return: The Dags that passed, and one message per rejected Dag.
    """
    valid_dags: list[LazyDeserializedDAG] = []
    errors: list[str] = []
    seen_dag_ids: set[str] = set()

    for dag in dags:
        dag_id = dag.data.get("dag", {}).get("dag_id")
        if dag_id in seen_dag_ids:
            errors.append(f"Dag {dag_id!r} is defined more than once in this file")
            continue
        seen_dag_ids.add(dag_id)

        if (error := _find_dag_structure_error(dag)) is not None:
            errors.append(f"Dag {dag_id!r} is malformed: {error}")
            continue

        valid_dags.append(dag)

    return valid_dags, errors
