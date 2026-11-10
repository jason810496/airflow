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
Check and normalize the task loops of a Dag a language SDK serialized.

Python checks a loop where the author writes it. A language SDK may check too, but the Dag processor is
what decides, so every language gets the same errors and the same graph.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.serialization.definitions.mappedoperator import SerializedMappedOperator
from airflow.serialization.definitions.taskgroup import SerializedLoopTaskGroup, SerializedMappedTaskGroup

if TYPE_CHECKING:
    from airflow.serialization.definitions.dag import SerializedDAG


def normalize_loops(data: dict[str, Any], dag: SerializedDAG) -> None:
    """
    Raise ``ValueError`` for a loop core cannot run, and make each loop's gate its only way out.

    *dag* is *data* deserialized. The edges are changed in *data*, which is what gets stored: the
    terminal task is put before the gate, and an edge from a body task to a task outside the loop
    leaves from the gate instead, as Python's ``create_loop`` does.
    """
    loops = [
        group
        for group in dag.task_group.get_task_group_dict().values()
        if isinstance(group, SerializedLoopTaskGroup)
    ]
    if not loops:
        return
    downstream = {
        task["__var"]["task_id"]: set(task["__var"].get("downstream_task_ids", []))
        for task in data["dag"]["tasks"]
    }
    for loop in loops:
        members = _check_loop(loop, dag, downstream)
        _rewire_edges_out(loop, members, downstream)
    for task in data["dag"]["tasks"]:
        edges = downstream[task["__var"]["task_id"]]
        if edges:
            task["__var"]["downstream_task_ids"] = sorted(edges)
        else:
            task["__var"].pop("downstream_task_ids", None)


def _check_loop(
    loop: SerializedLoopTaskGroup, dag: SerializedDAG, downstream: dict[str, set[str]]
) -> set[str]:
    """Raise unless *loop* is one core can run, and return the ids of the tasks it holds."""
    parent = loop.parent_group
    while parent is not None:
        if isinstance(parent, SerializedLoopTaskGroup):
            raise ValueError(
                f"Loop {loop.group_id!r} is inside loop {parent.group_id!r}; loops cannot be nested"
            )
        if isinstance(parent, SerializedMappedTaskGroup):
            raise ValueError(f"Loop {loop.group_id!r} cannot be inside mapped task group {parent.group_id!r}")
        parent = parent.parent_group
    members = {task.task_id for task in loop.iter_tasks()}
    if loop.gate_task_id not in members:
        raise ValueError(
            f"Loop {loop.group_id!r} names gate {loop.gate_task_id!r}, which is not a task of it"
        )
    body = members - {loop.gate_task_id}
    leaves = sorted(task_id for task_id in body if not downstream[task_id] & body)
    if leaves != [loop.terminal_task_id]:
        raise ValueError(
            f"The body of loop {loop.group_id!r} has to end in exactly one task, its terminal task "
            f"{loop.terminal_task_id!r}, but it ends in {leaves}"
        )
    consumers: list[SerializedMappedOperator | SerializedMappedTaskGroup] = [
        task for task in dag.task_dict.values() if isinstance(task, SerializedMappedOperator)
    ]
    consumers.extend(
        group
        for group in dag.task_group.get_task_group_dict().values()
        if isinstance(group, SerializedMappedTaskGroup)
    )
    for consumer in consumers:
        consumer_id = (
            consumer.task_id if isinstance(consumer, SerializedMappedOperator) else consumer.group_id
        )
        inside = consumer_id in members or (
            isinstance(consumer, SerializedMappedTaskGroup) and _is_within(consumer, loop)
        )
        for producer in consumer.iter_mapped_dependencies():
            if producer.task_id in body and not inside:
                raise ValueError(
                    f"{consumer_id!r} cannot expand over {producer.task_id!r}, a task in loop "
                    f"{loop.group_id!r}; only tasks in the same loop can expand over its output"
                )
    return members


def _is_within(group: SerializedMappedTaskGroup, loop: SerializedLoopTaskGroup) -> bool:
    parent = group.parent_group
    while parent is not None:
        if parent.group_id == loop.group_id:
            return True
        parent = parent.parent_group
    return False


def _rewire_edges_out(
    loop: SerializedLoopTaskGroup, members: set[str], downstream: dict[str, set[str]]
) -> None:
    gate = loop.gate_task_id
    downstream[loop.terminal_task_id].add(gate)
    for task_id in members - {gate}:
        outside = downstream[task_id] - members
        downstream[task_id] -= outside
        downstream[gate] |= outside
