#
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
Check the mapped tasks of a Dag a language SDK serialized.

Python checks these shapes when a stub task is serialized. A language SDK may check them too, but the
Dag processor is what decides, so every language gets the same errors (ADR-0016, decision 4).
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from airflow.serialization.enums import Encoding

if TYPE_CHECKING:
    from airflow.serialization.definitions.dag import SerializedDAG


def check_mapped_tasks(data: dict[str, Any], dag: SerializedDAG) -> None:
    """
    Raise ``ValueError`` for a mapping shape no binding can deliver to a language SDK runtime.

    *dag* is *data* deserialized.
    """
    for encoded in data["dag"]["tasks"]:
        task = encoded[Encoding.VAR]
        if not task.get("_is_mapped"):
            continue
        label = f"Task {task['task_id']!r}"
        _check_expand_input(task[task.get("_expand_input_attr", "expand_input")], label, dag)
        for name, value in _iter_dict_items((task.get("partial_kwargs") or {}).get("op_kwargs")):
            _check_whole_value(value, f"{label} argument {name!r}")
    for group_id, expand_input in _iter_mapped_group_inputs(data["dag"]["task_group"]):
        _check_expand_input(expand_input, f"Mapped task group {group_id!r}", dag)
    _check_group_reads(data, dag)


def _check_expand_input(expand_input: dict[str, Any], label: str, dag: SerializedDAG) -> None:
    value = expand_input["value"]
    if expand_input["type"] == "dict-of-lists":
        for name, item in _iter_dict_items(value):
            if _is_xcom_ref(item):
                _check_shape(item[Encoding.VAR], f"{label} expand argument {name!r}")
            elif isinstance(item, dict) and item.get(Encoding.TYPE) == "dict":
                raise ValueError(
                    f"{label} expand argument {name!r} is a dict, whose item order the stored Dag does "
                    "not keep; expand over a list of [key, value] pairs instead"
                )
        return
    if _is_xcom_ref(value):
        _check_shape(value[Encoding.VAR], f"{label} expand_kwargs() input")
        return
    for item in value:
        if _is_xcom_ref(item):
            ref = item[Encoding.VAR]
            _check_whole_value(item, f"{label} expand_kwargs() item")
            if (producer := dag.task_dict.get(ref["task_id"])) is not None and producer.get_needs_expansion():
                raise ValueError(
                    f"{label} expand_kwargs() item is the whole output of the mapped task "
                    f"{ref['task_id']!r}, a list rather than a dict"
                )
            continue
        for name, entry in _iter_dict_items(item):
            if _contains_xcom_ref(entry):
                raise ValueError(
                    f"{label} expand_kwargs() dict passes an upstream output as {name!r}; a literal "
                    "dict cannot hold one, so expand_kwargs() over that upstream's output instead"
                )


def _check_shape(ref: dict[str, Any], label: str, *, outermost: bool = True, in_zip: bool = False) -> None:
    match ref.get("type"):
        case "map":
            if not outermost:
                raise ValueError(
                    f"{label} has a map() inside a zip() or concat(); call map() last, on that result"
                )
            _check_shape(ref["arg"], label, outermost=False, in_zip=in_zip)
        case "zip":
            if in_zip:
                raise ValueError(f"{label} has a zip() inside a zip(); zip every argument in one call")
            for arg in ref["args"]:
                _check_shape(arg, label, outermost=False, in_zip=True)
        case "concat":
            for arg in ref["args"]:
                _check_shape(arg, label, outermost=False, in_zip=in_zip)


def _check_whole_value(value: Any, label: str) -> None:
    if _is_xcom_ref(value) and value[Encoding.VAR].get("type") is not None:
        raise ValueError(
            f"{label} receives a whole map(), zip() or concat() result; only an expand argument can use one"
        )


def _check_group_reads(data: dict[str, Any], dag: SerializedDAG) -> None:
    """Raise for a task that reads a task mapped again inside the innermost mapped task group they share."""
    for encoded in data["dag"]["tasks"]:
        task = encoded[Encoding.VAR]
        op = dag.get_task(task["task_id"])
        group_ids = {group.group_id for group in op.iter_mapped_task_groups()}
        if not group_ids:
            continue
        read_ids = {
            entry["task_id"]
            for entry in task.get("_arg_bindings") or ()
            if entry.get("kind") in ("xcom", "sequence")
        }
        for producer_id in sorted(read_ids):
            producer = dag.get_task(producer_id)
            producer_group_ids = [group.group_id for group in producer.iter_mapped_task_groups()]
            common = next((i for i, gid in enumerate(producer_group_ids) if gid in group_ids), None)
            if common is not None and (common > 0 or producer.is_mapped):
                raise ValueError(
                    f"Task {op.task_id!r} reads task {producer_id!r}, which is mapped again inside the "
                    "mapped task group they share; read it from outside that group"
                )


def _iter_mapped_group_inputs(group: dict[str, Any]) -> Iterator[tuple[str, dict[str, Any]]]:
    for child_id, (kind, child) in group["children"].items():
        if kind != "taskgroup":
            continue
        if child.get("is_mapped") and (expand_input := child.get("expand_input")) is not None:
            yield child_id, expand_input
        yield from _iter_mapped_group_inputs(child)


def _iter_dict_items(value: Any) -> Iterator[tuple[str, Any]]:
    if isinstance(value, dict) and value.get(Encoding.TYPE) == "dict":
        value = value[Encoding.VAR]
    if isinstance(value, dict):
        yield from value.items()


def _is_xcom_ref(value: Any) -> bool:
    return isinstance(value, dict) and value.get(Encoding.TYPE) == "xcomref"


def _contains_xcom_ref(value: Any) -> bool:
    if _is_xcom_ref(value):
        return True
    if isinstance(value, dict):
        return any(_contains_xcom_ref(v) for v in value.values())
    if isinstance(value, list):
        return any(_contains_xcom_ref(v) for v in value)
    return False
