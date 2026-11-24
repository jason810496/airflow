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
Resolve a mapped stub task's arguments in the supervisor's Python sidecar (ADR-0016).

The sidecar resolves the task as the Python task runner does, with ``render_template_fields``, and
tells the runtime what each argument resolved to.
"""

from __future__ import annotations

import inspect
from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from airflow.sdk.execution_time.lazy_sequence import LazyXComSequence

if TYPE_CHECKING:
    from airflow.sdk.api.datamodels._generated import TaskArgBinding, TaskInstance, TIRunContext
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance


class ArgResolutionError(ValueError):
    """A stub task's argument has no value at this map index."""


def needs_resolution(ti: TaskInstance, arg_bindings: Iterable[TaskArgBinding] | None) -> bool:
    """
    Whether the sidecar has to resolve *arg_bindings* before the runtime can bind them.

    A ``param`` or ``kwargs`` binding varies per map index. In a mapped task group a ``sequence``
    binding may name a task of the same group, which Python narrows to this instance's row.
    """
    kinds = {binding.root.kind for binding in arg_bindings or ()}
    in_expansion = ti.map_index is not None and ti.map_index >= 0
    return bool(kinds & {"param", "kwargs"}) or ("sequence" in kinds and in_expansion)


def resolve_arg_bindings(ti: TaskInstance, ti_context: TIRunContext, task: Any) -> list[dict[str, Any]]:
    """
    Resolve the bindings in *ti_context* at this map index, from *task*, the Python Dag's stub task.

    ``map()`` callables run here, as they do in a Python task.

    :raises AirflowSkipException: if a ``map()`` callable skips this instance.
    :raises ArgResolutionError: if an argument has no value at this map index.
    """
    entries = [
        binding.model_dump(mode="json", exclude_unset=True) for binding in ti_context.arg_bindings or ()
    ]
    if not task.is_mapped:
        task = task.prepare_for_execution()
    return resolve_entries(build_runtime_ti(ti, ti_context, task), entries)


def build_runtime_ti(ti: TaskInstance, ti_context: TIRunContext, task: Any) -> RuntimeTaskInstance:
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

    return RuntimeTaskInstance.model_construct(
        **ti.model_dump(exclude_unset=True),
        task=task,
        bundle_instance=None,
        _ti_context_from_server=ti_context,
        max_tries=ti_context.max_tries,
        start_date=ti_context.start_date or ti_context.dag_run.start_date,
    )


def resolve_entries(ti: RuntimeTaskInstance, entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Render *ti*'s task as a Python task renders itself, and bind each entry to what it resolved to."""
    ti.task.render_template_fields(context=ti.get_template_context())
    # A mapped task is replaced by its unmapped form once rendered.
    rendered: Any = ti.task
    bound = inspect.signature(rendered.python_callable).bind_partial(*rendered.op_args, **rendered.op_kwargs)
    given = set(bound.arguments)
    bound.apply_defaults()
    bindings = []
    for entry in entries:
        name = entry["name"]
        if entry["kind"] == "literal":
            bindings.append(entry)
            continue
        if name not in bound.arguments:
            raise ArgResolutionError(f"Argument {name!r} has no value at map index {ti.map_index}")
        value = bound.arguments[name]
        if entry["kind"] == "sequence" and isinstance(value, LazyXComSequence):
            bindings.append(entry)
        else:
            bindings.append(build_literal(entry, value, from_default=name not in given))
    return bindings


def build_literal(entry: Mapping[str, Any], value: Any, *, from_default: bool = False) -> dict[str, Any]:
    binding: dict[str, Any] = {"name": entry["name"], "kind": "literal", "value": to_json(value)}
    if (value_schema := entry.get("value_schema")) is not None:
        binding["value_schema"] = value_schema
    if from_default:
        binding["from_default"] = True
    return binding


def to_json(value: Any) -> Any:
    """Turn what Python resolution gives, such as a ``zip()`` tuple, into the JSON the runtime receives."""
    if isinstance(value, Mapping):
        return {str(key): to_json(item) for key, item in value.items()}
    if isinstance(value, (str, bytes)):
        return value
    if isinstance(value, Sequence):
        return [to_json(item) for item in value]
    return value
