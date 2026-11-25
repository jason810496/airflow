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
Turn a stub task's bindings into what its runtime receives (ADR-0016).

The API server sends each upstream value as a coordinate. The supervisor reads it, narrows it to this map
index's value, and sends the runtime a ``literal``, so a runtime receives only ``literal`` and ``sequence``.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any

from airflow.sdk.api.datamodels._generated import (
    ArgPart,
    LiteralArgBinding,
    LiteralValue,
    TaskArgBinding,
    TupleArgBinding,
    XComArgBinding,
    XComRef,
)

ReadXCom = Callable[[XComRef], Any]
"""Read an upstream task's ``return_value`` row, as the configured XCom backend loads it."""

_NO_FALLBACK = object()


class ArgBindingError(ValueError):
    """A stub task's argument has no value at this map index."""


def has_upstream_values(arg_bindings: Iterable[TaskArgBinding] | None) -> bool:
    """Whether any of *arg_bindings* names an upstream value the supervisor has to read."""
    return any(_is_read(binding.root) for binding in arg_bindings or ())


def read_arg_bindings(arg_bindings: Iterable[TaskArgBinding], read: ReadXCom) -> list[TaskArgBinding]:
    """
    Read and narrow each binding the runtime cannot read itself, keeping the rest as they are.

    A binding with ``apply_map`` keeps it, with its ``kwarg`` and its ``fallback`` read: the runtime runs the
    ``map()`` callables first, and only then can the key be taken.

    :raises ArgBindingError: if an ``expand_kwargs()`` argument has neither its key nor a fallback, or a
        value cannot be indexed.
    """
    return [TaskArgBinding(_read_binding(binding.root, read)) for binding in arg_bindings]


def _is_read(binding: Any) -> bool:
    if binding.kind in ("xcom", "tuple"):
        return True
    return binding.kind != "sequence" and (binding.kwarg is not None or binding.fallback is not None)


def _read_binding(binding: Any, read: ReadXCom) -> Any:
    if not _is_read(binding):
        return binding
    value = _read_value(binding, read)
    fallback: Any = _read_part(binding.fallback.root, read) if binding.fallback is not None else _NO_FALLBACK
    literal: dict[str, Any] = {"name": binding.name, "kind": "literal"}
    if binding.value_schema is not None:
        literal["value_schema"] = binding.value_schema
    if binding.apply_map:
        literal |= {"value": value, "apply_map": True, "kwarg": binding.kwarg}
        if fallback is not _NO_FALLBACK:
            literal["fallback"] = ArgPart(LiteralValue(kind="literal", value=fallback))
        return LiteralArgBinding(**literal)
    if binding.kwarg is not None:
        value = _select_kwarg(binding.name, binding.kwarg, value, fallback)
    return LiteralArgBinding(**literal, value=value)


def _read_value(binding: Any, read: ReadXCom) -> Any:
    match binding:
        case XComArgBinding():
            return _read_part(binding, read)
        case TupleArgBinding():
            return [_read_part(part.root, read) for part in binding.parts]
        case _:
            return binding.value


def _read_part(part: XComRef | LiteralValue | XComArgBinding, read: ReadXCom) -> Any:
    if part.kind == "literal":
        return part.value
    value = read(XComRef(kind="xcom", task_id=part.task_id, map_index=part.map_index))
    if part.element_index is None:
        return value
    return _get_element(part.task_id, value, part.element_index)


def _get_element(task_id: str, value: Any, index: int) -> Any:
    """Get element *index* of *value*, a dict's being its ``(key, value)`` pair in stored order."""
    if isinstance(value, Mapping):
        items: Sequence[Any] = [list(item) for item in value.items()]
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        items = value
    else:
        raise ArgBindingError(f"Task {task_id!r} returned {type(value).__name__}, which has no elements")
    try:
        return items[index]
    except IndexError:
        raise ArgBindingError(f"Task {task_id!r} returned {len(items)} elements, not {index + 1}") from None


def _select_kwarg(name: str, key: str, value: Any, fallback: Any) -> Any:
    if not isinstance(value, Mapping):
        raise ArgBindingError(
            f"expand_kwargs() expects a list of dicts, not a list of {type(value).__name__}"
        )
    if key in value:
        return value[key]
    if fallback is _NO_FALLBACK:
        raise ArgBindingError(f"Argument {name!r} is missing: expand_kwargs() did not set it")
    return fallback
