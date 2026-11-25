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
Resolve a Dag definition's mapped task arguments in the supervisor's Python sidecar (ADR-0016).

The runtime sends its serialized Dag. The sidecar rebuilds the task from it as Task SDK objects, with a
placeholder operator for each upstream task, and resolves it with the Python task runner's own code.
The ``map()`` callables are the runtime's, so the sidecar takes an outermost ``map()`` off, picks the
element, and leaves the callables to the runtime.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, cast

from airflow.sdk.bases.decorator import DecoratedOperator, task_decorator_factory
from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk.coordinators._stub_mapping import (
    ArgResolutionError,
    build_literal,
    build_runtime_ti,
    to_json,
)
from airflow.sdk.definitions._internal.expandinput import (
    DictOfListsExpandInput,
    ListOfDictsExpandInput,
    MappedArgument,
)
from airflow.sdk.definitions.dag import DAG
from airflow.sdk.definitions.taskgroup import MappedTaskGroup, TaskGroup
from airflow.sdk.definitions.xcom_arg import (
    ConcatXComArg,
    MapXComArg,
    PlainXComArg,
    XComArg,
    ZipXComArg,
)
from airflow.sdk.execution_time.lazy_sequence import LazyXComSequence

if TYPE_CHECKING:
    from airflow.sdk.api.datamodels._generated import TaskInstance, TIRunContext
    from airflow.sdk.definitions.context import Context
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

SPREAD_NAME = "**"
"""The name of a ``spread`` binding, whose dict fills several parameters."""

_ENCODED_TYPE = "__type"
_ENCODED_VAR = "__var"


class _PlaceholderOperator(BaseOperator):
    """Stands in for an upstream task: only its id, its task group and whether it is mapped matter."""

    def __init__(self, *, placeholder: Any = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)


class _StubPlaceholderOperator(DecoratedOperator):
    custom_operator_name = "@task.stub"


def resolve_dag_definition_bindings(
    ti: TaskInstance, ti_context: TIRunContext, dag_data: Mapping[str, Any]
) -> list[dict[str, Any]]:
    """
    Resolve the bindings in *ti_context* at this map index, from *dag_data*, the runtime's serialized Dag.

    :raises ArgResolutionError: if an argument has no value at this map index.
    """
    entries = [
        binding.model_dump(mode="json", exclude_unset=True) for binding in ti_context.arg_bindings or ()
    ]
    task = _PlaceholderDag(ti.dag_id, dag_data).build_stub(ti.task_id, entries)
    runtime_ti = build_runtime_ti(ti, ti_context, task.operator)
    return task.resolve(runtime_ti, runtime_ti.get_template_context())


class _PlaceholderDag:
    """The Task SDK objects one task's resolution reads, built from the serialized Dag."""

    def __init__(self, dag_id: str, dag_data: Mapping[str, Any]) -> None:
        self.tasks: dict[str, dict[str, Any]] = {
            encoded[_ENCODED_VAR]["task_id"]: encoded[_ENCODED_VAR] for encoded in dag_data["tasks"]
        }
        self.group_data: dict[str, tuple[str | None, dict[str, Any]]] = {}
        self.group_of_task: dict[str, str | None] = {}
        self._index_group(dag_data["task_group"], None, None)
        self.dag = DAG(dag_id, schedule=None)
        self.groups: dict[str | None, TaskGroup] = {None: self.dag.task_group}
        self.operators: dict[str, Any] = {}
        # Keys of a mapped task group's dict-of-lists expand input whose outermost map() was taken off.
        self.mapped_group_keys: dict[str, set[str]] = {}

    def _index_group(self, data: dict[str, Any], group_id: str | None, parent_id: str | None) -> None:
        if group_id is not None:
            self.group_data[group_id] = (parent_id, data)
        for child_id, (kind, child) in data["children"].items():
            if kind == "taskgroup":
                self._index_group(child, child_id, group_id)
            else:
                self.group_of_task[child_id] = group_id

    def get_task_data(self, task_id: str) -> dict[str, Any]:
        if (data := self.tasks.get(task_id)) is None:
            raise ArgResolutionError(f"Task {task_id!r} is not in the Dag")
        return data

    def get_group(self, group_id: str | None) -> TaskGroup:
        if (existing := self.groups.get(group_id)) is not None:
            return existing
        if group_id is None or group_id not in self.group_data:
            raise ArgResolutionError(f"Task group {group_id!r} is not in the Dag")
        parent_id, data = self.group_data[group_id]
        common = {
            "group_id": data["_group_id"],
            "parent_group": self.get_group(parent_id),
            "dag": self.dag,
            "prefix_group_id": data.get("prefix_group_id", True),
        }
        if data.get("is_mapped") and (expand_input := data.get("expand_input")) is not None:
            decoded, mapped_keys = self.decode_expand_input(expand_input)
            self.mapped_group_keys[group_id] = mapped_keys
            group: TaskGroup = MappedTaskGroup(expand_input=decoded, **common)
        else:
            group = TaskGroup(**common)
        self.groups[group_id] = group
        return group

    def get_operator(self, task_id: str) -> Any:
        if (op := self.operators.get(task_id)) is not None:
            return op
        group_id = self.group_of_task.get(task_id)
        kwargs = {
            "task_id": self._get_local_id(task_id, group_id),
            "task_group": self.get_group(group_id),
            "dag": self.dag,
        }
        if self.get_task_data(task_id).get("_is_mapped"):
            op = _PlaceholderOperator.partial(**kwargs).expand(placeholder=[None])
        else:
            op = _PlaceholderOperator(**kwargs)
        self.operators[task_id] = op
        return op

    def _get_local_id(self, task_id: str, group_id: str | None) -> str:
        if group_id is not None and self.group_data[group_id][1].get("prefix_group_id", True):
            return task_id.removeprefix(f"{group_id}.")
        return task_id

    def build_stub(self, task_id: str, entries: list[dict[str, Any]]) -> _StubTask:
        data = self.get_task_data(task_id)
        group_id = self.group_of_task.get(task_id)
        params = [entry for entry in entries if entry["kind"] != "kwargs"]
        factory: Any = task_decorator_factory(
            _build_signature_function(task_id, params),
            decorated_operator_class=_StubPlaceholderOperator,
        ).override(
            task_id=self._get_local_id(task_id, group_id), task_group=self.get_group(group_id), dag=self.dag
        )
        stub = _StubTask(self, entries)
        if not data.get("_is_mapped"):
            stub.operator = factory(**{entry["name"]: self._decode_entry(entry) for entry in params}).operator
            return stub
        partial = self.decode((data.get("partial_kwargs") or {}).get("op_kwargs") or {})
        stub.partial = partial
        expand_input, stub.mapped_keys = self.decode_expand_input(
            data[data.get("_expand_input_attr", "expand_input")]
        )
        if partial:
            factory = factory.partial(**partial)
        if isinstance(expand_input, DictOfListsExpandInput):
            stub.operator = factory.expand(**expand_input.value).operator
        else:
            stub.kwargs_source = expand_input
            stub.operator = factory.expand_kwargs(
                expand_input.value, strict=bool(data.get("_disallow_kwargs_override"))
            ).operator
        return stub

    def _decode_entry(self, entry: dict[str, Any]) -> Any:
        if (group_arg := entry.get("group_arg")) is not None:
            group = self.get_group(group_arg["group_id"])
            if not isinstance(group, MappedTaskGroup):
                raise ArgResolutionError(f"Task group {group_arg['group_id']!r} is not mapped")
            if isinstance(group._expand_input, _ApplyMapSource):
                # Its items are dicts only once the runtime applies map(), so _StubTask reads it itself.
                return None
            return MappedArgument(input=group._expand_input, key=group_arg["key"])
        match entry["kind"]:
            case "xcom" | "sequence":
                return PlainXComArg(self.get_operator(entry["task_id"]))
            case "literal":
                return entry.get("value")
            case kind:
                raise ArgResolutionError(
                    f"Argument {entry['name']!r} has binding kind {kind!r} outside a mapping"
                )

    def decode_expand_input(
        self, data: Mapping[str, Any]
    ) -> tuple[DictOfListsExpandInput | ListOfDictsExpandInput, set[str]]:
        """Decode an expand input, taking off each outermost ``map()``; return the keys it was taken off."""
        value = self.decode(data["value"])
        if data["type"] != "dict-of-lists":
            if isinstance(value, MapXComArg):
                return _ApplyMapSource(value.arg), {SPREAD_NAME}
            return ListOfDictsExpandInput(value), set()
        mapped = {key for key, item in value.items() if isinstance(item, MapXComArg)}
        return DictOfListsExpandInput(
            {key: item.arg if key in mapped else item for key, item in value.items()}
        ), mapped

    def decode(self, value: Any) -> Any:
        if isinstance(value, list):
            return [self.decode(item) for item in value]
        if not isinstance(value, dict):
            return value
        match value.get(_ENCODED_TYPE):
            case None:
                return {key: self.decode(item) for key, item in value.items()}
            case "dict":
                return {key: self.decode(item) for key, item in value[_ENCODED_VAR].items()}
            case "xcomref":
                return self._decode_xcom_arg(value[_ENCODED_VAR], outermost=True)
            case other:
                raise ArgResolutionError(f"A serialized Dag value has the unsupported type {other!r}")

    def _decode_xcom_arg(self, data: Mapping[str, Any], *, outermost: bool = False) -> XComArg:
        match data.get("type"):
            case None:
                return PlainXComArg(self.get_operator(data["task_id"]), data.get("key", "return_value"))
            case "map" if outermost:
                # The callables run in the runtime, so the sidecar never calls these.
                return MapXComArg(
                    self._decode_xcom_arg(data["arg"]), [_runtime_callable] * len(data["callables"])
                )
            case "zip":
                kwargs = {"fillvalue": data["fillvalue"]} if "fillvalue" in data else {}
                return ZipXComArg([self._decode_xcom_arg(arg) for arg in data["args"]], **kwargs)
            case "concat":
                return ConcatXComArg([self._decode_xcom_arg(arg) for arg in data["args"]])
            case other:
                raise ArgResolutionError(f"A serialized Dag has a {other!r} that is not outermost")


def _runtime_callable(value: Any) -> Any:
    raise ArgResolutionError("A map() callable runs in the runtime, not the sidecar")


class _ApplyMapSource(ListOfDictsExpandInput):
    """An ``expand_kwargs()`` input whose outermost ``map()`` was taken off, so its items are not dicts yet."""


class _StubTask:
    """A rebuilt task, and what its resolution needs beyond the operator."""

    def __init__(self, dag: _PlaceholderDag, entries: list[dict[str, Any]]) -> None:
        self.dag = dag
        self.entries = entries
        self.operator: Any = None
        self.partial: dict[str, Any] = {}
        self.mapped_keys: set[str] = set()
        self.kwargs_source: ListOfDictsExpandInput | None = None

    def resolve(self, ti: RuntimeTaskInstance, context: Context) -> list[dict[str, Any]]:
        if self.kwargs_source is not None and (
            isinstance(self.kwargs_source, _ApplyMapSource)
            or any(entry["kind"] == "kwargs" for entry in self.entries)
        ):
            return self._resolve_kwargs(ti, context)
        ti.task.render_template_fields(context=context)
        # A mapped task is replaced by its unmapped form once rendered.
        rendered: Any = ti.task
        bound = inspect.signature(rendered.python_callable).bind_partial(
            *rendered.op_args, **rendered.op_kwargs
        )
        given = set(bound.arguments)
        bound.apply_defaults()
        bindings = []
        for entry in self.entries:
            name = entry["name"]
            if name not in bound.arguments:
                raise ArgResolutionError(f"Argument {name!r} has no value at map index {ti.map_index}")
            value = bound.arguments[name]
            if (group_input := self._get_map_built_group_input(entry)) is not None:
                binding = build_literal(entry, _get_item(group_input, context, _get_map_index(ti)))
                binding.update({"apply_map": True, "kwarg": entry["group_arg"]["key"]})
                bindings.append(binding)
            elif entry["kind"] == "literal":
                bindings.append(entry)
            elif entry["kind"] == "sequence" and isinstance(value, LazyXComSequence):
                bindings.append(entry)
            else:
                binding = build_literal(entry, value, from_default=name not in given)
                if self._is_mapped_argument(entry):
                    binding["apply_map"] = True
                bindings.append(binding)
        return bindings

    def _get_map_built_group_input(self, entry: Mapping[str, Any]) -> _ApplyMapSource | None:
        """Get the ``expand_kwargs()`` input of the mapped task group *entry* reads, if ``map()`` builds its dicts."""
        if (group_arg := entry.get("group_arg")) is None:
            return None
        group = self.dag.get_group(group_arg["group_id"])
        expand_input = group._expand_input if isinstance(group, MappedTaskGroup) else None
        return expand_input if isinstance(expand_input, _ApplyMapSource) else None

    def _is_mapped_argument(self, entry: Mapping[str, Any]) -> bool:
        if (group_arg := entry.get("group_arg")) is not None:
            return group_arg["key"] in self.dag.mapped_group_keys.get(group_arg["group_id"], ())
        return entry["kind"] == "param" and entry["name"] in self.mapped_keys

    def _resolve_kwargs(self, ti: RuntimeTaskInstance, context: Context) -> list[dict[str, Any]]:
        """Resolve an ``expand_kwargs()`` task whose dicts the runtime finishes: spread, or built by ``map()``."""
        source = self.kwargs_source
        if source is None:
            raise ArgResolutionError("The task has no expand_kwargs() input")
        apply_map = isinstance(source, _ApplyMapSource)
        if apply_map:
            item = _get_item(source, context, _get_map_index(ti))
        else:
            mapping, _ = source.resolve(context)
            item = to_json(dict(mapping))
        bindings = []
        for entry in self.entries:
            if entry["kind"] == "kwargs":
                binding = {"name": SPREAD_NAME, "kind": "literal", "value": item, "spread": True}
                if apply_map:
                    binding["apply_map"] = True
                bindings.append(binding)
                continue
            name = entry["name"]
            fallback = self._get_fallback(entry, context)
            if not apply_map:
                if name in item:
                    bindings.append(build_literal(entry, item[name]))
                elif fallback is not None:
                    bindings.append(
                        build_literal(entry, fallback["value"], from_default=name not in self.partial)
                    )
                else:
                    raise ArgResolutionError(f"Argument {name!r} has no value at map index {ti.map_index}")
                continue
            binding = build_literal(entry, item)
            binding.update({"apply_map": True, "kwarg": name})
            if fallback is not None:
                binding["fallback"] = fallback
            bindings.append(binding)
        return bindings

    def _get_fallback(self, entry: Mapping[str, Any], context: Context) -> dict[str, Any] | None:
        name = entry["name"]
        if name in self.partial:
            value = self.partial[name]
            if isinstance(value, XComArg):
                value = value.resolve(context)
            return {"value": to_json(value)}
        if "default" in entry:
            return {"value": entry["default"]}
        return None


def _get_item(source: ListOfDictsExpandInput, context: Context, index: int) -> Any:
    """Read item *index* of an ``expand_kwargs()`` input whose ``map()`` the runtime applies."""
    items = cast("XComArg", source.value).resolve(context)
    if not 0 <= index < len(items):
        raise ArgResolutionError(f"expand_kwargs() has no item at map index {index}")
    return to_json(items[index])


def _get_map_index(ti: RuntimeTaskInstance) -> int:
    return -1 if ti.map_index is None else ti.map_index


def _build_signature_function(task_id: str, entries: list[dict[str, Any]]) -> Callable[..., None]:
    def fn(**kwargs: Any) -> None: ...

    fn.__name__ = fn.__qualname__ = task_id.rsplit(".", 1)[-1]
    fn.__signature__ = inspect.Signature(  # type: ignore[attr-defined]
        [
            inspect.Parameter(
                entry["name"],
                inspect.Parameter.KEYWORD_ONLY,
                **({"default": entry["default"]} if "default" in entry else {}),
            )
            for entry in entries
        ]
    )
    return fn
