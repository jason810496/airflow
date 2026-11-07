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
"""Version 2026-04-06: Airflow 3.2.0 and 3.2.2, stamped ``3``."""

from __future__ import annotations

from typing import Any

from airflow.serialization.versions.base import (
    SerializedDagChange,
    Version,
    iter_partial_kwargs,
    iter_tasks,
)

_PARTITIONED_ASSET_TIMETABLE = "airflow.timetables.simple.PartitionedAssetTimetable"
_ASSET_TRIGGERED_TIMETABLE = "airflow.timetables.simple.AssetTriggeredTimetable"
_EVENTS_TIMETABLE = "airflow.timetables.events.EventsTimetable"
_MAPPED_ARGUMENT = "mapped_argument"


def _get_dag(data: dict[str, Any]) -> dict[str, Any] | None:
    dag = data.get("dag")
    return dag if isinstance(dag, dict) else None


def _iter_param_lists(data: dict[str, Any], *, include_tasks: bool):
    if (dag := _get_dag(data)) is not None and isinstance(dag.get("params"), list):
        yield dag["params"]
    if include_tasks:
        for task in iter_tasks(data):
            if isinstance(task.get("params"), list):
                yield task["params"]


def _iter_params(params: list) -> Any:
    for item in params:
        # Each item is a [name, encoded Param] pair.
        if isinstance(item, (list, tuple)) and len(item) == 2 and isinstance(item[1], dict):
            yield item[1]


class AllowRetryBackoffMultiplier(SerializedDagChange):
    """
    Change task and ``partial_kwargs`` ``retry_exponential_backoff`` from a bool to a number.

    The schema default is 0. Readers map an old bool to 2.0 or 0.
    """

    description = __doc__

    def downgrade(self, data: dict[str, Any]) -> None:
        containers = [*iter_tasks(data), *iter_partial_kwargs(data)]
        client_defaults = data.get("client_defaults")
        if isinstance(client_defaults, dict) and isinstance(client_defaults.get("tasks"), dict):
            containers.append(client_defaults["tasks"])
        lossy = False
        for container in containers:
            value = container.get("retry_exponential_backoff")
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                continue
            # A reader of this version maps True back to 2.0.
            lossy = lossy or value not in (0, 2)
            container["retry_exponential_backoff"] = value != 0
        if lossy:
            self.warn_lossy(data, "retry_exponential_backoff multiplier became a bool")


class AddParamSource(SerializedDagChange):
    """
    Add ``source`` (``"dag"``, ``"task"`` or null) to every serialized Param.

    Upgrade can only tell Dag-level params apart, so task params keep no source.
    """

    description = __doc__

    def upgrade(self, data: dict[str, Any]) -> None:
        for params in _iter_param_lists(data, include_tasks=False):
            for param in _iter_params(params):
                param.setdefault("source", "dag")

    def downgrade(self, data: dict[str, Any]) -> None:
        for params in _iter_param_lists(data, include_tasks=True):
            for param in _iter_params(params):
                param.pop("source", None)


class AddPartitionedAssetTimetable(SerializedDagChange):
    """Add the ``PartitionedAssetTimetable`` timetable, with ``asset_condition`` and partition mappers."""

    description = __doc__

    def downgrade(self, data: dict[str, Any]) -> None:
        if (dag := _get_dag(data)) is None:
            return
        timetable = dag.get("timetable")
        if not isinstance(timetable, dict) or timetable.get("__type") != _PARTITIONED_ASSET_TIMETABLE:
            return
        # Older readers cannot import the class, so it becomes the unpartitioned timetable.
        encoded = timetable.get("__var") or {}
        dag["timetable"] = {
            "__type": _ASSET_TRIGGERED_TIMETABLE,
            "__var": {
                key: encoded[key] for key in ("asset_condition", "batch_asset_events") if key in encoded
            },
        }
        self.warn_lossy(data, "PartitionedAssetTimetable became AssetTriggeredTimetable")


class DropEventsTimetableSummary(SerializedDagChange):
    """Stop writing ``_summary`` for ``EventsTimetable``."""

    description = __doc__

    def downgrade(self, data: dict[str, Any]) -> None:
        if (dag := _get_dag(data)) is None:
            return
        timetable = dag.get("timetable")
        if not isinstance(timetable, dict) or timetable.get("__type") != _EVENTS_TIMETABLE:
            return
        encoded = timetable.get("__var")
        if not isinstance(encoded, dict) or "_summary" in encoded:
            return
        # Matches EventsTimetable.__init__ in the releases that read _summary.
        encoded["_summary"] = encoded.get("description") or f"{len(encoded.get('event_dates') or ())} events"


def _stringify_mapped_arguments(value: Any) -> tuple[Any, bool]:
    if isinstance(value, dict):
        if value.get("__type") == _MAPPED_ARGUMENT and isinstance(encoded := value.get("__var"), dict):
            input_type = (encoded.get("input") or {}).get("type")
            return f"MappedArgument(_input=<{input_type}>, _key={encoded.get('key')!r})", True
        changed = False
        for key, item in value.items():
            value[key], item_changed = _stringify_mapped_arguments(item)
            changed = changed or item_changed
        return value, changed
    if isinstance(value, list):
        changed = False
        for index, item in enumerate(value):
            value[index], item_changed = _stringify_mapped_arguments(item)
            changed = changed or item_changed
        return value, changed
    return value, False


class EncodeMappedArgument(SerializedDagChange):
    """
    Encode a ``MappedArgument`` as ``{__type: "mapped_argument", __var: {input, key}}`` instead of ``str()``.

    Applies inside mapped ``expand_input.value`` and ``partial_kwargs``. The old repr cannot be parsed.
    """

    description = __doc__

    def downgrade(self, data: dict[str, Any]) -> None:
        lossy = False
        for task in iter_tasks(data):
            for key in ("expand_input", "op_kwargs_expand_input", "partial_kwargs"):
                if key in task:
                    task[key], changed = _stringify_mapped_arguments(task[key])
                    lossy = lossy or changed
        if lossy:
            self.warn_lossy(data, "MappedArgument became its string form")


class AddTaskRenderTemplateAsNativeObj(SerializedDagChange):
    """Add task key ``render_template_as_native_obj``, written only when set."""

    description = __doc__

    def downgrade(self, data: dict[str, Any]) -> None:
        lossy = False
        for container in (*iter_tasks(data), *iter_partial_kwargs(data)):
            lossy = container.pop("render_template_as_native_obj", None) is not None or lossy
        if lossy:
            self.warn_lossy(data, "dropped task render_template_as_native_obj")


class ReferenceDeadlineAlertsById(SerializedDagChange):
    """
    Store ``dag.deadline`` as UUIDs of ``deadline_alert`` rows; inline items lose their encoding wrapper.

    Migration 0101 moves the data both ways.
    """

    description = __doc__
    requires_migration = True


class AddAllowedRunTypes(SerializedDagChange):
    """Add ``dag.allowed_run_types``, a sorted list of strings or null, always written."""

    description = __doc__

    def upgrade(self, data: dict[str, Any]) -> None:
        if (dag := _get_dag(data)) is not None:
            dag.setdefault("allowed_run_types", None)

    def downgrade(self, data: dict[str, Any]) -> None:
        if (dag := _get_dag(data)) is not None and dag.pop("allowed_run_types", None) is not None:
            self.warn_lossy(data, "dropped allowed_run_types")


class AlwaysWriteDagLimits(SerializedDagChange):
    """
    Always write ``catchup``, the run and task limits and ``disable_bundle_versioning`` (Airflow 3.2.2).

    Their schema defaults were removed. From 3.1.0 to 3.2.1 a missing key meant the old default.
    A Lang SDK Dag gets the config values from ``fill_config_defaults`` before it is read.
    """

    description = __doc__

    OLD_DEFAULTS: dict[str, Any] = {
        "catchup": False,
        "max_active_tasks": 16,
        "max_active_runs": 16,
        "max_consecutive_failed_dag_runs": 0,
        "disable_bundle_versioning": False,
    }

    def upgrade(self, data: dict[str, Any]) -> None:
        if (dag := _get_dag(data)) is None:
            return
        for key, value in self.OLD_DEFAULTS.items():
            dag.setdefault(key, value)


VERSION = Version(
    id="2026-04-06",
    changes=(
        AllowRetryBackoffMultiplier,
        AddParamSource,
        AddPartitionedAssetTimetable,
        DropEventsTimetableSummary,
        EncodeMappedArgument,
        AddTaskRenderTemplateAsNativeObj,
        ReferenceDeadlineAlertsById,
        AddAllowedRunTypes,
        AlwaysWriteDagLimits,
    ),
    airflow_version="3.2.0",
    stamp=3,
)
