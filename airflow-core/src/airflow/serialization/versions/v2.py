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
"""Version 2: Airflow 3.0."""

from __future__ import annotations

import datetime
import itertools
from typing import Any

from airflow.serialization.versions.base import SerializedDagChange, Version


def _replace_dataset_in_str(s: str) -> str:
    return s.replace("Dataset", "Asset").replace("dataset", "asset")


def _replace_dataset_with_asset_in_timetables(obj: Any, parent_key: str | None = None) -> Any:
    if isinstance(obj, dict):
        new_obj = {}
        for k, v in obj.items():
            new_key = _replace_dataset_in_str(k) if isinstance(k, str) else k
            # Don't replace uri values
            if new_key == "uri":
                new_obj[new_key] = v
            else:
                new_value = (
                    _replace_dataset_in_str(v)
                    if isinstance(v, str)
                    else _replace_dataset_with_asset_in_timetables(v, parent_key=new_key)
                )
                new_obj[new_key] = new_value
        # Insert "name" and "group" if this is inside the 'objects' list
        if parent_key == "objects":
            new_obj["name"] = None
            new_obj["group"] = None
        return new_obj
    if isinstance(obj, list):
        return [_replace_dataset_with_asset_in_timetables(i, parent_key=parent_key) for i in obj]
    return obj


class ConvertAirflow2Dag(SerializedDagChange):
    """Convert a Dag serialized by Airflow 2 to the Airflow 3 format."""

    description = __doc__

    def upgrade(self, data: dict[str, Any]) -> None:
        from airflow import settings
        from airflow._shared.timezones.timezone import parse_timezone
        from airflow.sdk.definitions.dag import _create_timetable  # noqa: SDK001
        from airflow.serialization.decoders import decode_relativedelta
        from airflow.serialization.encoders import encode_timetable
        from airflow.serialization.serialized_objects import DagSerialization

        dag_dict = data["dag"]
        dag_renames = [
            ("_dag_id", "dag_id"),
            ("_task_group", "task_group"),
            ("_access_control", "access_control"),
        ]
        task_renames = [("_task_type", "task_type"), ("task_display_name", "_task_display_name")]
        tasks_remove = [
            "_log_config_logger_name",
            "deps",
            "sla",
            # Operator extra links from Airflow 2 won't work anymore, only new ones, so remove these
            "_operator_extra_links",
        ]

        def _create_compat_timetable(value):
            if tzs := dag_dict.get("timezone"):
                timezone = parse_timezone(tzs)
            else:
                timezone = settings.TIMEZONE
            return encode_timetable(_create_timetable(value, timezone))

        for old, new in dag_renames:
            if old in dag_dict:
                dag_dict[new] = dag_dict.pop(old)

        if default_args := dag_dict.get("default_args"):
            for k in tasks_remove:
                default_args["__var"].pop(k, None)

        if timetable := dag_dict.get("timetable"):
            if timetable["__type"] in {
                "airflow.timetables.simple.DatasetTriggeredTimetable",
                "airflow.timetables.datasets.DatasetOrTimeSchedule",
            }:
                dag_dict["timetable"] = _replace_dataset_with_asset_in_timetables(dag_dict["timetable"])
        elif (sched := dag_dict.pop("schedule_interval", None)) is None:
            dag_dict["timetable"] = _create_compat_timetable(None)
        elif isinstance(sched, str):
            dag_dict["timetable"] = _create_compat_timetable(sched)
        elif sched.get("__type") == "timedelta":
            dag_dict["timetable"] = _create_compat_timetable(datetime.timedelta(seconds=sched["__var"]))
        elif sched.get("__type") == "relativedelta":
            dag_dict["timetable"] = _create_compat_timetable(decode_relativedelta(sched["__var"]))
        else:
            # We should maybe convert this to None and warn instead
            raise ValueError(f"Unknown schedule_interval field {sched!r}")

        if "dag_dependencies" in dag_dict:
            for dep in dag_dict["dag_dependencies"]:
                dep_type = dep.get("dependency_type")
                if dep_type in ("dataset", "dataset-alias"):
                    dep["dependency_type"] = dep_type.replace("dataset", "asset")

                if not dep.get("label"):
                    dep["label"] = dep["dependency_id"]

                for fld in ("target", "source"):
                    val = dep.get(fld)
                    if val == dep_type and val in ("dataset", "dataset-alias"):
                        dep[fld] = dep[fld].replace("dataset", "asset")
                    elif val.startswith("dataset:"):
                        dep[fld] = dep[fld].replace("dataset:", "asset:")
                    elif val.startswith("dataset-alias:"):
                        dep[fld] = dep[fld].replace("dataset-alias:", "asset-alias:")

        op_defaults = DagSerialization.get_schema_defaults("operator")
        for task in dag_dict["tasks"]:
            task_var: dict = task["__var"]
            if "airflow.ti_deps.deps.ready_to_reschedule.ReadyToRescheduleDep" in task_var.get("deps", []):
                task_var["_is_sensor"] = True
            for k in tasks_remove:
                task_var.pop(k, None)
            for old, new in task_renames:
                if old in task_var:
                    task_var[new] = task_var.pop(old)
            for item in itertools.chain(*(task_var.get(key, []) for key in ("inlets", "outlets"))):
                original_item_type = item["__type"]
                if isinstance(item, dict) and "__type" in item:
                    item["__type"] = _replace_dataset_in_str(original_item_type)

                var_ = item["__var"]
                if original_item_type == "dataset":
                    var_["name"] = var_["uri"]
                var_["group"] = "asset"

            for k, v in list(task_var.items()):
                if k in op_defaults and v == op_defaults[k]:
                    del task_var[k]

        # Set on the root TG
        dag_dict["task_group"]["group_display_name"] = ""

    def downgrade(self, data: dict[str, Any]) -> None:
        raise NotImplementedError("Airflow 2 does not read serialized Dags written by Airflow 3")


VERSION = Version(id=2, changes=(ConvertAirflow2Dag,), airflow_version="3.0.0")
