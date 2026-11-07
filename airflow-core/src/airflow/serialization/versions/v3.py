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
"""Version 3: Airflow 3.1.0."""

from __future__ import annotations

from typing import Any

from airflow.serialization.versions.base import (
    SerializedDagChange,
    Version,
    iter_partial_kwargs,
    iter_tasks,
)

_CALLBACK_TYPES = ("execute", "failure", "success", "retry", "skipped")
# Airflow 3.0 decodes these partial_kwargs values with BaseSerialization.deserialize, which only
# turns the encoded form back into a timedelta.
_TIMEDELTA_FIELDS = frozenset({"retry_delay", "execution_timeout", "max_retry_delay"})


class AddClientDefaults(SerializedDagChange):
    """
    Move task defaults that differ from the schema defaults to a top-level ``client_defaults.tasks``.

    Task values equal to a schema default or to ``client_defaults.tasks`` are no longer written,
    in tasks and in mapped ``partial_kwargs``.
    """

    description = __doc__

    def downgrade(self, data: dict[str, Any]) -> None:
        # Airflow 3.0 closes the root object, so client_defaults must go.
        client_defaults = data.pop("client_defaults", None)
        task_defaults = client_defaults.get("tasks") if isinstance(client_defaults, dict) else None
        if not task_defaults:
            return
        for task in iter_tasks(data):
            if isinstance(partial_kwargs := task.get("partial_kwargs"), dict):
                # A MappedOperator in 3.0 reads these from partial_kwargs, not from the task.
                for key, value in task_defaults.items():
                    if key not in partial_kwargs:
                        partial_kwargs[key] = (
                            {"__type": "timedelta", "__var": value}
                            if key in _TIMEDELTA_FIELDS and value is not None
                            else value
                        )
            else:
                for key, value in task_defaults.items():
                    task.setdefault(key, value)


class ReplaceTaskCallbacksWithFlags(SerializedDagChange):
    """Write ``has_on_*_callback: true`` instead of ``on_*_callback``, in tasks and ``partial_kwargs``."""

    description = __doc__

    def downgrade(self, data: dict[str, Any]) -> None:
        for container in (*iter_tasks(data), *iter_partial_kwargs(data)):
            for callback_type in _CALLBACK_TYPES:
                flag = f"has_on_{callback_type}_callback"
                if flag in container:
                    if container.pop(flag):
                        container[f"on_{callback_type}_callback"] = True


class ExcludeDagSchemaDefaults(SerializedDagChange):
    """Stop writing Dag keys equal to their new schema defaults, None values and empty collections."""

    description = __doc__

    # Airflow 3.0 sets every Dag key missing from the blob to None, so the defaults go back in.
    # has_on_*_callback is left out: 3.0 reads the key's presence as true, and 3.1 writes it
    # only when true.
    SCHEMA_DEFAULTS: dict[str, Any] = {
        "catchup": False,
        "fail_fast": False,
        "render_template_as_native_obj": False,
        "disable_bundle_versioning": False,
        "max_active_tasks": 16,
        "max_active_runs": 16,
        "max_consecutive_failed_dag_runs": 0,
    }

    def downgrade(self, data: dict[str, Any]) -> None:
        dag = data.get("dag")
        if not isinstance(dag, dict):
            return
        for key, value in self.SCHEMA_DEFAULTS.items():
            dag.setdefault(key, value)


class RenamePythonCallableToName(SerializedDagChange):
    """
    Write mapped ``partial_kwargs.python_callable_name`` (a qualname) instead of ``python_callable``.

    The upgrade cannot be backfilled: the old ``str()`` repr lacks the module path.
    """

    description = __doc__

    def downgrade(self, data: dict[str, Any]) -> None:
        for partial_kwargs in iter_partial_kwargs(data):
            if "python_callable_name" in partial_kwargs:
                partial_kwargs["python_callable"] = partial_kwargs.pop("python_callable_name")
                self.warn_lossy(data, "python_callable is restored as its qualname")


class AddDagDeadline(SerializedDagChange):
    """Add ``dag.deadline``: first a dict or null, then a list of dicts."""

    description = __doc__

    def downgrade(self, data: dict[str, Any]) -> None:
        dag = data.get("dag")
        if isinstance(dag, dict) and dag.pop("deadline", None):
            self.warn_lossy(data, "dropped deadline")


VERSION = Version(
    id=3,
    changes=(
        AddClientDefaults,
        ReplaceTaskCallbacksWithFlags,
        ExcludeDagSchemaDefaults,
        RenamePythonCallableToName,
        AddDagDeadline,
    ),
    airflow_version="3.1.0",
)
