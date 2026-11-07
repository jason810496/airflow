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
"""Version 2026-10-30: Airflow 3.4.0, stamped ``3``."""

from __future__ import annotations

from typing import Any

from airflow.serialization.versions.base import (
    SerializedDagChange,
    Version,
    iter_task_groups,
    iter_tasks,
    pop_keys,
)

_STUB_TASK_MODULE = "airflow.providers.standard.decorators.stub"
_STUB_TASK_TYPE = "_StubOperator"


class AddBundleName(SerializedDagChange):
    """
    Add ``dag.bundle_name``, written only when set and left out of the hash.

    Nothing to upgrade: the value lives in ``DagModel.bundle_name``.
    """

    description = __doc__

    def downgrade(self, data: dict[str, Any]) -> None:
        # The dag object is closed in older schemas; the value is not lost, DagModel keeps it.
        dag = data.get("dag")
        if isinstance(dag, dict):
            dag.pop("bundle_name", None)


class AddStubTaskBindings(SerializedDagChange):
    """
    Add task keys ``is_stub: true`` and ``_arg_bindings`` for Lang SDK stub tasks.

    ``_arg_bindings`` cannot be backfilled.
    """

    description = __doc__

    def upgrade(self, data: dict[str, Any]) -> None:
        for task in iter_tasks(data):
            if task.get("_task_module") == _STUB_TASK_MODULE and task.get("task_type") == _STUB_TASK_TYPE:
                task["is_stub"] = True

    def downgrade(self, data: dict[str, Any]) -> None:
        lossy = False
        for task in iter_tasks(data):
            lossy = bool(pop_keys(task, ("is_stub", "_arg_bindings"))) or lossy
        if lossy:
            self.warn_lossy(data, "dropped is_stub and _arg_bindings")


class AddTaskGroupLoop(SerializedDagChange):
    """Add task group ``loop: {max_iterations, has_until, terminal_task_id, gate_task_id}``."""

    description = __doc__

    def downgrade(self, data: dict[str, Any]) -> None:
        lossy = False
        for group in iter_task_groups(data):
            lossy = group.pop("loop", None) is not None or lossy
        if lossy:
            self.warn_lossy(data, "dropped task group loop")


class AddReturnsDagResult(SerializedDagChange):
    """Add task key ``returns_dag_result: true``, never written when false."""

    description = __doc__

    def downgrade(self, data: dict[str, Any]) -> None:
        lossy = False
        for task in iter_tasks(data):
            lossy = bool(task.pop("returns_dag_result", None)) or lossy
        if lossy:
            self.warn_lossy(data, "dropped returns_dag_result")


VERSION = Version(
    id="2026-10-30",
    changes=(AddBundleName, AddStubTaskBindings, AddTaskGroupLoop, AddReturnsDagResult),
    airflow_version="3.4.0",
    stamp=3,
)
