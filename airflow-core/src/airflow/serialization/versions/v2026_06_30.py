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
"""Version 2026-06-30: Airflow 3.3.0, stamped ``3``."""

from __future__ import annotations

from typing import Any

from airflow.serialization.versions.base import (
    SerializedDagChange,
    Version,
    get_default_args,
    iter_partial_kwargs,
    iter_task_groups,
)


class AddRerunWithLatestVersion(SerializedDagChange):
    """Add ``dag.rerun_with_latest_version``, written only when not null."""

    description = __doc__

    def downgrade(self, data: dict[str, Any]) -> None:
        dag = data.get("dag")
        if isinstance(dag, dict) and dag.pop("rerun_with_latest_version", None) is not None:
            self.warn_lossy(data, "dropped rerun_with_latest_version")


class AddTaskGroupDocMd(SerializedDagChange):
    """Add task group ``doc_md``, written only when not null."""

    description = __doc__

    def downgrade(self, data: dict[str, Any]) -> None:
        # The task_group object is closed in older schemas.
        lossy = False
        for group in iter_task_groups(data):
            lossy = group.pop("doc_md", None) is not None or lossy
        if lossy:
            self.warn_lossy(data, "dropped task group doc_md")


class ReplaceRetryPolicyWithFlag(SerializedDagChange):
    """
    Write ``has_retry_policy: true`` instead of the ``retry_policy`` repr.

    Applies to ``partial_kwargs`` and ``default_args``. The old form only existed in 3.3.0
    pre-releases, so there is nothing to upgrade.
    """

    description = __doc__

    def downgrade(self, data: dict[str, Any]) -> None:
        containers = list(iter_partial_kwargs(data))
        if (default_args := get_default_args(data)) is not None:
            containers.append(default_args)
        lossy = False
        for container in containers:
            lossy = container.pop("has_retry_policy", None) is not None or lossy
        if lossy:
            self.warn_lossy(data, "dropped has_retry_policy")


VERSION = Version(
    id="2026-06-30",
    changes=(AddRerunWithLatestVersion, AddTaskGroupDocMd, ReplaceRetryPolicyWithFlag),
    airflow_version="3.3.0",
    stamp=3,
)
