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
"""Version 2025-10-27: Airflow 3.1.1, stamped ``3``."""

from __future__ import annotations

from typing import Any

from airflow.serialization.versions.base import SerializedDagChange, Version


class OmitDefaultRetryDelay(SerializedDagChange):
    """
    Add the schema default ``retry_delay: 300.0`` to tasks.

    ``client_defaults.tasks.retry_delay`` is no longer written at 300 seconds, and an empty
    ``client_defaults`` is dropped.
    """

    description = __doc__

    def downgrade(self, data: dict[str, Any]) -> None:
        # Airflow 3.1.0's SerializedBaseOperator.retry_delay has no default and needs this value.
        client_defaults = data.get("client_defaults")
        if not isinstance(client_defaults, dict):
            client_defaults = data["client_defaults"] = {}
        task_defaults = client_defaults.get("tasks")
        if not isinstance(task_defaults, dict):
            task_defaults = client_defaults["tasks"] = {}
        task_defaults.setdefault("retry_delay", 300.0)


VERSION = Version(id="2025-10-27", changes=(OmitDefaultRetryDelay,), airflow_version="3.1.1", stamp=3)
