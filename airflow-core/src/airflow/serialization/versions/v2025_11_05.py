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
"""Version 2025-11-05: Airflow 3.1.2 and 3.1.4, stamped ``3``."""

from __future__ import annotations

from typing import Any

from airflow.serialization.versions.base import SerializedDagChange, Version, get_default_args

_CALLBACK_KEYS = tuple(
    f"on_{callback_type}_callback" for callback_type in ("execute", "failure", "success", "retry", "skipped")
)


class ReplaceDefaultArgsCallbacksWithFlags(SerializedDagChange):
    """Write ``has_on_*_callback: true`` in ``dag.default_args`` instead of the callback's repr."""

    description = __doc__

    def upgrade(self, data: dict[str, Any]) -> None:
        if (default_args := get_default_args(data)) is None:
            return
        for key in _CALLBACK_KEYS:
            if key in default_args and default_args.pop(key):
                default_args[f"has_{key}"] = True

    def downgrade(self, data: dict[str, Any]) -> None:
        if (default_args := get_default_args(data)) is None:
            return
        if [key for key in _CALLBACK_KEYS if default_args.pop(f"has_{key}", None) is not None]:
            self.warn_lossy(data, "dropped has_on_*_callback from default_args")


class KeepExplicitSchemaDefaults(SerializedDagChange):
    """
    Write a task value equal to its schema default when the user set it and ``client_defaults`` differs.

    Nothing to convert either way: older writers dropped the user's choice, so it cannot be backfilled.
    """

    description = __doc__


VERSION = Version(
    id="2025-11-05",
    changes=(ReplaceDefaultArgsCallbacksWithFlags, KeepExplicitSchemaDefaults),
    airflow_version="3.1.2",
    stamp=3,
)
