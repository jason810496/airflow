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
from __future__ import annotations

from pathlib import Path

from airflow.dag_processing.bundles.base import BaseDagBundle


class PlaceholderProviderDagBundle(BaseDagBundle):
    """
    DAG bundle for PlaceholderProvider.

    :param conn_id: Connection ID to use.
    :param path_prefix: Remote path prefix.
    """

    supports_versioning = False

    def __init__(
        self, *, conn_id: str = "placeholder_provider_default", path_prefix: str = "", **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.path_prefix = path_prefix

    def get_path(self) -> Path:
        """Return the local path for DAG files."""
        raise NotImplementedError

    def refresh(self) -> None:
        """Refresh the bundle contents."""
        raise NotImplementedError
