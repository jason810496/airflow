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

from typing import Any
from urllib.parse import urlsplit


def remote_log_io_factory(
    *,
    base_log_folder: str,
    remote_base_log_folder: str,
    delete_local_copy: bool,
    remote_task_handler_kwargs: dict[str, Any],
) -> tuple[Any, str | None]:
    """Build an HdfsRemoteLogIO instance from Airflow configuration."""
    from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
    from airflow.providers.apache.hdfs.log.hdfs_task_handler import HdfsRemoteLogIO

    remote_log_io = HdfsRemoteLogIO(
        **(
            {
                "base_log_folder": base_log_folder,
                "remote_base": urlsplit(remote_base_log_folder).path,
                "delete_local_copy": delete_local_copy,
            }
            | remote_task_handler_kwargs
        )
    )
    return remote_log_io, WebHDFSHook.default_conn_name
