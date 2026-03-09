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

from airflow.configuration import conf


def remote_log_io_factory(
    *,
    base_log_folder: str,
    remote_base_log_folder: str,
    delete_local_copy: bool,
    remote_task_handler_kwargs: dict[str, Any],
) -> tuple[Any, None]:
    """Build an ElasticsearchRemoteLogIO instance from Airflow configuration."""
    from airflow.providers.elasticsearch.log.es_task_handler import ElasticsearchRemoteLogIO

    es_host: str = conf.get("elasticsearch", "HOST")
    remote_log_io = ElasticsearchRemoteLogIO(
        host=es_host,
        target_index=conf.get("elasticsearch", "TARGET_INDEX"),
        write_stdout=conf.getboolean("elasticsearch", "WRITE_STDOUT"),
        write_to_es=conf.getboolean("elasticsearch", "WRITE_TO_ES"),
        offset_field=conf.get("elasticsearch", "OFFSET_FIELD"),
        host_field=conf.get("elasticsearch", "HOST_FIELD"),
        base_log_folder=base_log_folder,
        delete_local_copy=delete_local_copy,
        json_format=conf.getboolean("elasticsearch", "JSON_FORMAT"),
        log_id_template=conf.get(
            "elasticsearch",
            "log_id_template",
            fallback="{dag_id}-{task_id}-{run_id}-{map_index}-{try_number}",
        ),
    )
    return remote_log_io, None
