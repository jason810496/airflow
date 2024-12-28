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

from airflow.plugins_manager import AirflowPlugin
from airflow.providers.openlineage import conf
from airflow.providers.openlineage.plugins.listener import get_openlineage_listener
from airflow.providers.openlineage.plugins.macros import (
    lineage_job_name,
    lineage_job_namespace,
    lineage_parent_id,
    lineage_run_id,
)
from airflow.providers.openlineage.version_compat import AIRFLOW_V_2_10_PLUS


class OpenLineageProviderPlugin(AirflowPlugin):
    """
    Listener that emits numerous Events.

    OpenLineage Plugin provides listener that emits OL events on DAG start,
    complete and failure and TaskInstances start, complete and failure.
    """

    name = "OpenLineageProviderPlugin"
    if not conf.is_disabled():
        macros = [lineage_job_namespace, lineage_job_name, lineage_run_id, lineage_parent_id]
        listeners = [get_openlineage_listener()]
        if AIRFLOW_V_2_10_PLUS:
            from airflow.providers.common.compat.lineage.hook import HookLineageReader

            hook_lineage_readers = [HookLineageReader]
    else:
        macros = []
        listeners = []

    def on_load(*args, **kwargs):
        """Monkey patches the core modules with compatible lineage logic."""
        import airflow.io.path as airflow_io_path
        import airflow.models.baseoperator as airflow_baseoperator
        from airflow.providers.common.compat.lineage import (
            apply_lineage,
            object_storage_path_copy_lineage_decorator,
            object_storage_path_move_lineage_decorator,
            prepare_lineage,
            tracking_file_getattr_decorator,
        )

        # monkey patch the BaseOperator
        airflow_baseoperator.BaseOperator.pre_execute = prepare_lineage(
            airflow_baseoperator.BaseOperator.pre_execute
        )
        airflow_baseoperator.BaseOperator.post_execute = apply_lineage(
            airflow_baseoperator.BaseOperator.post_execute
        )
        # monkey patch the TrackingFileWrapper
        airflow_io_path.TrackingFileWrapper.__getattr__ = tracking_file_getattr_decorator(
            airflow_io_path.TrackingFileWrapper.__getattr__
        )
        # monkey patch the ObjectStoragePath
        airflow_io_path.ObjectStoragePath.copy = object_storage_path_copy_lineage_decorator(
            airflow_io_path.ObjectStoragePath.copy
        )
        airflow_io_path.ObjectStoragePath.move = object_storage_path_move_lineage_decorator(
            airflow_io_path.ObjectStoragePath.move
        )
