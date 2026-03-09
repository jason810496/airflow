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

import importlib
import sys
import types
from unittest import mock

from airflow.config_templates import airflow_local_settings
from airflow.providers_manager import ProvidersManager
from airflow.providers_manager import RemoteLoggingInfo

from tests_common.test_utils.config import conf_vars


class DummyHook:
    default_conn_name = "dummy_default"


class DummyRemoteLogIO:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    @classmethod
    def from_airflow_config(cls, **kwargs):
        return cls(**kwargs)


def test_remote_logging_is_discovered_from_provider_scheme():
    module = types.ModuleType("dummy_remote_logging_backend")
    module.DummyHook = DummyHook
    module.DummyRemoteLogIO = DummyRemoteLogIO
    sys.modules[module.__name__] = module
    try:
        with conf_vars(
            {
                ("logging", "remote_logging"): "True",
                ("logging", "remote_base_log_folder"): "dummy://logs",
                ("logging", "remote_task_handler_kwargs"): '{"test_value": "from_kwargs"}',
            }
        ):
            with (
                mock.patch.object(
                    ProvidersManager,
                    "get_remote_logging_info_for_uri",
                    return_value=RemoteLoggingInfo(
                        task_handler_class_name="dummy.TaskHandler",
                        remote_log_io_class_name="dummy_remote_logging_backend.DummyRemoteLogIO",
                        provider_name="dummy-provider",
                        schemes=("dummy",),
                        hook_class_name="dummy_remote_logging_backend.DummyHook",
                    ),
                ),
                mock.patch.object(ProvidersManager, "get_remote_logging_info_for_backend", return_value=None),
            ):
                importlib.reload(airflow_local_settings)

        assert isinstance(airflow_local_settings.REMOTE_TASK_LOG, DummyRemoteLogIO)
        assert airflow_local_settings.DEFAULT_REMOTE_CONN_ID == "dummy_default"
        assert airflow_local_settings.REMOTE_TASK_LOG.kwargs == {
            "base_log_folder": airflow_local_settings.BASE_LOG_FOLDER,
            "remote_base_log_folder": "dummy://logs",
            "delete_local_logs": False,
            "remote_task_handler_kwargs": {"test_value": "from_kwargs"},
        }
    finally:
        sys.modules.pop(module.__name__, None)
        importlib.reload(airflow_local_settings)


def test_remote_logging_uses_backend_name_fallback():
    module = types.ModuleType("dummy_backend_logging_backend")
    module.DummyRemoteLogIO = DummyRemoteLogIO
    sys.modules[module.__name__] = module
    try:
        with conf_vars(
            {
                ("logging", "remote_logging"): "True",
                ("logging", "remote_base_log_folder"): "unused://logs",
                ("opensearch", "host"): "example.com",
            }
        ):
            with (
                mock.patch.object(ProvidersManager, "get_remote_logging_info_for_uri", return_value=None),
                mock.patch.object(
                    ProvidersManager,
                    "get_remote_logging_info_for_backend",
                    return_value=RemoteLoggingInfo(
                        task_handler_class_name="dummy.TaskHandler",
                        remote_log_io_class_name="dummy_backend_logging_backend.DummyRemoteLogIO",
                        provider_name="dummy-provider",
                        backend_name="opensearch",
                    ),
                ),
            ):
                importlib.reload(airflow_local_settings)

        assert isinstance(airflow_local_settings.REMOTE_TASK_LOG, DummyRemoteLogIO)
    finally:
        sys.modules.pop(module.__name__, None)
        importlib.reload(airflow_local_settings)
