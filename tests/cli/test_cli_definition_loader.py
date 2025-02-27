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

from unittest import mock
from unittest.mock import MagicMock, patch, PropertyMock

import pytest
from airflow.providers_manager import ProvidersManager
from airflow.cli.cli_definition_loader import CliDefinitionLoader
from airflow.exceptions import (
    AirflowLoadAuthManagerCliDefinitionException,
    AirflowLoadCliDefinitionsException,
    AirflowLoadExecutorCliDefinitionException,
    AirflowLoadProviderCliDefinitionException,
)

KUBERNETES_PROVIDER_NAME = "apache-airflow-providers-cncf-kubernetes"

class TestCliDefinitionLoader:
    @mock.patch("airflow.providers.cncf.kubernetes.cli.definition.KUBERNETES_GROUP_COMMANDS", autospec=True)
    def test__load_provider_cli_definitions(self, mock_provider_cli_definitions):
        mock_provider_cli_definitions = [MagicMock()] # Ensure it's a list
        with patch.object(ProvidersManager, "providers", new_callable=PropertyMock) as mock_provider_manager_providers:
            mock_provider_manager_providers.return_value = {
                KUBERNETES_PROVIDER_NAME: MagicMock()
            }
            with patch("airflow.cli.cli_definition_loader.import_string") as mock_import_string:
                mock_import_string.return_value = mock_provider_cli_definitions
                assert CliDefinitionLoader._load_provider_cli_definitions() == (mock_provider_cli_definitions, [])
                mock_import_string.assert_called_once_with("airflow.providers.cncf.kubernetes.cli.definition.KUBERNETES_GROUP_COMMANDS")


    def test__load_provider_cli_definitions_exception(self):
        with patch.object(ProvidersManager, "providers", new_callable=PropertyMock) as mock_provider_manager_providers:
            mock_provider_manager_providers.return_value = {
                KUBERNETES_PROVIDER_NAME: MagicMock()
            }
            with patch("airflow.cli.cli_definition_loader.import_string") as mock_import_string:
                mock_import_string.side_effect = ImportError
                loaded_cli, errors = CliDefinitionLoader._load_provider_cli_definitions()
                assert loaded_cli == []
                assert len(errors) == 1
                assert str(errors[0]) == str(AirflowLoadProviderCliDefinitionException(f"Failed to load CLI commands from provider: {KUBERNETES_PROVIDER_NAME}"))
                

    @pytest.mark.skip(
        reason="Will test this method after decoupling `get_cli_commands` method from `Executor` and `AuthManager`"
    )
    def test_get_cli_commands(self):
        # will test this method after decoupling
        pass
