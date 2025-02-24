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
"""
Same idea with ExecutorLoader, but for CLI definitions.

Currently, only AuthManager.get_cli_commands() and Executor.get_cli_commands() will be called and extend in cli_parser.
By introducing this class, we can add provider module level CLI commands to the CLI parser.

Additionally, we can speedup CLI for 3-4x based on https://github.com/apache/airflow/issues/46789 benchmark.
"""
from typing import TYPE_CHECKING
from airflow.providers_manager import ProvidersManager
from airflow.configuration import conf
from airflow.executors.executor_constants import (
    LOCAL_KUBERNETES_EXECUTOR,
    CELERY_EXECUTOR,
    CELERY_KUBERNETES_EXECUTOR,
    KUBERNETES_EXECUTOR
)
if TYPE_CHECKING:
    from airflow.cli.cli_config import CLICommand

class CliDefinitionLoader:
    """CLI Definition Loader."""

    # for [kubernetes cleanup-pods CLI command only available when using KubernetesExecutor #46978]
    providers_cli_definitions = {
        "apache-airflow-providers-cncf-kubernetes": "airflow.providers.cncf.kubernetes.cli.definition",
        "apache-airflow-providers-amazon": "airflow.providers.amazon.aws.cli.definition",
    }

    # Further step, decuple the `get_cli_commands` method from `Executor` and `AuthManager`
    executors_cli_definitions = {
        LOCAL_KUBERNETES_EXECUTOR: "airflow.providers.cncf.kubernetes.cli.local_kubernetes_executor_cli_definition",
        CELERY_EXECUTOR: "airflow.providers.celery.cli.celery_executor_cli_definition",
        CELERY_KUBERNETES_EXECUTOR: "airflow.providers.celery.cli.celery_kubernetes_executor_cli_definition",
        KUBERNETES_EXECUTOR: "airflow.providers.cncf.kubernetes.cli.kubernetes_executor_cli_definition",
    }
    auth_manager_cli_definitions = {
        "SimpleAuthManager": "airflow.auth.managers.cli.simple_auth_manager_cli_definition",
        "AwsAuthManager": "airflow.providers.amazon.aws.auth.cli.aws_auth_manager_cli_definition",
    }

    def _load_provider_cli_definitions(self) -> list[CLICommand]:
        # base on ProvidersManager, load provider module level CLI definitions
        return []
    
    def _load_executor_cli_definitions(self) -> list[CLICommand]:
        # base on ExecutorLoader, load executor module level CLI definitions
        # Note: only get executor name from conf, not loading the whole executor module
        return []
    
    def _load_auth_manager_cli_definitions(self) -> list[CLICommand]:
        # load auth manager module level CLI definitions from conf
        return []

    @classmethod
    def get_cli_commands(self):
        """Get CLI commands."""
        return self._load_provider_cli_definitions() + self._load_executor_cli_definitions() + self._load_auth_manager_cli_definitions()