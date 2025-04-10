#
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

from importlib import reload
from unittest import mock

import pytest

from airflow.cli import cli_parser
from airflow.cli.commands import scheduler_command
from airflow.executors import executor_loader
from airflow.providers.fab.www.serve_logs import serve_logs
from airflow.utils.scheduler_health import serve_health_check

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test


class TestSchedulerCommand:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    @pytest.mark.parametrize(
        "executor, expect_serve_logs",
        [
            ("CeleryExecutor", False),
            ("LocalExecutor", True),
            ("KubernetesExecutor", False),
        ],
    )
    @mock.patch("airflow.cli.commands.scheduler_command.SchedulerJobRunner")
    @mock.patch("airflow.cli.commands.scheduler_command.Process")
    def test_serve_logs_on_scheduler(self, mock_process, mock_scheduler_job, executor, expect_serve_logs):
        mock_scheduler_job.return_value.job_type = "SchedulerJob"
        args = self.parser.parse_args(["scheduler"])

        with conf_vars({("core", "executor"): executor}):
            reload(executor_loader)
            scheduler_command.scheduler(args)
            if expect_serve_logs:
                mock_process.assert_has_calls([mock.call(target=serve_logs)])
            else:
                with pytest.raises(AssertionError):
                    mock_process.assert_has_calls([mock.call(target=serve_logs)])

    @mock.patch("airflow.cli.commands.scheduler_command.SchedulerJobRunner")
    @mock.patch("airflow.cli.commands.scheduler_command.Process")
    @pytest.mark.parametrize("executor", ["LocalExecutor"])
    def test_skip_serve_logs(self, mock_process, mock_scheduler_job, executor):
        mock_scheduler_job.return_value.job_type = "SchedulerJob"
        args = self.parser.parse_args(["scheduler", "--skip-serve-logs"])
        with conf_vars({("core", "executor"): executor}):
            reload(executor_loader)
            scheduler_command.scheduler(args)
            with pytest.raises(AssertionError):
                mock_process.assert_has_calls([mock.call(target=serve_logs)])

    @mock.patch("airflow.utils.db.check_and_run_migrations")
    @mock.patch("airflow.utils.db.synchronize_log_template")
    @mock.patch("airflow.cli.commands.scheduler_command.SchedulerJobRunner")
    @mock.patch("airflow.cli.commands.scheduler_command.Process")
    def test_check_migrations_is_false(self, mock_process, mock_scheduler_job, mock_log, mock_run_migration):
        mock_scheduler_job.return_value.job_type = "SchedulerJob"
        args = self.parser.parse_args(["scheduler"])
        with conf_vars({("database", "check_migrations"): "False"}):
            scheduler_command.scheduler(args)
            mock_run_migration.assert_not_called()
            mock_log.assert_called_once()

    @mock.patch("airflow.utils.db.check_and_run_migrations")
    @mock.patch("airflow.utils.db.synchronize_log_template")
    @mock.patch("airflow.cli.commands.scheduler_command.SchedulerJobRunner")
    @mock.patch("airflow.cli.commands.scheduler_command.Process")
    def test_check_migrations_is_true(self, mock_process, mock_scheduler_job, mock_log, mock_run_migration):
        mock_scheduler_job.return_value.job_type = "SchedulerJob"
        args = self.parser.parse_args(["scheduler"])
        with conf_vars({("database", "check_migrations"): "True"}):
            scheduler_command.scheduler(args)
            mock_run_migration.assert_called_once()
            mock_log.assert_called_once()

    @mock.patch("airflow.cli.commands.scheduler_command.SchedulerJobRunner")
    @mock.patch("airflow.cli.commands.scheduler_command.Process")
    @pytest.mark.parametrize("executor", ["LocalExecutor"])
    def test_graceful_shutdown(self, mock_process, mock_scheduler_job, executor):
        mock_scheduler_job.return_value.job_type = "SchedulerJob"
        args = self.parser.parse_args(["scheduler"])
        with conf_vars({("core", "executor"): executor}):
            reload(executor_loader)
            mock_scheduler_job.run.side_effect = Exception("Mock exception to trigger runtime error")
            try:
                scheduler_command.scheduler(args)
            finally:
                mock_process().terminate.assert_called()

    @mock.patch("airflow.cli.commands.scheduler_command.SchedulerJobRunner")
    @mock.patch("airflow.cli.commands.scheduler_command.Process")
    def test_enable_scheduler_health(self, mock_process, mock_scheduler_job):
        with conf_vars({("scheduler", "enable_health_check"): "True"}):
            mock_scheduler_job.return_value.job_type = "SchedulerJob"
            args = self.parser.parse_args(["scheduler"])
            scheduler_command.scheduler(args)
            mock_process.assert_has_calls([mock.call(target=serve_health_check)])

    @mock.patch("airflow.cli.commands.scheduler_command.SchedulerJobRunner")
    @mock.patch("airflow.cli.commands.scheduler_command.Process")
    def test_disable_scheduler_health(self, mock_process, mock_scheduler_job):
        mock_scheduler_job.return_value.job_type = "SchedulerJob"
        args = self.parser.parse_args(["scheduler"])
        scheduler_command.scheduler(args)
        with pytest.raises(AssertionError):
            mock_process.assert_has_calls([mock.call(target=serve_health_check)])

    @mock.patch("airflow.utils.scheduler_health.HTTPServer")
    def test_scheduler_health_host(
        self,
        http_server_mock,
    ):
        health_check_host = "192.168.0.0"
        health_check_port = 1111
        with conf_vars(
            {
                ("scheduler", "SCHEDULER_HEALTH_CHECK_SERVER_HOST"): health_check_host,
                ("scheduler", "SCHEDULER_HEALTH_CHECK_SERVER_PORT"): str(health_check_port),
            }
        ):
            serve_health_check()
        assert http_server_mock.call_args.args[0] == (health_check_host, health_check_port)

    @mock.patch("airflow.cli.commands.scheduler_command.SchedulerJobRunner")
    @mock.patch("airflow.cli.commands.scheduler_command.Process")
    @mock.patch(
        "airflow.cli.commands.scheduler_command.run_job",
        side_effect=Exception("run_job failed"),
    )
    def test_run_job_exception_handling(self, mock_run_job, mock_process, mock_scheduler_job):
        args = self.parser.parse_args(["scheduler"])
        with pytest.raises(Exception, match="run_job failed"):
            scheduler_command.scheduler(args)

        # Make sure that run_job is called, that the exception has been logged, and that the serve_logs
        # sub-process has been terminated
        mock_run_job.assert_called_once_with(
            job=mock_scheduler_job().job,
            execute_callable=mock_scheduler_job()._execute,
        )
        mock_process.assert_called_once_with(target=serve_logs)
        mock_process().terminate.assert_called_once_with()
