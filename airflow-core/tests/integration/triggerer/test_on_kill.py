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

import logging
import os
import subprocess
import time

import pytest

from airflow.utils.state import State

from tests_common.test_utils.api_client_helpers import make_authenticated_rest_api_request
from tests_common.test_utils.integration_setup import (
    serialize_and_get_dags,
    start_scheduler,
    start_triggerer,
    terminate_process,
    unpause_trigger_dag_and_get_run_id,
    wait_for_ti_state,
)

log = logging.getLogger("integration.triggerer.test_on_kill")


@pytest.mark.backend("postgres")
class TestTriggererOnKillIntegration:
    """
    End-to-end coverage for ``BaseTrigger.on_kill()``.

    Runs a real Scheduler, API server, and Triggerer, defers a task instance, kills it via
    the public REST API (mimicking a user's mark-failed action), and asserts the Triggerer
    invoked ``on_kill()`` on the running trigger instance.
    """

    test_dir = os.path.dirname(os.path.abspath(__file__))
    dag_folder = os.path.join(test_dir, "dags")

    @classmethod
    def setup_class(cls):
        # The pytest plugin strips AIRFLOW__*__* env vars (including the JWT secret set
        # by Breeze). The scheduler, api-server, and triggerer subprocesses must share the
        # same secret; otherwise each generates its own random key and token verification fails.
        os.environ["AIRFLOW__API_AUTH__JWT_SECRET"] = "test-secret-key-for-testing"
        os.environ["AIRFLOW__API_AUTH__JWT_ISSUER"] = "airflow"

        # Lets the test's REST call skip the auth-token dance. This is scoped to this test's
        # own process-isolated api-server, torn down at test end, so it's not load-bearing for
        # anything else.
        os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS"] = "True"

        # Shrink from the 30s default so a failing test doesn't spend it waiting on the timeout.
        os.environ["AIRFLOW__TRIGGERER__ON_KILL_TIMEOUT"] = "5"

        os.environ["AIRFLOW__SCHEDULER__STANDALONE_DAG_PROCESSOR"] = "False"
        os.environ["AIRFLOW__SCHEDULER__PROCESSOR_POLL_INTERVAL"] = "2"

        os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = f"{cls.dag_folder}"
        os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
        os.environ["AIRFLOW__CORE__PLUGINS_FOLDER"] = "/dev/null"
        os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "False"

        # Reset the DB once at the beginning and serialize the dags.
        reset_command = ["airflow", "db", "reset", "--yes"]
        subprocess.run(reset_command, check=True, env=os.environ.copy())

        migrate_command = ["airflow", "db", "migrate"]
        subprocess.run(migrate_command, check=True, env=os.environ.copy())

        serialize_and_get_dags(dag_folder=cls.dag_folder)

    @pytest.mark.execution_timeout(150)
    def test_on_kill_invoked_when_deferred_task_marked_failed(self, tmp_path):
        dag_id = "on_kill_test_dag"
        task_id = "defer_task"
        marker_path = tmp_path / "on_kill_fired"

        scheduler_process = None
        apiserver_process = None
        triggerer_process = None
        try:
            scheduler_process, apiserver_process = start_scheduler()
            triggerer_process = start_triggerer()

            run_id = unpause_trigger_dag_and_get_run_id(dag_id=dag_id, conf={"marker_path": str(marker_path)})

            ti_state = wait_for_ti_state(
                dag_id=dag_id, run_id=run_id, task_id=task_id, states=[State.DEFERRED], max_wait_time=60
            )
            assert ti_state == State.DEFERRED, (
                f"Task instance never reached the deferred state, ended up as: {ti_state}."
            )

            # Wait for the triggerer to actually claim and start running the trigger's asyncio
            # task before killing it, otherwise the REST PATCH below could race the cancellation
            # against a trigger that isn't registered in the triggerer's in-memory dict yet, in
            # which case on_kill() would never be invoked for that cancellation attempt.
            started_marker = f"{marker_path}.started"
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline and not os.path.exists(started_marker):
                time.sleep(1)
            assert os.path.exists(started_marker), "The Triggerer never started running the trigger."

            make_authenticated_rest_api_request(
                path=f"/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}",
                method="PATCH",
                body={"new_state": "failed"},
            )

            deadline = time.monotonic() + 30
            while time.monotonic() < deadline and not marker_path.exists():
                time.sleep(1)
            assert marker_path.exists(), "on_kill() was never invoked by the Triggerer."

            ti_state = wait_for_ti_state(
                dag_id=dag_id, run_id=run_id, task_id=task_id, states=[State.FAILED], max_wait_time=30
            )
            assert ti_state == State.FAILED, (
                f"Task instance never reached the failed state, ended up as: {ti_state}."
            )
        finally:
            if triggerer_process is not None:
                terminate_process(triggerer_process)
                triggerer_status = triggerer_process.poll()
                assert triggerer_status is not None, (
                    "The triggerer process status is None, which means that it hasn't terminated as expected."
                )

            if scheduler_process is not None:
                terminate_process(scheduler_process)
                scheduler_status = scheduler_process.poll()
                assert scheduler_status is not None, (
                    "The scheduler process status is None, which means that it hasn't terminated as expected."
                )

            if apiserver_process is not None:
                terminate_process(apiserver_process)
                apiserver_status = apiserver_process.poll()
                assert apiserver_status is not None, (
                    "The apiserver process status is None, which means that it hasn't terminated as expected."
                )
