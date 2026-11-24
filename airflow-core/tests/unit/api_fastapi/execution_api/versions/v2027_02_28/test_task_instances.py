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

from datetime import timedelta

import pytest

from airflow.sdk import task
from airflow.utils.state import State

from tests_common.test_utils.db import clear_db_runs

pytestmark = pytest.mark.db_test

RUN_PATCH_BODY = {
    "state": "running",
    "hostname": "h",
    "unixname": "u",
    "pid": 1,
    "start_date": "2024-09-30T12:00:00Z",
}


@pytest.fixture
def old_ver_client(client):
    """Execution API version immediately before ``execution_timeout`` was added."""
    client.headers["Airflow-API-Version"] = "2026-10-30"
    return client


@pytest.mark.parametrize("version", ["2026-10-30", "2027-02-28"])
def test_execution_timeout_schema_by_version(client, version):
    response = client.get(f"/execution/openapi.json?version={version}")
    assert response.status_code == 200
    context = response.json()["components"]["schemas"]["TIRunContext"]["properties"]
    assert ("execution_timeout" in context) == (version == "2027-02-28")


class TestExecutionTimeoutBackwardCompat:
    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    @pytest.fixture
    def stub_ti(self, dag_maker):
        with dag_maker("test_execution_timeout_compat_dag", serialized=True):

            @task.stub(execution_timeout=timedelta(seconds=90))
            def sleeper(): ...

            sleeper()

        (ti,) = dag_maker.create_dagrun().get_task_instances()
        ti.set_state(State.QUEUED)
        dag_maker.session.flush()
        return ti

    def test_old_version_strips_execution_timeout(self, old_ver_client, stub_ti):
        response = old_ver_client.patch(f"/execution/task-instances/{stub_ti.id}/run", json=RUN_PATCH_BODY)
        assert response.status_code == 200
        assert "execution_timeout" not in response.json()

    def test_head_version_includes_execution_timeout(self, client, stub_ti):
        response = client.patch(f"/execution/task-instances/{stub_ti.id}/run", json=RUN_PATCH_BODY)
        assert response.status_code == 200
        assert response.json()["execution_timeout"] == 90.0


class TestMappedStubArgBindingsBackwardCompat:
    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    @pytest.mark.parametrize("version", ["2026-10-30", "2027-02-28"])
    def test_mapped_binding_kinds_by_version(self, client, version):
        response = client.get(f"/execution/openapi.json?version={version}")
        assert response.status_code == 200
        assert ("ParamArgBinding" in response.json()["components"]["schemas"]) == (version == "2027-02-28")
        context = response.json()["components"]["schemas"]["TIRunContext"]["properties"]
        assert ("needs_mapped_length" in context) == (version == "2027-02-28")

    def test_old_version_keeps_ignoring_a_mapped_stubs_arguments(self, old_ver_client, dag_maker):
        with dag_maker("test_mapped_stub_compat", serialized=True):

            @task.stub
            def transform(country: str): ...

            transform.expand(country=["uk", "fr"])

        ti = next(t for t in dag_maker.create_dagrun().get_task_instances() if t.map_index == 0)
        ti.set_state(State.QUEUED)
        dag_maker.session.flush()

        response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)
        assert response.status_code == 200
        assert "arg_bindings" not in response.json()
        assert "needs_mapped_length" not in response.json()

    def test_old_version_cannot_run_a_stub_reading_a_mapped_upstream(self, old_ver_client, dag_maker):
        with dag_maker("test_reduce_stub_compat", serialized=True):

            @task.stub
            def produce(n: int): ...

            @task.stub
            def report(values: list): ...

            report(produce.expand(n=[1, 2]))

        ti = next(t for t in dag_maker.create_dagrun().get_task_instances() if t.task_id == "report")
        ti.set_state(State.QUEUED)
        dag_maker.session.flush()

        response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)
        assert response.status_code == 422
        assert response.json()["detail"]["reason"] == "arg_bindings_need_newer_client"
