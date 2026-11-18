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
from unittest import mock

import pytest

from airflow.models.dagbag import DBDagBag
from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, task
from airflow.serialization.enums import Encoding
from airflow.serialization.serialized_objects import DagSerialization
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


@pytest.mark.parametrize("version", ["2026-10-30", "2027-02-28"])
def test_provider_operator_schema_by_version(client, version):
    response = client.get(f"/execution/openapi.json?version={version}")
    assert response.status_code == 200
    context = response.json()["components"]["schemas"]["TIRunContext"]["properties"]
    assert ("provider_operator" in context) == (version == "2027-02-28")


@pytest.mark.parametrize("version", ["2026-10-30", "2027-02-28"])
def test_stub_kind_schema_by_version(client, version):
    response = client.get(f"/execution/openapi.json?version={version}")
    assert response.status_code == 200
    schemas = response.json()["components"]["schemas"]
    assert ("stub_kind" in schemas["TaskInstance"]["properties"]) == (version == "2027-02-28")
    assert ("StubKind" in schemas) == (version == "2027-02-28")


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


class TestProviderOperatorBackwardCompat:
    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    @pytest.fixture
    def provider_operator_ti(self, dag_maker):
        with dag_maker("test_provider_operator_compat_dag", serialized=True):
            EmptyOperator(task_id="notify")

        (ti,) = dag_maker.create_dagrun().get_task_instances()
        ti.set_state(State.QUEUED)
        dag_maker.session.get(TaskInstance, ti.id).is_stub = True
        dag_maker.session.commit()
        return ti

    @pytest.fixture
    def provider_operator_dag(self):
        with DAG("test_provider_operator_compat_dag", schedule=None) as dag:
            EmptyOperator(task_id="notify")
        data = DagSerialization.to_dict(dag)
        (notify,) = (task[Encoding.VAR] for task in data["dag"]["tasks"])
        notify["_operator_kwargs"] = [{"name": "retries", "kind": "literal", "value": 2}]
        with mock.patch.object(DBDagBag, "get_dag", autospec=True) as get_dag:
            get_dag.return_value = DagSerialization.from_dict(data)
            yield

    @pytest.mark.usefixtures("provider_operator_dag")
    def test_old_version_strips_provider_operator_without_reading_it(
        self, old_ver_client, provider_operator_ti
    ):
        with mock.patch(
            "airflow.api_fastapi.execution_api.routes.task_instances.get_provider_operator", autospec=True
        ) as get_provider_operator:
            response = old_ver_client.patch(
                f"/execution/task-instances/{provider_operator_ti.id}/run", json=RUN_PATCH_BODY
            )

        assert response.status_code == 200
        assert "provider_operator" not in response.json()
        get_provider_operator.assert_not_called()

    @pytest.mark.usefixtures("provider_operator_dag")
    def test_head_version_includes_provider_operator(self, client, provider_operator_ti):
        response = client.patch(
            f"/execution/task-instances/{provider_operator_ti.id}/run", json=RUN_PATCH_BODY
        )

        assert response.status_code == 200
        assert response.json()["provider_operator"] == {
            "classpath": "airflow.providers.standard.operators.empty.EmptyOperator",
            "kwargs": [{"name": "retries", "kind": "literal", "value": 2}],
            "downstream_task_ids": {"notify": []},
        }
