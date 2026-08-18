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
End-to-end test of the lang-SDK coordinators on KubernetesExecutor.

Triggers the ``lang_sdk_combined`` Dag (Python + Go + Java + TypeScript tasks in
one graph) and asserts every task instance and the Dag run reach ``success``. This
exercises the full path the worktree-1 feature enables: the
``golang``/``java``/``typescript`` queues are routed to their coordinators, each
coordinator's ``pod_template_file`` launches a worker pod whose init-container
stages the artifact from localstack S3 via the DagBundle interface, and the
coordinator then runs the Go binary / Java jar / Node bundle.

The TypeScript task additionally covers TaskFlow argument binding: the Dag calls
it with an upstream Python task's output and a literal, and the assertion on its
``return_value`` XCom is what proves both arguments reached the handler.

Prerequisites are provisioned by ``breeze k8s setup-lang-sdk-test``.
"""

from __future__ import annotations

import os
from typing import Any

import pytest

from kubernetes_tests.test_base import EXECUTOR, BaseK8STest

_RUN_LANG_SDK = os.environ.get("RUN_LANG_SDK_K8S_TESTS", "").lower() in ("true", "1")

DAG_ID = "lang_sdk_combined"
TASK_IDS = [
    "python_task_1",
    "go_extract",
    "go_transform",
    "java_extract",
    "java_transform",
    "ts_build_message",
    "python_task_2",
]
# Each task is a fresh pod (KubernetesExecutor) and the lang tasks also pull an
# artifact + start a coordinator subprocess, so allow generous headroom.
_TIMEOUT = 600

# What the Dag's ``ts_build_message(python_task_1(), "uk")`` call binds: the
# upstream task's return value and a literal.
_UPSTREAM_VALUE = "value_from_python_task_1"
_COUNTRY = "uk"


@pytest.mark.skipif(
    EXECUTOR != "KubernetesExecutor" or not _RUN_LANG_SDK,
    reason="Runs only on KubernetesExecutor with the lang-SDK env provisioned (RUN_LANG_SDK_K8S_TESTS)",
)
class TestLangSdkCoordinatorExecutor(BaseK8STest):
    def _ensure_variable(self, key: str, value: str) -> None:
        """Create the Airflow Variable the Go/Java transform tasks read (idempotent)."""
        resp = self.session.post(f"http://{self.host}/variables", json={"key": key, "value": value})
        # 409 == already exists from a previous run; both are acceptable.
        assert resp.status_code in (200, 201, 409), f"Could not create variable {key}: {resp.text}"

    def _fetch_xcom(self, dag_run_id: str, task_id: str, key: str = "return_value") -> Any:
        resp = self.session.get(
            f"http://{self.host}/dags/{DAG_ID}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{key}"
        )
        assert resp.status_code == 200, f"Could not read {key} XCom of {task_id}: {resp.text}"
        return resp.json()["value"]

    @pytest.mark.execution_timeout(900)
    def test_lang_sdk_combined_dag_succeeds(self):
        self._ensure_variable("my_variable", "value_from_test")

        dag_run_id, logical_date = self.start_job_in_kubernetes(DAG_ID, self.host)
        print(f"Triggered {DAG_ID} run {dag_run_id} (logical_date={logical_date})")

        for task_id in TASK_IDS:
            self.monitor_task(
                host=self.host,
                dag_run_id=dag_run_id,
                dag_id=DAG_ID,
                task_id=task_id,
                expected_final_state="success",
                timeout=_TIMEOUT,
            )

        self.ensure_dag_expected_state(
            host=self.host,
            logical_date=logical_date,
            dag_id=DAG_ID,
            expected_final_state="success",
            timeout=_TIMEOUT,
        )

        assert self._fetch_xcom(dag_run_id, "python_task_1") == _UPSTREAM_VALUE
        assert self._fetch_xcom(dag_run_id, "ts_build_message") == {
            "upstream": _UPSTREAM_VALUE,
            "country": _COUNTRY,
            "message": f"{_UPSTREAM_VALUE} ({_COUNTRY})",
        }
