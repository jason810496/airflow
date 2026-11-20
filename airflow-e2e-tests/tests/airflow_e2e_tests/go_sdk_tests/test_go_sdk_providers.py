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
"""E2E tests for provider operators in a native Go Dag.

Run with::

    E2E_TEST_MODE=go_sdk uv run --project airflow-e2e-tests pytest \\
        tests/airflow_e2e_tests/go_sdk_tests/test_go_sdk_providers.py -xvs

What is verified
----------------
``go-sdk/example/providers`` declares ``go_provider_operators`` with no Python Dag file::

    build_report -> create_bucket -> write_report -> wait_for_report -> run_bash -> summarize

``build_report`` and ``summarize`` run in Go. The other tasks are amazon and standard operators, declared
through the generated ``go-sdk/providers/amazon`` and ``go-sdk/providers/standard`` packages: the SDK writes
each one's Python class and arguments into the serialized Dag, and a Python worker builds the operator from it.

1. The provider tasks run as their Python classes on the default queue, while the Go tasks keep the Dag's
   ``golang`` queue.
2. A templated argument given a Go task's result is resolved from that task's XCom, and a Jinja template in
   another one is rendered by the worker, as in a Python Dag.
3. The S3 operators reach LocalStack: the sensor finds the object the operator wrote.
4. ``summarize``, back in Go, receives the line the Bash operator printed.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient

_DAG_ID = "go_provider_operators"
_DAG_PARSE_TIMEOUT = 600
# Every container installs the in-tree go.localfile provider before it starts, then the S3 tasks run.
_RUN_TIMEOUT = 900

# Match `buildReport` in go-sdk/example/providers/main.go.
_REPORT = json.dumps({"orders": 3, "total": 120}, separators=(",", ":"))
_PROVIDER_TASKS = {
    "create_bucket": "S3CreateBucketOperator",
    "write_report": "S3CreateObjectOperator",
    "wait_for_report": "S3KeySensor",
    "run_bash": "BashOperator",
}
_GO_TASKS = ("build_report", "summarize")


@dataclass
class _Run:
    client: AirflowClient
    run_id: str
    state: str
    task_instances: dict[str, dict]

    def xcom(self, task_id: str):
        return self.client.get_xcom_value(
            dag_id=_DAG_ID, task_id=task_id, run_id=self.run_id, key="return_value"
        ).get("value")


def _states(task_instances: dict[str, dict]) -> dict[str, str | None]:
    return {task_id: ti.get("state") for task_id, ti in task_instances.items()}


@pytest.fixture(scope="module")
def run() -> _Run:
    """Trigger the Dag once and wait for it to finish."""
    client = AirflowClient()
    client.wait_for_dag(_DAG_ID, timeout=_DAG_PARSE_TIMEOUT)
    run_id = client.trigger_dag(_DAG_ID, json={"logical_date": datetime.now(UTC).isoformat()})["dag_run_id"]
    state = client.wait_for_dag_run(dag_id=_DAG_ID, run_id=run_id, timeout=_RUN_TIMEOUT)
    resp = client.get_task_instances(dag_id=_DAG_ID, run_id=run_id)
    task_instances = {ti["task_id"]: ti for ti in resp.get("task_instances", [])}
    return _Run(client, run_id, state, task_instances)


def test_every_task_succeeds(run: _Run):
    states = _states(run.task_instances)
    assert set(states) == {*_PROVIDER_TASKS, *_GO_TASKS}, states
    assert set(states.values()) == {"success"}, states
    assert run.state == "success", states


def test_provider_tasks_run_as_their_python_class_on_a_python_queue(run: _Run):
    tis = run.task_instances
    for task_id, operator in _PROVIDER_TASKS.items():
        assert tis[task_id]["operator"] == operator, tis[task_id]
        assert tis[task_id]["queue"] == "default", tis[task_id]
    for task_id in _GO_TASKS:
        assert tis[task_id]["queue"] == "golang", tis[task_id]


def test_s3_tasks_write_the_report_and_find_it(run: _Run):
    rendered = run.task_instances["write_report"]["rendered_fields"]
    # The Go task's result, resolved from its XCom by the Python worker.
    assert rendered["data"] == _REPORT, rendered
    # A Jinja template, rendered by the Python worker.
    assert rendered["s3_key"] == f"runs/{run.run_id}/report.json", rendered
    assert run.xcom("build_report") == _REPORT


def test_summarize_receives_what_the_bash_operator_printed(run: _Run):
    printed = f"the report of run {run.run_id} is stored"
    assert run.xcom("run_bash") == printed
    assert run.xcom("summarize") == f"bash printed: {printed}"
