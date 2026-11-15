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
"""E2E tests for provider operators in a native TypeScript Dag.

Run with::

    E2E_TEST_MODE=ts_sdk uv run --project airflow-e2e-tests pytest \\
        tests/airflow_e2e_tests/ts_sdk_tests/test_ts_sdk_providers.py -xvs

What is verified
----------------
``ts-sdk/example/src/providers.ts`` declares ``ts_provider_operators`` with no Python Dag file::

    build_report -> create_bucket -> write_report -> wait_for_report -> summarize -> review -> publish

``build_report`` and ``publish`` run in Node. The other tasks are amazon, common.ai and standard operators,
declared through the generated ``@apache-airflow/providers-*`` packages: the SDK writes each one's Python class
and arguments into the serialized Dag, and a Python worker builds the operator from it.

1. The provider tasks run as their Python classes on the default queue, while the TypeScript tasks keep the
   Dag's ``typescript`` queue.
2. A templated argument given a TypeScript task's result is rendered from that task's XCom, and a Jinja
   template in another one is rendered by the worker, as in a Python Dag.
3. The S3 operators reach LocalStack: the sensor finds the object the operator wrote.
4. The common.ai operator runs pydantic-ai's offline ``test`` model on the report.
5. ``ApprovalOperator`` parks for a person, and the answer resumes the run.
6. ``publish``, back in Node, receives the summary and the answer the Python operators returned.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient

_DAG_ID = "ts_provider_operators"
_DAG_PARSE_TIMEOUT = 300
# Every container installs the in-tree common.ai provider before it starts, then the S3 tasks run.
_PARK_TIMEOUT = 900
_RESUME_TIMEOUT = 600

# Match `build_report` in ts-sdk/example/src/providers.ts.
_REPORT = json.dumps({"orders": 3, "total": 120}, separators=(",", ":"))
# What pydantic-ai's `test` model answers when it has no tools to call.
_SUMMARY = "success (no tool calls)"
_PROVIDER_TASKS = {
    "create_bucket": "S3CreateBucketOperator",
    "write_report": "S3CreateObjectOperator",
    "wait_for_report": "S3KeySensor",
    "summarize": "LLMOperator",
    "review": "ApprovalOperator",
}


def _states(task_instances: dict[str, dict]) -> dict[str, str | None]:
    return {task_id: ti.get("state") for task_id, ti in task_instances.items()}


def _get_task_instances(client: AirflowClient, run_id: str) -> dict[str, dict]:
    resp = client.get_task_instances(dag_id=_DAG_ID, run_id=run_id)
    return {ti["task_id"]: ti for ti in resp.get("task_instances", [])}


@dataclass
class _ParkedRun:
    client: AirflowClient
    run_id: str
    task_instances: dict[str, dict]
    hitl_detail: dict


@pytest.fixture(scope="module")
def parked_run() -> _ParkedRun:
    """Trigger the Dag once and wait for ``review`` to park."""
    client = AirflowClient()
    client.wait_for_dag(_DAG_ID, timeout=_DAG_PARSE_TIMEOUT)
    run_id = client.trigger_dag(_DAG_ID, json={"logical_date": datetime.now(UTC).isoformat()})["dag_run_id"]
    deadline = time.monotonic() + _PARK_TIMEOUT
    task_instances: dict[str, dict] = {}
    while time.monotonic() < deadline:
        task_instances = _get_task_instances(client, run_id)
        if task_instances.get("review", {}).get("state") == "awaiting_input":
            hitl_detail = client.get_hitl_detail(dag_id=_DAG_ID, run_id=run_id, task_id="review")
            return _ParkedRun(client, run_id, task_instances, hitl_detail)
        states = _states(task_instances)
        assert not {"failed", "upstream_failed"} & set(states.values()), f"a task failed: {states}"
        time.sleep(5)
    raise TimeoutError(f"'review' did not park within {_PARK_TIMEOUT}s: {_states(task_instances)}")


@dataclass
class _ApprovedRun:
    client: AirflowClient
    run_id: str
    state: str
    task_instances: dict[str, dict]

    def xcom(self, task_id: str):
        return self.client.get_xcom_value(
            dag_id=_DAG_ID, task_id=task_id, run_id=self.run_id, key="return_value"
        ).get("value")


@pytest.fixture(scope="module")
def approved_run(parked_run: _ParkedRun) -> _ApprovedRun:
    """Approve the summary and wait for the run to finish."""
    client = parked_run.client
    client.respond_to_hitl(
        dag_id=_DAG_ID, run_id=parked_run.run_id, task_id="review", chosen_options=["Approve"]
    )
    state = client.wait_for_dag_run(dag_id=_DAG_ID, run_id=parked_run.run_id, timeout=_RESUME_TIMEOUT)
    return _ApprovedRun(client, parked_run.run_id, state, _get_task_instances(client, parked_run.run_id))


def test_provider_tasks_run_as_their_python_class_on_a_python_queue(parked_run: _ParkedRun):
    tis = parked_run.task_instances
    for task_id, operator in _PROVIDER_TASKS.items():
        assert tis[task_id]["operator"] == operator, tis[task_id]
        assert tis[task_id]["queue"] == "default", tis[task_id]
    for task_id in ("build_report", "publish"):
        assert tis[task_id]["queue"] == "typescript", tis[task_id]


def test_s3_tasks_write_the_report_and_find_it(parked_run: _ParkedRun):
    tis = parked_run.task_instances
    for task_id in ("build_report", "create_bucket", "write_report", "wait_for_report"):
        assert tis[task_id]["state"] == "success", _states(tis)
    rendered = tis["write_report"]["rendered_fields"]
    # The TypeScript task's result, resolved from its XCom by the Python worker.
    assert rendered["data"] == _REPORT, rendered
    # A Jinja template, rendered by the Python worker.
    assert rendered["s3_key"] == f"runs/{parked_run.run_id}/report.json", rendered


def test_review_parks_with_the_summary(parked_run: _ParkedRun):
    assert parked_run.task_instances["summarize"]["state"] == "success"
    detail = parked_run.hitl_detail
    assert detail["subject"] == "Publish the report summary?"
    assert detail["options"] == ["Approve", "Reject"]
    assert detail["body"] == _SUMMARY


def test_the_run_succeeds_after_approval(approved_run: _ApprovedRun):
    assert approved_run.state == "success", _states(approved_run.task_instances)


def test_publish_receives_what_the_python_operators_returned(approved_run: _ApprovedRun):
    assert approved_run.xcom("summarize") == _SUMMARY
    assert approved_run.xcom("publish") == {"published": True, "summary": _SUMMARY}
