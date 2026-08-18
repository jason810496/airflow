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
"""End-to-end tests for coordinator-mode TypeScript tasks.

Run with::

    E2E_TEST_MODE=ts_sdk uv run --project airflow-e2e-tests pytest \\
        tests/airflow_e2e_tests/ts_sdk_tests/ -xvs

One ``airflow-ts-pack`` bundle, built by ``conftest._setup_ts_sdk_integration``,
serves two Dags; each is triggered once by a module-scoped fixture and every
test inspects that single run.

``typescript_example`` (``completed_run``) mixes a Python task with
``@task.stub`` TypeScript tasks and confirms end-to-end that ``NodeCoordinator``
launches the bundle on the volume-provided Node runtime, that Variable and
Connection reads and Python <-> TypeScript XCom round-trips work through the
Task Execution API, and that coordinator-channel logs reach the task-log store.

``typescript_taskflow_example`` (``taskflow_run``) exercises TaskFlow argument
binding on its own, with several arguments per task: ``summarize`` types them
flat and ``report`` declares them on one interface, together covering
XCom-backed and literal bindings, an argument left at its stub default, and an
``int64`` literal.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient

# Coordinator startup only needs to launch node with the prebuilt bundle;
# allow room for scheduling and the Python upstream task.
_TS_TASK_TIMEOUT = 600
# Task logs are written when the task finishes; allow a little slack for them
# to become retrievable through the API after the run reaches a terminal state.
_LOG_FETCH_TIMEOUT = 120

_DAG_ID = "typescript_example"
_TASKFLOW_DAG_ID = "typescript_taskflow_example"


@dataclass
class _CompletedRun:
    client: AirflowClient
    dag_id: str
    run_id: str
    state: str
    ti_states: dict[str, str]

    def xcom(self, task_id: str, key: str = "return_value"):
        return self.client.get_xcom_value(
            dag_id=self.dag_id, task_id=task_id, run_id=self.run_id, key=key
        ).get("value")

    def logs(self, task_id: str, try_number: int = 1) -> str:
        """Fetch task logs, retrying until present (log upload is async)."""
        deadline = time.monotonic() + _LOG_FETCH_TIMEOUT
        while True:
            resp = self.client.get_task_logs(
                dag_id=self.dag_id, run_id=self.run_id, task_id=task_id, try_number=try_number
            )
            text = "\n".join(str(entry) for entry in resp.get("content", []))
            if text.strip() or time.monotonic() > deadline:
                return text
            time.sleep(3)


def _trigger_and_wait(dag_id: str) -> _CompletedRun:
    client = AirflowClient()
    resp = client.trigger_dag(dag_id, json={"logical_date": datetime.now(timezone.utc).isoformat()})
    run_id = resp["dag_run_id"]
    state = client.wait_for_dag_run(dag_id=dag_id, run_id=run_id, timeout=_TS_TASK_TIMEOUT)
    ti_resp = client.get_task_instances(dag_id=dag_id, run_id=run_id)
    ti_states = {ti["task_id"]: ti.get("state") for ti in ti_resp.get("task_instances", [])}
    return _CompletedRun(client=client, dag_id=dag_id, run_id=run_id, state=state, ti_states=ti_states)


@pytest.fixture(scope="module")
def completed_run() -> _CompletedRun:
    """Trigger ``typescript_example`` once; every test inspects the same run."""
    return _trigger_and_wait(_DAG_ID)


@pytest.fixture(scope="module")
def taskflow_run() -> _CompletedRun:
    """Trigger ``typescript_taskflow_example`` once, as ``completed_run`` does.

    Its own Dag and so its own run: it exercises argument binding on its own,
    with more arguments per task than the mixed example carries and one handler
    in each TypeScript authoring style.
    """
    return _trigger_and_wait(_TASKFLOW_DAG_ID)


def test_dag_run_succeeded(completed_run: _CompletedRun):
    assert completed_run.state == "success", (
        f"expected the run to succeed; got {completed_run.state!r}. task states: {completed_run.ti_states}"
    )


def test_task_states(completed_run: _CompletedRun):
    expected = {
        "python_start": "success",
        "build_message": "success",
        "read_message": "success",
        "read_connection": "success",
    }
    for task_id, want in expected.items():
        assert completed_run.ti_states.get(task_id) == want, (
            f"{task_id!r} expected {want!r}. all task states: {completed_run.ti_states}"
        )


_MESSAGE = "greetings from e2e (uk); upstream=hello from Python"


def test_build_message_binds_the_dag_call_arguments(completed_run: _CompletedRun):
    """``build_message`` combines the arguments the Dag's TaskFlow call bound —
    ``python_start``'s XCom and a literal — with the Variable, pushes the result
    under ``typescript_message``, and returns it."""
    assert completed_run.xcom("python_start") == "hello from Python"

    value = completed_run.xcom("build_message")
    assert value == {"message": _MESSAGE, "country": "uk"}, (
        f"unexpected 'build_message' return_value: {value!r}"
    )
    assert completed_run.xcom("build_message", key="typescript_message") == _MESSAGE


def test_read_message_pulls_a_custom_xcom_key(completed_run: _CompletedRun):
    """The escape hatch: a key no call argument can name is read via the client."""
    value = completed_run.xcom("read_message")
    assert value == {"message": _MESSAGE}, f"unexpected 'read_message' return_value: {value!r}"


def test_read_connection_xcom(completed_run: _CompletedRun):
    value = completed_run.xcom("read_connection")
    assert value == {
        "id": "typescript_example_http",
        "type": "http",
        "host": "example.com",
        "login": "user",
        "hasPassword": True,
    }, f"unexpected 'read_connection' return_value: {value!r}"


def test_coordinator_logs_reach_task_log_store(completed_run: _CompletedRun):
    assert "[ts-sdk.runtime] Coordinator runtime started" in completed_run.logs("build_message")


def test_taskflow_dag_run_succeeded(taskflow_run: _CompletedRun):
    assert taskflow_run.state == "success", (
        f"expected the run to succeed; got {taskflow_run.state!r}. task states: {taskflow_run.ti_states}"
    )


def test_taskflow_task_states(taskflow_run: _CompletedRun):
    expected = {
        "make_region": "success",
        "make_totals": "success",
        "summarize": "success",
        "report": "success",
    }
    for task_id, want in expected.items():
        assert taskflow_run.ti_states.get(task_id) == want, (
            f"{task_id!r} expected {want!r}. all task states: {taskflow_run.ti_states}"
        )


def test_summarize_binds_flat_annotated_arguments(taskflow_run: _CompletedRun):
    """``summarize`` types its arguments flat, inline in the parameter.

    Its four bindings cover both kinds at once: ``region`` and ``totals`` are
    pulled from upstream XComs, ``currency`` travels as a literal, and the Dag
    call omits ``dry_run`` so it arrives from the stub's default. Every value
    below is derived from one of them, so a mis-bound argument cannot pass.
    """
    assert taskflow_run.xcom("make_region") == {"code": "uk", "name": "United Kingdom"}
    assert taskflow_run.xcom("make_totals") == {"orders": 12, "revenue": 3400.5}

    value = taskflow_run.xcom("summarize")
    assert value == {
        "region": "uk",
        "orders": 12,
        # 3400.5 / 12, rounded — proves the numbers survived as numbers.
        "averageOrder": 283.38,
        "currency": "GBP",
        "dryRun": False,
    }, f"unexpected 'summarize' return_value: {value!r}"

    # dry_run bound False from the default, so the guarded push happened.
    assert taskflow_run.xcom("summarize", key="summary_line") == "United Kingdom: 12 orders"


def test_report_binds_interface_declared_arguments(taskflow_run: _CompletedRun):
    """``report`` declares its five arguments on one interface.

    ``summary`` is an upstream XCom, the rest are literals. ``matchesSummary``
    compares the XCom-backed argument against the ``region_code`` literal, so it
    only holds if both bound correctly; ``retriesUsed`` is an ``int64`` literal
    that must survive without precision loss.
    """
    value = taskflow_run.xcom("report")
    assert value == {
        "label": "nightly",
        "regionCode": "uk",
        "matchesSummary": True,
        "healthy": True,
        "retriesUsed": 3,
        "taskId": "report",
    }, f"unexpected 'report' return_value: {value!r}"
