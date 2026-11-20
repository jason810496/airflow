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
"""E2E tests for a provider implemented in Java and used from a Python Dag and a Java Dag.

Run with::

    E2E_TEST_MODE=java_sdk uv run --project airflow-e2e-tests pytest \\
        tests/airflow_e2e_tests/java_sdk_tests/test_java_sdk_localfile.py -xvs

What is verified
----------------
``apache-airflow-providers-java-localfile`` is implemented in Java. Both Dags have the same chain::

    build_content -> create -> write -> has_file -> delete -> has_after_delete -> after_skip

and a ``write_variable`` task that has no upstream.

``java_localfile`` is the native Dag that ``airflow-e2e-tests/java-provider-bundle`` declares. Its tasks run
from that bundle's JAR, which holds the provider. ``python_java_localfile`` in
``java_localfile_example.py`` writes the chain with the generated Python wrappers. Every provider task sets
``queue="java-native"``, which ``[sdk] queue_to_coordinator`` routes to the ``java-jdk`` coordinator. No bundle
declares ``python_java_localfile``, so the coordinator runs the SDK's generic runtime from its
``java_providers_path``, where the SDK and the provider are installed. The runtime loads the class the task's
run context names from the provider's own directory, in a class loader of its own.

1. Both Dags run to success, and ``after_skip`` ends skipped: ``has_after_delete`` finds the file gone and
   skips its direct downstream task.
2. The operators return what their Java implementation says: ``create`` created the file, ``write``
   reports the size and SHA-256 of the file on disk, ``has_file`` finds it, ``delete`` removes it and
   ``has_after_delete`` does not find it.
3. The ``write`` task's ``path`` is rendered with the run id, and its ``content`` is the result of
   ``build_content``, pulled from its XCom and not rendered.
4. ``write_variable`` reads an Airflow Variable, which the test sets through the REST API first, through the
   SDK's client while it runs, and writes its value to a file.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient

_DAG_PARSE_TIMEOUT = 300
_RUN_TIMEOUT = 600

# Match `BuildContent` in java-provider-bundle and the Python Dag. It has non-ASCII characters, so its size
# in bytes differs from its length.
_CONTENT = "localfile report\nnaïve café, 3 orders\n"
_CONTENT_BYTES = _CONTENT.encode()
# What `write_variable` reads. The Dags name the Variable `java_localfile_greeting`.
_VARIABLE_KEY = "java_localfile_greeting"
_VARIABLE_VALUE = "greetings from the Java e2e, café"
_VARIABLE_BYTES = _VARIABLE_VALUE.encode()
_QUEUE = "java-native"
_PROVIDER_TASK_IDS = ("create", "write", "has_file", "delete", "has_after_delete", "write_variable")
_TASK_IDS = {
    "build_content",
    "create",
    "write",
    "has_file",
    "delete",
    "has_after_delete",
    "after_skip",
    "write_variable",
}


@dataclass
class _Run:
    client: AirflowClient
    dag_id: str
    run_id: str
    state: str
    task_instances: dict[str, dict]

    def xcom(self, task_id: str):
        return self.client.get_xcom_value(
            dag_id=self.dag_id, task_id=task_id, run_id=self.run_id, key="return_value"
        ).get("value")


def _run_dag(dag_id: str) -> _Run:
    client = AirflowClient()
    client.set_variable(_VARIABLE_KEY, _VARIABLE_VALUE)
    client.wait_for_dag(dag_id, timeout=_DAG_PARSE_TIMEOUT)
    run_id = client.trigger_dag(dag_id, json={"logical_date": datetime.now(UTC).isoformat()})["dag_run_id"]
    state = client.wait_for_dag_run(dag_id=dag_id, run_id=run_id, timeout=_RUN_TIMEOUT)
    resp = client.get_task_instances(dag_id=dag_id, run_id=run_id)
    task_instances = {ti["task_id"]: ti for ti in resp.get("task_instances", [])}
    return _Run(client, dag_id, run_id, state, task_instances)


@pytest.fixture(scope="module", params=["python_java_localfile", "java_localfile"])
def localfile_run(request: pytest.FixtureRequest) -> _Run:
    return _run_dag(request.param)


def _states(run: _Run) -> dict[str, str | None]:
    return {task_id: ti.get("state") for task_id, ti in run.task_instances.items()}


def test_tasks_succeed_and_the_one_after_a_missing_file_is_skipped(localfile_run: _Run):
    states = _states(localfile_run)
    assert set(states) == _TASK_IDS, states
    assert states == {**dict.fromkeys(_TASK_IDS, "success"), "after_skip": "skipped"}, states
    assert localfile_run.state == "success", states


def test_provider_tasks_run_on_the_java_queue(localfile_run: _Run):
    tis = localfile_run.task_instances
    for task_id in _PROVIDER_TASK_IDS:
        assert tis[task_id]["queue"] == _QUEUE, tis[task_id]


def test_operators_return_what_the_java_implementation_reports(localfile_run: _Run):
    path = localfile_run.task_instances["write"]["rendered_fields"]["path"]
    assert localfile_run.xcom("create") == {"path": path, "created": True}
    assert localfile_run.xcom("write") == {
        "path": path,
        "bytes": len(_CONTENT_BYTES),
        "sha256": hashlib.sha256(_CONTENT_BYTES).hexdigest(),
    }
    assert localfile_run.xcom("has_file") is True
    assert localfile_run.xcom("delete") == {"path": path, "deleted": True}
    assert localfile_run.xcom("has_after_delete") is False


def test_write_renders_the_run_id_and_writes_the_upstream_result(localfile_run: _Run):
    rendered = localfile_run.task_instances["write"]["rendered_fields"]
    assert rendered["path"] == (
        f"/tmp/airflow-localfile/{localfile_run.dag_id}/{localfile_run.run_id}/report.txt"
    ), rendered
    # The size and hash that `write` reports are those of the upstream result, which the runtime pulls.
    assert localfile_run.xcom("write")["sha256"] == hashlib.sha256(_CONTENT_BYTES).hexdigest()


def test_write_variable_writes_the_value_it_reads_from_airflow(localfile_run: _Run):
    rendered = localfile_run.task_instances["write_variable"]["rendered_fields"]
    path = f"/tmp/airflow-localfile/{localfile_run.dag_id}/{localfile_run.run_id}/variable.txt"
    assert rendered["path"] == path, rendered
    assert localfile_run.xcom("write_variable") == {
        "path": path,
        "bytes": len(_VARIABLE_BYTES),
        "sha256": hashlib.sha256(_VARIABLE_BYTES).hexdigest(),
    }
