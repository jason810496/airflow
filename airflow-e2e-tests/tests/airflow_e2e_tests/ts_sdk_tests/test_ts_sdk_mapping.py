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
End-to-end test of mapped tasks whose bodies run in the TypeScript runtime.

Run with::

    E2E_TEST_MODE=ts_sdk uv run --project airflow-e2e-tests pytest \\
        tests/airflow_e2e_tests/ts_sdk_tests/test_ts_sdk_mapping.py -xvs

``typescript_mapping_example`` is declared in TypeScript (``ts-sdk/example/src/mapping.ts``) and
applies its own ``map()`` callables. ``typescript_mapping_stub_example`` is declared in Python
(``ts-sdk/example/dags/typescript_mapping_stub_example.py``) with TypeScript stub handlers, so
Airflow's Python sidecar runs its ``map()`` callables. In both, the sidecar resolves each mapped
instance's arguments with the Python task runner's own code.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient

_TS_TASK_TIMEOUT = 600


@dataclass
class _Run:
    client: AirflowClient
    dag_id: str
    run_id: str
    state: str
    instances: dict[str, dict[int, dict]]

    def values(self, task_id: str) -> list:
        """The return value of every instance of *task_id*, in map-index order."""
        return [
            self.client.get_xcom_value(
                dag_id=self.dag_id, task_id=task_id, run_id=self.run_id, key="return_value", map_index=index
            ).get("value")
            for index in sorted(self.instances[task_id])
            if self.instances[task_id][index]["state"] == "success"
        ]

    def value(self, task_id: str):
        return self.values(task_id)[0]

    def states(self, task_id: str) -> list[str]:
        return [self.instances[task_id][index]["state"] for index in sorted(self.instances[task_id])]


def _run(dag_id: str) -> _Run:
    client = AirflowClient()
    client.wait_for_dag(dag_id, timeout=_TS_TASK_TIMEOUT)
    client.un_pause_dag(dag_id)
    run_id = client.trigger_dag(dag_id, json={"logical_date": datetime.now(UTC).isoformat()})["dag_run_id"]
    state = client.wait_for_dag_run(dag_id=dag_id, run_id=run_id, timeout=_TS_TASK_TIMEOUT)
    instances: dict[str, dict[int, dict]] = defaultdict(dict)
    for ti in client.get_task_instances(dag_id=dag_id, run_id=run_id).get("task_instances", []):
        instances[ti["task_id"]][ti["map_index"]] = ti
    return _Run(client=client, dag_id=dag_id, run_id=run_id, state=state, instances=instances)


@pytest.fixture(scope="module")
def native_run() -> _Run:
    return _run("typescript_mapping_example")


@pytest.fixture(scope="module")
def stub_run() -> _Run:
    return _run("typescript_mapping_stub_example")


class TestDagDeclaredInTypeScript:
    def test_the_run_succeeds(self, native_run: _Run):
        assert native_run.state == "success"

    def test_expand_with_partial_and_map(self, native_run: _Run):
        assert native_run.values("measure") == [8, 9]
        labels = [native_run.instances["measure"][i]["rendered_map_index"] for i in (0, 1)]
        assert labels == ["a.csv", "bb.csv"]

    def test_a_map_callable_skips_its_instance(self, native_run: _Run):
        assert native_run.states("picky") == ["success", "success", "skipped"]
        assert native_run.values("picky") == ["a.csv", "bb.csv"]

    def test_a_cross_product_follows_python_order(self, native_run: _Run):
        assert native_run.values("grid") == ["a1/4", "b1/4", "a2/4", "b2/4"]

    def test_expanding_over_a_record_gives_its_pairs(self, native_run: _Run):
        assert native_run.values("tag") == ["env=prod", "team=data"]

    def test_zip_pads_the_shorter_source(self, native_run: _Run):
        assert native_run.values("pair") == ["a.csv:10", "bb.csv:null"]

    def test_expand_kwargs_keeps_the_handlers_defaults(self, native_run: _Run):
        assert native_run.values("sized") == [True, False]

    def test_expand_kwargs_over_map_spreads_each_element(self, native_run: _Run):
        assert native_run.values("locate") == ["raw/a.csv", "raw/bb.csv"]

    def test_a_mapped_task_is_expanded_over_again(self, native_run: _Run):
        assert native_run.values("audit") == [16, 18]

    def test_a_reduce_reads_every_instance(self, native_run: _Run):
        assert native_run.value("summarize") == {"count": 2, "total": 17, "last": 9}


class TestDagDeclaredInPython:
    def test_the_run_succeeds(self, stub_run: _Run):
        assert stub_run.state == "success"

    def test_the_sidecar_runs_the_python_map_callables(self, stub_run: _Run):
        assert stub_run.values("transform") == ["eu:uk", "eu:fr"]

    def test_a_reduce_reads_every_instance(self, stub_run: _Run):
        assert stub_run.value("report") == "eu:uk|eu:fr"

    def test_zip(self, stub_run: _Run):
        assert stub_run.values("pair") == ["uk-1", "fr-2"]

    def test_expand_kwargs_over_map_falls_back_to_the_stub_default(self, stub_run: _Run):
        assert stub_run.values("load") == ["raw/uk", "raw/fr"]

    def test_a_mapped_task_group_member_reads_its_own_row(self, stub_run: _Run):
        assert stub_run.values("per_region.first") == ["NORTH", "SOUTH"]
        assert stub_run.values("per_region.second") == ["NORTH-north-0", "SOUTH-south-1"]

    def test_a_reduce_over_a_mapped_task_group(self, stub_run: _Run):
        assert stub_run.value("report_regions") == ["NORTH-north-0", "SOUTH-south-1"]
