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

from uuid import uuid4

import pytest

TIMESTAMP_STR = "2024-12-01T01:00:00Z"


def _startup_details_body() -> dict:
    return {
        "type": "StartupDetails",
        "ti": {
            "id": str(uuid4()),
            "dag_version_id": str(uuid4()),
            "task_id": "t1",
            "dag_id": "d1",
            "run_id": "r1",
            "try_number": 1,
            "pool_slots": 1,
            "queue": "default",
            "priority_weight": 1,
        },
        "dag_rel_path": "dags/example.jar",
        "bundle_info": {"name": "my-bundle", "version": "v1"},
        "start_date": TIMESTAMP_STR,
        "ti_context": {
            "dag_run": {
                "dag_id": "d1",
                "run_id": "r1",
                "logical_date": TIMESTAMP_STR,
                "data_interval_start": TIMESTAMP_STR,
                "data_interval_end": TIMESTAMP_STR,
                "run_after": TIMESTAMP_STR,
                "start_date": TIMESTAMP_STR,
                "clear_number": 0,
                "run_type": "manual",
                "state": "running",
                "conf": None,
                "consumed_asset_events": [],
            },
            "task_reschedule_count": 0,
            "max_tries": 0,
            "should_retry": False,
        },
        "sentry_integration": "",
    }


def _dag_file_parse_request_body() -> dict:
    return {
        "type": "DagFileParseRequest",
        "file": "/bundles/my-bundle/dags/example.jar",
        "bundle_path": "/bundles/my-bundle",
        "bundle_name": "my-bundle",
    }


class TestCompatEndpointAvailableFromIntroductionVersion:
    """
    The compat endpoint is introduced at version 2026-06-16 by
    ``IntroduceCompatEndpoints``. A foreign runtime built against that
    exact version must be able to call it and receive a body shaped for
    its schema.
    """

    @pytest.fixture
    def introduction_version_client(self, client):
        client.headers["Airflow-API-Version"] = "2026-06-16"
        return client

    def test_startup_details_works_at_introduction_version(self, introduction_version_client):
        response = introduction_version_client.post(
            "/execution/compat/startup-details", json=_startup_details_body()
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["type"] == "StartupDetails"
        # The DagRun shape at 2026-06-16 includes team_name (added in
        # 2026-04-17) -- this proves the migration chain is wired up,
        # since requesting an even older version would strip it.
        assert "team_name" in body["ti_context"]["dag_run"]

    def test_dag_file_parse_request_works_at_introduction_version(self, introduction_version_client):
        response = introduction_version_client.post(
            "/execution/compat/dag-file-parse-request", json=_dag_file_parse_request_body()
        )
        assert response.status_code == 200, response.text
        assert response.json()["bundle_name"] == "my-bundle"


class TestCompatEndpointAbsentBeforeIntroductionVersion:
    """
    Versions strictly older than ``IntroduceCompatEndpoints`` (2026-06-16)
    must respond as if the endpoint does not exist. This is what foreign
    runtimes built before the compat surface existed will see, and the
    supervisor needs a clean 404 to detect "the server you're talking to
    does not speak the compat protocol".
    """

    @pytest.fixture
    def predates_compat_client(self, client):
        # The version immediately before IntroduceCompatEndpoints was added.
        client.headers["Airflow-API-Version"] = "2026-04-17"
        return client

    @pytest.mark.parametrize(
        ("path", "make_body"),
        [
            ("/execution/compat/startup-details", _startup_details_body),
            ("/execution/compat/dag-file-parse-request", _dag_file_parse_request_body),
        ],
    )
    def test_endpoint_returns_404_before_introduction(self, predates_compat_client, path, make_body):
        response = predates_compat_client.post(path, json=make_body())
        assert response.status_code == 404
