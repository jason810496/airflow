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

TIMESTAMP_STR = "2024-12-01T01:00:00Z"


def _startup_details_body() -> dict:
    """Build a minimal valid StartupDetails wire body."""
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
    """Build a minimal valid DagFileParseRequestCompat wire body."""
    return {
        "type": "DagFileParseRequest",
        "file": "/bundles/my-bundle/dags/example.jar",
        "bundle_path": "/bundles/my-bundle",
        "bundle_name": "my-bundle",
    }


class TestCompatStartupDetailsHead:
    """The compat startup-details echo endpoint at the HEAD schema version."""

    def test_echoes_body_at_head(self, client):
        body = _startup_details_body()
        response = client.post("/execution/compat/startup-details", json=body)

        assert response.status_code == 200, response.text
        echoed = response.json()
        # HEAD-shaped echo carries every field we sent, including the
        # ``team_name`` field added in version 2026-04-17.
        assert echoed["type"] == "StartupDetails"
        assert echoed["dag_rel_path"] == body["dag_rel_path"]
        assert echoed["bundle_info"] == body["bundle_info"]
        assert echoed["ti"]["task_id"] == body["ti"]["task_id"]
        assert echoed["ti_context"]["dag_run"]["dag_id"] == body["ti_context"]["dag_run"]["dag_id"]
        # ``team_name`` is part of the HEAD-shaped DagRun (added by AddTeamNameField).
        assert "team_name" in echoed["ti_context"]["dag_run"]

    def test_rejects_invalid_body(self, client):
        # Missing required ``ti_context`` field; the route must reject rather
        # than echoing arbitrary input back to a runtime that cannot parse it.
        body = _startup_details_body()
        del body["ti_context"]
        response = client.post("/execution/compat/startup-details", json=body)

        assert response.status_code == 422

    def test_rejects_get(self, client):
        # The compat surface is POST-only -- a GET must not silently echo a
        # cached body.
        response = client.get("/execution/compat/startup-details")
        assert response.status_code == 405


class TestCompatDagFileParseRequestHead:
    """The compat dag-file-parse-request echo endpoint at the HEAD schema version."""

    def test_echoes_body_at_head(self, client):
        body = _dag_file_parse_request_body()
        response = client.post("/execution/compat/dag-file-parse-request", json=body)

        assert response.status_code == 200, response.text
        echoed = response.json()
        assert echoed["type"] == "DagFileParseRequest"
        assert echoed["file"] == body["file"]
        assert echoed["bundle_name"] == body["bundle_name"]
        # The slim compat shape does not carry the Python-only ``callback_requests``
        # field; foreign runtimes never see it.
        assert "callback_requests" not in echoed

    def test_strips_callback_requests_on_input(self, client):
        # If a (misbehaving) client sends ``callback_requests`` anyway, the
        # slim compat schema must ignore it rather than crash, and must not
        # leak it back into the response.
        body = _dag_file_parse_request_body()
        body["callback_requests"] = [{"type": "task_callback"}]
        response = client.post("/execution/compat/dag-file-parse-request", json=body)

        assert response.status_code == 200, response.text
        assert "callback_requests" not in response.json()

    def test_rejects_invalid_body(self, client):
        body = _dag_file_parse_request_body()
        del body["bundle_name"]
        response = client.post("/execution/compat/dag-file-parse-request", json=body)

        assert response.status_code == 422
