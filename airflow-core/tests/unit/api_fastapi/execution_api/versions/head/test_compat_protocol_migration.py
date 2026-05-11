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
End-to-end test of the compat protocol's version migration mechanism.

The real ``StartupDetails`` schema cannot grow fake fields just for a
test, so this module spins up an isolated Cadwyn app with a synthetic
schema that mirrors ``StartupDetails`` shape and a synthetic version
bundle anchored at fake future dates (3026-04-17, 3026-06-16). The
mechanism is the same Cadwyn uses for the real bundle, so what this
proves about the synthetic schema applies to the real one.

The promise the compat protocol makes to a foreign-language SDK runtime
is: "tell me the schema version you were built against, and I'll send
you a body shaped for that version". The tests below pin that promise
by registering a chain of ``VersionChange`` entries that each introduce
an incompatible field, then asserting that a client requesting each
date receives exactly the fields that existed on or before that date.
"""

from __future__ import annotations

from typing import Literal

import pytest
from cadwyn import (
    Cadwyn,
    HeadVersion,
    Version,
    VersionBundle,
    VersionChange,
    VersionedAPIRouter,
    schema,
)
from fastapi.testclient import TestClient
from pydantic import BaseModel


class StartupDetails(BaseModel):
    """
    A synthetic mirror of ``airflow.sdk.execution_time.comms.StartupDetails``.

    Carries fields that simulate breaking changes added at fake future
    versions. The real ``StartupDetails`` cannot grow these fields just
    for this test, but the Cadwyn migration mechanism is class-agnostic
    so what the test proves here applies equally to the real schema.
    """

    type: Literal["StartupDetails"] = "StartupDetails"
    ti_id: str
    # Field introduced at fake version 3026-04-17.
    queue_capacity: int | None = None
    # Fields introduced at fake version 3026-06-16.
    sentry_trace_id: str | None = None
    sentry_baggage: str | None = None


class IntroduceQueueCapacityField(VersionChange):
    """3026-04-17: introduce ``queue_capacity`` on ``StartupDetails``."""

    description = __doc__
    instructions_to_migrate_to_previous_version = (
        schema(StartupDetails).field("queue_capacity").didnt_exist,
    )


class IntroduceSentryTraceFields(VersionChange):
    """3026-06-16: introduce ``sentry_trace_id`` and ``sentry_baggage`` on ``StartupDetails``."""

    description = __doc__
    instructions_to_migrate_to_previous_version = (
        schema(StartupDetails).field("sentry_trace_id").didnt_exist,
        schema(StartupDetails).field("sentry_baggage").didnt_exist,
    )


# Fake-future bundle. Cadwyn orders newest-first; the baseline version
# anchors the bottom of the migration chain (no further migrations apply
# below it).
BUNDLE = VersionBundle(
    HeadVersion(),
    Version("3026-06-16", IntroduceSentryTraceFields),
    Version("3026-04-17", IntroduceQueueCapacityField),
    Version("3025-01-01"),
)


@pytest.fixture
def compat_protocol_client():
    """An isolated Cadwyn app that exposes a single compat echo route
    over the synthetic ``StartupDetails`` and the fake-future bundle."""

    router = VersionedAPIRouter()

    @router.post("/compat/startup-details")
    def echo_startup_details(body: StartupDetails) -> StartupDetails:
        return body

    app = Cadwyn(
        title="Compat Protocol Migration Test",
        versions=BUNDLE,
        api_version_parameter_name="Airflow-API-Version",
        api_version_default_value=BUNDLE.versions[0].value,
    )
    app.generate_and_include_versioned_routers(router)
    return TestClient(app)


def _full_payload() -> dict:
    """A body that carries every field introduced across the bundle."""
    return {
        "type": "StartupDetails",
        "ti_id": "t1",
        "queue_capacity": 8,
        "sentry_trace_id": "00-trace-span-00",
        "sentry_baggage": "sentry-trace_id=abc",
    }


class TestStartupDetailsCompatProtocolMigration:
    """
    Verify that each registered schema version sees only the fields
    that existed at or before its date.
    """

    def test_head_carries_every_field(self, compat_protocol_client):
        # Default version is the latest registered (3026-06-16). The
        # head request must therefore see every field across the chain.
        resp = compat_protocol_client.post("/compat/startup-details", json=_full_payload())

        assert resp.status_code == 200, resp.text
        echoed = resp.json()
        assert echoed["ti_id"] == "t1"
        assert echoed["queue_capacity"] == 8
        assert echoed["sentry_trace_id"] == "00-trace-span-00"
        assert echoed["sentry_baggage"] == "sentry-trace_id=abc"

    def test_version_3026_06_16_carries_sentry_and_queue_capacity_fields(self, compat_protocol_client):
        # 3026-06-16 introduced sentry_trace_id/sentry_baggage; the
        # earlier queue_capacity (from 3026-04-17) is still in the
        # response because it existed at this version.
        resp = compat_protocol_client.post(
            "/compat/startup-details",
            json=_full_payload(),
            headers={"Airflow-API-Version": "3026-06-16"},
        )

        assert resp.status_code == 200, resp.text
        echoed = resp.json()
        assert echoed["queue_capacity"] == 8
        assert echoed["sentry_trace_id"] == "00-trace-span-00"
        assert echoed["sentry_baggage"] == "sentry-trace_id=abc"

    def test_version_3026_04_17_strips_sentry_fields_added_later(self, compat_protocol_client):
        # 3026-04-17 introduced queue_capacity; sentry_trace_id and
        # sentry_baggage were added later (3026-06-16) and must be
        # stripped from the response so a runtime built against this
        # earlier schema does not encounter unknown fields.
        resp = compat_protocol_client.post(
            "/compat/startup-details",
            json=_full_payload(),
            headers={"Airflow-API-Version": "3026-04-17"},
        )

        assert resp.status_code == 200, resp.text
        echoed = resp.json()
        assert echoed["ti_id"] == "t1"
        assert echoed["queue_capacity"] == 8
        assert "sentry_trace_id" not in echoed
        assert "sentry_baggage" not in echoed

    def test_baseline_version_strips_every_later_field(self, compat_protocol_client):
        # The baseline (3025-01-01) predates every breaking change. A
        # runtime targeting it must see the schema as it existed before
        # either later version added anything.
        resp = compat_protocol_client.post(
            "/compat/startup-details",
            json=_full_payload(),
            headers={"Airflow-API-Version": "3025-01-01"},
        )

        assert resp.status_code == 200, resp.text
        echoed = resp.json()
        assert echoed["ti_id"] == "t1"
        assert "queue_capacity" not in echoed
        assert "sentry_trace_id" not in echoed
        assert "sentry_baggage" not in echoed

    @pytest.mark.parametrize(
        ("api_version", "expected_present", "expected_absent"),
        [
            pytest.param(
                "3026-06-16",
                {"queue_capacity", "sentry_trace_id", "sentry_baggage"},
                set(),
                id="latest-keeps-all",
            ),
            pytest.param(
                "3026-04-17",
                {"queue_capacity"},
                {"sentry_trace_id", "sentry_baggage"},
                id="middle-keeps-earlier-only",
            ),
            pytest.param(
                "3025-01-01",
                set(),
                {"queue_capacity", "sentry_trace_id", "sentry_baggage"},
                id="baseline-keeps-none",
            ),
        ],
    )
    def test_each_version_receives_its_own_schema(
        self, compat_protocol_client, api_version, expected_present, expected_absent
    ):
        # Same property as the dedicated tests above, restated as a
        # parameterized truth table so a future migration regression
        # (e.g. someone reordering the bundle, or attaching a migration
        # to the wrong version) shows up as an obvious table mismatch.
        resp = compat_protocol_client.post(
            "/compat/startup-details",
            json=_full_payload(),
            headers={"Airflow-API-Version": api_version},
        )
        assert resp.status_code == 200, resp.text
        echoed = resp.json()
        for field in expected_present:
            assert field in echoed, f"{field!r} expected at version {api_version}"
        for field in expected_absent:
            assert field not in echoed, f"{field!r} must be stripped at version {api_version}"


class TestMigratedStartupDetailsAtEachVersion:
    """
    The task-sdk-side counterpart: when ``client.compat.migrate_startup_details``
    requests a target version, the returned ``MigratedStartupDetails``
    must carry exactly the fields that existed at that version. This pins
    the contract from the language-runtime's point of view: whatever
    Cadwyn produced on the server, the client wraps verbatim and hands
    to the runtime.
    """

    @pytest.mark.parametrize(
        ("target_version", "expected_present", "expected_absent"),
        [
            pytest.param(
                "3026-06-16",
                {"queue_capacity", "sentry_trace_id", "sentry_baggage"},
                set(),
                id="latest-version",
            ),
            pytest.param(
                "3026-04-17",
                {"queue_capacity"},
                {"sentry_trace_id", "sentry_baggage"},
                id="middle-version",
            ),
            pytest.param(
                "3025-01-01",
                set(),
                {"queue_capacity", "sentry_trace_id", "sentry_baggage"},
                id="baseline-version",
            ),
        ],
    )
    def test_client_compatible_wraps_version_specific_body(
        self, compat_protocol_client, target_version, expected_present, expected_absent
    ):
        # We don't reuse the real task-sdk Client here because its
        # request wrapper has wire concerns (retries, content-type
        # injection) orthogonal to the migration we want to pin. The
        # contract the language-runtime cares about is: server returns
        # a body shaped for ``target_version``, and that body is what
        # ends up wrapped in ``MigratedStartupDetails`` on the
        # caller side. So we hit the endpoint directly and wrap the
        # response the same way the production client does.
        from airflow.sdk.execution_time.coordinator import MigratedStartupDetails

        resp = compat_protocol_client.post(
            "/compat/startup-details",
            json=_full_payload(),
            headers={"Airflow-API-Version": target_version},
        )
        assert resp.status_code == 200, resp.text

        wrapped = MigratedStartupDetails(resp.json())

        assert isinstance(wrapped, MigratedStartupDetails)
        assert isinstance(wrapped, dict)
        for field in expected_present:
            assert field in wrapped, f"{field!r} missing at {target_version}"
        for field in expected_absent:
            assert field not in wrapped, f"{field!r} leaked at {target_version}"
