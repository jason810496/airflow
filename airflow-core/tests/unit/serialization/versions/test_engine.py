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

from typing import Any
from unittest import mock

import pytest

from airflow.serialization import versions
from airflow.serialization.serialized_objects import DagSerialization
from airflow.serialization.versions import SerializedDagChange, Version


def _make_change(name: str, *, requires_migration: bool = False) -> type[SerializedDagChange]:
    def upgrade(self, data: dict[str, Any]) -> None:
        data["log"].append(f"up:{name}")

    def downgrade(self, data: dict[str, Any]) -> None:
        data["log"].append(f"down:{name}")

    return type(
        name,
        (SerializedDagChange,),
        {
            "description": name,
            "requires_migration": requires_migration,
            "upgrade": upgrade,
            "downgrade": downgrade,
        },
    )


# Mirrors the real layout: integers, then dates that share stamp 3, then a date stamp.
FAKE_VERSIONS = (
    Version(id=1, changes=(), airflow_version="1.10.7"),
    Version(id=2, changes=(_make_change("A"),), airflow_version="3.0.0"),
    Version(id=3, changes=(_make_change("B"), _make_change("C")), airflow_version="3.1.0"),
    Version(
        id="2025-01-01",
        changes=(_make_change("D"), _make_change("M", requires_migration=True)),
        airflow_version="3.2.0",
        stamp=3,
    ),
    Version(id="2025-06-01", changes=(_make_change("E"),), airflow_version="3.3.0", stamp=3),
    Version(id="2026-01-01", changes=(_make_change("F"),), airflow_version="3.5.0"),
)


@pytest.fixture
def fake_versions():
    with (
        mock.patch.object(versions, "VERSIONS", FAKE_VERSIONS),
        mock.patch.object(versions, "HEAD", FAKE_VERSIONS[-1]),
    ):
        yield


@pytest.mark.usefixtures("fake_versions")
class TestUpgrade:
    @pytest.mark.parametrize(
        ("stamp", "expected"),
        [
            # A v1 blob runs every later change, not only the first step.
            pytest.param(1, ["up:A", "up:B", "up:C", "up:D", "up:E", "up:F"], id="v1"),
            pytest.param(2, ["up:B", "up:C", "up:D", "up:E", "up:F"], id="v2"),
            # A shared stamp is read as the oldest version holding it.
            pytest.param(3, ["up:D", "up:E", "up:F"], id="shared-stamp"),
            pytest.param("2026-01-01", [], id="head"),
        ],
    )
    def test_runs_every_later_change_and_stamps_head(self, stamp, expected):
        data = {"__version": stamp, "log": []}

        assert versions.upgrade(data) is data
        assert data == {"__version": "2026-01-01", "log": expected}

    @pytest.mark.parametrize("stamp", [4, "2025-01-01", "2024-01-01", True, None])
    def test_rejects_unknown_stamp(self, stamp):
        with pytest.raises(ValueError, match="Unsure how to deserialize version"):
            versions.upgrade({"__version": stamp, "log": []})

    def test_rejects_missing_stamp(self):
        with pytest.raises(ValueError, match="'<not present>'"):
            versions.upgrade({"log": []})


@pytest.mark.usefixtures("fake_versions")
class TestDowngrade:
    @pytest.mark.parametrize(
        ("stamp", "to", "expected_stamp", "expected"),
        [
            # A shared stamp is read as the newest version holding it, so every change runs.
            pytest.param(3, 2, 2, ["down:E", "down:D", "down:C", "down:B"], id="shared-stamp-to-2"),
            pytest.param(
                "2026-01-01", 2, 2, ["down:F", "down:E", "down:D", "down:C", "down:B"], id="head-to-2"
            ),
            pytest.param("2026-01-01", "2025-06-01", 3, ["down:F"], id="head-to-date"),
            pytest.param("2026-01-01", 3, 3, ["down:F", "down:E", "down:D"], id="head-to-3"),
            pytest.param(3, "2025-06-01", 3, [], id="shared-stamp-at-target"),
            pytest.param(2, 3, 2, [], id="already-below-target"),
        ],
    )
    def test_runs_changes_down_to_target(self, stamp, to, expected_stamp, expected):
        data = {"__version": stamp, "log": []}

        assert versions.downgrade(data, to=to) is data
        assert data == {"__version": expected_stamp, "log": expected}

    def test_rejects_target_below_2(self):
        with pytest.raises(ValueError, match="below version 2"):
            versions.downgrade({"__version": 3, "log": []}, to=1)

    @pytest.mark.parametrize("to", [4, "2025-02-02", True])
    def test_rejects_unknown_target(self, to):
        with pytest.raises(ValueError, match="Unknown serialized Dag version"):
            versions.downgrade({"__version": 3, "log": []}, to=to)

    def test_rejects_unknown_stamp(self):
        with pytest.raises(ValueError, match="Unsure how to downgrade"):
            versions.downgrade({"__version": 7, "log": []}, to=2)


@pytest.mark.parametrize(
    ("airflow_version", "expected"),
    [
        ("2.10.5", 1),
        ("3.0.0", 2),
        ("3.0.6", 2),
        ("3.1.0", 3),
        ("3.1.1", "2025-10-27"),
        ("3.1.8", "2025-11-05"),
        ("3.2.0", "2026-04-06"),
        ("3.3.0", "2026-06-30"),
        ("3.4.0", "2026-10-30"),
        ("3.5.0.dev0", "2027-02-28"),
        ("3.5.0rc1", "2027-02-28"),
    ],
)
def test_get_version_for_airflow(airflow_version, expected):
    assert versions.get_version_for_airflow(airflow_version).id == expected


def test_get_version_for_airflow_rejects_release_before_serialized_dags():
    with pytest.raises(ValueError, match="predates serialized Dags"):
        versions.get_version_for_airflow("1.10.6")


def test_versions_up_to_3_4_write_stamp_3():
    # Cores up to 3.4 accept only 1, 2 and 3.
    stamps = {version.id: version.stamp for version in versions.VERSIONS}

    assert [stamp for version_id, stamp in stamps.items() if version_id != versions.HEAD.id] == [
        1,
        2,
        3,
        3,
        3,
        3,
        3,
        3,
    ]
    assert versions.HEAD.stamp == versions.HEAD.id == "2027-02-28"
    assert versions.HEAD.stamp == DagSerialization.SERIALIZER_VERSION


def test_lossy_downgrade_warns_once_per_change_and_dag(caplog):
    data = {
        "__version": 3,
        "dag": {
            "dag_id": "lossy",
            "tasks": [
                {"__type": "operator", "__var": {"task_id": "a", "returns_dag_result": True}},
                {"__type": "operator", "__var": {"task_id": "b", "returns_dag_result": True}},
            ],
        },
    }

    versions.downgrade(data, to="2026-06-30")

    warnings = [
        record
        for record in caplog.entries
        if record["event"] == "Serialized Dag downgrade dropped data"
        and record["change"] == "AddReturnsDagResult"
    ]
    assert warnings == [mock.ANY]
    assert warnings[0]["dag_id"] == "lossy"
    assert warnings[0]["log_level"] == "warning"


def test_downgrade_without_loss_does_not_warn(caplog):
    data = {"__version": 3, "dag": {"dag_id": "plain", "tasks": []}}

    versions.downgrade(data, to="2026-06-30")

    assert "Serialized Dag downgrade dropped data" not in caplog
