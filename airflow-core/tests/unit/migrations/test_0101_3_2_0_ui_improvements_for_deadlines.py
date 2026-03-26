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

import importlib
from datetime import datetime, timedelta, timezone

migration = importlib.import_module("airflow.migrations.versions.0101_3_2_0_ui_improvements_for_deadlines")


def _async_callback(path: str) -> dict:
    return {
        "__classname__": "airflow.sdk.definitions.callback.AsyncCallback",
        "__version__": 0,
        "__data__": {"path": path, "kwargs": {}},
    }


def _executor_callback_row(path: str) -> dict:
    return {"path": path, "kwargs": {}, "executor": "local", "prefix": "deadline_alerts", "dag_id": "example"}


def _triggerer_callback_row(path: str) -> dict:
    return {"path": path, "kwargs": {}, "prefix": "deadline_alerts", "dag_id": "example"}


def test_build_deadline_alert_assignments_matches_multiple_alerts_for_same_callback() -> None:
    logical_date = datetime(2026, 3, 1, tzinfo=timezone.utc)
    callback = _async_callback("pkg.callbacks.alert")
    callback_row = _triggerer_callback_row("pkg.callbacks.alert")

    deadline_rows = [
        {
            "deadline_id": "deadline-1",
            "dagrun_id": 42,
            "deadline_time": logical_date + timedelta(minutes=5),
            "logical_date": logical_date,
            "queued_at": None,
            "callback_type": "triggerer",
            "callback_data": callback_row,
        },
        {
            "deadline_id": "deadline-2",
            "dagrun_id": 42,
            "deadline_time": logical_date + timedelta(minutes=10),
            "logical_date": logical_date,
            "queued_at": None,
            "callback_type": "triggerer",
            "callback_data": callback_row,
        },
    ]
    migrated_alerts = [
        {
            "alert_id": "alert-1",
            migration.REFERENCE_KEY: {"reference_type": "DagRunLogicalDateDeadline"},
            migration.INTERVAL_KEY: 300.0,
            migration.CALLBACK_KEY: callback,
        },
        {
            "alert_id": "alert-2",
            migration.REFERENCE_KEY: {"reference_type": "DagRunLogicalDateDeadline"},
            migration.INTERVAL_KEY: 600.0,
            migration.CALLBACK_KEY: callback,
        },
    ]

    assignments, unmatched_rows, unmatched_alert_slots = migration._build_deadline_alert_assignments(
        deadline_rows, migrated_alerts
    )

    assert assignments == {"deadline-1": "alert-1", "deadline-2": "alert-2"}
    assert unmatched_rows == 0
    assert unmatched_alert_slots == 0


def test_build_deadline_alert_assignments_falls_back_for_non_deterministic_references() -> None:
    callback = {
        "__classname__": "airflow.sdk.definitions.callback.SyncCallback",
        "__version__": 0,
        "__data__": {"path": "pkg.callbacks.alert", "kwargs": {}, "executor": "local"},
    }
    callback_row = _executor_callback_row("pkg.callbacks.alert")
    logical_date = datetime(2026, 3, 1, tzinfo=timezone.utc)

    deadline_rows = [
        {
            "deadline_id": "deadline-1",
            "dagrun_id": 99,
            "deadline_time": logical_date + timedelta(minutes=3),
            "logical_date": logical_date,
            "queued_at": logical_date,
            "callback_type": "executor",
            "callback_data": callback_row,
        },
        {
            "deadline_id": "deadline-2",
            "dagrun_id": 99,
            "deadline_time": logical_date + timedelta(minutes=7),
            "logical_date": logical_date,
            "queued_at": logical_date,
            "callback_type": "executor",
            "callback_data": callback_row,
        },
    ]
    migrated_alerts = [
        {
            "alert_id": "alert-1",
            migration.REFERENCE_KEY: {
                "reference_type": "AverageRuntimeDeadline",
                "max_runs": 5,
                "min_runs": 3,
            },
            migration.INTERVAL_KEY: 60.0,
            migration.CALLBACK_KEY: callback,
        },
        {
            "alert_id": "alert-2",
            migration.REFERENCE_KEY: {
                "reference_type": "AverageRuntimeDeadline",
                "max_runs": 5,
                "min_runs": 3,
            },
            migration.INTERVAL_KEY: 120.0,
            migration.CALLBACK_KEY: callback,
        },
    ]

    assignments, unmatched_rows, unmatched_alert_slots = migration._build_deadline_alert_assignments(
        deadline_rows, migrated_alerts
    )

    assert assignments == {"deadline-1": "alert-1", "deadline-2": "alert-2"}
    assert unmatched_rows == 0
    assert unmatched_alert_slots == 0


def test_callback_key_from_alert_ignores_callback_runtime_metadata() -> None:
    serialized_callback = _async_callback("pkg.callbacks.alert")

    assert migration._callback_key_from_alert(serialized_callback) == migration._normalize_callback_key(
        "triggerer",
        _triggerer_callback_row("pkg.callbacks.alert"),
    )
