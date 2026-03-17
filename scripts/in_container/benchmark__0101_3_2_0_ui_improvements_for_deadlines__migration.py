#!/usr/bin/env python3
#
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
Benchmark data generator for migration 0101_3_2_0_ui_improvements_for_deadlines.

Populates deadline, serialized_dag, and supporting tables with configurable row counts
to measure the full upgrade path from Airflow 3.1.8 schema to main.

Data is inserted in the **3.1.8 schema format** — the deadline table stores the callback
as inline JSON (no separate callback table), and the serialized_dag table contains
deadline alert definitions in its dag JSON.

Key migrations that process this data:
  0093 — Creates the callback table (renames callback_request → callback).
  0094 — Replaces deadline.callback (JSON) with deadline.callback_id FK + deadline.missed.
  0101 — Adds deadline.created_at / last_updated_at, creates deadline_alert table,
         migrates data from serialized_dag JSON.

Scenarios
---------
1. **Full upgrade benchmark** (recommended):
       Start from the 3.1.8 schema, run this script to populate data,
       then ``airflow db migrate`` to apply all migrations and measure timing.

2. **Single-migration benchmark** (0101 only):
       Run migrations through 0100, populate data (deadlines will already have
       callback_id/missed from 0094), then apply only migration 0101.

Run inside Breeze with PostgreSQL backend
-----------------------------------------
    breeze --backend postgres shell
    python /opt/airflow/scripts/in_container/\
benchmark__0101_3_2_0_ui_improvements_for_deadlines__migration.py

Environment variables
---------------------
    BENCHMARK_NUM_DAGS            Number of DAGs (default: 100000)
    BENCHMARK_RUNS_PER_DAG        Runs per DAG (default: 100)
    BENCHMARK_DEADLINES_PER_RUN   Deadlines per run (default: 1)
    BENCHMARK_ALERTS_PER_DAG      Deadline alerts per serialized DAG (default: 2)
    BENCHMARK_CLEANUP             Set to "1" to delete all benchmark data and exit

Default configuration produces:
    100,000 DAGs x 100 runs x 1 deadline = 10,000,000 deadline rows
    100,000 serialized_dag rows (with 2 deadline alerts each)
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone as dt_tz

import sqlalchemy as sa
import uuid6
from sqlalchemy import column, func, insert, select, table, text

from airflow import settings
from airflow.models.dag import DagModel
from airflow.models.dag_version import DagVersion
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagrun import DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.tasklog import LogTemplate
from airflow.utils.hashlib_wrapper import md5
from airflow.utils.session import NEW_SESSION, create_session, provide_session

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
NUM_DAGS = int(os.environ.get("BENCHMARK_NUM_DAGS", "100000"))
RUNS_PER_DAG = int(os.environ.get("BENCHMARK_RUNS_PER_DAG", "100"))
DEADLINES_PER_RUN = int(os.environ.get("BENCHMARK_DEADLINES_PER_RUN", "1"))
ALERTS_PER_DAG = int(os.environ.get("BENCHMARK_ALERTS_PER_DAG", "2"))

DAG_PREFIX = "benchmark_dag_"
RUN_PREFIX = "benchmark_run_"
BASE_DATE = datetime(2020, 1, 1, tzinfo=dt_tz.utc)
BENCHMARK_BUNDLE = "benchmark"

# ---------------------------------------------------------------------------
# 3.1.8 table definitions
# ---------------------------------------------------------------------------
# The deadline table in Airflow 3.1.8 stores the callback as inline JSON.
# There is no separate callback table. We define the table explicitly
# because the current ORM model on main has a completely different layout
# (callback_id FK, missed boolean, etc.).
deadline_table = table(
    "deadline",
    column("id", sa.Uuid()),
    column("dagrun_id", sa.Integer()),
    column("deadline_time", sa.DateTime(timezone=True)),
    column("callback", sa.JSON()),
    column("callback_state", sa.String(20)),
    column("trigger_id", sa.Integer()),
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _make_serialized_callback() -> dict:
    """Create a callback in the 3.1.8 serde format (airflow.serialization.serde)."""
    return {
        "__data__": {"path": "benchmark.callback_func", "kwargs": {}},
        "__classname__": "airflow.sdk.definitions.deadline.AsyncCallback",
        "__version__": 0,
    }


def _make_dag_data(dag_id: str) -> dict:
    """Create minimal serialized DAG data with deadline alerts embedded in the JSON."""
    deadline_alerts = []
    for i in range(ALERTS_PER_DAG):
        deadline_alerts.append(
            {
                "__type": "deadline_alert",
                "__var": {
                    "reference": {"reference_type": "DagRunLogicalDateDeadline"},
                    "interval": 3600.0 * (i + 1),
                    "callback": {
                        "__type": "async_callback",
                        "__var": {"path": "benchmark.callback_func", "kwargs": {}},
                    },
                },
            }
        )

    return {
        "dag": {
            "dag_id": dag_id,
            "fileloc": "/opt/airflow/dags/benchmark.py",
            "deadline": deadline_alerts,
            "tasks": [],
            "timetable": {"__type": "NullTimetable"},
        }
    }


def _compute_dag_hash(dag_data: dict) -> str:
    """Compute dag_hash consistent with SerializedDagModel.hash()."""
    import copy

    data_ = copy.deepcopy(dag_data)
    data_["dag"].pop("fileloc", None)
    data_json = json.dumps(data_, sort_keys=True).encode("utf-8")
    return md5(data_json).hexdigest()


# ---------------------------------------------------------------------------
# Phase functions
# ---------------------------------------------------------------------------
@provide_session
def _ensure_prerequisites(*, session=NEW_SESSION) -> int:
    """Ensure log_template and dag_bundle exist. Returns log_template_id."""
    log_template_id = session.scalar(select(func.max(LogTemplate.id)))
    if log_template_id is None:
        lt = LogTemplate(filename="benchmark.log", elasticsearch_id="benchmark")
        session.add(lt)
        session.flush()
        log_template_id = lt.id

    if not session.scalar(select(DagBundleModel).where(DagBundleModel.name == BENCHMARK_BUNDLE)):
        session.add(DagBundleModel(name=BENCHMARK_BUNDLE))

    return log_template_id


def _insert_dags_and_serialized_dags() -> None:
    """Insert DagModel, DagVersion, and SerializedDagModel rows."""
    print(f"\n[Phase 2] Inserting {NUM_DAGS:,} dags + dag_versions + serialized_dags...")
    t0 = time.monotonic()

    batch_size = 1000
    inserted = 0
    now = datetime(2024, 1, 1, tzinfo=dt_tz.utc)

    for batch_start in range(0, NUM_DAGS, batch_size):
        batch_end = min(batch_start + batch_size, NUM_DAGS)
        dag_rows: list[dict] = []
        dv_rows: list[dict] = []
        sd_rows: list[dict] = []

        for dag_idx in range(batch_start, batch_end):
            dag_id = f"{DAG_PREFIX}{dag_idx}"
            dv_id = uuid6.uuid7()
            sd_id = uuid6.uuid7()

            dag_data = _make_dag_data(dag_id)
            dag_hash = _compute_dag_hash(dag_data)

            # Only include columns that exist in the 3.1.8 schema.
            # Columns like timetable_type, fail_fast, exceeds_max_non_backfill
            # are added by later migrations (0092, 0099, 0100).
            dag_rows.append(
                {
                    "dag_id": dag_id,
                    "is_paused": False,
                    "is_stale": False,
                    "bundle_name": BENCHMARK_BUNDLE,
                    "max_active_tasks": 16,
                    "max_consecutive_failed_dag_runs": 0,
                    "has_task_concurrency_limits": False,
                }
            )

            dv_rows.append(
                {
                    "id": dv_id,
                    "dag_id": dag_id,
                    "version_number": 1,
                    "created_at": now,
                    "last_updated": now,
                }
            )

            # Column name in the SQL table is "data" (mapped as _data in ORM)
            sd_rows.append(
                {
                    "id": sd_id,
                    "dag_id": dag_id,
                    "data": dag_data,
                    "dag_hash": dag_hash,
                    "dag_version_id": dv_id,
                    "created_at": now,
                    "last_updated": now,
                }
            )

        with create_session() as session:
            session.execute(insert(DagModel.__table__), dag_rows)
            session.execute(insert(DagVersion.__table__), dv_rows)
            session.execute(insert(SerializedDagModel.__table__), sd_rows)

        inserted += batch_end - batch_start
        elapsed = time.monotonic() - t0
        if inserted % 10_000 == 0 or batch_end == NUM_DAGS:
            print(f"  {inserted:,}/{NUM_DAGS:,} ({elapsed:.1f}s)")

    print(f"  Done in {time.monotonic() - t0:.1f}s")


def _insert_dag_runs_and_deadlines(log_template_id: int) -> None:
    """Insert DagRun and Deadline rows in batches.

    For each batch of DAGs:
    1. Insert dag_run rows.
    2. Query back the auto-generated dag_run.id values.
    3. Insert deadline rows (with inline JSON callback) using those IDs.
    """
    total_runs = NUM_DAGS * RUNS_PER_DAG
    total_deadlines = total_runs * DEADLINES_PER_RUN
    print(f"\n[Phase 3] Inserting {total_runs:,} dag_runs + {total_deadlines:,} deadlines...")
    t0 = time.monotonic()

    # Keep each transaction around ~10K dag_runs
    dags_per_batch = max(1, 10_000 // RUNS_PER_DAG)
    inserted_runs = 0
    inserted_deadlines = 0
    serialized_callback = _make_serialized_callback()

    for batch_start in range(0, NUM_DAGS, dags_per_batch):
        batch_end = min(batch_start + dags_per_batch, NUM_DAGS)
        dag_ids_in_batch = [f"{DAG_PREFIX}{i}" for i in range(batch_start, batch_end)]

        # Build dag_run rows — only columns that exist in 3.1.8
        dr_rows: list[dict] = []
        for dag_idx in range(batch_start, batch_end):
            dag_id = f"{DAG_PREFIX}{dag_idx}"
            for run_idx in range(RUNS_PER_DAG):
                gs = dag_idx * RUNS_PER_DAG + run_idx
                logical_date = BASE_DATE + timedelta(seconds=gs)
                dr_rows.append(
                    {
                        "dag_id": dag_id,
                        "run_id": f"{RUN_PREFIX}{gs}",
                        "run_type": "manual",
                        "state": "success",
                        "logical_date": logical_date,
                        "run_after": logical_date,
                        "log_template_id": log_template_id,
                        "clear_number": 0,
                    }
                )

        with create_session() as session:
            # Insert dag_runs — the DB assigns auto-increment IDs
            session.execute(insert(DagRun.__table__), dr_rows)
            session.flush()

            # Query back the auto-generated dag_run.id for each run_id
            result = session.execute(
                select(DagRun.__table__.c.id, DagRun.__table__.c.run_id).where(
                    DagRun.__table__.c.dag_id.in_(dag_ids_in_batch)
                )
            )
            run_id_to_db_id = {row.run_id: row.id for row in result}

            # Build deadline rows using the 3.1.8 schema (inline JSON callback,
            # no separate callback table, no missed/callback_id columns).
            dl_rows: list[dict] = []
            for dag_idx in range(batch_start, batch_end):
                for run_idx in range(RUNS_PER_DAG):
                    gs = dag_idx * RUNS_PER_DAG + run_idx
                    run_id = f"{RUN_PREFIX}{gs}"
                    db_id = run_id_to_db_id[run_id]

                    for dl_idx in range(DEADLINES_PER_RUN):
                        dl_id = uuid6.uuid7()
                        deadline_time = BASE_DATE + timedelta(seconds=gs, hours=dl_idx + 1)

                        dl_rows.append(
                            {
                                "id": dl_id,
                                "dagrun_id": db_id,
                                "deadline_time": deadline_time,
                                "callback": serialized_callback,
                            }
                        )

            session.execute(insert(deadline_table), dl_rows)

        inserted_runs += len(dr_rows)
        inserted_deadlines += len(dl_rows)
        elapsed = time.monotonic() - t0

        batch_num = batch_start // dags_per_batch + 1
        if batch_num % 10 == 0 or batch_end == NUM_DAGS:
            rate = inserted_deadlines / elapsed if elapsed > 0 else 0
            print(
                f"  dag_runs: {inserted_runs:,}/{total_runs:,}, "
                f"deadlines: {inserted_deadlines:,}/{total_deadlines:,} "
                f"({elapsed:.1f}s, {rate:,.0f} dl/s)"
            )

    print(f"  Done in {time.monotonic() - t0:.1f}s")


def _vacuum_analyze() -> None:
    """VACUUM ANALYZE key tables to update planner statistics."""
    print("\n[Phase 4] Running VACUUM ANALYZE...")
    t0 = time.monotonic()
    # VACUUM cannot run inside a transaction block
    raw_conn = settings.engine.raw_connection()
    try:
        raw_conn.set_session(autocommit=True)
        cursor = raw_conn.cursor()
        for tbl in ("deadline", "dag_run", "serialized_dag"):
            print(f"  VACUUM ANALYZE {tbl}...")
            cursor.execute(f"VACUUM ANALYZE {tbl}")
        cursor.close()
    finally:
        raw_conn.close()
    print(f"  Done in {time.monotonic() - t0:.1f}s")


@provide_session
def _print_report(*, session=NEW_SESSION) -> None:
    """Print table sizes and row counts."""
    dl_count = session.scalar(text("SELECT count(*) FROM deadline"))
    dr_count = session.scalar(select(func.count()).select_from(DagRun.__table__))
    sd_count = session.scalar(select(func.count()).select_from(SerializedDagModel.__table__))

    dl_size = session.scalar(text("SELECT pg_size_pretty(pg_total_relation_size('deadline'))"))
    dr_size = session.scalar(text("SELECT pg_size_pretty(pg_total_relation_size('dag_run'))"))
    sd_size = session.scalar(text("SELECT pg_size_pretty(pg_total_relation_size('serialized_dag'))"))

    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"  deadline:          {dl_count:,} rows  ({dl_size})")
    print(f"  dag_run:           {dr_count:,} rows  ({dr_size})")
    print(f"  serialized_dag:    {sd_count:,} rows  ({sd_size})")
    print("=" * 60)


def _cleanup() -> None:
    """Delete all benchmark data in FK order."""
    print("Cleaning up benchmark data...")
    t0 = time.monotonic()

    # Delete deadlines in batches (10M rows is too many for a single DELETE).
    total_dl = 0
    batch_size = 50_000

    while True:
        with create_session() as session:
            rows = session.execute(
                select(deadline_table.c.id)
                .join(
                    DagRun.__table__,
                    deadline_table.c.dagrun_id == DagRun.__table__.c.id,
                )
                .where(DagRun.__table__.c.dag_id.like(f"{DAG_PREFIX}%"))
                .limit(batch_size)
            ).all()

            if not rows:
                break

            dl_ids = [r[0] for r in rows]
            session.execute(deadline_table.delete().where(deadline_table.c.id.in_(dl_ids)))

        total_dl += len(dl_ids)
        elapsed = time.monotonic() - t0
        print(f"  {total_dl:,} deadlines ({elapsed:.1f}s)")

    # Delete remaining supporting data
    with create_session() as session:
        dr_del = session.execute(
            DagRun.__table__.delete().where(DagRun.__table__.c.dag_id.like(f"{DAG_PREFIX}%"))
        ).rowcount
        sd_del = session.execute(
            SerializedDagModel.__table__.delete().where(
                SerializedDagModel.__table__.c.dag_id.like(f"{DAG_PREFIX}%")
            )
        ).rowcount
        dv_del = session.execute(
            DagVersion.__table__.delete().where(DagVersion.__table__.c.dag_id.like(f"{DAG_PREFIX}%"))
        ).rowcount
        dm_del = session.execute(
            DagModel.__table__.delete().where(DagModel.__table__.c.dag_id.like(f"{DAG_PREFIX}%"))
        ).rowcount
        session.execute(
            DagBundleModel.__table__.delete().where(DagBundleModel.__table__.c.name == BENCHMARK_BUNDLE)
        )

    total_elapsed = time.monotonic() - t0
    print(
        f"Deleted: {total_dl:,} deadlines, {dr_del:,} dag_runs, "
        f"{sd_del:,} serialized_dags, {dv_del:,} dag_versions, {dm_del:,} dag_models "
        f"({total_elapsed:.1f}s)"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    if os.environ.get("BENCHMARK_CLEANUP", "0") == "1":
        _cleanup()
        return 0

    total_runs = NUM_DAGS * RUNS_PER_DAG
    total_deadlines = total_runs * DEADLINES_PER_RUN

    print("=" * 60)
    print("Migration 0101 Benchmark Data Generator")
    print("=" * 60)
    print(f"  DAGs:                {NUM_DAGS:,}")
    print(f"  Runs/DAG:            {RUNS_PER_DAG:,}")
    print(f"  Deadlines/Run:       {DEADLINES_PER_RUN:,}")
    print(f"  Alerts/DAG (JSON):   {ALERTS_PER_DAG:,}")
    print(f"  Total dag_runs:      {total_runs:,}")
    print(f"  Total deadlines:     {total_deadlines:,}")
    print(f"  Total serialized:    {NUM_DAGS:,}")
    print("=" * 60)

    overall_start = time.monotonic()

    print("\n[Phase 1] Setting up prerequisites...")
    log_template_id = _ensure_prerequisites()
    print(f"  log_template id={log_template_id}")

    _insert_dags_and_serialized_dags()
    _insert_dag_runs_and_deadlines(log_template_id)
    _vacuum_analyze()
    _print_report()

    total_elapsed = time.monotonic() - overall_start
    print(f"\nTotal elapsed: {total_elapsed:.1f}s")
    print("\nData is ready. Run the migration to benchmark:")
    print("  airflow db migrate    # apply all migrations from 3.1.8 to main")

    return 0


if __name__ == "__main__":
    sys.exit(main())
