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
to measure migration timing for the deadline UI improvements migration.

The migration has three performance-critical phases:
1. UPDATE deadline SET created_at=now, last_updated_at=now — holds row-level locks
   on all deadline rows for the duration of the update (~7 min at 10M rows).
2. ALTER COLUMN created_at/last_updated_at SET NOT NULL — acquires ACCESS EXCLUSIVE lock
   on the deadline table, blocking all reads and writes.
3. Migrate deadline alert data from serialized_dag JSON into the new deadline_alert table.

Scenarios
---------
1. **Pre-migration benchmark** (recommended):
       Downgrade to revision e79fc784f145 (the revision before 0101), run this script
       to populate data, then apply the migration and measure timing.

2. **Shadow-column migration benchmark**:
       Same data as scenario 1, but test the alternative shadow-column migration path
       (add new column, backfill, swap) to compare lock duration.

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
                                         + 10,000,000 callback rows
    100,000 serialized_dag rows (with 2 deadline alerts each)
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone as dt_tz

import uuid6
from sqlalchemy import func, insert, select, text

from airflow import settings
from airflow.models.callback import Callback
from airflow.models.dag import DagModel
from airflow.models.dag_version import DagVersion
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagrun import DagRun
from airflow.models.deadline import Deadline
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
# Helpers
# ---------------------------------------------------------------------------
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

            dag_rows.append(
                {
                    "dag_id": dag_id,
                    "is_paused": False,
                    "is_stale": False,
                    "bundle_name": BENCHMARK_BUNDLE,
                    "max_active_tasks": 16,
                    "max_consecutive_failed_dag_runs": 0,
                    "has_task_concurrency_limits": False,
                    "timetable_type": "",
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

            # Column name in the table is "data" (not "_data")
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
    """Insert DagRun, Callback, and Deadline rows in batches.

    For each batch of DAGs:
    1. Insert dag_run rows.
    2. Query back the auto-generated dag_run.id values.
    3. Insert callback + deadline rows using those IDs.
    """
    total_runs = NUM_DAGS * RUNS_PER_DAG
    total_deadlines = total_runs * DEADLINES_PER_RUN
    print(
        f"\n[Phase 3] Inserting {total_runs:,} dag_runs "
        f"+ {total_deadlines:,} callbacks + {total_deadlines:,} deadlines..."
    )
    t0 = time.monotonic()

    # Keep each transaction around ~10K dag_runs
    dags_per_batch = max(1, 10_000 // RUNS_PER_DAG)
    inserted_runs = 0
    inserted_deadlines = 0

    for batch_start in range(0, NUM_DAGS, dags_per_batch):
        batch_end = min(batch_start + dags_per_batch, NUM_DAGS)
        dag_ids_in_batch = [f"{DAG_PREFIX}{i}" for i in range(batch_start, batch_end)]

        # Build dag_run rows
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

            # Build callback + deadline rows using the dag_run IDs
            cb_rows: list[dict] = []
            dl_rows: list[dict] = []
            for dag_idx in range(batch_start, batch_end):
                for run_idx in range(RUNS_PER_DAG):
                    gs = dag_idx * RUNS_PER_DAG + run_idx
                    run_id = f"{RUN_PREFIX}{gs}"
                    db_id = run_id_to_db_id[run_id]

                    for dl_idx in range(DEADLINES_PER_RUN):
                        cb_id = uuid6.uuid7()
                        dl_id = uuid6.uuid7()
                        deadline_time = BASE_DATE + timedelta(seconds=gs, hours=dl_idx + 1)

                        cb_rows.append(
                            {
                                "id": cb_id,
                                "type": "triggerer",
                                "fetch_method": "import_path",
                                "data": {
                                    "path": "benchmark.callback_func",
                                    "kwargs": {},
                                    "prefix": "deadline_alerts",
                                },
                                "state": "scheduled",
                                "priority_weight": 1,
                                "created_at": BASE_DATE,
                            }
                        )

                        # Only include pre-migration columns: the migration adds
                        # created_at, last_updated_at, and deadline_alert_id later.
                        dl_rows.append(
                            {
                                "id": dl_id,
                                "dagrun_id": db_id,
                                "deadline_time": deadline_time,
                                "missed": False,
                                "callback_id": cb_id,
                            }
                        )

            session.execute(insert(Callback.__table__), cb_rows)
            session.execute(insert(Deadline.__table__), dl_rows)

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
        for table in ("deadline", "callback", "dag_run", "serialized_dag"):
            print(f"  VACUUM ANALYZE {table}...")
            cursor.execute(f"VACUUM ANALYZE {table}")
        cursor.close()
    finally:
        raw_conn.close()
    print(f"  Done in {time.monotonic() - t0:.1f}s")


@provide_session
def _print_report(*, session=NEW_SESSION) -> None:
    """Print table sizes and row counts."""
    dl_count = session.scalar(select(func.count()).select_from(Deadline.__table__))
    cb_count = session.scalar(select(func.count()).select_from(Callback.__table__))
    dr_count = session.scalar(select(func.count()).select_from(DagRun.__table__))
    sd_count = session.scalar(select(func.count()).select_from(SerializedDagModel.__table__))

    dl_size = session.scalar(text("SELECT pg_size_pretty(pg_total_relation_size('deadline'))"))
    cb_size = session.scalar(text("SELECT pg_size_pretty(pg_total_relation_size('callback'))"))
    dr_size = session.scalar(text("SELECT pg_size_pretty(pg_total_relation_size('dag_run'))"))
    sd_size = session.scalar(text("SELECT pg_size_pretty(pg_total_relation_size('serialized_dag'))"))

    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"  deadline:          {dl_count:,} rows  ({dl_size})")
    print(f"  callback:          {cb_count:,} rows  ({cb_size})")
    print(f"  dag_run:           {dr_count:,} rows  ({dr_size})")
    print(f"  serialized_dag:    {sd_count:,} rows  ({sd_size})")
    print("=" * 60)


def _cleanup() -> None:
    """Delete all benchmark data in FK order."""
    print("Cleaning up benchmark data...")
    t0 = time.monotonic()

    # Phase 1: Delete deadlines and their callbacks in batches.
    # We batch because collecting 10M+ IDs at once is impractical.
    total_dl = 0
    total_cb = 0
    batch_size = 50_000

    while True:
        with create_session() as session:
            rows = session.execute(
                select(Deadline.__table__.c.id, Deadline.__table__.c.callback_id)
                .join(
                    DagRun.__table__,
                    Deadline.__table__.c.dagrun_id == DagRun.__table__.c.id,
                )
                .where(DagRun.__table__.c.dag_id.like(f"{DAG_PREFIX}%"))
                .limit(batch_size)
            ).all()

            if not rows:
                break

            dl_ids = [r[0] for r in rows]
            cb_ids = [r[1] for r in rows]

            session.execute(Deadline.__table__.delete().where(Deadline.__table__.c.id.in_(dl_ids)))
            session.execute(Callback.__table__.delete().where(Callback.__table__.c.id.in_(cb_ids)))

        total_dl += len(dl_ids)
        total_cb += len(cb_ids)
        elapsed = time.monotonic() - t0
        print(f"  {total_dl:,} deadlines, {total_cb:,} callbacks ({elapsed:.1f}s)")

    # Phase 2: Delete remaining supporting data
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
        f"Deleted: {total_dl:,} deadlines, {total_cb:,} callbacks, {dr_del:,} dag_runs, "
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
    print(f"  Total callbacks:     {total_deadlines:,}")
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
    print("  airflow db migrate                      # upgrade")
    print("  airflow db downgrade -r e79fc784f145     # downgrade back to pre-0101")

    return 0


if __name__ == "__main__":
    sys.exit(main())
