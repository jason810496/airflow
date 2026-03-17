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
Benchmark data generator for migration 0102_3_2_0_make_external_executor_id_text.

Populates task_instance and task_instance_history with configurable row counts
to measure migration timing for the external_executor_id VARCHAR(250) -> TEXT change.

Scenarios
---------
1. **Upgrade benchmark** (VARCHAR -> TEXT, instant on PostgreSQL):
       Run this script *before* applying the migration.
       Use default settings (BENCHMARK_LONG_ID_PCT=0) so all values fit VARCHAR(250).

2. **Downgrade benchmark** (TEXT -> VARCHAR, requires full table rewrite + validation):
       Run this script *after* applying the migration (column is already TEXT).
       Set BENCHMARK_LONG_ID_PCT > 0 to insert values > 250 chars and verify that
       the downgrade fails with a data-truncation error.

3. **Shadow-column migration benchmark**:
       Same data as scenario 2, but test the alternative shadow-column migration path
       (add new column, backfill, swap) to compare lock duration.

Run inside Breeze with PostgreSQL backend
-----------------------------------------
    breeze --backend postgres shell
    python /opt/airflow/scripts/in_container/\
benchmark__0102_3_2_0_make_external_executor_id_text__migration.py

Environment variables
---------------------
    BENCHMARK_NUM_DAGS        Number of DAGs (default: 100)
    BENCHMARK_TASKS_PER_DAG   Tasks per DAG (default: 100)
    BENCHMARK_RUNS_PER_DAG    Runs per DAG (default: 100)
    BENCHMARK_EXT_ID_PCT      % of rows with external_executor_id set (default: 30)
    BENCHMARK_LONG_ID_PCT     % of those IDs that exceed 250 chars (default: 0)
    BENCHMARK_CLEANUP         Set to "1" to delete all benchmark data and exit
    BENCHMARK_NUM_WORKERS     Parallel workers for Phase 3 (default: 4)

Default configuration produces:
    100 DAGs x 1,00 runs x 100 tasks = 1,000,000 task_instance rows
                                       + 1,000,000 task_instance_history rows
"""

from __future__ import annotations

import io
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone as dt_tz

import uuid6
from sqlalchemy import func, insert, select, text

from airflow import settings
from airflow.models.dagrun import DagRun
from airflow.models.pool import Pool
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskinstancehistory import TaskInstanceHistory
from airflow.models.tasklog import LogTemplate
from airflow.utils.session import NEW_SESSION, create_session, provide_session

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
NUM_DAGS = int(os.environ.get("BENCHMARK_NUM_DAGS", "100"))
TASKS_PER_DAG = int(os.environ.get("BENCHMARK_TASKS_PER_DAG", "100"))
RUNS_PER_DAG = int(os.environ.get("BENCHMARK_RUNS_PER_DAG", "100"))
EXT_ID_PCT = int(os.environ.get("BENCHMARK_EXT_ID_PCT", "30")) / 100.0
LONG_ID_PCT = int(os.environ.get("BENCHMARK_LONG_ID_PCT", "0")) / 100.0
NUM_WORKERS = int(os.environ.get("BENCHMARK_NUM_WORKERS", "4"))

DAG_PREFIX = "benchmark_dag_"
RUN_PREFIX = "benchmark_run_"
BASE_DATE = datetime(2020, 1, 1, tzinfo=dt_tz.utc)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _make_ext_id(task_idx: int, run_id: str) -> str | None:
    """Generate an external_executor_id based on configured percentages."""
    if random.random() >= EXT_ID_PCT:
        return None
    if random.random() < LONG_ID_PCT:
        # Value > 250 chars: triggers truncation error on downgrade
        return f"long-executor-id-{task_idx}-{run_id}-{'x' * 280}"
    return f"executor-id-{task_idx}-{run_id}"


# ---------------------------------------------------------------------------
# Phase functions
# ---------------------------------------------------------------------------
@provide_session
def _ensure_prerequisites(*, session=NEW_SESSION) -> int:
    """Ensure log_template and default_pool exist. Returns log_template_id."""
    log_template_id = session.scalar(select(func.max(LogTemplate.id)))
    if log_template_id is None:
        lt = LogTemplate(filename="benchmark.log", elasticsearch_id="benchmark")
        session.add(lt)
        session.flush()
        log_template_id = lt.id

    if not session.scalar(select(Pool).where(Pool.pool == Pool.DEFAULT_POOL_NAME)):
        session.add(
            Pool(
                pool=Pool.DEFAULT_POOL_NAME,
                slots=128,
                description="Default pool",
                include_deferred=False,
            )
        )
    return log_template_id


def _insert_dag_runs(log_template_id: int) -> None:
    """Insert dag_run rows for all benchmark DAGs."""
    total = NUM_DAGS * RUNS_PER_DAG
    print(f"\n[Phase 2] Inserting {total:,} dag_runs...")
    t0 = time.monotonic()

    # ~50K dag_runs per batch (up from ~10K)
    dags_per_batch = max(1, 50_000 // RUNS_PER_DAG)
    inserted = 0

    for batch_start in range(0, NUM_DAGS, dags_per_batch):
        batch_end = min(batch_start + dags_per_batch, NUM_DAGS)
        rows = []
        for dag_idx in range(batch_start, batch_end):
            for run_idx in range(RUNS_PER_DAG):
                gs = dag_idx * RUNS_PER_DAG + run_idx
                logical_date = BASE_DATE + timedelta(seconds=gs)
                rows.append(
                    {
                        "dag_id": f"{DAG_PREFIX}{dag_idx}",
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
            session.execute(text("SET LOCAL synchronous_commit = off"))
            session.execute(insert(DagRun.__table__), rows)

        inserted += len(rows)
        elapsed = time.monotonic() - t0
        if inserted % 50_000 == 0 or batch_end == NUM_DAGS:
            print(f"  {inserted:,}/{total:,} ({elapsed:.1f}s)")

    print(f"  Done in {time.monotonic() - t0:.1f}s")


def _insert_single_batch(batch_start: int, batch_end: int) -> tuple[int, int]:
    """Insert task_instance + task_instance_history for a batch of DAGs via COPY.

    Returns ``(num_task_instances, num_task_instance_history)`` inserted.
    """
    pool_name = Pool.DEFAULT_POOL_NAME
    ti_buf = io.StringIO()
    tih_buf = io.StringIO()
    ti_count = 0

    for dag_idx in range(batch_start, batch_end):
        dag_id = f"{DAG_PREFIX}{dag_idx}"
        for run_idx in range(RUNS_PER_DAG):
            gs = dag_idx * RUNS_PER_DAG + run_idx
            run_id = f"{RUN_PREFIX}{gs}"

            for task_idx in range(TASKS_PER_DAG):
                ti_id = uuid6.uuid7()
                ext_id = _make_ext_id(task_idx, run_id)
                task_id = f"task_{task_idx}"
                ext_id_copy = (
                    ext_id.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n")
                    if ext_id is not None
                    else "\\N"
                )

                # task_instance columns: id, task_id, dag_id, run_id, map_index,
                # state, try_number, max_tries, hostname, unixname, pool, pool_slots,
                # queue, priority_weight, custom_operator_name, executor_config,
                # external_executor_id, span_status
                ti_buf.write(
                    f"{ti_id}\t{task_id}\t{dag_id}\t{run_id}\t-1\tsuccess\t"
                    f"1\t0\t\t\t{pool_name}\t1\tdefault\t1\t\t"
                    f"{{}}\t{ext_id_copy}\tUNSET\n"
                )

                # task_instance_history columns: task_instance_id, task_id, dag_id,
                # run_id, map_index, try_number, state, max_tries, hostname, unixname,
                # pool, pool_slots, queue, priority_weight, executor_config,
                # external_executor_id, span_status
                tih_buf.write(
                    f"{ti_id}\t{task_id}\t{dag_id}\t{run_id}\t-1\t1\tsuccess\t"
                    f"0\t\t\t{pool_name}\t1\tdefault\t1\t"
                    f"{{}}\t{ext_id_copy}\tUNSET\n"
                )
                ti_count += 1

    # COPY both tables via raw psycopg2 in a single transaction
    ti_buf.seek(0)
    tih_buf.seek(0)
    raw_conn = settings.engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        cursor.execute("SET synchronous_commit = off")
        cursor.copy_from(
            ti_buf,
            "task_instance",
            columns=(
                "id",
                "task_id",
                "dag_id",
                "run_id",
                "map_index",
                "state",
                "try_number",
                "max_tries",
                "hostname",
                "unixname",
                "pool",
                "pool_slots",
                "queue",
                "priority_weight",
                "custom_operator_name",
                "executor_config",
                "external_executor_id",
                "span_status",
            ),
        )
        cursor.copy_from(
            tih_buf,
            "task_instance_history",
            columns=(
                "task_instance_id",
                "task_id",
                "dag_id",
                "run_id",
                "map_index",
                "try_number",
                "state",
                "max_tries",
                "hostname",
                "unixname",
                "pool",
                "pool_slots",
                "queue",
                "priority_weight",
                "executor_config",
                "external_executor_id",
                "span_status",
            ),
        )
        raw_conn.commit()
        cursor.close()
    finally:
        raw_conn.close()

    return ti_count, ti_count


def _insert_task_data() -> None:
    """Insert task_instance and task_instance_history rows using parallel workers + COPY."""
    total = NUM_DAGS * RUNS_PER_DAG * TASKS_PER_DAG
    print(
        f"\n[Phase 3] Inserting {total:,} task_instances + {total:,} task_instance_history "
        f"({NUM_WORKERS} workers)..."
    )
    t0 = time.monotonic()

    # Target ~50K TI rows per batch.  Each DAG produces RUNS_PER_DAG * TASKS_PER_DAG rows.
    dags_per_batch = max(1, 50_000 // (RUNS_PER_DAG * TASKS_PER_DAG))
    total_batches = (NUM_DAGS + dags_per_batch - 1) // dags_per_batch
    inserted_ti = 0
    inserted_tih = 0
    batches_done = 0

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as pool:
        futures = {}
        for batch_start in range(0, NUM_DAGS, dags_per_batch):
            batch_end = min(batch_start + dags_per_batch, NUM_DAGS)
            future = pool.submit(_insert_single_batch, batch_start, batch_end)
            futures[future] = (batch_start, batch_end)

        for future in as_completed(futures):
            ti_count, tih_count = future.result()
            inserted_ti += ti_count
            inserted_tih += tih_count
            batches_done += 1
            elapsed = time.monotonic() - t0

            if batches_done % 10 == 0 or batches_done == total_batches:
                rate = inserted_ti / elapsed if elapsed > 0 else 0
                print(
                    f"  TIs: {inserted_ti:,}/{total:,}, "
                    f"TIH: {inserted_tih:,}/{total:,} "
                    f"({elapsed:.1f}s, {rate:,.0f} rows/s)",
                    flush=True,
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
        for tbl in ("task_instance", "task_instance_history", "dag_run"):
            print(f"  VACUUM ANALYZE {tbl}...")
            try:
                cursor.execute(f"VACUUM ANALYZE {tbl}")
            except Exception:
                # VACUUM can fail in memory-constrained containers (e.g. Docker shm too small).
                # Fall back to ANALYZE-only which still updates planner statistics.
                print(f"    VACUUM ANALYZE failed, falling back to ANALYZE {tbl}...")
                cursor.execute(f"ANALYZE {tbl}")
        cursor.close()
    finally:
        raw_conn.close()
    print(f"  Done in {time.monotonic() - t0:.1f}s")


@provide_session
def _print_report(*, session=NEW_SESSION) -> None:
    """Print table sizes and row counts."""
    ti_count = session.scalar(select(func.count()).select_from(TaskInstance))
    tih_count = session.scalar(select(func.count()).select_from(TaskInstanceHistory))
    ti_size = session.scalar(text("SELECT pg_size_pretty(pg_total_relation_size('task_instance'))"))
    tih_size = session.scalar(text("SELECT pg_size_pretty(pg_total_relation_size('task_instance_history'))"))
    ti_ext = session.scalar(
        select(func.count()).select_from(TaskInstance).where(TaskInstance.external_executor_id.isnot(None))
    )
    ti_long = session.scalar(
        select(func.count())
        .select_from(TaskInstance)
        .where(
            TaskInstance.external_executor_id.isnot(None),
            func.length(TaskInstance.external_executor_id) > 250,
        )
    )

    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"  task_instance:              {ti_count:,} rows  ({ti_size})")
    print(f"  task_instance_history:      {tih_count:,} rows  ({tih_size})")
    print(f"  Rows with ext executor id:  {ti_ext:,}")
    print(f"  Rows with id > 250 chars:   {ti_long:,}")
    print("=" * 60)


@provide_session
def _cleanup(*, session=NEW_SESSION) -> None:
    """Delete all benchmark data in FK order."""
    print("Cleaning up benchmark data...")
    tih_del = session.execute(
        TaskInstanceHistory.__table__.delete().where(
            TaskInstanceHistory.__table__.c.dag_id.like(f"{DAG_PREFIX}%")
        )
    ).rowcount
    ti_del = session.execute(
        TaskInstance.__table__.delete().where(TaskInstance.__table__.c.dag_id.like(f"{DAG_PREFIX}%"))
    ).rowcount
    dr_del = session.execute(
        DagRun.__table__.delete().where(DagRun.__table__.c.dag_id.like(f"{DAG_PREFIX}%"))
    ).rowcount
    print(f"Deleted: {tih_del:,} TI history, {ti_del:,} TIs, {dr_del:,} dag_runs")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    # Force line-buffered stdout so progress is visible in docker exec without -t.
    sys.stdout.reconfigure(line_buffering=True)

    # Ensure the ORM engine/session is initialized (required for standalone scripts).
    settings.configure_orm()

    if os.environ.get("BENCHMARK_CLEANUP", "0") == "1":
        _cleanup()
        return 0

    total_dag_runs = NUM_DAGS * RUNS_PER_DAG
    total_tis = total_dag_runs * TASKS_PER_DAG

    print("=" * 60)
    print("Migration 0102 Benchmark Data Generator")
    print("=" * 60)
    print(f"  DAGs:              {NUM_DAGS:,}")
    print(f"  Tasks/DAG:         {TASKS_PER_DAG:,}")
    print(f"  Runs/DAG:          {RUNS_PER_DAG:,}")
    print(f"  Total dag_runs:    {total_dag_runs:,}")
    print(f"  Total TIs:         {total_tis:,}")
    print(f"  Total TI history:  {total_tis:,}")
    print(f"  External ID %:     {EXT_ID_PCT * 100:.0f}%")
    print(f"  Long ID (>250) %:  {LONG_ID_PCT * 100:.0f}%")
    print(f"  Workers:           {NUM_WORKERS}")
    print("=" * 60)

    overall_start = time.monotonic()

    print("\n[Phase 1] Setting up prerequisites...")
    log_template_id = _ensure_prerequisites()
    print(f"  log_template id={log_template_id}")

    _insert_dag_runs(log_template_id)
    _insert_task_data()
    # after inserting all the data, rest of the steps are optional
    try:
        _vacuum_analyze()
    except Exception as e:
        print(f"  Warning: VACUUM ANALYZE failed: {e}")
        print("  This may lead to slower migration performance due to outdated planner statistics.")
    try:
        _print_report()
    except Exception as e:
        print(f"  Warning: Failed to print report: {e}")

    total_elapsed = time.monotonic() - overall_start
    print(f"\nTotal elapsed: {total_elapsed:.1f}s")
    print("\nData is ready. Run the migration to benchmark:")
    print("  airflow db migrate          # upgrade  (VARCHAR -> TEXT, instant)")
    print("  airflow db downgrade -r 1   # downgrade (TEXT -> VARCHAR, slow rewrite)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
