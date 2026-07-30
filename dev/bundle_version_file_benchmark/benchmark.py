#!/usr/bin/env python
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
Benchmark the initial-load of ``BundleVersionFile`` records (AIP-XX directory-tree code view).

This measures the H5 "inventory population" step end to end: when the Dag processor first observes a new
``(bundle_name, bundle_version)`` it (1) walks the bundle checkout on disk, stats each file, and -- if the
schema keeps ``file_content_hash`` -- reads each file's bytes to hash them, then (2) bulk-inserts one metadata
row per file into the metadata DB. Both halves matter, so both are timed:

  * ``fs_scan_metadata_only``      -- os.walk + stat + path hash, no file content read.
  * ``fs_scan_with_content_hash``  -- the above plus reading every file and md5-hashing its bytes.
  * ``db_insert_*``                -- the bulk insert of the produced rows.
  * ``TOTAL_*``                    -- fs phase + db insert, i.e. the real per-commit cost.

The gap between the two fs phases is exactly the cost of computing ``file_content_hash`` at build time, which
informs whether that column is worth reading every file on every new commit (vs. e.g. reusing a git blob sha).

Run (SQLite file, no Docker needed):
    uv run --project airflow-core python dev/bundle_version_file_benchmark/benchmark.py

Against a real backend (start it with ``breeze`` first), e.g. Postgres / MySQL:
    uv run --project airflow-core --with psycopg2-binary python dev/bundle_version_file_benchmark/benchmark.py \
        --db-url postgresql+psycopg2://postgres:airflow@127.0.0.1:25462/airflow

Notes:
  * The synthetic bundle is written to disk immediately before scanning, so reads are warm (page cache) -- this
    mirrors a bundle just materialized by ``git checkout`` / object-store sync. Cold-cache reads would be
    slower; pass ``--repeats 1`` and drop caches externally to approximate that.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import os
import platform
import random
import shutil
import statistics
import tempfile
import time
from pathlib import Path

import sqlalchemy
from sqlalchemy import (
    Column,
    DateTime,
    Index,
    Integer,
    String,
    UniqueConstraint,
    create_engine,
    insert,
)
from sqlalchemy.orm import Session, declarative_base

try:
    from uuid6 import uuid7 as _new_uuid

    UUID_KIND = "uuid7"
except ImportError:
    from uuid import uuid4 as _new_uuid

    UUID_KIND = "uuid4"

Base = declarative_base()

BUNDLE_NAME = "monorepo"
TEAMS = 8
PKGS = 64  # dirs per tree = TEAMS * PKGS = 512, holding the N files
MIN_FILE_BYTES = 200
MAX_FILE_BYTES = 8_000  # realistic Python-module spread; avg ~4 KB


class BundleVersionFile(Base):
    """Standalone mirror of the proposed AIP-XX table; metadata only, no file content."""

    __tablename__ = "bundle_version_file"

    id = Column(String(36), primary_key=True)
    bundle_name = Column(String(250), nullable=False)
    bundle_version = Column(String(200), nullable=False)
    relative_fileloc = Column(String(2000), nullable=False)
    relative_fileloc_hash = Column(String(32), nullable=False)
    file_content_hash = Column(String(32), nullable=False)
    size = Column(Integer, nullable=False)
    created_at = Column(DateTime, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "bundle_name", "bundle_version", "relative_fileloc_hash", name="bundle_version_file_uq"
        ),
        Index("idx_bvf_lookup", "bundle_name", "bundle_version"),
    )


def materialize_bundle(root: Path, n: int, rng: random.Random) -> int:
    """Write ``n`` .py files with realistic sizes under ``root``. Returns total bytes. Not timed."""
    for team in range(TEAMS):
        for pkg in range(PKGS):
            (root / "dags" / f"team_{team}" / f"pkg_{pkg}").mkdir(parents=True, exist_ok=True)
    total = 0
    for i in range(n):
        path = root / "dags" / f"team_{i % TEAMS}" / f"pkg_{i % PKGS}" / f"module_{i}.py"
        size = rng.randint(MIN_FILE_BYTES, MAX_FILE_BYTES)
        header = f"# module {i}\nimport airflow\n".encode()
        data = header + b"x" * max(0, size - len(header))
        path.write_bytes(data)
        total += len(data)
    return total


def scan_bundle(root: Path, bundle_version: str, *, with_content_hash: bool) -> list[dict]:
    """Walk the bundle and build inventory rows. Timed. Reads file bytes only when hashing content."""
    now = datetime.datetime.now(datetime.timezone.utc)
    rows: list[dict] = []
    for dirpath, _dirs, files in os.walk(root):
        for fname in files:
            if not fname.endswith(".py"):  # extension allow-list, applied at build time
                continue
            abs_path = os.path.join(dirpath, fname)
            rel = os.path.relpath(abs_path, root)
            stat = os.stat(abs_path)
            if with_content_hash:
                with open(abs_path, "rb") as fh:
                    content_hash = hashlib.md5(fh.read()).hexdigest()
            else:
                content_hash = "0" * 32  # placeholder; metadata-only pipeline does not read content
            rows.append(
                {
                    "id": str(_new_uuid()),
                    "bundle_name": BUNDLE_NAME,
                    "bundle_version": bundle_version,
                    "relative_fileloc": rel,
                    "relative_fileloc_hash": hashlib.md5(rel.encode()).hexdigest(),
                    "file_content_hash": content_hash,
                    "size": stat.st_size,
                    "created_at": now,
                }
            )
    return rows


def insert_core_executemany(session: Session, rows: list[dict], chunk: int) -> None:
    """Core executemany, chunked and committed once -- the pattern collection.py uses (line 1032)."""
    for start in range(0, len(rows), chunk):
        session.execute(insert(BundleVersionFile), rows[start : start + chunk])
    session.commit()


def insert_orm_bulk_save(session: Session, rows: list[dict], chunk: int) -> None:
    for start in range(0, len(rows), chunk):
        session.bulk_save_objects([BundleVersionFile(**r) for r in rows[start : start + chunk]])
    session.commit()


DB_STRATEGIES = {
    "core_executemany": insert_core_executemany,
    "orm_bulk_save": insert_orm_bulk_save,
}


def reset_schema(engine) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


def median_ms(fn, repeats: int) -> float:
    durations = [_timed(fn) for _ in range(repeats)]
    return statistics.median(durations) * 1000


def _timed(fn) -> float:
    start = time.perf_counter()
    fn()
    return time.perf_counter() - start


def time_db_insert(engine, strategy_fn, rows: list[dict], chunk: int, repeats: int) -> float:
    def run() -> None:
        reset_schema(engine)
        with Session(engine) as session:
            strategy_fn(session, rows, chunk)

    # reset_schema is included but its cost is negligible vs. the insert; kept inside so each run is independent.
    return median_ms(run, repeats)


def make_engine(db_url: str | None) -> tuple[object, str]:
    if db_url:
        return create_engine(db_url), db_url
    tmp_dir = Path(tempfile.mkdtemp(prefix="bvf_bench_db_"))
    url = f"sqlite:///{tmp_dir / 'bench.db'}"
    return create_engine(url), url


def human_bytes(num: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if num < 1024 or unit == "GB":
            return f"{num:.1f} {unit}"
        num /= 1024
    return f"{num:.1f} GB"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", type=int, nargs="+", default=[1_000, 10_000, 100_000])
    parser.add_argument("--repeats", type=int, default=3, help="runs per phase; median reported")
    parser.add_argument("--chunk", type=int, default=1_000, help="rows per executemany batch")
    parser.add_argument("--db-url", default=None, help="SQLAlchemy URL; default is a temp SQLite file")
    parser.add_argument("--seed", type=int, default=1234)
    args = parser.parse_args()

    engine, url = make_engine(args.db_url)
    dialect = engine.dialect.name
    rng = random.Random(args.seed)

    print("BundleVersionFile initial-load benchmark (filesystem scan + DB insert)")
    print(f"  sqlalchemy={sqlalchemy.__version__}  python={platform.python_version()}  uuid={UUID_KIND}")
    print(f"  backend={dialect}  url={url}")
    print(
        f"  repeats={args.repeats}  chunk={args.chunk}  seed={args.seed}  file_bytes={MIN_FILE_BYTES}..{MAX_FILE_BYTES}"
    )

    for n in args.sizes:
        bundle_dir = Path(tempfile.mkdtemp(prefix=f"bvf_bench_fs_{n}_"))
        try:
            total_bytes = materialize_bundle(bundle_dir, n, rng)
            version = f"sha-{n}"

            fs_meta = median_ms(
                lambda: scan_bundle(bundle_dir, version, with_content_hash=False), args.repeats
            )
            fs_content = median_ms(
                lambda: scan_bundle(bundle_dir, version, with_content_hash=True), args.repeats
            )
            rows = scan_bundle(bundle_dir, version, with_content_hash=True)

            db_times = {
                name: time_db_insert(engine, fn, rows, args.chunk, args.repeats)
                for name, fn in DB_STRATEGIES.items()
            }
            db_core = db_times["core_executemany"]

            print()
            print(
                f"size={n:,}  files={len(rows):,}  total={human_bytes(total_bytes)}  "
                f"avg_file={human_bytes(total_bytes / n)}"
            )
            print(
                f"  fs_scan_metadata_only        {fs_meta:>9.1f} ms   ({n / (fs_meta / 1000):>11,.0f} files/s)"
            )
            print(
                f"  fs_scan_with_content_hash    {fs_content:>9.1f} ms   "
                f"({total_bytes / 1024 / 1024 / (fs_content / 1000):>8,.0f} MB/s incl. read+md5)"
            )
            for name, ms in db_times.items():
                print(f"  db_insert_{name:<18}{ms:>9.1f} ms   ({n / (ms / 1000):>11,.0f} rows/s)")
            print("  " + "-" * 52)
            print(f"  TOTAL metadata pipeline      {fs_meta + db_core:>9.1f} ms   (fs_meta + db_core)")
            print(f"  TOTAL content-hash pipeline  {fs_content + db_core:>9.1f} ms   (fs_content + db_core)")
        finally:
            shutil.rmtree(bundle_dir, ignore_errors=True)

    engine.dispose()


if __name__ == "__main__":
    main()
