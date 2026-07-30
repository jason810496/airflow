<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [`BundleVersionFile` initial-load benchmark](#bundleversionfile-initial-load-benchmark)
  - [Phases measured](#phases-measured)
  - [Running](#running)
  - [Results](#results)
  - [Interpretation](#interpretation)
  - [Design consequence for the AIP](#design-consequence-for-the-aip)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# `BundleVersionFile` initial-load benchmark

Supports AIP-XX (directory-tree code view). It measures the "inventory population" step end to end: when the Dag processor first observes a new `(bundle_name, bundle_version)`, it walks the bundle checkout, gathers per-file metadata, and bulk-inserts one row per file. The step has two halves and both are timed, because -- as the results show -- the filesystem half, not the DB, is the bottleneck.

## Phases measured

- `fs_scan_metadata_only` -- `os.walk` + `stat` + path hash, no file content read. This is the floor for enumerating files.
- `fs_scan_with_content_hash` -- the above plus reading every file's bytes and md5-hashing them, i.e. populating `file_content_hash` by reading files.
- `db_insert_core_executemany` -- `session.execute(insert(model), rows)`, chunked (1000/batch), committed once; the pattern the processor already uses (`airflow-core/src/airflow/dag_processing/collection.py:1032`).
- `db_insert_orm_bulk_save` -- `session.bulk_save_objects(...)`, for comparison.
- `TOTAL metadata pipeline` = `fs_scan_metadata_only` + `db_insert_core_executemany`.
- `TOTAL content-hash pipeline` = `fs_scan_with_content_hash` + `db_insert_core_executemany`.

The standalone table mirrors the proposed schema (metadata only, no content; `uuid7` PK, the `(bundle_name, bundle_version, relative_fileloc_hash)` unique key, the `(bundle_name, bundle_version)` lookup index). The synthetic bundle is real `.py` files (200 B - 8 KB, avg ~4 KB) written to disk immediately before scanning, so reads are warm (page cache) -- matching a bundle just materialized by `git checkout` / object-store sync. Cold-cache content reads would be slower.

## Running

```bash
# SQLite (temp file, no Docker)
uv run --project airflow-core python dev/bundle_version_file_benchmark/benchmark.py

# Postgres / MySQL (start a backend first, then pass --db-url)
uv run --project airflow-core --with psycopg2-binary python dev/bundle_version_file_benchmark/benchmark.py \
    --db-url postgresql+psycopg2://postgres:airflow@127.0.0.1:25462/airflow
```

Options: `--sizes 1000 10000 100000`, `--repeats 3`, `--chunk 1000`, `--seed 1234`.

## Results

One developer machine (macOS, SQLAlchemy 2.0.51, Python 3.12, `uuid7` PKs, `chunk=1000`, median of 3, warm cache). SQLite is a temp file on local SSD; Postgres is a `postgres:16` container over loopback (no network RTT). Order-of-magnitude, not authoritative. All times in ms; the filesystem phase is backend-independent (the small differences between the two tables are run-to-run noise).

### SQLite

| files | fs meta-only | fs +content-hash | db insert (core) | TOTAL meta | TOTAL +content-hash |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 18 | 38 | 5 | 23 | 43 |
| 10,000 | 74 | 323 | 36 | 110 | 359 |
| 100,000 | 734 | 4,818 | 489 | 1,222 | 5,306 |

### Postgres (loopback)

| files | fs meta-only | fs +content-hash | db insert (core) | TOTAL meta | TOTAL +content-hash |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 15 | 37 | 26 | 41 | 63 |
| 10,000 | 78 | 326 | 187 | 265 | 513 |
| 100,000 | 721 | 3,919 | 1,669 | 2,389 | 5,588 |

## Interpretation

- The DB insert is **not** the bottleneck. Reading every file to compute `file_content_hash` is: ~3.9-4.8s for a 100k-file bundle, roughly **8-10x** the DB insert (0.5-1.7s) and ~6x the metadata-only walk (~0.7s). The filesystem read scales with total bytes (~100 MB/s here, warm), so a larger average file size makes it worse, and a cold cache makes it worse still.
- The metadata-only path is cheap and dominated by one `stat` syscall per file (~135k files/s), giving a ~1.2s (SQLite) / ~2.4s (Postgres) end-to-end total for 100k files -- fine once per commit.
- `core_executemany` beats `orm_bulk_save` by ~2x on SQLite and ~1.3x on Postgres; batching bounds a remote DB to `ceil(N/1000)` round trips, so DB latency scales with round trips, not rows.

## Design consequence for the AIP

Do not populate `file_content_hash` by reading files during inventory build. Derive it from backend-native metadata that already exists after a checkout/sync:

- **Git:** `git ls-tree -r -l <sha>` returns path, size, and the blob sha for every file in one command -- no working-tree walk, no `stat`, no read. Use the blob sha as the content hash.
- **Object store (S3/GCS):** the sync manifest / list-objects response carries per-object size and ETag/MD5; reuse those.
- **Generic fallback:** if no native hash is available, either drop `file_content_hash` (it only backs future diff / change detection) or accept the read cost, but never make it the default on the parse path.

With native metadata the whole inventory build collapses toward (and below) the `fs meta-only` column, keeping the once-per-commit cost at ~1-2.5s even for an extreme 100k-file bundle.
