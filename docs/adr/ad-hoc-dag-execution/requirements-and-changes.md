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

# Ad-hoc DAG execution: functional requirements and required changes

> Companion to [the AIP](./aip-ad-hoc-dag-execution.md),
> [ADR 0001](./0001-ad-hoc-dag-execution.md), and
> [ADR 0002](./0002-parsing-and-execution-mechanism.md).
>
> The AIP captures the proposal at the level needed for
> dev@airflow.apache.org discussion. This document is the
> implementation-side companion: numbered functional requirements that
> reviews, issues, and tests can cite, plus a concrete inventory of the
> code, schema, and configuration changes the proposal introduces.

## Functional requirements

Each requirement is given a stable identifier (`FR-N`) so reviews,
issues, and tests can reference them directly.

### Submission lifecycle

- **FR-1 — Submit a local DAG file with one command.** A user runs
  `airflow submit <entry_file>` (or the airflow-ctl / REST equivalent)
  and receives a submission ID and, on completion, the user DagRun's
  final state.
- **FR-2 — Automatic local-dependency discovery.** The CLI walks the
  entry file's imports (`ast` + `importlib.util.find_spec`), bundles
  local modules, and excludes installed packages.
- **FR-3 — Explicit file inclusion.** The CLI accepts a way to add
  files the static walker missed (runtime imports, data files). Form
  is TBD (e.g. `--include path`).
- **FR-4 — Pluggable archive transport.** Upload uses `get_fs()`, so
  any scheme the deployment has a configured connection for (`s3`,
  `gs`, `azure`, `file`) works without bespoke transport.
- **FR-5 — Single-node fallback.** When no object-store is configured,
  the API server proxies the upload to a path the worker can read, so
  the feature is usable in `breeze` and minimal local setups.
- **FR-6 — Submission state is observable.** A submission row tracks
  `PENDING_UPLOAD → READY → RUNNING → SUCCESS / FAILED / CANCELLED`
  and is queryable via the public REST API.
- **FR-7 — Cancellation.** A user can cancel their own submission;
  cancellation tears down the user DagRun (if running) and cleans up
  the archive.
- **FR-8 — Forwarded parameters.** `--param key=value ...` flags are
  forwarded as the user DagRun's `conf`.
- **FR-9 — Watch mode.** `--watch` (default on) streams the user
  DagRun's task logs to the CLI and exits with a non-zero status if
  the run fails. `--no-watch` returns immediately after triggering.

### Architecture & boundaries

- **FR-10 — Worker writes only via the Execution API.** Ad-hoc DAG
  registration goes through a new Execution API endpoint; no worker
  path imports DB-bound models.
- **FR-11 — DAG provenance enforced.** `DagModel` carries a
  `BUNDLE`/`ADHOC` marker. Ad-hoc registration of a `dag_id` that
  already has a `BUNDLE` row is rejected.
- **FR-12 — Structured collision response.** Ad-hoc-vs-ad-hoc
  collisions on the same `dag_id` return a clear, machine-readable
  indicator so deployment-side governance can react.
- **FR-13 — Default queries hide ad-hoc DAGs.** Existing `DagModel`
  listings (UI, metrics, `airflow dags list`) keep their current
  shape; ad-hoc rows are filtered out by default and surfaced through
  an opt-in filter.
- **FR-14 — Orchestration as a system DAG.** The
  download → parse → register → trigger sequence is a built-in DAG
  shipped with Airflow, not a bespoke dispatcher; logs, retries, and
  UI come from the existing DagRun infrastructure.
- **FR-15 — Two-DagRun model.** The system DAG run (orchestrator) and
  the user DAG run (workload) are separate first-class DagRuns;
  failure of either is observable independently.
- **FR-16 — `OPERATOR_TRIGGERED` user run.** The user DAG run is
  created by `TriggerDagRunOperator` and uses
  `DagRunType.OPERATOR_TRIGGERED`. No new `DagRunType` is introduced.

### Auth, security, governance

- **FR-17 — Authenticated submissions only.** Anonymous access to the
  submission endpoints is rejected.
- **FR-18 — Per-task-instance JWT.** The worker's call to the
  Execution API registration endpoint uses the existing per-TI JWT;
  no new credential surface is added.
- **FR-19 — Per-user visibility default.** A user sees and cancels
  only their own submissions unless RBAC grants broader access.
- **FR-20 — Submission identity recorded.** Each submission row
  records `created_by` and is queryable by it.

### Operability & failure handling

- **FR-21 — Parse errors surface as a failed run.** A bad archive,
  missing entry file, or import error is reported as a failed system
  DAG task with a stack trace through the standard task-log endpoint,
  not a stuck task.
- **FR-22 — Logs survive the archive.** Task logs of the system DAG
  and the user DAG are retained per the deployment's normal log
  retention policy, independent of archive retention.
- **FR-23 — Configurable archive retention.** Deployments can choose
  retention per success/failure and a default expiry for
  `PENDING_UPLOAD` rows that never receive an upload.
- **FR-24 — REST surface is the source of truth.** The CLI and
  airflow-ctl commands are clients of the public REST API; no
  CLI-only behaviour exists.

### Out of scope (explicitly not requirements for this AIP)

- A "Submissions" UI tab. Ad-hoc DagRuns appear in the existing DAG
  views with their provenance marker; a dedicated tab is a follow-up.
- Multi-team executor/queue scoping for submissions. Coordinated with
  the multi-team owners in a follow-up.
- Per-user quotas enforced at the API server. Recommended at GA but
  not part of the initial landing.
- A Python-callable submit form (`client.submit(fn, *args)`).
- "Promoting" a submission into a permanent bundle DAG.

## Required changes

A non-exhaustive but committed inventory of the code, schema, and
configuration changes the AIP introduces. Paths reflect today's
repository layout.

### Data model and migrations

- New ORM model `Submission` in
  `airflow-core/src/airflow/models/submission.py` with
  fields: `id` (UUID), `created_by`, `created_at`, `state`,
  `archive_uri`, `entry_file`, `manifest` (JSON), `dag_run_id` FK.
  Indexes on `created_by`, `state`.
- Provenance marker on `DagModel`
  (`airflow-core/src/airflow/models/dag.py`) — column or sibling
  table; values `BUNDLE` (default) and `ADHOC`. Default-query filter
  applied at the query layer.
- Alembic migration in `airflow-core/src/airflow/migrations/versions/`
  for the new table, the provenance marker, and the FK.

### Public REST API (api_fastapi/core_api)

- New routes module
  `airflow-core/src/airflow/api_fastapi/core_api/routes/public/submissions.py`
  exposing create, upload-target, run, get, list, cancel.
- New Pydantic schemas in
  `airflow-core/src/airflow/api_fastapi/core_api/datamodels/`
  (`submissions.py`).
- OpenAPI generation refreshed for the public client SDKs.

### Execution API

- New endpoint under
  `airflow-core/src/airflow/api_fastapi/execution_api/` accepting a
  serialised parsed DAG plus submitter identity and submission ID.
  Enforces provenance rules from FR-11/FR-12.
- Per-task-instance JWT scoping extended (where needed) so a system
  DAG task can call the registration endpoint with its own TI token
  (FR-18).

### System DAG (orchestrator)

- New built-in DAG shipped with Airflow with three tasks: download
  archive (`get_fs()`), parse + POST serialised DAG to the new
  Execution API endpoint, `TriggerDagRunOperator` against the
  registered `dag_id`. Location TBD (open question in ADR 0002).

### Task SDK and worker runtime

- Hook in
  `task-sdk/src/airflow/sdk/execution_time/task_runner.py` (or in the
  system DAG's task implementations) for the
  download / unpack / parse path on the worker.
- Helper for invoking the existing parser against an unpacked entry
  file in-process (mirrors what `DagBag.process_file()` does today,
  without registering with `DagBundlesManager`).

### CLI

- New `airflow submit <entry_file>` command in the Task SDK / CLI
  (`airflow-core/src/airflow/cli/`) with flags `--param`, `--watch`,
  `--no-watch`, plus a way to add explicit files (FR-3).
- Local dependency-discovery utility (`ast` walker +
  `importlib.util.find_spec` resolver).
- Archive packing (zip) and presigned-upload client.

### airflow-ctl

- New subcommand group (e.g. `airflowctl submissions ...`) that
  consumes the new public REST endpoints. No bespoke transport.

### UI (minimal)

- Provenance marker rendered on existing DAG / DagRun views so users
  can tell ad-hoc DAGs apart. A dedicated "Submissions" tab is a
  follow-up.

### Configuration

- New config keys (under a new `[ad_hoc]` section or equivalent):
  default object-store URI for archives, retention policy, default
  `PENDING_UPLOAD` expiry, optional per-user quota knobs.

### Documentation

- User guide: "Submitting ad-hoc DAGs" under
  `airflow-core/docs/`.
- Deployment Manager guide: object-store configuration, retention,
  quotas, and the recommended `dag_id` naming convention.
- Update to `airflow-core/docs/security/security_model.rst`
  documenting the new public surface, the unchanged trust model, and
  the required mitigations (auth, RBAC, quotas).

### Tests

- Unit tests for the new ORM model, the provenance check, and the
  default-query filter.
- Unit + integration tests for the new public REST routes and the
  Execution API registration endpoint (provenance rejection,
  collision response shape).
- End-to-end integration test that submits a real archive, runs the
  system DAG, registers the user DAG, and asserts the user DagRun
  reaches `success`.
- CLI tests for local dependency discovery, packing, upload, watch
  mode exit codes.
- Static-analysis test asserting no DB-bound model imports from
  worker paths added by this AIP (FR-10).

### Newsfragment

- A `feature` newsfragment describing the new `airflow submit`
  workflow and the new REST surface.
