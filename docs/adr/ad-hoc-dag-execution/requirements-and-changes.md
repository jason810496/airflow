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
- **FR-5 — Object-store backend is required.** Object storage is the
  only supported archive backend (`s3`, `gs`, `azure`, or any fsspec
  scheme that supports presigned URLs via `get_fs()`). The API
  server does not proxy archive bytes and there is no local-
  filesystem fallback. Deployments without a configured object
  store, including `breeze` defaults, cannot use ad-hoc submission
  until they point at one (e.g. a local MinIO).
- **FR-6 — Submission state is observable.** A submission row tracks
  `PENDING_UPLOAD → READY → RUNNING → SUCCESS / FAILED / CANCELLED`
  and is queryable via the public REST API.
- **FR-7 — Cancellation.** A user can cancel their own submission;
  cancellation tears down the user DagRun (if running) and cleans up
  the archive. The cancellation flow itself is implemented as a system
  DAG (see `FR-25`) so its execution carries logs and an audit trail.
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
- **FR-25 — Cancellation as a system DAG.** Cancellation is performed
  by a separate built-in DAG. Its tasks signal the user DagRun (if
  running) to terminate, mark the submission `CANCELLED`, and clean
  up the archive. The DagRun of the cancellation DAG is the audit
  trail of who cancelled what and when, with task logs covering each
  step.
- **FR-26 — Auth-manager exposes submission actions.** The
  auth-manager interface gains a `Submission` resource and the
  actions needed to gate it (`create`, `view`, `cancel`). Every
  public REST entry point that handles a submission consults the
  auth-manager rather than checking ownership inline.
- **FR-27 — Audit-log entries for submission actions.** Submission
  create, upload completion, run trigger, cancel-requested, and
  cancel-completed events are recorded in the existing audit `Log`
  table. Records carry the submission ID, the actor, the event name,
  and a small structured payload.
- **FR-28 — Archive-download capability is minted on demand.** The
  parsing system DAG's download task obtains its archive-download
  credential by calling a new Execution API endpoint
  (`POST /execution/submissions/{submission_id}/archive_download_token`).
  The credential is never persisted on the `Submission` row or in
  `DagRun.conf`. See [ADR 0003](./0003-ephemeral-archive-download-token.md).
- **FR-29 — Token is scoped and time-bounded.** The minted credential
  binds to one `submission_id` and one `archive_uri`, with a TTL
  driven by `[api_auth] submission_archive_download_jwt_expiration_time`.
  Defaults are short (suggested 60–300 s) because the consumer is a
  task that is already running when it asks.
- **FR-30 — Workload DagRun cannot mint or use the archive-download
  token.** The Execution API endpoint authorizes the caller by
  checking `submission.parsing_dag_run_id == caller's dag_run_id`;
  workload DagRun task instances are refused with `403`. Cross-
  submission attempts (parsing DagRun for submission A asking for
  submission B's token) are similarly refused.
- **FR-31 — Archive bytes are served by the object store, not by
  Airflow.** The Execution API endpoint returns a presigned URL
  minted via `get_fs()` against the object-store-backed `archive_uri`
  (`FR-5`). Airflow does not host an archive bytes endpoint and does
  not proxy archive bytes for any deployment shape; the Execution
  API's `parsing_dag_run_id` check is the only Airflow-side access
  gate.
- **FR-32 — Atomic linkage of submission and parsing DagRun.** When
  the API server triggers the orchestrator system DAG, it must
  trigger the DagRun with `dag_run.conf = {"submission_id": "<uuid>"}`
  **and** record the resulting `dag_run_id` on
  `submission.parsing_dag_run_id` in the same transaction. This is
  what every submission-scoped Execution API authorization check
  binds to (`FR-30`, ADR 0003); failing the row write must fail the
  trigger so a parsing DagRun never exists without its row pointer
  set. `submission_id` is the only field the orchestrator carries in
  its `conf`; the parsing tasks fetch everything else (`archive_uri`,
  `entry_file`, `dag_run_conf` to forward) from the row via Execution
  API calls.

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

#### `submission` table

New ORM model in `airflow-core/src/airflow/models/submission.py`.

| Column | Type | Nullable | Notes |
| --- | --- | --- | --- |
| `id` | `UUID` | NO | Primary key. Stable submission identifier surfaced by the public REST API and CLI. |
| `created_by` | `str` (FK to auth user) | NO | Submitter identity. Drives `FR-19` (per-user visibility) and `FR-20` (queryable by submitter). |
| `created_at` | `datetime` | NO | Submission creation timestamp. |
| `state` | enum | NO | One of `PENDING_UPLOAD`, `READY`, `RUNNING`, `SUCCESS`, `FAILED`, `CANCELLED` (`FR-6`). |
| `archive_uri` | `str` | YES | Object-store URI where the uploaded archive lives. Null between create and upload completion. Resolved via `get_fs()` (`FR-4`). |
| `entry_file` | `str` | NO | Relative path inside the archive identifying the user-authored DAG file the worker should import. The archive may contain many local modules (`FR-2`/`FR-3`); the worker must be told which one is the entry point. |
| `dag_run_conf` | `JSON` | YES | The `conf` payload to be forwarded to the workload DagRun (`FR-8`). Mirrors `DagRun.conf`'s contract: opaque user-supplied JSON, accessible inside tasks via the templating context. The orchestrator system DAG's `TriggerDagRunOperator` task passes this dict through verbatim when it creates the workload DagRun. |
| `parsed_dag_id` | `str` (FK to `dag.dag_id`) | YES | The `dag_id` produced by the parse step. Null until the orchestrator system DAG's register task succeeds. The FK is constrained against rows with `provenance = ADHOC` so it cannot be wired to a bundle DAG. |
| `parsing_dag_run_id` | `UUID` (FK to `dag_run.id`) | YES | The orchestrator system DAG run that handled download → parse → register → trigger. Source of logs and the failure surface for the parsing phase (`FR-14`, `FR-21`). |
| `workload_dag_run_id` | `UUID` (FK to `dag_run.id`) | YES | The user's workload DagRun, created by `TriggerDagRunOperator` from the orchestrator with `dag_run_conf` as its `conf` (`FR-15`, `FR-16`). Source of the workload's logs and final state. |
| `cancellation_dag_run_id` | `UUID` (FK to `dag_run.id`) | YES | The cancellation system DAG run, if cancellation has been requested (`FR-7`, `FR-25`). Provides the audit trail and logs for the cancellation flow. |

Indexes:

- `(created_by, state)` — listing per user filtered by lifecycle state.
- `(state)` — global queries (e.g. expire stale `PENDING_UPLOAD` rows).
- `(parsed_dag_id)` — reverse lookup ("which submission produced this DAG").

#### Why three DagRun columns?

The submission has up to three DagRuns associated with it, each
load-bearing for a different reason; they are not interchangeable.

1. **Parsing DagRun** (`parsing_dag_run_id`) — the system DAG run
   that downloads, parses, registers, and triggers.
2. **Workload DagRun** (`workload_dag_run_id`) — the actual
   workload, created by `TriggerDagRunOperator` from the parsing DAG
   with `dag_run_conf` as its `conf`. Distinct from (1) because it
   runs user code with user parameters against the user's DAG.
3. **Cancellation DagRun** (`cancellation_dag_run_id`) — the system
   DAG run that performs cancellation. Its DagRun is the audit
   record: actor, time, steps taken, outcome.

Each column is nullable because the corresponding DagRun may not
exist yet (parse not started, trigger not fired) or may never exist
at all (a successful submission has no cancellation DagRun).

#### `DagModel` provenance marker

- New marker on `DagModel`
  (`airflow-core/src/airflow/models/dag.py`): column or sibling
  table; values `BUNDLE` (default) and `ADHOC` (`FR-11`). The
  default-query filter is applied at the query layer (`FR-13`).

#### Note: no separate `manifest` column

Earlier drafts proposed a `manifest` JSON column for client-side
metadata (bundled file list, declared requirements, queue, client
info). It is **not** included in the schema above. The roles a
`manifest` would have played are covered by:

- **Object-storage location** → `archive_uri`.
- **User-supplied DagRun input** → `dag_run_conf`, mirroring
  `DagRun.conf`.
- **Audit of who/when/what** → `created_by`, `created_at`, plus the
  `Log` rows from `FR-27`.
- **Diagnostics for the bundled archive** → recoverable from the
  archive itself while it exists, and from the parsing DagRun's task
  logs after the archive is gone.

If a future need (e.g. cheap pre-flight validation without unpacking
the archive) requires a structured client-side metadata blob, it can
be re-introduced as a dedicated column with a defined schema rather
than an open-ended JSON dump.

#### Migration

- Alembic migration in `airflow-core/src/airflow/migrations/versions/`
  for the new `submission` table, its FKs (one to `dag.dag_id`, three
  to `dag_run.id`), the `DagModel` provenance marker, and the indexes
  above.

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
- New endpoint
  `POST /execution/submissions/{submission_id}/archive_download_token`
  that mints a per-submission, short-lived presigned archive-download
  URL for the parsing system DAG (`FR-28`/`FR-29`/`FR-30`/`FR-31`,
  ADR 0003). Uses `get_fs()` against the object-store-backed
  `archive_uri`; no local-filesystem mode (`FR-5`).
- Per-task-instance JWT scoping extended (where needed) so a system
  DAG task can call the registration and token endpoints with its
  own TI token (FR-18).

### System DAG (orchestrator)

- New built-in DAG shipped with Airflow with three tasks: download
  archive (`get_fs()`), parse + POST serialised DAG to the new
  Execution API endpoint, `TriggerDagRunOperator` against the
  registered `dag_id`. Location TBD (open question in ADR 0002).

### System DAG (canceller)

- New built-in DAG shipped with Airflow that performs cancellation
  steps as tasks (`FR-7`, `FR-25`):
  1. Signal the user DagRun to terminate (no-op if not running).
  2. Mark the submission row `CANCELLED`.
  3. Delete the archive from object storage (subject to the retention
     policy from `FR-23`).
  4. Write the cancellation audit-log entry (`FR-27`).
- Triggered by the public REST `cancel` endpoint, which records the
  resulting DagRun's id on the submission's `cancellation_dag_run_id`.
- The DagRun of this DAG is the audit record of the cancellation;
  task logs cover each step. Failures (e.g. archive delete fails) are
  visible through the standard task-log endpoint, not silently
  swallowed.

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

### Auth-manager interface

- Extend the auth-manager interface
  (`airflow-core/src/airflow/api_fastapi/auth/managers/`) with a new
  `Submission` resource and the actions needed to gate it: at minimum
  `create`, `view`, and `cancel` (`FR-26`).
- Wire every public REST entry point under `/submissions` to consult
  the auth-manager. The default policy (a user can act on their own
  submissions only) is implemented inside the default auth-manager,
  not inline in the route handlers; deployments that use a custom
  auth-manager get the same hook surface as for existing resources.
- Expose the new resource and actions in the auth-manager's
  permissions enumeration so RBAC roles can grant cross-user view or
  cancel rights to platform-team identities.

### Audit log

- Emit `Log` entries
  (`airflow-core/src/airflow/models/log.py`) for the submission
  lifecycle (`FR-27`), at minimum:
  - `submission.create` — new row, archive uri minted.
  - `submission.upload_complete` — client confirmed upload.
  - `submission.run_triggered` — orchestrator system DAG dispatched.
  - `submission.cancel_requested` — canceller system DAG dispatched.
  - `submission.cancel_completed` — canceller finished (success or
    failure).
- Each entry carries `submission_id`, the actor (`owner`), and a
  small structured `extra` payload (e.g. `parsed_dag_id`,
  `parsing_dag_run_id`, archive size, failure reason).
- The orchestrator and canceller system DAGs emit their own audit
  entries from their tasks via the Execution API rather than writing
  `Log` rows directly, preserving the worker→API-server boundary
  (`FR-10`).

### UI (minimal)

- Provenance marker rendered on existing DAG / DagRun views so users
  can tell ad-hoc DAGs apart. A dedicated "Submissions" tab is a
  follow-up.

### Configuration

- New config keys (under a new `[ad_hoc]` section or equivalent):
  default object-store URI for archives, retention policy, default
  `PENDING_UPLOAD` expiry, optional per-user quota knobs.
- New config keys under `[api_auth]` for the archive-download
  capability (ADR 0003):
  - `submission_archive_download_jwt_secret` — signing secret for
    archive-download tokens. Defaults to an auto-generated value at
    server startup, matching the existing `jwt_secret` behaviour.
    Documented as separately rotatable from `jwt_secret`.
  - `submission_archive_download_jwt_expiration_time` — TTL in
    seconds for the URL returned by the archive-download endpoint.
    Default low (suggested 60–300 s) since the consumer is a task
    that is already running when it asks.
  - The signing algorithm reuses the existing `[api_auth]
    jwt_algorithm` setting, the same one used by every other JWT
    Airflow signs.

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
- Cancellation tests: cancel-while-pending (no user DagRun yet),
  cancel-while-running (user DagRun is active), cancel-after-success
  (idempotent no-op for the workload, archive cleanup still runs),
  and a test that asserts `cancellation_dag_run_id` is populated and
  the cancellation DagRun's logs include each step (`FR-7`,
  `FR-25`).
- Auth-manager tests: a non-owner user is rejected on view and
  cancel by default; a custom auth-manager that grants cross-user
  rights sees the same hooks fired (`FR-26`).
- Audit-log tests: each lifecycle event in `FR-27` produces exactly
  one `Log` row with the expected actor, event name, and payload
  shape.
- Archive-download token tests (`FR-28`/`FR-29`/`FR-30`/`FR-31`,
  ADR 0003):
  - Parsing DagRun task: token mint succeeds, returned token works
    against the archive endpoint, and `exp` matches the configured
    TTL.
  - Workload DagRun task: token mint endpoint returns `403`.
  - Cross-submission: a parsing DagRun task for submission A is
    refused when asking for submission B's token.
  - Expired token: download with an expired token is refused; a
    fresh mint succeeds.
  - Object-store path: with an `s3://` (or equivalent) `archive_uri`,
    the endpoint returns a presigned URL minted via `get_fs()` and
    the worker fetches directly from the object store.
  - Non-object-store rejection: a `Submission` whose `archive_uri`
    scheme is not object-store-backed (`file://`, etc.) is rejected
    at submission-create time with a clear error pointing at `FR-5`.
  - Log redaction: the parsing DagRun's task logs do not contain the
    returned presigned URL.
- Atomic-trigger tests (`FR-32`):
  - Happy path: triggering the orchestrator DAG leaves the
    `submission` row with `parsing_dag_run_id` populated and equal to
    the new DagRun's id, and the orchestrator DagRun's
    `conf["submission_id"]` matches the row.
  - Row-write failure rolls back the trigger (no orphaned DagRun
    whose authorization checks would refuse it).
  - A parsing-DAG task with a tampered `conf["submission_id"]`
    pointing at a different submission is refused by every
    submission-scoped Execution API endpoint.

### Newsfragment

- A `feature` newsfragment describing the new `airflow submit`
  workflow and the new REST surface.
