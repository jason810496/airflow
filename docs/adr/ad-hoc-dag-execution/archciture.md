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

**Table of contents**

- [Architecture: ad-hoc Dag execution](#architecture-ad-hoc-dag-execution)
  - [Status and scope](#status-and-scope)
  - [Layer 1: Metadata DB](#layer-1-metadata-db)
    - [`submission` table](#submission-table)
    - [`DagModel` provenance marker](#dagmodel-provenance-marker)
    - [Default-query filter](#default-query-filter)
    - [Alembic migration](#alembic-migration)
  - [Layer 2: Execution API](#layer-2-execution-api)
    - [`POST /execution/dags/parsed`](#post-executiondagsparsed)
    - [`POST /execution/submissions/{submission_id}/archive_download_token`](#post-executionsubmissionssubmission_idarchive_download_token)
    - [`GET /execution/submissions/{submission_id}`](#get-executionsubmissionssubmission_id)
    - [Authorization model](#authorization-model)
  - [Layer 3: Public REST API](#layer-3-public-rest-api)
    - [Routes](#routes)
    - [Schemas](#schemas)
    - [Atomic submission-to-DagRun linkage](#atomic-submission-to-dagrun-linkage)
  - [Layer 4: System Dags](#layer-4-system-dags)
    - [Orchestrator system Dag](#orchestrator-system-dag)
    - [Canceller system Dag](#canceller-system-dag)
    - [Shipping and versioning](#shipping-and-versioning)
  - [Layer 5: Task SDK and worker runtime](#layer-5-task-sdk-and-worker-runtime)
  - [Layer 6: CLI (`airflow submit`) and airflow-ctl](#layer-6-cli-airflow-submit-and-airflow-ctl)
  - [Layer 7: Auth-manager](#layer-7-auth-manager)
  - [Layer 8: Audit log](#layer-8-audit-log)
  - [Layer 9: Configuration](#layer-9-configuration)
  - [Layer 10: UI (minimal)](#layer-10-ui-minimal)
  - [End-to-end call graph](#end-to-end-call-graph)

# Architecture: ad-hoc Dag execution

Date: 2026-05-10

## Status and scope

This document is the implementation-side architectural spec for ad-hoc
Dag execution. It enumerates only the changes Airflow core has to
ship, organised bottom-up from the metadata DB through the worker
runtime to the user-facing CLI. Rationale, alternatives considered,
and the broader proposal live in:

- [AIP](./aip-ad-hoc-dag-execution.md)
- [ADR 0001 — feature scope](./0001-ad-hoc-dag-execution.md)
- [ADR 0002 — parsing/execution mechanism](./0002-parsing-and-execution-mechanism.md)
- [ADR 0003 — ephemeral archive-download token](./0003-ephemeral-archive-download-token.md)
- [Functional requirements](./requirements-and-changes.md)

Functional-requirement IDs (`FR-N`) used below are defined in the
requirements document.

## Layer 1: Metadata DB

### `submission` table

New ORM model in `airflow-core/src/airflow/models/submission.py`.
This row owns archive lifecycle and is the single source of truth for
every submission-scoped authorization check; the orchestrator carries
only `submission_id` in `DagRun.conf` and reads everything else from
this row via the Execution API (`FR-32`).

| Column | Type | Nullable | Notes |
| --- | --- | --- | --- |
| `id` | `UUID` | NO | Primary key. Surfaced by REST and CLI. |
| `created_by` | `str` (FK to auth user) | NO | Submitter identity (`FR-19`, `FR-20`). |
| `created_at` | `datetime` (UTC) | NO | Creation timestamp. |
| `state` | `Enum` | NO | `PENDING_UPLOAD`, `READY`, `RUNNING`, `SUCCESS`, `FAILED`, `CANCELLED` (`FR-6`). |
| `archive_uri` | `str` | YES | Object-store URI. Null between create and upload completion. Resolved via `get_fs()` (`FR-4`, `FR-5`). |
| `entry_file` | `str` | NO | Relative path inside the archive of the user-authored Dag file. |
| `dag_run_conf` | `JSON` | YES | Forwarded to the workload DagRun's `conf` (`FR-8`). |
| `parsed_dag_id` | `str` (FK to `dag.dag_id`, `ON DELETE SET NULL`) | YES | Set by the parse step. FK constraint: target row must have `provenance = ADHOC`. |
| `parsing_dag_run_id` | `UUID` (FK to `dag_run.id`) | YES | Orchestrator system Dag run (`FR-14`, `FR-21`). |
| `workload_dag_run_id` | `UUID` (FK to `dag_run.id`) | YES | User workload DagRun (`FR-15`, `FR-16`). |
| `cancellation_dag_run_id` | `UUID` (FK to `dag_run.id`) | YES | Canceller system Dag run (`FR-7`, `FR-25`). |

Indexes:

- `(created_by, state)` — per-user listing filtered by lifecycle state.
- `(state)` — global queries (e.g. expire stale `PENDING_UPLOAD` rows).
- `(parsed_dag_id)` — reverse lookup ("which submission produced this Dag").

The three nullable DagRun FKs are not interchangeable: each
corresponds to a distinct DagRun (parsing, workload, cancellation)
and is load-bearing for a different authorization check or audit
trail.

### `DagModel` provenance marker

`airflow-core/src/airflow/models/dag.py` gains a provenance column
(or sibling-table equivalent; placement deferred but the marker is
mandatory):

| Column | Type | Default | Notes |
| --- | --- | --- | --- |
| `provenance` | `Enum('BUNDLE', 'ADHOC')` | `BUNDLE` | `BUNDLE` for Dag-file-processor output; `ADHOC` for rows registered by the Execution API parsed-Dag endpoint (`FR-11`). |

A `dag_id` may have at most one row regardless of provenance.
Ad-hoc registration of a `dag_id` that already has a `BUNDLE` row is
rejected at the Execution API layer; ad-hoc-vs-ad-hoc collisions
return a structured indicator (`FR-12`).

### Default-query filter

A query-layer default predicate (`provenance = BUNDLE`) is applied
on every `DagModel` listing path that today implicitly assumes
bundle-parsed Dags: `airflow dags list`, the legacy listings, the
default UI Dag-list query, and metrics emitters (`FR-13`). Callers
that need the unfiltered set opt in via an explicit query parameter
or filter argument.

### Alembic migration

One Alembic migration in
`airflow-core/src/airflow/migrations/versions/` covers:

1. `submission` table with all columns and indexes above.
2. FKs from `submission` to `dag.dag_id` and three FKs to
   `dag_run.id`.
3. `DagModel.provenance` column with default `BUNDLE` for existing
   rows.
4. A check constraint (or equivalent) enforcing that
   `submission.parsed_dag_id` only references `DagModel` rows with
   `provenance = ADHOC`.

The downgrade path drops the table, the FKs, the provenance column,
and the constraint.

## Layer 2: Execution API

The worker never touches the metadata DB. Three new endpoints under
`airflow-core/src/airflow/api_fastapi/execution_api/` cover the
worker-side write and read paths the orchestrator system Dag needs
(`FR-10`, `FR-18`).

### `POST /execution/dags/parsed`

Worker submits a serialised parsed Dag. This is the **only** path by
which an `ADHOC`-provenance row enters the metadata DB.

Request body:

```json
{
  "submission_id": "<uuid>",
  "serialized_dag": { /* same shape SerializedDagModel stores today */ },
  "dag_id": "<dag_id authored by the user>"
}
```

Server behaviour:

1. Authenticate via the caller's per-TI JWT.
2. Authorize: the caller's TI must belong to a DagRun whose id equals
   `submission.parsing_dag_run_id` for the supplied `submission_id`
   (`FR-30`, `FR-32`).
3. Provenance check:
   - If a `DagModel` row exists for `dag_id` with
     `provenance = BUNDLE`, return `409 Conflict` with
     `{"reason": "bundle_dag_exists"}` (`FR-11`). A bundle Dag must
     never be shadowed by an ad-hoc submission.
   - If a `DagModel` row exists for `dag_id` with
     `provenance = ADHOC`, **register a new Dag version** under the
     same row rather than rejecting. Re-submitting the same `dag_id`
     (same author iterating, or two users coincidentally picking
     the same name) is a normal case; the existing Dag-version
     mechanism is the right place to disambiguate. The response
     carries the version id so the orchestrator can pin the workload
     trigger to it.
4. Otherwise insert the `DagModel` row with `provenance = ADHOC`,
   write the `SerializedDagModel` as version 1, and update
   `submission.parsed_dag_id` and `submission.state = READY` in one
   transaction.

Responses:

- `200 OK`
  `{"dag_id": "...", "parsed_dag_id": "...", "dag_version_id": "..."}`
  The `dag_version_id` is what the orchestrator's trigger task
  pins the workload DagRun to so the workload runs against the
  serialised Dag this submission produced, not whatever later
  submissions on the same `dag_id` overwrite.
- `403 Forbidden` if the authorization check fails.
- `409 Conflict` `{"reason": "bundle_dag_exists"}` only.

**Note on Dag-version propagation.** Pinning the workload DagRun to
the parsed version produced by this submission (rather than
"whatever is current for this `dag_id`") is load-bearing for
correctness when the same `dag_id` is submitted concurrently or
re-submitted later. The detailed mechanism (how `dag_version_id`
flows from this endpoint into `TriggerDagRunOperator` and onto the
workload DagRun row, how `Dag.get_latest_version()`-style helpers
are bypassed, and how the version stays visible in UI / logs for
the workload run) is the subject of a follow-up ADR. This
architecture doc only commits to "the endpoint returns a version
id, and the orchestrator's trigger task uses it to pin the workload
DagRun".

### `POST /execution/submissions/{submission_id}/archive_download_token`

Mints a short-lived presigned archive URL on demand (ADR 0003,
`FR-28`/`FR-29`/`FR-30`/`FR-31`).

Server behaviour:

1. Authenticate via the caller's per-TI JWT.
2. Authorize: caller's `dag_run_id == submission.parsing_dag_run_id`
   for the path's `submission_id`.
3. Mint a presigned URL via `get_fs().sign(submission.archive_uri,
   ...)` with TTL =
   `[api_auth] submission_archive_download_jwt_expiration_time`.
4. Return:

   ```json
   {
     "url": "<presigned download url>",
     "expires_at": "<RFC3339 timestamp>"
   }
   ```

The URL is never persisted on the row. Workload DagRun TIs and
cross-submission callers receive `403`. If `archive_uri`'s scheme
is not object-store-backed, return `400` referencing `FR-5`.

### `GET /execution/submissions/{submission_id}`

Returns the row fields the orchestrator system Dag's tasks need to
do their work without putting the values in `DagRun.conf`:
`archive_uri`, `entry_file`, `dag_run_conf`, `parsed_dag_id`,
`state`, `created_by`. Authorization is the same
`parsing_dag_run_id`-binding check; the canceller system Dag uses
the same endpoint with its own
`cancellation_dag_run_id`-binding variant.

### Authorization model

Every submission-scoped Execution API endpoint binds the caller's
`dag_run_id` to one of `submission.parsing_dag_run_id` or
`submission.cancellation_dag_run_id`. The workload DagRun is never
authorized against any of these endpoints. This makes a tampered
`conf["submission_id"]` useless: every endpoint refuses a caller
whose `dag_run_id` does not match the row.

## Layer 3: Public REST API

New routes module
`airflow-core/src/airflow/api_fastapi/core_api/routes/public/submissions.py`
and Pydantic schemas in
`airflow-core/src/airflow/api_fastapi/core_api/datamodels/submissions.py`.

### Routes

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/submissions` | Insert row in `PENDING_UPLOAD`, mint a presigned upload URL via `get_fs()`, return `{id, upload_url, upload_expires_at}`. |
| `POST` | `/submissions/{id}/uploaded` | Client confirms upload completion. Server transitions row to `READY` and emits `submission.upload_complete`. |
| `POST` | `/submissions/{id}/run` | Trigger the orchestrator system Dag. See [Atomic submission-to-DagRun linkage](#atomic-submission-to-dagrun-linkage). |
| `GET` | `/submissions/{id}` | Return submission state, the three DagRun ids, `parsed_dag_id`, timestamps. |
| `GET` | `/submissions` | List, filtered by the auth-manager. Supports `created_by`, `state`. |
| `POST` | `/submissions/{id}/cancel` | Trigger the canceller system Dag. Records `cancellation_dag_run_id`. |
| `DELETE` | `/submissions/{id}` | Hard-delete a terminal-state submission row and its archive (subject to retention; `FR-23`). |

Every entry point consults the auth-manager (`FR-26`) before
touching the row.

### Schemas

Pydantic models for the request/response shapes above. Notable
fields:

- `SubmissionCreateRequest`: `entry_file: str`, optional
  `dag_run_conf: dict | None`, optional `archive_uri: str | None`
  (when the deployment configures multiple object-store roots and
  the client picks one).
- `SubmissionCreateResponse`: `id: UUID`, `upload_url: HttpUrl`,
  `upload_expires_at: datetime`.
- `SubmissionRead`: `id`, `created_by`, `created_at`, `state`,
  `entry_file`, `parsed_dag_id`, `parsing_dag_run_id`,
  `workload_dag_run_id`, `cancellation_dag_run_id`. `archive_uri`
  is intentionally **not** in the read shape; archive bytes are
  fetched out-of-band by the worker via the Execution API token
  endpoint.

The OpenAPI spec is regenerated for the public client SDKs.

### Atomic submission-to-DagRun linkage

`POST /submissions/{id}/run` performs two writes that must succeed
or fail together (`FR-32`):

1. Trigger the orchestrator system DagRun with
   `dag_run.conf = {"submission_id": "<uuid>"}`.
2. `UPDATE submission SET parsing_dag_run_id = <new_dag_run_id>,
   state = 'RUNNING' WHERE id = <id>`.

Both writes happen in the same transaction (or via an existing
DagRun-create path that already records both, depending on which is
cheaper to wire; the contract is "no orphaned parsing DagRun whose
authorization checks would refuse it"). If the row write fails, the
DagRun is rolled back. The same atomicity contract applies to
`POST /submissions/{id}/cancel` and `cancellation_dag_run_id`.

## Layer 4: System Dags

Two built-in Dags ship with Airflow. They are parsed and stored like
any other Dag, run as normal DagRuns, and use the Execution API for
every metadata-DB-bound write (`FR-14`, `FR-25`).

### Orchestrator system Dag

`dag_id`: `airflow.system.adhoc.orchestrator` (exact id TBD; reserved
under the `airflow.system.` prefix). Two tasks:

1. **`download_parse_register`**

   - Calls `GET /execution/submissions/{submission_id}` to read
     `archive_uri` and `entry_file`.
   - Calls `POST /execution/submissions/{submission_id}/archive_download_token`
     for a short-lived presigned URL.
   - Streams the archive into a scratch directory on the worker,
     imports the entry file in-process using a parser helper that
     mirrors `DagBag.process_file()` without registering with
     `DagBundlesManager`, and serialises the resulting Dag (same
     path `SerializedDagModel.write_dag()` uses today, invoked as a
     pure-function helper with no DB write).
   - POSTs to `POST /execution/dags/parsed`. On the only remaining
     `409` (`bundle_dag_exists`), fails with a surfaceable error.
   - Pushes only the returned `dag_version_id` to XCom for the next
     task. The archive-download URL and the archive bytes stay in
     this task's process memory and on the local scratch path; they
     are **never** put on XCom.
   - Logs the HTTP status and byte count; **never logs the URL**
     (`FR-31`).

   The download, parse, and register steps live in a single task
   precisely so the presigned URL and the on-disk archive path do
   not have to cross the task boundary. Splitting them would force
   the URL through XCom (or the row), defeating the "never persist
   the credential" property in ADR 0003.

2. **`trigger_user_dag`**

   - `TriggerDagRunOperator(trigger_dag_id=parsed_dag_id,
     conf=dag_run_conf, dag_version_id=<from XCom>)`.
   - The `dag_version_id` pin is what keeps the workload DagRun
     bound to the Dag version this submission produced, even if a
     later submission registers a newer version under the same
     `dag_id`. The exact propagation mechanism is the subject of a
     follow-up ADR (see the note under
     [`POST /execution/dags/parsed`](#post-executiondagsparsed)).
   - Records the workload DagRun's id back onto the row by calling a
     small "set workload run" Execution API helper (or by relying on
     the trigger path to do it; the contract is that
     `submission.workload_dag_run_id` is set before this task
     completes).

The DagRun produced by `TriggerDagRunOperator` is
`DagRunType.OPERATOR_TRIGGERED` (`FR-16`); no new `DagRunType` is
introduced.

### Canceller system Dag

`dag_id`: `airflow.system.adhoc.canceller`. Tasks:

1. **`signal_workload`**: signal `workload_dag_run_id` to terminate
   if it is in a non-terminal state. Idempotent no-op otherwise.
2. **`mark_cancelled`**: transition `submission.state = CANCELLED`
   via the Execution API.
3. **`delete_archive`**: `get_fs().rm(submission.archive_uri)`,
   subject to the deployment's retention policy (`FR-23`).
4. **`audit`**: emit `submission.cancel_completed` (`FR-27`).

The cancellation DagRun is the audit record; task logs cover each
step (`FR-25`).

### Shipping and versioning

The two system Dags live under
`task-sdk/src/airflow/sdk/system_dags/` and are exposed as
`airflow.sdk.system_dags.adhoc.orchestrator` and
`airflow.sdk.system_dags.adhoc.canceller`. They are packaged as a
separate distribution, **`apache-airflow-system-dags`**, so:

- Core stays lean and `providers/` is not overwhelmed with a
  distribution that isn't a provider.
- Deployments install the system Dags by adding
  `apache-airflow-system-dags` to their environment (it is a hard
  dependency for any deployment that enables ad-hoc submission, and
  the public REST `/submissions/{id}/run` endpoint surfaces a clear
  error if it is not installed).
- The Dags are loaded through a dedicated "system" Dag-bundle entry
  that the deployment cannot delete. The bundle resolves the Dag
  files by importing `airflow.sdk.system_dags.adhoc` and reading
  the Dag objects from there, rather than scanning a filesystem
  path, so the bundle does not race with user bundles on the
  Dag-files mount.

The distribution is versioned independently of `airflow-core` but
must stay compatible with the Execution API contracts in this doc;
its release cadence is pinned to Airflow minor releases. Drift risk
(`SerializedDagModel` schema, Execution API endpoint shapes) is
covered by integration tests that run the system Dags against the
in-tree Execution API.

## Layer 5: Task SDK and worker runtime

- **Parser helper** in
  `task-sdk/src/airflow/sdk/execution_time/` (or a sibling module
  under the system-Dag implementation) that imports an entry file
  in-process and returns a serialisable Dag. Mirrors
  `DagBag.process_file()`'s parsing path without registering with
  `DagBundlesManager`. Heavy provider-only imports stay behind
  `TYPE_CHECKING` per the project coding standards.
- **`get_fs()` reuse**: download and cancellation cleanup go through
  the existing fsspec abstraction
  (`task-sdk/src/airflow/sdk/io/fs.py`). No new transport.
- **No bootstrap hook in `task_runner.py`** is required: parsing
  happens inside the orchestrator system Dag's task code, not in the
  generic worker bootstrap. The worker runtime is unchanged for
  workload DagRuns.

## Layer 6: CLI (`airflow submit`) and airflow-ctl

- New command `airflow submit <entry_file>` in
  `airflow-core/src/airflow/cli/` with flags:

  | Flag | Default | Purpose |
  | --- | --- | --- |
  | `--conf <json>` (`-c` short form) | none | JSON object forwarded as the workload DagRun's `conf` (`FR-8`). Mirrors `airflow dags trigger --conf` so users hit the same shape they already know. |
  | `--include path` (repeatable) | none | Add files the static walker missed (`FR-3`). |
  | `--watch` / `--no-watch` | `--watch` | Stream user DagRun task logs and exit non-zero on failure (`FR-9`). |

- **Local dependency discovery** utility: `ast` walker over the
  entry file's imports plus `importlib.util.find_spec` resolution.
  Modules resolving to a path under the entry file's project root
  are bundled; modules resolving to site-packages are assumed to be
  installed on the worker (`FR-2`).
- **Archive packing**: zip of entry file + discovered local modules
  + `--include` paths. Streamed to the presigned upload URL minted
  by `POST /submissions`.
- **airflow-ctl**: a new `airflowctl submissions` subcommand group
  that consumes the public REST endpoints. No bespoke transport
  (`FR-24`).

## Layer 7: Auth-manager

`airflow-core/src/airflow/api_fastapi/auth/managers/` gains a new
resource and action set (`FR-26`):

- Resource: `Submission`.
- Actions: `create`, `view`, `cancel`.
- Default policy (in the default auth-manager): a user can act on
  their own submissions only; cross-user access requires an explicit
  RBAC grant.
- Every public REST entry point under `/submissions` consults the
  auth-manager rather than checking ownership inline. Custom
  auth-managers see the same hooks.
- The new resource and actions appear in the auth-manager's
  permissions enumeration so platform-team roles can grant cross-user
  view or cancel rights.

## Layer 8: Audit log

Lifecycle events emitted into the existing `Log` table
(`airflow-core/src/airflow/models/log.py`) with `submission_id` as
the subject (`FR-27`):

| Event | Emitted by | `extra` payload |
| --- | --- | --- |
| `submission.create` | Public REST | archive scheme. |
| `submission.upload_complete` | Public REST | archive size. |
| `submission.run_triggered` | Public REST | `parsing_dag_run_id`. |
| `submission.cancel_requested` | Public REST | `cancellation_dag_run_id`. |
| `submission.cancel_completed` | Canceller system Dag | success/failure, archive-delete result. |

The orchestrator and canceller system Dags emit their audit entries
via an Execution API helper, never by writing `Log` rows directly
(`FR-10`).

## Layer 9: Configuration

New keys (additive; no existing key changes meaning):

Under a new `[ad_hoc]` section:

- `default_archive_uri` — base object-store URI under which
  per-submission archives are written. No default; ad-hoc submission
  is disabled until set (`FR-5`).
- `archive_retention_policy` — one of `delete_on_success`,
  `delete_on_terminal`, `keep` (`FR-23`).
- `pending_upload_expiration_time` — seconds before a
  `PENDING_UPLOAD` row is reaped if no upload arrives.

Under the existing `[api_auth]` section (parallel to
`jwt_secret`/`jwt_expiration_time`; ADR 0003):

- `submission_archive_download_jwt_secret` — signing secret for
  archive-download tokens. Auto-generated at API-server startup if
  unset, matching `jwt_secret` behaviour. Separately rotatable.
- `submission_archive_download_jwt_expiration_time` — TTL in seconds
  for the URL returned by the archive-download endpoint. Default
  low (suggested 60–300 s).

The signing algorithm reuses the existing `[api_auth] jwt_algorithm`.

## Layer 10: UI (minimal)

- Render the `provenance` marker on existing Dag and DagRun views so
  ad-hoc rows are distinguishable from bundle rows when they appear.
- Default Dag-list views keep the `provenance = BUNDLE` filter; an
  opt-in toggle reveals ad-hoc rows.
- A dedicated "Submissions" tab is **out of scope** for this
  landing.

## End-to-end call graph

```
[CLI]                                      [Public REST]                    [Object store]                 [Orchestrator system Dag]                 [Execution API]                 [User workload DagRun]
  |                                              |                                |                                      |                                  |                                  |
  | POST /submissions  (entry_file, conf)        |                                |                                      |                                  |                                  |
  |--------------------------------------------->| INSERT submission              |                                      |                                  |                                  |
  |                                              | (PENDING_UPLOAD)               |                                      |                                  |                                  |
  |                                              | mint presigned PUT             |                                      |                                  |                                  |
  |<--------- {id, upload_url} ------------------|                                |                                      |                                  |                                  |
  |                                              |                                |                                      |                                  |                                  |
  | PUT upload_url (zip) ----------------------------------------------> [stored] |                                      |                                  |                                  |
  |                                              |                                |                                      |                                  |                                  |
  | POST /submissions/{id}/uploaded              |                                |                                      |                                  |                                  |
  |--------------------------------------------->| state=READY                    |                                      |                                  |                                  |
  |                                              |                                |                                      |                                  |                                  |
  | POST /submissions/{id}/run                   |                                |                                      |                                  |                                  |
  |--------------------------------------------->| TX: trigger orch DagRun        |                                      |                                  |                                  |
  |                                              |     SET parsing_dag_run_id     |                                      |                                  |                                  |
  |                                              |---------------------------------------------------------------------->|                                  |                                  |
  |                                              |                                |                                      | t1: GET /execution/submissions/{id}                                 |
  |                                              |                                |                                      | t1: POST .../archive_download_token --> mint presigned GET          |
  |                                              |                                |                                      | t1: GET presigned --> bytes (in-memory + scratch path, no XCom)     |
  |                                              |                                |                                      | t1: parse + serialise (in-process)                                  |
  |                                              |                                |                                      | t1: POST /execution/dags/parsed    |                                |
  |                                              |                                |                                      |     (BUNDLE shadow check only;     | INSERT or new-version DagModel |
  |                                              |                                |                                      |      ADHOC re-submit -> new version)| (ADHOC) + SerializedDagModel; |
  |                                              |                                |                                      |     XCom push: dag_version_id      | SET submission.parsed_dag_id,  |
  |                                              |                                |                                      |                                    | state=READY                    |
  |                                              |                                |                                      | t2: TriggerDagRunOperator ------------------------------------> dispatch workload |
  |                                              |                                |                                      |     (pinned to dag_version_id)     |                                  |
  |                                              |                                |                                      |     SET workload_dag_run_id        |                                  |---> run user tasks
  |                                              |                                |                                      |                                                                       |
  | (poll GET /submissions/{id} + stream task logs of workload DagRun)            |                                      |                                                                       |
  |<==============================================================================================================================================================================================|
```

The cancellation flow is parallel: `POST /submissions/{id}/cancel`
atomically triggers the canceller system Dag and records
`cancellation_dag_run_id`; the canceller's tasks signal the workload,
mark the row `CANCELLED`, and delete the archive, all via the
Execution API and `get_fs()`.
