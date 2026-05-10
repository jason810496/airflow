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

- [3. Ephemeral archive-download token for ad-hoc submissions](#3-ephemeral-archive-download-token-for-ad-hoc-submissions)
  - [Status](#status)
  - [Context](#context)
  - [Decision](#decision)
    - [Scope of this ADR](#scope-of-this-adr)
    - [Core decision: mint on demand via the Execution API](#core-decision-mint-on-demand-via-the-execution-api)
    - [Authorization properties](#authorization-properties)
    - [Archive download URL](#archive-download-url)
    - [Configuration](#configuration)
  - [Alternatives considered](#alternatives-considered)
  - [Consequences](#consequences)
  - [Resolved questions](#resolved-questions)

# 3. Ephemeral archive-download token for ad-hoc submissions

Date: 2026-05-10

## Status

Proposed. Builds on
[1. Ad-hoc DAG execution](./0001-ad-hoc-dag-execution.md) and
[2. Parsing and execution mechanism](./0002-parsing-and-execution-mechanism.md).

## Context

ADR 0002 introduces an orchestrator "system DAG" that performs
download → parse → register → trigger for each ad-hoc submission. Its
first task fetches the user-uploaded archive from object storage —
the only supported archive backend (see ADR 0001 / `FR-5`). That
fetch needs *some* authority: the archive is per-user content and
must not be readable by arbitrary tasks on the cluster.

Three constraints make the existing credentials unsuitable:

- **The per-task-instance JWT is too broad.** It grants whatever
  task-side capabilities the worker already has. Reusing it as the
  download credential means any task on the cluster could call the
  archive endpoint and read any archive.
- **Storing a long-lived token on the `Submission` row leaks
  capabilities at rest.** The token would live for as long as the
  row, even though the only consumer is one task during a few seconds
  of the parsing window.
- **The workload DagRun must not gain archive-download capability.**
  Once the parse step has produced the registered DAG, the workload
  has no reason to redownload the archive. Any design that hands a
  reusable token to the workload widens the blast radius.

We need a credential that is **scoped to one submission's archive**,
**time-bounded to the parsing window**, **reachable by the parsing
DagRun's download task**, and **not reachable by the workload DagRun**.

## Decision

### Scope of this ADR

This ADR commits to:

- The archive-download capability is **minted on demand** by a new
  Execution API endpoint, not stored on the `Submission` row.
- Two new configuration keys (signing secret, expiration time) govern
  the token.
- The archive itself is fetched as a presigned URL minted by
  `get_fs()` against the object-store-backed `archive_uri`.
  **Airflow does not host an archive bytes endpoint** and does not
  proxy archive bytes for any deployment shape.

It does **not** commit to: exact JWT claim names, the token's exact
wire format, or whether the canceller eventually needs its own
delete credential. Those are implementation details for the follow-up
PRs.

### Core decision: mint on demand via the Execution API

The parsing system DAG's download task calls a new Execution API
endpoint with its existing per-TI JWT:

```
POST /execution/submissions/{submission_id}/archive_download_token
```

The endpoint:

1. Authenticates the caller via the existing per-TI JWT.
2. Authorizes by checking that the caller's TI belongs to a parsing
   system DAG run linked to `submission_id` — i.e.
   `submission.parsing_dag_run_id == caller's dag_run_id`. Any other
   caller (a workload DagRun task, a different parsing DagRun, an
   unrelated bundle DAG task) is rejected with `403`.
3. Mints a fresh, short-lived presigned URL via `get_fs()` against
   the row's `archive_uri`, with TTL =
   `submission_archive_download_jwt_expiration_time`. The object
   store enforces the scope and expiry; Airflow does not sit in the
   data path.
4. Returns the URL and its expiry in the response. The task uses the
   URL once to fetch the archive, then discards it.

The URL is **never persisted**. It exists only in the endpoint's
response and in the task's transient memory while it streams the
archive.

### Authorization properties

- **Workload DagRun cannot mint.** Its TI JWT is tied to
  `submission.workload_dag_run_id`, not `submission.parsing_dag_run_id`,
  so the endpoint's `parsing_dag_run_id` check refuses it.
- **Cross-submission requests are refused.** The check binds the
  caller's `dag_run_id` to one specific submission row; a parsing
  DagRun for submission *A* cannot mint a token for submission *B*.
- **Replay window is bounded.** The TTL is intended to be short
  (seconds, not minutes), because the only consumer is a task that is
  already running when it asks. A token that leaks expires before it
  is useful.
- **Retry-safe.** If the parsing task retries after a token expires,
  it just calls the endpoint again. No re-mint-and-overwrite logic
  on the `Submission` row, no risk of stale long-lived tokens.

### Archive download URL

The archive bytes are always served by the object store — never by
Airflow. Two consequences:

- **No public archive endpoint to maintain.** The submission REST
  surface (`POST /submissions/...`) handles control-plane operations;
  the archive bytes are out-of-band, fetched directly from object
  storage by the worker.
- **The Execution API endpoint's authorization is the gate.** It
  binds the caller's parsing DagRun to one submission, then mints a
  short-lived presigned URL the object store will validate.

### Configuration

Two new keys under `[api_auth]`, parallel in style to the existing
`jwt_secret` and `jwt_expiration_time`:

- **`[api_auth] submission_archive_download_jwt_secret`** — signing
  secret for archive-download tokens. If unset, generated at API
  server startup, matching the behaviour of the existing `jwt_secret`.
- **`[api_auth] submission_archive_download_jwt_expiration_time`** —
  TTL in seconds for the URL returned by the Execution API endpoint.
  Default low (suggested range: 60–300 seconds), since the consumer
  is a task that is already running when it asks.

The signing algorithm is **not** a separate decision: it is
controlled by the existing `[api_auth] jwt_algorithm` config, the
same one used by every other JWT Airflow signs. Symmetric vs
asymmetric is therefore a deployment choice, not an ADR-level one.

Two reasons not to share the existing `[api_auth] jwt_secret`:

- **Rotation policies differ.** Archive-download tokens are short-
  lived and can be rotated aggressively without disrupting other
  flows.
- **Blast radius.** A leak of `submission_archive_download_jwt_secret`
  exposes only archive downloads, not the rest of the API surface.

## Alternatives considered

- **Store the token on the `Submission` row.** Rejected: long-lived
  credential at rest in the metadata DB, weaker scoping (would have
  to be enforced by an extra check anyway), and re-mint-on-retry
  forces overwrites on a row that is otherwise stable. The single
  benefit is "fewer endpoints", which does not pay for the cost.
- **Pass the token in `DagRun.conf`.** Rejected: `DagRun.conf` is
  surfaced in the UI, included in audit-log payloads, and frequently
  copied into XCom/templating contexts. Storing a credential there
  is a leak risk.
- **Reuse the per-task-instance JWT directly.** Rejected: it is not
  scoped to an archive or a submission. Granting it download rights
  effectively grants every task on the cluster the same rights.
- **Host an Airflow-served archive endpoint.** Validate the token at
  a public REST or Execution API endpoint that streams the archive
  bytes. Rejected: it puts Airflow in the data path for archives that
  the object store can serve directly, and adds a public surface
  whose only purpose is bytes-shuffling.

## Consequences

Positive:

- **No credential at rest.** The signing secret stays on the API
  server; the token exists only in flight.
- **Tight scoping.** One submission, one DagRun, one short window.
- **Workload DagRun naturally excluded.** The authorization check
  rules it out; no extra "is this a workload run?" logic in the
  download task.
- **Retry-safe by construction.** The endpoint is idempotent from the
  caller's perspective.

Negative / costs:

- **One additional Execution API endpoint** to design, document, and
  maintain.
- **One additional JWT secret** in `[api_auth]`. Rotation,
  documentation, and Helm-chart secret wiring all have to be updated.
- **Object-store dependency.** The feature requires a working object
  store with presigned-URL support. Deployments that do not have one
  (purely-local setups, `breeze` defaults) cannot use ad-hoc
  submission until they configure an object-store backend.

Risks to watch:

- **Clock skew between API server and worker.** A 60-second TTL
  leaves little margin. The implementation should set a
  conservative-but-not-tiny default and document the dependency on
  cluster time sync.
- **Token leakage via task logs.** The download task must not log
  the token it received. A test should assert this.

## Resolved questions

1. **Signing algorithm.** Controlled by the existing
   `[api_auth] jwt_algorithm` config, the same one used by every
   other JWT Airflow signs. Symmetric vs asymmetric is a deployment
   choice, not an ADR-level decision.
2. **Where the archive endpoint lives.** Nowhere in Airflow. The
   archive backend is always an object store (see ADR 0001 / `FR-5`),
   and bytes are served directly by it via a presigned URL minted by
   `get_fs()`. The Execution API endpoint's `parsing_dag_run_id`
   check is the only Airflow-side access gate.
3. **Canceller archive-delete credential.** Not needed. The
   canceller system DAG (ADR 0002) performs archive cleanup through
   the API server's own credentials via `get_fs()`; no parallel
   ephemeral-token mechanism is required for deletion.
