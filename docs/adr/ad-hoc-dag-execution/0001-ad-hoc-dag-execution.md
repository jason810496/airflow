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

- [1. Ad-hoc DAG execution](#1-ad-hoc-dag-execution)
  - [Status](#status)
  - [Context](#context)
    - [Motivating use case](#motivating-use-case)
    - [Why existing primitives are insufficient](#why-existing-primitives-are-insufficient)
  - [Decision](#decision)
    - [Goals](#goals)
    - [Non-goals](#non-goals)
    - [User-facing interface](#user-facing-interface)
    - [Submission lifecycle](#submission-lifecycle)
    - [Component changes](#component-changes)
    - [Data model](#data-model)
    - [Authentication and isolation](#authentication-and-isolation)
    - [Staging plan](#staging-plan)
  - [Alternatives considered](#alternatives-considered)
  - [Consequences](#consequences)
  - [Open questions](#open-questions)
  - [Note on dag_id collisions between users](#note-on-dag_id-collisions-between-users)

# 1. Ad-hoc DAG execution

Date: 2026-05-03

## Status

Proposed.

## Context

### Motivating use case

Airflow today is optimised for DAGs that live in a bundle (Git, local
filesystem, or a custom `BaseDagBundle` implementation) and are parsed by
the DAG file processor before any run can be created. That model fits
production scheduling, where DAGs are reviewed, deployed, and run on a
cadence. It does not fit a class of workflows that show up repeatedly on
the user side, particularly in ML and data-science workflows:

- A user has a pipeline checked out on their laptop and wants to run it
  once on a remote cluster (commonly because the cluster has GPUs, more
  memory, or access to data the laptop does not).
- They want a tight feedback loop: edit locally, submit, watch logs,
  iterate. Going through "commit, push, wait for bundle refresh,
  trigger, watch" is too slow for experimentation.
- They expect a `spark-submit`-style command. Submit a script and its
  dependencies, get a run, get logs, get an exit status.
  Ray, SkyPilot, and Slurm all expose this shape. Airflow does not.

<!-- This came up explicitly in the SkyPilot Slack as a gap users hit when
they want Airflow to host the orchestration layer of an ML pipeline but
keep the development loop local. -->

### Why existing primitives are insufficient

- `airflow dags test` runs the DAG **locally**, in-process with the CLI
  (see `airflow-core/src/airflow/cli/commands/dag_command.py:667`). It
  does not exercise the remote workers, scheduler, or executor, so it
  cannot validate behaviour on the cluster the user actually wants to
  use.
- `airflow dags trigger` (`dag_command.py:75`) requires the DAG to
  already exist in a bundle and be parsed. It triggers a `MANUAL` run
  against an already-known `dag_id`. There is no path to ship a DAG
  file from the local machine to the cluster.
- `BaseDagBundle` (`airflow-core/src/airflow/dag_processing/bundles/base.py:232`)
  and `DagBundleModel` (`airflow-core/src/airflow/models/dagbundle.py:31`)
  are designed around named, versioned, refreshable sources. Adding a
  bundle for every ad-hoc submission would pollute the bundle namespace
  and force the DAG processor and scheduler to track ephemeral state
  they should not own.
- The DAG processor itself only parses files from registered bundles
  (`DagBag.process_file()` at
  `airflow-core/src/airflow/dag_processing/dagbag.py:281`). One-shot
  parsing of a user-supplied archive is not a concept it currently
  exposes.

### Existing patterns we can reuse

- **TestConnection** (`airflow-core/src/airflow/api_fastapi/core_api/routes/public/connections.py:223`)
  shows a precedent for an endpoint that accepts a transient payload,
  uses Airflow internals to act on it, and returns a structured result
  without committing long-lived schedule state. The interaction is
  stateless from the scheduler's perspective.
- **`get_fs()`** in the Task SDK
  (`task-sdk/src/airflow/sdk/io/fs.py:74`) provides an fsspec-backed,
  provider-pluggable abstraction over object storage. We can ship an
  archive over any scheme the deployment already has a connection for
  (`s3`, `gs`, `azure`, `file`) without baking provider choice into
  core.
- **`DagRunType.OPERATOR_TRIGGERED`**
  (`airflow-core/src/airflow/utils/types.py:25`) already exists for
  runs created by `TriggerDagRunOperator`. The ad-hoc path naturally
  inherits this run type via the system DAG defined in ADR 0002, so
  no new `DagRunType` value is needed; provenance on `DagModel` is
  what distinguishes ad-hoc DAGs from bundle DAGs.
- **airflow-ctl** is by design REST-only
  (`airflow-ctl/src/airflowctl/api/client.py`), which means a new
  REST endpoint is automatically usable from the management CLI
  without bespoke transport.

## Decision

We will introduce a first-class **ad-hoc DAG execution** path. A user
can package a DAG file (with optional supporting modules) on their
local machine, submit it to the API server, and have a worker download,
parse, and execute it as a one-shot run. The submission is tracked as a
distinct first-class entity, not as a permanent DAG.

### Goals

1. `spark-submit`-style workflow: one command to ship local code and
   get a run.
2. Reuse the existing executor and worker plumbing. Workers run the
   submitted DAG through the same Task SDK runtime they use today.
3. No scheduler awareness of ad-hoc submissions. The scheduler does
   not see submissions as DAGs to schedule, only as runs to dispatch.
4. Pluggable transport. The archive can land in any object store
   reachable via a configured connection, using `get_fs()`.
5. Observable. The submission shows up in the UI and API with logs,
   status, and a stable submission ID.

### Non-goals

- Replacing the DAG bundle model for production DAGs.
- Cross-submission scheduling, dependencies, or retries beyond the
  single one-shot run.
- Mutating or "promoting" a submission into a permanent DAG. (A user
  who likes their experiment commits it to a bundle the normal way.)
- Multi-tenant isolation guarantees beyond what Airflow's existing
  worker model already provides. Submissions inherit the same trust
  model as DAGs in a bundle.

### User-facing interface

Three surfaces, layered:

1. **REST endpoint** under the public API:

   ```
   POST   /submissions          # create submission, returns presigned upload + submission_id
   PUT    <presigned-url>       # client uploads the archive directly to object storage
   POST   /submissions/{id}/run # finalise: schema check, trigger one-shot run
   GET    /submissions/{id}     # status, logs link, run id
   GET    /submissions          # list
   DELETE /submissions/{id}     # cancel and clean up archive
   ```

   This is the source of truth. Everything else is a client.

2. **CLI in airflow-ctl**: `airflowctl submissions submit <path>`,
   `airflowctl submissions logs <id>`, etc. Because airflow-ctl only
   speaks REST, no new transport is needed.

3. **`airflow submit <dag-file>` shortcut** in the Task SDK / CLI for
   familiarity with the `spark-submit` convention. This is sugar over
   the airflow-ctl path, packaged for users who already type `airflow`.

### Submission lifecycle

```
[client]                      [api server]                [object store]      [executor]      [worker]
   |                                |                            |                |               |
   | POST /submissions              |                            |                |               |
   | (manifest: entry file,         |                            |                |               |
   |  module list, requirements)    |                            |                |               |
   |------------------------------->|                            |                |               |
   |                                | INSERT submission row      |                |               |
   |                                | (state=PENDING_UPLOAD)     |                |               |
   |                                | mint presigned upload URL  |                |               |
   |<------ {id, upload_url} -------|                            |                |               |
   |                                |                            |                |               |
   | PUT upload_url (zip)           |                            |                |               |
   |------------------------------------------------------------>|                |               |
   |                                |                            |                |               |
   | POST /submissions/{id}/run     |                            |                |               |
   |------------------------------->|                            |                |               |
   |                                | parse archive metadata     |                |               |
   |                                | (cheap: read manifest,     |                |               |
   |                                |  not user code)            |                |               |
   |                                | INSERT dag_run             |                |               |
   |                                |   (linked to submission)   |                |               |
   |                                | enqueue task instances     |                |               |
   |                                |--------------------------->| dispatch       |               |
   |                                |                            |                |-------------->|
   |                                |                            |                |               | download archive
   |                                |                            |                |               | parse DAG in-process
   |                                |                            |                |               | run task via Task SDK
   |                                |                            |                |               |   (task_runner.py)
   |                                |                            |                |<--------------|
```

The key inversion versus today is that **the worker, not the DAG file
processor, is responsible for parsing the submitted archive**. This
mirrors the architecture boundary already documented in the project's
`CLAUDE.md`: workers run user code, the scheduler does not. We do not
want to push ad-hoc parsing into the scheduler-side DAG processor
because (a) the DAG processor's contract is "owned, versioned bundles",
and (b) parsing a one-shot archive on the scheduler side just to
serialise it for the worker is wasted work for a run that will execute
exactly once.

### Component changes

1. **New `Submission` model** in `airflow-core/src/airflow/models/`.
   Tracks: `id` (UUID), `created_by`, `created_at`, `state`
   (`PENDING_UPLOAD`, `READY`, `RUNNING`, `SUCCESS`, `FAILED`,
   `CANCELLED`), `archive_uri` (object-store URI), `entry_file`,
   `manifest` (JSON: dependency list, parameters, requested executor
   queue), `dag_run_id` (FK once dispatched). Indexed on
   `created_by` and `state` for listing.

2. **New REST routes** under
   `airflow-core/src/airflow/api_fastapi/core_api/routes/public/submissions.py`,
   following the same shape as `connections.py`. Schemas live next to
   existing ones in `core_api/datamodels/`.

3. **New `AdHocDagBundle`** subclass of `BaseDagBundle` (or a sibling
   abstraction; see Open Questions). It is constructed per-submission
   from the archive URI, exposes the unpacked archive at a worker-local
   path, and is **not** registered with `DagBundlesManager`. The Task
   SDK's existing parser is invoked against the unpacked entry file
   inside the worker process, before the operator's `execute()` runs.

4. **Object storage integration** uses `get_fs()` against an
   object-store-backed `archive_uri` (`s3`, `gs`, `azure`, or any
   fsspec scheme that supports presigned URLs). Object storage is
   the **only** supported archive backend; the API server does not
   proxy archive bytes and there is no local-filesystem fallback.
   Deployments that do not have an object store configured cannot
   use ad-hoc submission.

5. **Worker-side runtime hook** in
   `task-sdk/src/airflow/sdk/execution_time/task_runner.py`'s task
   bootstrap: if the run carries an `ad_hoc_submission_id`, fetch the
   submission record via the Execution API, download the archive,
   unpack into a scratch directory, parse the entry file, and proceed
   with execution against the resulting DAG object.

6. **No scheduler change.** The user DAG run is created by
   `TriggerDagRunOperator` from the system DAG (ADR 0002), so it is
   indistinguishable to the scheduler from any other operator-triggered
   run. The provenance marker on `DagModel` lets components that care
   filter ad-hoc DAGs out of default views.

### Data model

Two viable shapes; we choose (b) and explain why.

(a) **Reuse `DagModel`** with a `is_ad_hoc` flag and synthetic `dag_id`
per submission. Pro: minimal new tables. Con: pollutes
`dag` listings, breaks assumptions across the codebase that
`DagModel` rows are long-lived and bundle-backed, and forces the DAG
processor to either ignore or special-case these rows everywhere.

(b) **New `Submission` table linked to `DagRun`** via `submission_id`.
The submission owns the archive lifecycle; the run owns task-instance
state. The DAG row is **transient** and either (i) created
just-in-time as a non-bundle-backed `DagModel` with `is_ad_hoc=True`
and excluded from default queries, or (ii) elided entirely with the
run keyed off the submission. We propose (i) for the first cut to
reduce blast radius on code that joins through `DagModel`. Excluded
from default queries is enforced via a default `where is_ad_hoc =
False` filter at the query layer, similar to how soft-deleted rows
are typically handled.

This mirrors the TestConnection precedent in spirit (transient,
purpose-built record) while accepting that submissions, unlike
connection tests, must persist long enough to drive a real run and
serve logs after the fact.

### Authentication and isolation

- Submissions require an authenticated user. The submission row stores
  `created_by`. The default policy: a user can only see and cancel
  their own submissions. RBAC for cross-user visibility is a follow-up.
- The archive lives under an object-store prefix scoped to the
  submission ID. Presigned URLs are submission-scoped and short-lived.
- Workers receive a JWT scoped to the task instance, the same as
  every other run. They additionally receive permission via the
  Execution API to read the submission row and download the archive.
  No new credential surface.
- **Trust model is unchanged**: the submitted code runs on the worker
  with the worker's identity. This is the same trust model as a
  bundle-backed DAG. Deployment Managers who want stronger isolation
  for untrusted submissions are pointed at the existing multi-team and
  per-pool isolation guidance.

### Staging plan

1. **PoC in a provider** (e.g., a new `apache-airflow-providers-submit`
   distribution under `providers/`). Lets us iterate on the worker-side
   archive handling and the CLI shape without committing core to the
   data model. The PoC depends on `apache-airflow` and registers a CLI
   plugin and a small REST extension.
2. **Promote the data model into core** once the lifecycle is stable:
   `Submission` table, `DagModel` provenance marker, public REST
   endpoints, airflow-ctl commands.
3. **Polish**: UI surface (a "Submissions" tab parallel to "DAGs"),
   structured logs link, retention policy for archives.

## Alternatives considered

- **Just commit and trigger.** Force users to commit to a Git bundle
  and trigger normally. Rejected for the motivating use case: the
  feedback loop is too slow, and "commit to experiment" is a poor fit
  for ML iteration where most attempts are throwaway.
- **Special bundle that points at a user-supplied URI.** A bundle
  type that accepts an arbitrary archive URI per refresh. Rejected
  because bundles are designed around stable identity (a bundle's
  contents change over time, but the bundle exists across runs). An
  ad-hoc submission has identity for one run, then is done. Forcing it
  through the bundle abstraction either bloats `DagBundleModel` with
  ephemeral rows or invents a synthetic bundle name per submission.
- **Make `airflow dags test` remote.** Add a `--executor=remote` flag
  to `dags test`. Rejected because `dags test` runs in-process with
  the CLI by design, and inverting that to "ship the file and run on
  a worker" is the same feature, just hidden behind an existing
  command. Better to give it its own name and verb.
- **Introduce a new `DagRunType.AD_HOC`.** Rejected because the
  provenance marker on `DagModel` already distinguishes ad-hoc DAGs
  from bundle DAGs, and the user DAG run is created by
  `TriggerDagRunOperator` from the system DAG (ADR 0002), which
  produces `DagRunType.OPERATOR_TRIGGERED`. A new run type would be
  redundant with the provenance column and would force every existing
  `run_type` consumer (UI filters, metrics, scheduler logic) to learn
  about it.
- **Submit as a Python callable, not a file.** A `client.submit(fn,
  *args)` API that pickles a callable. Rejected for v1: it is a
  different (smaller) feature, and pickling user closures across
  Python versions is a known footgun. We can add it later as a thin
  wrapper that synthesises a DAG file.

## Consequences

Positive:

- Closes a real gap users hit today, with a shape they already know
  from `spark-submit`, Ray, and SkyPilot.
- Reuses existing primitives (`get_fs`, Task SDK runtime, executor
  dispatch, Execution API) instead of inventing new transport.
- Keeps the scheduler out of user code, consistent with the documented
  architecture boundary.

Negative / costs:

- A new public surface to maintain: REST endpoints, CLI commands, a
  data model, archive lifecycle, retention policy.
- The worker now has a new code path: "download archive, unpack,
  parse, execute". Failure modes (corrupt archive, missing entry file,
  import errors at parse time) need to surface as a failed run with a
  useful error, not a stuck task.
- Object-store access is a hard dependency for ad-hoc execution. We
  intentionally do not provide a local-filesystem fallback or proxy
  the upload through the API server: deployments without a
  configured object store cannot use the feature. This keeps the
  data path uniform across all deployments and avoids two parallel
  storage code paths.
- Submissions are a new vector for users to run code on workers. The
  trust model is unchanged versus bundle-backed DAGs, but the
  *attack surface for credential exfiltration via "submit a malicious
  DAG"* is now reachable from anyone who can hit the public API.
  Authentication and per-user quotas are mandatory at GA, not optional.

Risks to watch:

- Archive size and worker startup time. Large archives on small
  workers will be slow. Mitigation: document a reasonable size cap
  and recommend dependency layers stay in the worker image.
- Log retention coupling. If we delete the archive but keep logs,
  reproducing a failure later requires the user to keep the source
  themselves. We should make this explicit in the docs and offer an
  optional "keep archive on success / failure / both" knob.

## Open questions

1. **Bundle abstraction or sibling?** Is `AdHocDagBundle` a real
   subclass of `BaseDagBundle` (with `DagBundlesManager` taught to
   skip it), or a separate `BaseSubmissionSource` abstraction that
   reuses parsing helpers but does not pretend to be a bundle? The
   former is less code; the latter keeps the bundle contract clean.
2. **Per-task vs. per-DAG submission.** Is a submission always a full
   DAG, or do we also want a one-task variant ("run this Python
   callable on a worker")? The latter overlaps with `@task` and
   should probably stay out of v1.
3. **Quotas and lifecycle.** Default retention for archives, default
   per-user submission quotas, default expiration of `PENDING_UPLOAD`
   rows that never receive an upload. Needs a follow-up ADR or design
   doc.
4. **Multi-team scoping.** How does this interact with the multi-team
   isolation work? Submissions need to be scoped to a team's executor
   queues and connections. Likely "team is a column on `Submission`,
   inherited from the user's team", but this should be confirmed
   with the multi-team owners.
5. **Variables and connections.** The submitted DAG can reference
   `Variable.get(...)` and `Connection.get(...)`. We need to decide
   whether ad-hoc runs see the deployment's full variable/connection
   set, a user-scoped subset, or only what the submission manifest
   explicitly requests. Default proposal: same as bundle-backed DAGs
   (full set, gated by the user's RBAC), and revisit if abuse is
   observed.

## Note on dag_id collisions between users

This ADR (and ADR 0002) intentionally does **not** prescribe a
technical guard against two users authoring an ad-hoc DAG with the
same `dag_id` and submitting at the same time. The first registration
wins; a concurrent or later registration of the same `dag_id` will
collide on the existing ad-hoc row, and the Execution API endpoint
defined in ADR 0002 surfaces that collision in its response.

We do not bake in a per-owner namespace, a `--force` overwrite flag,
or a composite uniqueness key because naming hygiene for ad-hoc DAGs
is a governance question, not an architecture one. Different
deployments will want different policies: a single ML team may be
fine with a free-for-all and last-write-wins; a regulated environment
may want strict per-user prefixes; a multi-team platform may want
team-scoped names enforced in CI. Any choice we hard-code in core
will be wrong for half of them.

Deployment Managers and platform teams adopting this feature should
agree on a naming convention up-front and enforce it through code
review, lint, or a deployment-side policy hook rather than through
Airflow core. The Execution API's structured collision indicator is
the signal whatever governance the deployment applies should react
to.
