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

# AIP-XX: Ad-hoc DAG execution (`airflow submit`)

> Draft of an Airflow Improvement Proposal. The fields below mirror the
> Confluence AIP template; they will be transcribed into the wiki page
> when the proposal moves to discussion. Detailed design lives in
> [ADR 0001](./0001-ad-hoc-dag-execution.md) and
> [ADR 0002](./0002-parsing-and-execution-mechanism.md).

## Status

| Field | Value |
| --- | --- |
| State | Draft |
| Discussion Thread | _(to be filed on dev@airflow.apache.org)_ |
| Vote Thread | _(n/a until discussion closes)_ |
| Vote Result Thread | _(n/a)_ |
| Progress Tracking (PR / GitHub Project / Issue Label) | _(to be filed; suggested label: `AIP-XX`)_ |
| Date Created | 2026-05-06 |
| Version Released | _(target: a future Airflow 3.x minor)_ |
| Authors | Jason Liu (@jason810496) |

## Motivation

Airflow today is optimised for DAGs that live in a bundle (Git, local
filesystem, custom `BaseDagBundle`) and are parsed by the DAG file
processor before any run can be created. That model fits production
scheduling well, but it does not fit a class of workflows that show up
repeatedly in ML and data-science work:

- A user has a pipeline checked out on their laptop and wants to run it
  once on a remote cluster (typically because the cluster has GPUs,
  more memory, or access to data the laptop does not).
- They want a tight feedback loop: edit locally, submit, watch logs,
  iterate. "Commit, push, wait for bundle refresh, trigger, watch" is
  too slow for experimentation.
- They expect a `spark-submit`-style command. Submit a script and its
  dependencies, get a run, get logs, get an exit status. Ray, SkyPilot,
  and Slurm all expose this shape; Airflow does not.

Existing primitives do not cover this:

- `airflow dags test` runs the DAG **locally**, in-process with the
  CLI. It does not exercise the remote workers, scheduler, or executor.
- `airflow dags trigger` requires the DAG to already exist in a bundle
  and be parsed; it cannot ship a file from the local machine.
- `BaseDagBundle` is designed around named, versioned, refreshable
  sources. Treating every ad-hoc submission as a new bundle pollutes
  the bundle namespace and forces the DAG processor to track ephemeral
  state it should not own.

This AIP introduces a first-class **ad-hoc DAG execution** path so a
user can package a DAG file (with optional supporting modules) on their
laptop, submit it to the API server, and have a worker download, parse,
and execute it as a one-shot run.

## Considerations

The proposal touches three architectural surfaces and needs them to
land together to be useful end-to-end. Each is described in detail in
ADR 0002.

1. **DAG provenance on `DagModel`.** A new marker (`BUNDLE` vs `ADHOC`)
   so ad-hoc submissions are distinguishable from bundle-parsed DAGs
   and cannot shadow them. The marker is mandatory; the exact column
   placement (on `DagModel` directly or on a sibling table) is
   deferred.
2. **A new Execution API endpoint** that accepts a serialised parse
   result from a worker. This is the **only** path by which an ad-hoc
   DAG enters the metadata DB; workers do not import DB-bound models.
   Authentication piggybacks on the existing per-task-instance JWT.
3. **A built-in "system DAG"** that orchestrates the
   download → parse → register → trigger sequence. The CLI does not
   talk to a bespoke dispatcher; it triggers the system DAG with the
   submission as parameters. This means retries, logs, observability,
   and the UI all work for free.

Special considerations and known difficulties:

- **Two DagRuns per submission** (system DAG + user DAG). This is more
  observable surface to explain to users and document in the UI.
- **Local dependency discovery is best-effort.** The CLI uses Python's
  `ast` plus `importlib.util.find_spec` to walk imports of the entry
  file. Runtime-only imports (`__import__(name)`, `importlib` by
  string) will be missed; users must be able to add files explicitly.
- **Object-store access is a hard dependency** for ad-hoc execution.
  Object storage is the only supported archive backend; there is no
  local-filesystem fallback and the API server does not proxy archive
  bytes. Deployments without a configured object store cannot use
  the feature.
- **Inter-user `dag_id` collisions** among ad-hoc submissions are
  intentionally **not** technically guarded. Naming hygiene is treated
  as a governance question; the Execution API surfaces collisions
  clearly so deployments can apply their own policy. See the closing
  notes in both ADRs.
- **Trust model is unchanged** versus bundle-backed DAGs: submitted
  code runs on the worker with the worker's identity. The new attack
  surface is "submit a malicious DAG via the public API", so
  authentication and per-user quotas are mandatory at GA, not optional.

## What change do you propose to make?

Add a `spark-submit`-style ad-hoc DAG execution path to Airflow:

- A new `airflow submit <entry_file> [--param key=value ...]
  [--watch / --no-watch]` CLI in the Task SDK / CLI, with the same
  operations exposed via airflow-ctl as REST clients.
- A new `Submission` data model in core that owns the archive and run
  lifecycle and is linked to a transient ad-hoc `DagModel` (with
  provenance marker `ADHOC`) and the resulting `DagRun`.
- A new public REST surface under
  `airflow-core/src/airflow/api_fastapi/core_api/routes/public/submissions.py`
  for create / upload / run / status / list / cancel.
- A new Execution API endpoint that accepts a serialised parsed DAG
  from a worker, enforces provenance rules, and persists it.
- A built-in system DAG that downloads the archive (via `get_fs()`),
  parses and registers it via the Execution API endpoint, and triggers
  the user's DAG via `TriggerDagRunOperator`.
- A provenance marker (`BUNDLE` / `ADHOC`) on `DagModel` so ad-hoc
  rows are distinguishable from bundle-parsed rows and cannot shadow
  them.

The user DAG run is created by `TriggerDagRunOperator` from the system
DAG, so it is `DagRunType.OPERATOR_TRIGGERED`. **No new `DagRunType` is
introduced**; the provenance column is what distinguishes ad-hoc DAGs.

The numbered functional requirements (`FR-N`) and the concrete
inventory of code, schema, configuration, and test changes the AIP
introduces live in the companion document
[Functional requirements and required changes](./requirements-and-changes.md).

## What problem does it solve?

It closes the gap between "I have a pipeline on my laptop" and "I want
to run it on the cluster, now, once." Today users either:

- Commit, push, wait for a bundle refresh, then trigger — a feedback
  loop measured in minutes that is hostile to ML iteration where most
  attempts are throwaway.
- Run `airflow dags test` locally — which does not use the cluster's
  workers, executors, or resources, so it cannot validate behaviour on
  the system the user actually wants to use.
- Reach for a different tool (Ray, SkyPilot, Slurm) for ad-hoc remote
  execution and use Airflow only for the scheduled side. The
  orchestration layer ends up split across two systems.

## Why is it needed?

The use case has been raised explicitly by users (e.g. on the SkyPilot
Slack) who want Airflow to host the orchestration layer of an ML
pipeline but keep the development loop local. The shape they expect is
the `spark-submit` shape; offering it natively means Airflow can serve
both the scheduled production workflow and the ad-hoc experimentation
workflow with the same mental model, the same CLI, the same UI, and
the same observability.

The mechanism is also useful well beyond ML: any team that runs short,
parameterised, one-off jobs against a remote cluster benefits from the
same workflow.

## Are there any downsides to this change?

- **New public surface to maintain**: REST endpoints, CLI commands, a
  data model, archive lifecycle, retention policy, UI surface.
- **New code path on the worker**: "download archive, unpack, parse,
  execute". Failure modes (corrupt archive, missing entry file, import
  errors at parse time) need to surface as a failed run with a useful
  error, not a stuck task.
- **Object-store access becomes a hard dependency** for ad-hoc
  execution in any non-trivial deployment.
- **New abuse vector**: a way for any authenticated user to run code
  on a worker via the public API. Authentication, RBAC, and per-user
  quotas are mandatory at GA.
- **Two DagRuns per submission** (system DAG + user DAG) is more
  observable surface to explain.
- **Drift risk**: the system DAG runs paths it does not own (parse +
  serialise). It must be versioned with Airflow and tested against
  changes to `SerializedDAG` and the Execution API.
- **Inter-user `dag_id` collisions** among ad-hoc submissions are not
  guarded by core; deployments must establish a governance model.

## Which users are affected by the change?

- **DAG Authors / ML & data-science engineers**: gain a new submission
  workflow. No change required to existing DAGs; the feature is purely
  additive.
- **Deployment Managers**: gain a new feature to enable, configure
  (object-store backend, archive retention, quotas), and govern
  (naming convention for ad-hoc `dag_id`s).
- **Platform / Security teams**: must review the new public surface
  for their auth and RBAC posture before enabling it; need to set
  quotas and a naming convention.
- **Provider authors**: unaffected, with the possible exception of a
  new optional `apache-airflow-providers-submit` PoC distribution that
  lives under `providers/` during the staging phase.

Bundle-backed DAGs and their authors are **not** affected: provenance
ensures ad-hoc submissions can never shadow bundle DAGs.

## How are users affected by the change? (e.g. DB upgrade required)

- **DB upgrade required**: a new `Submission` table, a provenance
  marker on `DagModel` (column or sibling table), and an FK from the
  ad-hoc `DagRun` back to the `Submission`. Standard Alembic migration
  applies.
- **No change required to existing DAGs.** Bundle-parsed DAGs continue
  to behave identically; default queries filter ad-hoc rows out.
- **New CLI subcommand** (`airflow submit ...`) and matching
  airflow-ctl commands. Discoverable via `--help`.
- **New REST surface** under `/submissions`. Existing endpoints are
  unchanged.
- **UI surface** gets a "Submissions" tab in a follow-up; until then
  ad-hoc runs appear in the existing DAG / DagRun views, distinguished
  by the provenance marker.

## What is the level of migration effort (manual and automated) needed for the users to adapt to the breaking changes?

**No breaking changes for existing users.** The proposal is purely
additive:

- Existing DAGs continue to parse and run unchanged. They are still
  `BUNDLE` provenance and have no relationship to the new
  `Submission` model.
- Existing `airflow dags test`, `airflow dags trigger`, and
  `airflow dags backfill` are unchanged in semantics. The new path is
  a separate verb (`airflow submit`).
- The DB migration is automated via Alembic. No DAG-author-side
  changes are required.
- Default query filters exclude `ADHOC`-provenance rows from listings
  that today implicitly expect bundle DAGs, so dashboards and metrics
  built on those queries see no change in shape.

For Airflow 3 specifically, the new path is built **on** Airflow 3
boundaries (workers talk only to the API server, including for DAG
registration). It does not extend the documented "DFP / Triggerer
direct-DB access" limitation; it deliberately routes through the
Execution API.

A migration utility is **not** required, since no existing DAGs need
to change.

## Other considerations?

- **Staging plan.** Land as a PoC in a provider distribution
  (`apache-airflow-providers-submit`) first, to iterate on the worker-
  side archive handling and CLI shape without committing core to the
  data model. Promote into core once the lifecycle is stable, then
  add UI polish (Submissions tab) and retention controls.
- **Variables, connections, and RBAC.** Default proposal: ad-hoc runs
  see the same variable / connection set as bundle-backed DAGs, gated
  by the user's RBAC. Revisit if abuse is observed.
- **Multi-team scoping.** Submissions need to be scoped to a team's
  executor queues and connections in multi-team deployments. Likely
  "team is a column on `Submission`, inherited from the user's team",
  but this needs confirmation with the multi-team owners.
- **Quotas and lifecycle.** Default retention for archives, default
  per-user submission quotas, default expiration of `PENDING_UPLOAD`
  rows that never receive an upload. Will be addressed in a follow-up
  ADR or design doc.
- **Single-node and offline deployments.** Not supported. Object
  storage is the only archive backend; deployments without one
  cannot use ad-hoc submission. `breeze` and other local setups must
  point at a configured object-store URI (e.g. a local MinIO) to
  exercise the feature.
- **Logs and reproducibility.** If the archive is deleted but logs are
  kept, reproducing a failure later requires the user to keep the
  source. Documented; offer an optional "keep archive on success /
  failure / both" knob.

## What defines this AIP as "done"?

The AIP is "done" when all of the following are true:

1. **Functional end-to-end path** in Airflow core:
   - `airflow submit <entry_file>` packs and uploads an archive,
     triggers the system DAG, and streams logs from the resulting user
     DagRun until completion.
   - The same flow is reachable via the public REST API and via
     airflow-ctl.
2. **Provenance is enforced** at the Execution API endpoint:
   - Ad-hoc registration of a `dag_id` that already has a `BUNDLE` row
     is rejected.
   - Collisions between two ad-hoc submissions on the same `dag_id`
     return a clear, structured indicator that deployment-side
     governance can react to.
3. **Workers do not bypass the Execution API** to register ad-hoc
   DAGs. Verified by tests that assert no direct ORM use from worker
   paths.
4. **The system DAG ships with Airflow** and is exercised by an
   integration test that covers the full
   download → parse → register → trigger flow against the real
   Execution API.
5. **Migration applied**: Alembic migration for the `Submission` table
   and the `DagModel` provenance marker is in `main`, with downgrade
   tested.
6. **Documentation in tree**:
   - User-facing "Submitting ad-hoc DAGs" guide.
   - Deployment Manager guide covering object-store configuration,
     quotas, and the dag_id naming-convention recommendation.
   - Security model entry noting the new attack surface and the
     required mitigations (auth, quotas, RBAC).
7. **Default UI surface**: ad-hoc DagRuns are visible in the existing
   DAG / DagRun views with provenance distinguishable; a dedicated
   "Submissions" tab is a follow-up and not blocking.
8. **Tests**: unit and integration coverage for the Execution API
   endpoint (provenance rules, collision response shape), the system
   DAG (each task, end-to-end), the CLI (local dependency discovery,
   pack-and-upload), and the worker path (archive download,
   parse-error surfacing).
9. **Provider PoC retired or marked deprecated** once the core
   surface lands.

The AIP does **not** need to deliver, in the same release: a UI
"Submissions" tab, full multi-team scoping, per-user quotas at the
API-server level, or a Python-callable submit form (`client.submit(fn,
*args)`). Those are explicitly follow-ups.
