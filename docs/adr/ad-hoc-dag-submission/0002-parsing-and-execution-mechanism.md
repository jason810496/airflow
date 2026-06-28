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

- [2. Parsing and execution mechanism for ad-hoc DAGs](#2-parsing-and-execution-mechanism-for-ad-hoc-dags)
  - [Status](#status)
  - [Context](#context)
  - [Decision](#decision)
    - [Scope of this ADR](#scope-of-this-adr)
    - [Core decision 1: DAG provenance](#core-decision-1-dag-provenance)
    - [Core decision 2: Workers persist parsed DAGs via the Execution API](#core-decision-2-workers-persist-parsed-dags-via-the-execution-api)
    - [Core decision 3: Submission flow is itself an Airflow DAG](#core-decision-3-submission-flow-is-itself-an-airflow-dag)
    - [End-to-end flow](#end-to-end-flow)
    - [User-facing CLI shape](#user-facing-cli-shape)
  - [Alternatives considered](#alternatives-considered)
  - [Consequences](#consequences)
  - [Open questions](#open-questions)
  - [Note on dag_id collisions between users](#note-on-dag_id-collisions-between-users)

# 2. Parsing and execution mechanism for ad-hoc DAGs

Date: 2026-05-03

## Status

Proposed. Builds on
[1. Ad-hoc Dag submission](./0001-ad-hoc-dag-submission.md).

## Context

ADR 0001 establishes that we want a `spark-submit`-style ad-hoc Dag
submission path and sketches a submission lifecycle, a `Submission`
data model, and a REST surface. That ADR is deliberately broad: it
spans data model, lifecycle, RBAC, retention, UI, and staging.

This ADR narrows in on the **architectural core**: how an ad-hoc DAG
is registered into the metadata DB without overriding bundle-parsed
DAGs, how the worker writes that registration without violating the
Airflow 3 DB-access boundary, and how the download/parse/trigger
sequence is orchestrated. Schema details, retention, RBAC, and UI
surface remain in the broader feature ADR (0001) and follow-ups; this
ADR commits only to the three architectural decisions and the flow
they imply.

Reusable primitives the design leans on:

- **Execution API**
  (`airflow-core/src/airflow/api_fastapi/execution_api/`) is the
  worker-to-API-server boundary. Workers do not touch the metadata DB;
  they call this API. Any new "worker writes parsed DAG state" action
  must go through this surface, not direct ORM use.
- **`get_fs()`** in the Task SDK
  (`task-sdk/src/airflow/sdk/io/fs.py:74`) is an fsspec-backed,
  provider-pluggable abstraction over object storage. The archive can
  ride over any scheme the deployment already has a connection for
  (`s3`, `gs`, `azure`, `file`).
- **`TriggerDagRunOperator`** already encapsulates "create a DagRun
  for another dag_id and (optionally) wait for it". We do not need new
  dispatcher logic to fire the user's DAG once it is registered.

## Decision

### Scope of this ADR

This ADR commits to three architectural decisions and the end-to-end
flow that they imply. It does **not** finalise the metadata schema,
RBAC model, archive retention, UI surface, or per-team quotas. Those
remain with ADR 0001 and its follow-ups.

### Core decision 1: DAG provenance

`DagModel` rows produced by the ad-hoc path must be distinguishable
from rows produced by the regular DAG file processor, and the ad-hoc
path must not be able to shadow a regular DAG.

Concretely:

- `DagModel` carries a **provenance** marker. Two values for now:
  `BUNDLE` (the default; what the DAG file processor produces) and
  `ADHOC` (what the new path produces). The exact column name and
  whether it lives on `DagModel` directly or on a sibling table is
  deferred, but the marker is mandatory.
- An ad-hoc registration **must reject** any `dag_id` that already has
  a `BUNDLE` row. Bundle DAGs are the source of truth and an ad-hoc
  submission can never silently override one.

This gives us the property we need: clear provenance and no accidental
shadowing of bundle-parsed DAGs. Collisions between ad-hoc submissions
themselves (two users authoring a DAG with the same `dag_id`) are a
separate concern that this ADR does **not** technically guard
against; see
[Note on dag_id collisions between users](#note-on-dag_id-collisions-between-users)
at the end.

### Core decision 2: Workers persist parsed DAGs via the Execution API

When a worker parses an ad-hoc archive, it must not write the result
to the metadata DB directly. The Execution API
(`airflow-core/src/airflow/api_fastapi/execution_api/`) gains a new
endpoint that accepts a serialised parse result and persists it under
the rules from Core decision 1.

Properties of this endpoint:

- It accepts the serialised DAG payload (the same shape that
  `SerializedDagModel` already stores), plus the submitter identity
  and the ad-hoc submission ID.
- It enforces the provenance rules: rejects collisions with `BUNDLE`
  rows, surfaces collisions with existing `ADHOC` rows clearly in its
  response so deployment-side policy can react.
- It is the **only** way an ad-hoc DAG enters the metadata DB. The
  worker does not import any DB-bound model.
- Authentication piggybacks on the existing per-task-instance JWT.
  Permission to register an ad-hoc DAG is granted by the system DAG's
  task context (see Core decision 3), not by a new credential.

This is consistent with Airflow 3's documented architecture: workers
talk to the API server; the API server is the only writer to the
metadata DB. The DAG file processor's direct DB access for `BUNDLE`
DAGs is the documented "known limitation" we are explicitly choosing
not to extend to the ad-hoc path.

### Core decision 3: Submission flow is itself an Airflow DAG

The download → parse → register → trigger sequence is implemented as a
**built-in Airflow DAG** that ships with Airflow (a "system DAG"). The
CLI does not invoke a bespoke dispatcher; it triggers this system DAG
with the submission as its parameters.

The system DAG has roughly three tasks:

1. **Download archive** from the object-storage URI handed to it as a
   parameter. Uses `get_fs()` so the scheme is whatever the deployment
   supports.
2. **Parse and register**: import the entry file in the worker
   process, serialise the resulting DAG, and POST it to the Execution
   API endpoint from Core decision 2. On success, the user's DAG now
   exists as an `ADHOC`-provenance row.
3. **Trigger user DAG**: a `TriggerDagRunOperator` against the just-
   registered dag_id, with the user's parameters.

The system DAG run completes once the trigger fires. The user's DAG
runs independently as its own DagRun, observable through the normal
DagRun and TaskInstance surfaces.

**Trigger contract: how the parsing DagRun knows which submission it
belongs to.** When the API server triggers the orchestrator DAG, it
does two writes that must happen atomically:

1. Trigger the orchestrator DagRun with
   `dag_run.conf = {"submission_id": "<uuid>"}`.
2. Record the resulting `dag_run_id` on `submission.parsing_dag_run_id`.

`submission_id` is the only field the orchestrator carries in its
`conf`. Everything else (`archive_uri`, `entry_file`, `dag_run_conf`
to forward, `parsed_dag_id` once produced) lives on the `Submission`
row and is fetched by parsing tasks via Execution API calls — keeping
the row as the single source of truth and avoiding drift between conf
and row.

The atomicity matters because every Execution API endpoint that takes
a `submission_id` authorizes by checking
`submission.parsing_dag_run_id == caller's dag_run_id` (see ADR 0003
for the archive-download token; the same pattern applies to the
parsed-DAG registration endpoint and to "fetch submission" reads). If
the trigger succeeds but the row write fails, the parsing DagRun
exists but every authorization check refuses it, so it cannot
escalate using a tampered `conf`. The required behaviour is therefore
**fail-closed**: do both writes in one transaction, or perform the
trigger via the same path that already records DagRun-create state so
the row write is part of the trigger flow.

This decision has a few important consequences:

- **No new dispatcher logic.** The "ad-hoc execution" feature is, at
  the orchestration layer, just another DAG that happens to ship with
  Airflow. Logs, retries, monitoring, and the UI all work for free.
- **Two DagRuns per submission.** The system DAG's run (the
  orchestrator) and the user DAG's run (the actual workload) are
  separate. The CLI distinguishes between them.
- **Failure isolation is natural.** A parse error fails the system
  DAG's parse task with a normal stack trace, viewable through the
  standard task-log endpoints. The user DAG never gets created and
  there is no orphaned state.
- **The system DAG is parsed and stored like any other DAG.** It can
  be versioned and updated through normal Airflow upgrade channels.

The CLI side is light:

- **Local dependency discovery.** Use Python's native introspection
  (e.g. `ast` to walk imports of the entry file, plus
  `importlib.util.find_spec` to resolve which of those are local
  modules versus installed packages) to determine the set of files to
  pack. Installed packages are assumed to be available on the worker;
  local modules are bundled.
- **Pack and upload.** Zip the entry file plus discovered local
  modules, request an upload target from the API server, push the
  archive to object storage.
- **Trigger and watch.** Trigger the system DAG with the archive URI
  and submission parameters, then poll the Execution API / public API
  for the user DagRun's state and stream its task logs.

### End-to-end flow

```
[client CLI]              [api server]            [object store]      [worker (system DAG)]      [worker (user DAG)]
     |                          |                         |                       |                         |
     | resolve local imports    |                         |                       |                         |
     | (ast + find_spec)        |                         |                       |                         |
     | pack zip                 |                         |                       |                         |
     |                          |                         |                       |                         |
     | request upload target    |                         |                       |                         |
     |------------------------->|                         |                       |                         |
     |<-- upload uri/creds -----|                         |                       |                         |
     |                                                                                                       |
     | PUT archive ----------------------------------------->                     |                         |
     |                          |                         |                       |                         |
     | trigger system DAG       |                         |                       |                         |
     | (archive_uri,            |                         |                       |                         |
     |  authored_dag_id, params)|                         |                       |                         |
     |------------------------->|                         |                       |                         |
     |                          | dispatch system DAG run |                       |                         |
     |                          |--------------------------------------------- ->|                         |
     |                          |                         |                       | task1: download archive |
     |                          |                         |<----------------------|                         |
     |                          |                         |                       | task2: parse +          |
     |                          |                         |                       |        POST serialised  |
     |                          |                         |                       |        DAG to           |
     |                          |                         |                       |        Execution API    |
     |                          |<------------------------|-----------------------|                         |
     |                          | provenance check;       |                       |                         |
     |                          | insert ADHOC row        |                       |                         |
     |                          |                         |                       | task3: TriggerDagRun    |
     |                          |<------------------------|-----------------------|                         |
     |                          | dispatch user DAG run --|-----------------------|------------------------>|
     |                          |                         |                       |                         | run user tasks
     | poll user DagRun state   |                         |                       |                         |
     | + stream task logs       |                         |                       |                         |
     |<------------------------>|                                                                            |
```

### User-facing CLI shape

`airflow submit` deliberately mirrors the `spark-submit` family of
"submit local code to a remote cluster" tools. Those tools converge on
the same handful of concerns — name an entry point, ship the local
code, pass parameters, target compute, and choose whether to wait — but
differ in how much they put on the command line. Flag names below
reflect each tool's current official CLI docs:

| Purpose | `spark-submit` | `ray job submit` | `pyflyte run` | `sky launch` | `sbatch` | `airflow submit` |
| --- | --- | --- | --- | --- | --- | --- |
| Entry point | `app.py` / `--class` | `-- python x.py` | `FILE.py ENTITY` | YAML / inline | `script.sh` | `<entry_file>` |
| Ship local code | `--py-files` `--files` `--archives` | `--working-dir` | `--copy all` | `--workdir` | shared FS | auto-discovery + `--include` |
| Root / working dir | — | `--working-dir` | copy root | `--workdir` | `-D/--chdir` | `--project-dir` |
| Parameters / inputs | app args | args after `--` | `--<input>` | YAML / `--env` | script args | `--conf <json>` |
| Environment vars | `--conf …executorEnv` | runtime-env `env_vars` | `--env` | `--env` / `--env-file` | `--export` | via `--conf` |
| Target / queue | `--master` `--queue` | `--address` | `--remote -p -d` | `--infra` `--cluster` | `-p/--partition` | `--queue` |
| Compute resources | `--executor-memory` `--num-executors` | `--entrypoint-num-gpus` | `--resource-requests` | `--gpus` `--cpus` | `--gres=gpu` `--mem` | from the DAG + `--queue` |
| Name / identity | `--name` | `--submission-id` | `--name` | `-n/--name` | `-J/--job-name` | `--run-id` |
| Wait / detach | `--deploy-mode` | `--no-wait` | `--wait` | `-d/--detach-run` | `--wait` | `--watch / --no-watch` |

The proposed first-cut flag set borrows a flag only where it maps
cleanly onto an Airflow-native verb (`conf`, queue, DAG):

```
airflow submit <entry_file>

  # local code to ship
  --project-dir <dir>     root to discover and pack local imports from
                          (default: the entry file's directory)
  --include <path>        extra file or directory to add to the archive; repeatable
                          (runtime imports or data files the static walker misses)
  --exclude <glob>        path to omit from the archive; repeatable

  # parameters
  --conf <json>           JSON string forwarded to the user DAG run's conf
                          (same flag as `airflow dags trigger`)

  # target & identity
  --queue <name>          executor queue to dispatch the workload to
  --run-id <id>           run id for the workload DAG run (as in `airflow dags trigger`)

  # lifecycle
  --watch / --no-watch    stream the workload's logs and exit on its result (default: --watch)
  --timeout <duration>    stop watching after this long (does not cancel the run)
```

Two purposes are deliberately **not** turned into flags, because Airflow
already owns them elsewhere:

- **Which Airflow to talk to.** The target API server comes from the
  existing CLI / airflow-ctl configuration (config file or environment),
  the same way `ray job submit` reads `RAY_ADDRESS` and `spark-submit`
  takes `--master`. There is no per-command `--server` flag.
- **Compute resources and third-party dependencies.** CPU, GPU, and
  memory are expressed in the DAG itself (operator arguments,
  `executor_config`, pools), and third-party packages come from the
  worker image — the same model every bundle-backed DAG already uses.
  `airflow submit` therefore exposes only `--queue` as the compute
  target and does **not** replicate `--gpus` / `--cpus` / `--memory`,
  which would create a second, conflicting resource model. `--include`
  ships first-party local modules; it is not a dependency installer.

The same operations are exposed in airflow-ctl as REST clients, since
the submission, registration, and DagRun-watching are all REST calls.

## Alternatives considered

- **Bespoke dispatcher in the API server.** Have the API server
  download the archive, parse it server-side, and create the run
  directly. Rejected: parsing user code on the API server breaks the
  scheduler-side "no user code" boundary, and reimplements
  orchestration logic that `TriggerDagRunOperator` already provides.
- **Direct DB write from the worker.** The simplest way for the worker
  to register a parsed DAG is to call `SerializedDagModel.write_dag()`
  directly. Rejected: this violates the Airflow 3 boundary that says
  workers talk to the API server only. We do not want to add to the
  list of "known limitations" where components bypass the Execution
  API.
- **A standalone background worker (not a system DAG) for orchestration.**
  Rejected: it would duplicate retry, log, and observability code that
  Airflow already has for DAG runs. Reusing the DAG abstraction is
  cheaper and more consistent.

## Consequences

Positive:

- Workers stay on the right side of the DB boundary: all writes go
  through the Execution API, including DAG registration.
- Provenance makes the feature safe to enable next to production
  bundle-backed DAGs. There is no path by which an ad-hoc submission
  can shadow a real DAG.
- The orchestration is just another DAG. We get retries, logs, the
  task-log endpoint, the UI, and the standard failure path for free.
- Reuses existing primitives end-to-end: `get_fs`, the Task SDK
  runtime, the Execution API, `TriggerDagRunOperator`. No new
  dispatcher.

Negative / costs:

- Three architectural changes (provenance marker, new Execution API
  endpoint, system DAG) need to land together to be useful end-to-end.
  This ADR is the bottleneck that gates ADR 0001's lifecycle work.
- Two DagRuns per submission (system DAG + user DAG) is more
  observable surface to explain to users and document in the UI.
- Local dependency discovery via `ast` + `find_spec` is best-effort.
  It will miss runtime-only imports (e.g. `__import__(name)`,
  `importlib` by string). The CLI must let users explicitly add files
  to the archive.
- Inter-user `dag_id` collisions among ad-hoc submissions are not
  technically guarded; deployments must put a governance model in
  place (see closing note).

Risks to watch:

- **Drift between the system DAG and core.** The system DAG runs user
  code paths it does not own (parse + serialise). It must be versioned
  with Airflow and tested against changes to `SerializedDAG` and the
  Execution API.
- **Worker startup cost** for large archives. Document a size cap and
  recommend dependency layers stay in the worker image.

## Open questions

These need answers before the schema/lifecycle ADR that follows:

1. **Where the system DAG lives.** Shipped in `airflow-core` as a
   built-in DAG, in a new "system bundle", or installed by the
   deployment. This affects upgrade and customisation paths.
2. **Execution API endpoint shape.** A single
   `POST /execution/dags/parsed` that accepts the serialised DAG, or
   a small set of finer-grained endpoints (register, attach
   artifacts, mark ready). The shape determines how testable and
   reusable the registration path is.

## Note on dag_id collisions between users

This ADR intentionally does **not** prescribe a technical guard
against two users on the same deployment authoring an ad-hoc DAG with
the same `dag_id` and submitting at the same time. The first
registration wins; a concurrent or later registration of the same
`dag_id` will collide on the existing `ADHOC` row, and the Execution
API endpoint surfaces that collision in its response.

We do not bake in a per-owner namespace, a `--force` overwrite flag,
or a composite uniqueness key here because naming hygiene for ad-hoc
DAGs is a governance question, not an architecture one. Different
deployments will want different policies: a single ML team may be
fine with a free-for-all and last-write-wins; a regulated environment
may want strict per-user prefixes; a multi-team platform may want
team-scoped names enforced in CI. Any choice we hard-code in core
will be wrong for half of them.

Deployment Managers and platform teams adopting this feature should:

- Agree on a naming convention up-front (e.g. "prefix your ad-hoc
  `dag_id` with your username" or "prefix with your project tag").
- Enforce the convention through code review, lint, or a deployment-
  side policy hook rather than through Airflow core.
- Treat the Execution API's collision response as the signal that
  the convention has been violated, and react accordingly (reject,
  warn, page, etc.).

The Execution API endpoint from Core decision 2 must, at minimum,
return a clear, structured collision indicator so that whatever
governance the deployment applies can detect violations
deterministically.
