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

# Plan: versioning framework for the serialized Dag format

Implements apache/airflow#71364. Builds on branch `fix/serialization/strict-task-schema-validation`, which makes `schema.json` enforce the operator rules.

## 1. Problem

- `SerializedDAG.SERIALIZER_VERSION` is `3`, but nothing says when it must change. Since 3.1, 27 format changes shipped with no bump (section 5), and #70570 adds `_mapped_arg_binding_params` the same way.
- `conversion_v2_to_v3` only sets `__version = 3`.
- `from_dict` never moves its local `ver` forward, so a v1 blob skips `conversion_v2_to_v3`. This is harmless today only because that step does nothing.
- Downgrade lives in per-dialect SQL inside migration `0085_3_1_0_downgrade_serialized_dag_version_to_v2.py`.
  - It reads `serialized_dag.data_compressed` with `gzip.decompress`, but `SerializedDagModel` writes it with `zlib.compress`. Every compressed row fails, the error is printed and skipped, and the row stays at `3`. Airflow 3.0 then fails to load it ("Unsure how to deserialize version 3").
  - It drops `client_defaults` without copying the values back into the tasks.
- Every migration that edits the blob re-implements reading and writing rows, compression, batching and hashing. Migration 0101 carries its own copies of `get_dag_data`, `hash_dag` and `_sort_serialized_dag_dict`.
- Old blobs are read for a long time: Dag versioning keeps every `serialized_dag` row, and runs pinned to old versions read them. Upgrade-on-read has to stay for the whole major, as ashb said on the issue.

## 2. Agreed decisions

| # | Decision |
|---|---|
| D1 | In-house engine, no Cadwyn dependency. Cadwyn's value is generating per-version Pydantic models, and the serialized Dag has no Pydantic models. |
| D2 | Grouped the Cadwyn way: one version per release holding many change classes. Changes accumulate in the in-progress version, and the version moves at most once per release. |
| D3 | Each change class implements `upgrade(data)` and `downgrade(data)` on the dict, in place. |
| D4 | Date ids from now on, matching the Execution API and the supervisor schema. `1`, `2` and `3` stay as integers. Each release reuses the Execution API date it shipped. A patch release with no new Execution API version folds into the previous date. |
| D5 | Callers stay where they are today: upgrade in `DagSerialization.from_dict`, downgrade from alembic migrations. |
| D6 | Lang SDKs stamp the version they were generated from. Core rejects a stamp it doesn't know. |
| D7 | Old versions stay readable for the whole major. |
| D8 | Every `schema.json` change adds a change class to the in-progress version, enforced by prek. A released version file never changes, also enforced by prek. |
| D9 | Every version from `3` through `2026-10-30` writes stamp `3`, because cores up to 3.4 accept only `1`, `2` and `3`. Upgrade reads a `3` blob as "anywhere from 3.1.0 to 3.4" and applies every backfilled upgrade, so upgrades must be safe to apply twice. |
| D10 | Changes that need other tables are marked `requires_migration`. The engine skips them, and their alembic migration does the work (the deadline alert move, migration 0101). |
| D11 | A lossy downgrade drops the field and logs a warning. It never fails the migration: a refused change would block the whole `airflow db downgrade`, and Dags using that feature fail to import on the older release anyway. |
| D12 | Format-only changes need no upgrade migration. A release that changes the format gets one alembic revision whose `downgrade()` calls a shared helper. |
| D13 | Downgrade recomputes `dag_hash`, so the first parse after a downgrade does not create a new Dag version for every Dag. |
| D14 | The head of this PR is `2027-02-28` (Airflow 3.5). 3.4 keeps writing `3`. |
| K1 | `SerializedDagModel.hash` stops hashing `__version`, so a stamp change at each release does not create a new version of every Dag. Upgrading to 3.5 still creates one new version per Dag, once. When the helper recomputes a hash for a destination that writes an integer stamp, it keeps `__version`, matching that release's hash. |
| K2 | The TS, Go and Java SDKs stamp `"2027-02-28"` in this PR. `scripts/ci/lang_sdk_serialization/compare.py` requires the SDK stamp to equal Python's. Under D6, these SDK builds need core 3.5. |
| K4 | The 3.5 migration downgrades to the destination release's version, but stops at the newest version above the destination that holds a `requires_migration` change. The older migrations continue from there. Example: downgrading to 3.1 stops at `2026-04-06`; migration 0101 then moves deadline alerts back inline; the rewritten 0085 finishes down to `2`. Below 3.0.0, migration 0047 deletes every `serialized_dag` row, so the engine never needs to go below `2`. |
| K5 | 0085's `downgrade()` is rewritten on the shared helper, which fixes the zlib bug on main. A minimal standalone fix is tracked separately so it can be backported. |
| K6 | Offline downgrade (`--show-sql-only`) prints a notice that serialized Dags need an online downgrade, and emits no SQL. |

## 3. Engine

Package `airflow-core/src/airflow/serialization/versions/`:

```python
class SerializedDagChange:
    description: ClassVar[str]
    requires_migration: ClassVar[bool] = False

    def upgrade(self, data: dict) -> None: ...  # previous version -> this one, in place, idempotent
    def downgrade(self, data: dict) -> None: ...  # this version -> previous one, in place, idempotent


@dataclass(frozen=True)
class Version:
    id: int | str  # 1, 2, 3 or "YYYY-MM-DD"
    changes: tuple[type[SerializedDagChange], ...]
    airflow_version: str  # first release that wrote it, e.g. "3.4.0"
    stamp: int | str | None = None  # value written to __version; defaults to id


VERSIONS: tuple[Version, ...] = ...  # ordered by position, oldest first
HEAD = VERSIONS[-1]


def upgrade(data: dict) -> dict: ...
def downgrade(data: dict, *, to: int | str) -> dict: ...
def get_version_for_airflow(airflow_version: str) -> Version: ...
```

Rules:

- **Order** comes from the position in `VERSIONS`, never from comparing ids, so integers and dates can share the tuple.
- **`upgrade`** finds the first version whose stamp equals the blob's `__version`. It runs every change of every later version up to `HEAD`, then writes `HEAD.stamp`.
- **`downgrade`** finds the last version whose stamp equals the blob's `__version`. It runs `downgrade` of every change in that version and each version down to, but not including, the target. It skips `requires_migration` changes, then writes the target's stamp.
- **Unknown stamp** raises `ValueError`, as `from_dict` does today. Downgrading below `2` raises: v2 to v1 is Airflow 2, and migration 0047 deletes the rows first.
- **Lossy downgrades** log one warning per change and Dag (D11).

Version files, one per version:

```
versions/
  __init__.py    VERSIONS, HEAD, upgrade, downgrade, get_version_for_airflow
  base.py        SerializedDagChange, Version
  v2.py          ConvertAirflow2Dag: the current conversion_v1_to_v2, moved
  v3.py
  v2025_10_27.py
  v2025_11_05.py
  v2026_04_06.py
  v2026_06_30.py
  v2026_10_30.py
  v2027_02_28.py in progress; #70570 adds AddMappedArgBindingParams here
  AGENTS.md      bump rule, cadence, how to add a change
```

`AGENTS.md` follows `task-sdk/src/airflow/sdk/execution_time/schema/AGENTS.md`:

- When a change needs a class: always, including additive fields.
- The release manager freezes the in-progress file at release and opens the next date.
- How to write a change that is safe to apply twice, when to set `requires_migration`, and how to log a lossy downgrade.

## 4. Callers

- **`airflow-core/src/airflow/serialization/serialized_objects.py`:**
  - `SERIALIZER_VERSION = versions.HEAD.stamp`, i.e. `"2027-02-28"`.
  - `DagSerialization.from_dict` calls `versions.upgrade(serialized_obj)` before reading `client_defaults`.
  - `conversion_v1_to_v2` and `conversion_v2_to_v3` are removed; nothing outside core uses them.
- **`airflow-core/src/airflow/serialization/dag_version_diff.py`:**
  - `_canonicalize_payload_v1` calls `versions.upgrade` instead of the two conversions.
  - `_get_schema_version` accepts a date string as well as an integer. The diff result carries both versions as written.
- **`airflow-core/src/airflow/serialization/schema.json`:** the root `__version` accepts a positive integer or a string matching `^\d{4}-\d{2}-\d{2}$`. Regenerate the Lang SDK copies with the existing sync hooks.
- **`airflow-core/src/airflow/models/serialized_dag.py`:** `SerializedDagModel.hash` pops `__version` (K1).
- **Lang SDK stamps (K2):**
  - `ts-sdk/scripts/generate-dag-schema.mjs` sets `SERIALIZATION_VERSION`; regenerate `ts-sdk/src/generated/dag-schema-fields.ts`.
  - `go-sdk/airflow/serialize.go` sets `dagSerializationVersion` and its comment.
  - `java-sdk/sdk/src/main/kotlin/org/apache/airflow/sdk/execution/Serde.kt` sets `"__version" to 3`.
  - Update their tests (for example `go-sdk/pkg/execution/dag_parse_test.go`).
- **`airflow-core/tests/unit/serialization/test_ts_sdk_serialization_version.py`:** the regex accepts a quoted date.
- **`scripts/ci/lang_sdk_serialization/compare.py` and `scripts/tests/ci/lang_sdk_serialization/test_compare.py`:** use the new stamp.
- **Raw readers that bypass `from_dict`:** for example `SerializedDagModel` reads `["dag"]["dag_dependencies"]` from the compressed row directly. They keep tolerating older shapes. The engine only covers the `from_dict` path, and that is a documented limitation.

## 5. Backfill: format changes since 3.0

This comes from a sweep of the 121 commits that touched `serialized_objects.py` or `schema.json` between the 3.0 branch cut and upstream/main `51140809d2a`. First releases come from PR milestones, or from the merge date against the release branch cuts (v3-1-test 2025-09-09, v3-2-test 2026-03-23, v3-3-test 2026-06-24, v3-4-test 2026-10-13).

### `2` (3.0.x): no new classes

| PR | Release | Change | Why no class |
|---|---|---|---|
| #49020 | 3.0.0 | `__version` 1 to 2; `_is_empty` and `_can_skip_downstream` written only when true | Already covered by `conversion_v1_to_v2` (moved into `ConvertAirflow2Dag`) |
| #50359 | 3.0.2 | Trigger kwargs values in `asset.watchers[].trigger.kwargs` are wrapped with `BaseSerialization.serialize` | The reader accepts both forms, and downgrades between patch releases run no migration |
| #53835 | 3.0.4 | Float NaN/Inf written as the strings `"nan"`, `"inf"`, `"-inf"` | No-op both ways |

### `3` (3.1.0, stamp `3`, airflow_version `3.1.0`)

Splits today's do-nothing v2 to v3 step into real classes. Their downgrades replace 0085's logic.

| Class | PR | Change | Upgrade | Downgrade |
|---|---|---|---|---|
| `AddClientDefaults` | #54569 | New top-level `client_defaults.tasks`. Task keys equal to a schema default or to `client_defaults.tasks` are no longer written. `start_trigger_args` is written only when set, and `email_on_failure`/`email_on_retry` are dropped when `email` is empty. Mapped `partial_kwargs` follow the same rules. | No-op | Copy `client_defaults.tasks` into each task and each mapped `partial_kwargs` where the key is missing, then drop `client_defaults`. The 3.0 root object is closed (`additionalProperties: false`). |
| `ReplaceTaskCallbacksWithFlags` | #54569 | `on_{execute,failure,success,retry,skipped}_callback` becomes `has_on_*_callback: true`, only when true, in tasks and `partial_kwargs` | No-op (the reader maps both) | Rename back to `on_*_callback`. Lossy: only true/false survives. |
| `ExcludeDagSchemaDefaults` | #55849 | Dag keys equal to new schema defaults are no longer written: `catchup`, `fail_fast`, `has_on_success_callback`, `has_on_failure_callback`, `render_template_as_native_obj`, `disable_bundle_versioning` (false); `max_active_tasks`, `max_active_runs` (16); `max_consecutive_failed_dag_runs` (0). None values and empty optional collections are also dropped. | No-op | Write the schema defaults back explicitly. Otherwise a 3.0 reader falls back to config values. |
| `RenamePythonCallableToName` | #55288 | Mapped `partial_kwargs.python_callable` (a `str()` repr) becomes `python_callable_name` (qualname) | No-op; can't be backfilled because the repr lacks the module path | Rename back (lossy) |
| `AddDagDeadline` | #50093, #55086 | New `dag.deadline`: first a dict or null, then a list of dicts | No-op | Drop `deadline`; the 3.0 `dag` object is closed (lossy) |

### `2025-10-27` (3.1.1, stamp `3`)

| Class | PR | Change | Upgrade | Downgrade |
|---|---|---|---|---|
| `OmitDefaultRetryDelay` | #56202 | Schema default `retry_delay: 300.0`. `client_defaults.tasks.retry_delay` is no longer written at 300s, and an empty `client_defaults` is dropped. | No-op | Put back `client_defaults.tasks.retry_delay = 300.0` when no task and no client default sets it. 3.1.0's `SerializedBaseOperator.retry_delay` has no default and crashes without it. |

### `2025-11-05` (3.1.2 and 3.1.4, stamp `3`)

| Class | PR | Change | Upgrade | Downgrade |
|---|---|---|---|---|
| `ReplaceDefaultArgsCallbacksWithFlags` | #57397 | `dag.default_args.__var.on_*_callback` (repr) becomes `has_on_*_callback: true` | For each `on_*_callback` key, set `has_<key>` to the value's truthiness, then drop the key | Drop the `has_on_*_callback` keys (lossy) |
| `KeepExplicitSchemaDefaults` | #57853 | A task value equal to its schema default is written when `client_defaults` differs and the user set it explicitly | No-op. Can't be fully backfilled: older writers dropped the user's choice. | No-op. The class exists so the change is recorded. |

### `2026-04-06` (3.2.0 and 3.2.2, stamp `3`)

| Class | PR | Change | Upgrade | Downgrade |
|---|---|---|---|---|
| `AllowRetryBackoffMultiplier` | #56866 | Task and `partial_kwargs` `retry_exponential_backoff` goes from bool to number (schema default 0) | No-op (the reader maps bool to 2.0/0) | Convert to bool `v != 0` (lossy) |
| `AddParamSource` | #58615 | New `source` (`"dag"`, `"task"` or null) on every serialized Param | Set `"dag"` on Dag-level params when missing; task params can't be told apart | Drop `source` |
| `AddPartitionedAssetTimetable` | #58289 | New timetable `airflow.timetables.simple.PartitionedAssetTimetable` with `asset_condition` and `partition_mapper` | No-op | Rewrite as `AssetTriggeredTimetable` and drop `partition_mapper` (lossy), since older readers can't import the class |
| `DropEventsTimetableSummary` | #58669 | The SDK `EventsTimetable` encoder no longer writes `_summary` | No-op | Put back `_summary` = `description`, or `f"{len(event_dates)} events"` |
| `EncodeMappedArgument` | #59815 | A `MappedArgument` inside mapped `expand_input.value` or `partial_kwargs` is written as `{__type: "mapped_argument", __var: {input: {type, value}, key}}` instead of `str(obj)` | No-op; the old repr can't be parsed | Replace with its string form (lossy). A 3.1 reader raises `TypeError` on the new type. |
| `AddTaskRenderTemplateAsNativeObj` | #60619 | New task key `render_template_as_native_obj`, written only when set | No-op | Drop (lossy) |
| `ReferenceDeadlineAlertsById` (`requires_migration`) | #58248, #61118 | `dag.deadline` becomes a list of UUID strings pointing at `deadline_alert` rows; inline items lose the `{__type, __var}` wrapper | Migration 0101 | Migration 0101 |
| `AddAllowedRunTypes` | #61833 | New `dag.allowed_run_types`, a sorted list of strings or null, always written | Set `None` if missing | Drop (lossy if not null); the `dag` object is closed |
| `AlwaysWriteDagLimits` | #65310 (3.2.2) | Schema defaults removed for `catchup`, `max_active_tasks`, `max_active_runs`, `max_consecutive_failed_dag_runs` and `disable_bundle_versioning`, so they are always written | When a key is missing, set the old default (false/16/16/0/false). In 3.1.0 to 3.2.1 a missing key meant exactly that. This is safe for Lang SDK Dags because `fill_config_defaults` runs before `validate_serialized_dag` calls `from_dict` (`lang_sdk_processor.py:430-432`). | No-op |

### `2026-06-30` (3.3.0, stamp `3`)

| Class | PR | Change | Upgrade | Downgrade |
|---|---|---|---|---|
| `AddRerunWithLatestVersion` | #63884 | New `dag.rerun_with_latest_version`, written only when not null | No-op | Drop (lossy) |
| `AddTaskGroupDocMd` | #67207 | New task group `doc_md`, written only when not null | No-op | Drop; the `task_group` object is closed (lossy) |
| `ReplaceRetryPolicyWithFlag` | #69243 | `partial_kwargs.retry_policy` and `default_args` `retry_policy` (repr) become `has_retry_policy: true` | No-op; the old form only existed in 3.3.0 pre-releases | Drop `has_retry_policy` (lossy) |

### `2026-10-30` (3.4.0, stamp `3`)

| Class | PR | Change | Upgrade | Downgrade |
|---|---|---|---|---|
| `AddBundleName` | #68583 | New `dag.bundle_name`, written only when set, excluded from the hash | No-op; the value lives in `DagModel.bundle_name` | Drop; the `dag` object is closed |
| `AddStubTaskBindings` | #69757 | New task keys `is_stub: true` and `_arg_bindings` (list of `{name, kind, value_schema, task_id, value, from_default}`) | Set `is_stub` when `_task_module == "airflow.providers.standard.decorators.stub"` and `task_type == "_StubOperator"`; `_arg_bindings` can't be backfilled | Drop both (lossy) |
| `AddTaskGroupLoop` | #74345 | New task group object `loop: {max_iterations, has_until, terminal_task_id, gate_task_id}` | No-op | Drop and warn (D11); the `task_group` object is closed |
| `AddReturnsDagResult` | #74346 | New task key `returns_dag_result: true`, never written when false | No-op | Drop (lossy) |

### `2027-02-28` (3.5.0, stamp `"2027-02-28"`, in progress)

Empty in this PR. #70570 adds `AddMappedArgBindingParams` (task key `_mapped_arg_binding_params`; upgrade no-op, downgrade drops it).

### Commits that are not format changes

- **Reader only:** #49708, #51494, #55833, #55906, #55111, #56694, #56733, #58038, #60414, #66002, #68831, #68511, #75765.
- **Schema only:** #70148, #73644, #74041.
- **Internal:** #48487, #49445, #50641, #51326, #51530, #52176, #52072, #52644, #52693, #53004, #52234, #53289, #53149, #53153, #54321, #53687, #54420, #53223, #54428, #53696, #53918, #54461, #53389, #53071, #54816, #54979, #55037, #55043, #55108, #49164, #54383, #52325, #55197, #55169, #54752, #55538, #55681, #55836 (the bump to 3 itself), #55860, #55088, #55971, #55998, #56692, #56346, #57269, #57679, #57215, #57360, #57703, #57700, #58258, #54505, #56187, #59139, #58993, #59596, #59627, #59708, #59776, #59777, #59628, #59780, #59828, #59851, #59922, #60112, #60799, #60204, #61661, #65407, #66343, #67701, #67702, #67895, #68528, #69864, #74869.

## 6. Migrations

- **`airflow-core/src/airflow/migrations/utils.py`:** add `downgrade_serialized_dags(conn, *, to: int | str) -> None`.
  - Plain SQL on the stable columns `id`, `data`, `data_compressed` and `dag_hash`. No ORM import: the model changes over time, which is why 0101 copied its helpers.
  - Batches keyed by `id`. `data` is JSON or JSONB (a dict or a string depending on the dialect); `data_compressed` is zlib.
  - Calls `versions.downgrade(blob, to=to)` and writes back to the same column.
  - Recomputes `dag_hash` with a frozen copy of the hash logic: sort recursively, drop `dag.fileloc` and `dag.bundle_name`, and drop `__version` only when the target stamp is a date (K1).
- **Also in `utils.py`:** add `get_serialized_dag_downgrade_target() -> int | str`.
  - It takes alembic's destination from `alembic.context.get_revision_argument()`.
  - It resolves that revision's module attribute `airflow_version` through `alembic.script.ScriptDirectory`, maps it with `versions.get_version_for_airflow`, then applies the K4 stop rule.
- **New migration `0155_3_5_0_downgrade_serialized_dag_format.py`:**
  - `upgrade()` does nothing, because blobs are upgraded on read.
  - `downgrade()` calls `downgrade_serialized_dags(op.get_bind(), to=get_serialized_dag_downgrade_target())`. In offline mode it prints the K6 notice instead.
  - Update `_REVISION_HEADS_MAP` in `airflow-core/src/airflow/utils/db.py` and `airflow-core/docs/migrations-ref.rst` with the usual tooling.
- **`0085_3_1_0_downgrade_serialized_dag_version_to_v2.py`:** `downgrade()` becomes `downgrade_serialized_dags(op.get_bind(), to=2)`, keeping its offline notice. This also fixes the zlib bug (K5).
- **Migration 0101:** unchanged.

## 7. Prek

New script `scripts/ci/prek/check_serialized_dag_versions.py`, registered in `.pre-commit-config.yaml` with files `^airflow-core/src/airflow/serialization/(schema\.json|versions/.*\.py)$`. It reuses the target-branch helpers in `scripts/ci/prek/check_supervisor_schemas_versions.py`, or moves them to `common_prek_utils.py` as that file's TODO suggests. Two checks:

1. `schema.json` differs from the target branch, but no `versions/v*.py` file was added or changed: fail. Tell the author to add a change class to the newest version file.
2. A `versions/v*.py` file that exists on the target branch changed, and it is not the newest one: fail. Migrations depend on released files never changing.

Add tests under `scripts/tests/ci/prek/`.

## 8. Tests

- **`airflow-core/tests/unit/serialization/versions/test_engine.py`:**
  - Ordering by position, stamp lookup for upgrade and downgrade, unknown stamp, downgrade below `2`.
  - A v1 blob reaches head (regression for the chaining bug).
  - The warning on lossy downgrades.
- **`airflow-core/tests/unit/serialization/versions/test_changes.py`:**
  - Parametrized over every change class: applying upgrade twice, or downgrade twice, gives the same result.
  - A check of the actual behaviour for each class whose upgrade or downgrade does more than drop a key: `AddClientDefaults`, `ReplaceTaskCallbacksWithFlags`, `ExcludeDagSchemaDefaults`, `OmitDefaultRetryDelay`, `ReplaceDefaultArgsCallbacksWithFlags`, `AllowRetryBackoffMultiplier`, `AddPartitionedAssetTimetable`, `DropEventsTimetableSummary`, `EncodeMappedArgument`, `AlwaysWriteDagLimits`, `AddStubTaskBindings`.
- **Round trip:** v1, v2 and v3 fixture blobs upgraded to head deserialize to the same Dag as today's `from_dict`. Reuse the v1 fixture in `test_dag_serialization.py` near line 4054, which currently calls the conversion functions directly.
- **`airflow-core/tests/unit/migrations/test_serialized_dag_downgrade.py`** (sqlite, `db_test`):
  - Raw and compressed rows are downgraded, and hashes are recomputed per K1.
  - Destination resolution for 3.4.0, 3.2.0, 3.1.0 and 3.0.0 (the K4 stop rule).
- **`airflow-core/tests/unit/models/test_serialized_dag.py`:** the hash ignores `__version`.
- **`dag_version_diff`:** accepts a date stamp.

## 9. Verification

```bash
uv run --project airflow-core pytest airflow-core/tests/unit/serialization airflow-core/tests/unit/models/test_serialized_dag.py airflow-core/tests/unit/dag_processing/test_lang_sdk_processor.py airflow-core/tests/unit/migrations -q
uv run --project scripts pytest scripts/tests/ci/lang_sdk_serialization scripts/tests/ci/prek -q
(cd ts-sdk && pnpm test)
(cd go-sdk && go test ./...)
SKIP=check-supervisor-schemas-versions,regenerate-java-sdk-verification-metadata prek run --from-ref upstream/main
```

- **Java:** run Gradle tests only with `-Dmaven.repo.local` pointed at a scratch directory, never `~/.m2`.
- **End-to-end downgrade** with a temporary `AIRFLOW_HOME` and sqlite, run in Breeze:
  1. `airflow db migrate`, then parse the example Dags with raw rows and again with `[core] compress_serialized_dags = True`.
  2. `airflow db downgrade -n 3.4.0 -y`: every row is stamped `3`, and the 3.5 fields are gone.
  3. `airflow db downgrade -n 3.1.0 -y`: every row is stamped `2`, `client_defaults` is gone, and its values are in the tasks.
- **Postgres and MySQL:** repeat the round trip in Breeze with `--backend postgres` and `--backend mysql` to cover JSONB and MySQL JSON.

## 10. Sequencing

1. Branch `fix/serialization/strict-task-schema-validation`: strict `tasks` validation.
2. This branch: framework, backfill, migrations, prek checks, Lang SDK stamps. Suggested commits: engine with callers; backfill classes; migrations; prek; Lang SDK stamps.
3. #70570 rebases and adds `AddMappedArgBindingParams` to `v2027_02_28.py`.
4. Separately, a minimal backportable fix for the 0085 zlib bug.

## 11. Related findings

- On main and in 3.2.0 to 3.4.0rc2, a Dag using `airflow.sdk.EventsTimetable` fails `DagSerialization.to_dict` with `KeyError: '_summary'` (`airflow/timetables/events.py:140`, reached from `detect_dag_dependencies`). Open PR #69401 fixes it. Because of this, no stored blob lacks `_summary`, and `DropEventsTimetableSummary` matters only for downgrades.
- Strict validation of task objects (the other branch) found four stale rules in `schema.json`, unnoticed since 3.1. That shows how far the schema and the writer can drift without a forcing check.
