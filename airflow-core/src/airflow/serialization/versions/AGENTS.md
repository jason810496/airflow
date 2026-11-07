<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Serialized Dag Format Versions — Agent Instructions

## What this package owns

The versions of the serialized Dag format (the JSON blob stored in
`serialized_dag.data` / `data_compressed`), and the engine that moves a blob
between them:

- `versions.upgrade(data)` — called by `DagSerialization.from_dict` before a blob is
  read. Old blobs are read for the whole major: Dag versioning keeps every
  `serialized_dag` row, and runs pinned to an old version read it.
- `versions.downgrade(data, to=...)` — called from alembic migrations, through
  `airflow.migrations.utils.downgrade_serialized_dags`, when `airflow db downgrade`
  goes to an older release.

`__version` in a blob is the version's **stamp**. `SERIALIZER_VERSION` is
`HEAD.stamp`.

## Files in this folder

- `__init__.py` — `VERSIONS` (ordered oldest first; order comes from the position,
  never from comparing ids), `HEAD`, `upgrade`, `downgrade`,
  `get_version_for_airflow`.
- `base.py` — `SerializedDagChange`, `Version` and small helpers to walk a blob.
- `v2.py`, `v3.py`, `vYYYY_MM_DD.py` — one file per version, holding its change
  classes and a `VERSION`. The newest file is the **in-progress** version.

## Ids and stamps

- `1`, `2` and `3` are integers. Every later version is the date of the Execution API
  version the release shipped (`airflow/api_fastapi/execution_api/versions/`). A patch
  release with no new Execution API version folds into the previous date.
- Every version from `3` through `2026-10-30` writes stamp `3`, because cores up to
  3.4 accept only `1`, `2` and `3`. So `upgrade` reads a `3` blob as the oldest version
  with that stamp and runs every later change, and `downgrade` reads it as the newest
  one. **Every `upgrade` and `downgrade` must be safe to apply twice.**
- From `2027-02-28` (Airflow 3.5) on, the stamp is the date.

## When making changes

### Every change to the format needs a change class

Add a class whenever a writer emits a different blob: a new key (even an additive,
optional one), a removed key, a renamed key, a changed value type, a new default that
stops a key from being written. A change to `schema.json` always needs one; the
`check-serialized-dag-versions` prek hook fails otherwise.

1. Open the in-progress `vYYYY_MM_DD.py` (the newest date).
2. Add a `SerializedDagChange` subclass with a docstring and `description = __doc__`.
3. Implement `upgrade` (previous version → this one) and `downgrade` (this one →
   previous), in place on the whole blob. Leave either as the inherited no-op when there
   is nothing to convert. Never write `__version`: the engine does.
4. Append the class to the file's `VERSION.changes`.
5. Add a test in `airflow-core/tests/unit/serialization/versions/test_changes.py`.

```python
class AddTaskFoo(SerializedDagChange):
    """Add task key ``foo``, written only when set."""

    description = __doc__

    def downgrade(self, data: dict[str, Any]) -> None:
        lossy = False
        for task in iter_tasks(data):
            lossy = task.pop("foo", None) is not None or lossy
        if lossy:
            self.warn_lossy(data, "dropped foo")
```

### Writing a change that is safe to apply twice

- Use `setdefault` and `pop(key, None)`, not unconditional writes or `del`.
- Check the current shape before converting a value (for example, only turn a number
  into a bool if it is still a number).
- Don't count or append: a second run must find nothing left to do.

### Lossy downgrades

When the older release cannot hold the data, drop it and call `self.warn_lossy(data,
detail)` once per change and Dag, and only when something was actually dropped. Never
raise: a refused change would block the whole `airflow db downgrade`, and Dags using the
feature fail to import on the older release anyway.

### `requires_migration`

Set `requires_migration = True` when the change needs other tables (for example
migration 0101, which moves deadline alerts to the `deadline_alert` table). The engine
skips such a change; its alembic migration does the work both ways. When downgrading,
the newest release's migration stops at the newest version above the destination that
holds such a change, and that change's migration finishes the downgrade with
`downgrade_serialized_dags(op.get_bind(), to=get_serialized_dag_downgrade_target(below=...))`.

### Releasing

The release manager freezes the in-progress file at release, without editing it (its
`airflow_version` already names the release):

1. Add the next file, named after the next release's Execution API date, with an empty
   `VERSION` whose `airflow_version` is the next release, and append it to `VERSIONS` in
   `__init__.py`.
2. Add an alembic revision for the new release whose `downgrade()` calls
   `downgrade_serialized_dags(op.get_bind(), to=get_serialized_dag_downgrade_target())`
   (see `0155_3_5_0_downgrade_serialized_dag_format.py`).

**A released version file never changes**: migrations of later releases depend on it.
The `check-serialized-dag-versions` prek hook fails when any version file other than the
newest one changes.

## Callers

- `DagSerialization.from_dict` and `dag_version_diff` call `upgrade`.
- Readers that bypass `from_dict` (for example `SerializedDagModel` reading
  `["dag"]["dag_dependencies"]` from the row) are not covered, and must keep tolerating
  older shapes.
