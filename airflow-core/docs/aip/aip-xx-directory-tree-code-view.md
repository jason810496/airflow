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

# AIP-XX: Directory-Tree Code View for Dag Bundles

| | |
|---|---|
| **State** | Draft |
| **Discussion thread** | TBD |
| **Champion** | TBD |
| **Created** | 2026-07-26 |
| **AIP number** | to be assigned |
| **Depends on** | Object-store bundle versioning (branch `feature/dag-bundles/object-store-versioning`) |

## Motivation

Today the UI Code tab shows only the single parsed Dag file, read from the `dag_code` metadata table, which snapshots the file's full source at parse time. Real Dags import helpers, SQL, YAML/JSON config, and other modules that live next to them in the bundle, and none of those are inspectable from the UI. The naive fix -- storing the whole directory tree in `dag_code` -- bloats the metadata database (source is already duplicated per Dag and per version) and still would not respect the boundary that the API server must not load or execute bundle code.

This AIP adds a directory-tree code view: users can browse and read every relevant file of a Dag's bundle at a given version, while the metadata database stores only per-file metadata and the API server never stores, proxies, or executes bundle source.

## Goals

- Browse the file tree of a Dag's bundle at a specific version, and read any individual file, read-only, from the UI.
- Keep the metadata database small: store per-file metadata only, never file content.
- Preserve the architectural boundary: the API server does not check out bundles and does not execute user code.
- Respect the existing per-`dag_id` code authorization model.

## Non-goals

- Cross-version tree diff (individual-file history/diff may follow later).
- Editing or writing bundle files from the UI.
- Capturing files imported from outside the bundle root (arbitrary `sys.path` modules), or dynamically/`importlib`-loaded files.
- Supporting git remotes that expose no raw/contents API (self-hosted SSH bare repos, plain git, CodeCommit); these are explicitly unsupported and degrade to the single-file view.

## Background and constraints

- The API server has no access to Dag bundles by design (`airflow-core/docs/core-concepts/overview.rst`, `airflow-core/docs/security/security_model.rst`): the Code tab is served from the metadata database, and a deployment manager may run the API server without bundle credentials.
- Code authorization is per-`dag_id` via `DagAccessEntity.CODE`, finer than per-bundle under the FAB/base auth manager; the current `GET /dagSources/{dag_id}` endpoint applies a redaction overlay so a multi-Dag file does not leak co-located Dags the caller cannot read.
- `DagCode` is 1:1 with `DagVersion` and stores full source keyed per Dag, so a file defining N Dags is stored N times.
- `DagVersion` carries `bundle_name` and `bundle_version` (a git commit sha for git bundles); only `GitDagBundle` sets `supports_versioning = True` today.
- A bundle is a whole on-disk tree exposed via `BaseDagBundle.path`, materialized only on the Dag processor and workers. `view_url_template` already supports `{version}` and `{subdir}` placeholders and is rendered from `DagBundleModel.signed_url_template`.
- A bundle maps to at most one team (a unique index on `dag_bundle_name` in the bundle/team association table enforces this at the DB level); a team may own many bundles.

## Design

### The trilemma

Whole-tree code view cannot simultaneously satisfy all three of: (A) the API server stays credential-free, (B) no source is stored in the metadata database, and (C) the UI browses the whole tree. This AIP keeps **(B) + (C)** and spends a minimal slice of **(A)**: for supported hosted backends the API server may hold a scoped read/mint credential, but it never stores file bytes, never proxies bytes, and never checks out a bundle. Source bytes always travel from the backing store to the user's browser directly, via a short-lived URL.

### Overview

1. A new `BundleVersionFile` table stores per-file metadata once per `(bundle_name, bundle_version)`, populated by the Dag processor when a new bundle version is first observed.
2. The API server lists a Dag's tree from `BundleVersionFile` (no bundle access), filtered by per-file authorization.
3. To open a file, the API server returns a source reference -- a URL the browser fetches directly from the backing store -- resolved through a narrow bundle interface. It never returns bytes itself.
4. Only versioned bundles participate; unsupported bundles/versions degrade to the existing single-file view.

### `BundleVersionFile` table

The inventory is a property of a bundle commit, not of any Dag, so it is keyed by `(bundle_name, bundle_version)` and shared by every `DagVersion` at that commit. It stores metadata only -- never content.

```python
class BundleVersionFile(Base):
    """One row per source file in a bundle at a specific version. Metadata only, no content."""

    __tablename__ = "bundle_version_file"

    id = mapped_column(Uuid(), primary_key=True, default=uuid7)
    bundle_name = mapped_column(StringID(), ForeignKey("dag_bundle.name", ondelete="CASCADE"), nullable=False)
    bundle_version = mapped_column(String(200), nullable=False)
    relative_fileloc = mapped_column(String(2000), nullable=False)
    relative_fileloc_hash = mapped_column(String(32), nullable=False)  # md5(path); backs the unique key
    file_content_hash = mapped_column(
        String(32), nullable=False
    )  # md5(bytes); change detection / future diff
    size = mapped_column(Integer, nullable=False)
    created_at = mapped_column(UtcDateTime, default=timezone.utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "bundle_name", "bundle_version", "relative_fileloc_hash", name="bundle_version_file_uq"
        ),
        Index("idx_bvf_lookup", "bundle_name", "bundle_version"),
    )
```

`relative_fileloc` (`String(2000)`) cannot go into an index or unique constraint directly -- `DagCode.fileloc` is deliberately left unindexed for the same MySQL index-key-length reason -- so the unique key uses `relative_fileloc_hash`. `file_content_hash` is used only for change detection and future per-file diff. It is populated from backend-native metadata (git blob sha via `git ls-tree`, or S3/GCS ETag/MD5), never by reading each file -- the benchmark below shows reads dominate the build -- and is dropped where no native hash exists.

Relationship: `bundle_name` is a real FK to `dag_bundle.name` (cascade on bundle deletion). There is deliberately **no** FK to `DagVersion`: many `DagVersion` rows (one per `dag_id` at a commit) share one inventory, so the link to `DagVersion` is a logical join on `(bundle_name, bundle_version)` -- both columns already exist on `DagVersion`. A future normalization could introduce a `BundleVersion(bundle_name, version)` entity and migrate `DagVersion.bundle_version` to reference it, giving cascade-based cleanup; that migration is more invasive and is out of scope for the first iteration.

### Source-fetch modes

The API server never returns bytes; it resolves a file to a browser-fetchable URL. Which mode applies is a per-bundle configuration, not runtime detection.

| Mode | Backend fit | API-server credential | Durable | Notes |
|---|---|---|---|---|
| Static URL | object store (presigned), public git (raw via template) | signing only / none | no | rendered from `view_url_template` with a `{relative_fileloc}` placeholder |
| Live STS URL | private hosted git (GitHub, GitLab, ...) via provider adapter | mint credential + one API call per file-open | no | short-lived, file-scoped token; cannot be precomputed or stored |
| Unsupported | self-hosted/SSH/plain git with no raw/contents API | none | n/a | tree view unavailable; degrade to single-file `dag_code` view |

URLs are minted lazily on file-open, never for the whole tree: tree listing is served entirely from `BundleVersionFile`, so opening one file makes at most one backend/API call, bounding provider rate-limit exposure to actual views. Because the browser fetches the bytes, this requires browser egress and CORS to the backing store; air-gapped deployments where only the API server can reach the store are not served by URL mode.

### Narrow bundle interface

The API server is given only a fetch capability, exposed as a separate protocol that bundles opt into; it must never see `path`, `initialize`, or `refresh`.

```python
class RemoteSourceFetcher(Protocol):
    def get_source(self, relative_fileloc: str, version: str) -> SourceRef: ...
```

`SourceRef` is a discriminated result carrying a URL (static or freshly minted). Object-store bundles return a presigned URL; the git bundle returns a public raw URL (via template) or a minted STS URL through a per-provider adapter, and raises "unsupported" when no adapter matches. Keeping this a distinct protocol (rather than blocking methods on the full bundle object at runtime) makes the API server's single capability statically clear.

### API endpoints

- `GET /dagSources/{dag_id}/tree?version_number=` -- resolve `DagVersion` to `(bundle_name, bundle_version)`, list `BundleVersionFile` rows for that tuple, filter by per-file authorization, return the tree. No bundle access.
- `GET /dagSources/{dag_id}/files/{relative_fileloc}?version_number=` -- re-check authorization, re-validate the path against the recorded, allowed inventory (path-traversal guard), resolve to a `SourceRef` via `get_source`, and return it. Unsupported bundle or missing inventory returns a signal to degrade.
- `GET /dagSources/{dag_id}` -- unchanged; remains the single-file, DB-sourced view and the degradation fallback.

### Authorization

Authorization is per file, all-or-nothing, layered on top of the existing `DagAccessEntity.CODE` check. For each inventory file the co-located `dag_ids` are derived by joining `DagModel` on `(bundle_name, relative_fileloc)` (the same key the redaction overlay and import-error matching use):

- File defines Dags: require read on **all** of them (reuse the existing overlay logic). A failing file is omitted from the tree and returns 403 on open. This subsumes the old byte-level `REDACTED_SOURCE` overlay -- visibility is decided per file, so no partial redaction is needed in the tree view.
- File defines no Dag (a helper): require read on **at least one** Dag in the bundle. This is safe because a bundle maps to at most one team, so a helper is never exposed across a team boundary.

Under `SimpleAuthManager` (which ignores `dag_id`) authorization is by role, as today. The tree's authorization cost scales with the number of Dags in the bundle, not the number of files: the bundle's `DagModel` rows are loaded once per request and the `relative_fileloc -> dag_ids` map is built in memory (mirroring `deactivate_deleted_dags`); a composite index on `(bundle_name, relative_fileloc)` can be added later if profiling requires it.

### Inventory population

`BundleVersionFile` is populated by the Dag processor -- the only process that has both the bundle checkout and metadata-database access (per-file parse subprocesses have neither). It runs at bundle refresh, once per new `(bundle_name, bundle_version)`:

1. Read the current `bundle_version`; skip if the tuple already has rows (idempotent across restarts, repeated refreshes, and the many Dag files sharing the commit), guarded by the bundle lock or the unique constraint.
2. Walk `bundle.path`, apply the extension filter, and bulk-insert one row per surviving file with its path, hashes, and size.

`is_dag_file` is not stored: Dag-vs-helper is derived at request time from `DagModel`, so the build stays a pure function of the filesystem walk with no post-parse reconciliation. Cost is one walk plus one bulk insert per new commit, off the per-file parse path. The populate function takes a keyword-only `session` and does not commit; the processor's existing commit point flushes it.

Benchmark (`dev/bundle_version_file_benchmark/`), once-per-commit build of a 100k-file bundle (median of 3, warm cache):

| phase | SQLite | Postgres |
|---|---:|---:|
| fs walk + stat (metadata only) | 0.7s | 0.7s |
| fs read + hash every file | 4.8s | 3.9s |
| DB bulk insert (chunked executemany) | 0.5s | 1.7s |

Reading files to hash them is 8-10x the DB cost and the single largest term, so `file_content_hash` is taken from backend-native metadata (git blob sha, object ETag), not from reads. The build then stays at ~1-2.5s at 100k files (tens of ms for realistic 1k-10k bundles), on the parse path, no background deferral.

### File filter

An allow-list (default `.py`, extensible by the deployment manager) or deny-list (e.g. exclude `.env`) governs which files are recorded. It is enforced both at inventory-write (primary control) and at file-open (defense in depth): the API server serves only paths that are recorded, allowed inventory rows, which also doubles as the path-traversal guard.

### Garbage collection

`BundleVersionFile` has no FK to `DagVersion` and therefore does not cascade when a `DagVersion` is deleted; its rows become orphaned only when the last `DagVersion` at that `(bundle_name, bundle_version)` is gone. Cleanup is an orphan-prune entry added to `db_cleanup.config_list`, run by operator-invoked `airflow db clean`: delete rows whose `(bundle_name, bundle_version)` no longer appears in `dag_version`, batched with `LIMIT` per the bulk-delete standard. Deleting a bundle cascades via the `bundle_name` FK.

### Configuration

A feature flag gates both population (Dag processor) and serving (API server); default off. Bundles opt into static-URL mode by configuring `view_url_template` with a `{relative_fileloc}` placeholder, and into STS mode via provider-adapter configuration and a scoped API-server credential.

## Security model impact

The API server may hold a scoped read/mint credential for supported hosted backends -- a documented, opt-in relaxation of the "API server needs no bundle credentials" property. It never stores or proxies file bytes, never checks out a bundle, and never executes user code; the "Scheduler and API Server do not need Dag files" property is otherwise preserved. Minted URLs are short-lived and file-scoped, and bytes flow store-to-browser. `security_model.rst` and `overview.rst` must be updated to describe the relaxation and its default-off nature.

## Durability

Historical source is not guaranteed. An old `bundle_version` that has been garbage-collected or force-pushed at the remote returns 404, and the view degrades to the single-file `dag_code` snapshot (which remains durable). This is an accepted trade-off of storing metadata only; deployments needing durable historical trees should retain bundle history at the source.

## Backward compatibility and migration

Additive only: a new table (one migration), new endpoints, a new optional bundle protocol, and a new opt-in flag. `dag_code`, `DagCode`, and `GET /dagSources/{dag_id}` are untouched and remain the fallback. With the flag off, behavior is identical to today. Old bundle versions parsed before the feature have no inventory and degrade to the single-file view.

## Dependencies

- Object-store bundle versioning is required to extend this beyond Git to S3/GCS (branch `feature/dag-bundles/object-store-versioning`). Until it lands, this feature is Git-only, since `GitDagBundle` is the only versioned backend. Object-store versioning must retain object content for a version, not merely stamp a version label, for its files to be fetchable.

## What defines this AIP as done

- `BundleVersionFile` table and migration.
- Dag-processor population at bundle refresh, idempotent and filtered, plus the `db_cleanup` orphan-prune.
- The `RemoteSourceFetcher` protocol and a `GitDagBundle` adapter covering public raw URLs and at least one private STS provider (GitHub).
- `tree` and `file` endpoints with the per-file authorization rule; existing single-file endpoint kept as fallback.
- UI tree browser in the Code tab, with graceful degradation to the single-file view.
- Security-model documentation updated for the opt-in credential relaxation.
- Tests covering exactly the changed behavior at 100%.
