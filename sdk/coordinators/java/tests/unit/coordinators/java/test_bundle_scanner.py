#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import os
import zipfile
from pathlib import Path

import pytest
import yaml

from airflow.sdk.coordinators.java.bundle_scanner import (
    DAG_CODE_MANIFEST_KEY,
    MAIN_CLASS_MANIFEST_KEY,
    MANIFEST_PATH,
    METADATA_MANIFEST_KEY,
    SCHEMA_VERSION_MANIFEST_KEY,
    SDK_VERSION_MANIFEST_KEY,
    BundleScanner,
    ResolvedJarBundle,
    _jar_files,
    _normalize_bundle_home,
    _parse_dag_ids_from_metadata,
    _read_bundle_jar,
    read_dag_code,
    read_schema_version,
)

METADATA_YAML_PATH = "META-INF/airflow-metadata.yaml"
DAG_CODE_PATH = "JavaExample.java"
TEST_MAIN_CLASS = "com.example.MyDag"
TEST_SDK_VERSION = "1.0.0"
TEST_SCHEMA_VERSION = "2026-04-06"
"""Default Airflow-SDK-Supervisor-Schema-Version stamped onto bundle JARs.

The attribute is required, so the helper produces a valid bundle by default.
Tests that need to exercise the missing-attribute failure path pass
``schema_version=None`` explicitly.
"""


def _make_manifest(
    *,
    main_class: str | None = TEST_MAIN_CLASS,
    metadata_path: str | None = METADATA_YAML_PATH,
    sdk_version: str | None = TEST_SDK_VERSION,
    schema_version: str | None = TEST_SCHEMA_VERSION,
    dag_code_path: str | None = None,
) -> str:
    lines = ["Manifest-Version: 1.0"]
    if main_class:
        lines.append(f"{MAIN_CLASS_MANIFEST_KEY}: {main_class}")
    if metadata_path:
        lines.append(f"{METADATA_MANIFEST_KEY}: {metadata_path}")
    if sdk_version:
        lines.append(f"{SDK_VERSION_MANIFEST_KEY}: {sdk_version}")
    if schema_version:
        lines.append(f"{SCHEMA_VERSION_MANIFEST_KEY}: {schema_version}")
    if dag_code_path:
        lines.append(f"{DAG_CODE_MANIFEST_KEY}: {dag_code_path}")
    return "\n".join(lines) + "\n"


def _make_metadata_yaml(dag_ids: list[str]) -> str:
    return yaml.dump({"dags": {dag_id: {} for dag_id in dag_ids}})


def _create_bundle_jar(
    jar_path: Path,
    *,
    dag_ids: list[str] | None = None,
    main_class: str | None = TEST_MAIN_CLASS,
    include_metadata: bool = True,
    include_manifest: bool = True,
    dag_code: str | None = None,
    schema_version: str | None = TEST_SCHEMA_VERSION,
) -> Path:
    """Create a minimal JAR (zip) file with Airflow Java SDK manifest attributes."""
    with zipfile.ZipFile(jar_path, "w") as zf:
        if include_manifest:
            dag_code_path = DAG_CODE_PATH if dag_code else None
            manifest = _make_manifest(
                main_class=main_class,
                metadata_path=METADATA_YAML_PATH if include_metadata else None,
                schema_version=schema_version,
                dag_code_path=dag_code_path,
            )
            zf.writestr(MANIFEST_PATH, manifest)

        if include_metadata and dag_ids is not None:
            zf.writestr(METADATA_YAML_PATH, _make_metadata_yaml(dag_ids))

        if dag_code:
            zf.writestr(DAG_CODE_PATH, dag_code)
    return jar_path


class TestJarFiles:
    def test_lists_jar_files_sorted(self, tmp_path: Path):
        (tmp_path / "b.jar").touch()
        (tmp_path / "a.jar").touch()
        (tmp_path / "c.txt").touch()
        result = _jar_files(tmp_path)
        assert result == [tmp_path / "a.jar", tmp_path / "b.jar"]

    def test_returns_empty_for_nonexistent_directory(self, tmp_path: Path):
        assert _jar_files(tmp_path / "nonexistent") == []

    def test_returns_empty_for_directory_with_no_jars(self, tmp_path: Path):
        (tmp_path / "readme.txt").touch()
        assert _jar_files(tmp_path) == []

    def test_ignores_jar_directories(self, tmp_path: Path):
        (tmp_path / "fake.jar").mkdir()
        assert _jar_files(tmp_path) == []


class TestNormalizeBundleHome:
    def test_jar_file_returns_parent(self, tmp_path: Path):
        jar = tmp_path / "bundle.jar"
        jar.touch()
        assert _normalize_bundle_home(jar) == tmp_path.resolve()

    def test_dir_with_lib_containing_jars(self, tmp_path: Path):
        lib = tmp_path / "lib"
        lib.mkdir()
        (lib / "dep.jar").touch()
        assert _normalize_bundle_home(tmp_path) == lib.resolve()

    def test_dir_with_empty_lib(self, tmp_path: Path):
        lib = tmp_path / "lib"
        lib.mkdir()
        assert _normalize_bundle_home(tmp_path) == tmp_path.resolve()

    def test_plain_directory(self, tmp_path: Path):
        assert _normalize_bundle_home(tmp_path) == tmp_path.resolve()


class TestParseDagIdsFromMetadata:
    def test_parses_dag_ids(self):
        content = yaml.dump({"dags": {"dag_a": {}, "dag_b": {"key": "val"}}})
        assert _parse_dag_ids_from_metadata(content) == {"dag_a", "dag_b"}

    @pytest.mark.parametrize(
        "yaml_content",
        [
            pytest.param(yaml.dump({"other": 1}), id="missing_dags_key"),
            pytest.param("just a string", id="non_dict"),
            pytest.param(yaml.dump({"dags": {}}), id="empty_dags"),
        ],
    )
    def test_returns_empty_set(self, yaml_content):
        assert _parse_dag_ids_from_metadata(yaml_content) == set()


def _write_bad_zip(tmp_path: Path, name: str = "bad.jar") -> Path:
    """Create a non-zip file with a ``.jar`` suffix to exercise the bad-archive path."""
    jar = tmp_path / name
    jar.write_text("not a zip")
    return jar


def _write_jar_with_missing_internal_file(
    tmp_path: Path,
    name: str,
    *,
    metadata_path: str | None = None,
    dag_code_path: str | None = None,
) -> Path:
    """Create a JAR whose manifest points at an internal file that does not exist."""
    jar = tmp_path / name
    with zipfile.ZipFile(jar, "w") as zf:
        manifest = _make_manifest(metadata_path=metadata_path, dag_code_path=dag_code_path)
        zf.writestr(MANIFEST_PATH, manifest)
    return jar


class TestReadBundleJar:
    def test_valid_jar(self, tmp_path: Path):
        jar = _create_bundle_jar(tmp_path / "valid.jar", dag_ids=["my_dag"])
        result = _read_bundle_jar(jar)
        assert result is not None
        main_class, dag_ids, schema_version = result
        assert main_class == TEST_MAIN_CLASS
        assert dag_ids == {"my_dag"}
        assert schema_version == TEST_SCHEMA_VERSION

    def test_multiple_dag_ids(self, tmp_path: Path):
        jar = _create_bundle_jar(tmp_path / "multi.jar", dag_ids=["dag_1", "dag_2", "dag_3"])
        result = _read_bundle_jar(jar)
        assert result is not None
        _, dag_ids, _ = result
        assert dag_ids == {"dag_1", "dag_2", "dag_3"}

    @pytest.mark.parametrize(
        "make_jar",
        [
            pytest.param(
                lambda p: _create_bundle_jar(p / "no_manifest.jar", include_manifest=False),
                id="missing-manifest",
            ),
            pytest.param(
                lambda p: _create_bundle_jar(p / "no_meta.jar", include_metadata=False),
                id="missing-metadata-attribute",
            ),
            pytest.param(
                lambda p: _create_bundle_jar(p / "no_main.jar", dag_ids=["d"], main_class=None),
                id="missing-main-class",
            ),
            pytest.param(
                lambda p: _create_bundle_jar(p / "no_schema.jar", dag_ids=["d"], schema_version=None),
                id="missing-schema-version",
            ),
            pytest.param(
                lambda p: _write_jar_with_missing_internal_file(
                    p, "missing_meta_file.jar", metadata_path="nonexistent.yaml"
                ),
                id="metadata-file-missing-inside-jar",
            ),
            pytest.param(_write_bad_zip, id="bad-zip"),
            pytest.param(
                lambda p: _create_bundle_jar(p / "empty_dags.jar", dag_ids=[]),
                id="empty-dag-ids",
            ),
        ],
    )
    def test_returns_none_when_jar_is_not_a_valid_bundle(self, tmp_path: Path, make_jar):
        assert _read_bundle_jar(make_jar(tmp_path)) is None


class TestReadSchemaVersion:
    def test_returns_version_when_present(self, tmp_path: Path):
        jar = _create_bundle_jar(tmp_path / "with_schema.jar", dag_ids=["d"], schema_version="2026-04-06")
        assert read_schema_version(jar) == "2026-04-06"

    @pytest.mark.parametrize(
        ("make_jar", "match"),
        [
            pytest.param(
                lambda p: _create_bundle_jar(p / "no_schema.jar", dag_ids=["d"], schema_version=None),
                "missing required manifest attribute",
                id="missing-schema-version-attribute",
            ),
            pytest.param(
                lambda p: _create_bundle_jar(p / "no_manifest.jar", include_manifest=False),
                "has no manifest",
                id="missing-manifest",
            ),
            pytest.param(_write_bad_zip, "not a valid zip archive", id="bad-zip"),
        ],
    )
    def test_raises_when_schema_version_unavailable(self, tmp_path: Path, make_jar, match):
        with pytest.raises(ValueError, match=match):
            read_schema_version(make_jar(tmp_path))


class TestReadDagCode:
    def test_reads_embedded_dag_code(self, tmp_path: Path):
        code = "public class MyDag {}"
        jar = _create_bundle_jar(tmp_path / "with_code.jar", dag_ids=["d"], dag_code=code)
        assert read_dag_code(jar) == code

    @pytest.mark.parametrize(
        "make_jar",
        [
            pytest.param(
                lambda p: _create_bundle_jar(p / "no_code.jar", dag_ids=["d"]),
                id="missing-dag-code-attribute",
            ),
            pytest.param(
                lambda p: _create_bundle_jar(p / "no_manifest.jar", include_manifest=False),
                id="missing-manifest",
            ),
            pytest.param(_write_bad_zip, id="bad-zip"),
            pytest.param(
                lambda p: _write_jar_with_missing_internal_file(
                    p, "broken_code.jar", dag_code_path="missing_source.py"
                ),
                id="dag-code-file-missing-inside-jar",
            ),
        ],
    )
    def test_returns_none_when_dag_code_unavailable(self, tmp_path: Path, make_jar):
        assert read_dag_code(make_jar(tmp_path)) is None


class TestBundleScannerResolveJar:
    def test_returns_main_class(self, tmp_path: Path):
        jar = _create_bundle_jar(tmp_path / "bundle.jar", dag_ids=["d"])
        assert BundleScanner.resolve_jar(jar) == TEST_MAIN_CLASS

    def test_raises_for_invalid_jar(self, tmp_path: Path):
        jar = tmp_path / "not_bundle.jar"
        jar.write_text("not a zip")
        with pytest.raises(FileNotFoundError, match="Not a valid Airflow Java SDK bundle"):
            BundleScanner.resolve_jar(jar)


class TestBundleScannerCandidateHomes:
    def test_nested_layout(self, tmp_path: Path):
        sub_a = tmp_path / "bundle_a"
        sub_a.mkdir()
        (sub_a / "app.jar").touch()

        sub_b = tmp_path / "bundle_b"
        sub_b.mkdir()
        (sub_b / "app.jar").touch()

        scanner = BundleScanner(tmp_path)
        homes = scanner._candidate_homes()
        assert len(homes) == 3
        assert sub_a.resolve() in homes
        assert sub_b.resolve() in homes
        assert tmp_path.resolve() in homes

    def test_flat_layout(self, tmp_path: Path):
        (tmp_path / "app.jar").touch()
        scanner = BundleScanner(tmp_path)
        homes = scanner._candidate_homes()
        assert homes == [tmp_path.resolve()]

    def test_nested_with_lib_subdir(self, tmp_path: Path):
        sub = tmp_path / "my_bundle"
        sub.mkdir()
        lib = sub / "lib"
        lib.mkdir()
        (lib / "dep.jar").touch()

        scanner = BundleScanner(tmp_path)
        homes = scanner._candidate_homes()
        assert lib.resolve() in homes


class TestBundleScannerResolve:
    def test_finds_matching_dag(self, tmp_path: Path):
        bundle_dir = tmp_path / "my_bundle"
        bundle_dir.mkdir()
        _create_bundle_jar(bundle_dir / "app.jar", dag_ids=["target_dag"])

        scanner = BundleScanner(tmp_path)
        result = scanner.resolve("target_dag")
        assert isinstance(result, ResolvedJarBundle)
        assert result.main_class == TEST_MAIN_CLASS
        assert str((bundle_dir / "app.jar").resolve()) in result.classpath

    def test_raises_when_no_match(self, tmp_path: Path):
        bundle_dir = tmp_path / "my_bundle"
        bundle_dir.mkdir()
        _create_bundle_jar(bundle_dir / "app.jar", dag_ids=["other_dag"])

        scanner = BundleScanner(tmp_path)
        with pytest.raises(FileNotFoundError, match="No JAR bundle containing dag_id='missing'"):
            scanner.resolve("missing")

    def test_classpath_includes_all_jars(self, tmp_path: Path):
        bundle_dir = tmp_path / "my_bundle"
        bundle_dir.mkdir()
        _create_bundle_jar(bundle_dir / "app.jar", dag_ids=["my_dag"])
        with zipfile.ZipFile(bundle_dir / "dep.jar", "w") as zf:
            zf.writestr("placeholder.class", b"")

        scanner = BundleScanner(tmp_path)
        result = scanner.resolve("my_dag")
        parts = result.classpath.split(os.pathsep)
        assert len(parts) == 2

    def test_flat_layout_resolve(self, tmp_path: Path):
        _create_bundle_jar(tmp_path / "app.jar", dag_ids=["flat_dag"])

        scanner = BundleScanner(tmp_path)
        result = scanner.resolve("flat_dag")
        assert result.main_class == TEST_MAIN_CLASS

    def test_skips_non_bundle_jars(self, tmp_path: Path):
        bundle_dir = tmp_path / "my_bundle"
        bundle_dir.mkdir()
        with zipfile.ZipFile(bundle_dir / "plain.jar", "w") as zf:
            zf.writestr("placeholder.class", b"")
        _create_bundle_jar(bundle_dir / "real.jar", dag_ids=["real_dag"])

        scanner = BundleScanner(tmp_path)
        result = scanner.resolve("real_dag")
        assert result.main_class == TEST_MAIN_CLASS

    def test_empty_bundles_dir(self, tmp_path: Path):
        scanner = BundleScanner(tmp_path)
        with pytest.raises(FileNotFoundError):
            scanner.resolve("any_dag")

    def test_propagates_schema_version(self, tmp_path: Path):
        bundle_dir = tmp_path / "my_bundle"
        bundle_dir.mkdir()
        _create_bundle_jar(bundle_dir / "app.jar", dag_ids=["versioned_dag"], schema_version="2026-04-06")

        scanner = BundleScanner(tmp_path)
        result = scanner.resolve("versioned_dag")
        assert result.schema_version == "2026-04-06"

    def test_jar_without_schema_version_is_not_a_valid_bundle(self, tmp_path: Path):
        # The supervisor needs to know which message-schema version to migrate to
        # before it can talk to the runtime, so a JAR missing the manifest
        # attribute is treated as not-a-bundle and skipped during resolve.
        bundle_dir = tmp_path / "my_bundle"
        bundle_dir.mkdir()
        _create_bundle_jar(bundle_dir / "app.jar", dag_ids=["plain_dag"], schema_version=None)

        scanner = BundleScanner(tmp_path)
        with pytest.raises(FileNotFoundError, match="No JAR bundle containing dag_id='plain_dag'"):
            scanner.resolve("plain_dag")
