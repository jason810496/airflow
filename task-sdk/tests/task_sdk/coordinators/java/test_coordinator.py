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

import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml

from airflow.sdk.api.datamodels._generated import BundleInfo
from airflow.sdk.coordinators.java.bundle_scanner import (
    MAIN_CLASS_MANIFEST_KEY,
    MANIFEST_PATH,
    METADATA_MANIFEST_KEY,
    SCHEMA_VERSION_MANIFEST_KEY,
    SDK_VERSION_MANIFEST_KEY,
)
from airflow.sdk.coordinators.java.coordinator import JavaCoordinator
from airflow.sdk.execution_time.comms import StartupDetails
from airflow.sdk.execution_time.workloads.task import TaskInstanceDTO

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

if not AIRFLOW_V_3_3_PLUS:
    pytest.skip("Coordinator is only compatible with Airflow >= 3.3.0", allow_module_level=True)

METADATA_YAML_PATH = "META-INF/airflow-metadata.yaml"
DAG_CODE_PATH = "dag_source.py"
TEST_MAIN_CLASS = "com.example.MyBundle"
TEST_SCHEMA_VERSION = "2026-04-06"


def _make_manifest(
    *,
    main_class: str | None = TEST_MAIN_CLASS,
    metadata_path: str | None = METADATA_YAML_PATH,
    dag_code_path: str | None = None,
    schema_version: str | None = TEST_SCHEMA_VERSION,
) -> str:
    lines = ["Manifest-Version: 1.0"]
    if main_class:
        lines.append(f"{MAIN_CLASS_MANIFEST_KEY}: {main_class}")
    if metadata_path:
        lines.append(f"{METADATA_MANIFEST_KEY}: {metadata_path}")
    lines.append(f"{SDK_VERSION_MANIFEST_KEY}: 1.0.0")
    if schema_version:
        lines.append(f"{SCHEMA_VERSION_MANIFEST_KEY}: {schema_version}")
    if dag_code_path:
        lines.append(f"Airflow-Java-SDK-Dag-Code: {dag_code_path}")
    return "\n".join(lines) + "\n"


def _create_bundle_jar(
    jar_path: Path,
    *,
    dag_ids: list[str] | None = None,
    dag_code: str | None = None,
    schema_version: str | None = TEST_SCHEMA_VERSION,
) -> Path:
    with zipfile.ZipFile(jar_path, "w") as zf:
        dag_code_path = DAG_CODE_PATH if dag_code else None
        manifest = _make_manifest(dag_code_path=dag_code_path, schema_version=schema_version)
        zf.writestr(MANIFEST_PATH, manifest)
        if dag_ids is not None:
            metadata = yaml.dump({"dags": {d: {} for d in dag_ids}})
            zf.writestr(METADATA_YAML_PATH, metadata)
        if dag_code:
            zf.writestr(DAG_CODE_PATH, dag_code)
    return jar_path


def _make_ti(dag_id: str = "test_dag") -> TaskInstanceDTO:
    return TaskInstanceDTO(
        id=uuid.uuid4(),
        dag_version_id=uuid.uuid4(),
        task_id="task_1",
        dag_id=dag_id,
        run_id="run_1",
        try_number=1,
        map_index=-1,
        pool_slots=1,
        queue="default",
        priority_weight=1,
    )


def _make_startup_details(
    *,
    dag_id: str = "test_dag",
    dag_rel_path: str = "dags/example.jar",
    bundle_info: BundleInfo | None = None,
) -> StartupDetails:
    return StartupDetails.model_construct(
        ti=_make_ti(dag_id=dag_id),
        dag_rel_path=dag_rel_path,
        bundle_info=bundle_info or BundleInfo(name="my_bundle"),
        ti_context=None,
        start_date=datetime(2024, 12, 1, tzinfo=timezone.utc),
        sentry_integration="",
    )


class TestJavaCoordinatorAttributes:
    def test_default_kwargs(self):
        coordinator = JavaCoordinator()
        assert coordinator.java_executable == "java"
        assert coordinator.jvm_args == []
        assert coordinator.bundles_folder is None

    def test_custom_kwargs(self):
        coordinator = JavaCoordinator(
            java_executable="/opt/java/bin/java",
            jvm_args=["-Xmx512m", "-Xms256m"],
            bundles_folder="/airflow/java-bundles",
        )
        assert coordinator.java_executable == "/opt/java/bin/java"
        assert coordinator.jvm_args == ["-Xmx512m", "-Xms256m"]
        assert coordinator.bundles_folder == "/airflow/java-bundles"


class TestGetCodeFromFile:
    def test_returns_embedded_code(self, tmp_path: Path):
        code = "from airflow import DAG\ndag = DAG('my_dag')"
        jar = _create_bundle_jar(tmp_path / "with_code.jar", dag_ids=["d"], dag_code=code)
        assert JavaCoordinator().get_code_from_file(str(jar)) == code

    def test_raises_when_no_code(self, tmp_path: Path):
        jar = _create_bundle_jar(tmp_path / "no_code.jar", dag_ids=["d"])
        with pytest.raises(FileNotFoundError, match="No DAG source code found in JAR"):
            JavaCoordinator().get_code_from_file(str(jar))


class TestTaskExecutionCmd:
    def test_pure_java_dag(self, tmp_path: Path):
        jar = _create_bundle_jar(tmp_path / "app.jar", dag_ids=["test_dag"])
        bundle_path = str(tmp_path)
        ti = _make_ti()
        bundle_info = BundleInfo(name="my_bundle")

        cmd = JavaCoordinator().task_execution_cmd(
            what=ti,  # type: ignore[arg-type]
            dag_file_path=str(jar),
            bundle_path=bundle_path,
            bundle_info=bundle_info,
            comm_addr="localhost:1234",
            logs_addr="localhost:5678",
        )
        assert cmd == [
            "java",
            "-classpath",
            f"{bundle_path}/*",
            TEST_MAIN_CLASS,
            "--comm=localhost:1234",
            "--logs=localhost:5678",
        ]

    def test_python_stub_dag_uses_bundles_folder_kwarg(self, tmp_path: Path):
        bundles_folder = tmp_path / "java_bundles"
        bundle_sub = bundles_folder / "my_bundle"
        bundle_sub.mkdir(parents=True)
        _create_bundle_jar(bundle_sub / "app.jar", dag_ids=["stub_dag"])

        ti = _make_ti(dag_id="stub_dag")
        bundle_info = BundleInfo(name="my_bundle")

        coordinator = JavaCoordinator(bundles_folder=str(bundles_folder))
        cmd = coordinator.task_execution_cmd(
            what=ti,  # type: ignore[arg-type]
            dag_file_path="/dags/stub_dag.py",
            bundle_path="/some/bundle/path",
            bundle_info=bundle_info,
            comm_addr="localhost:1234",
            logs_addr="localhost:5678",
        )

        assert cmd == [
            "java",
            "-classpath",
            f"{bundles_folder}/my_bundle/app.jar",
            TEST_MAIN_CLASS,
            "--comm=localhost:1234",
            "--logs=localhost:5678",
        ]

    def test_python_stub_dag_without_bundles_folder_raises(self):
        ti = _make_ti()
        bundle_info = BundleInfo(name="my_bundle")

        with pytest.raises(ValueError, match="bundles_folder kwarg must be set"):
            JavaCoordinator().task_execution_cmd(
                what=ti,  # type: ignore[arg-type]
                dag_file_path="/dags/stub_dag.py",
                bundle_path="/some/bundle/path",
                bundle_info=bundle_info,
                comm_addr="localhost:1234",
                logs_addr="localhost:5678",
            )


class TestTargetMsgSchemaVersion:
    def test_pure_java_task_resolves_relative_path_from_dag_bundle(self, mocker, tmp_path: Path):
        jar = tmp_path / "dags" / "example.jar"
        jar.parent.mkdir(exist_ok=True)
        _create_bundle_jar(jar, dag_ids=["test_dag"], schema_version="2026-04-17")
        bundle_instance = mocker.Mock(path=tmp_path)
        mocker.patch("airflow.sdk.execution_time.task_runner.resolve_bundle", return_value=bundle_instance)

        startup_details = _make_startup_details(dag_rel_path="dags/example.jar")

        assert JavaCoordinator().target_msg_schema_version(startup_details) == "2026-04-17"

    def test_python_stub_task_uses_matched_bundle_schema_not_first_classpath_entry(self, tmp_path: Path):
        bundles_folder = tmp_path / "java_bundles"
        bundle_sub = bundles_folder / "my_bundle"
        bundle_sub.mkdir(parents=True)
        with zipfile.ZipFile(bundle_sub / "bundle-without-schema-version", "w") as zf:
            zf.writestr("placeholder.class", b"")
        _create_bundle_jar(
            bundle_sub / "valid-bundle.jar",
            dag_ids=["stub_dag"],
            schema_version="2026-04-17",
        )

        startup_details = _make_startup_details(dag_id="stub_dag", dag_rel_path="dags/stub_dag.py")

        assert (
            JavaCoordinator(bundles_folder=str(bundles_folder)).target_msg_schema_version(startup_details)
            == "2026-04-17"
        )

    def test_python_stub_task_picks_bundle_matching_startup_details_dag_id(self, tmp_path: Path):
        # Two valid bundles live side by side under ``bundles_folder``;
        # the coordinator must pick the one whose airflow-metadata.yaml
        # lists the dag_id from ``StartupDetails.ti.dag_id`` and return
        # *its* schema version, not the other bundle's. Guards against
        # cross-contamination when one foreign-runtime install serves
        # multiple unrelated DAG bundles at different schema versions.
        bundles_folder = tmp_path / "java_bundles"
        bundle_a = bundles_folder / "bundle_a"
        bundle_b = bundles_folder / "bundle_b"
        bundle_a.mkdir(parents=True)
        bundle_b.mkdir(parents=True)
        _create_bundle_jar(bundle_a / "app.jar", dag_ids=["dag_a"], schema_version="2026-04-17")
        _create_bundle_jar(bundle_b / "app.jar", dag_ids=["dag_b"], schema_version="2026-06-16")

        startup_b = _make_startup_details(dag_id="dag_b", dag_rel_path="dags/stub.py")
        startup_a = _make_startup_details(dag_id="dag_a", dag_rel_path="dags/stub.py")

        coordinator = JavaCoordinator(bundles_folder=str(bundles_folder))
        assert coordinator.target_msg_schema_version(startup_b) == "2026-06-16"
        assert coordinator.target_msg_schema_version(startup_a) == "2026-04-17"
