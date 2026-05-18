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
"""Java runtime coordinator that launches a JVM subprocess for Dag file processing and task execution."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from airflow.sdk.coordinators.java.bundle_scanner import BundleScanner, read_dag_code
from airflow.sdk.execution_time.coordinator import BaseCoordinator

if TYPE_CHECKING:
    from airflow.sdk.api.datamodels._generated import BundleInfo
    from airflow.sdk.execution_time.workloads.task import TaskInstanceDTO


class JavaCoordinator(BaseCoordinator):
    """
    Coordinator that launches a JVM subprocess for DAG parsing and task execution.

    Configuration is taken from the ``[sdk] coordinators`` entry that constructs
    this instance::

        {
            "name": "jdk-17",
            "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
            "kwargs": {
                "java_executable": "/usr/lib/jvm/java-17-openjdk/bin/java",
                "jvm_args": ["-Xmx1024m"],
                "jar_root": ["~/airflow/java-bundles"],
            },
        }

    :param java_executable: Path to the ``java`` binary (defaults to ``"java"``,
        which relies on ``$PATH``).
    :param jvm_args: Extra arguments passed to the JVM (e.g. ``["-Xmx512m"]``).
    :param jar_root: Paths scanned for JAR bundles when a Python stub DAG
        delegates task execution to Java.  Each entry may be a bundle JAR
        file or a directory (flat or nested layout).  Entries are searched
        in order until a bundle for the target dag is found.  Required for
        the stub-DAG flow; unused for pure-Java DAGs.
    """

    def __init__(
        self,
        *,
        java_executable: str = "java",
        jvm_args: list[str] | None = None,
        jar_root: list[str] | None = None,
    ) -> None:
        self._java_executable = java_executable
        self._jvm_args = list(jvm_args) if jvm_args else []
        self._jar_root: list[Path] = [Path(p) for p in jar_root] if jar_root else []

    def get_code_from_file(self, fileloc: str) -> str:
        """Read embedded DAG source code from a JAR bundle."""
        code = read_dag_code(Path(fileloc))
        if code is None:
            raise FileNotFoundError(f"No DAG source code found in JAR: {fileloc}")
        return code

    def task_execution_cmd(
        self,
        *,
        what: TaskInstanceDTO,
        dag_file_path: str,
        bundle_path: str,
        bundle_info: BundleInfo,
        comm_addr: str,
        logs_addr: str,
    ) -> list[str]:
        """Build the ``java`` command for executing a task in a JAR bundle."""
        if dag_file_path.endswith(".jar"):
            # Case 1: Pure Java Dag -- the dag_file_path points directly to a
            # bundle JAR inside the Airflow Core Dag Bundle.
            jar_path = Path(dag_file_path)
            classpath = f"{bundle_path}/*"
            return [
                self._java_executable,
                *self._jvm_args,
                "-classpath",
                classpath,
                BundleScanner.resolve_jar(jar_path),
                f"--comm={comm_addr}",
                f"--logs={logs_addr}",
            ]

        # Case 2: Python Stub Dag -- the dag_file_path is a Python file but
        # the task delegates to a Java runtime.  The actual JAR bundle lives
        # under one of the ``jar_root`` directories (passed to __init__ from
        # the [sdk] coordinators config entry).
        if not self._jar_root:
            raise ValueError(
                "JavaCoordinator: jar_root kwarg must be set for Python stub DAGs "
                "that delegate to Java task execution."
            )

        resolved = BundleScanner(self._jar_root).resolve(dag_id=what.dag_id)
        return [
            self._java_executable,
            *self._jvm_args,
            "-classpath",
            resolved.classpath,
            resolved.main_class,
            f"--comm={comm_addr}",
            f"--logs={logs_addr}",
        ]
