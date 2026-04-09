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
"""Java DAG importer — registers ``.jar`` with the DAG importer registry."""

from __future__ import annotations

import email
import os
import zipfile
from collections.abc import Iterator
from pathlib import Path

from airflow.dag_processing.importers.base import (
    AbstractDagImporter,
    DagImportResult,
)


class JavaDagImporter(AbstractDagImporter):
    """
    Importer that registers ``.jar`` files with the DAG importer registry.

    This importer enables the :class:`~airflow.dag_processing.manager.DagFileProcessorManager`
    to discover JAR files during bundle scanning.  Actual DAG parsing is handled
    out-of-process by the :class:`~airflow.providers.languages.java.dag_file_processors.JavaDagFileProcessor`,
    which bridges to a Java subprocess; :meth:`import_file` therefore returns an
    empty result and is not expected to be called during normal operation.

    This is used for AIP-85's Java Dag Support.
    """

    required_jar_manifest_attributes = ["Main-Class", "Airflow-Java-SDK-Version", "Airflow-Java-SDK-Metadata"]

    @classmethod
    def supported_extensions(cls) -> list[str]:
        return [".jar"]

    def list_dag_files(
        self,
        directory: str | os.PathLike[str],
        safe_mode: bool = True,
    ) -> Iterator[str]:
        """Yield ``.jar`` files found under *directory*."""
        from airflow._shared.module_loading.file_discovery import find_path_from_directory
        from airflow.providers.common.compat.sdk import conf
        # TODO: we should update the prek hook the support "# ignore: airflow-conf-import" to import the airflow.configuration
        # Even we define JavaDagImporter in provider, but it will really only be used in Airflow Core

        path = Path(directory)

        # Single file — just check the extension.
        if path.is_file() and self.can_handle(path):
            return [path.as_posix()]

        if not path.is_dir():
            return []

        ignore_file_syntax = conf.get_mandatory_value("core", "DAG_IGNORE_FILE_SYNTAX", fallback="glob")
        for file_path in find_path_from_directory(directory, ".airflowignore", ignore_file_syntax):
            p = Path(file_path)
            if self.can_handle(p):
                yield p.as_posix()

    def can_handle(self, file_path: Path) -> bool:
        """Check that *file_path* is a valid DAG JAR by verifying required manifest attributes."""
        if file_path.suffix.lower() != ".jar":
            return False

        with zipfile.ZipFile(file_path) as zf:
            with zf.open("META-INF/MANIFEST.MF") as f:
                jar_metadata = email.message_from_binary_file(f)
                return all(attr in jar_metadata for attr in self.required_jar_manifest_attributes)

    def import_file(
        self,
        file_path: str | Path,
        *,
        bundle_path: Path | None = None,
        bundle_name: str | None = None,
        safe_mode: bool = True,
    ) -> DagImportResult:
        """Return an empty result — actual parsing is handled by the Java subprocess bridge."""
        return DagImportResult(file_path="should-be-handled-by-java-subprocess-bridge", dags=[])
