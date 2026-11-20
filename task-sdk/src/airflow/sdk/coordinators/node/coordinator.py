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
"""Node.js runtime coordinator that launches a Node.js subprocess for task execution and Dag parsing."""

from __future__ import annotations

import json
import os
import pathlib
from typing import TYPE_CHECKING

import attrs
import structlog

from airflow.sdk.coordinators._bundle_metadata import ResolvedBundle, walk_files
from airflow.sdk.coordinators._subprocess import SubprocessCoordinator
from airflow.sdk.coordinators.node._bundle_reader import BUNDLE_SUFFIX, read_bundle
from airflow.sdk.execution_time.coordinator import TaskLaunchError

if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing import Self

    from structlog.typing import FilteringBoundLogger

    from airflow.sdk.api.datamodels._generated import TaskInstance

log: FilteringBoundLogger = structlog.get_logger(logger_name="coordinators.node")


_PROVIDER_RUNNER_PATH = pathlib.PurePosixPath("dist/coordinator/provider-runner.js")
_SDK_PACKAGE = pathlib.PurePosixPath("@apache-airflow/ts-sdk")


def _is_bundle(path: pathlib.Path) -> bool:
    return path.name.endswith(BUNDLE_SUFFIX)


@attrs.define
class _Bundle(ResolvedBundle):
    @classmethod
    def find(cls, roots: Sequence[pathlib.Path], dag_id: str) -> Self:
        """Return the first verified configured bundle that declares *dag_id*."""
        log.debug("Finding TypeScript bundles recursively", roots=roots, dag_id=dag_id)
        rejected: list[tuple[pathlib.Path, str]] = []
        for candidate in walk_files(roots, match=_is_bundle):
            try:
                metadata = read_bundle(candidate)
                if dag_id not in metadata.dag_ids:
                    log.debug(
                        "TypeScript bundle does not contain requested Dag; skipping",
                        path=candidate,
                        dag_id=dag_id,
                    )
                    rejected.append(
                        (candidate, f"verified bundle declares dag_ids={sorted(metadata.dag_ids)!r}")
                    )
                    continue
                bundle = cls(path=candidate, schema_version=metadata.supervisor_schema_version)
            except (OSError, TypeError, ValueError) as exc:
                log.debug(
                    "TypeScript bundle rejected; skipping",
                    path=candidate,
                    reason=str(exc),
                    exc_info=True,
                )
                rejected.append((candidate, str(exc)))
                continue
            log.debug("Selected TypeScript bundle", path=candidate, dag_id=dag_id)
            return bundle

        searched = os.pathsep.join(os.fspath(root) for root in roots)
        if rejected:
            details = "; ".join(f"{path}: {reason}" for path, reason in rejected)
            raise FileNotFoundError(
                f"Cannot find usable TypeScript bundle containing dag_id={dag_id!r} in {searched}: "
                f"rejected candidates ({details})"
            )
        raise FileNotFoundError(f"Cannot find TypeScript bundle containing dag_id={dag_id!r} in {searched}")


@attrs.define(kw_only=True)
class NodeCoordinator(SubprocessCoordinator):
    """
    Coordinator that launches a Node.js subprocess for task execution and Dag parsing.

    Configuration is taken from the ``[sdk] coordinators`` entry that constructs
    this instance::

        {
            "ts": {
                "classpath": "airflow.sdk.coordinators.node.NodeCoordinator",
                "kwargs": {
                    "node_executable": "node",
                    "task_handler_bundle_name": "ts-task-handlers",
                },
            }
        }

    :param node_executable: Path to the ``node`` binary (defaults to
        ``"node"``, which relies on ``$PATH``).
    :param task_handler_bundle_name: Name of the Dag bundle searched recursively for the first
        verified ``*.min.mjs`` bundle declaring the task instance's Dag. It must be registered in
        ``[dag_processor] dag_bundle_config_list``. If unset, the task's own Dag bundle is used.
    :param node_modules_path: A ``node_modules`` directory that holds ``@apache-airflow/ts-sdk`` and the
        provider packages that implement operators in TypeScript. A provider operator task of a Dag
        that is not a native TypeScript Dag runs in the SDK's provider runner from it, which imports
        the operator's package. If unset, such a task fails.
    :param task_startup_timeout: Maximum time the coordinator waits for a task
        process to start, in seconds. The default is 10 seconds.

    A task of a native TypeScript Dag runs the ``*.min.mjs`` bundle its Dag was parsed from.

    A task of a Python Dag may be a provider operator implemented in TypeScript, such as one of
    ``apache-airflow-providers-ts-localfile``. Its ``stub_kind`` is ``provider_operator``, so
    ``node_modules_path`` selects the provider runner, whether or not a bundle declares the task's
    ``dag_id``. Any other task of a Python Dag, such as a TypeScript task handler, runs the bundle that
    declares its ``dag_id``.

    The coordinator also parses native TypeScript Dags: every packed ``*.min.mjs`` bundle in a
    Dag bundle is run to list its Dags. With one NodeCoordinator configured, it parses the bundles
    of every Dag bundle. With several, ``[sdk] dag_bundle_to_coordinator`` picks the one that
    parses a Dag bundle.
    """

    node_executable: str = "node"
    node_modules_path: str | None = None

    def _build_execute_task_command(self, *, what: TaskInstance) -> tuple[list[str], str | None]:
        bundle = _Bundle.find(self._get_scan_roots(), what.dag_id)
        return [self.node_executable, os.fspath(bundle.path)], bundle.schema_version

    def _build_provider_operator_command(self, *, what: TaskInstance) -> tuple[list[str], str | None]:
        if self.node_modules_path is None:
            raise TaskLaunchError(
                f"Task {what.task_id!r} in Dag {what.dag_id!r} is a provider operator, which the Node.js "
                "coordinator runs from node_modules_path, and it is not set. Set it to the node_modules "
                "directory that holds @apache-airflow/ts-sdk and the provider package.",
                retryable=False,
            )
        return self._build_provider_runner_command()

    def _build_provider_runner_command(self) -> tuple[list[str], str]:
        """Return the command that runs the SDK's provider runner, and its supervisor schema version."""
        package = pathlib.Path(self.node_modules_path or "") / _SDK_PACKAGE
        runner = package / _PROVIDER_RUNNER_PATH
        try:
            manifest = json.loads((package / "package.json").read_text())
            schema_version = manifest["airflow"]["supervisorSchemaVersion"]
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise FileNotFoundError(
                f"Cannot read the supervisor schema version of {package}; install @apache-airflow/ts-sdk "
                f"into node_modules_path {self.node_modules_path!r}: {exc}"
            ) from exc
        return [self.node_executable, os.fspath(runner)], schema_version

    def _build_bundle_command(self, path: pathlib.Path) -> tuple[list[str], str]:
        """Return the command that runs the packed bundle at *path*, and its supervisor schema version."""
        return [self.node_executable, os.fspath(path)], read_bundle(path).supervisor_schema_version

    def _build_dag_file_command(
        self, *, what: TaskInstance, path: pathlib.Path
    ) -> tuple[list[str], str | None]:
        return self._build_bundle_command(path)

    def _build_parse_dag_command(self, *, path: pathlib.Path) -> tuple[list[str], str | None]:
        return self._build_bundle_command(path)
