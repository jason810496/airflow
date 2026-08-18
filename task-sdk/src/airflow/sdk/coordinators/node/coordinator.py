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
"""Node.js runtime coordinator that launches a Node.js subprocess for task execution."""

from __future__ import annotations

import base64
import os
import pathlib
from typing import TYPE_CHECKING, Any

import attrs
import structlog

from airflow.sdk.coordinators._bundle_metadata import (
    ResolvedBundle,
    convert_roots,
    extract_supervisor_schema_version,
    parse_metadata_mapping,
)
from airflow.sdk.coordinators._subprocess import SubprocessCoordinator

if TYPE_CHECKING:
    from collections.abc import Sequence

    from structlog.typing import FilteringBoundLogger

    from airflow.sdk.api.datamodels._generated import TaskInstance

log: FilteringBoundLogger = structlog.get_logger(logger_name="coordinators.node")

BUNDLE_FILENAME = "bundle.mjs"
SENTINEL_PREFIX = b"//# "
EMBEDDED_METADATA_MARKER = b"//# airflowMetadata="
EMBEDDED_METADATA_MAX_BYTES = 1024 * 1024
EMBEDDED_SOURCE_MARKER = b"//# airflowDagCode="
EMBEDDED_SOURCE_MAX_BYTES = 1024 * 1024


def _read_sentinel_payload(bundle_path: pathlib.Path, marker: bytes, max_bytes: int, *, what: str) -> bytes:
    """
    Decode the payload of the ``marker`` comment in the bundle's sentinel block.

    ``airflow-ts-pack`` heads the bundle with one ``//# <name>=<base64>`` line
    comment per embedded payload, so a bundle and everything Airflow needs to
    read off it stay a single artifact. The block ends at the first line that is
    not such a comment. *what* names the payload in error messages; a missing,
    oversized, or undecodable one raises ``ValueError``.
    """
    try:
        with bundle_path.open("rb") as bundle_file:
            while True:
                line = bundle_file.readline(max_bytes + 1)
                if not line.startswith(SENTINEL_PREFIX):
                    raise ValueError(
                        f"{bundle_path.name} has no embedded {what}; rebuild with airflow-ts-pack"
                    )
                if line.startswith(marker):
                    break
    except OSError as exc:
        raise ValueError(f"cannot read {bundle_path.name}: {exc}") from exc

    if len(line) > max_bytes:
        raise ValueError(
            f"embedded {what} exceeds {max_bytes} bytes; rebuild {bundle_path.name} with airflow-ts-pack"
        )

    try:
        return base64.b64decode(line[len(marker) :].strip(), validate=True)
    except ValueError as exc:
        raise ValueError(f"cannot parse embedded {what}: {exc}") from exc


def _read_embedded_metadata(bundle_path: pathlib.Path) -> dict[str, Any]:
    """Read the manifest ``airflow-ts-pack`` embeds in the bundle itself."""
    decoded = _read_sentinel_payload(
        bundle_path, EMBEDDED_METADATA_MARKER, EMBEDDED_METADATA_MAX_BYTES, what="airflow metadata"
    )
    return parse_metadata_mapping(decoded, source="embedded airflow metadata")


def _find_bundle(bundles_root: Sequence[pathlib.Path]) -> ResolvedBundle:
    """
    Locate the ``.mjs`` entry point in *bundles_root*.

    Scans each configured directory for ``bundle.mjs`` and reads the bundle's
    supervisor schema version from the metadata embedded in the bundle.

    This is an ordered fallback search, not Dag/task-aware multi-bundle
    routing. The first bundle found wins. A future version can use the
    metadata's ``dags`` section together with ``TaskInstance.dag_id`` and
    ``TaskInstance.task_id`` to select the bundle that owns a specific task.
    """
    rejected: list[tuple[pathlib.Path, str]] = []
    for root in bundles_root:
        candidate = root / BUNDLE_FILENAME
        if not candidate.is_file():
            continue
        try:
            metadata = _read_embedded_metadata(candidate)
            log.debug("Selected TypeScript bundle", path=candidate, root=root)
            return ResolvedBundle(
                path=candidate,
                schema_version=extract_supervisor_schema_version(metadata),
            )
        except (TypeError, ValueError) as exc:
            log.debug(
                "TypeScript bundle metadata rejected; skipping",
                path=candidate,
                root=root,
                exc_info=True,
            )
            rejected.append((candidate.resolve(), str(exc)))

    searched = os.pathsep.join(os.fspath(p.resolve()) for p in bundles_root)
    if rejected:
        details = "; ".join(f"{path}: {reason}" for path, reason in rejected)
        raise FileNotFoundError(
            f"Cannot find usable TypeScript bundle in {searched}: matching bundles were rejected ({details})"
        )
    raise FileNotFoundError(f"Cannot find {BUNDLE_FILENAME} in {searched}")


@attrs.define(kw_only=True)
class NodeCoordinator(SubprocessCoordinator):
    """
    Coordinator that launches a Node.js subprocess for task execution.

    Configuration is taken from the ``[sdk] coordinators`` entry that constructs
    this instance::

        {
            "ts": {
                "classpath": "airflow.sdk.coordinators.node.NodeCoordinator",
                "kwargs": {
                    "node_executable": "node",
                    "bundles_root": ["/opt/airflow/ts-bundles"],
                },
            }
        }

    :param node_executable: Path to the ``node`` binary (defaults to
        ``"node"``, which relies on ``$PATH``).
    :param bundles_root: Ordered list of directories scanned for a usable
        TypeScript bundle. Each bundle directory must contain ``bundle.mjs``
        with embedded metadata (as produced by ``airflow-ts-pack``). This is a
        fallback search path; it does not yet route different Dag/task pairs
        to different bundles.
    :param task_startup_timeout: Maximum time the coordinator waits for a task
        process to start, in seconds. The default is 10 seconds.
    """

    node_executable: str = "node"
    bundles_root: list[pathlib.Path] = attrs.field(
        converter=convert_roots,
        validator=attrs.validators.min_len(1),
    )

    @staticmethod
    def get_code_from_file(bundle_path: pathlib.Path) -> str:
        """
        Return the Dag entrypoint source ``airflow-ts-pack`` embedded in *bundle_path*.

        Only the entrypoint is packed, so this is the single-file source display
        of a natively authored TypeScript Dag, not the whole project tree that
        `ADR-0006 <https://github.com/apache/airflow/blob/main/airflow-core/adr/lang-sdk/0006-no-lang-sdk-source-display.md>`_
        declined for mixed-language Dags.

        Nothing calls this yet: ``DagCode`` reads Dag source straight off the
        filesystem and no coordinator hook exists on that path until AIP-85
        lands a Dag importer that can delegate to one.

        :param bundle_path: Path to the ``bundle.mjs`` to read.
        :raises ValueError: If the bundle carries no readable embedded source.
        """
        decoded = _read_sentinel_payload(
            bundle_path, EMBEDDED_SOURCE_MARKER, EMBEDDED_SOURCE_MAX_BYTES, what="Dag source"
        )
        try:
            return decoded.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError(f"embedded Dag source of {bundle_path.name} is not valid UTF-8: {exc}") from exc

    def _build_execute_task_command(self, *, what: TaskInstance) -> tuple[list[str], str | None]:
        # Multi-bundle routing should be added here by passing `what.dag_id` and
        # `what.task_id` into bundle selection and matching against metadata["dags"].
        bundle = _find_bundle(self.bundles_root)
        return [self.node_executable, os.fspath(bundle.path)], bundle.schema_version
