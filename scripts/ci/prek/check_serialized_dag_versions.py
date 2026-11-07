#!/usr/bin/env python
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
# /// script
# requires-python = ">=3.11,<3.12"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
"""
Keep the serialized Dag format versions honest.

Fails when:

1. ``schema.json`` differs from the target branch but no version file
   (``airflow-core/src/airflow/serialization/versions/v*.py``) was added or changed. Every change
   to the format needs a change class in the in-progress (newest) version file.
2. A version file that exists on the target branch changed and it is not the newest one. Migrations
   of later releases depend on released version files never changing.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

from common_prek_utils import (
    AIRFLOW_ROOT_PATH,
    console,
    get_changed_files,
    get_remote_for_main,
    get_target_branch,
)

SCHEMA_PATH = "airflow-core/src/airflow/serialization/schema.json"
VERSIONS_PREFIX = "airflow-core/src/airflow/serialization/versions/"
_VERSION_FILE_RE = re.compile(r"^v(?:(\d+)|(\d{4})_(\d{2})_(\d{2}))\.py$")


def is_version_file(path: str) -> bool:
    return path.startswith(VERSIONS_PREFIX) and bool(_VERSION_FILE_RE.match(Path(path).name))


def get_version_sort_key(path: str) -> tuple[int, int]:
    """Integer versions (``v2.py``) sort before date versions (``v2025_10_27.py``)."""
    match = _VERSION_FILE_RE.match(Path(path).name)
    if match is None:
        raise ValueError(f"{path} is not a version file")
    if match.group(1) is not None:
        return (0, int(match.group(1)))
    return (1, int("".join(match.group(2, 3, 4))))


def find_newest_version_file(version_files: list[str]) -> str | None:
    return max(version_files, key=get_version_sort_key, default=None)


def find_problems(
    *,
    schema_changed: bool,
    changed_version_files: list[str],
    released_version_files: set[str],
    all_version_files: list[str],
) -> list[str]:
    """Return a message per broken rule; empty when the change is fine."""
    problems = []
    newest = find_newest_version_file(all_version_files)
    if schema_changed and not changed_version_files:
        problems.append(
            f"{SCHEMA_PATH} changed, but no version file was added or changed. Add a change class "
            f"to the in-progress version file ({newest}). See {VERSIONS_PREFIX}AGENTS.md."
        )
    for path in sorted(changed_version_files):
        if path in released_version_files and path != newest:
            problems.append(
                f"{path} is a released version and must not change: migrations depend on it. "
                f"Add the change to the in-progress version file ({newest}) instead."
            )
    return problems


def _git(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], capture_output=True, text=True, check=False, cwd=AIRFLOW_ROOT_PATH)


def main() -> int:
    changed_files = get_changed_files(sys.argv[1:])
    if not any(path == SCHEMA_PATH or is_version_file(path) for path in changed_files):
        return 0

    remote = get_remote_for_main()
    target_branch = get_target_branch()
    ref = f"{remote}/{target_branch}"
    _git("fetch", remote, target_branch)
    if _git("rev-parse", "--verify", "--quiet", ref).returncode != 0:
        console.print(f"[yellow]Skipping serialized Dag versions check:[/] cannot find {ref}.")
        return 0

    all_version_files = [
        path.relative_to(AIRFLOW_ROOT_PATH).as_posix()
        for path in (AIRFLOW_ROOT_PATH / VERSIONS_PREFIX).glob("v*.py")
        if is_version_file(path.relative_to(AIRFLOW_ROOT_PATH).as_posix())
    ]
    changed_against_target = set(
        _git("diff", "--name-only", ref, "--", VERSIONS_PREFIX, SCHEMA_PATH).stdout.split()
    )
    changed_version_files = [path for path in changed_against_target if is_version_file(path)]
    # Covers deleted files too, which are no longer in the worktree.
    released_version_files = {
        path for path in changed_version_files if _git("cat-file", "-e", f"{ref}:{path}").returncode == 0
    }
    problems = find_problems(
        schema_changed=SCHEMA_PATH in changed_against_target,
        changed_version_files=changed_version_files,
        released_version_files=released_version_files,
        all_version_files=all_version_files,
    )
    for problem in problems:
        console.print(f"[bold red]ERROR:[/] {problem}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
