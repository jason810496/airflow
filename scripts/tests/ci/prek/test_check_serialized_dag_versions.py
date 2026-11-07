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

import pytest
from check_serialized_dag_versions import (
    SCHEMA_PATH,
    VERSIONS_PREFIX,
    find_newest_version_file,
    find_problems,
    is_version_file,
)

V2 = f"{VERSIONS_PREFIX}v2.py"
V3 = f"{VERSIONS_PREFIX}v3.py"
RELEASED = f"{VERSIONS_PREFIX}v2026_10_30.py"
IN_PROGRESS = f"{VERSIONS_PREFIX}v2027_02_28.py"
ALL_VERSION_FILES = [V3, IN_PROGRESS, V2, RELEASED]


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        (V2, True),
        (RELEASED, True),
        (f"{VERSIONS_PREFIX}base.py", False),
        (f"{VERSIONS_PREFIX}__init__.py", False),
        (f"{VERSIONS_PREFIX}v2026_1_30.py", False),
        ("airflow-core/src/airflow/serialization/v3.py", False),
    ],
)
def test_is_version_file(path, expected):
    assert is_version_file(path) is expected


def test_newest_version_file_sorts_dates_after_integers():
    assert find_newest_version_file(ALL_VERSION_FILES) == IN_PROGRESS
    assert find_newest_version_file([V2, V3]) == V3
    assert find_newest_version_file([]) is None


@pytest.mark.parametrize(
    ("schema_changed", "changed_version_files", "released_version_files"),
    [
        pytest.param(True, [IN_PROGRESS], {IN_PROGRESS}, id="schema-with-in-progress-change"),
        pytest.param(True, [IN_PROGRESS], set(), id="schema-with-new-version-file"),
        pytest.param(False, [IN_PROGRESS], {IN_PROGRESS}, id="in-progress-only"),
        pytest.param(False, [RELEASED], set(), id="file-not-on-target-branch"),
        pytest.param(False, [], set(), id="nothing"),
    ],
)
def test_accepts(schema_changed, changed_version_files, released_version_files):
    assert (
        find_problems(
            schema_changed=schema_changed,
            changed_version_files=changed_version_files,
            released_version_files=released_version_files,
            all_version_files=ALL_VERSION_FILES,
        )
        == []
    )


def test_rejects_schema_change_without_version_change():
    problems = find_problems(
        schema_changed=True,
        changed_version_files=[],
        released_version_files=set(),
        all_version_files=ALL_VERSION_FILES,
    )

    assert len(problems) == 1
    assert SCHEMA_PATH in problems[0]
    assert IN_PROGRESS in problems[0]


@pytest.mark.parametrize("released", [RELEASED, V3], ids=["date", "integer"])
def test_rejects_change_to_released_version_file(released):
    problems = find_problems(
        schema_changed=True,
        changed_version_files=[released, IN_PROGRESS],
        released_version_files={released, IN_PROGRESS},
        all_version_files=ALL_VERSION_FILES,
    )

    assert problems == [
        f"{released} is a released version and must not change: migrations depend on it. "
        f"Add the change to the in-progress version file ({IN_PROGRESS}) instead."
    ]
