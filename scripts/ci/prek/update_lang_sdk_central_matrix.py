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
# requires-python = ">=3.10,<3.11"
# dependencies = ["rich>=13.6.0"]
# ///
"""Regenerate the consolidated Language SDK compatibility matrix in the user-facing docs.

Reads every ``<sdk>/capabilities.json`` listed in the ``LANG_SDKS`` registry (an SDK that has
not published one yet simply shows the "absent" mark) and rewrites the ``list-table`` in
``airflow-core/docs/authoring-and-scheduling/language-sdks/index.rst`` between the
AUTO-GENERATED markers. Exits non-zero when the file changed so the contributor re-stages it.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common_prek_utils import console, insert_documentation
from lang_sdk_compat_matrix import (
    AIRFLOW_ROOT_PATH,
    CENTRAL_MATRIX_FOOTER,
    CENTRAL_MATRIX_HEADER,
    LANG_SDKS,
    load_capabilities,
    render_central_table,
)

INDEX_RST = (
    AIRFLOW_ROOT_PATH / "airflow-core" / "docs" / "authoring-and-scheduling" / "language-sdks" / "index.rst"
)


def main() -> int:
    docs = {
        sdk["id"]: (
            load_capabilities(sdk["capabilities_json"], expected_sdk=sdk["id"])
            if sdk["capabilities_json"].exists()
            else None
        )
        for sdk in LANG_SDKS
    }
    changed = insert_documentation(
        INDEX_RST,
        render_central_table(docs),
        CENTRAL_MATRIX_HEADER,
        CENTRAL_MATRIX_FOOTER,
        extra_information="the Language SDK compatibility matrix",
    )
    if changed:
        console.print(
            "[yellow]Regenerated the Language SDK compatibility matrix in index.rst; re-stage the file.[/]"
        )
        return 1
    return 0


if __name__ in ("__main__", "__mp_main__"):
    raise SystemExit(main())
