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
"""Regenerate the Go SDK compatibility table in ``go-sdk/README.md``.

Renders the Markdown matrix from ``go-sdk/capabilities.json`` between the AUTO-GENERATED markers.
Exits non-zero when the file changed so the contributor re-stages it.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common_prek_utils import console, insert_documentation
from lang_sdk_compat_matrix import (
    LANG_SDKS,
    README_MATRIX_FOOTER,
    README_MATRIX_HEADER,
    load_capabilities,
    render_markdown_table,
)

SDK_ID = "go"


def main() -> int:
    sdk = next(entry for entry in LANG_SDKS if entry["id"] == SDK_ID)
    changed = insert_documentation(
        sdk["readme"],
        render_markdown_table(load_capabilities(sdk["capabilities_json"], expected_sdk=SDK_ID)),
        README_MATRIX_HEADER,
        README_MATRIX_FOOTER,
        extra_information="the Go SDK compatibility matrix",
    )
    if changed:
        console.print(
            "[yellow]Regenerated the Go SDK compatibility matrix in go-sdk/README.md; re-stage it.[/]"
        )
        return 1
    return 0


if __name__ in ("__main__", "__mp_main__"):
    raise SystemExit(main())
