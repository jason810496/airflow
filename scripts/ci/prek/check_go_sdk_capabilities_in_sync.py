#!/usr/bin/env python3
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
"""Keep ``go-sdk/generated/lang-sdk/capabilities.json`` in sync with the Go capability constant.

The Go SDK declares what it supports in ``go-sdk/pkg/conformance`` (``conformance.Capabilities``),
which is the single source of truth. ``go-sdk/generated/lang-sdk/capabilities.json`` is a generated artifact consumed
by the compatibility-matrix prek hooks. This check regenerates the JSON from the constant with
``go run ./cmd/dump-capabilities`` and fails if the committed file is stale.

Run from the repo root::

    uv run --project scripts python scripts/ci/prek/check_go_sdk_capabilities_in_sync.py

Exits 0 if ``capabilities.json`` matches the constant, 1 otherwise (and rewrites the file).
"""

from __future__ import annotations

import os
import pathlib
import shutil
import subprocess
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
GO_SDK = REPO_ROOT / "go-sdk"
CAPABILITIES_JSON = GO_SDK / "generated" / "lang-sdk" / "capabilities.json"


def generate() -> str:
    """Return the capability manifest JSON produced by the Go dumper."""
    completed = subprocess.run(
        ["go", "run", "./cmd/dump-capabilities"],
        cwd=GO_SDK,
        capture_output=True,
        text=True,
        check=True,
    )
    return completed.stdout


def main() -> int:
    if shutil.which("go") is None:
        if os.environ.get("CI"):
            print("ERROR: `go` is not on PATH but this is a CI run — the toolchain is required here.")
            return 1
        print("SKIPPED: `go` is not on PATH, cannot verify go-sdk/generated/lang-sdk/capabilities.json.")
        return 0
    try:
        generated = generate()
    except subprocess.CalledProcessError as error:
        print("ERROR: could not run `go run ./cmd/dump-capabilities`:")
        print(error.stderr or error.stdout or "(no output)")
        return 1
    current = CAPABILITIES_JSON.read_text() if CAPABILITIES_JSON.exists() else ""
    if generated == current:
        print("OK: go-sdk/generated/lang-sdk/capabilities.json is in sync with conformance.Capabilities.")
        return 0
    CAPABILITIES_JSON.parent.mkdir(parents=True, exist_ok=True)
    CAPABILITIES_JSON.write_text(generated)
    print(
        "ERROR: go-sdk/generated/lang-sdk/capabilities.json was stale and has been regenerated from "
        "conformance.Capabilities. Re-stage the file."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
