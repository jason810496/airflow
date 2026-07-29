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
"""
Keep ``ts-sdk/generated/lang-sdk/capabilities.json`` in sync with the TypeScript capability constant.

The TS SDK declares what it supports in ``src/conformance/capabilities.ts`` (the single source of
truth). ``ts-sdk/generated/lang-sdk/capabilities.json`` is a generated artifact consumed by the compatibility-matrix
prek hooks. This check runs ``pnpm run generate:capabilities``, compares the emitted manifest with
the committed file *by content* (formatting is irrelevant), and rewrites it if it is stale.

Exits 0 if ``capabilities.json`` matches the constant, 1 otherwise.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "scripts" / "ci" / "prek"))

from lang_sdk_compat_matrix import AIRFLOW_ROOT_PATH, validate_capabilities

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(f"This file is intended to be executed directly, e.g. ./{__file__}")

TS_SDK = AIRFLOW_ROOT_PATH / "ts-sdk"
CAPABILITIES_JSON = TS_SDK / "generated" / "lang-sdk" / "capabilities.json"


def canonical(data: dict) -> str:
    return json.dumps(data, indent=2) + "\n"


def generate() -> dict:
    """Install deps and run the pnpm generator, returning the parsed capability manifest."""
    subprocess.run(["pnpm", "config", "set", "store-dir", ".pnpm-store"], cwd=TS_SDK, check=True)
    subprocess.run(
        ["pnpm", "install", "--frozen-lockfile", "--config.confirmModulesPurge=false"],
        cwd=TS_SDK,
        check=True,
    )
    completed = subprocess.run(
        ["pnpm", "--silent", "run", "generate:capabilities"],
        cwd=TS_SDK,
        capture_output=True,
        text=True,
        check=True,
    )
    out = completed.stdout
    start, end = out.find("{"), out.rfind("}")
    if start == -1 or end == -1:
        raise ValueError(f"no JSON object found in generate:capabilities output:\n{out}")
    return json.loads(out[start : end + 1])


if __name__ in ("__main__", "__mp_main__"):
    try:
        generated = generate()
    except (subprocess.CalledProcessError, ValueError) as error:
        detail = getattr(error, "stderr", None) or str(error)
        print(f"ERROR: could not run `pnpm run generate:capabilities`:\n{detail}")
        sys.exit(1)
    validate_capabilities(generated, source="generate:capabilities", expected_sdk="ts")
    current = CAPABILITIES_JSON.read_text() if CAPABILITIES_JSON.exists() else ""
    if current and json.loads(current) == generated:
        print("OK: ts-sdk/generated/lang-sdk/capabilities.json is in sync with the capability constant.")
        sys.exit(0)
    CAPABILITIES_JSON.parent.mkdir(parents=True, exist_ok=True)
    CAPABILITIES_JSON.write_text(canonical(generated))
    print(
        "ERROR: ts-sdk/generated/lang-sdk/capabilities.json was stale and has been regenerated from "
        "src/conformance/capabilities.ts. Re-stage the file."
    )
    sys.exit(1)
