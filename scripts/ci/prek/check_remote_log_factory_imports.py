#!/usr/bin/env python
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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
"""
Check that remote_log_factory modules only import from allowed modules.

remote_log_factory.py files (matching pattern */log/*remote_log_factory.py) must stay
lightweight. Heavy provider SDK imports (hooks, task handlers, etc.) must be deferred
to inside the factory function body so that provider discovery remains fast.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))
from common_prek_utils import console, get_imports_from_file

ALLOWED_MODULES = {
    "airflow.configuration",
    "airflow.providers.common.compat.sdk",
}

STDLIB_PREFIXES = (
    "typing",
    "collections",
    "functools",
    "pathlib",
    "os",
    "sys",
    "re",
    "json",
    "urllib",
)


def is_allowed_import(import_name: str) -> bool:
    """Check if an import is allowed at the top level of a remote_log_factory module."""
    for allowed_module in ALLOWED_MODULES:
        if import_name == allowed_module or import_name.startswith(f"{allowed_module}."):
            return True

    for prefix in STDLIB_PREFIXES:
        if import_name == prefix or import_name.startswith(f"{prefix}."):
            return True

    return False


def parse_args():
    parser = argparse.ArgumentParser(
        description="Check that remote_log_factory modules only import from allowed modules."
    )
    parser.add_argument("files", nargs="*", type=Path, help="Python source files to check.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.files:
        console.print("[yellow]No files provided.[/]")
        return 0

    factory_files = [
        path
        for path in args.files
        if path.name.endswith("remote_log_factory.py") and len(path.parts) >= 2 and path.parts[-2] == "log"
    ]

    if not factory_files:
        console.print("[yellow]No remote_log_factory files found to check.[/]")
        return 0

    console.print(f"[blue]Checking {len(factory_files)} remote_log_factory file(s)...[/]")

    errors: list[str] = []

    for path in factory_files:
        try:
            imports = get_imports_from_file(path, only_top_level=True)
        except Exception as e:
            console.print(f"[red]Failed to parse {path}: {e}[/]")
            return 2

        forbidden_imports = [imp for imp in imports if not is_allowed_import(imp)]

        if forbidden_imports:
            errors.append(f"\n[red]{path}:[/]")
            for imp in forbidden_imports:
                errors.append(f"  - {imp}")

    if errors:
        console.print("\n[red] Some remote_log_factory files contain forbidden top-level imports![/]\n")
        console.print(
            "[yellow]remote_log_factory.py files should only import at the top level from:[/]\n"
            "  - airflow.configuration\n"
            "  - airflow.providers.common.compat.sdk\n"
            f"  - Standard library modules ({', '.join(STDLIB_PREFIXES)})\n"
        )
        console.print(
            "[yellow]Heavy imports (hooks, task handlers, provider SDKs) must be deferred\n"
            "inside the factory function body so that provider discovery stays fast.[/]\n"
        )
        console.print("[red]Found forbidden top-level imports in:[/]")
        for error in errors:
            console.print(error)
        return 1

    console.print("[green] All remote_log_factory files import only from allowed modules![/]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
