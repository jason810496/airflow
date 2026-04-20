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
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
"""
Update the timetable definition in schema.json with known built-in timetable types.

Extracts timetable import paths from the BUILTIN_TIMETABLES mapping in encoders.py
and updates the ``examples`` field in schema.json so that the schema stays in sync
with the actual set of supported timetable types while still allowing custom timetables.
"""
from __future__ import annotations

import ast
import json
import sys
from pathlib import Path

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To execute this script, run ./{__file__} [FILE] ..."
    )

sys.path.insert(0, str(Path(__file__).parent.resolve()))
from common_prek_utils import AIRFLOW_CORE_SOURCES_PATH

from rich.console import Console

console = Console(color_system="standard", width=200)

ENCODERS_PATH = AIRFLOW_CORE_SOURCES_PATH / "airflow" / "serialization" / "encoders.py"
TIMETABLES_DIR = AIRFLOW_CORE_SOURCES_PATH / "airflow" / "timetables"
SCHEMA_PATH = AIRFLOW_CORE_SOURCES_PATH / "airflow" / "serialization" / "schema.json"

TIMETABLE_MODULES_TO_SKIP = {"__init__", "base", "_cron", "_delta"}


def extract_builtin_timetable_paths() -> list[str]:
    """
    Extract timetable import path strings from BUILTIN_TIMETABLES in encoders.py.

    Parses the AST of encoders.py, finds the _Serializer class, and extracts
    the string literal values from its BUILTIN_TIMETABLES class variable.
    """
    tree = ast.parse(ENCODERS_PATH.read_text())
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef) or node.name != "_Serializer":
            continue
        for item in node.body:
            if not isinstance(item, ast.AnnAssign):
                continue
            target = item.target
            if isinstance(target, ast.Name) and target.id == "BUILTIN_TIMETABLES" and item.value:
                return _extract_string_values(item.value)
    return []


def _extract_string_values(node: ast.expr) -> list[str]:
    """Extract string literal values from a dict AST node."""
    if not isinstance(node, ast.Dict):
        return []
    result = []
    for val in node.values:
        if isinstance(val, ast.Constant) and isinstance(val.value, str):
            result.append(val.value)
    return result


def scan_core_timetable_classes() -> list[str]:
    """
    Scan airflow.timetables modules for public concrete class definitions.

    Returns fully-qualified import paths like ``airflow.timetables.simple.NullTimetable``.
    """
    paths: list[str] = []
    for py_file in sorted(TIMETABLES_DIR.glob("*.py")):
        module_name = py_file.stem
        if module_name in TIMETABLE_MODULES_TO_SKIP:
            continue
        tree = ast.parse(py_file.read_text())
        for node in ast.iter_child_nodes(tree):
            if isinstance(node, ast.ClassDef) and not node.name.startswith("_"):
                paths.append(f"airflow.timetables.{module_name}.{node.name}")
    return paths


def update_schema(known_types: list[str]) -> bool:
    """
    Update the timetable definition in schema.json with known built-in type examples.

    Returns True if the file was modified.
    """
    schema = json.loads(SCHEMA_PATH.read_text())
    timetable_def = schema.get("definitions", {}).get("timetable")
    if timetable_def is None:
        console.print("[red]Error: 'timetable' definition not found in schema.json[/]")
        sys.exit(1)

    type_prop = timetable_def.get("properties", {}).get("__type")
    if type_prop is None:
        console.print("[red]Error: '__type' property not found in timetable definition[/]")
        sys.exit(1)

    current_examples = type_prop.get("examples", [])
    if current_examples == known_types:
        return False

    type_prop["examples"] = known_types

    SCHEMA_PATH.write_text(json.dumps(schema, indent=2) + "\n")
    return True


def main() -> int:
    builtin_paths = set(extract_builtin_timetable_paths())
    core_paths = set(scan_core_timetable_classes())
    all_types = sorted(builtin_paths | core_paths)

    if not all_types:
        console.print("[red]Error: no timetable types found — check encoders.py and timetables/[/]")
        return 1

    console.print(f"[blue]Found {len(all_types)} built-in timetable types[/]")

    if update_schema(all_types):
        console.print("[yellow]Updated timetable examples in schema.json[/]")
        return 1

    console.print("[green]Timetable examples in schema.json are up to date[/]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
