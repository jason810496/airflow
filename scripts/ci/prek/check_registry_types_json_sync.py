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
"""Check that registry/src/_data/types.json is in sync with registry_tools/types.py."""

from __future__ import annotations

import ast
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))

from common_prek_utils import AIRFLOW_ROOT_PATH, console

TYPES_PY = AIRFLOW_ROOT_PATH / "dev" / "registry" / "registry_tools" / "types.py"
TYPES_JSON = AIRFLOW_ROOT_PATH / "registry" / "src" / "_data" / "types.json"


def _extract_string(node: ast.expr) -> str | None:
    """Extract a string value from an AST constant node."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def load_types_from_py() -> list[dict]:
    """Parse MODULE_TYPES from types.py using AST and build the expected JSON list."""
    tree = ast.parse(TYPES_PY.read_text(), filename=str(TYPES_PY))

    for node in ast.walk(tree):
        target: ast.expr
        # MODULE_TYPES uses annotated assignment: MODULE_TYPES: dict[str, dict] = {...}
        if isinstance(node, ast.AnnAssign):
            target = node.target
            value = node.value
        elif isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            value = node.value
        else:
            continue

        if isinstance(target, ast.Name) and target.id == "MODULE_TYPES":
            if not isinstance(value, ast.Dict):
                continue
            result = []
            for key_node, value_node in zip(value.keys, value.values):
                if key_node is None:
                    continue
                type_id = _extract_string(key_node)
                if type_id is None or not isinstance(value_node, ast.Dict):
                    continue
                info = {}
                for k, v in zip(value_node.keys, value_node.values):
                    if k is None:
                        continue
                    field_name = _extract_string(k)
                    if field_name in ("label", "icon"):
                        info[field_name] = _extract_string(v)
                if "label" in info and "icon" in info:
                    result.append({"id": type_id, "label": info["label"], "icon": info["icon"]})
            return result

    console.print(f"[red]ERROR: Could not find MODULE_TYPES in {TYPES_PY}[/]", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    if not TYPES_JSON.exists():
        console.print(f"[red]ERROR: {TYPES_JSON} does not exist.[/]", file=sys.stderr)
        console.print("Run: uv run python dev/registry/generate_types_json.py", file=sys.stderr)
        sys.exit(1)

    expected = load_types_from_py()
    actual = json.loads(TYPES_JSON.read_text())

    if expected == actual:
        sys.exit(0)

    console.print("[red]ERROR: registry/src/_data/types.json is out of sync with[/]", file=sys.stderr)
    console.print("       dev/registry/registry_tools/types.py", file=sys.stderr)
    console.print(file=sys.stderr)
    console.print("Run: uv run python dev/registry/generate_types_json.py", file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    main()
