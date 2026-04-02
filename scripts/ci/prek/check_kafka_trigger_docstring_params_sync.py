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
"""Check that KafkaMessageQueueTrigger docstring :param entries stay in sync with AwaitMessageTrigger."""

# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
from __future__ import annotations

import argparse
import ast
import re
import sys
from pathlib import Path

from common_prek_utils import AIRFLOW_ROOT_PATH, console

TRIGGERS_PATH = (
    AIRFLOW_ROOT_PATH
    / "providers"
    / "apache"
    / "kafka"
    / "src"
    / "airflow"
    / "providers"
    / "apache"
    / "kafka"
    / "triggers"
)
AWAIT_MESSAGE_FILE = TRIGGERS_PATH / "await_message.py"
MSG_QUEUE_FILE = TRIGGERS_PATH / "msg_queue.py"

PARAM_RE = re.compile(r":param\s+(\w+):")


def extract_docstring_params(file_path: Path, class_name: str) -> list[str]:
    """Extract :param names from the docstring of a class, preserving order."""
    tree = ast.parse(file_path.read_text("utf-8"), filename=str(file_path))
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            docstring = ast.get_docstring(node, clean=False)
            if docstring is None:
                return []
            return PARAM_RE.findall(docstring)
    return []


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check that KafkaMessageQueueTrigger and AwaitMessageTrigger "
        "docstring :param entries are in sync."
    )
    parser.add_argument(
        "--files", nargs="*", type=Path, help="Accepted for pre-commit compatibility but ignored."
    )
    return parser.parse_args()


def main() -> int:
    parse_args()

    await_params = extract_docstring_params(AWAIT_MESSAGE_FILE, "AwaitMessageTrigger")
    mq_params = extract_docstring_params(MSG_QUEUE_FILE, "KafkaMessageQueueTrigger")

    if not await_params:
        console.print(
            f"[red]ERROR: Could not extract :param entries from AwaitMessageTrigger "
            f"in {AWAIT_MESSAGE_FILE}[/]"
        )
        return 1

    if not mq_params:
        console.print(
            f"[red]ERROR: Could not extract :param entries from KafkaMessageQueueTrigger "
            f"in {MSG_QUEUE_FILE}[/]"
        )
        return 1

    await_set = set(await_params)
    mq_set = set(mq_params)

    errors: list[str] = []

    only_in_await = sorted(await_set - mq_set)
    only_in_mq = sorted(mq_set - await_set)

    if only_in_await:
        errors.append(
            f"  :param entries in AwaitMessageTrigger but missing from KafkaMessageQueueTrigger: "
            f"{', '.join(only_in_await)}"
        )
    if only_in_mq:
        errors.append(
            f"  :param entries in KafkaMessageQueueTrigger but missing from AwaitMessageTrigger: "
            f"{', '.join(only_in_mq)}"
        )

    if errors:
        console.print(
            "\n[red]ERROR: Docstring :param entries are out of sync between "
            "AwaitMessageTrigger and KafkaMessageQueueTrigger![/]\n"
        )
        for err in errors:
            console.print(f"[yellow]{err}[/]")
        console.print(
            f"\nPlease update the docstrings so both classes document the same parameters:\n"
            f"  - {AWAIT_MESSAGE_FILE}\n"
            f"  - {MSG_QUEUE_FILE}"
        )
        return 1

    console.print(
        "[green]KafkaMessageQueueTrigger and AwaitMessageTrigger docstring :param entries are in sync.[/]"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
