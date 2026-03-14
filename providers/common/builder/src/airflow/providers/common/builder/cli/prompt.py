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
"""Custom prompt CLI utilities for provider creation."""

from __future__ import annotations

from collections.abc import Callable

from rich.console import Console

console = Console()


def prompt_yes_no(question: str, default: str = "y") -> str:
    """Prompt for yes/no answer."""
    valid_answers = {"y", "n", "yes", "no"}
    prompt_text = f"{question} [y/n] (default: [green]{default}[/green]): "

    while True:
        answer = console.input(prompt_text).strip().lower() or default
        if answer in valid_answers:
            return "y" if answer in {"y", "yes"} else "n"
        console.print("[yellow]Please answer 'y' or 'n'[/yellow]")


def prompt_text(
    question: str,
    *,
    default: str | None = None,
    validation_callable: Callable[[str], tuple[bool, str | None]] | None = None,
) -> str:
    """Prompt for text input."""
    prompt_str = f"{question} (default: [green]{default}[/green]): " if default else f"{question}: "

    while True:
        answer = console.input(prompt_str).strip()
        if default is not None and not answer:
            return default
        if not answer:
            console.print("[yellow]This field is required.[/yellow]")
            continue

        valid, error = validation_callable(answer) if validation_callable else (True, None)
        if not valid and error:
            console.print(f"[yellow]{error}[/yellow]")
            continue
        return answer
