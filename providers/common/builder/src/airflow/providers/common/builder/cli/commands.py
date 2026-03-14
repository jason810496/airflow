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

import dataclasses
from typing import TYPE_CHECKING

from rich.console import Console

from airflow.providers.common.builder.config import (
    NewProviderRequest,
    ProviderFeatures,
)
from airflow.providers.common.builder.provider_creator import ProviderCreator

if TYPE_CHECKING:
    import argparse

console = Console()


def _build_features_from_excludes(exclude_features: list[str] | None) -> ProviderFeatures:
    """Build a ``ProviderFeatures`` instance with excluded features set to ``False``."""
    features = ProviderFeatures()
    if not exclude_features:
        return features
    valid_names = {f.name for f in dataclasses.fields(ProviderFeatures)}
    for raw_name in exclude_features:
        name = raw_name.strip()
        if name not in valid_names:
            console.print(f"[yellow]Unknown feature to exclude: {name}[/yellow]")
            continue
        object.__setattr__(features, name, False)
    return features


def create_new_provider_command(cli_args: argparse.Namespace) -> None:
    """Create a new Airflow provider package skeleton."""
    package_name: str | None = getattr(cli_args, "package_name", None)
    description: str = getattr(cli_args, "provider_description", "An Apache Airflow provider package.")
    output_dir: str = getattr(cli_args, "output_dir", ".")
    interactive: bool = getattr(cli_args, "interactive", True)
    exclude_features: list[str] | None = getattr(cli_args, "exclude_features", None)

    if interactive and package_name is None:
        from airflow.providers.common.builder.cli.textual_ui import run_textual_ui

        request = run_textual_ui(output_dir=output_dir, description=description)
        if request is None:
            console.print("[yellow]Provider creation cancelled.[/yellow]")
            return
    else:
        if package_name is None:
            console.print("[bold red]Error: --package-name is required in non-interactive mode.[/bold red]")
            return

        is_valid, error = NewProviderRequest.validate_package_name(package_name)
        if not is_valid:
            console.print(f"[bold red]Error: {error}[/bold red]")
            return

        features = _build_features_from_excludes(exclude_features)
        request = NewProviderRequest.from_package_name(
            package_name,
            description=description,
            output_dir=output_dir,
            features=features,
        )

    console.print(f"\n[bold cyan]=== Creating provider '{request.class_name}' ===[/bold cyan]\n")

    creator = ProviderCreator(request)
    result_path = creator.create()

    console.print(f"\n[bold green]Provider created successfully in: {result_path}[/bold green]")
