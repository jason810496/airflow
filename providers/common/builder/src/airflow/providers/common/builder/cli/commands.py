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

from pathlib import Path
from typing import TYPE_CHECKING, TypedDict

from cookiecutter.main import cookiecutter
from rich.console import Console

from airflow.providers.common.builder.cli.prompt import prompt_yes_no, to_pascal_case

if TYPE_CHECKING:
    import argparse

console = Console()


class ProviderContext(TypedDict):
    """Type definition for provider context dictionary."""

    provider_name: str
    package_name: str
    provider_description: str
    name: str
    description: str
    class_name: str
    include_all: str
    include_integrations: str
    include_operators: str
    include_sensors: str
    include_hooks: str
    include_triggers: str
    include_transfers: str
    include_connection_types: str
    include_extra_links: str
    include_secrets_backends: str
    include_logging: str
    include_auth_backends: str
    include_auth_managers: str
    include_notifications: str
    include_executors: str
    include_cli: str
    include_config: str
    include_task_decorators: str
    include_plugins: str
    include_queues: str
    include_filesystems: str
    include_asset_uris: str
    include_dataset_uris: str
    include_dialects: str
    include_bundles: str
    include_hook_class_names: str


class ProviderBuilderArgs:
    """Container for provider builder CLI arguments and validation methods."""

    def __init__(self, args: argparse.Namespace) -> None:
        self.provider_name: str | None = getattr(args, "provider_name", None)
        self.package_name: str | None = getattr(args, "package_name", None)
        self.provider_description: str | None = getattr(args, "provider_description", None)
        self.output_dir: str = getattr(args, "output_dir")
        self.interactive: bool = getattr(args, "interactive")
        self.exclude_unit_tests: bool = getattr(args, "exclude_unit_tests")
        self.include_all_features: bool = getattr(args, "include_all_features")
        self.exclude_features: set[str] = set(getattr(args, "exclude_features", None) or [])

    def validate_required_for_non_interactive(self) -> bool:
        """Check if all required fields are provided for non-interactive mode."""
        if self.interactive:
            return True

        missing_required = []
        if self.provider_name is None:
            missing_required.append("provider_name")
        if self.package_name is None:
            missing_required.append("package_name")
        if self.provider_description is None:
            missing_required.append("provider_description")

        if missing_required:
            console.print(
                f"[yellow]Missing required arguments in non-interactive mode: {', '.join(missing_required)}[/yellow]"
            )
            return False

        # Validate the provided values
        is_valid, error = self.validate_provider_name(self.provider_name)
        if not is_valid:
            console.print(f"[red]Invalid provider_name: {error}[/red]")
            return False

        is_valid, error = self.validate_package_name(self.package_name)
        if not is_valid:
            console.print(f"[red]Invalid package_name: {error}[/red]")
            return False

        is_valid, error = self.validate_description(self.provider_description)
        if not is_valid:
            console.print(f"[red]Invalid provider_description: {error}[/red]")
            return False

        if self.exclude_features:
            is_valid, error = self.validate_exclude_features(",".join(self.exclude_features))
            if not is_valid:
                console.print(f"[red]Invalid exclude_features: {error}[/red]")
                return False

        return True

    @property
    def class_name(self) -> str:
        """Compute class name from provider name."""
        if self.provider_name:
            return to_pascal_case(self.provider_name)
        return ""

    @staticmethod
    def validate_provider_name(provider_name: str) -> tuple[bool, str | None]:
        """Validate that provider name is in snake_case format."""
        if all(c.islower() or c == "_" or c.isdigit() for c in provider_name):
            return True, None
        return False, "Provider name must be in snake_case format."

    @staticmethod
    def validate_package_name(package_name: str) -> tuple[bool, str | None]:
        """Validate that package name starts with 'apache-airflow-providers-'."""
        if package_name.startswith("apache-airflow-providers-") and all(
            c.islower() or c == "-" or c.isdigit() for c in package_name
        ):
            return True, None
        return (
            False,
            "Package name must start with 'apache-airflow-providers-' and be in lowercase with hyphens.",
        )

    @staticmethod
    def validate_description(description: str) -> tuple[bool, str | None]:
        """Validate that description is not empty."""
        if description.strip():
            return True, None
        return False, "Description must not be empty."

    @staticmethod
    def validate_exclude_features(exclude_features: str | None) -> tuple[bool, str | None]:
        """Validate that exclude features are valid."""
        from airflow.providers.common.builder.cli.config import features_set

        if not exclude_features:
            return True, None

        exclude_features_list = [
            feature.strip() for feature in exclude_features.split(",") if feature.strip()
        ]
        invalid_features = [feature for feature in exclude_features_list if feature not in features_set]
        if not invalid_features:
            return True, None
        return (
            False,
            f"Invalid features to exclude: {', '.join(invalid_features)}. Valid features are: {', '.join(sorted(features_set))}.",
        )


def create_cookiecutter_context_from_args(args: ProviderBuilderArgs) -> ProviderContext:
    """Collect all provider configuration inputs from user."""
    console.print("\n[bold cyan]=== Airflow Provider Builder ===[/bold cyan]\n")

    from airflow.providers.common.builder.cli.config import feature_field_questions

    # Build context with computed fields
    context: ProviderContext = {
        "provider_name": args.provider_name,
        "package_name": args.package_name,
        "provider_description": args.provider_description,
        "name": args.provider_name,
        "package-name": args.package_name,
        "description": args.provider_description,
        "class_name": args.class_name,
        **{field: "n" for field, _ in feature_field_questions},
    }

    # Include all or selective?
    console.print("\n[bold cyan]--- Component Selection ---[/bold cyan]\n")
    include_all = prompt_yes_no("Include all components?", default="y")
    context["include_all"] = include_all

    if include_all == "y":
        # Set all to 'y' without prompting
        for field, _ in feature_field_questions:
            context[field] = "y" if field not in args.exclude_features else "n"
        if not args.exclude_features:
            console.print("\n[green]All components will be included[/green]")
        else:
            console.print(
                f"\n[green]All components will be included except: {', '.join(sorted(args.exclude_features))}[/green]"
            )
    else:
        # Ask for each component individually
        console.print("\n[bold]Select individual components:[/bold]\n")
        for field, question in feature_field_questions:
            context[field] = prompt_yes_no(
                question, default="y" if field not in args.exclude_features else "n"
            )

    return context


def create_provider(args: ProviderBuilderArgs) -> None:
    """Create provider using collected inputs."""
    # Collect all inputs
    context = create_cookiecutter_context_from_args(args)

    console.print(f"\n[bold cyan]=== Creating provider '{context['provider_name']}' ===[/bold cyan]\n")

    # Call cookiecutter with no-input and our context
    cookiecutter(
        Path(__file__).parent.parent.as_posix(),
        no_input=True,
        extra_context=context,
        output_dir=args.output_dir,
    )

    console.print(
        f"\n[bold green]Provider created successfully in: "
        f"{args.output_dir}/{context['provider_name']}[/bold green]"
    )


def create_new_provider_command(cli_args: argparse.Namespace) -> None:
    """Create a new Airflow provider package skeleton."""
    # Use Textual UI if interactive mode
    args = ProviderBuilderArgs(cli_args)
    if not args.interactive:
        # Validate required fields for non-interactive mode
        if not args.validate_required_for_non_interactive():
            console.print("[bold red]Error: Invalid arguments provided. Cannot create provider.[/bold red]")
            return

        create_provider(args)
        return

    from airflow.providers.common.builder.cli.textual_ui import run_textual_ui

    context = run_textual_ui(args)
    if context is None:
        console.print("[yellow]Provider creation cancelled.[/yellow]")
        return

    console.print(f"\n[bold cyan]=== Creating provider '{context['provider_name']}' ===[/bold cyan]\n")

    # Call cookiecutter with no-input and our context
    cookiecutter(
        Path(__file__).parent.parent.as_posix(),
        no_input=True,
        extra_context=context,
        output_dir=args.output_dir,
    )

    console.print(
        f"\n[bold green]Provider created successfully in: "
        f"{args.output_dir}/{context['provider_name']}[/bold green]"
    )
