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

from airflow.providers.common.builder.cli.prompt import prompt_text, prompt_yes_no, to_pascal_case, validate

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
    """Namespace for provider builder CLI arguments."""

    def __init__(self, args: argparse.Namespace) -> None:
        self._provider_name: str | None = getattr(args, "provider_name", None)
        self._package_name: str | None = getattr(args, "package_name", None)
        self._provider_description: str | None = getattr(args, "provider_description", None)
        self.output_dir: str = getattr(args, "output_dir")
        self.interactive: bool = getattr(args, "interactive")
        self.exclude_unit_tests: bool = getattr(args, "exclude_unit_tests")
        self.include_all_features: bool = getattr(args, "include_all_features")
        self._exclude_features: list[str] | set[str] | None = getattr(args, "exclude_features", None)
        # Track if any invalid fields were found during validation
        self.invalid = False
        self.initialized = False
        self._validate_config(args)

    def _validate_config(self, args: argparse.Namespace) -> None:
        """Validate all arguments."""
        # check required fields are provided for non-interactive mode
        required_fields = ["provider_name", "package_name", "provider_description"]
        if not self.interactive:
            missing_required = []
            for field_name in required_fields:
                if getattr(args, field_name, None) is None:
                    missing_required.append(field_name)

            if missing_required:
                console.print(
                    f"[yellow]Missing required arguments in non-interactive mode: {', '.join(missing_required)}[/yellow]"
                )
                self.invalid = True
                return

        # access all properties to trigger validation or prompting
        for field in required_fields:
            getattr(self, field)
        if not self.invalid:
            self.initialized = True

    @property
    def provider_name(self) -> str:
        """Get provider name."""
        if self._provider_name is None and self.interactive:
            self._provider_name = prompt_text(
                "Provider name (snake_case)",
                default="my_provider",
                validation_callable=ProviderBuilderArgs.validate_provider_name,
            )
        elif not self.initialized and not validate(
            self._provider_name, ProviderBuilderArgs.validate_provider_name
        ):
            self.invalid = True

        return self._provider_name

    @property
    def package_name(self) -> str:
        """Get package name."""
        if self._package_name is None and self.interactive:
            self._package_name = prompt_text(
                "Package name",
                default=f"apache-airflow-providers-{self.provider_name.replace('_', '-')}",
                validation_callable=ProviderBuilderArgs.validate_package_name,
            )
        elif not self.initialized and not validate(
            self._package_name, ProviderBuilderArgs.validate_package_name
        ):
            self.invalid = True

        return self._package_name

    @property
    def provider_description(self) -> str:
        if self._provider_description is None and self.interactive:
            self._provider_description = prompt_text(
                "Provider description",
                default=f"{to_pascal_case(self.provider_name)} provider",
                validation_callable=ProviderBuilderArgs.validate_description,
            )
        elif not self.initialized and not validate(
            self._provider_description, ProviderBuilderArgs.validate_description
        ):
            self.invalid = True

        return self._provider_description

    @property
    def exclude_features(self) -> set[str]:
        """Get list of features to exclude."""
        if self._exclude_features is None and self.interactive:
            exclude_str = prompt_text(
                "Comma-separated list of features to exclude",
                default="",
                validation_callable=ProviderBuilderArgs.validate_exclude_features,
            )
            self._exclude_features = {
                feature.strip() for feature in exclude_str.split(",") if feature.strip()
            }
        elif not self.initialized and not validate(
            ", ".join(self._exclude_features) if self._exclude_features else "",
            ProviderBuilderArgs.validate_exclude_features,
        ):
            self.invalid = True
            self._exclude_features = set(self._exclude_features or [])

        return self._exclude_features

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

        if exclude_features is None:
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
    args = ProviderBuilderArgs(cli_args)
    if args.invalid:
        console.print("[bold red]Error: Invalid arguments provided. Cannot create provider.[/bold red]")
        return

    create_provider(args)
