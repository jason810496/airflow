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

import argparse

from airflow.cli.cli_config import (
    ActionCommand,
    Arg,
    lazy_load_command,
    string_list_type,
)

ARG_PACKAGE_NAME = Arg(
    ("--package-name",),
    help=(
        "Dotted package path in the format airflow.providers.<module_path>.<ClassName>. "
        "The last segment must be PascalCase (the class prefix). "
        "Examples: airflow.providers.coffee.Coffee, "
        "airflow.providers.my_company.cloud.MyCloud"
    ),
    default=None,
)
ARG_PROVIDER_DESCRIPTION = Arg(
    ("--provider-description",),
    help="A short description of the provider package.",
    default="An Apache Airflow provider package.",
)
ARG_PATH = Arg(
    ("--output-dir",),
    help=(
        "The directory where the provider package skeleton should be created. "
        "If not provided, the current working directory will be used."
    ),
    default=".",
)
ARG_INTERACTIVE = Arg(
    ("--interactive",),
    help="If set, the command will prompt for additional information to customize the provider package.",
    action="store_true",
    default=True,
)
ARG_EXCLUDE_FEATURES = Arg(
    ("--exclude-features",),
    help=(
        "A comma-separated list of features to exclude from the provider package skeleton. "
        "Available features: operators, sensors, hooks, triggers, transfers, bundles, "
        "extra_links, filesystems, asset_uris, dialects, secrets_backends, logging, "
        "auth_backends, auth_managers, notifications, executors, cli, task_decorators, "
        "plugins, queues, xcom, connection_types, integrations, config."
    ),
    type=string_list_type,
)


def get_builder_cli_commands():
    """Return CLI commands for Provider Builder."""
    return [
        ActionCommand(
            name="new-provider",
            help="Create a new Airflow provider package skeleton",
            func=lazy_load_command(
                "airflow.providers.common.builder.cli.commands.create_new_provider_command"
            ),
            args=(
                ARG_PACKAGE_NAME,
                ARG_PROVIDER_DESCRIPTION,
                ARG_PATH,
                ARG_INTERACTIVE,
                ARG_EXCLUDE_FEATURES,
            ),
        ),
    ]


def get_parser() -> argparse.ArgumentParser:
    """
    Generate documentation; used by Sphinx argparse.

    :meta private:
    """
    from airflow.cli.cli_parser import AirflowHelpFormatter, DefaultHelpParser, _add_command

    parser = DefaultHelpParser(prog="airflow", formatter_class=AirflowHelpFormatter)
    subparsers = parser.add_subparsers(dest="subcommand", metavar="GROUP_OR_COMMAND")
    for group_command in get_builder_cli_commands():
        _add_command(subparsers, group_command)
    return parser
