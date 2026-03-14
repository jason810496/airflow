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
import shutil
import textwrap
from pathlib import Path
from typing import Any

from airflow.providers.common.builder.config import (
    FEATURE_DIRECTORY_MAP,
    NewProviderRequest,
)

_LICENSE_HEADER = """\
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

_TEMPLATE_DIR = Path(__file__).parent / "placeholder_provider"


class ProviderCreator:
    """Create a new Airflow provider package from a ``NewProviderRequest``."""

    def __init__(self, request: NewProviderRequest) -> None:
        self.request = request

    def create(self) -> Path:
        """Generate the provider and return the path to the created package."""
        r = self.request
        target = Path(r.output_dir)

        for part in r.module_parts:
            target = target / part
            target.mkdir(parents=True, exist_ok=True)
            init_file = target / "__init__.py"
            if not init_file.exists():
                init_file.write_text(_LICENSE_HEADER)

        shutil.copytree(
            _TEMPLATE_DIR,
            target,
            dirs_exist_ok=True,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        )

        self._remove_excluded_features(target)
        self._replace_placeholders(target)
        self._rename_files(target)
        self._write_provider_info(target)

        return target

    def _remove_excluded_features(self, target: Path) -> None:
        for f in dataclasses.fields(self.request.features):
            if getattr(self.request.features, f.name):
                continue
            directory = FEATURE_DIRECTORY_MAP.get(f.name)
            if directory is None:
                continue
            feature_dir = target / directory
            if feature_dir.is_dir():
                shutil.rmtree(feature_dir)

    def _replace_placeholders(self, target: Path) -> None:
        r = self.request
        replacements = [
            ("apache-airflow-providers-placeholder-provider", r.pip_package_name),
            ("airflow.providers.placeholder_provider", r.module_path),
            ("placeholder_provider", r.provider_name),
            ("PlaceholderProvider", r.class_name),
        ]
        for path in target.rglob("*"):
            if path.is_file() and path.suffix == ".py":
                text = path.read_text()
                for old, new in replacements:
                    text = text.replace(old, new)
                path.write_text(text)

    def _rename_files(self, target: Path) -> None:
        for path in sorted(target.rglob("placeholder_provider.py"), reverse=True):
            new_name = path.parent / f"{self.request.provider_name}.py"
            path.rename(new_name)

    def _write_provider_info(self, target: Path) -> None:
        info = self._build_provider_info()
        source = self._render_provider_info_source(info)
        (target / "get_provider_info.py").write_text(source)

    def _build_provider_info(self) -> dict[str, Any]:
        r = self.request
        f = r.features
        module = r.module_path

        info: dict[str, Any] = {
            "package-name": r.pip_package_name,
            "name": r.class_name,
            "description": r.description,
        }

        if f.integrations:
            info["integrations"] = [
                {
                    "integration-name": r.class_name,
                    "external-doc-url": "https://example.com",
                    "tags": ["service"],
                }
            ]
        if f.operators:
            info["operators"] = [
                {
                    "integration-name": r.class_name,
                    "python-modules": [f"{module}.operators.{r.provider_name}"],
                }
            ]
        if f.sensors:
            info["sensors"] = [
                {
                    "integration-name": r.class_name,
                    "python-modules": [f"{module}.sensors.{r.provider_name}"],
                }
            ]
        if f.hooks:
            info["hooks"] = [
                {
                    "integration-name": r.class_name,
                    "python-modules": [f"{module}.hooks.{r.provider_name}"],
                }
            ]
        if f.triggers:
            info["triggers"] = [
                {
                    "integration-name": r.class_name,
                    "python-modules": [f"{module}.triggers.{r.provider_name}"],
                }
            ]
        if f.transfers:
            info["transfers"] = [
                {
                    "source-integration-name": r.class_name,
                    "target-integration-name": r.class_name,
                    "python-module": f"{module}.transfers.{r.provider_name}",
                }
            ]
        if f.bundles:
            info["bundles"] = [
                {
                    "integration-name": r.class_name,
                    "python-modules": [f"{module}.bundles.{r.provider_name}"],
                }
            ]
        if f.connection_types:
            info["connection-types"] = [
                {
                    "connection-type": r.provider_name,
                    "hook-class-name": f"{module}.hooks.{r.provider_name}.{r.class_name}Hook",
                }
            ]
        if f.extra_links:
            info["extra-links"] = [f"{module}.links.{r.provider_name}.{r.class_name}Link"]
        if f.filesystems:
            info["filesystems"] = [f"{module}.fs.{r.provider_name}"]
        if f.asset_uris:
            info["asset-uris"] = [
                {
                    "schemes": [r.provider_name],
                    "handler": f"{module}.assets.{r.provider_name}.sanitize_uri",
                    "factory": f"{module}.assets.{r.provider_name}.create_asset",
                }
            ]
        if f.dialects:
            info["dialects"] = [
                {
                    "dialect-type": r.provider_name,
                    "dialect-class-name": f"{module}.dialects.{r.provider_name}.{r.class_name}Dialect",
                }
            ]
        if f.secrets_backends:
            info["secrets-backends"] = [f"{module}.secrets.{r.provider_name}.{r.class_name}SecretsBackend"]
        if f.logging:
            info["logging"] = [f"{module}.log.{r.provider_name}.{r.class_name}TaskHandler"]
        if f.auth_backends:
            info["auth-backends"] = [f"{module}.auth_backend.{r.provider_name}"]
        if f.auth_managers:
            info["auth-managers"] = [f"{module}.auth_managers.{r.provider_name}.{r.class_name}AuthManager"]
        if f.notifications:
            info["notifications"] = [f"{module}.notifications.{r.provider_name}.{r.class_name}Notifier"]
        if f.executors:
            info["executors"] = [f"{module}.executors.{r.provider_name}.{r.class_name}Executor"]
        if f.cli:
            info["cli"] = [f"{module}.cli.definition.get_{r.provider_name}_cli_commands"]
        if f.task_decorators:
            info["task-decorators"] = [
                {
                    "name": r.provider_name,
                    "path": f"{module}.decorators.{r.provider_name}.{r.provider_name}_task",
                }
            ]
        if f.plugins:
            info["plugins"] = [
                {
                    "name": r.provider_name,
                    "plugin-class": f"{module}.plugins.{r.provider_name}.{r.class_name}Plugin",
                }
            ]
        if f.queues:
            info["queues"] = [
                {
                    "name": r.provider_name,
                    "message-queue-class": f"{module}.queues.{r.provider_name}.{r.class_name}MessageQueue",
                }
            ]
        if f.config:
            info["config"] = {
                r.provider_name: {
                    "description": f"{r.class_name} provider configuration",
                    "options": {
                        "api_key": {
                            "description": f"API key for {r.class_name}",
                            "version_added": "0.1.0",
                            "type": "string",
                            "example": "my-api-key",
                            "default": "",
                            "sensitive": True,
                        }
                    },
                }
            }
        return info

    @staticmethod
    def _render_provider_info_source(info: dict[str, Any]) -> str:
        lines = _LICENSE_HEADER + "\n\ndef get_provider_info():\n    return "
        rendered = _format_dict(info, indent=8)
        return lines + rendered + "\n"


def _format_dict(obj: Any, indent: int = 0) -> str:
    """Pretty-format a Python literal with the given *indent* for the first line."""
    import pprint

    raw = pprint.pformat(obj, width=100, sort_dicts=False)
    indented = textwrap.indent(raw, " " * indent)
    return indented.strip()
