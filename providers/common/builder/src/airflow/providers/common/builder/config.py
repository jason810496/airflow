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
from dataclasses import dataclass, field


@dataclass
class ProviderFeatures:
    """Feature flags for provider generation. All default to True."""

    operators: bool = True
    sensors: bool = True
    hooks: bool = True
    triggers: bool = True
    transfers: bool = True
    bundles: bool = True
    extra_links: bool = True
    filesystems: bool = True
    asset_uris: bool = True
    dialects: bool = True
    secrets_backends: bool = True
    logging: bool = True
    auth_backends: bool = True
    auth_managers: bool = True
    notifications: bool = True
    executors: bool = True
    cli: bool = True
    task_decorators: bool = True
    plugins: bool = True
    queues: bool = True
    xcom: bool = True
    connection_types: bool = True
    integrations: bool = True
    config: bool = True

    @classmethod
    def all_feature_names(cls) -> list[str]:
        """Return a sorted list of all feature field names."""
        return sorted(f.name for f in dataclasses.fields(cls))

    def disabled_features(self) -> list[str]:
        """Return feature names that are set to False."""
        return [f.name for f in dataclasses.fields(self) if not getattr(self, f.name)]

    def enabled_features(self) -> list[str]:
        """Return feature names that are set to True."""
        return [f.name for f in dataclasses.fields(self) if getattr(self, f.name)]


@dataclass
class NewProviderRequest:
    """All metadata needed to generate a new provider, parsed from --package-name."""

    package_name: str
    class_name: str
    module_path: str
    module_parts: list[str] = field(default_factory=list)
    provider_name: str = ""
    pip_package_name: str = ""
    description: str = "An Apache Airflow provider package."
    output_dir: str = "."
    features: ProviderFeatures = field(default_factory=ProviderFeatures)

    @classmethod
    def from_package_name(
        cls,
        package_name: str,
        *,
        description: str = "An Apache Airflow provider package.",
        output_dir: str = ".",
        features: ProviderFeatures | None = None,
    ) -> NewProviderRequest:
        """Parse a dotted package name into a full request."""
        parts = package_name.split(".")
        class_name = parts[-1]
        module_parts = parts[2:-1]
        return cls(
            package_name=package_name,
            class_name=class_name,
            module_path=".".join(parts[:-1]),
            module_parts=module_parts,
            provider_name=parts[-2],
            pip_package_name="apache-airflow-providers-"
            + "-".join(p.replace("_", "-") for p in module_parts),
            description=description,
            output_dir=output_dir,
            features=features or ProviderFeatures(),
        )

    @staticmethod
    def validate_package_name(package_name: str) -> tuple[bool, str | None]:
        """Validate that *package_name* has the expected format."""
        parts = package_name.split(".")
        if len(parts) < 4:
            return (
                False,
                "Package name must have at least 4 segments: airflow.providers.<module>.<ClassName>",
            )
        if parts[0] != "airflow" or parts[1] != "providers":
            return False, "Package name must start with 'airflow.providers.'"
        for part in parts[2:-1]:
            if not part.isidentifier():
                return False, f"Module segment '{part}' is not a valid Python identifier."
            if part != part.lower():
                return False, f"Module segment '{part}' must be lowercase."
        last = parts[-1]
        if not last[0].isupper():
            return False, f"Class name '{last}' must start with an uppercase letter."
        return True, None


FEATURE_DIRECTORY_MAP: dict[str, str | None] = {
    "operators": "operators",
    "sensors": "sensors",
    "hooks": "hooks",
    "triggers": "triggers",
    "transfers": "transfers",
    "bundles": "bundles",
    "extra_links": "links",
    "filesystems": "fs",
    "asset_uris": "assets",
    "dialects": "dialects",
    "secrets_backends": "secrets",
    "logging": "log",
    "auth_backends": "auth_backend",
    "auth_managers": "auth_managers",
    "notifications": "notifications",
    "executors": "executors",
    "cli": "cli",
    "task_decorators": "decorators",
    "plugins": "plugins",
    "queues": "queues",
    "xcom": "xcom",
    "connection_types": None,
    "integrations": None,
    "config": None,
}

FEATURE_PROVIDER_INFO_KEY_MAP: dict[str, str] = {
    "operators": "operators",
    "sensors": "sensors",
    "hooks": "hooks",
    "triggers": "triggers",
    "transfers": "transfers",
    "bundles": "bundles",
    "extra_links": "extra-links",
    "filesystems": "filesystems",
    "asset_uris": "asset-uris",
    "dialects": "dialects",
    "secrets_backends": "secrets-backends",
    "logging": "logging",
    "auth_backends": "auth-backends",
    "auth_managers": "auth-managers",
    "notifications": "notifications",
    "executors": "executors",
    "cli": "cli",
    "task_decorators": "task-decorators",
    "plugins": "plugins",
    "queues": "queues",
    "xcom": "xcom",
    "connection_types": "connection-types",
    "integrations": "integrations",
    "config": "config",
}

FEATURE_QUESTIONS: list[tuple[str, str]] = [
    ("operators", "Include operators?"),
    ("sensors", "Include sensors?"),
    ("hooks", "Include hooks?"),
    ("triggers", "Include triggers?"),
    ("transfers", "Include transfers?"),
    ("bundles", "Include DAG bundles?"),
    ("extra_links", "Include extra links?"),
    ("filesystems", "Include filesystems?"),
    ("asset_uris", "Include asset URIs?"),
    ("dialects", "Include SQL dialects?"),
    ("secrets_backends", "Include secrets backends?"),
    ("logging", "Include logging handlers?"),
    ("auth_backends", "Include auth backends?"),
    ("auth_managers", "Include auth managers?"),
    ("notifications", "Include notifications?"),
    ("executors", "Include executors?"),
    ("cli", "Include CLI commands?"),
    ("task_decorators", "Include task decorators?"),
    ("plugins", "Include plugins?"),
    ("queues", "Include message queues?"),
    ("xcom", "Include XCom backends?"),
    ("connection_types", "Include connection types?"),
    ("integrations", "Include integrations metadata?"),
    ("config", "Include config section?"),
]
