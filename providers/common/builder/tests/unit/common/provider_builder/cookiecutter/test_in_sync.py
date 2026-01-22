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

"""Unit tests to ensure cookiecutter.json is in sync with provider_info.schema.json."""

from __future__ import annotations

import json
from pathlib import Path

import pytest


@pytest.fixture
def root_dir() -> Path:
    """Get the root directory of the Airflow repository."""
    return Path(__file__).resolve().parents[8]


@pytest.fixture
def schema_path(root_dir: Path) -> Path:
    """Get the path to provider_info.schema.json."""
    return root_dir / "airflow-core" / "src" / "airflow" / "provider_info.schema.json"


@pytest.fixture
def cookiecutter_path(root_dir: Path) -> Path:
    """Get the path to cookiecutter.json."""
    return (
        root_dir
        / "providers"
        / "common"
        / "builder"
        / "src"
        / "airflow"
        / "providers"
        / "common"
        / "builder"
        / "cookiecutter.json"
    )


@pytest.fixture
def schema_properties(schema_path: Path) -> set[str]:
    """Extract all properties from the provider_info.schema.json."""
    with open(schema_path) as f:
        schema = json.load(f)
    return set(schema.get("properties", {}).keys())


@pytest.fixture
def cookiecutter_features(cookiecutter_path: Path) -> set[str]:
    """Extract all features from the cookiecutter.json."""
    with open(cookiecutter_path) as f:
        cookiecutter = json.load(f)

    # Cookiecutter-specific metadata fields that shouldn't be in schema
    cookiecutter_metadata = {"provider_name", "package_name", "provider_description"}

    features = set()
    for key in cookiecutter.keys():
        if key in cookiecutter_metadata:
            # These are cookiecutter input fields
            continue
        if key.startswith("include_"):
            # Convert include_operators -> operators, include_connection_types -> connection-types
            feature_name = key.replace("include_", "").replace("_", "-")
            features.add(feature_name)
        else:
            # Direct mappings like "name", "package-name", "description"
            features.add(key)

    return features


def test_cookiecutter_has_all_schema_properties(
    schema_properties: set[str], cookiecutter_features: set[str]
) -> None:
    """Test that cookiecutter.json includes all properties from provider_info.schema.json."""
    missing_in_cookiecutter = schema_properties - cookiecutter_features

    assert not missing_in_cookiecutter, (
        f"Missing features in cookiecutter.json: {sorted(missing_in_cookiecutter)}. "
        "Add the corresponding include_* fields to cookiecutter.json."
    )


def test_cookiecutter_has_no_extra_properties(
    schema_properties: set[str], cookiecutter_features: set[str]
) -> None:
    """Test that cookiecutter.json doesn't have extra properties not in provider_info.schema.json."""
    extra_in_cookiecutter = cookiecutter_features - schema_properties

    assert not extra_in_cookiecutter, (
        f"Extra features in cookiecutter.json not in schema: {sorted(extra_in_cookiecutter)}. "
        "Either add these to provider_info.schema.json or remove from cookiecutter.json."
    )


def test_cookiecutter_and_schema_are_in_sync(
    schema_properties: set[str], cookiecutter_features: set[str]
) -> None:
    """Test that cookiecutter.json and provider_info.schema.json are completely in sync."""
    assert schema_properties == cookiecutter_features, (
        f"Schema and cookiecutter are out of sync.\n"
        f"Missing in cookiecutter: {sorted(schema_properties - cookiecutter_features)}\n"
        f"Extra in cookiecutter: {sorted(cookiecutter_features - schema_properties)}"
    )
