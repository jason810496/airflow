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
"""Tests to ensure template features stay in sync with provider_info.schema.json."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from airflow.providers.common.builder.config import (
    FEATURE_DIRECTORY_MAP,
    FEATURE_PROVIDER_INFO_KEY_MAP,
    ProviderFeatures,
)


@pytest.fixture
def provider_yaml_schema_path() -> Path:
    return (
        Path(__file__).resolve().parents[8] / "airflow-core" / "src" / "airflow" / "provider.yaml.schema.json"
    )


@pytest.fixture
def schema_properties(provider_yaml_schema_path: Path) -> set[str]:
    with open(provider_yaml_schema_path) as f:
        schema = json.load(f)
    return set(schema.get("properties", {}).keys())


def test_feature_directory_map_covers_all_features():
    """All ProviderFeatures fields must have an entry in FEATURE_DIRECTORY_MAP."""
    feature_names = set(ProviderFeatures.all_feature_names())
    map_keys = set(FEATURE_DIRECTORY_MAP.keys())
    missing = feature_names - map_keys
    assert not missing, f"Missing FEATURE_DIRECTORY_MAP entries: {sorted(missing)}"


def test_feature_provider_info_key_map_covers_all_features():
    """All ProviderFeatures fields must have an entry in FEATURE_PROVIDER_INFO_KEY_MAP."""
    feature_names = set(ProviderFeatures.all_feature_names())
    map_keys = set(FEATURE_PROVIDER_INFO_KEY_MAP.keys())
    missing = feature_names - map_keys
    assert not missing, f"Missing FEATURE_PROVIDER_INFO_KEY_MAP entries: {sorted(missing)}"


def test_provider_info_keys_exist_in_schema(schema_properties: set[str]):
    """All provider info keys used by FEATURE_PROVIDER_INFO_KEY_MAP must exist in provider_info.schema.json."""
    info_keys = set(FEATURE_PROVIDER_INFO_KEY_MAP.values())
    missing = info_keys - schema_properties
    assert not missing, (
        f"Provider info keys not found in schema: {sorted(missing)}. "
        "Either add them to provider_info.schema.json or remove from FEATURE_PROVIDER_INFO_KEY_MAP."
    )
