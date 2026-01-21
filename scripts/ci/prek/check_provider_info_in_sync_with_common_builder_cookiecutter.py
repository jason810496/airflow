#!/usr/bin/env python
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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = []
# ///
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[3]
SCHEMA_PATH = ROOT_DIR / "airflow-core" / "src" / "airflow" / "provider_info.schema.json"
COOKIECUTTER_PATH = (
    ROOT_DIR
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


def get_schema_properties() -> set[str]:
    """Extract all properties from the provider_info.schema.json."""
    with open(SCHEMA_PATH) as f:
        schema = json.load(f)
    return set(schema.get("properties", {}).keys())


def get_cookiecutter_features() -> set[str]:
    """Extract all features from the cookiecutter.json."""
    with open(COOKIECUTTER_PATH) as f:
        cookiecutter = json.load(f)
    
    # Map include_* fields to their corresponding schema properties
    # Cookiecutter-specific metadata fields that shouldn't be in schema
    cookiecutter_metadata = {"provider_name", "package_name", "provider_description"}
    
    features = set()
    for key in cookiecutter.keys():
        if key in cookiecutter_metadata:
            # These are cookiecutter input fields
            continue
        elif key.startswith("include_"):
            # Convert include_operators -> operators, include_connection_types -> connection-types
            feature_name = key.replace("include_", "").replace("_", "-")
            features.add(feature_name)
        else:
            # Direct mappings like "name", "package-name", "description"
            features.add(key)
    
    return features


def main() -> int:
    """Check if cookiecutter.json is in sync with provider_info.schema.json."""
    schema_properties = get_schema_properties()
    cookiecutter_features = get_cookiecutter_features()

    missing_in_cookiecutter = schema_properties - cookiecutter_features
    extra_in_cookiecutter = cookiecutter_features - schema_properties

    errors = []

    if missing_in_cookiecutter:
        errors.append("Missing features in cookiecutter.json:")
        for feature in sorted(missing_in_cookiecutter):
            errors.append(f"  - {feature}")

    if extra_in_cookiecutter:
        errors.append("Extra features in cookiecutter.json (not in schema):")
        for feature in sorted(extra_in_cookiecutter):
            errors.append(f"  - {feature}")

    if errors:
        print("ERROR: cookiecutter.json is not in sync with provider_info.schema.json")
        print()
        for error in errors:
            print(error)
        print()
        print(f"Schema location: {SCHEMA_PATH}")
        print(f"Cookiecutter location: {COOKIECUTTER_PATH}")
        return 1

    print("SUCCESS: cookiecutter.json is in sync with provider_info.schema.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
