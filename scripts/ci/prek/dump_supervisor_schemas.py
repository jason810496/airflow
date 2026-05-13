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
"""
Dump the supervisor schema snapshot. Prints JSON to stdout.

Mirrors :mod:`scripts.ci.prek.generate_execution_api_schema` but for
the supervisor schema ``VersionBundle``: walks
:func:`airflow.sdk.execution_time.supervisor_schemas.registered_models`
and emits the head-version ``model_json_schema()`` for every wire body.

Run with cwd at the repo root.
"""

from __future__ import annotations

import json
import os
import sys

os.environ["_AIRFLOW__AS_LIBRARY"] = "1"

from airflow.sdk.execution_time.supervisor_schemas import bundle, registered_models

snapshot = {
    "api_version": str(bundle.versions[0].value),
    "schemas": {cls.__name__: cls.model_json_schema() for cls in registered_models()},
}
json.dump(snapshot, sys.stdout, indent=2, sort_keys=True)
sys.stdout.write("\n")
