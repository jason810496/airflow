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

# List of all feature fields
feature_field_questions = [
    ("include_integrations", "Include integrations (integration-name, external-doc-url, logo, tags)?"),
    ("include_operators", "Include operators?"),
    ("include_sensors", "Include sensors?"),
    ("include_hooks", "Include hooks?"),
    ("include_triggers", "Include triggers?"),
    ("include_transfers", "Include transfers?"),
    ("include_connection_types", "Include connection types?"),
    ("include_extra_links", "Include extra links?"),
    ("include_secrets_backends", "Include secrets backends?"),
    ("include_logging", "Include logging?"),
    ("include_auth_backends", "Include auth backends?"),
    ("include_auth_managers", "Include auth managers?"),
    ("include_notifications", "Include notifications?"),
    ("include_executors", "Include executors?"),
    ("include_cli", "Include CLI?"),
    ("include_config", "Include config?"),
    ("include_task_decorators", "Include task decorators?"),
    ("include_plugins", "Include plugins?"),
    ("include_queues", "Include queues?"),
    ("include_filesystems", "Include filesystems?"),
    ("include_asset_uris", "Include asset URIs?"),
    ("include_dataset_uris", "Include dataset URIs?"),
    ("include_dialects", "Include dialects?"),
    ("include_bundles", "Include bundles?"),
    ("include_hook_class_names", "Include hook class names?"),
]
"""List of all feature fields with their corresponding questions."""

features_set = frozenset(field[8:] for field, _ in feature_field_questions)
"""Set of all feature fields (removed 'include_' prefix)"""
