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
Datamodel re-exports for the compat echo endpoints.

The two IPC schemas covered by the compat protocol
(``StartupDetails`` and ``DagFileParseRequest``) are defined in their
semantic homes -- ``task-sdk`` for the supervisor-to-runtime startup
message, and ``airflow-core`` for the parser-supervisor request. Re-
exporting them here makes them visible from the
``execution_api/datamodels/`` directory the existing
``check-execution-api-versions`` prek hook already watches, so any
schema change inevitably touches this module and must be accompanied
by a corresponding ``VersionChange`` under ``execution_api/versions/``.
"""

from __future__ import annotations

from airflow.dag_processing.processor import DagFileParseRequest
from airflow.sdk.execution_time.comms import StartupDetails

__all__ = ["DagFileParseRequest", "StartupDetails"]
