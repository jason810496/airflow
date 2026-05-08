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
Compat protocol echo endpoints.

The handlers here are intentional no-ops: they receive a payload in the
latest schema and return it unchanged. Cadwyn rewrites the response body
on the way out according to the ``Airflow-API-Version`` header on the
request, producing a payload shaped for that older schema version.

Foreign-language SDK runtimes (e.g. Java) recognize ``StartupDetails``
and ``DagFileParseRequest`` against a schema version frozen at SDK build
time. The supervisor uses these endpoints to migrate the payload it would
otherwise send directly down to the runtime over IPC, so an older runtime
sees a payload its parser understands.
"""

from __future__ import annotations

from fastapi import APIRouter

from airflow.api_fastapi.execution_api.datamodels.compat import (
    DagFileParseRequestCompat,
    StartupDetails,
)

router = APIRouter()


@router.post("/startup-details")
def echo_startup_details(body: StartupDetails) -> StartupDetails:
    """Echo a ``StartupDetails`` payload, migrated by Cadwyn to the requested API version."""
    return body


@router.post("/dag-file-parse-request")
def echo_dag_file_parse_request(body: DagFileParseRequestCompat) -> DagFileParseRequestCompat:
    """
    Echo a ``DagFileParseRequest`` payload, migrated by Cadwyn to the requested API version.

    Uses the slim ``DagFileParseRequestCompat`` shape rather than the full
    ``DagFileParseRequest`` to keep ``callback_requests`` (Python-runtime-only)
    out of the OpenAPI surface.
    """
    return body
