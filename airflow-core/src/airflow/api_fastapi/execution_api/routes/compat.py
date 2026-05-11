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
Compat protocol routes -- OpenAPI publication only.

These endpoints exist solely so the head-shape schemas of
``StartupDetails`` and ``DagFileParseRequest`` appear in the
Execution API's OpenAPI document. Foreign-language SDKs (Java, etc.)
run their own codegen against that document at SDK build time and
freeze the resulting decoders against the schema version they saw.

The handlers themselves are intentional no-op echoes: a payload comes
in shaped at the head version and goes back out unchanged. Cadwyn
would still rewrite the response body according to the
``Airflow-API-Version`` header if these routes were actually called,
but **production callers do not call them**. The supervisor and the
parser-supervisor migrate ``StartupDetails`` and
``DagFileParseRequest`` in-process via
:class:`~airflow.sdk.execution_time.schema_migrator.SchemaVersionMigrator`,
which is anchored on the same single
:data:`~airflow.api_fastapi.execution_api.versions.bundle` -- one
source of truth for what the schemas look like at each version,
exposed two ways (OpenAPI here, callable migration there).

If you are tempted to add a new endpoint here, ask first whether the
foreign runtime needs the schema published in OpenAPI. If yes, add it;
if the only need is in-process migration, extend the bundle's
``VersionChange`` entries and use the migrator directly -- do not
add another echo route.
"""

from __future__ import annotations

from fastapi import APIRouter

from airflow.api_fastapi.execution_api.datamodels.compat import (
    DagFileParseRequestCompat,
    StartupDetails,
)

router = APIRouter()


# OpenAPI-only: the path is registered so the StartupDetails schema is
# emitted into the spec foreign-language SDK codegen consumes. The
# handler body is a no-op echo; the supervisor's in-process compat
# interceptor is what actually migrates payloads in production.
@router.post("/startup-details")
def echo_startup_details(body: StartupDetails) -> StartupDetails:
    """Echo a ``StartupDetails`` payload. Exists for OpenAPI schema publication only."""
    return body


# OpenAPI-only: see the comment on echo_startup_details. Uses the slim
# ``DagFileParseRequestCompat`` shape rather than the full
# ``DagFileParseRequest`` to keep ``callback_requests``
# (Python-runtime-only) out of the OpenAPI surface.
@router.post("/dag-file-parse-request")
def echo_dag_file_parse_request(body: DagFileParseRequestCompat) -> DagFileParseRequestCompat:
    """Echo a ``DagFileParseRequest`` payload. Exists for OpenAPI schema publication only."""
    return body
