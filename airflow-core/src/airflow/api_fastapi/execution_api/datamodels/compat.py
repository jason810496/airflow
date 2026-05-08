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
Datamodels for the compat echo endpoints.

``StartupDetails`` is re-exported from its semantic home in ``task-sdk``
so the existing ``check-execution-api-versions`` prek hook sees it.

``DagFileParseRequestCompat`` is a slim wire schema dedicated to the
compat route. The full ``DagFileParseRequest`` (in
``airflow.dag_processing.processor``) carries a ``callback_requests``
field whose discriminated union of callback types breaks the OpenAPI
codegen pipeline (nullable ``Literal`` discriminators are not legal in
Pydantic). Foreign-language SDK runtimes never execute Python
callbacks anyway, so we expose only the fields they actually need to
parse a Dag file. The original ``DagFileParseRequest`` is also re-
exported here so the prek hook covers schema changes to it.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel

from airflow.dag_processing.processor import DagFileParseRequest
from airflow.sdk.execution_time.comms import StartupDetails


class DagFileParseRequestCompat(BaseModel):
    """
    Slim compat-protocol shape for ``DagFileParseRequest``.

    Mirrors the parser-supervisor's ``DagFileParseRequest`` minus the
    ``callback_requests`` field (which is Python-runtime-specific and
    causes the OpenAPI codegen to emit a malformed ``Literal | None``
    discriminator). The discriminator literal value matches the original
    so a foreign runtime that recognises ``"DagFileParseRequest"`` can
    decode either shape interchangeably.
    """

    file: str
    bundle_path: Path
    bundle_name: str
    type: Literal["DagFileParseRequest"] = "DagFileParseRequest"


__all__ = ["DagFileParseRequest", "DagFileParseRequestCompat", "StartupDetails"]
