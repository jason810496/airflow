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
"""XCom backend for PlaceholderProvider."""

from __future__ import annotations

from typing import Any

from airflow.models.xcom import BaseXCom


class PlaceholderProviderXComBackend(BaseXCom):
    """XCom backend for PlaceholderProvider."""

    @staticmethod
    def serialize_value(value: Any, **kwargs) -> Any:
        """Serialize an XCom value."""
        return BaseXCom.serialize_value(value, **kwargs)

    @staticmethod
    def deserialize_value(result) -> Any:
        """Deserialize an XCom value."""
        return BaseXCom.deserialize_value(result)
