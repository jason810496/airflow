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

from collections.abc import Sequence
from typing import TYPE_CHECKING

from airflow.models import BaseOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class PlaceholderProviderTransferOperator(BaseOperator):
    """
    Transfer operator for PlaceholderProvider.

    :param source_conn_id: Source connection ID.
    :param dest_conn_id: Destination connection ID.
    """

    template_fields: Sequence[str] = ("source_conn_id", "dest_conn_id")

    def __init__(
        self,
        *,
        source_conn_id: str = "placeholder_provider_default",
        dest_conn_id: str = "placeholder_provider_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.source_conn_id = source_conn_id
        self.dest_conn_id = dest_conn_id

    def execute(self, context: Context) -> None:
        """Execute the transfer."""
        raise NotImplementedError
