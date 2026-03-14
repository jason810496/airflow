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

from typing import TYPE_CHECKING

from airflow.auth.managers.base_auth_manager import BaseAuthManager, ResourceMethod

if TYPE_CHECKING:
    from airflow.auth.managers.models.resource_details import (
        AccessView,
        ConnectionDetails,
        DagAccessEntity,
        DagDetails,
        PoolDetails,
        VariableDetails,
    )


class PlaceholderProviderAuthManager(BaseAuthManager):
    """Auth manager for PlaceholderProvider."""

    def get_user_display_name(self) -> str:
        """Return the display name of the user."""
        raise NotImplementedError

    def is_authorized_configuration(self, *, method: ResourceMethod, user) -> bool:
        """Return whether the user is authorized to access configuration."""
        raise NotImplementedError

    def is_authorized_connection(
        self, *, method: ResourceMethod, details: ConnectionDetails | None = None, user=None
    ) -> bool:
        """Return whether the user is authorized to access connections."""
        raise NotImplementedError

    def is_authorized_dag(
        self,
        *,
        method: ResourceMethod,
        access_entity: DagAccessEntity | None = None,
        details: DagDetails | None = None,
        user=None,
    ) -> bool:
        """Return whether the user is authorized to access DAGs."""
        raise NotImplementedError

    def is_authorized_pool(
        self, *, method: ResourceMethod, details: PoolDetails | None = None, user=None
    ) -> bool:
        """Return whether the user is authorized to access pools."""
        raise NotImplementedError

    def is_authorized_variable(
        self, *, method: ResourceMethod, details: VariableDetails | None = None, user=None
    ) -> bool:
        """Return whether the user is authorized to access variables."""
        raise NotImplementedError

    def is_authorized_view(self, *, access_view: AccessView, user=None) -> bool:
        """Return whether the user is authorized to access a view."""
        raise NotImplementedError

    def is_logged_in(self) -> bool:
        """Return whether the user is logged in."""
        raise NotImplementedError
