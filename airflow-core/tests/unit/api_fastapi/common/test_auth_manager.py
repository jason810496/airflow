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

from unittest.mock import Mock

from airflow.api_fastapi.common.auth_manager import auth_manager_from_app


class TestAuthManagerDependency:
    """Test the auth_manager_from_app dependency function."""

    def test_auth_manager_from_app_returns_instance_from_state(self):
        """Test that auth_manager_from_app correctly retrieves auth_manager from app.state."""
        # Create a mock auth manager
        mock_auth_manager = Mock()

        # Create a mock request with app.state.auth_manager
        mock_request = Mock()
        mock_request.app.state.auth_manager = mock_auth_manager

        # Call the dependency function
        result = auth_manager_from_app(mock_request)

        # Assert it returns the correct auth manager
        assert result is mock_auth_manager

    def test_auth_manager_from_app_integration_with_test_client(self, test_client):
        """Test that auth_manager_from_app works with the test client setup."""
        # The test_client fixture should have auth_manager set up
        from airflow.api_fastapi.common.auth_manager import auth_manager_from_app

        # Create a mock request using the test client's app
        mock_request = Mock()
        mock_request.app = test_client.app

        # Get the auth manager
        auth_manager = auth_manager_from_app(mock_request)

        # Verify it's not None (should be SimpleAuthManager from test fixture)
        assert auth_manager is not None
        assert hasattr(auth_manager, "get_url_login")
        assert hasattr(auth_manager, "get_url_logout")
