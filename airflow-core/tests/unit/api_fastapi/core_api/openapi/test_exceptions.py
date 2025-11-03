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

import pytest
from fastapi import status

from airflow.api_fastapi.core_api.openapi.exceptions import (
    HTTPExceptionDoc,
    HTTPExceptionResponse,
    create_openapi_http_exception_doc,
)


class TestCreateOpenAPIHTTPExceptionDoc:
    """Tests for create_openapi_http_exception_doc function."""

    def test_backward_compatibility_simple_list(self):
        """Test that the function works with a simple list of status codes (backward compatibility)."""
        result = create_openapi_http_exception_doc([
            status.HTTP_404_NOT_FOUND,
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_422_UNPROCESSABLE_ENTITY
        ])

        assert status.HTTP_400_BAD_REQUEST in result
        assert status.HTTP_404_NOT_FOUND in result
        assert status.HTTP_422_UNPROCESSABLE_ENTITY in result
        assert result[status.HTTP_404_NOT_FOUND] == {"model": HTTPExceptionResponse}
        assert result[status.HTTP_400_BAD_REQUEST] == {"model": HTTPExceptionResponse}
        assert result[status.HTTP_422_UNPROCESSABLE_ENTITY] == {"model": HTTPExceptionResponse}

    def test_single_example_per_status_code(self):
        """Test with a single example per status code."""
        result = create_openapi_http_exception_doc(
            [
                HTTPExceptionDoc(
                    status_code=status.HTTP_404_NOT_FOUND,
                    examples=[
                        {
                            "summary": "Not found",
                            "description": "Resource not found",
                            "value": {"detail": "Resource not found"},
                        }
                    ]
                )
            ]
        )

        assert status.HTTP_404_NOT_FOUND in result
        assert result[status.HTTP_404_NOT_FOUND]["model"] == HTTPExceptionResponse
        assert result[status.HTTP_404_NOT_FOUND]["description"] == "Resource not found"

    def test_multiple_examples_per_status_code(self):
        """Test with multiple examples for the same status code."""
        result = create_openapi_http_exception_doc(
            [
                HTTPExceptionDoc(
                    status_code=status.HTTP_404_NOT_FOUND,
                    examples=[
                        {
                            "summary": "Task instance not found",
                            "description": "The requested task instance does not exist",
                            "value": {"detail": "Task instance with id X not found"},
                        },
                        {
                            "summary": "Task instance is mapped",
                            "description": "Task instance is mapped, map_index required",
                            "value": {"detail": "Task instance is mapped, add the map_index value to the URL"},
                        },
                    ]
                )
            ]
        )

        assert status.HTTP_404_NOT_FOUND in result
        assert result[status.HTTP_404_NOT_FOUND]["model"] == HTTPExceptionResponse
        assert "content" in result[status.HTTP_404_NOT_FOUND]
        assert "application/json" in result[status.HTTP_404_NOT_FOUND]["content"]
        assert "examples" in result[status.HTTP_404_NOT_FOUND]["content"]["application/json"]

        examples = result[status.HTTP_404_NOT_FOUND]["content"]["application/json"]["examples"]
        assert "task_instance_not_found" in examples
        assert "task_instance_is_mapped" in examples

        # Check first example
        example1 = examples["task_instance_not_found"]
        assert example1["summary"] == "Task instance not found"
        assert example1["description"] == "The requested task instance does not exist"
        assert example1["value"]["detail"] == "Task instance with id X not found"

        # Check second example
        example2 = examples["task_instance_is_mapped"]
        assert example2["summary"] == "Task instance is mapped"
        assert example2["description"] == "Task instance is mapped, map_index required"
        assert example2["value"]["detail"] == "Task instance is mapped, add the map_index value to the URL"

    def test_mixed_examples(self):
        """Test with mixed examples: some status codes with multiple examples, some with single."""
        result = create_openapi_http_exception_doc(
            [
                HTTPExceptionDoc(
                    status_code=status.HTTP_404_NOT_FOUND,
                    examples=[
                        {
                            "summary": "Not found",
                            "description": "Resource not found",
                            "value": {"detail": "Resource not found"},
                        }
                    ]
                ),
                HTTPExceptionDoc(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    examples=[
                        {
                            "summary": "Invalid parameter",
                            "description": "The provided parameter is invalid",
                            "value": {"detail": "Invalid parameter value"},
                        },
                        {
                            "summary": "Missing parameter",
                            "description": "Required parameter is missing",
                            "value": {"detail": "Missing required parameter"},
                        },
                    ]
                ),
            ]
        )

        # Check single example (404)
        assert status.HTTP_404_NOT_FOUND in result
        assert result[status.HTTP_404_NOT_FOUND]["model"] == HTTPExceptionResponse
        assert result[status.HTTP_404_NOT_FOUND]["description"] == "Resource not found"
        assert "content" not in result[status.HTTP_404_NOT_FOUND]

        # Check multiple examples (400)
        assert status.HTTP_400_BAD_REQUEST in result
        assert result[status.HTTP_400_BAD_REQUEST]["model"] == HTTPExceptionResponse
        assert "content" in result[status.HTTP_400_BAD_REQUEST]
        examples = result[status.HTTP_400_BAD_REQUEST]["content"]["application/json"]["examples"]
        assert "invalid_parameter" in examples
        assert "missing_parameter" in examples

    def test_empty_examples_list(self):
        """Test with an empty examples list."""
        result = create_openapi_http_exception_doc([
            HTTPExceptionDoc(status_code=status.HTTP_404_NOT_FOUND, examples=[])
        ])

        assert status.HTTP_404_NOT_FOUND in result
        assert result[status.HTTP_404_NOT_FOUND] == {"model": HTTPExceptionResponse}

    def test_mixed_int_and_namedtuple(self):
        """Test mixing simple ints and HTTPExceptionDoc with examples in the same list."""
        result = create_openapi_http_exception_doc(
            [
                status.HTTP_400_BAD_REQUEST,  # Simple int
                status.HTTP_422_UNPROCESSABLE_ENTITY,  # Another simple int
                HTTPExceptionDoc(
                    status_code=status.HTTP_404_NOT_FOUND,
                    examples=[
                        {
                            "summary": "Not found",
                            "description": "Resource not found",
                            "value": {"detail": "Resource not found"},
                        }
                    ]
                ),
            ]
        )

        # Check simple ints
        assert status.HTTP_400_BAD_REQUEST in result
        assert result[status.HTTP_400_BAD_REQUEST] == {"model": HTTPExceptionResponse}
        assert status.HTTP_422_UNPROCESSABLE_ENTITY in result
        assert result[status.HTTP_422_UNPROCESSABLE_ENTITY] == {"model": HTTPExceptionResponse}

        # Check HTTPExceptionDoc with examples
        assert status.HTTP_404_NOT_FOUND in result
        assert result[status.HTTP_404_NOT_FOUND]["model"] == HTTPExceptionResponse
        assert result[status.HTTP_404_NOT_FOUND]["description"] == "Resource not found"
