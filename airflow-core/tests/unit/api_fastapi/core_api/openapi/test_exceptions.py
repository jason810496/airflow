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

from airflow.api_fastapi.core_api.openapi.exceptions import (
    HTTPExceptionDoc,
    HTTPExceptionResponse,
    create_openapi_http_exception_doc,
)


class TestCreateOpenAPIHTTPExceptionDoc:
    """Tests for create_openapi_http_exception_doc function."""

    def test_backward_compatibility_simple_list(self):
        """Test that the function works with a simple list of status codes (backward compatibility)."""
        result = create_openapi_http_exception_doc([404, 400, 422])

        assert 400 in result
        assert 404 in result
        assert 422 in result
        assert result[404] == {"model": HTTPExceptionResponse}
        assert result[400] == {"model": HTTPExceptionResponse}
        assert result[422] == {"model": HTTPExceptionResponse}

    def test_single_example_per_status_code(self):
        """Test with a single example per status code."""
        result = create_openapi_http_exception_doc(
            [
                HTTPExceptionDoc(
                    status_code=404,
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

        assert 404 in result
        assert result[404]["model"] == HTTPExceptionResponse
        assert result[404]["description"] == "Resource not found"

    def test_multiple_examples_per_status_code(self):
        """Test with multiple examples for the same status code."""
        result = create_openapi_http_exception_doc(
            [
                HTTPExceptionDoc(
                    status_code=404,
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

        assert 404 in result
        assert result[404]["model"] == HTTPExceptionResponse
        assert "content" in result[404]
        assert "application/json" in result[404]["content"]
        assert "examples" in result[404]["content"]["application/json"]

        examples = result[404]["content"]["application/json"]["examples"]
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
                    status_code=404,
                    examples=[
                        {
                            "summary": "Not found",
                            "description": "Resource not found",
                            "value": {"detail": "Resource not found"},
                        }
                    ]
                ),
                HTTPExceptionDoc(
                    status_code=400,
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
        assert 404 in result
        assert result[404]["model"] == HTTPExceptionResponse
        assert result[404]["description"] == "Resource not found"
        assert "content" not in result[404]

        # Check multiple examples (400)
        assert 400 in result
        assert result[400]["model"] == HTTPExceptionResponse
        assert "content" in result[400]
        examples = result[400]["content"]["application/json"]["examples"]
        assert "invalid_parameter" in examples
        assert "missing_parameter" in examples

    def test_empty_examples_list(self):
        """Test with an empty examples list."""
        result = create_openapi_http_exception_doc([HTTPExceptionDoc(status_code=404, examples=[])])

        assert 404 in result
        assert result[404] == {"model": HTTPExceptionResponse}

    def test_mixed_int_and_namedtuple(self):
        """Test mixing simple ints and HTTPExceptionDoc with examples in the same list."""
        result = create_openapi_http_exception_doc(
            [
                400,  # Simple int
                422,  # Another simple int
                HTTPExceptionDoc(
                    status_code=404,
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
        assert 400 in result
        assert result[400] == {"model": HTTPExceptionResponse}
        assert 422 in result
        assert result[422] == {"model": HTTPExceptionResponse}

        # Check HTTPExceptionDoc with examples
        assert 404 in result
        assert result[404]["model"] == HTTPExceptionResponse
        assert result[404]["description"] == "Resource not found"
