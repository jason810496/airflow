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

from typing import TypedDict

from pydantic import BaseModel


class HTTPExceptionResponse(BaseModel):
    """HTTPException Model used for error response."""

    detail: str | dict


class HTTPExceptionExample(TypedDict, total=False):
    """Structure for a single HTTP exception example."""

    summary: str
    description: str
    value: dict


def create_openapi_http_exception_doc(
    responses_status_code: list[int] | dict[int, list[HTTPExceptionExample]],
) -> dict:
    """
    Will create additional response example for errors raised by the endpoint.

    There is no easy way to introspect the code and automatically see what HTTPException are actually
    raised by the endpoint implementation. This piece of documentation needs to be kept
    in sync with the endpoint code manually.

    Validation error i.e 422 are natively added to the openapi documentation by FastAPI.

    Args:
        responses_status_code: Either a list of status codes (for backward compatibility)
            or a dict mapping status codes to lists of HTTPExceptionExample objects.

    Returns:
        A dict suitable for use in FastAPI's `responses` parameter.

    Examples:
        Simple usage (backward compatible):
            responses=create_openapi_http_exception_doc([404, 400])

        Multiple examples for same status code:
            responses=create_openapi_http_exception_doc({
                404: [
                    {
                        "summary": "Task instance not found",
                        "description": "The requested task instance does not exist",
                        "value": {"detail": "Task instance with id X not found"}
                    },
                    {
                        "summary": "Task instance is mapped",
                        "description": "Task instance is mapped, map_index required",
                        "value": {"detail": "Task instance is mapped, add the map_index value to the URL"}
                    }
                ]
            })
    """
    # Handle backward compatibility: list of status codes
    if isinstance(responses_status_code, list):
        responses_status_code = sorted(responses_status_code)
        return {status_code: {"model": HTTPExceptionResponse} for status_code in responses_status_code}

    # Handle new format: dict with examples
    result = {}
    for status_code, examples in responses_status_code.items():
        if not examples:
            # If no examples provided, use simple format
            result[status_code] = {"model": HTTPExceptionResponse}
        elif len(examples) == 1:
            # Single example: use simple format with description
            example = examples[0]
            result[status_code] = {
                "model": HTTPExceptionResponse,
                "description": example.get("description", example.get("summary", "")),
            }
        else:
            # Multiple examples: use content with examples
            examples_dict = {}
            for i, example in enumerate(examples):
                example_key = example.get("summary", f"example_{i}").lower().replace(" ", "_")
                examples_dict[example_key] = {
                    "summary": example.get("summary", ""),
                    "description": example.get("description", ""),
                    "value": example.get("value", {"detail": ""}),
                }

            result[status_code] = {
                "model": HTTPExceptionResponse,
                "content": {
                    "application/json": {
                        "examples": examples_dict,
                    }
                },
            }

    return result
