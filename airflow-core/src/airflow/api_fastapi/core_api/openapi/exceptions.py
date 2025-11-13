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

from typing import Any, NamedTuple, TypedDict

from pydantic import BaseModel


class HTTPExceptionResponse(BaseModel):
    """HTTPException Model used for error response."""

    detail: str | dict


class HTTPExceptionValue(TypedDict):
    """Structure for the value field in an HTTP exception example."""

    detail: str | dict


class HTTPExceptionExample(TypedDict, total=True):
    """Structure for a single HTTP exception example."""

    summary: str
    description: str
    value: HTTPExceptionValue


class HTTPExceptionDoc(NamedTuple):
    """Structure for documenting HTTP exceptions with examples for a status code."""

    status_code: int
    examples: list[HTTPExceptionExample]


def create_openapi_http_exception_doc(
    responses_status_code: list[int | HTTPExceptionDoc],
) -> dict[int, dict[str, Any]]:
    """
    Will create additional response example for errors raised by the endpoint.

    There is no easy way to introspect the code and automatically see what HTTPException are actually
    raised by the endpoint implementation. This piece of documentation needs to be kept
    in sync with the endpoint code manually.

    Validation error i.e 422 are natively added to the openapi documentation by FastAPI.

    :param responses_status_code: A list where each element can be either:
        - An int representing a simple status code (for backward compatibility)
        - An HTTPExceptionDoc NamedTuple with status_code and examples
    :return: A dict suitable for use in FastAPI's `responses` parameter.

    **Examples**::

        Simple usage (backward compatible):
            responses=create_openapi_http_exception_doc([
                status.HTTP_404_NOT_FOUND,
                status.HTTP_400_BAD_REQUEST
            ])

        Multiple examples for same status code:
            responses=create_openapi_http_exception_doc([
                HTTPExceptionDoc(
                    status_code=status.HTTP_404_NOT_FOUND,
                    examples=[
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
                )
            ])

        Mixed usage:
            responses=create_openapi_http_exception_doc([
                status.HTTP_400_BAD_REQUEST,  # Simple status code
                HTTPExceptionDoc(
                    status_code=status.HTTP_404_NOT_FOUND,
                    examples=[
                        {
                            "summary": "Not found",
                            "description": "Resource not found",
                            "value": {"detail": "Resource not found"}
                        }
                    ]
                )
            ])
    """
    result = {}
    
    # Process each element in the list
    for item in responses_status_code:
        if isinstance(item, int):
            # Simple status code without examples
            result[item] = {"model": HTTPExceptionResponse}
        elif isinstance(item, HTTPExceptionDoc):
            # HTTPExceptionDoc with status code and examples
            status_code = item.status_code
            examples = item.examples
            
            if not examples:
                # If no examples provided, use simple format
                result[status_code] = {"model": HTTPExceptionResponse}
            elif len(examples) == 1:
                # Single example: use simple format with description
                example = examples[0]
                description = example["description"] or example["summary"]
                result[status_code] = {
                    "model": HTTPExceptionResponse,
                    "description": description,
                }
            else:
                # Multiple examples: use content with examples
                examples_dict = {}
                for i, example in enumerate(examples):
                    # Generate a key from summary (converted to snake_case)
                    summary = example["summary"]
                    example_key = summary.lower().replace(" ", "_")
                    
                    examples_dict[example_key] = {
                        "summary": example["summary"],
                        "description": example["description"],
                        "value": example["value"],
                    }

                result[status_code] = {
                    "model": HTTPExceptionResponse,
                    "content": {
                        "application/json": {
                            "examples": examples_dict,
                        }
                    },
                }

    # Sort by status code for consistent ordering
    return dict(sorted(result.items()))
