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

import logging

from fastapi import HTTPException, status

from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.execution_api import deps
from airflow.api_fastapi.execution_api.datamodels.token import TIToken
from airflow.api_fastapi.execution_api.datamodels.variable import VariablePostBody, VariableResponse
from airflow.models.variable import Variable

# TODO: Add dependency on JWT token
router = AirflowRouter(
    responses={status.HTTP_404_NOT_FOUND: {"description": "Variable not found"}},
)

log = logging.getLogger(__name__)


@router.get(
    "/{variable_key}",
    responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Unauthorized"},
        status.HTTP_403_FORBIDDEN: {"description": "Task does not have access to the variable"},
    },
)
def get_variable(variable_key: str, token: deps.TokenDep) -> VariableResponse:
    """Get an Airflow Variable."""
    if not has_variable_access(variable_key, token):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "reason": "access_denied",
                "message": f"Task does not have access to variable {variable_key}",
            },
        )

    try:
        variable_value = Variable.get(variable_key)
    except KeyError:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": f"Variable with key '{variable_key}' not found",
            },
        )

    return VariableResponse(key=variable_key, value=variable_value)


@router.put(
    "/{variable_key}",
    status_code=status.HTTP_201_CREATED,
    responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Unauthorized"},
        status.HTTP_403_FORBIDDEN: {"description": "Task does not have access to the variable"},
    },
)
def put_variable(variable_key: str, body: VariablePostBody, token: deps.TokenDep):
    """Set an Airflow Variable."""
    if not has_variable_access(variable_key, token, write_access=True):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "reason": "access_denied",
                "message": f"Task does not have access to write variable {variable_key}",
            },
        )
    Variable.set(key=variable_key, value=body.value, description=body.description)
    return {"message": "Variable successfully set"}


def has_variable_access(variable_key: str, token: TIToken, write_access: bool = False) -> bool:
    """Check if the task has access to the variable."""
    # TODO: Placeholder for actual implementation

    ti_key = token.ti_key
    log.debug(
        "Checking access for task instance with key '%s' to variable '%s'",
        ti_key,
        variable_key,
    )
    return True
