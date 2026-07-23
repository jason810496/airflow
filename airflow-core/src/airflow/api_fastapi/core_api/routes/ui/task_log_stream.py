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

from fastapi import Depends, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from itsdangerous import BadSignature, URLSafeSerializer
from pydantic import NonNegativeInt
from sqlalchemy import exists, or_
from sqlalchemy.sql import select

from airflow.api_fastapi.common.dagbag import DagBagDep
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import DagAccessEntity, requires_access_dag
from airflow.api_fastapi.core_api.services.ui.task_log_streamer import TaskLogStreamer
from airflow.configuration import conf
from airflow.models import TaskInstance
from airflow.models.taskinstancehistory import TaskInstanceHistory
from airflow.utils.log.log_reader import TaskLogReader

task_log_stream_router = AirflowRouter(
    tags=["Task Log Stream"], prefix="/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
)


@task_log_stream_router.get(
    "/{task_id}/logs/{try_number}/stream",
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag("GET", DagAccessEntity.TASK_LOGS))],
    response_class=StreamingResponse,
)
def stream_task_log(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    try_number: NonNegativeInt,
    request: Request,
    dag_bag: DagBagDep,
    session: SessionDep,
    map_index: int = -1,
    resume_token: str | None = None,
) -> StreamingResponse:
    """
    Stream logs of a task instance try as NDJSON until it reaches a terminal state.

    Log lines are interleaved with control records discriminated by the reserved
    ``_airflow_stream_control`` key: ``resume`` records carry a signed token to
    reconnect without re-reading from the start, ``heartbeat`` records keep idle
    connections alive, and a final ``end_of_stream`` record (with a ``reason`` of
    ``finished``, ``not_found``, or ``superseded``) marks a completed stream. A
    stream that ends with a ``resume`` record hit the server-side duration limit
    and should be resumed with ``resume_token``.
    """
    if not resume_token:
        metadata = {}
    else:
        try:
            metadata = URLSafeSerializer(request.app.state.secret_key).loads(resume_token)
        except BadSignature:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST, "Bad Signature. Please use only the tokens provided by the API."
            )

    task_log_reader = TaskLogReader()
    if not task_log_reader.supports_read:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Task log handler does not support read logs.")

    ti_filter = {
        "task_id": task_id,
        "dag_id": dag_id,
        "run_id": dag_run_id,
        "map_index": map_index,
        "try_number": try_number,
    }
    ti_exists = session.scalar(
        select(
            or_(
                exists().where(*(getattr(TaskInstance, k) == v for k, v in ti_filter.items())),
                exists().where(*(getattr(TaskInstanceHistory, k) == v for k, v in ti_filter.items())),
            )
        )
    )
    if not ti_exists:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "TaskInstance not found")

    streamer = TaskLogStreamer(
        dag_id=dag_id,
        run_id=dag_run_id,
        task_id=task_id,
        map_index=map_index,
        try_number=try_number,
        dag_bag=dag_bag,
        secret_key=request.app.state.secret_key,
        tick_interval=conf.getfloat("api", "log_stream_tick_interval_seconds"),
        max_duration=conf.getfloat("api", "log_stream_max_duration_seconds"),
        batch_size=conf.getint("api", "log_stream_buffer_size"),
        metadata=metadata,
        task_log_reader=task_log_reader,
    )
    return StreamingResponse(media_type="application/x-ndjson", content=streamer.stream())
