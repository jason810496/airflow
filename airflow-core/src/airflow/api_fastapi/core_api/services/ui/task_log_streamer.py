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

import asyncio
import contextlib
import json
import time
from itertools import islice
from typing import TYPE_CHECKING, Any

import attrs
from itsdangerous import URLSafeSerializer
from sqlalchemy.orm import joinedload
from sqlalchemy.sql import select
from starlette.concurrency import run_in_threadpool

from airflow.exceptions import TaskNotFound
from airflow.models import TaskInstance, Trigger
from airflow.models.taskinstancehistory import TaskInstanceHistory
from airflow.utils.log.log_reader import TaskLogReader
from airflow.utils.session import create_session
from airflow.utils.state import State

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterator

    from sqlalchemy.orm import Session

    from airflow.api_fastapi.common.dagbag import DBDagBag
    from airflow.utils.log.file_task_handler import StructuredLogMessage

# Control records are told apart from log lines by this reserved key, which log
# lines never carry ("event" cannot discriminate: a log line's text is
# user-controlled and could equal a control marker).
CONTROL_KEY = "_airflow_stream_control"

RESUME_CONTROL = "resume"
HEARTBEAT_CONTROL = "heartbeat"
END_OF_STREAM_CONTROL = "end_of_stream"

END_REASON_FINISHED = "finished"
END_REASON_NOT_FOUND = "not_found"
END_REASON_SUPERSEDED = "superseded"


@attrs.define(kw_only=True)
class _TickHandle:
    stream: Iterator[StructuredLogMessage]
    metadata: dict[str, Any]
    state: str | None
    current_try_number: int


@attrs.define(kw_only=True)
class TaskLogStreamer:
    """
    Tail the logs of one task instance try until it reaches a terminal state.

    Each tick opens its own short-lived session (so a slow client never pins a DB
    connection), re-reads the task instance state, and reads the log delta through
    the task log handler; the handler's ``metadata`` dict carried between ticks is
    the in-connection read position. Control records are interleaved in-band,
    discriminated by ``CONTROL_KEY``: a ``resume`` record carries a signed token
    the client can use to reconnect without re-reading from the start,
    ``heartbeat`` keeps idle connections alive through proxies, and
    ``end_of_stream`` marks a completed stream with the reason (``finished``,
    ``not_found``, or ``superseded`` by a newer try).
    """

    dag_id: str
    run_id: str
    task_id: str
    map_index: int
    try_number: int
    dag_bag: DBDagBag
    secret_key: str
    tick_interval: float
    max_duration: float
    batch_size: int
    metadata: dict[str, Any] = attrs.field(factory=dict)
    task_log_reader: TaskLogReader = attrs.field(factory=TaskLogReader)

    def _fetch_ti(self, session: Session) -> TaskInstance | TaskInstanceHistory | None:
        query = (
            select(TaskInstance)
            .where(
                TaskInstance.task_id == self.task_id,
                TaskInstance.dag_id == self.dag_id,
                TaskInstance.run_id == self.run_id,
                TaskInstance.map_index == self.map_index,
                TaskInstance.try_number == self.try_number,
            )
            .join(TaskInstance.dag_run)
            .options(joinedload(TaskInstance.trigger).joinedload(Trigger.triggerer_job))
            .options(joinedload(TaskInstance.dag_model))
        )
        ti = session.scalar(query)
        if ti is None:
            query = (
                select(TaskInstanceHistory)
                .where(
                    TaskInstanceHistory.task_id == self.task_id,
                    TaskInstanceHistory.dag_id == self.dag_id,
                    TaskInstanceHistory.run_id == self.run_id,
                    TaskInstanceHistory.map_index == self.map_index,
                    TaskInstanceHistory.try_number == self.try_number,
                )
                # FileTaskHandler._render_filename needs ti.dag_run
                .options(joinedload(TaskInstanceHistory.dag_run))
            )
            ti = session.scalar(query)
        return ti

    def _open_tick(self) -> _TickHandle | None:
        """
        Fetch the task instance and start the log read for one tick.

        The session closes before the returned stream is consumed: the handler
        resolves everything session-bound (filename rendering, source discovery)
        inside ``read_log_chunks``, so iterating the stream afterwards only touches
        log sources, never the DB.
        """
        with create_session(scoped=False) as session:
            ti = self._fetch_ti(session)
            if ti is None:
                return None
            dag = self.dag_bag.get_dag_for_run(ti.dag_run, session=session)
            if dag:
                with contextlib.suppress(TaskNotFound):
                    ti.task = dag.get_task(ti.task_id)
            # LogMetadata(TypedDict) is used as type annotation for log_reader
            stream, out_metadata = self.task_log_reader.read_log_chunks(
                ti,
                self.try_number,
                self.metadata,  # type: ignore[arg-type]
            )
            return _TickHandle(
                stream=iter(stream),
                metadata=dict(out_metadata),
                state=ti.state,
                current_try_number=ti.try_number,
            )

    def _drain_batch(self, stream: Iterator[StructuredLogMessage]) -> str:
        lines: list[str] = []
        for message in islice(stream, self.batch_size):
            # a log line must not be able to impersonate a control record
            if message.__pydantic_extra__:
                message.__pydantic_extra__.pop(CONTROL_KEY, None)
            lines.append(f"{message.model_dump_json()}\n")
        return "".join(lines)

    def _serialize_resume_token(self) -> str:
        return URLSafeSerializer(self.secret_key).dumps(self.metadata)

    @staticmethod
    def _control_record(control: str, **extra: Any) -> str:
        return json.dumps({CONTROL_KEY: control, **extra}) + "\n"

    async def stream(self) -> AsyncGenerator[str, None]:
        deadline = time.monotonic() + self.max_duration
        while True:
            tick = await run_in_threadpool(self._open_tick)
            if tick is None:
                yield self._control_record(END_OF_STREAM_CONTROL, state=None, reason=END_REASON_NOT_FOUND)
                return

            emitted_lines = False
            while batch := await run_in_threadpool(self._drain_batch, tick.stream):
                emitted_lines = True
                yield batch

            self.metadata = tick.metadata
            if tick.current_try_number != self.try_number or tick.state not in State.unfinished:
                reason = (
                    END_REASON_SUPERSEDED
                    if tick.current_try_number != self.try_number
                    else END_REASON_FINISHED
                )
                yield self._control_record(END_OF_STREAM_CONTROL, state=tick.state, reason=reason)
                return

            if time.monotonic() >= deadline:
                # the client reconnects with the token to keep tailing
                yield self._control_record(RESUME_CONTROL, token=self._serialize_resume_token())
                return

            if emitted_lines:
                yield self._control_record(RESUME_CONTROL, token=self._serialize_resume_token())
            else:
                yield self._control_record(HEARTBEAT_CONTROL)

            await asyncio.sleep(self.tick_interval)
