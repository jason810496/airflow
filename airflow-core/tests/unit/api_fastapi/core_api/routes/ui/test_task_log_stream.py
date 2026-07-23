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

import json
from unittest import mock

import pytest
from itsdangerous import URLSafeSerializer

from airflow._shared.timezones import timezone
from airflow.api_fastapi.app import create_dag_bag
from airflow.api_fastapi.common.dagbag import dag_bag_from_app
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.log.file_task_handler import StructuredLogMessage
from airflow.utils.session import create_session
from airflow.utils.state import TaskInstanceState
from airflow.utils.types import DagRunType

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_runs
from tests_common.test_utils.file_task_handler import convert_list_to_stream

pytestmark = [pytest.mark.db_test, pytest.mark.need_serialized_dag]

CONTROL_KEY = "_airflow_stream_control"

FAST_STREAM_CONF = {
    ("api", "log_stream_tick_interval_seconds"): "0.01",
    ("api", "log_stream_max_duration_seconds"): "30",
}


def _messages(*events: str) -> list[StructuredLogMessage]:
    return [StructuredLogMessage(event=event) for event in events]


def _events(records: list[dict]) -> list[str]:
    return [record["event"] for record in records if CONTROL_KEY not in record]


def _controls(records: list[dict]) -> list[dict]:
    return [record for record in records if CONTROL_KEY in record]


class TestStreamTaskLog:
    DAG_ID = "dag_for_testing_log_stream_endpoint"
    RUN_ID = "run_for_testing_log_stream_endpoint"
    TASK_ID = "task_for_testing_log_stream_endpoint"
    ENDPOINT = f"/dags/{DAG_ID}/dagRuns/{RUN_ID}/taskInstances/{TASK_ID}/logs/1/stream"

    default_time = "2020-06-10T20:00:00+00:00"

    @pytest.fixture(autouse=True)
    def setup_attrs(self, test_client, configure_loggers, dag_maker, session) -> None:
        self.app = test_client.app
        self.client = test_client

        with dag_maker(self.DAG_ID, start_date=timezone.parse(self.default_time), session=session):
            EmptyOperator(task_id=self.TASK_ID)

        dr = dag_maker.create_dagrun(
            run_id=self.RUN_ID,
            run_type=DagRunType.SCHEDULED,
            logical_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
        )
        self.ti = dr.task_instances[0]
        self.ti.try_number = 1
        self.ti.hostname = "localhost"
        session.merge(self.ti)
        session.commit()

        dagbag = create_dag_bag()
        test_client.app.dependency_overrides[dag_bag_from_app] = lambda: dagbag

    @pytest.fixture
    def configure_loggers(self, tmp_path, create_log_template):
        self.log_dir = tmp_path
        dir_path = tmp_path / f"dag_id={self.DAG_ID}" / f"run_id={self.RUN_ID}" / f"task_id={self.TASK_ID}"
        dir_path.mkdir(parents=True)
        (dir_path / "attempt=1.log").write_text("Log for testing.")

        with mock.patch(
            "airflow.utils.log.file_task_handler.FileTaskHandler.local_base",
            new_callable=mock.PropertyMock,
            create=True,
        ) as local_base:
            local_base.return_value = self.log_dir
            yield

    def teardown_method(self):
        clear_db_runs()

    def _set_ti_state(self, state: TaskInstanceState | None) -> None:
        with create_session() as session:
            ti = session.merge(self.ti)
            ti.state = state

    def _get_stream_records(self, resume_token: str | None = None) -> list[dict]:
        params = {"resume_token": resume_token} if resume_token else None
        response = self.client.get(self.ENDPOINT, params=params)
        assert response.status_code == 200, response.content.decode()
        return [json.loads(line) for line in response.content.decode().splitlines()]

    def test_terminal_task_streams_log_and_ends(self):
        self._set_ti_state(TaskInstanceState.SUCCESS)

        records = self._get_stream_records()

        assert "Log for testing." in _events(records)
        assert records[-1] == {CONTROL_KEY: "end_of_stream", "state": "success", "reason": "finished"}
        assert all(control[CONTROL_KEY] == "end_of_stream" for control in _controls(records))

    @conf_vars(FAST_STREAM_CONF)
    @mock.patch("airflow.utils.log.file_task_handler.FileTaskHandler.read")
    def test_running_task_ticks_until_terminal(self, mock_read):
        self._set_ti_state(TaskInstanceState.RUNNING)

        def read_ticks(ti, try_number, metadata=None):
            if not mock_read.call_count > 1:
                # the state flip lands after this tick's TI fetch, so the next tick
                # observes SUCCESS and performs the final read
                self._set_ti_state(TaskInstanceState.SUCCESS)
                return convert_list_to_stream(_messages("line 1", "line 2")), {
                    "end_of_log": False,
                    "log_pos": 2,
                }
            return convert_list_to_stream(_messages("line 3")), {"end_of_log": True}

        mock_read.side_effect = read_ticks

        records = self._get_stream_records()

        assert _events(records) == ["line 1", "line 2", "line 3"]
        resume = _controls(records)[0]
        assert resume[CONTROL_KEY] == "resume"
        token = URLSafeSerializer(self.app.state.secret_key).loads(resume["token"])
        assert token == {"end_of_log": False, "log_pos": 2}
        assert records[-1] == {CONTROL_KEY: "end_of_stream", "state": "success", "reason": "finished"}

    @pytest.mark.parametrize(
        "pending_state",
        [
            None,
            TaskInstanceState.QUEUED,
            TaskInstanceState.SCHEDULED,
            TaskInstanceState.UP_FOR_RETRY,
            TaskInstanceState.RESTARTING,
        ],
    )
    @conf_vars(FAST_STREAM_CONF)
    @mock.patch("airflow.utils.log.file_task_handler.FileTaskHandler.read")
    def test_unfinished_non_running_state_keeps_streaming(self, mock_read, pending_state):
        """A queued/scheduled/retry-pending task has not finished: the stream must
        heartbeat and keep tailing rather than ending prematurely."""
        self._set_ti_state(pending_state)

        def read_ticks(ti, try_number, metadata=None):
            if not mock_read.call_count > 1:
                self._set_ti_state(TaskInstanceState.SUCCESS)
                return convert_list_to_stream([]), {"end_of_log": False, "log_pos": 0}
            return convert_list_to_stream(_messages("first line")), {"end_of_log": True}

        mock_read.side_effect = read_ticks

        records = self._get_stream_records()

        assert _controls(records)[0][CONTROL_KEY] == "heartbeat"
        assert _events(records) == ["first line"]
        assert records[-1] == {CONTROL_KEY: "end_of_stream", "state": "success", "reason": "finished"}

    @conf_vars(FAST_STREAM_CONF)
    @mock.patch("airflow.utils.log.file_task_handler.FileTaskHandler.read")
    def test_log_line_cannot_impersonate_control_record(self, mock_read):
        self._set_ti_state(TaskInstanceState.SUCCESS)
        spoofed = StructuredLogMessage.model_validate(
            {"event": "innocent looking line", CONTROL_KEY: "end_of_stream"}
        )
        mock_read.return_value = (convert_list_to_stream([spoofed]), {"end_of_log": True})

        records = self._get_stream_records()

        assert _events(records) == ["innocent looking line"]
        line = next(record for record in records if record.get("event") == "innocent looking line")
        assert CONTROL_KEY not in line

    @conf_vars(
        {
            ("api", "log_stream_tick_interval_seconds"): "0.01",
            ("api", "log_stream_max_duration_seconds"): "0",
        }
    )
    @mock.patch("airflow.utils.log.file_task_handler.FileTaskHandler.read")
    def test_max_duration_ends_stream_with_resume_record(self, mock_read):
        self._set_ti_state(TaskInstanceState.RUNNING)
        mock_read.return_value = (
            convert_list_to_stream(_messages("line 1")),
            {"end_of_log": False, "log_pos": 1},
        )

        records = self._get_stream_records()

        assert _events(records) == ["line 1"]
        assert records[-1][CONTROL_KEY] == "resume"
        token = URLSafeSerializer(self.app.state.secret_key).loads(records[-1]["token"])
        assert token == {"end_of_log": False, "log_pos": 1}
        assert mock_read.call_count == 1

    @conf_vars({("api", "log_stream_buffer_size"): "2"})
    @mock.patch("airflow.utils.log.file_task_handler.FileTaskHandler.read")
    def test_lines_are_flushed_in_bounded_batches(self, mock_read):
        self._set_ti_state(TaskInstanceState.SUCCESS)
        mock_read.return_value = (
            convert_list_to_stream(_messages("line 1", "line 2", "line 3", "line 4", "line 5")),
            {"end_of_log": True},
        )

        response = self.client.get(self.ENDPOINT)
        assert response.status_code == 200

        records = [json.loads(line) for line in response.content.decode().splitlines()]
        assert _events(records) == ["line 1", "line 2", "line 3", "line 4", "line 5"]

    @mock.patch("airflow.utils.log.file_task_handler.FileTaskHandler.read")
    def test_resume_token_seeds_read_metadata(self, mock_read):
        self._set_ti_state(TaskInstanceState.SUCCESS)
        mock_read.return_value = (
            convert_list_to_stream(_messages("line 3")),
            {"end_of_log": True},
        )
        resume_token = URLSafeSerializer(self.app.state.secret_key).dumps({"end_of_log": False, "log_pos": 2})

        records = self._get_stream_records(resume_token=resume_token)

        assert mock_read.call_args.kwargs["metadata"] == {"end_of_log": False, "log_pos": 2}
        assert _events(records) == ["line 3"]
        assert records[-1][CONTROL_KEY] == "end_of_stream"

    def test_bad_resume_token_returns_400(self):
        response = self.client.get(self.ENDPOINT, params={"resume_token": "not-a-signed-token"})
        assert response.status_code == 400

    def test_task_instance_not_found_returns_404(self):
        response = self.client.get(
            f"/dags/{self.DAG_ID}/dagRuns/{self.RUN_ID}/taskInstances/no-such-task/logs/1/stream"
        )
        assert response.status_code == 404
