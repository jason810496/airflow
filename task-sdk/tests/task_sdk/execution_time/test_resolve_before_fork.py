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
"""Tests of the experiment that resolves a mapped task instance's expand input before the fork."""

from __future__ import annotations

import os
from unittest import mock
from uuid import UUID

import pytest
import structlog
from uuid6 import uuid7

from airflow.sdk import DAG, task, task_group, timezone
from airflow.sdk.api import client as sdk_client
from airflow.sdk.api.datamodels._generated import BundleInfo, TaskInstance, TaskInstanceState, XComResponse
from airflow.sdk.bases.xcom import BaseXCom
from airflow.sdk.execution_time import context as sdk_context, resolve_before_fork, task_runner
from airflow.sdk.execution_time.comms import GetXCom, StartupDetails, XComResult
from airflow.sdk.execution_time.supervisor import ActivitySubprocess, WatchedSubprocess
from airflow.sdk.execution_time.task_runner import run, startup

TI_ID = UUID("01a21aaf-5e22-7481-b657-7bea6b1a0f77")
FAKE_BUNDLE = BundleInfo(name="my-bundle", version=None)


@pytest.fixture(autouse=True)
def clear_bound_ti_identifiers():
    # startup() binds TI identifiers to the logs of the whole process.
    yield
    structlog.contextvars.clear_contextvars()


@pytest.fixture
def client_with_ti_start(make_ti_context):
    client = mock.MagicMock(spec=sdk_client.Client)
    client.task_instances.start.return_value = make_ti_context()
    return client


def get_group_expand_input(dag: DAG):
    (group,) = [g for g in dag.task_group.get_task_group_dict().values() if hasattr(g, "_expand_input")]
    return group._expand_input


def build_ti(map_index: int = 0) -> TaskInstance:
    return TaskInstance(
        id=TI_ID,
        task_id="consume",
        dag_id="d",
        run_id="r",
        try_number=1,
        map_index=map_index,
        dag_version_id=uuid7(),
        queue="default",
    )


def send_xcom(value):
    """Answer every XCom read with *value*, as the supervisor would for the producing task."""

    def _send(msg=None, *args, **kwargs):
        assert isinstance(msg, GetXCom)
        return XComResult(key="return_value", value=BaseXCom.serialize_value(value))

    return _send


class TestGetExpandInputs:
    def test_mapped_operator_has_its_own_expand_input(self):
        with DAG("d"):

            @task
            def make():
                return [1, 2]

            @task
            def consume(x):
                return x

            mapped = consume.expand(x=make()).operator

        assert resolve_before_fork.get_expand_inputs(mapped) == [mapped.op_kwargs_expand_input]

    def test_task_in_mapped_group_reads_the_group_expand_input(self):
        with DAG("d") as dag:

            @task
            def make():
                return [1, 2]

            @task
            def step(x):
                return x

            @task_group
            def group(x):
                step(x)

            group.expand(x=make())

        assert resolve_before_fork.get_expand_inputs(dag.get_task("group.step")) == [
            get_group_expand_input(dag)
        ]

    def test_task_in_mapped_group_that_takes_no_mapped_argument_has_none(self):
        with DAG("d") as dag:

            @task
            def make():
                return [1, 2]

            @task
            def step(x):
                return x

            @task
            def after(previous):
                return previous

            @task_group
            def group(x):
                after(step(x))

            group.expand(x=make())

        assert resolve_before_fork.get_expand_inputs(dag.get_task("group.after")) == []

    def test_unmapped_task_has_none(self):
        with DAG("d") as dag:

            @task
            def make():
                return [1, 2]

            make()

        assert resolve_before_fork.get_expand_inputs(dag.get_task("make")) == []


class TestIsEnabled:
    @pytest.mark.parametrize(
        ("env", "map_index", "expected"),
        [
            pytest.param("1", 0, True, id="mapped"),
            pytest.param("1", 3, True, id="mapped-later-index"),
            pytest.param("1", -1, False, id="unmapped"),
            pytest.param("1", None, False, id="no-map-index"),
            pytest.param(None, 0, False, id="flag-unset"),
            pytest.param("0", 0, False, id="flag-off"),
        ],
    )
    def test_is_enabled(self, monkeypatch, env, map_index, expected):
        if env is None:
            monkeypatch.delenv(resolve_before_fork.ENV_FLAG, raising=False)
        else:
            monkeypatch.setenv(resolve_before_fork.ENV_FLAG, env)

        assert resolve_before_fork.is_enabled(build_ti(map_index)) is expected


def test_encode_keeps_python_types():
    payloads = [{"pair": (1, "a"), "n": 2}, None]

    decoded = resolve_before_fork.decode(resolve_before_fork.encode(payloads))

    assert decoded == payloads
    assert isinstance(decoded[0]["pair"], tuple)


class TestTaskProcessUsesHandedOverValues:
    def test_dict_of_lists_returns_them_without_reading_xcom(self, create_runtime_ti, mock_supervisor_comms):
        with DAG("d"):

            @task
            def make():
                return [1, 2, 3]

            @task
            def consume(x, z, y=0):
                return x

            op = consume.partial(y=1).expand(x=make(), z=["a", "b"]).operator
        expand_input = op._get_specified_expand_input()
        runtime_ti = create_runtime_ti(op, map_index=1)
        handed_over = {"x": 20, "z": "b"}
        runtime_ti._pre_resolved_expand_inputs = {id(expand_input): handed_over}

        data, resolved_oids = expand_input.resolve(runtime_ti.get_template_context())

        assert data is handed_over
        # The literal "z" is not an XCom value, so it is still rendered as a template.
        assert resolved_oids == {id(handed_over["x"])}
        mock_supervisor_comms.send.assert_not_called()

    def test_list_of_dicts_returns_them_without_reading_xcom(self, create_runtime_ti, mock_supervisor_comms):
        with DAG("d"):

            @task
            def make_kwargs():
                return [{"a": 1}, {"a": 2}]

            @task
            def consume(a):
                return a

            op = consume.expand_kwargs(make_kwargs()).operator
        expand_input = op._get_specified_expand_input()
        runtime_ti = create_runtime_ti(op, map_index=1)
        handed_over = {"a": 2}
        runtime_ti._pre_resolved_expand_inputs = {id(expand_input): handed_over}

        data, _ = expand_input.resolve(runtime_ti.get_template_context())

        assert data is handed_over
        mock_supervisor_comms.send.assert_not_called()

    def test_task_runs_with_them(self, create_runtime_ti, mock_supervisor_comms):
        seen = []
        with DAG("d"):

            @task
            def make():
                return [1, 2, 3]

            @task
            def consume(x):
                seen.append(x)

            op = consume.expand(x=make()).operator
        runtime_ti = create_runtime_ti(op, map_index=2)
        runtime_ti._pre_resolved_expand_inputs = {id(op._get_specified_expand_input()): {"x": 30}}

        state, _, error = run(runtime_ti, context=runtime_ti.get_template_context(), log=mock.MagicMock())

        assert (state, error) == (TaskInstanceState.SUCCESS, None)
        assert seen == [30]
        sent = [c.args[0] if c.args else c.kwargs["msg"] for c in mock_supervisor_comms.send.call_args_list]
        assert not [m for m in sent if isinstance(m, GetXCom)]

    def test_install_matches_values_to_expand_inputs_in_order(self, create_runtime_ti):
        with DAG("d") as dag:

            @task
            def make():
                return [1, 2]

            @task
            def step(x):
                return x

            @task_group
            def group(x):
                step(x)

            group.expand(x=make())
        group_expand_input = get_group_expand_input(dag)
        runtime_ti = create_runtime_ti(dag.get_task("group.step"), map_index=1)
        encoded = resolve_before_fork.encode([{"x": 2}])

        resolve_before_fork.install(runtime_ti, encoded, mock.MagicMock())

        assert runtime_ti._pre_resolved_expand_inputs == {id(group_expand_input): {"x": 2}}

    def test_install_skips_expand_inputs_the_supervisor_left_to_the_task_process(self, create_runtime_ti):
        with DAG("d"):

            @task
            def make():
                return [1, 2]

            @task
            def consume(x):
                return x

            op = consume.expand(x=make()).operator
        runtime_ti = create_runtime_ti(op, map_index=0)

        resolve_before_fork.install(runtime_ti, resolve_before_fork.encode([None]), mock.MagicMock())

        assert runtime_ti._pre_resolved_expand_inputs == {}

    def test_install_ignores_values_that_do_not_match_the_dag(self, create_runtime_ti):
        with DAG("d"):

            @task
            def make():
                return [1, 2]

            @task
            def consume(x):
                return x

            op = consume.expand(x=make()).operator
        runtime_ti = create_runtime_ti(op, map_index=0)
        task_log = mock.MagicMock()

        resolve_before_fork.install(runtime_ti, resolve_before_fork.encode([{"x": 1}, {"y": 2}]), task_log)

        assert runtime_ti._pre_resolved_expand_inputs is None
        task_log.warning.assert_called_once()

    def test_install_does_nothing_without_values(self, create_runtime_ti):
        with DAG("d"):

            @task
            def consume(x):
                return x

            op = consume.expand(x=[1, 2]).operator
        runtime_ti = create_runtime_ti(op, map_index=0)

        resolve_before_fork.install(runtime_ti, None, mock.MagicMock())

        assert runtime_ti._pre_resolved_expand_inputs is None

    def test_startup_installs_the_values_of_startup_details(self, mocked_parse, make_ti_context):
        with DAG("d"):

            @task
            def consume(x):
                return x

            op = consume.expand(x=[1, 2]).operator
        what = StartupDetails(
            ti=build_ti(map_index=1),
            dag_rel_path="",
            bundle_info=FAKE_BUNDLE,
            ti_context=make_ti_context(),
            start_date=timezone.utcnow(),
            sentry_integration="",
            pre_resolved_expand_inputs=resolve_before_fork.encode([{"x": 2}]),
        )
        mocked_parse(what, "d", op)

        runtime_ti, _, _ = startup(what)

        assert runtime_ti._pre_resolved_expand_inputs == {id(op._get_specified_expand_input()): {"x": 2}}


class TestResolveInSupervisor:
    def test_resolves_the_expand_input_of_the_map_index(self, create_runtime_ti, mock_supervisor_comms):
        with DAG("d"):

            @task
            def make():
                return [10, 20, 30]

            @task
            def consume(x, z):
                return x

            op = consume.expand(x=make(), z=["a"]).operator
        runtime_ti = create_runtime_ti(op, map_index=1)
        mock_supervisor_comms.send.side_effect = send_xcom([10, 20, 30])

        encoded = resolve_before_fork._resolve_all(runtime_ti)

        assert resolve_before_fork.decode(encoded) == [{"x": 20, "z": "a"}]

    def test_keeps_python_types_that_map_gives(self, create_runtime_ti, mock_supervisor_comms):
        def pair(value):
            return (value, value * 2)

        with DAG("d"):

            @task
            def make():
                return [1, 2]

            @task
            def consume(pair):
                return pair

            op = consume.expand(pair=make().map(pair)).operator
        runtime_ti = create_runtime_ti(op, map_index=1)
        mock_supervisor_comms.send.side_effect = send_xcom([1, 2])

        (payload,) = resolve_before_fork.decode(resolve_before_fork._resolve_all(runtime_ti))

        assert payload == {"pair": (2, 4)}
        assert isinstance(payload["pair"], tuple)

    def test_leaves_values_that_still_need_resolving_to_the_task_process(
        self, create_runtime_ti, mock_supervisor_comms
    ):
        with DAG("d"):

            @task
            def make():
                return 1

            @task
            def consume(a):
                return a

            op = consume.expand_kwargs([{"a": make()}]).operator
        runtime_ti = create_runtime_ti(op, map_index=0)

        assert resolve_before_fork._resolve_all(runtime_ti) is None

    def test_has_nothing_to_resolve_for_an_unmapped_task(self, create_runtime_ti):
        with DAG("d"):

            @task
            def make():
                return 1

            op = make().operator
        runtime_ti = create_runtime_ti(op)

        assert resolve_before_fork._resolve_all(runtime_ti) is None


class TestResolveExpandInputs:
    def test_reads_xcoms_through_the_supervisor_client(
        self, mocker, create_runtime_ti, client_with_ti_start, make_ti_context
    ):
        with DAG("d"):

            @task
            def make():
                return [10, 20, 30]

            @task
            def consume(x):
                return x

            op = consume.expand(x=make()).operator
        runtime_ti = create_runtime_ti(op, map_index=2)
        team_names = []

        def parse(what, log):
            team_names.append(sdk_context._current_team_name)
            return runtime_ti

        mock_parse = mocker.patch.object(task_runner, "parse", autospec=True, side_effect=parse)
        ti_context = make_ti_context()
        ti_context.dag_run.team_name = "team_a"
        client_with_ti_start.xcoms.get.return_value = XComResponse(
            key="return_value", value=BaseXCom.serialize_value([10, 20, 30])
        )
        comms_before = getattr(task_runner, "SUPERVISOR_COMMS", None)
        team_name_before = sdk_context._current_team_name

        encoded = resolve_before_fork.resolve_expand_inputs(
            ti=build_ti(map_index=2),
            ti_context=ti_context,
            dag_rel_path="d.py",
            bundle_info=FAKE_BUNDLE,
            sentry_integration="",
            client=client_with_ti_start,
        )

        assert resolve_before_fork.decode(encoded) == [{"x": 30}]
        client_with_ti_start.xcoms.get.assert_called_once()
        assert mock_parse.call_args.args[0].ti_context is ti_context
        assert team_names == ["team_a"]
        # The task process must not inherit the supervisor's stand-ins.
        assert getattr(task_runner, "SUPERVISOR_COMMS", None) is comms_before
        assert sdk_context._current_team_name == team_name_before


class TestPrestart:
    @staticmethod
    def run_prestart(client):
        return resolve_before_fork.prestart(
            ti=build_ti(),
            dag_rel_path="d.py",
            bundle_info=FAKE_BUNDLE,
            sentry_integration="",
            client=client,
        )

    @mock.patch.object(resolve_before_fork, "resolve_expand_inputs", autospec=True, return_value="encoded")
    def test_starts_with_the_supervisor_pid_then_resolves(self, mock_resolve, client_with_ti_start):
        pre = self.run_prestart(client_with_ti_start)

        client_with_ti_start.task_instances.start.assert_called_once_with(TI_ID, os.getpid(), mock.ANY)
        assert pre.start_pid == os.getpid()
        assert pre.ti_context is client_with_ti_start.task_instances.start.return_value
        assert pre.encoded == "encoded"
        assert mock_resolve.call_args.kwargs["ti_context"] is pre.ti_context

    @mock.patch.object(resolve_before_fork, "resolve_expand_inputs", autospec=True)
    def test_keeps_a_failed_start_for_the_supervisor_to_handle(self, mock_resolve, client_with_ti_start):
        error = RuntimeError("start rejected")
        client_with_ti_start.task_instances.start.side_effect = error

        pre = self.run_prestart(client_with_ti_start)

        assert pre.start_error is error
        assert pre.ti_context is None
        mock_resolve.assert_not_called()

    @pytest.mark.parametrize("failure", [RuntimeError("boom"), SystemExit(1)])
    @mock.patch.object(resolve_before_fork, "resolve_expand_inputs", autospec=True)
    def test_falls_back_to_the_task_process_when_resolving_fails(
        self, mock_resolve, failure, client_with_ti_start
    ):
        mock_resolve.side_effect = failure

        pre = self.run_prestart(client_with_ti_start)

        assert pre.encoded is None
        assert pre.start_error is None
        assert pre.ti_context is client_with_ti_start.task_instances.start.return_value


class TestSupervisor:
    @staticmethod
    def build_proc(mocker, client, prestart=None):
        proc = ActivitySubprocess(
            process_log=mocker.MagicMock(),
            id=TI_ID,
            pid=4321,
            stdin=mocker.Mock(),
            process=mocker.Mock(),
            client=client,
        )
        proc._exp_prestart = prestart
        return proc

    @staticmethod
    def start_child(proc):
        proc._on_child_started(
            ti=build_ti(), dag_rel_path="d.py", bundle_info=FAKE_BUNDLE, sentry_integration=""
        )

    def test_sends_the_resolved_values_without_starting_again(self, mocker, client_with_ti_start):
        mock_send = mocker.patch.object(ActivitySubprocess, "send_msg", autospec=True)
        pre = resolve_before_fork.PreStart(ti_context=client_with_ti_start.task_instances.start.return_value)
        pre.encoded = "encoded"
        proc = self.build_proc(mocker, client_with_ti_start, pre)

        self.start_child(proc)

        client_with_ti_start.task_instances.start.assert_not_called()
        assert mock_send.call_args.args[1].pre_resolved_expand_inputs == "encoded"

    def test_sends_no_values_without_the_experiment(self, mocker, client_with_ti_start):
        mock_send = mocker.patch.object(ActivitySubprocess, "send_msg", autospec=True)
        proc = self.build_proc(mocker, client_with_ti_start)

        self.start_child(proc)

        client_with_ti_start.task_instances.start.assert_called_once_with(TI_ID, 4321, mock.ANY)
        assert mock_send.call_args.args[1].pre_resolved_expand_inputs is None

    def test_a_failed_pre_fork_start_is_handled_as_a_failed_start(self, mocker, client_with_ti_start):
        mock_kill = mocker.patch.object(ActivitySubprocess, "kill", autospec=True)
        pre = resolve_before_fork.PreStart(start_error=RuntimeError("start rejected"))
        proc = self.build_proc(mocker, client_with_ti_start, pre)

        with pytest.raises(RuntimeError, match="start rejected"):
            self.start_child(proc)

        mock_kill.assert_called_once()

    @pytest.mark.parametrize(
        ("prestart", "expected_pid"),
        [
            pytest.param(None, 4321, id="task-process-pid"),
            pytest.param(resolve_before_fork.PreStart(start_pid=999), 999, id="pid-recorded-at-start"),
        ],
    )
    def test_heartbeat_sends_the_pid_the_server_recorded(
        self, mocker, client_with_ti_start, prestart, expected_pid
    ):
        proc = self.build_proc(mocker, client_with_ti_start, prestart)
        proc._process.pid = 4321

        proc._send_heartbeat_if_needed()

        client_with_ti_start.task_instances.heartbeat.assert_called_once_with(TI_ID, pid=expected_pid)

    @pytest.mark.parametrize(
        ("env", "map_index", "pre_resolves"),
        [
            pytest.param("1", 0, True, id="flag-and-mapped"),
            pytest.param("1", -1, False, id="unmapped"),
            pytest.param(None, 0, False, id="flag-unset"),
        ],
    )
    def test_start_resolves_before_forking(
        self, mocker, monkeypatch, client_with_ti_start, env, map_index, pre_resolves
    ):
        if env is None:
            monkeypatch.delenv(resolve_before_fork.ENV_FLAG, raising=False)
        else:
            monkeypatch.setenv(resolve_before_fork.ENV_FLAG, env)
        calls = mocker.Mock()
        pre = resolve_before_fork.PreStart(ti_context=client_with_ti_start.task_instances.start.return_value)
        mock_prestart = mocker.patch.object(resolve_before_fork, "prestart", autospec=True, return_value=pre)
        calls.attach_mock(mock_prestart, "prestart")
        proc = self.build_proc(mocker, client_with_ti_start)

        def fork(*args, **kwargs):
            calls.fork()
            return proc

        mocker.patch.object(WatchedSubprocess, "start", side_effect=fork)
        mocker.patch("airflow.sdk.execution_time.supervisor._import_task_runner_before_fork")
        mocker.patch("airflow.sdk.execution_time.supervisor.task_process_uses_exec", return_value=False)
        mocker.patch.object(ActivitySubprocess, "_on_child_started", autospec=True)

        ActivitySubprocess.start(
            what=build_ti(map_index),
            dag_rel_path="d.py",
            bundle_info=FAKE_BUNDLE,
            client=client_with_ti_start,
        )

        if pre_resolves:
            assert [c[0] for c in calls.mock_calls] == ["prestart", "fork"]
            assert proc._exp_prestart is pre
        else:
            mock_prestart.assert_not_called()
            assert proc._exp_prestart is None
