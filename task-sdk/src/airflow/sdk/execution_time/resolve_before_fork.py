#
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
"""
Experiment: resolve a mapped task instance's expand input in the supervisor, before the fork.

Set ``AIRFLOW_EXP_RESOLVE_BEFORE_FORK=1`` to enable it. The supervisor then:

1. starts the task instance on the API server, which gives it the run context;
2. imports the Dag file with ``task_runner.parse``, as the task process does, and resolves the expand
   input(s) of the task with the same ``ExpandInput.resolve`` code, reading XComs through its own client;
3. forks the task process, and sends the resolved values in ``StartupDetails``.

The task process installs them on its ``RuntimeTaskInstance``, and ``ExpandInput.resolve`` returns them
instead of resolving again. Anything that goes wrong in step 2 leaves the field empty, and the task
process resolves for itself exactly as it does without the flag.

The values travel as a pickle, which keeps Python types such as the tuples that ``zip()`` and
``.map()`` give. It is only meant for this experiment: the supervisor and the task process trust each other.
"""

from __future__ import annotations

import base64
import os
import pickle
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import attrs
import psutil
import structlog

from airflow.sdk.definitions._internal.expandinput import MappedArgument
from airflow.sdk.definitions._internal.mixins import ResolveMixin
from airflow.sdk.definitions.mappedoperator import MappedOperator
from airflow.sdk.execution_time.lazy_sequence import LazyXComSequence

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger as Logger

    from airflow.sdk.api.client import Client
    from airflow.sdk.api.datamodels._generated import BundleInfo, TaskInstance, TIRunContext
    from airflow.sdk.definitions._internal.expandinput import ExpandInput
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

ENV_FLAG = "AIRFLOW_EXP_RESOLVE_BEFORE_FORK"

log = structlog.get_logger(logger_name=__name__)


@attrs.define
class PreStart:
    """What the supervisor learned before forking the task process."""

    start_pid: int = attrs.field(factory=os.getpid)
    """The pid the server recorded for the task instance: the supervisor's, as the task process is not forked yet."""
    ti_context: TIRunContext | None = None
    start_error: Exception | None = None
    """The failure of ``task_instances.start``, for ``_on_child_started`` to handle as it always does."""
    encoded: str | None = None
    """The resolved expand inputs for ``StartupDetails``, or ``None`` when the task process resolves."""


def is_enabled(ti: TaskInstance) -> bool:
    """Whether to resolve before the fork: the flag is set and the task instance is a mapped one."""
    return os.environ.get(ENV_FLAG) == "1" and ti.map_index is not None and ti.map_index >= 0


def get_expand_inputs(task: Any) -> list[ExpandInput]:
    """
    Return the expand inputs *task* resolves at run time, in an order both processes agree on.

    A mapped operator has its own. A task in a mapped task group reads the group's through the
    ``MappedArgument`` it gets, and only if it takes one.
    """
    inputs: list[ExpandInput] = []
    if isinstance(task, MappedOperator):
        inputs.append(task._get_specified_expand_input())
    for group in task.iter_mapped_task_groups():
        expand_input = group._expand_input
        if all(expand_input is not seen for seen in inputs) and any(
            _takes_mapped_argument(getattr(task, field, None), expand_input) for field in task.template_fields
        ):
            inputs.append(expand_input)
    return inputs


def _takes_mapped_argument(value: Any, expand_input: ExpandInput) -> bool:
    """Whether *value* is, or holds, a ``MappedArgument`` of *expand_input*."""
    if isinstance(value, MappedArgument):
        return value._input is expand_input
    if isinstance(value, Mapping):
        return any(_takes_mapped_argument(v, expand_input) for v in value.values())
    if isinstance(value, (list, tuple, set, frozenset)):
        return any(_takes_mapped_argument(v, expand_input) for v in value)
    return False


def _has_unpicklable_reference(value: Any) -> bool:
    """Whether *value* still points at the Dag or the task instance, which would drag them into a pickle."""
    if isinstance(value, (ResolveMixin, LazyXComSequence)):
        return True
    if isinstance(value, Mapping):
        return any(_has_unpicklable_reference(v) for v in value.values())
    if isinstance(value, (list, tuple, set, frozenset)):
        return any(_has_unpicklable_reference(v) for v in value)
    return False


def encode(payloads: list[Mapping[str, Any] | None]) -> str:
    return base64.b64encode(pickle.dumps(payloads, protocol=pickle.HIGHEST_PROTOCOL)).decode("ascii")


def decode(encoded: str) -> list[Mapping[str, Any] | None]:
    return pickle.loads(base64.b64decode(encoded))


def resolve_expand_inputs(
    *,
    ti: TaskInstance,
    ti_context: TIRunContext,
    dag_rel_path: str | os.PathLike[str],
    bundle_info: BundleInfo,
    sentry_integration: str,
    client: Client,
) -> str | None:
    """
    Import the Dag file and resolve the expand inputs of *ti*'s task, as the task process would.

    XCom reads and the like go through *client* by way of the same in-process comms ``dag.test()`` uses.
    """
    from airflow.sdk.definitions._internal.dag_parsing_context import _airflow_parsing_context_manager
    from airflow.sdk.execution_time import context as sdk_context, task_runner
    from airflow.sdk.execution_time.comms import StartupDetails
    from airflow.sdk.execution_time.supervisor import (
        InProcessSupervisorComms,
        InProcessTestSupervisor,
        set_supervisor_comms,
    )

    supervisor = InProcessTestSupervisor(
        id=ti.id,
        pid=os.getpid(),
        process=psutil.Process(),
        process_log=log,
        client=client,
    )
    supervisor.comms = InProcessSupervisorComms(supervisor=supervisor)
    previous_team_name = sdk_context._current_team_name
    try:
        # startup() does the same in the task process, for the lookups a .map() callable makes.
        sdk_context.set_current_team_name(ti_context.dag_run.team_name)
        with set_supervisor_comms(supervisor.comms):
            msg = StartupDetails.model_construct(
                ti=ti,
                dag_rel_path=os.fspath(dag_rel_path),
                bundle_info=bundle_info,
                ti_context=ti_context,
                start_date=ti_context.start_date or datetime.now(tz=UTC),
                sentry_integration=sentry_integration,
            )
            with _airflow_parsing_context_manager(dag_id=ti.dag_id, task_id=ti.task_id):
                runtime_ti = task_runner.parse(msg, log)
            return _resolve_all(runtime_ti)
    finally:
        sdk_context.set_current_team_name(previous_team_name)
        # InProcessTestSupervisor opens an epoll selector, which a forked task process must not inherit.
        supervisor.selector.close()


def _resolve_all(runtime_ti: RuntimeTaskInstance) -> str | None:
    expand_inputs = get_expand_inputs(runtime_ti.task)
    if not expand_inputs:
        return None
    context = runtime_ti.get_template_context()
    payloads: list[Mapping[str, Any] | None] = []
    for expand_input in expand_inputs:
        resolved, _ = expand_input.resolve(context)
        payloads.append(None if _has_unpicklable_reference(resolved) else dict(resolved))
    if all(payload is None for payload in payloads):
        return None
    return encode(payloads)


def prestart(
    *,
    ti: TaskInstance,
    dag_rel_path: str | os.PathLike[str],
    bundle_info: BundleInfo,
    sentry_integration: str,
    client: Client,
) -> PreStart:
    """
    Start the task instance and resolve its expand inputs, all before the task process exists.

    The server records the supervisor's pid rather than the task process's, which does not exist yet, so the
    supervisor must send that pid with its heartbeats.
    """
    pre = PreStart()
    try:
        pre.ti_context = client.task_instances.start(ti.id, pre.start_pid, datetime.now(tz=UTC))
    except Exception as e:
        pre.start_error = e
        return pre
    try:
        pre.encoded = resolve_expand_inputs(
            ti=ti,
            ti_context=pre.ti_context,
            dag_rel_path=dag_rel_path,
            bundle_info=bundle_info,
            sentry_integration=sentry_integration,
            client=client,
        )
    except (Exception, SystemExit):
        # parse() exits when the Dag or the task is missing; the task process reports that properly.
        log.warning("Could not resolve expand inputs before the fork", ti_id=str(ti.id), exc_info=True)
    return pre


def install(runtime_ti: RuntimeTaskInstance, encoded: str | None, task_log: Logger) -> None:
    """Make the task process use the values the supervisor resolved, if it sent any."""
    if not encoded:
        return
    payloads = decode(encoded)
    expand_inputs = get_expand_inputs(runtime_ti.task)
    if len(payloads) != len(expand_inputs):
        task_log.warning("Ignoring pre-resolved expand inputs that do not match the Dag", count=len(payloads))
        return
    runtime_ti._pre_resolved_expand_inputs = {
        id(expand_input): payload
        for expand_input, payload in zip(expand_inputs, payloads)
        if payload is not None
    }
    task_log.info("pre_resolved_expand_inputs installed", count=len(runtime_ti._pre_resolved_expand_inputs))
