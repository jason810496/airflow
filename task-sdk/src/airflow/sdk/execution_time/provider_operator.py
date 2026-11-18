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
Build a provider operator that a language SDK Dag declares.

A Python worker gets a task by importing its Dag file. A Dag written with another language SDK
has no file Python can import, so for a task that a Python operator runs, the SDK writes the
operator's class and constructor arguments (``_operator_kwargs``) into the serialized Dag, and the
Execution API sends them to the worker with the Dag's edges, in ``TIRunContext.provider_operator``.
See ``airflow-core/adr/lang-sdk/0009-provider-operators-as-generated-dsl.md``.

The other tasks of the Dag become placeholders that keep the task ids and the edges, which is
what an operator reads of its Dag: the tasks it skips, and the upstream tasks whose results its
arguments take.
"""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any

from airflow.sdk._shared.module_loading import import_string
from airflow.sdk.api.datamodels._generated import LiteralArgBinding, XComArgBinding
from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk.bases.xcom import BaseXCom
from airflow.sdk.definitions.dag import DAG
from airflow.sdk.definitions.xcom_arg import XComArg

if TYPE_CHECKING:
    from collections.abc import Callable

    from airflow.sdk.api.datamodels._generated import TaskArgBinding, TIProviderOperator


class ProviderOperatorError(ValueError):
    """The provider operator cannot be built."""


class _PlaceholderTask(BaseOperator):
    """Stands in for a task of the Dag that this worker does not run."""

    def execute(self, context):
        raise RuntimeError(f"Task {self.task_id!r} is a placeholder and does not run here")


def _parse_date_time(value: Any) -> datetime.datetime:
    moment = datetime.datetime.fromisoformat(value)
    return moment if moment.tzinfo else moment.replace(tzinfo=datetime.UTC)


# The ``format`` of a ``value_schema`` that a language SDK sets for a value JSON has no type for.
_LITERAL_CONVERTERS: dict[str, Callable[[Any], Any]] = {
    "date-time": _parse_date_time,
    "duration-seconds": lambda seconds: datetime.timedelta(seconds=seconds),
}


def _convert_literal(binding: LiteralArgBinding) -> Any:
    value = binding.value
    value_format = binding.value_schema.root.get("format") if binding.value_schema else None
    if value is None or value_format not in _LITERAL_CONVERTERS:
        return value
    try:
        return _LITERAL_CONVERTERS[value_format](value)
    except (TypeError, ValueError) as exc:
        raise ProviderOperatorError(
            f"Argument {binding.name!r} is not a valid {value_format}: {value!r}"
        ) from exc


def resolve_binding(binding: TaskArgBinding, dag: DAG) -> Any:
    """
    Resolve one constructor argument the way the Dag declares it.

    A literal is its value, as the Python type its ``value_schema`` names. A reference to another
    task's result becomes the :class:`XComArg` of that task in *dag*.
    """
    match binding.root:
        case LiteralArgBinding() as literal:
            return _convert_literal(literal)
        case XComArgBinding(task_id=task_id):
            try:
                upstream = dag.task_dict[task_id]
            except KeyError:
                raise ProviderOperatorError(f"An argument takes the result of unknown task {task_id!r}")
            return XComArg(upstream, key=BaseXCom.XCOM_RETURN_KEY)


def _find_operator_class(classpath: str) -> type[BaseOperator]:
    try:
        operator_class = import_string(classpath)
    except ImportError as exc:
        raise ProviderOperatorError(
            f"Cannot import operator {classpath}; is the provider that has it installed on this worker?"
        ) from exc
    # Only an operator class is built: the serialized Dag names a class, not arbitrary code.
    if not isinstance(operator_class, type) or not issubclass(operator_class, BaseOperator):
        raise ProviderOperatorError(f"{classpath} is not an operator class")
    return operator_class


def build_provider_operator(
    operator: TIProviderOperator, *, dag_id: str, task_id: str, fileloc: str
) -> BaseOperator:
    """
    Build task *task_id* from the provider *operator*, with its Dag around it.

    :raises ProviderOperatorError: when the operator class cannot be built.
    """
    operator_class = _find_operator_class(operator.classpath)

    dag = DAG(dag_id, schedule=None)
    dag.fileloc = fileloc
    with dag:
        # The placeholders come first, so that an argument can refer to any of them.
        for other_id in operator.downstream_task_ids:
            if other_id != task_id:
                _PlaceholderTask(task_id=other_id)
        kwargs = {binding.root.name: resolve_binding(binding, dag) for binding in operator.kwargs}
        task = operator_class(task_id=task_id, **kwargs)
    for upstream_id, downstream_ids in operator.downstream_task_ids.items():
        for downstream_id in downstream_ids:
            dag.task_dict[upstream_id].set_downstream(dag.task_dict[downstream_id])
    return task
