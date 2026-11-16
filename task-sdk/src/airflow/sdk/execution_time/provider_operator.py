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
Execution API sends them to the worker with the Dag's edges. See
``airflow-core/adr/lang-sdk/0009-provider-operators-as-generated-dsl.md``.

The other tasks of the Dag become placeholders that keep the task ids and the edges, which is
what an operator reads of its Dag: the tasks it skips, and the upstream tasks whose results its
arguments take.
"""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any

from airflow.sdk._shared.module_loading import import_string
from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk.definitions.dag import DAG
from airflow.sdk.definitions.xcom_arg import XComArg

if TYPE_CHECKING:
    from airflow.sdk.api.datamodels._generated import TIProviderOperatorResponse


class ProviderOperatorError(ValueError):
    """The provider operator cannot be built."""


class _PlaceholderTask(BaseOperator):
    """Stands in for a task of the Dag that this worker does not run."""

    def execute(self, context):
        raise RuntimeError(f"Task {self.task_id!r} is a placeholder and does not run here")


def decode_value(value: Any, dag: DAG) -> Any:
    """
    Decode one value the way ``BaseSerialization.deserialize`` does, for the types a language SDK writes.

    A reference to another task's result becomes the :class:`XComArg` of that task in *dag*.
    """
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, list):
        return [decode_value(item, dag) for item in value]
    if not isinstance(value, dict) or "__type" not in value or "__var" not in value:
        raise ProviderOperatorError(f"Cannot decode {value!r}: it is not a type-encoded value")
    type_, var = value["__type"], value["__var"]
    if type_ == "dict":
        return {key: decode_value(item, dag) for key, item in var.items()}
    if type_ == "timedelta":
        return datetime.timedelta(seconds=var)
    if type_ == "datetime":
        return datetime.datetime.fromtimestamp(var, tz=datetime.UTC)
    if type_ == "set":
        return {decode_value(item, dag) for item in var}
    if type_ == "tuple":
        return tuple(decode_value(item, dag) for item in var)
    if type_ == "xcomref":
        try:
            upstream = dag.task_dict[var["task_id"]]
        except KeyError:
            raise ProviderOperatorError(f"An argument takes the result of unknown task {var['task_id']!r}")
        return XComArg(upstream, key=var["key"])
    raise ProviderOperatorError(f"Cannot decode a value of type {type_!r}")


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
    operator: TIProviderOperatorResponse, *, dag_id: str, task_id: str, fileloc: str
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
        kwargs = {name: decode_value(value, dag) for name, value in operator.kwargs.items()}
        task = operator_class(task_id=task_id, **kwargs)
    for upstream_id, downstream_ids in operator.downstream_task_ids.items():
        for downstream_id in downstream_ids:
            dag.task_dict[upstream_id].set_downstream(dag.task_dict[downstream_id])
    return task
