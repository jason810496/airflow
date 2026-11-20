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

import pytest

from airflow.providers.java.localfile.operators.file import (
    CreateFileOperator,
    DeleteFileOperator,
    HasFileOperator,
    WriteFileOperator,
    WriteVariableOperator,
)
from airflow.sdk import DAG, LangSdkProviderOperator
from airflow.sdk.exceptions import AirflowFailException
from airflow.serialization.enums import Encoding
from airflow.serialization.serialized_objects import DagSerialization

OPERATORS = [
    CreateFileOperator,
    WriteFileOperator,
    HasFileOperator,
    DeleteFileOperator,
    WriteVariableOperator,
]


@pytest.mark.parametrize("operator_class", OPERATORS)
def test_operators_are_implemented_in_java(operator_class):
    assert issubclass(operator_class, LangSdkProviderOperator)
    assert operator_class.implemented_in == ("Java",)
    assert operator_class.implementation_module == (
        f"java-localfile/org.apache.airflow.providers.localfile.{operator_class.__name__}"
    )


@pytest.mark.parametrize(
    ("operator_class", "kwargs"),
    [
        (CreateFileOperator, {"path": "/tmp/a"}),
        (WriteFileOperator, {"path": "/tmp/a", "content": "x"}),
        (HasFileOperator, {"path": "/tmp/a"}),
        (DeleteFileOperator, {"path": "/tmp/a"}),
        (WriteVariableOperator, {"path": "/tmp/a", "key": "k"}),
    ],
)
def test_a_python_worker_fails_the_task_and_names_the_queue(operator_class, kwargs):
    task = operator_class(task_id="t", **kwargs)

    with pytest.raises(AirflowFailException) as exc_info:
        task.execute({})

    assert str(exc_info.value) == (
        f"{operator_class.__name__} is implemented in Java; set its queue to one that "
        "[sdk] queue_to_coordinator routes to a coordinator of that runtime."
    )


def test_template_fields():
    assert CreateFileOperator.template_fields == ("path",)
    assert WriteFileOperator.template_fields == ("path", "content")
    assert HasFileOperator.template_fields == ("path",)
    assert DeleteFileOperator.template_fields == ("path",)
    assert WriteVariableOperator.template_fields == ("path", "key")


def test_write_file_operator_serializes_its_arguments_as_bindings():
    with DAG(dag_id="write_report", schedule=None) as dag:
        WriteFileOperator(
            task_id="write",
            path="/tmp/{{ run_id }}.txt",
            content="done",
            mode="append",
            queue="java",
        )

    ser_dag = DagSerialization.to_dict(dag)
    DagSerialization.validate_schema(ser_dag)

    (task,) = (t[Encoding.VAR] for t in ser_dag["dag"]["tasks"])
    assert task["task_type"] == "WriteFileOperator"
    assert task["_task_module"] == "airflow.providers.java.localfile.operators.file"
    assert task["queue"] == "java"
    assert task["is_stub"] is True
    assert (
        task["_operator_module"] == "java-localfile/org.apache.airflow.providers.localfile.WriteFileOperator"
    )
    assert task["_operator_kwargs"] == [
        {"name": "path", "kind": "literal", "value": "/tmp/{{ run_id }}.txt"},
        {"name": "content", "kind": "literal", "value": "done"},
        {"name": "mode", "kind": "literal", "value": "append"},
        {"name": "encoding", "kind": "literal", "value": "utf-8"},
    ]


def test_has_file_operator_serializes_its_defaults():
    with DAG(dag_id="check_report", schedule=None) as dag:
        HasFileOperator(task_id="check", path="/tmp/a.txt", skip_downstream_if_missing=True)

    (task,) = (t[Encoding.VAR] for t in DagSerialization.to_dict(dag)["dag"]["tasks"])
    assert task["_operator_kwargs"] == [
        {"name": "path", "kind": "literal", "value": "/tmp/a.txt"},
        {"name": "skip_downstream_if_missing", "kind": "literal", "value": True},
    ]
