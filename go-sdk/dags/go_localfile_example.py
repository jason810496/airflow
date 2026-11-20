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
"""Python Dags that use the Go-implemented ``go.localfile`` provider, routed with ``queue``."""

from __future__ import annotations

from airflow.providers.go.localfile.operators.file import (
    CreateFileOperator,
    DeleteFileOperator,
    HasFileOperator,
    WriteFileOperator,
    WriteVariableOperator,
)
from airflow.sdk import dag, task

QUEUE = "golang"
REPORT_PATH = "/tmp/airflow-localfile/python_localfile/{{ run_id }}/report.txt"
VARIABLE_PATH = "/tmp/airflow-localfile/python_localfile/{{ run_id }}/variable.txt"
# The Airflow Variable that write_variable reads. The e2e test sets it before it triggers the Dag.
VARIABLE_KEY = "go_localfile_variable"


@task
def build_content() -> str:
    return "localfile report\nnaïve café, 3 orders\n"


@task
def after_skip() -> str:
    return "unreachable"


@dag(dag_id="python_localfile", schedule=None, catchup=False, tags=["go", "go.localfile"])
def python_localfile():
    content = build_content()
    create = CreateFileOperator(task_id="create", path=REPORT_PATH, queue=QUEUE)
    write = WriteFileOperator(task_id="write", path=REPORT_PATH, content=content, queue=QUEUE)
    has_file = HasFileOperator(task_id="has_file", path=REPORT_PATH, queue=QUEUE)
    delete = DeleteFileOperator(task_id="delete", path=REPORT_PATH, queue=QUEUE)
    has_after_delete = HasFileOperator(
        task_id="has_after_delete", path=REPORT_PATH, skip_downstream_if_missing=True, queue=QUEUE
    )
    # A root task: the operator reads the Variable through the Go SDK client, which calls back to Airflow.
    WriteVariableOperator(task_id="write_variable", path=VARIABLE_PATH, key=VARIABLE_KEY, queue=QUEUE)

    content >> create >> write >> has_file >> delete >> has_after_delete >> after_skip()


python_localfile()
