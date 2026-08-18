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

from airflow.sdk import dag, task


@task
def python_start():
    return "hello from Python"


@task.stub(queue="typescript")
def build_message(upstream: str, country: str): ...


@task.stub(queue="typescript")
def read_message(): ...


@task.stub(queue="typescript")
def read_connection(): ...


@dag(dag_id="typescript_example", schedule=None, catchup=False, tags=["typescript", "example"])
def typescript_example():
    # The TaskFlow call is what the TypeScript handler receives: `upstream` is
    # pulled from python_start's return value, `country` travels as a literal.
    message = build_message(python_start(), "uk")
    # A call argument carries an upstream task's return value and nothing else,
    # so read_message pulls the custom XCom key for itself.
    message >> read_message()
    read_connection()


typescript_example()
