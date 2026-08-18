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
TaskFlow argument binding across the language boundary.

``typescript_example`` shows the basics; this Dag exists to exercise binding
itself. Each stub takes several arguments of mixed kinds so the two TypeScript
authoring styles have something worth showing: ``summarize`` is annotated flat,
``report`` declares one interface. See ``src/taskflow.ts``.
"""

from __future__ import annotations

from airflow.sdk import dag, task


@task
def make_region():
    return {"code": "uk", "name": "United Kingdom"}


@task
def make_totals():
    return {"orders": 12, "revenue": 3400.5}


# Two XCom-backed arguments, one literal, and one the call leaves at its default
# so the binding arrives flagged `from_default`.
@task.stub(queue="typescript")
def summarize(region: dict, totals: dict, currency: str, dry_run: bool = False): ...


# One XCom-backed argument and four literals. `region_code` is snake_case on
# purpose: bound arguments keep their Python names, and the TypeScript side
# renames while destructuring.
@task.stub(queue="typescript")
def report(summary: dict, region_code: str, threshold: float, retries_used: int, label: str): ...


@dag(
    dag_id="typescript_taskflow_example",
    schedule=None,
    catchup=False,
    tags=["typescript", "example", "taskflow"],
)
def typescript_taskflow_example():
    summary = summarize(make_region(), make_totals(), "GBP")
    report(summary, "uk", 0.75, 3, "nightly")


typescript_taskflow_example()
