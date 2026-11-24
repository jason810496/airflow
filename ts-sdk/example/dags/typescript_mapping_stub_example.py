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
Mapped ``@task.stub`` tasks whose bodies are TypeScript.

The mapping is Python: ``map()`` callables, ``zip()``, ``expand_kwargs()`` and a mapped task group.
Airflow's Python sidecar resolves each instance's arguments, and runs the ``map()`` callables when
a handler's runtime asks, so the handlers in ``src/mapping-stubs.ts`` only see finished values.
"""

from __future__ import annotations

from airflow.sdk import dag, task, task_group


def strip(value):
    return value.strip()


def to_kwargs(value):
    return {"path": value.strip()}


@task
def rows():
    return [" uk ", " fr "]


@task
def lookup():
    return [1, 2]


@task.stub(queue="typescript")
def transform(region: str, row: str): ...


@task.stub(queue="typescript")
def report(totals: list): ...


@task.stub(queue="typescript")
def pair(item: list): ...


@task.stub(queue="typescript")
def load(path: str, bucket: str = "raw"): ...


@task.stub(queue="typescript")
def report_regions(values: list): ...


@task_group
def per_region(region):
    @task.stub(queue="typescript")
    def first(region: str): ...

    @task.stub(queue="typescript")
    def second(previous: str, region: str): ...

    return second(first(region), region)


@dag(catchup=False, tags=["typescript", "mapping"])
def typescript_mapping_stub_example():
    regions = rows()
    report(transform.partial(region="eu").expand(row=regions.map(strip)))
    pair.expand(item=regions.zip(lookup()))
    load.expand_kwargs(regions.map(to_kwargs))
    report_regions(per_region.expand(region=["north", "south"]))


typescript_mapping_stub_example()
