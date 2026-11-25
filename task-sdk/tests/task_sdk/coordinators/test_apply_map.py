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
from __future__ import annotations

from typing import Any

import pytest

from airflow.sdk import DAG, task, task_group
from airflow.sdk.coordinators._apply_map import apply_map, find_map_argument


def to_pair(pair):
    return {"is_tuple": isinstance(pair, tuple), "first": pair[0]}


def double(x):
    return x * 2


@task
def nums():
    return [1, 2]


@task
def letters():
    return ["a", "b"]


def fn_x(x: Any, y: int = 0): ...


@pytest.mark.parametrize(
    ("build", "value", "expected"),
    [
        pytest.param(lambda: task(fn_x).expand(x=nums().map(double).map(str)), 3, "6", id="chained-maps"),
        pytest.param(
            lambda: task(fn_x).expand(x=nums().zip(letters()).map(to_pair)),
            [1, "a"],
            {"is_tuple": True, "first": 1},
            id="zip-element-is-a-tuple",
        ),
        pytest.param(
            lambda: task(fn_x).expand_kwargs(nums().map(lambda n: {"x": (n, n)})),
            2,
            {"x": [2, 2]},
            id="expand-kwargs-as-json",
        ),
    ],
)
def test_apply_map_runs_the_callables_of_a_mapped_stub(build, value, expected):
    with DAG("d"):
        op = build()

    assert apply_map(find_map_argument(op.operator, "x"), value) == expected


def test_find_map_argument_reads_a_mapped_task_groups_expand_argument():
    with DAG("d") as dag:

        @task_group
        def tg(v):
            task(fn_x)(x=v)

        tg.expand(v=nums().map(double))

    assert apply_map(find_map_argument(dag.task_dict["tg.fn_x"], "x"), 4) == 8


@pytest.mark.parametrize("name", ["x", "y"])
def test_find_map_argument_refuses_an_argument_without_map(name):
    with DAG("d"):
        op = task(fn_x).partial(y=1).expand(x=nums())

    with pytest.raises(LookupError):
        find_map_argument(op.operator, name)
