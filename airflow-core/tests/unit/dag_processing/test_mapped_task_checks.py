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

import pytest

from airflow.dag_processing.mapped_task_checks import check_mapped_tasks
from airflow.sdk import DAG, task, task_group
from airflow.serialization.enums import Encoding
from airflow.serialization.serialized_objects import DagSerialization


def inc(x):
    return x + 1


@task
def nums():
    return [1, 2]


@task
def letters():
    return ["a", "b"]


@task
def show(x, y=None):
    return x


def _check(dag: DAG) -> None:
    data = DagSerialization.to_dict(dag)
    check_mapped_tasks(data, DagSerialization.from_dict(data))


def test_every_deliverable_shape_passes():
    with DAG("d") as dag:
        n = nums()
        show.partial(y=n).expand(x=n.zip(letters()).map(inc))
        show.override(task_id="concat").expand(x=n.concat(letters()), y=["a"])
        show.override(task_id="kwargs").expand_kwargs([{"x": 1}, {"x": 2, "y": [3]}])
        show.override(task_id="kwargs_upstream").expand_kwargs(n.map(inc))

    _check(dag)


@pytest.mark.parametrize(
    ("build", "error"),
    [
        pytest.param(
            lambda n, s: show.expand(x=n.map(inc).zip(s)), "map\\(\\) inside a zip\\(\\)", id="map-in-zip"
        ),
        pytest.param(
            lambda n, s: show.expand(x=n.map(inc).concat(s)),
            "map\\(\\) inside a zip\\(\\) or concat",
            id="map-in-concat",
        ),
        pytest.param(
            lambda n, s: show.expand(x=n.zip(s.zip(n))), "zip\\(\\) inside a zip\\(\\)", id="zip-in-zip"
        ),
        pytest.param(
            lambda n, s: show.partial(y=n.map(inc)).expand(x=s),
            "'y' receives a whole map",
            id="whole-partial",
        ),
        pytest.param(
            lambda n, s: show.expand_kwargs([n.zip(s)]), "item receives a whole map", id="whole-kwargs-item"
        ),
        pytest.param(
            lambda n, s: show.expand_kwargs([show.override(task_id="m").expand(x=s)]),
            "whole output of the mapped task 'm', a list rather than a dict",
            id="mapped-kwargs-item",
        ),
        pytest.param(
            lambda n, s: show.expand_kwargs([{"x": n}]),
            "literal dict cannot hold one",
            id="ref-in-literal-dict",
        ),
    ],
)
def test_undeliverable_shape_is_rejected(build, error):
    with DAG("d") as dag:
        build(nums(), letters())

    with pytest.raises(ValueError, match=error):
        _check(dag)


def test_mapped_task_group_expand_input_is_checked():
    with DAG("d") as dag:

        @task_group
        def tg(v):
            show(v)

        tg.expand(v=nums().map(inc).zip(letters()))

    with pytest.raises(ValueError, match="Mapped task group 'tg' expand argument 'v' has a map"):
        _check(dag)


def test_reading_a_task_mapped_again_inside_a_shared_group_is_rejected():
    with DAG("d") as dag:

        @task_group
        def outer(v):
            @task_group
            def inner(w):
                return show.override(task_id="producer")(w)

            show.override(task_id="consumer")(inner.expand(w=[v]))

        outer.expand(v=[1, 2])

    data = DagSerialization.to_dict(dag)
    for encoded in data["dag"]["tasks"]:
        if encoded[Encoding.VAR]["task_id"] == "outer.consumer":
            encoded[Encoding.VAR]["_arg_bindings"] = [
                {"name": "x", "kind": "sequence", "task_id": "outer.inner.producer"}
            ]

    with pytest.raises(ValueError, match="'outer.inner.producer', which is mapped again"):
        check_mapped_tasks(data, DagSerialization.from_dict(data))
