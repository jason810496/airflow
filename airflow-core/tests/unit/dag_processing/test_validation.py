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

from airflow.dag_processing.validation import validate_serialized_dags
from airflow.serialization.serialized_objects import LazyDeserializedDAG


def build_serialized_dag(dag_id: str, tasks: dict[str, list[str]]) -> LazyDeserializedDAG:
    """Build the serialized payload a language SDK would hand core, without going through ``DAG``."""
    return LazyDeserializedDAG(
        data={
            "dag": {
                "dag_id": dag_id,
                "tasks": [
                    {
                        "__type": "operator",
                        "__var": {"task_id": task_id, "downstream_task_ids": downstream_ids},
                    }
                    for task_id, downstream_ids in tasks.items()
                ],
            }
        }
    )


@pytest.mark.parametrize(
    "tasks",
    [
        pytest.param({}, id="no_tasks"),
        pytest.param({"a": []}, id="single_task"),
        pytest.param({"a": ["b"], "b": ["c"], "c": []}, id="linear_chain"),
        pytest.param({"a": ["b", "c"], "b": ["d"], "c": ["d"], "d": []}, id="diamond"),
        pytest.param({"a": ["b"], "b": [], "x": ["y"], "y": []}, id="disconnected_components"),
    ],
)
def test_well_formed_dags_are_accepted(tasks):
    dag = build_serialized_dag("my_dag", tasks)

    valid_dags, errors = validate_serialized_dags([dag])

    assert valid_dags == [dag]
    assert errors == []


@pytest.mark.parametrize(
    ("tasks", "cycle_task_id"),
    [
        pytest.param({"a": ["a"]}, "a", id="self_loop"),
        pytest.param({"a": ["b"], "b": ["a"]}, "b", id="two_task_cycle"),
        pytest.param({"a": ["b"], "b": ["c"], "c": ["a"]}, "c", id="three_task_cycle"),
        pytest.param({"a": ["b"], "b": [], "x": ["y"], "y": ["x"]}, "y", id="cycle_in_second_component"),
        pytest.param({"a": ["b"], "b": ["c"], "c": ["b"]}, "c", id="cycle_below_entry_point"),
    ],
)
def test_cyclic_dags_are_rejected(tasks, cycle_task_id):
    dag = build_serialized_dag("cyclic_dag", tasks)

    valid_dags, errors = validate_serialized_dags([dag])

    assert valid_dags == []
    assert errors == [
        f"Dag 'cyclic_dag' is malformed: a dependency cycle is closed by task {cycle_task_id!r}"
    ]


@pytest.mark.parametrize(
    ("tasks", "referencing_task_id"),
    [
        pytest.param({"a": ["ghost"]}, "a", id="only_task_points_nowhere"),
        pytest.param({"a": ["b"], "b": ["ghost"]}, "b", id="last_task_points_nowhere"),
    ],
)
def test_dangling_downstream_references_are_rejected(tasks, referencing_task_id):
    dag = build_serialized_dag("dangling_dag", tasks)

    valid_dags, errors = validate_serialized_dags([dag])

    assert valid_dags == []
    assert errors == [
        f"Dag 'dangling_dag' is malformed: task {referencing_task_id!r} lists unknown downstream task 'ghost'"
    ]


def test_duplicate_dag_ids_keep_only_the_first():
    first = build_serialized_dag("same_id", {"a": []})
    second = build_serialized_dag("same_id", {"b": []})
    third = build_serialized_dag("same_id", {"c": []})

    valid_dags, errors = validate_serialized_dags([first, second, third])

    assert valid_dags == [first]
    assert errors == [
        "Dag 'same_id' is defined more than once in this file",
        "Dag 'same_id' is defined more than once in this file",
    ]


def test_sound_siblings_survive_a_malformed_dag():
    before = build_serialized_dag("before", {"a": []})
    cyclic = build_serialized_dag("cyclic", {"a": ["b"], "b": ["a"]})
    after = build_serialized_dag("after", {"a": []})

    valid_dags, errors = validate_serialized_dags([before, cyclic, after])

    assert valid_dags == [before, after]
    assert errors == ["Dag 'cyclic' is malformed: a dependency cycle is closed by task 'b'"]


def test_dangling_reference_is_reported_ahead_of_a_cycle():
    """A dangling id must be caught first, otherwise cycle detection would fail to resolve it."""
    dag = build_serialized_dag("both_defects", {"a": ["b", "ghost"], "b": ["a"]})

    valid_dags, errors = validate_serialized_dags([dag])

    assert valid_dags == []
    assert errors == ["Dag 'both_defects' is malformed: task 'a' lists unknown downstream task 'ghost'"]


@pytest.mark.parametrize(
    "dag_data",
    [
        pytest.param({"dag": {"dag_id": "sparse"}}, id="tasks_key_missing"),
        pytest.param({"dag": {"dag_id": "sparse", "tasks": None}}, id="tasks_is_null"),
        pytest.param({"dag": {"dag_id": "sparse", "tasks": [{"__type": "operator"}]}}, id="var_missing"),
        pytest.param(
            {"dag": {"dag_id": "sparse", "tasks": [{"__type": "operator", "__var": {}}]}},
            id="task_id_missing",
        ),
        pytest.param(
            {"dag": {"dag_id": "sparse", "tasks": [{"__type": "operator", "__var": {"task_id": "a"}}]}},
            id="downstream_key_missing",
        ),
        pytest.param(
            {
                "dag": {
                    "dag_id": "sparse",
                    "tasks": [{"__type": "operator", "__var": {"task_id": "a", "downstream_task_ids": None}}],
                }
            },
            id="downstream_is_null",
        ),
    ],
)
def test_sparse_payloads_are_accepted_rather_than_crashing(dag_data):
    dag = LazyDeserializedDAG(data=dag_data)

    valid_dags, errors = validate_serialized_dags([dag])

    assert valid_dags == [dag]
    assert errors == []


def test_no_dags_produces_no_errors():
    assert validate_serialized_dags([]) == ([], [])
