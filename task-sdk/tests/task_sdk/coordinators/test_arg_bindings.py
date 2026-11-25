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

import re

import pytest

from airflow.sdk.api.datamodels._generated import TaskArgBinding
from airflow.sdk.coordinators._arg_bindings import ArgBindingError, has_upstream_values, read_arg_bindings


def _bindings(*entries: dict) -> list[TaskArgBinding]:
    return [TaskArgBinding.model_validate({"name": "row", **entry}) for entry in entries]


@pytest.mark.parametrize(
    ("entry", "expected"),
    [
        pytest.param({"kind": "literal", "value": 1}, False, id="literal"),
        pytest.param({"kind": "sequence", "task_id": "t"}, False, id="sequence"),
        pytest.param({"kind": "xcom", "task_id": "t"}, True, id="xcom"),
        pytest.param({"kind": "tuple", "parts": [{"kind": "literal", "value": 1}]}, True, id="tuple"),
    ],
)
def test_has_upstream_values(entry, expected):
    assert has_upstream_values(_bindings(entry)) is expected


def test_read_keeps_bindings_the_runtime_reads_itself():
    bindings = _bindings(
        {"kind": "literal", "value": 2, "from_default": True}, {"kind": "sequence", "task_id": "t"}
    )

    assert read_arg_bindings(bindings, read=pytest.fail) == bindings


def test_read_narrows_a_mapped_upstream_row():
    rows = {("t", 2): "c"}
    (binding,) = read_arg_bindings(
        _bindings({"kind": "xcom", "task_id": "t", "map_index": 2}),
        lambda ref: rows[ref.task_id, ref.map_index],
    )
    assert binding.root.model_dump(exclude_unset=True) == {"name": "row", "kind": "literal", "value": "c"}


@pytest.mark.parametrize(
    ("value", "entry", "message"),
    [
        pytest.param([1], {"element_index": 1}, "Task 't' returned 1 elements, not 2", id="out-of-range"),
        pytest.param(
            5, {"element_index": 0}, "Task 't' returned int, which has no elements", id="not-indexable"
        ),
        pytest.param(
            [1],
            {"element_index": 0, "kwarg": "x"},
            "expand_kwargs() expects a list of dicts",
            id="not-a-dict",
        ),
    ],
)
def test_read_refuses_a_value_with_no_element_for_this_index(value, entry, message):
    with pytest.raises(ArgBindingError, match=re.escape(message)):
        read_arg_bindings(_bindings({"kind": "xcom", "task_id": "t", **entry}), lambda ref: value)
