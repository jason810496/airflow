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

from airflow.sdk import LangSdkProviderOperator
from airflow.sdk.exceptions import AirflowFailException


class WriteFileOperator(LangSdkProviderOperator):
    operator_params = ("path",)
    implemented_in = ("TypeScript",)

    def __init__(self, *, path: str, **kwargs):
        super().__init__(**kwargs)
        self.path = path


def test_is_a_stub_that_has_no_implementation_module_by_default():
    task = WriteFileOperator(task_id="write", path="/tmp/a")

    assert task.is_stub is True
    assert task.implementation_module is None


@pytest.mark.parametrize(
    ("implemented_in", "runtimes"),
    [(("TypeScript",), "TypeScript"), (("TypeScript", "Go"), "TypeScript or Go")],
)
def test_execute_fails_without_retrying_and_names_the_runtimes(implemented_in, runtimes):
    operator_class = type("WriteFileOperator", (WriteFileOperator,), {"implemented_in": implemented_in})
    task = operator_class(task_id="write", path="/tmp/a")

    with pytest.raises(AirflowFailException) as exc_info:
        task.execute({})

    assert str(exc_info.value) == (
        f"WriteFileOperator is implemented in {runtimes}; set its queue to one that "
        "[sdk] queue_to_coordinator routes to a coordinator of that runtime."
    )
