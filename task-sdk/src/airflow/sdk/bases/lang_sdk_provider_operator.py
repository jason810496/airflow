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

from typing import TYPE_CHECKING, ClassVar

from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk.exceptions import AirflowFailException

if TYPE_CHECKING:
    from airflow.sdk.definitions.context import Context


class LangSdkProviderOperator(BaseOperator):
    """
    Base class of a provider operator that is implemented in a Lang SDK, not in Python.

    A provider package generates its classes from the implementation's spec. Such a class only
    describes the operator, so a Python Dag can use it, and has no body to run. Set the task's
    ``queue`` to one that ``[sdk] queue_to_coordinator`` routes to a coordinator of a runtime
    that implements the operator, and that runtime runs the task.

    The task is a stub. The arguments listed in ``operator_params`` are written to the serialized Dag
    as argument bindings, and the Execution API sends them to the runtime with the task's run context.

    :cvar operator_params: Names of the operator's own constructor arguments. Each is stored as an
        instance attribute of the same name.
    :cvar implemented_in: Runtimes that implement the operator, such as ``("TypeScript",)``.
    :cvar implementation_module: Module of the runtime's package that exports the operator under the
        class name, such as ``"@apache-airflow/providers-ts-localfile/operators/file"``. A runtime that
        does not declare the task imports it to run the operator.
    """

    is_stub = True
    operator_params: ClassVar[tuple[str, ...]] = ()
    implemented_in: ClassVar[tuple[str, ...]] = ()
    implementation_module: ClassVar[str | None] = None

    def execute(self, context: Context):
        runtimes = " or ".join(self.implemented_in) or "another language"
        raise AirflowFailException(
            f"{type(self).__name__} is implemented in {runtimes}; set its queue to one that "
            "[sdk] queue_to_coordinator routes to a coordinator of that runtime."
        )
