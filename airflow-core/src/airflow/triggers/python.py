# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use it except in compliance
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

"""Trigger that runs a Python callable serialized with dill."""

from __future__ import annotations

import base64
import inspect
from collections.abc import AsyncIterator
from typing import Any

import dill

from airflow.triggers.base import BaseTrigger, TriggerEvent

BASE_PYTHON_TRIGGER_CLASSPATH = "airflow.triggers.python.BasePythonTrigger"


def _validate_trigger_callable(fn: Any) -> None:
    """Validate that the callable accepts at least one parameter (self: BaseTrigger)."""
    try:
        sig = inspect.signature(fn)
    except (ValueError, TypeError):
        raise ValueError(
            f"callable must have inspectable signature, got {type(fn).__name__}"
        ) from None
    params = list(sig.parameters)
    if len(params) < 1:
        raise ValueError(
            f"callable must accept at least one parameter (self: BaseTrigger), "
            f"got {len(params)} parameters: {fn!r}"
        )


class BasePythonTrigger(BaseTrigger):
    """
    Trigger that serializes a Python callable with dill.

    Unlike classpath-based triggers, BasePythonTrigger can run callables defined
    in DAG code (bundle-aware). The callable must be an async generator that
    yields TriggerEvent instances and accept at least one parameter (self)
    for the trigger instance.

    The triggerer must run with bundle context when deserializing so that
    imports in the callable resolve correctly.
    """

    def __init__(self, *, callable: Any = None, callable_b64: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self._callable = callable
        self.callable_b64 = callable_b64
        if callable is not None:
            _validate_trigger_callable(callable)

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Return classpath and kwargs with callable serialized as base64-encoded dill bytes."""
        if self.callable_b64 is not None:
            return (BASE_PYTHON_TRIGGER_CLASSPATH, {"callable_b64": self.callable_b64})
        callable_b64 = base64.b64encode(dill.dumps(self._callable)).decode("ascii")
        return (BASE_PYTHON_TRIGGER_CLASSPATH, {"callable_b64": callable_b64})

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Deserialize the callable and run it as an async generator."""
        if self._callable is not None:
            fn = self._callable
        elif self.callable_b64 is not None:
            callable_bytes = base64.b64decode(self.callable_b64.encode("ascii"))
            fn = dill.loads(callable_bytes)
            _validate_trigger_callable(fn)
        else:
            raise ValueError("callable_b64 not set; trigger was not properly deserialized")
        async for event in fn(self):
            yield event
