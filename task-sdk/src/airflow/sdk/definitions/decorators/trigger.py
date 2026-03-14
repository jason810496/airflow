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

"""Decorator factory for creating BasePythonTrigger instances."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, overload

from airflow.triggers.base import BaseTrigger
from airflow.triggers.python import BasePythonTrigger

__all__ = ["trigger"]


@overload
def trigger(python_callable: Callable[..., Any]) -> BasePythonTrigger: ...


@overload
def trigger(python_callable: None = None, **kwargs: Any) -> Callable[[Callable[..., Any]], BasePythonTrigger]: ...


def trigger(
    python_callable: Callable[..., Any] | None = None,
    **kwargs: Any,
) -> BasePythonTrigger | Callable[[Callable[..., Any]], BasePythonTrigger]:
    """
    Decorator factory that creates a BasePythonTrigger from an async generator.

    The callable must accept at least one parameter (self: BaseTrigger) for the
    trigger instance. Use with DeferrableOperators::

        @trigger
        async def my_trigger(self: BaseTrigger):
            await asyncio.sleep(5)
            yield TriggerEvent({"done": True})

        # In operator:
        self.defer(trigger=my_trigger)

    Supports both ``@trigger`` and ``@trigger(**kwargs)`` forms.
    """
    if python_callable is not None:
        return BasePythonTrigger(callable=python_callable, **kwargs)

    def decorator(fn: Callable[..., Any]) -> BaseTrigger:
        return BasePythonTrigger(callable=fn, **kwargs)

    return decorator
