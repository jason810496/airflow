#
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
from __future__ import annotations

import pytest

from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.triggers.python import BASE_PYTHON_TRIGGER_CLASSPATH, BasePythonTrigger
from airflow.sdk import trigger


class TestTriggerDecorator:
    def test_trigger_bare_decorator_returns_base_python_trigger(self):
        """Test @trigger returns BasePythonTrigger instance."""

        @trigger
        async def my_trigger(self: BaseTrigger):
            yield TriggerEvent({"done": True})

        assert isinstance(my_trigger, BasePythonTrigger)
        classpath, kwargs = my_trigger.serialize()
        assert classpath == BASE_PYTHON_TRIGGER_CLASSPATH
        assert "callable_b64" in kwargs

    def test_trigger_factory_decorator_returns_base_python_trigger(self):
        """Test @trigger() returns decorator that produces BasePythonTrigger."""

        @trigger()
        async def my_trigger(self: BaseTrigger):
            yield TriggerEvent({"step": 1})

        assert isinstance(my_trigger, BasePythonTrigger)
        classpath, kwargs = my_trigger.serialize()
        assert classpath == BASE_PYTHON_TRIGGER_CLASSPATH

    @pytest.mark.asyncio
    async def test_trigger_decorator_run_yields_events(self):
        """Test decorated trigger yields events when run."""

        @trigger
        async def my_trigger(self: BaseTrigger):
            yield TriggerEvent({"payload": "test"})

        events = []
        async for event in my_trigger.run():
            events.append(event)

        assert len(events) == 1
        assert events[0].payload == {"payload": "test"}
