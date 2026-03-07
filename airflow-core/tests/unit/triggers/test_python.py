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

from airflow.triggers.base import TriggerEvent
from airflow.triggers.python import BASE_PYTHON_TRIGGER_CLASSPATH, BasePythonTrigger


async def _simple_trigger():
    """Simple async generator that yields one event."""
    yield TriggerEvent({"done": True})


async def _multi_event_trigger():
    """Async generator that yields multiple events."""
    yield TriggerEvent({"step": 1})
    yield TriggerEvent({"step": 2})
    yield TriggerEvent({"step": 3})


class TestBasePythonTrigger:
    def test_serialize_with_callable(self):
        """Test serialize returns classpath and base64-encoded callable."""
        trigger = BasePythonTrigger(callable=_simple_trigger)
        classpath, kwargs = trigger.serialize()

        assert classpath == BASE_PYTHON_TRIGGER_CLASSPATH
        assert "callable_b64" in kwargs
        assert isinstance(kwargs["callable_b64"], str)

    def test_serialize_deserialize_round_trip(self):
        """Test that serialize/deserialize round-trip reconstructs a working trigger."""
        original = BasePythonTrigger(callable=_simple_trigger)
        classpath, kwargs = original.serialize()

        # Simulate triggerer deserialization
        trigger = BasePythonTrigger(callable_b64=kwargs["callable_b64"])

        assert trigger.callable_b64 == kwargs["callable_b64"]

    @pytest.mark.asyncio
    async def test_run_yields_events(self):
        """Test run() yields events from the callable."""
        trigger = BasePythonTrigger(callable=_simple_trigger)
        classpath, kwargs = trigger.serialize()
        deserialized = BasePythonTrigger(callable_b64=kwargs["callable_b64"])

        events = []
        async for event in deserialized.run():
            events.append(event)

        assert len(events) == 1
        assert events[0].payload == {"done": True}

    @pytest.mark.asyncio
    async def test_run_yields_multiple_events(self):
        """Test run() yields all events from multiple-yield callable."""
        trigger = BasePythonTrigger(callable=_multi_event_trigger)
        classpath, kwargs = trigger.serialize()
        deserialized = BasePythonTrigger(callable_b64=kwargs["callable_b64"])

        events = []
        async for event in deserialized.run():
            events.append(event)

        assert len(events) == 3
        assert events[0].payload == {"step": 1}
        assert events[1].payload == {"step": 2}
        assert events[2].payload == {"step": 3}

    @pytest.mark.asyncio
    async def test_run_raises_when_callable_b64_not_set(self):
        """Test run() raises when callable_b64 was not provided during deserialization."""
        trigger = BasePythonTrigger(callable=_simple_trigger)
        # Manually clear callable_b64 to simulate bad deserialization
        trigger.callable_b64 = None

        with pytest.raises(ValueError, match="callable_b64 not set"):
            async for _ in trigger.run():
                pass
