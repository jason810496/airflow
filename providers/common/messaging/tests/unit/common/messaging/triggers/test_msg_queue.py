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

from airflow.providers.amazon.aws.triggers.sqs import SqsSensorTrigger
from airflow.providers.apache.kafka.triggers.await_message import AwaitMessageTrigger
from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger


class TestMessageQueueTrigger:
    @pytest.mark.parametrize(
        ("kwargs", "expected_trigger_class", "expected_trigger_attributes"),
        [
            pytest.param(
                {"queue": "https://sqs.us-east-1.amazonaws.com/0123456789/Test"},
                SqsSensorTrigger,
                None,
                id="SQS",
            ),
            pytest.param(
                {
                    "queue": "kafka://localhost:9092/t1,t2",
                    "apply_function": "mock_kafka_trigger_apply_function",
                },
                AwaitMessageTrigger,
                {
                    "topics": ["t1", "t2"],
                    "apply_function": "mock_kafka_trigger_apply_function",
                },
                id="Apache Kafka with topics in URI",
            ),
            pytest.param(
                {
                    "queue": "kafka://localhost:9092",
                    "apply_function": "mock_kafka_trigger_apply_function",
                    "topics": ["t1", "t2"],
                },
                AwaitMessageTrigger,
                {
                    "topics": ["t1", "t2"],
                    "apply_function": "mock_kafka_trigger_apply_function",
                },
                id="Apache Kafka with multiple topics in kwargs",
            ),
        ],
    )
    def test_provider_integrations(self, kwargs, expected_trigger_class, expected_trigger_attributes):
        trigger = MessageQueueTrigger(**kwargs)
        assert isinstance(trigger.trigger, expected_trigger_class)

        if expected_trigger_attributes is None:
            return
        # check trigger attributes if provided
        for key, value in expected_trigger_attributes.items():
            assert getattr(trigger.trigger, key) == value
