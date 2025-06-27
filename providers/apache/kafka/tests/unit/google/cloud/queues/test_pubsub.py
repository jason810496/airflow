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
import pytest
from airflow.providers.google.cloud.queues.pubsub import PubSubMessageQueueProvider
from airflow.providers.google.cloud.triggers.pubsub import PubsubPullTrigger
from airflow.providers.common.messaging.providers.base_provider import BaseMessageQueueProvider

class TestPubSubMessageQueueProvider:
    def setup_method(self):
        self.provider = PubSubMessageQueueProvider()

    def test_queue_create(self):
        assert isinstance(self.provider, BaseMessageQueueProvider)

    @pytest.mark.parametrize(
        "queue_uri, expected_result",
        [
            ("pubsub://project_id=test&subscription=sub", True),
            ("pubsub://?project_id=test&subscription=sub", True),
            ("kafka://broker/topic", False),
            ("not-a-uri", False),
        ],
    )
    def test_queue_matches(self, queue_uri, expected_result):
        assert self.provider.queue_matches(queue_uri) == expected_result

    def test_trigger_class(self):
        assert self.provider.trigger_class() is PubsubPullTrigger

    @pytest.mark.parametrize(
        "queue,expected_params",
        [
            (
                "pubsub://project_id=test-project&subscription=test-sub&max_messages=5&ack_messages=true&gcp_conn_id=my_conn&poke_interval=2.5",
                {
                    "project_id": "test-project",
                    "subscription": "test-sub",
                    "max_messages": 5,
                    "ack_messages": True,
                    "gcp_conn_id": "my_conn",
                    "poke_interval": 2.5,
                },
            ),
            (
                "pubsub://project_id=foo&subscription=bar&max_messages=1&ack_messages=0",
                {
                    "project_id": "foo",
                    "subscription": "bar",
                    "max_messages": 1,
                    "ack_messages": False,
                },
            ),
            (
                "pubsub://?project_id=abc&subscription=def&max_messages=10&ack_messages=1&poke_interval=3",
                {
                    "project_id": "abc",
                    "subscription": "def",
                    "max_messages": 10,
                    "ack_messages": True,
                    "poke_interval": 3.0,
                },
            ),
        ],
    )
    def test_trigger_kwargs_valid(self, queue, expected_params):
        kwargs = self.provider.trigger_kwargs(queue)
        for k, v in expected_params.items():
            assert kwargs[k] == v

    @pytest.mark.parametrize(
        "queue,extra_kwargs,expected_error,match",
        [
            ("pubsub://", {"max_messages": "not_an_int"}, ValueError, "invalid literal for int"),
            ("pubsub://project_id=foo&subscription=bar&max_messages=1&ack_messages=maybe", {}, ValueError, "could not convert"),
        ],
    )
    def test_trigger_kwargs_error_cases(self, queue, extra_kwargs, expected_error, match):
        with pytest.raises(expected_error, match=match):
            self.provider.trigger_kwargs(queue, **extra_kwargs)
