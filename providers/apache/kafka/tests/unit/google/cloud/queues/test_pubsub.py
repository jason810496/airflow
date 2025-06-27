import pytest
from airflow.providers.google.cloud.queues.pubsub import PubSubMessageQueueProvider
from airflow.providers.google.cloud.triggers.pubsub import PubsubPullTrigger

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
def test_pubsub_message_queue_provider_trigger_kwargs(queue, expected_params):
    provider = PubSubMessageQueueProvider()
    assert provider.queue_matches(queue)
    kwargs = provider.trigger_kwargs(queue)
    for k, v in expected_params.items():
        assert kwargs[k] == v
    assert provider.trigger_class() is PubsubPullTrigger
