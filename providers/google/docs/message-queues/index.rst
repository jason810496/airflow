.. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

.. NOTE TO CONTRIBUTORS:
   Please, only add notes to the Changelog just below the "Changelog" header when there are some breaking changes
   and you want to add an explanation to the users on how they are supposed to deal with them.
   The changelog is updated and maintained semi-automatically by release manager.


Google PubSub Message Queue
===========================

Google PubSub Queue Provider
----------------------------

Implemented by :class:`~airflow.providers.google.cloud.queues.pubsub.PubSubMessageQueueProvider`

The Google PubSub Queue Provider is a message queue provider that uses
Google Cloud PubSub as the underlying message queue system.
It allows you to receive messages using PubSub subscriptions in your Airflow workflows.

Queue URI Format:

.. code-block:: text

    pubsub://project_id=...&subscription=...&max_messages=...&ack_messages=...&gcp_conn_id=...&poke_interval=...&impersonation_chain=...

Where:

- ``project_id``: Google Cloud project ID
- ``subscription``: Pub/Sub subscription name
- ``max_messages``: Maximum number of messages to retrieve per pull
- ``ack_messages``: Whether to acknowledge messages immediately (true/false)
- ``gcp_conn_id``: Airflow connection ID for Google Cloud
- ``poke_interval``: Polling interval in seconds
- ``impersonation_chain``: Service account(s) to impersonate

All params are passed to :class:`~airflow.providers.google.cloud.triggers.pubsub.PubsubPullTrigger` constructor.

Example usage:

.. code-block:: python

    from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger

    trigger = MessageQueueTrigger(
        queue="pubsub://project_id=my-proj&subscription=my-sub&max_messages=5&ack_messages=true",
    )
