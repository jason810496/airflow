# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
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
"""
Google PubSub Message Queue Provider.
"""

from __future__ import annotations
from urllib.parse import urlparse, parse_qs
from airflow.providers.common.messaging.providers.base_provider import BaseMessageQueueProvider
from airflow.providers.google.cloud.triggers.pubsub import PubsubPullTrigger

class PubSubMessageQueueProvider(BaseMessageQueueProvider):
    """
    Message queue provider for Google PubSub.
    Matches queues of the form: pubsub://k1=v1&k2=v2...

    trigger_kwargs method passes the following parameters to PubsubPullTrigger:

    :param project_id: the Google Cloud project ID for the subscription (templated)
    :param subscription: the Pub/Sub subscription name. Do not include the full subscription path.
    :param max_messages: The maximum number of messages to retrieve per PubSub pull request
    :param ack_messages: If True, each message will be acknowledged immediately rather than by any downstream tasks
    :param gcp_conn_id: Reference to google cloud connection id
    :param poke_interval: polling period in seconds to check for the status
    :param impersonation_chain: Optional service account to impersonate using short-term credentials, or chained list of accounts required to get the access_token of the last account in the list, which will be impersonated in the request. If set as a string, the account must grant the originating account the Service Account Token Creator IAM role. If set as a sequence, the identities from the list must grant Service Account Token Creator IAM role to the directly preceding identity, with first account from the list granting this role to the originating account (templated).
    """
    def queue_matches(self, queue: str) -> bool:
        return queue.startswith("pubsub://")

    def trigger_class(self):
        return PubsubPullTrigger

    def trigger_kwargs(self, queue: str, **kwargs):
        """
        Returns kwargs for PubsubPullTrigger.
        Queue URI should be of the form:
        pubsub://project_id=...&subscription=...&max_messages=...&ack_messages=...&gcp_conn_id=...&poke_interval=...&impersonation_chain=...
        All params are passed to PubsubPullTrigger constructor.
        
        :param queue: The queue URI
        :param kwargs: Additional keyword arguments
        :return: Dict of trigger kwargs
        """
        # Parse URI
        parsed = urlparse(queue)
        # Accept both netloc and query string for params
        params = {}
        if parsed.netloc:
            params.update({k: v[0] for k, v in parse_qs(parsed.netloc).items()})
        if parsed.query:
            params.update({k: v[0] for k, v in parse_qs(parsed.query).items()})
        params.update(kwargs)
        if "max_messages" in params:
            params["max_messages"] = int(params["max_messages"])
        if "ack_messages" in params:
            params["ack_messages"] = params["ack_messages"] in ("1", "true", "True", True)
        if "poke_interval" in params:
            params["poke_interval"] = float(params["poke_interval"])
        return params
