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

from airflow.hooks.base import BaseHook


class {{ cookiecutter.name }}Hook(BaseHook):
    """
    Hook for {{ cookiecutter.name }}.

    :param conn_id: Connection ID to use
    """

    conn_name_attr = "{{ cookiecutter.provider_name }}_conn_id"
    default_conn_name = "{{ cookiecutter.provider_name }}_default"
    conn_type = "{{ cookiecutter.provider_name }}"
    hook_name = "{{ cookiecutter.name }}"

    def __init__(self, conn_id: str = default_conn_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id

    def get_conn(self):
        """Return connection for the hook."""
        # Implement connection logic here
        pass
