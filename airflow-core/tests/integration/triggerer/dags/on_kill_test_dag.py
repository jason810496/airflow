#
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

from datetime import datetime

from airflow.sdk import DAG, BaseOperator

from tests_common.test_utils.on_kill_trigger import MarkerFileTrigger


class DeferForeverOperator(BaseOperator):
    """Defers immediately on execute and never resumes on its own; the test
    kills it via the REST API to exercise the Triggerer's on_kill() path.
    """

    def execute(self, context):
        marker_path = context["dag_run"].conf["marker_path"]
        self.defer(trigger=MarkerFileTrigger(marker_path=marker_path), method_name="execute_complete")

    def execute_complete(
        self, context, event=None
    ):  # pragma: no cover - never reached; TI is killed while deferred
        pass


with DAG("on_kill_test_dag", start_date=datetime(2024, 9, 1), schedule=None, catchup=False):
    DeferForeverOperator(task_id="defer_task")
