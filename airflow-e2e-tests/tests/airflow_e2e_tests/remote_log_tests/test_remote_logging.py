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

import time

import pytest

from airflow_e2e_tests.e2e_test_utils.base_remote_log_search_test import BaseRemoteLoggingSearchTest
from airflow_e2e_tests.e2e_test_utils.clients import get_s3_client


class TestRemoteLoggingS3(BaseRemoteLoggingSearchTest):
    """
    S3 (LocalStack) remote logging e2e tests.

    The ``test-airflow-logs`` bucket is created by the LocalStack init script that is
    part of the docker-compose setup.
    """

    bucket_name = "test-airflow-logs"
    # example_xcom_test runs six task instances, each uploading one log object,
    # e.g. dag_id=example_xcom_test/run_id=manual__.../task_id=bash_pull/attempt=1.log
    task_count = 6

    def _list_log_objects(self) -> list[dict]:
        s3_client = get_s3_client()
        response = s3_client.list_objects_v2(Bucket=self.bucket_name)
        return response.get("Contents", [])

    def _wait_for_matching_logs(self, run_id: str) -> list:
        """Poll the S3 bucket until all task logs are uploaded and the expected log content appears."""
        s3_client = get_s3_client()
        contents: list[dict] = []
        for _ in range(self.max_retries):
            contents = self._list_log_objects()
            matching_keys = [
                obj["Key"]
                for obj in contents
                if f"dag_id={self.dag_id}" in obj["Key"]
                and f"run_id={run_id}" in obj["Key"]
                and f"task_id={self.task_id}" in obj["Key"]
            ]
            matching_logs = []
            for key in matching_keys:
                body = s3_client.get_object(Bucket=self.bucket_name, Key=key)["Body"].read().decode("utf-8")
                if self.expected_message in body:
                    matching_logs.append({"key": key, "content": body})
            if matching_logs and len(contents) >= self.task_count:
                return matching_logs
            time.sleep(self.retry_interval_in_seconds)

        pytest.fail(
            f"Expected at least {self.task_count} log objects in S3 bucket {self.bucket_name!r} and a "
            f"{self.task_id!r} log for run_id {run_id} containing {self.expected_message!r}; "
            f"found {len(contents)} objects: {[obj.get('Key') for obj in contents]}"
        )

    def _assert_task_logs_content(self, task_logs: dict, run_id: str) -> None:
        super()._assert_task_logs_content(task_logs, run_id)

        # The log source details in the API response must point at the S3 objects,
        # proving the API server served the logs from S3 rather than a local file.
        events = [item.get("event", "") for item in task_logs.get("content", []) if isinstance(item, dict)]
        log_files = [f"s3://{self.bucket_name}/{obj['Key']}" for obj in self._list_log_objects()]
        assert any(event in log_files for event in events), (
            f"None of the log sources in {events} point at the S3 objects {log_files}"
        )
