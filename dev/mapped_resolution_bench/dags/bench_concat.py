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
"""Benchmark Dag: ``expand`` over ``concat`` of two XComs."""

from __future__ import annotations

import os
import time

# Benchmark prologue, identical in every Dag file: count the imports and weigh the file down.
if _import_log := os.environ.get("BENCH_IMPORT_LOG"):
    with open(_import_log, "a") as _f:
        _f.write(f"{os.getpid()} {os.environ.get('BENCH_TI', '')}\n")
if os.environ.get("BENCH_WEIGHT") == "heavy":
    time.sleep(1)
    _BALLAST = b"\x01" * (200 * 1024 * 1024)

from airflow.sdk import DAG, task

with DAG("bench_concat", schedule=None):

    @task
    def make_low():
        return [1, 2]

    @task
    def make_high():
        return [10, 20]

    @task
    def consume(x):
        return {"x": x, "type": type(x).__name__}

    consume.expand(x=make_low().concat(make_high()))
