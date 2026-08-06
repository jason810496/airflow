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

import asyncio
from pathlib import Path
from typing import Any

from airflow._shared.module_loading import qualname
from airflow.triggers.base import BaseTrigger, TriggerEvent


class MarkerFileTrigger(BaseTrigger):
    """Stays pending; on_kill() writes a sentinel file so the test process
    (a different OS process than the Triggerer) can observe that it fired.

    Defined here, rather than inline in the fixture DAG, so its classpath is a
    normal importable dotted path. A trigger class embedded directly in a DAG
    file gets loaded under a per-process, hash-prefixed module name (see
    ``airflow.utils.file.get_unique_dag_module_name``), which the Triggerer
    process cannot resolve via ``import_string`` when deserializing the trigger.
    """

    def __init__(self, marker_path: str):
        super().__init__()
        self.marker_path = marker_path

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (qualname(self), {"marker_path": self.marker_path})

    async def run(self):
        # Signal that the triggerer has actually claimed and started this
        # trigger, before blocking — the test waits on this file before
        # issuing the kill, to avoid a race against the trigger not being
        # registered yet.
        await asyncio.to_thread(Path(f"{self.marker_path}.started").write_text, "started")
        await asyncio.sleep(1_000_000)
        yield TriggerEvent(True)  # pragma: no cover - never reached in this test

    async def on_kill(self) -> None:
        await asyncio.to_thread(Path(self.marker_path).write_text, "killed")
