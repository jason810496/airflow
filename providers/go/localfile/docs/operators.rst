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

.. _howto/operator:go.localfile:

Local File operators
====================

The operators work on files of the machine that runs the task. They are implemented in Go, so a task of
one of them has to run on a worker of the Go runtime. Set the task's ``queue`` to one that
``[sdk] queue_to_coordinator`` routes to the Go coordinator. A task left on a queue that a Python worker
serves fails and names the runtime to use.

The Go coordinator runs a task of a Python Dag in the SDK's generic provider runner, which loads the
operators from the plugin executable ``airflow-provider-go-localfile``. Install both in the directory that the
``go_providers_path`` setting of ``ExecutableCoordinator`` names: the runner as
``<go_providers_path>/airflow-provider-runner`` and the plugin as
``<go_providers_path>/plugins/airflow-provider-go-localfile``.

.. code-block:: python

    from airflow.providers.go.localfile.operators.file import HasFileOperator, WriteFileOperator

    with DAG("write_report"):
        write = WriteFileOperator(
            task_id="write",
            path="/tmp/{{ run_id }}.txt",
            content="done",
            queue="golang",
        )
        check = HasFileOperator(
            task_id="check",
            path="/tmp/{{ run_id }}.txt",
            skip_downstream_if_missing=True,
            queue="golang",
        )
        write >> check

``path``, ``content`` and ``key`` are template fields, which render in Python before the operator runs.

.. list-table::
   :header-rows: 1

   * - Operator
     - Result
   * - ``CreateFileOperator``
     - ``{"path": ..., "created": ...}``. Creates missing parent directories. With ``exist_ok=False`` an
       existing file fails the task.
   * - ``WriteFileOperator``
     - ``{"path": ..., "bytes": ..., "sha256": ...}`` of the file as read back from disk. ``mode`` is
       ``overwrite`` (default) or ``append``, and ``encoding`` is ``utf-8`` (default), ``ascii`` or ``latin1``.
   * - ``WriteVariableOperator``
     - The same result as ``WriteFileOperator``. It reads the Airflow Variable ``key`` through the SDK client
       while it runs, and replaces the file at ``path`` with its value.
   * - ``HasFileOperator``
     - ``True`` when the file exists. With ``skip_downstream_if_missing=True``, a missing file skips the
       direct downstream tasks.
   * - ``DeleteFileOperator``
     - ``{"path": ..., "deleted": ...}``. With ``missing_ok=False`` a missing file fails the task.
