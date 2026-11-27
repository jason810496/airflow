<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Variant B: Python tasks on the supervisor write path

Throwaway experiment on branch `exp/python-supervisor-path`. It is not a PR. Set `AIRFLOW_EXP_SUPERVISOR_WRITE_PATH=1` to turn it on; without the flag the build behaves as before.

## What moved

With the flag set, the task process no longer runs the XCom backend or measures the map length for `ti.xcom_push`, return values, `multiple_outputs` keys and `axcom_push`. It encodes the value with `serialize()` only so it can cross the wire, and sends it in an envelope inside `SetXCom.value`. The task process still tells the supervisor whether a mapped task needs a length, because it is the one that parsed the Dag.

The supervisor (`ActivitySubprocess._handle_set_xcom`) then does the work:

- Measures the length with the `is_mappable_value` rule: a list, a dict or a tuple. A missing or unmappable value is rejected, and the task process re-raises `XComForMappingNotPushed` or `UnmappableXComTypePushed` with the original value, so the message matches baseline.
- With the default backend, forwards the encoded value as the row. This is what `BaseXCom.store_value` would have stored.
- With a custom backend, stores the value through the helper process that Lang-SDK tasks use (`XComBackendProcess`, `XComBackendCall.store`). The row holds what the backend returns.

Not moved: `XCom.set()` and `XCom.aset()` called from user code, direct row writes (skipmixin key, operator extra links), the triggerer, reads, and `dag.test()`. An in-process supervisor has no helper process, so it stays on the task's own path.

The envelope avoids a supervisor schema version and Go, TS and Java schema regeneration in a throwaway. A real change needs a field on `SetXCom` with a `VersionChange`. ADR-0016 has the server set `needs_mapped_length` at `ti_run`. `get_needs_mapped_length` already works on serialized Python tasks, so adopting it would remove the task-side bit.

## What the supervisor now holds

For each push, the supervisor holds the whole value several times, briefly: the frame bytes, the decoded object, the validated `JsonValue`, and, with a custom backend, the re-encoded `XComBackendCall` for the helper. With a custom backend this is new. Baseline sent the supervisor a pointer, and the default backend already sent it the full value. It also imports the configured backend class, which a Python task's supervisor did not need before. The helper holds the decoded value, the deserialized object, and whatever the backend builds.

One-off check (single run each, mock API client, 500k strings of about 70 characters, about 40 MB as JSON). The supervisor's peak RSS started at about 106 MB.

| Backend | Flag off | Flag on |
|---|---|---|
| Default | 199 MB | 199 MB |
| Custom (file) | 106 MB | 213 MB |

The mock client keeps the pushed value, so the default row includes that copy. The largest child process peaked at 240 MB off and 294 MB on with the custom backend. Timings for a one-element value were 0.4 to 0.8 s in both modes, so the helper's start cost is not resolved here. Use the shared harness for that.

## Correctness

`test_xcom_write_path.py` runs each of plain, `expand`, `expand_kwargs`, `.map()`, `zip`, `concat`, a mapped task group and a reduce, with 13 return values, against the default and a custom backend. It compares what reaches `client.xcoms.set` (value, `mapped_length`, flags), the raised error, and the value the backend received. All 208 cases match baseline. Real task and helper processes also run with the flag off and on: the custom backend writes in the task process off, and in the helper on.

Existing suites, `test_task_runner.py` plus `test_supervisor.py`: this environment has 40 baseline failures and errors (missing `pyarrow`, `airflow_shared`, `kubernetes`). Flag off gives the same 40. Flag on adds 8, all asserting the old message shape: six `TestTriggerDagRunOperator` tests, `test_xcom_push_to_custom_xcom_backend` (expects `XCom._push`) and `test_dag_add_result`.

## Risks

- A custom backend receives `deserialize(serialize(value))`, not the original object. A class that is not on the allow list, or not importable in the helper, fails the write with `XCOM_BACKEND_ERROR`. The task process registers operator-declared classes at startup and the helper does not. `TestKnownDivergences` shows it.
- A backend that stores objects serde cannot encode now fails in the task process.
- Errors differ. An unserializable and unmappable value such as `bytes` raises `TypeError` instead of `UnmappableXComTypePushed`. A backend failure reaches the task as `AirflowRuntimeError` with the message but not the original type or traceback. With `multiple_outputs`, an unmappable return value is rejected after the per-key pushes instead of before.
- A dict whose keys collide after `str()` measures a shorter length than baseline.
- The supervisor is single threaded. Decoding and re-encoding a large payload blocks its request loop and heartbeats.
- A custom backend now costs a helper process for every task that pushes. The helper-management code is copied from `_PopenActivitySubprocess`; adopting this would hoist it into one mixin.

## Files

- `task-sdk/src/airflow/sdk/execution_time/xcom_write_path.py`: flag, envelope, measurement, supervisor-side writer.
- `task-sdk/src/airflow/sdk/execution_time/supervisor.py`: handler, writer field, helper stop in `wait()`.
- `task-sdk/src/airflow/sdk/execution_time/task_runner.py`: `_xcom_push`, `_axcom_push`, `_push_xcom_if_needed`.
- `task-sdk/tests/task_sdk/execution_time/test_xcom_write_path.py`.
