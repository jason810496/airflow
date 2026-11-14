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

# @apache-airflow/providers-standard

The operators and sensors of `apache-airflow-providers-standard` 1.22.0, for a Dag written with
[`@apache-airflow/ts-sdk`](https://www.npmjs.com/package/@apache-airflow/ts-sdk).

A binding has no task body. It records the Python operator class and its arguments in the
Dag, and a Python worker that has the provider installed runs the operator. Such a task does
not take the Dag's queue: give it one in its task options when the default queue is not
served by Python workers.

```ts
import { Dag } from "@apache-airflow/ts-sdk";
import { BashOperator } from "@apache-airflow/providers-standard/operators/bash";

const dag = new Dag("example");
dag.task("my_task", BashOperator({ /* ... */ }))();
```

A module mirrors the Python module of the same path, and an argument is the Python
argument in camel case. A templated argument takes a Jinja template, or the result of
another task. A duration is a number of seconds.

This package is generated. To change it, regenerate the operator specs and the packages:

```bash
python3 scripts/ci/lang_sdk_providers/generate_operator_specs.py --allow-list standard
```

```bash
pnpm --dir ts-sdk run generate:providers
```

## Operators

| Operator | Module |
| --- | --- |
| `BashOperator` | `@apache-airflow/providers-standard/operators/bash` |
| `BaseBranchOperator` | `@apache-airflow/providers-standard/operators/branch` |
| `BranchDateTimeOperator` | `@apache-airflow/providers-standard/operators/datetime` |
| `EmptyOperator` | `@apache-airflow/providers-standard/operators/empty` |
| `ApprovalOperator` | `@apache-airflow/providers-standard/operators/hitl` |
| `HITLBranchOperator` | `@apache-airflow/providers-standard/operators/hitl` |
| `HITLEntryOperator` | `@apache-airflow/providers-standard/operators/hitl` |
| `HITLOperator` | `@apache-airflow/providers-standard/operators/hitl` |
| `LatestOnlyOperator` | `@apache-airflow/providers-standard/operators/latest_only` |
| `SmoothOperator` | `@apache-airflow/providers-standard/operators/smooth` |
| `TriggerDagRunOperator` | `@apache-airflow/providers-standard/operators/trigger_dagrun` |
| `BranchDayOfWeekOperator` | `@apache-airflow/providers-standard/operators/weekday` |
| `BashSensor` | `@apache-airflow/providers-standard/sensors/bash` |
| `DateTimeSensor` | `@apache-airflow/providers-standard/sensors/date_time` |
| `DateTimeSensorAsync` | `@apache-airflow/providers-standard/sensors/date_time` |
| `ExternalTaskMarker` | `@apache-airflow/providers-standard/sensors/external_task` |
| `ExternalTaskSensor` | `@apache-airflow/providers-standard/sensors/external_task` |
| `FileSensor` | `@apache-airflow/providers-standard/sensors/filesystem` |
| `TimeDeltaSensor` | `@apache-airflow/providers-standard/sensors/time_delta` |
| `TimeDeltaSensorAsync` | `@apache-airflow/providers-standard/sensors/time_delta` |
| `WaitSensor` | `@apache-airflow/providers-standard/sensors/time_delta` |
| `DayOfWeekSensor` | `@apache-airflow/providers-standard/sensors/weekday` |

## Not available

| Operator | Reason |
| --- | --- |
| `BranchExternalPythonOperator` | required argument 'python_callable' (Callable) is not JSON-serializable |
| `BranchPythonOperator` | required argument 'python_callable' (Callable) is not JSON-serializable |
| `BranchPythonVirtualenvOperator` | required argument 'python_callable' (Callable) is not JSON-serializable |
| `ExternalPythonOperator` | required argument 'python_callable' (Callable) is not JSON-serializable |
| `PythonOperator` | required argument 'python_callable' (Callable) is not JSON-serializable |
| `PythonVirtualenvOperator` | required argument 'python_callable' (Callable) is not JSON-serializable |
| `ShortCircuitOperator` | required argument 'python_callable' (Callable) is not JSON-serializable |
| `AssetEventSensor` | required argument 'asset' (Asset \| AssetAlias \| AssetRef) is not JSON-serializable |
| `PythonSensor` | required argument 'python_callable' (Callable) is not JSON-serializable |
| `TimeSensor` | required argument 'target_time' (datetime.time) is not JSON-serializable |
| `TimeSensorAsync` | required argument 'target_time' (datetime.time) is not JSON-serializable |
