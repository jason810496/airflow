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

# github.com/apache/airflow/go-sdk/providers/standard

The operators and sensors of `apache-airflow-providers-standard` 1.22.0, for a Dag written with the Go SDK.

A wrapper has no task body. It records the Python operator class and its arguments in the
Dag, and a Python worker that has the provider installed runs the operator. Such a task takes
the Dag's queue like any other task: give it a `Queue` in its `TaskSpec` when the Dag's queue
is not served by Python workers.

```go
import "github.com/apache/airflow/go-sdk/providers/standard/operators/bash"

dag.Task(
    bash.BashOperator(bash.BashOperatorArgs{ /* ... */ }),
    airflow.TaskSpec{TaskID: "my_task", Queue: "default"},
)
```

A package mirrors the Python module of the same path, without the underscores, and an
argument is the Python argument in Go case. An argument that is not required is a pointer
or a slice, a map or `any`, so that `airflow.Ptr` sets it and nil leaves it out. A templated
argument is an `airflow.Templated`: it takes a Jinja template, or the result of another task
with `airflow.ResultOf`. A duration is a `time.Duration`.

This package is generated. To change it, regenerate the operator specs and the packages:

```bash
python3 scripts/ci/lang_sdk_providers/generate_operator_specs.py --allow-list standard
```

```bash
just --justfile go-sdk/Justfile generate-providers
```

## Operators

| Operator | Package |
| --- | --- |
| `BashOperator` | `github.com/apache/airflow/go-sdk/providers/standard/operators/bash` |
| `BaseBranchOperator` | `github.com/apache/airflow/go-sdk/providers/standard/operators/branch` |
| `BranchDateTimeOperator` | `github.com/apache/airflow/go-sdk/providers/standard/operators/datetime` |
| `EmptyOperator` | `github.com/apache/airflow/go-sdk/providers/standard/operators/empty` |
| `ApprovalOperator` | `github.com/apache/airflow/go-sdk/providers/standard/operators/hitl` |
| `HITLBranchOperator` | `github.com/apache/airflow/go-sdk/providers/standard/operators/hitl` |
| `HITLEntryOperator` | `github.com/apache/airflow/go-sdk/providers/standard/operators/hitl` |
| `HITLOperator` | `github.com/apache/airflow/go-sdk/providers/standard/operators/hitl` |
| `LatestOnlyOperator` | `github.com/apache/airflow/go-sdk/providers/standard/operators/latestonly` |
| `SmoothOperator` | `github.com/apache/airflow/go-sdk/providers/standard/operators/smooth` |
| `TriggerDagRunOperator` | `github.com/apache/airflow/go-sdk/providers/standard/operators/triggerdagrun` |
| `BranchDayOfWeekOperator` | `github.com/apache/airflow/go-sdk/providers/standard/operators/weekday` |
| `BashSensor` | `github.com/apache/airflow/go-sdk/providers/standard/sensors/bash` |
| `DateTimeSensor` | `github.com/apache/airflow/go-sdk/providers/standard/sensors/datetime` |
| `DateTimeSensorAsync` | `github.com/apache/airflow/go-sdk/providers/standard/sensors/datetime` |
| `ExternalTaskMarker` | `github.com/apache/airflow/go-sdk/providers/standard/sensors/externaltask` |
| `ExternalTaskSensor` | `github.com/apache/airflow/go-sdk/providers/standard/sensors/externaltask` |
| `FileSensor` | `github.com/apache/airflow/go-sdk/providers/standard/sensors/filesystem` |
| `TimeDeltaSensor` | `github.com/apache/airflow/go-sdk/providers/standard/sensors/timedelta` |
| `TimeDeltaSensorAsync` | `github.com/apache/airflow/go-sdk/providers/standard/sensors/timedelta` |
| `WaitSensor` | `github.com/apache/airflow/go-sdk/providers/standard/sensors/timedelta` |
| `DayOfWeekSensor` | `github.com/apache/airflow/go-sdk/providers/standard/sensors/weekday` |

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
