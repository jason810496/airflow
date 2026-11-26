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

# 11. Callbacks and deadline alerts authoring interface

Date: 2026-11-26

## Status

Proposed.

## Context

The shared contract for callbacks, deadline alerts and listeners is in
[`airflow-core/adr/lang-sdk/0020`](../../airflow-core/adr/lang-sdk/0020-callbacks-deadline-alerts-and-listeners-across-lang-sdks.md).
This ADR records the Go spelling of it. Listeners have no Go API.

## Decision

1. **Callbacks are `DagSpec` and `TaskSpec` fields of type `[]airflow.Callback`.** `DagSpec` gets
   `OnSuccessCallback` and `OnFailureCallback`; `TaskSpec` gets `OnExecuteCallback`,
   `OnSuccessCallback`, `OnFailureCallback`, `OnRetryCallback` and `OnSkippedCallback`.
   `internal/genspec` adds them as hand-written fields, since the schema keys are the
   `has_on_*_callback` flags, which the serializer sets when a field is non-empty.
2. **A callback is an `airflow.CallbackFunc` or a `python.CallbackRef`.** A `CallbackFunc` takes an
   `airflow.CallbackContext`, which embeds `context.Context` and gives the client, the logger and
   the callback's details. A returned error is logged, as a panic is.
3. **`DagSpec.Deadline` takes `[]airflow.DeadlineAlert`.** References come from the `deadline`
   package. `Interval` is a `time.Duration`. An alert whose callback is a `CallbackFunc` must have a
   `Name` unique in the Dag.
4. **`airflow.Callback` and `airflow.DeadlineCallback` are separate interfaces.** A sync callback
   implements both; `python.AsyncCallbackRef` implements only `DeadlineCallback`, so the compiler
   rejects it outside a deadline alert. Each interface has one exported method, since a type in
   another package cannot implement an unexported one. The `Executor` option of `python.CallbackRef`
   is rejected at registration outside a deadline alert.

## Signatures

```go
// package airflow
type Callback interface{ CallbackJSON() map[string]any }          // nil for a CallbackFunc
type DeadlineCallback interface{ DeadlineCallbackJSON() map[string]any }
type CallbackFunc func(ctx CallbackContext) error                 // implements both

type CallbackContext struct{ context.Context /* unexported */ }
func (c CallbackContext) Client() sdk.Client
func (c CallbackContext) Logger() *slog.Logger
func (c CallbackContext) Source() CallbackSource                  // SourceTask, SourceDag, SourceDeadline
func (c CallbackContext) DagID() string
func (c CallbackContext) RunID() string
func (c CallbackContext) TaskInstance() (TaskInstance, bool)      // task callbacks
func (c CallbackContext) Error() string                           // failure and retry callbacks
func (c CallbackContext) Reason() string                          // Dag callbacks
func (c CallbackContext) Deadline() (DeadlineInfo, bool)          // DeadlineInfo{ID string; DeadlineTime time.Time}

type DagSpec struct {
    OnSuccessCallback, OnFailureCallback []Callback
    Deadline                             []DeadlineAlert
    /* ... */
}
type TaskSpec struct {
    OnExecuteCallback, OnSuccessCallback, OnFailureCallback []Callback
    OnRetryCallback, OnSkippedCallback                      []Callback
    /* ... */
}
type DeadlineReference interface{ DeadlineReferenceJSON() map[string]any }
type DeadlineAlert struct {
    Name      string                // required, and unique in the Dag, for a CallbackFunc
    Reference DeadlineReference
    Interval  time.Duration
    Callback  DeadlineCallback
}

// package deadline
type AverageRuntimeOptions struct{ MaxRuns, MinRuns int }   // 0 takes Python's default
func DagRunLogicalDate() airflow.DeadlineReference
func DagRunQueuedAt() airflow.DeadlineReference
func FixedDatetime(at time.Time) airflow.DeadlineReference
func AverageRuntime(opts ...AverageRuntimeOptions) airflow.DeadlineReference

// package python
type CallbackOptions struct{ Executor string }              // deadline alerts only
type AsyncCallbackOptions struct{ Queue string }
type SyncCallback interface{ airflow.Callback; airflow.DeadlineCallback }
func CallbackRef(path string, kwargs map[string]any, opts ...CallbackOptions) SyncCallback
func AsyncCallbackRef(path string, kwargs map[string]any, opts ...AsyncCallbackOptions) airflow.DeadlineCallback
```

## Example

```go
import (
    "fmt"
    "time"

    "github.com/apache/airflow/go-sdk/airflow"
    "github.com/apache/airflow/go-sdk/deadline"
    "github.com/apache/airflow/go-sdk/python"
)

func notifyTeam(ctx airflow.CallbackContext) error {
    return ctx.Client().SetVariable(ctx, "last_notified_run", ctx.RunID(), "")
}

func page(ctx airflow.CallbackContext) error {
    ctx.Logger().Error("failed", "dag_id", ctx.DagID(), "error", ctx.Error())
    return nil
}

func escalate(ctx airflow.CallbackContext) error {
    if d, ok := ctx.Deadline(); ok {
        return fmt.Errorf("%s missed %s", ctx.DagID(), d.DeadlineTime)
    }
    return nil
}

func newDag() *airflow.DagRef {
    dag := airflow.Dag("daily_report", airflow.DagSpec{
        Queue:             "golang",
        OnSuccessCallback: []airflow.Callback{airflow.CallbackFunc(notifyTeam),
            python.CallbackRef("my_plugin.alerts.notify", map[string]any{"channel": "#ops"})},
        OnFailureCallback: []airflow.Callback{airflow.CallbackFunc(page)},
        Deadline: []airflow.DeadlineAlert{
            {Name: "sla", Reference: deadline.DagRunQueuedAt(), Interval: time.Hour,
                Callback: airflow.CallbackFunc(escalate)},
            {Reference: deadline.DagRunLogicalDate(), Interval: 2 * time.Hour,
                Callback: python.CallbackRef("my_plugin.alerts.page", nil, python.CallbackOptions{Executor: "CeleryExecutor"})},
            {Reference: deadline.FixedDatetime(time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC)),
                Callback: python.AsyncCallbackRef("my_plugin.alerts.slack", map[string]any{"channel": "#ops"},
                    python.AsyncCallbackOptions{Queue: "default"})},
            {Name: "slow", Reference: deadline.AverageRuntime(deadline.AverageRuntimeOptions{MaxRuns: 10, MinRuns: 3}),
                Interval: 10 * time.Minute, Callback: airflow.CallbackFunc(escalate)},
        },
    })
    dag.Task(load, airflow.TaskSpec{
        OnExecuteCallback: []airflow.Callback{airflow.CallbackFunc(func(ctx airflow.CallbackContext) error {
            ctx.Logger().Info("starting")
            return nil
        })},
        OnSuccessCallback: []airflow.Callback{airflow.CallbackFunc(notifyTeam)},
        OnFailureCallback: []airflow.Callback{airflow.CallbackFunc(page),
            python.CallbackRef("my_plugin.alerts.notify", map[string]any{"channel": "#oncall"})},
        OnRetryCallback:   []airflow.Callback{airflow.CallbackFunc(page)},
        OnSkippedCallback: []airflow.Callback{airflow.CallbackFunc(page)},
    })
    return dag
}
```

## Consequences

- `internal/genspec` stops excluding `deadline` and adds the callback fields; the serializer writes
  `has_on_*_callback` from them.
- The runtime handles `CallbackStartupDetails` beside `StartupDetails`.
- `capabilities.yaml` marks callbacks and deadline alerts as supported, and listeners as supported
  through Python listeners.
