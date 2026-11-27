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

# ADR-0005: Java Authoring Interface for Callbacks and Deadline Alerts

## Status

Proposed.

## Context

The shared contract for callbacks, deadline alerts and listeners is in
[`airflow-core/adr/lang-sdk/0020`](../../airflow-core/adr/lang-sdk/0020-callbacks-deadline-alerts-and-listeners-across-lang-sdks.md).
This ADR records the Java spelling of it on the interface surface (`DagDef`) and on the annotation
surface of [ADR-0002](0002-native-dag-interface.md). Listeners have no Java API.

## Decision

1. **Callbacks are methods on `DagDef` and `TaskRef`.** `DagDef` gets `onSuccessCallback` and
   `onFailureCallback`; `TaskRef` gets `onExecuteCallback`, `onSuccessCallback`,
   `onFailureCallback`, `onRetryCallback` and `onSkippedCallback`. Each call appends, so callbacks
   run in the order of the calls. Each has a `Callback...` overload and a `PythonCallback...`
   overload, since a lambda only converts to a functional interface.
2. **`Callback` is a functional interface** taking a `CallbackContext` and a `Client`, as
   `Task.execute` takes a `Context` and a `Client`. An exception it throws is logged.
3. **`DagDef.deadline(DeadlineAlert...)` adds deadline alerts.** Built-in references come
   from `org.apache.airflow.sdk.deadline.Deadlines`, and a custom reference is
   `Python.deadlineReferenceRef`. A `DeadlineAlert` with a `Callback` takes a
   required name, unique in the Dag; the overloads for a Python callback take an optional one.
4. **`Python` adds `callbackRef`, `asyncCallbackRef` and `deadlineReferenceRef`.** `PythonAsyncCallback` is accepted only by
   `DeadlineAlert`, so the compiler rejects it elsewhere. The `executor` argument of `callbackRef` is
   rejected at registration outside a deadline alert.
5. **The annotation surface has one annotation per callback.** `@Airflow.OnSuccessCallback` and
   `@Airflow.OnFailureCallback` on a method of the Dag class are Dag callbacks; with `task` they are
   task callbacks. `@Airflow.OnExecuteCallback`, `@Airflow.OnRetryCallback` and
   `@Airflow.OnSkippedCallback` require `task`. `@Airflow.Deadline` marks a method as an alert's
   callback. The method takes the same arguments as `Callback.call`. Methods carrying the same
   annotation run in source order. Python references on the annotation surface are left to a
   follow-up, since an annotation attribute cannot hold a `Map` of kwargs. Custom deadline
   references are left to the same follow-up.
6. **Tasks get an email recipient.** `TaskRef.email(String...)` on the interface surface and
   `@Airflow.Task(email = {...})` on the annotation surface set it. The serializer stops omitting
   `email_on_failure` and `email_on_retry`, and writes them only when the task has an email, as
   Python does.

## Signatures

```java
// org.apache.airflow.sdk
@FunctionalInterface
interface Callback {
  void call(CallbackContext context, Client client) throws Exception;
}
final class CallbackContext {
  CallbackSource getSource();            // TASK, DAG, DEADLINE
  String getDagId();
  String getRunId();
  String getTaskId();                    // task callbacks, otherwise null
  Integer getTryNumber();                // task callbacks
  Integer getMapIndex();                 // task callbacks
  String getError();                     // failure and retry callbacks
  String getReason();                    // Dag callbacks
  DeadlineInfo getDeadline();            // deadline callbacks: getId(), getDeadlineTime() (OffsetDateTime)
}

// DagDef
DagDef onSuccessCallback(Callback... callbacks);
DagDef onSuccessCallback(PythonCallback... callbacks);
DagDef onFailureCallback(Callback... callbacks);
DagDef onFailureCallback(PythonCallback... callbacks);
DagDef deadline(DeadlineAlert... alerts);

// TaskRef: the same two overloads for each of
TaskRef<T> onExecuteCallback(Callback... callbacks);
TaskRef<T> onSuccessCallback(Callback... callbacks);
TaskRef<T> onFailureCallback(Callback... callbacks);
TaskRef<T> onRetryCallback(Callback... callbacks);
TaskRef<T> onSkippedCallback(Callback... callbacks);
TaskRef<T> email(String... addresses);

final class DeadlineAlert {
  DeadlineAlert(String name, DeadlineReference reference, Duration interval, Callback callback);
  DeadlineAlert(DeadlineReference reference, Duration interval, PythonCallback callback);
  DeadlineAlert(String name, DeadlineReference reference, Duration interval, PythonCallback callback);
  DeadlineAlert(DeadlineReference reference, Duration interval, PythonAsyncCallback callback);
  DeadlineAlert(String name, DeadlineReference reference, Duration interval, PythonAsyncCallback callback);
}

// org.apache.airflow.sdk.deadline.Deadlines
static DeadlineReference dagRunLogicalDate();
static DeadlineReference dagRunQueuedAt();
static DeadlineReference fixedDatetime(OffsetDateTime at);
static DeadlineReference averageRuntime();
static DeadlineReference averageRuntime(int maxRuns);
static DeadlineReference averageRuntime(int maxRuns, int minRuns);

// org.apache.airflow.sdk.python.Python
static PythonCallback callbackRef(String path, Map<String, Object> kwargs);
static PythonCallback callbackRef(String path, Map<String, Object> kwargs, String executor);
static PythonAsyncCallback asyncCallbackRef(String path, Map<String, Object> kwargs);
static PythonAsyncCallback asyncCallbackRef(String path, Map<String, Object> kwargs, String queue);
static DeadlineReference deadlineReferenceRef(String path, Map<String, Object> data);

// org.apache.airflow.sdk.Airflow (annotation surface)
@interface Task { /* ... */ String[] email() default {}; }
@interface OnSuccessCallback { String task() default ""; }   // "" means a Dag callback
@interface OnFailureCallback { String task() default ""; }
@interface OnExecuteCallback { String task(); }
@interface OnRetryCallback { String task(); }
@interface OnSkippedCallback { String task(); }
@interface Deadline {
  String name();
  DeadlineRef reference();                 // enum Airflow.DeadlineRef: DAG_RUN_LOGICAL_DATE, DAG_RUN_QUEUED_AT,
                                           //   FIXED_DATETIME, AVERAGE_RUNTIME
  String interval();                       // ISO 8601 duration, such as "PT1H"
  String at() default "";                  // FIXED_DATETIME only, with an offset
  int maxRuns() default 0;                 // AVERAGE_RUNTIME only; 0 takes Python's default
  int minRuns() default 0;
}
```

## Example

```java
import static org.apache.airflow.sdk.deadline.Deadlines.*;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Map;
import org.apache.airflow.sdk.*;
import org.apache.airflow.sdk.python.Python;

public class Reports {
  static void notifyTeam(CallbackContext context, Client client) {
    client.setVariable("last_notified_run", context.getRunId());
  }

  static void page(CallbackContext context, Client client) {
    System.err.printf("%s failed: %s%n", context.getDagId(), context.getError());
  }

  static void escalate(CallbackContext context, Client client) {
    System.err.printf("%s missed %s%n", context.getDagId(), context.getDeadline().getDeadlineTime());
  }

  public static DagDef dag() {
    var dag = new DagDef("daily_report").config("queue", "java")
        .onSuccessCallback(Reports::notifyTeam)
        .onSuccessCallback(Python.callbackRef("my_plugin.alerts.notify", Map.of("channel", "#ops")))
        .onFailureCallback(Reports::page)
        .deadline(
            new DeadlineAlert("sla", dagRunQueuedAt(), Duration.ofHours(1), Reports::escalate),
            new DeadlineAlert(dagRunLogicalDate(), Duration.ofHours(2),
                Python.callbackRef("my_plugin.alerts.page", Map.of(), "CeleryExecutor")),
            new DeadlineAlert(fixedDatetime(OffsetDateTime.parse("2026-12-31T00:00:00+01:00")), Duration.ZERO,
                Python.asyncCallbackRef("my_plugin.alerts.slack", Map.of("channel", "#ops"), "default")),
            new DeadlineAlert("slow", averageRuntime(10, 3), Duration.ofMinutes(10), Reports::escalate),
            new DeadlineAlert("batch",
                Python.deadlineReferenceRef("my_plugin.deadlines.UpstreamBatchLanded", Map.of("batch", "eu")),
                Duration.ofMinutes(30), Reports::escalate));
    dag.task("load", Load.class)
        .onExecuteCallback((context, client) -> System.out.println("starting try " + context.getTryNumber()))
        .onSuccessCallback(Reports::notifyTeam)
        .onFailureCallback(Reports::page)
        .onFailureCallback(Python.callbackRef("my_plugin.alerts.notify", Map.of("channel", "#oncall")))
        .onRetryCallback(Reports::page)
        .onSkippedCallback(Reports::page)
        .email("oncall@example.com")
        .config("email_on_retry", false);
    return dag;
  }

  public static class Load implements Task {
    @Override
    public void execute(Context context, Client client) { /* ... */ }
  }
}
```

The annotation surface:

```java
@Airflow.Dag(id = "daily_report_annotated", queue = "java")
public class DailyReport {
  @Airflow.Task(email = {"oncall@example.com"})
  public void load(Client client) { /* ... */ }

  @Airflow.OnSuccessCallback
  public void notifyTeam(CallbackContext context, Client client) { /* ... */ }

  @Airflow.OnFailureCallback
  @Airflow.OnFailureCallback(task = "load")
  @Airflow.OnRetryCallback(task = "load")
  public void page(CallbackContext context, Client client) { /* ... */ }

  @Airflow.OnExecuteCallback(task = "load")
  @Airflow.OnSkippedCallback(task = "load")
  public void trace(CallbackContext context, Client client) { /* ... */ }

  @Airflow.Deadline(name = "sla", reference = DeadlineRef.DAG_RUN_QUEUED_AT, interval = "PT1H")
  @Airflow.Deadline(name = "new_year", reference = DeadlineRef.FIXED_DATETIME, interval = "PT0S",
      at = "2026-12-31T00:00:00+01:00")
  @Airflow.Deadline(name = "slow", reference = DeadlineRef.AVERAGE_RUNTIME, interval = "PT10M",
      maxRuns = 10, minRuns = 3)
  public void escalate(CallbackContext context, Client client) { /* ... */ }

  @Airflow.Deps
  static class Wiring implements DailyReportDeps {
    void depends() {
      load();
    }
  }
}
```

## Consequences

- The annotations are repeatable, and the processor rejects a `task` that names no task of the Dag,
  a callback method with other arguments, and a duplicate deadline name.
- The serializer writes `has_on_*_callback` from these calls and the deadline alerts into
  `deadline`, which it writes as `null` today.
- The runtime handles the callback startup messages of core ADR-0020 beside `StartupDetails`, and
  sends `RunPythonCallback` and `SendErrorEmail` to the supervisor.
- `capabilities.yaml` marks callbacks and deadline alerts as supported, and listeners as supported
  through Python listeners.
