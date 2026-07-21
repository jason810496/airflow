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

# 1. Watcher-owned terminal state for external-system operators

Date: 2026-07-21

## Status

Proposed. This has not been through a `[DISCUSS]`/`[PROPOSAL]` devlist thread
or a formal AIP. It records a design worked out in detail so it survives past
the conversation that produced it, and can be picked apart before any code is
written.

## Context

For operators like `KubernetesPodOperator` (and the `@task.kubernetes`
decorator built on it), the real work happens in an external resource — a
Kubernetes pod — that Airflow does not own. Today, regardless of whether the
operator polls synchronously or defers, a TaskInstance's terminal state
(`SUCCESS`/`FAILED`/...) is always **reported by the task process itself**,
never by anything watching the external resource on the task's behalf.

### The current push model

The task process (via its supervisor, `ActivitySubprocess` in
`task-sdk/src/airflow/sdk/execution_time/supervisor.py`) calls the Execution
API itself to report state: `PATCH /task-instances/{id}/state`,
`PATCH .../run`, `PUT .../heartbeat`
(`airflow-core/src/airflow/api_fastapi/execution_api/routes/task_instances.py`).
Callbacks (`on_success_callback`/`on_failure_callback`) and XCom push also run
**inside** the task process, in `finalize()`
(`task-sdk/src/airflow/sdk/execution_time/task_runner.py`).

### What happens today for `@task.kubernetes`

Non-deferrable (default): the worker task process itself creates the child
pod and **blocks in-process** polling it
(`KubernetesPodOperator.execute_sync`, `providers/cncf/kubernetes/.../operators/pod.py`).
The worker is the watcher.

Deferrable (`deferrable=True`): the worker creates the pod, then defers —
`KubernetesPodTrigger.run()`
(`providers/cncf/kubernetes/src/airflow/providers/cncf/kubernetes/triggers/pod.py`)
runs in the **Triggerer** and is a genuine watcher of the pod's phase. But
when it fires a `TriggerEvent`, `handle_event_submit`
(`airflow-core/src/airflow/models/trigger.py`) only flips the TaskInstance to
`SCHEDULED` — it does not finalize anything. The scheduler re-queues the
task, the Executor launches a **brand-new worker process**, and that new
process calls `trigger_reentry` (the KPO `execute_complete` equivalent),
which reconnects to the existing pod (does not relaunch it), extracts logs
and XCom, runs pod-lifecycle callbacks, and *then* self-reports through the
completely normal finish path — same as any non-deferred task.
`Trigger.submit_failure`'s docstring
(`airflow-core/src/airflow/models/trigger.py`) says the quiet part outright:
*"we have to actually run the failure code from a worker as it may have
linked callbacks, so hilariously we have to re-schedule the task instance to
a worker just so it can then fail."*

So even in the resource-efficient deferred path, the Triggerer's own
observation of the pod's terminal phase is never trusted as-is — a second
worker always spins up just to say so again.

### Existing non-self-report paths are failure backstops, not primary success determination

Airflow already has places where something other than the task process sets
terminal state, but all of them are reconciliation/failure paths:

- The scheduler's heartbeat-timeout zombie sweep
  (`_purge_task_instances_without_heartbeats`,
  `airflow-core/src/airflow/jobs/scheduler_job_runner.py`) calls
  `executor.change_state(ti.key, TaskInstanceState.FAILED, ...)` directly.
- `KubernetesExecutor`'s `KubernetesJobWatcher`
  (`providers/cncf/kubernetes/.../executors/kubernetes_executor_utils.py`)
  watches the **worker pod's** phase (not a task's own child pod). For a
  `Succeeded` phase it explicitly does **not** claim the state itself — it
  looks up whatever the task already self-reported. For `Failed`/OOMKilled/
  evicted pods it *is* authoritative, but purely as a backstop for when
  self-report never happened.
- `CeleryExecutor` is the one executor that genuinely polls an external
  system (the Celery result backend,
  `providers/celery/.../executors/celery_executor.py`) as authoritative for
  *both* success and failure. This is the closest existing precedent to
  "external system is source of truth" — but the result backend is still
  populated by the Celery worker process completing the task; it's
  self-report relayed through a different transport, not an independent
  observation.

None of this is a formal interface. `BaseExecutor.sync()`
(`airflow-core/src/airflow/executors/base_executor.py`) is an empty hook —
each executor invents its own mechanism inside it. There is no
`watch_external_resource()` abstraction.

### The mechanism that already gets most of the way there

`airflow-core/src/airflow/triggers/base.py` defines `BaseTaskEndEvent`
(`TaskSuccessEvent`/`TaskFailedEvent`/`TaskSkippedEvent`) — "end the task
without resuming on worker." When a Trigger yields one of these,
`handle_event_submit`'s `BaseTaskEndEvent` registration
(`airflow-core/src/airflow/models/trigger.py:550-613`):

- sets the TaskInstance's terminal state directly
  (`task_instance.set_state(event.task_instance_state, session=session)`),
- dispatches `on_success_callback`/`on_failure_callback` via a
  `TaskCallbackRequest` through `DatabaseCallbackSink`,
- pushes any XCom values supplied on the event
  (`task_instance.xcom_push(key=key, value=value)` per entry in
  `event.xcoms`),

— all **without ever queuing a worker process**. Combined with
`start_trigger_args` (lets a task defer straight into the Triggerer at
creation time, skipping even the initial worker-side submission step —
already used by `DataprocSubmitJobDirectTrigger`, `DateTimeSensorAsync`,
`FileSensorAsync`), the primitives for "Airflow is a pure watcher, the
external system is the source of truth" already exist in the codebase.

**It is almost unused.** Repo-wide, `BaseTaskEndEvent` has exactly one
consumer: a trivial time-based trigger in
`providers/standard/triggers/temporal.py`. No external-system trigger —
`KubernetesPodTrigger`, `DatabricksExecutionTrigger`,
`DataprocSubmitJobDirectTrigger` — uses it. They all yield a plain
`TriggerEvent` and pay the "spin up a worker just to finalize" cost
described above.

### Why the Trigger layer, not a new Executor interface

An Executor is a deployment-wide singleton managing the outer worker
process/pod that runs `airflow tasks run`. It has no visibility into
resources an operator's own `execute()` decides to create (a child pod, a
Spark driver, a Databricks run) — those are operator-private today. Making
the *Executor* watch them would mean either every executor growing bespoke
per-external-system code, or inventing a resource-plugin abstraction the
executor delegates to — which is just reinventing the Trigger system. A
Trigger is already exactly "an arbitrary per-task async watcher, keyed to an
operator-declared external reference, running in one process that can watch
thousands of tasks concurrently." That's the mechanism this proposal
extends, not `BaseExecutor`.

### Adjacent, but solving a different problem

`ResumableJobMixin`/`task_state_store` (AIP-103, 3.3) and KPO's in-flight
`durable` flag (apache/airflow#69914) make **the task process's own
self-report** crash-safe: on worker death, retry reconnects to the still-running
external job instead of resubmitting. The task process still self-reports
the whole time it's alive. That is a different axis from this proposal,
which is about not needing the task process to come back and self-report
*at all* once the Triggerer has already observed a terminal outcome.

## Decision

Extend real external-system triggers — starting with `KubernetesPodTrigger`
as the concrete, worked example — to optionally finalize the task themselves
via `TaskSuccessEvent`/`TaskFailedEvent`, gated by a new opt-in operator
flag, instead of always resuming a worker through `trigger_reentry`.

### New opt-in flag

```python
# providers/cncf/kubernetes/.../operators/pod.py — KubernetesPodOperator.__init__
def __init__(
    self,
    *,
    deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
    watcher_owns_state: bool = False,  # NEW — only meaningful when deferrable=True
    **kwargs,
):
    super().__init__(**kwargs)
    self.watcher_owns_state = watcher_owns_state
```

### Sequence today vs. proposed

```text
TODAY (deferrable=True, watcher_owns_state not set)

Worker A                Triggerer                    Scheduler         Worker B
   │                        │                             │               │
   ├─ create pod            │                             │               │
   ├─ defer() ──► DEFERRED  │                             │               │
   │ (exits)                ├─ KubernetesPodTrigger.run()│               │
   │                        │   watches pod phase          │               │
   │                        ├─ pod terminal ──► TriggerEvent               │
   │                        ├─ handle_event_submit:        │               │
   │                        │   TI = SCHEDULED             │               │
   │                        │   next_method="trigger_reentry"              │
   │                        │                             ├─ re-queues ──►│
   │                        │                             │                ├─ trigger_reentry:
   │                        │                             │                │   reconnect to pod
   │                        │                             │                │   extract xcom, logs
   │                        │                             │                │   run pod callbacks
   │                        │                             │                │   self-report via
   │                        │                             │                │   Execution API
   │                        │                             │                └─ TI terminal

PROPOSED (watcher_owns_state=True)

Worker A                Triggerer                                     Scheduler
   │                        │                                             │
   ├─ create pod            │                                             │
   ├─ defer() ──► DEFERRED  │                                             │
   │ (exits)                ├─ KubernetesPodTrigger.run()                 │
   │                        │   watches pod phase                        │
   │                        ├─ pod terminal                               │
   │                        ├─ extract xcom (sync_to_async-wrapped)       │
   │                        ├─ flush remaining logs                       │
   │                        ├─ delete/keep pod per on_finish_action       │
   │                        ├─ yield TaskSuccessEvent(xcoms=...) /         │
   │                        │         TaskFailedEvent(...)                │
   │                        ├─ handle_event_submit (BaseTaskEndEvent):     │
   │                        │   TI = terminal state directly              │
   │                        │   dispatch callback via DatabaseCallbackSink│
   │                        │   push xcoms                                │
   │                        └─ done — no second worker ever runs ────────►│
```

### `KubernetesPodTrigger` sketch

```python
from airflow.triggers.base import BaseTrigger, TriggerEvent, TaskSuccessEvent, TaskFailedEvent


class KubernetesPodTrigger(BaseTrigger):
    ...

    async def run(self) -> AsyncIterator[TriggerEvent]:
        ...
        try:
            state = await self._wait_for_pod_start_within_deadline()
            if state == ContainerState.TERMINATED:
                event = await self._finish("success")
            elif state == ContainerState.FAILED:
                event = await self._finish("failed", message="pod failed")
            else:
                event = await self._wait_for_container_completion()  # now calls _finish internally
            self._fired_event = True
            yield event
            return
        except PodLaunchTimeoutException as e:
            ...  # timeout/error branches unchanged — ambiguous outcomes
            # still resume a worker rather than silently self-reporting

    async def _finish(self, status: str, *, message: str | None = None) -> TriggerEvent:
        """Terminal outcome reached. Either hand back to a worker (today's
        behavior) or finalize here and end the task directly."""
        base_payload = {
            "status": status,
            "namespace": self.pod_namespace,
            "name": self.pod_name,
            "message": message,
            **self.trigger_kwargs,
        }
        if not self.trigger_kwargs.get("_watcher_owns_state"):
            return TriggerEvent(base_payload)

        pod = await self._get_pod()
        xcoms = await self._extract_xcom_if_needed(pod)
        await self._flush_remaining_logs(pod)
        await self._cleanup_pod(pod, succeeded=(status == "success"))

        if status == "success":
            return TaskSuccessEvent(xcoms=xcoms)
        return TaskFailedEvent(xcoms=xcoms)  # xcoms usually None on failure

    async def _extract_xcom_if_needed(self, pod) -> dict[str, Any] | None:
        if not self.trigger_kwargs.get("do_xcom_push"):
            return None
        # Reuses the existing *sync* PodManager.extract_xcom via sync_to_async —
        # the same established pattern this file already uses for get_task_state().
        # No new async k8s-exec plumbing required.
        from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager

        sync_manager = PodManager(kube_client=await sync_to_async(self.hook.core_v1_client)())
        raw = await sync_to_async(sync_manager.extract_xcom)(pod)
        return {"return_value": json.loads(raw)} if raw else None

    async def _flush_remaining_logs(self, pod) -> None:
        if self.get_logs:
            await self.pod_manager.fetch_container_logs_before_current_sec(
                pod, container_name=self.base_container_name, since_time=self.last_log_time
            )
            # self.log.* inside the trigger is already routed into the TI's
            # own log file by the Triggerer's per-task logging handler — this
            # is why deferred-task logs already show up mid-poll today. No
            # new log-plumbing needed here.

    async def _cleanup_pod(self, pod, *, succeeded: bool) -> None:
        # AsyncKubernetesHook.delete_pod already exists and is already used by
        # this trigger's own on_kill/cleanup() path — reuse it rather than
        # adding a new async delete implementation.
        if self.on_finish_action == OnFinishAction.DELETE_POD or (
            self.on_finish_action == OnFinishAction.DELETE_SUCCEEDED_POD and succeeded
        ):
            await self.hook.delete_pod(pod.metadata.name, pod.metadata.namespace)
```

This sketch is deliberately simplified — a full patch would also need the
pod-GC'd/404 edge case `trigger_reentry` already handles, and
`_push_xcom_with_fan_out`'s `multiple_outputs` fan-out.

### What's reused vs. genuinely new

| Piece | Status |
|---|---|
| Async pod polling / phase detection | Already exists (`_wait_for_container_completion`) |
| Async log fetch mid-poll | Already exists (`fetch_container_logs_before_current_sec`) |
| Trigger logs landing in the TI's log store | Already true today (Triggerer's logging handler) |
| Async pod deletion | Already exists (`AsyncKubernetesHook.delete_pod`, used by `cleanup()`) |
| `TaskSuccessEvent`/`TaskFailedEvent` + direct terminal `set_state`, callback dispatch, XCom push | Already exists in core (`trigger.py:550-613`) — this proposal is the first real caller |
| Async XCom extraction | **New**, but cheap — wrap the existing sync `PodManager.extract_xcom` in `sync_to_async` rather than reimplementing the k8s exec-stream logic |

## Consequences

### Capability gains

- `@task.kubernetes` (and anything built on `KubernetesPodOperator`, e.g.
  `SparkKubernetesOperator`, which subclasses it directly and inherits the
  deferrable machinery unchanged) can complete a task entirely from the
  Triggerer's observation of the pod, with zero worker processes spun up
  after the initial pod creation.
- Gives the existing, almost entirely unused `BaseTaskEndEvent` mechanism its
  first real-world, external-system consumer.

### Compatibility

- Strictly opt-in (`watcher_owns_state=False` by default) and only
  meaningful under `deferrable=True`. No change to default behavior for any
  existing DAG.

### Blocking prerequisites (must land first)

1. **Retry-eligibility bypass.** `handle_event_submit`'s `BaseTaskEndEvent`
   branch (`trigger.py:566`) calls `task_instance.set_state(event.task_instance_state,
   session=session)` directly — not `TaskInstance.handle_failure(...)`. That
   bypasses the retry-eligibility logic `_process_executor_events`/
   `handle_failure` use elsewhere in the scheduler. Today's sole
   `BaseTaskEndEvent` consumer (a time-based trigger) never fails, so this
   has gone unnoticed. A `TaskFailedEvent` from `KubernetesPodTrigger` on an
   operator with `retries=3` would go straight to terminal `FAILED`,
   silently skipping `up_for_retry`. This needs a fix — routing
   `BaseTaskEndEvent`'s failure case through the same retry-eligibility path
   — before any retryable operator can safely adopt `watcher_owns_state`.
2. **KPO pod-lifecycle callbacks have no Trigger-side home.**
   `trigger_reentry` runs `self.callbacks`
   (`on_pod_completion`/`on_pod_teardown` — e.g. Istio-sidecar quiesce logic)
   today. Those are operator-level `KubernetesPodOperatorCallback` objects
   the Trigger doesn't currently carry or serialize. Either the Trigger
   needs to gain a (serializable) reference to them, or
   `watcher_owns_state=True` should ship documented as incompatible with
   custom pod callbacks initially.

### New ongoing costs

- A second finalization code path to maintain in `KubernetesPodTrigger`
  alongside `trigger_reentry` (until/unless `trigger_reentry` itself is
  refactored to share the finalization logic).
- `PodManager.extract_xcom` used via `sync_to_async` from the Triggerer's
  event loop briefly blocks that loop's thread pool per finalizing task —
  fine at today's typical Triggerer concurrency, but worth measuring if this
  pattern spreads to many high-volume k8s tasks.

### Out of scope (for this ADR)

- Moving pod *submission* itself off the worker via `start_trigger_args` —
  this proposal only removes the *finalization* resume; the worker still
  creates the pod before deferring. A zero-worker-at-all path is a natural
  follow-up once this lands.
- A parallel write-up for `@task.spark`. No `@task.spark` decorator exists
  today. `SparkSubmitOperator` is not deferrable (it uses
  `ResumableJobMixin`/`task_state_store` for crash-safety, not a Trigger).
  `DatabricksSubmitRunOperator` is deferrable with an architecturally
  identical `DatabricksExecutionTrigger`/`execute_complete` pattern to KPO's,
  making it the next natural candidate — but there's no running trigger
  code to sketch against in this ADR the way there is for KPO.
- Any change to `BaseExecutor`. See "Why the Trigger layer, not a new
  Executor interface" above.
