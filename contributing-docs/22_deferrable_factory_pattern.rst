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

Deferrable Factory Pattern
===========================

The Deferrable Factory Pattern is a design pattern for building robust Airflow operators that handle
long-running external workloads (e.g., AI training jobs, batch inference, distributed computing tasks).
This pattern addresses critical infrastructure instability issues that affect traditional operators when
managing external processes on Unix systems.

.. contents:: Table of Contents
   :depth: 3
   :local:

Architecture Overview
---------------------

The Deferrable Factory Pattern decomposes a long-running task into three distinct components,
each with a specific responsibility:

1. **Submit Task**: Synchronously submits the job to an external system and returns a job identifier
2. **Wait Task**: A deferrable operator that polls for job completion without occupying worker slots
3. **Cancel/Teardown Task**: Handles explicit user cancellations via Airflow's teardown mechanism

This separation ensures that infrastructure failures (worker crashes, pod evictions) do not inadvertently
terminate healthy external jobs, while still allowing users to cancel jobs through the Airflow UI.

Factory Implementation
^^^^^^^^^^^^^^^^^^^^^^

A Factory Operator is a Python function that returns a ``TaskGroup`` containing the three tasks described above:

.. code-block:: python

    def long_running_job_factory(
        task_id: str,
        job_config: dict,
        **kwargs
    ) -> TaskGroup:
        """
        Factory function that returns a TaskGroup with submit, wait, and cancel tasks.
        
        Args:
            task_id: Unique identifier for the task group
            job_config: Configuration for the external job
            
        Returns:
            TaskGroup containing submit, wait, and teardown tasks
        """
        with TaskGroup(group_id=task_id) as task_group:
            # Submit task - returns job_id via XCom
            submit = PythonOperator(
                task_id=f"{task_id}_submit",
                python_callable=submit_job,
                op_kwargs={"config": job_config},
            )
            
            # Wait task - deferrable, doesn't occupy worker slots
            wait = ExternalJobSensor(
                task_id=f"{task_id}_wait",
                job_id="{{ task_instance.xcom_pull(task_ids='" + f"{task_id}_submit" + "') }}",
                deferrable=True,
            )
            
            # Teardown task - only runs on cancellation/failure
            cancel = PythonOperator(
                task_id=f"{task_id}_cancel",
                python_callable=cancel_job,
                trigger_rule=TriggerRule.ONE_FAILED,
            ).as_teardown(setups=submit)
            
            submit >> wait
            
        return task_group

Best Practices
--------------

When implementing the Deferrable Factory Pattern, follow these best practices:

Avoid Blocking Operations in Submit Tasks
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The submit task should only initiate the job and return immediately. Do not wait for job completion
in this task:

.. code-block:: python

    # Good: Submit and return job_id immediately
    def submit_job(config):
        job_id = external_api.submit(config)
        return job_id
    
    # Bad: Blocking wait in submit task
    def submit_job_blocking(config):
        job_id = external_api.submit(config)
        while not external_api.is_complete(job_id):  # Don't do this!
            time.sleep(60)
        return job_id

Use Deferrable Operators for Waiting
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Always use deferrable operators (or custom triggers) for the wait task to avoid occupying worker slots:

.. code-block:: python

    # Good: Deferrable operator
    wait = ExternalJobSensor(
        task_id="wait_for_completion",
        job_id="{{ ti.xcom_pull(task_ids='submit_job') }}",
        deferrable=True,  # Runs in Triggerer, not Worker
        poke_interval=60,
    )
    
    # Bad: Non-deferrable sensor (occupies worker slot)
    wait = ExternalJobSensor(
        task_id="wait_for_completion",
        job_id="{{ ti.xcom_pull(task_ids='submit_job') }}",
        deferrable=False,  # Occupies worker slot for entire duration
        poke_interval=60,
    )

Implement Proper Teardown Logic
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use Airflow's ``@teardown`` decorator or ``.as_teardown()`` method to ensure cleanup happens on failures:

.. code-block:: python

    # Good: Proper teardown
    cancel = PythonOperator(
        task_id="cancel_job",
        python_callable=cancel_job,
        op_kwargs={"job_id": "{{ ti.xcom_pull(task_ids='submit_job') }}"},
    ).as_teardown(setups=submit)
    
    # Also good: Using decorator
    @task.teardown(trigger_rule=TriggerRule.ONE_FAILED)
    def cancel_job_decorator(job_id):
        external_api.cancel(job_id)

Comparison with Traditional Operators
--------------------------------------

Traditional Monolithic Operator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A traditional operator combines submission, waiting, and cancellation in a single class:

.. code-block:: python

    class TraditionalOperator(BaseOperator):
        def execute(self, context):
            # Submit job
            job_id = self.submit_job()
            
            # Wait for completion (blocks worker)
            while not self.is_complete(job_id):
                time.sleep(60)
                
            return self.get_result(job_id)
        
        def on_kill(self):
            # Problem: Called on both user cancellation AND infrastructure failures
            self.cancel_job()

**Problems:**

1. Worker slot occupied for the entire job duration (could be hours or days)
2. ``on_kill()`` cannot distinguish between user intent and infrastructure failure
3. Worker crashes result in lost job tracking
4. No resilience to pod evictions or node failures

Deferrable Factory Pattern
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The factory pattern separates concerns:

.. code-block:: python

    def job_factory(task_id: str) -> TaskGroup:
        with TaskGroup(group_id=task_id) as tg:
            submit = PythonOperator(
                task_id=f"{task_id}_submit",
                python_callable=submit_job,
            )
            
            wait = JobSensor(
                task_id=f"{task_id}_wait",
                job_id="{{ ti.xcom_pull(task_ids='" + f"{task_id}_submit" + "') }}",
                deferrable=True,  # Key: Runs in Triggerer
            )
            
            cancel = PythonOperator(
                task_id=f"{task_id}_cancel",
                python_callable=cancel_job,
            ).as_teardown(setups=submit)
            
            submit >> wait
        return tg

**Benefits:**

1. Worker slots freed immediately after submission
2. Clear separation between infrastructure failures (wait task fails, restarts polling) and user intent (teardown task executes)
3. Triggerer handles polling asynchronously
4. Resilient to worker crashes and pod evictions

Design Principles
-----------------

The Deferrable Factory Pattern is built on several key principles:

Separation of Concerns
^^^^^^^^^^^^^^^^^^^^^^^

Each task has a single, well-defined responsibility:

- **Submit**: Initiate external work
- **Wait**: Monitor external work progress
- **Cancel**: Clean up on failures or explicit cancellation

This separation prevents the conflation of infrastructure failures with user intent.

Stateless Polling
^^^^^^^^^^^^^^^^^

The wait task is stateless and can be restarted at any time without affecting the external job:

.. code-block:: python

    class ExternalJobSensor(BaseSensorOperator):
        def execute(self, context):
            # Stateless: Only reads job status, doesn't modify it
            job_id = context['ti'].xcom_pull(task_ids='submit_task')
            if self.check_job_complete(job_id):
                return True
            self.defer(
                trigger=JobCompletionTrigger(job_id=job_id),
                method_name="execute_complete",
            )
        
        def execute_complete(self, context, event):
            if event['status'] == 'complete':
                return True
            elif event['status'] == 'failed':
                raise AirflowException(f"Job failed: {event['error']}")

Non-Blocking Operations
^^^^^^^^^^^^^^^^^^^^^^^

All operations should be non-blocking to maximize worker efficiency:

- Submit task returns immediately after initiating the job
- Wait task defers to the Triggerer
- Cancel task executes quickly (just sends cancellation signal)

Examples and Use Cases
----------------------

Machine Learning Training
^^^^^^^^^^^^^^^^^^^^^^^^^

A typical ML training job that might run for hours or days:

.. code-block:: python

    from airflow.decorators import dag, task
    from airflow.utils.task_group import TaskGroup
    from datetime import datetime
    
    @dag(start_date=datetime(2024, 1, 1), schedule=None)
    def ml_training_dag():
        def training_job_factory(task_id: str, config: dict) -> TaskGroup:
            with TaskGroup(group_id=task_id) as tg:
                @task(task_id=f"{task_id}_submit")
                def submit():
                    import ml_platform
                    job_id = ml_platform.submit_training(config)
                    return job_id
                
                @task.sensor(
                    task_id=f"{task_id}_wait",
                    poke_interval=300,
                    timeout=86400,
                    mode="reschedule",
                )
                def wait(job_id):
                    import ml_platform
                    return ml_platform.is_training_complete(job_id)
                
                @task.teardown(task_id=f"{task_id}_cancel")
                def cancel(job_id):
                    import ml_platform
                    ml_platform.cancel_training(job_id)
                
                job_id = submit()
                wait(job_id)
                cancel(job_id)
            return tg
        
        # Use the factory to create training tasks
        training_job_factory("train_model", {"model": "llama-3", "gpus": 8})
    
    ml_training_dag()

Batch Data Processing
^^^^^^^^^^^^^^^^^^^^^

A long-running Spark job processing large datasets:

.. code-block:: python

    @dag(start_date=datetime(2024, 1, 1), schedule="@daily")
    def batch_processing_dag():
        def spark_job_factory(task_id: str) -> TaskGroup:
            with TaskGroup(group_id=task_id) as tg:
                submit = SparkSubmitOperator(
                    task_id=f"{task_id}_submit",
                    application="process_data.py",
                    conn_id="spark_default",
                )
                
                wait = SparkJobSensor(
                    task_id=f"{task_id}_wait",
                    job_id="{{ ti.xcom_pull(task_ids='" + f"{task_id}_submit" + "') }}",
                    deferrable=True,
                )
                
                cancel = PythonOperator(
                    task_id=f"{task_id}_cancel",
                    python_callable=cancel_spark_job,
                ).as_teardown(setups=submit)
                
                submit >> wait
            return tg
        
        spark_job_factory("daily_etl")
    
    batch_processing_dag()

Infrastructure Instability Problem
-----------------------------------

Traditional Airflow operators face critical challenges when managing long-running external jobs on Unix systems.
Understanding these problems is essential for appreciating why the Deferrable Factory Pattern is necessary.

Worker Crashes and Evictions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Problem**: In production environments, Airflow workers can crash or be evicted for various reasons:

- **Kubernetes Pod Evictions**: Nodes are drained for maintenance, resource pressure causes pod preemption
- **Spot Instance Terminations**: Cloud providers reclaim spot instances with little notice
- **Out-of-Memory (OOM) Kills**: Worker processes exceed memory limits
- **Network Partitions**: Workers lose connectivity to the scheduler
- **Node Failures**: Physical or virtual machines fail unexpectedly

When a worker crashes while executing a traditional operator that manages an external job, the operator's
state is lost, but the external job continues running.

**Impact**:

1. **Lost Tracking**: Airflow loses track of the external job_id
2. **Zombie Jobs**: External jobs become "zombies" - running but unmonitored
3. **Resource Waste**: Zombie jobs consume resources until manually discovered and terminated
4. **Failed Task Recovery**: When the task is retried, it may start a new job instead of reconnecting to the existing one

On_Kill Dilemma
^^^^^^^^^^^^^^^

**Problem**: The ``on_kill()`` method in Airflow operators is triggered when a ``SIGTERM`` signal is sent to the
worker process. However, ``SIGTERM`` is sent in multiple scenarios:

1. **User Intent**: User clicks "Mark Failed" or "Cancel" in the Airflow UI
2. **Infrastructure Events**: Kubernetes sends SIGTERM before pod eviction (15-30 second grace period)
3. **Graceful Shutdown**: Airflow scheduler requests worker shutdown for deployment updates
4. **Resource Management**: System sends SIGTERM due to resource constraints

**The Dilemma**: The operator cannot distinguish between these scenarios. This creates two equally problematic approaches:

**Option 1: Cancel Job in on_kill()**

.. code-block:: python

    class TraditionalOperator(BaseOperator):
        def on_kill(self):
            # Cancels job on ANY SIGTERM
            external_api.cancel_job(self.job_id)

**Problem**: Infrastructure failures (pod eviction, worker crash) inadvertently cancel healthy external jobs,
wasting hours of computation and resources.

**Example**: A 10-hour GPU training job is 9 hours in when Kubernetes evicts the worker pod for node maintenance.
The ``on_kill()`` method cancels the training job, destroying 9 hours of progress.

**Option 2: Do Nothing in on_kill()**

.. code-block:: python

    class TraditionalOperator(BaseOperator):
        def on_kill(self):
            # Doesn't cancel job
            pass

**Problem**: Users cannot cancel jobs through the Airflow UI. Jobs become unstoppable zombies that must be
manually terminated via CLI or external systems.

**Example**: User realizes a training job has incorrect hyperparameters, clicks "Mark Failed" in the UI,
but the job continues running for hours, wasting expensive GPU resources.

Signal Handling (Unix-Specific)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Unix Signal Ambiguity**: On Unix systems, the same signal (SIGTERM) is used for different purposes:

.. code-block:: bash

    # User cancellation (via Airflow UI)
    kill -SIGTERM <worker_pid>
    
    # Kubernetes pod eviction (grace period)
    kubectl delete pod airflow-worker-abc --grace-period=30
    # -> Sends SIGTERM, waits 30s, then SIGKILL
    
    # Graceful worker shutdown
    airflow celery worker --stop
    # -> Sends SIGTERM to all running tasks

**Process Tree Implications**: When a worker receives SIGTERM:

1. Worker process begins shutdown sequence
2. Worker sends SIGTERM to all child processes (task executors)
3. Task executors may send SIGTERM to their children (external job clients)

.. code-block:: text

    airflow-worker (PID 100)  <- SIGTERM
      └─ task executor (PID 101)  <- SIGTERM
           └─ external job client (PID 102)  <- SIGTERM?

**Challenge**: Should the external job client propagate SIGTERM to the remote job? There's no way to know
if the SIGTERM originated from user intent or infrastructure maintenance.

**Unix Signal Handlers**: Traditional operators may attempt to use signal handlers:

.. code-block:: python

    import signal
    
    class TraditionalOperator(BaseOperator):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.explicit_cancel = False
            
        def on_kill(self):
            # Still can't distinguish SIGTERM origin
            if self.explicit_cancel:  # How to set this reliably?
                external_api.cancel_job(self.job_id)

**Problem**: There's no reliable way to set ``explicit_cancel`` because:

- Signal handlers run in a restricted context
- Race conditions between signal delivery and flag checking
- Worker process may be killed before handler completes

Inconsistent State Tracking
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Problem**: When workers crash or are evicted, the task instance state in Airflow's database may not reflect
the actual state of the external job:

**Scenarios**:

1. **False Failures**: Worker crashes, task marked as failed, but external job completes successfully

   .. code-block:: text

       External Job: RUNNING -> COMPLETED (job succeeded)
       Task Instance: RUNNING -> FAILED (worker crashed)
       Result: Success ignored, task retried, duplicate job started

2. **False Successes**: Worker crashes after job fails, but before status is recorded

   .. code-block:: text

       External Job: RUNNING -> FAILED (job failed)
       Task Instance: RUNNING -> (worker crashed, task retried)
       Result: Failure undetected, task retried indefinitely

3. **Zombie Tracking**: Worker restarts, but retains stale job_id from previous execution

   .. code-block:: text

       Execution 1: Worker submits job_id=123, then crashes
       Execution 2: Worker submits job_id=456
       Result: Both jobs running, but only 456 tracked

**Root Cause**: Traditional operators maintain state in the worker process memory, which is lost on crashes.
While task instance state is persisted to the database, there's often a window between state changes where
crashes can occur.

Process Lifecycle on Unix
^^^^^^^^^^^^^^^^^^^^^^^^^^

**Unix Process States**: Understanding Unix process lifecycle is crucial:

.. code-block:: text

    RUNNING -> SIGTERM received -> STOPPING (grace period) -> SIGKILL -> TERMINATED
               (15-30 seconds)

**Airflow Worker Lifecycle**:

1. **Normal Operation**:

   .. code-block:: text

       Worker starts -> Pulls task -> Executes task -> Completes task -> Pulls next task

2. **Graceful Shutdown** (deployment update):

   .. code-block:: text

       Worker receives SIGTERM -> Finishes current task -> Shuts down cleanly

3. **Forced Eviction** (pod eviction):

   .. code-block:: text

       Worker receives SIGTERM (grace period: 30s) -> Must cleanup within 30s -> SIGKILL

**Challenge**: If a task has been running for hours and receives SIGTERM with a 30-second grace period,
it cannot:

- Wait for external job completion (would exceed grace period)
- Safely determine if it should cancel the job (no signal context)
- Persist enough state to resume on retry (30 seconds insufficient)

**Container Exit Codes**: Exit codes don't help distinguish scenarios:

.. code-block:: text

    Exit Code 0: Clean exit (but why? Task done? Worker shutdown? Job cancelled?)
    Exit Code 137 (128+9): SIGKILL (but why? OOM? Grace period expired? Node failure?)
    Exit Code 143 (128+15): SIGTERM (but why? User? Eviction? Deployment?)

Solution: Deferrable Factory Pattern
-------------------------------------

The Deferrable Factory Pattern solves infrastructure instability problems by architectural design rather than
trying to solve signal ambiguity.

How It Solves Infrastructure Instability
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**1. Worker Crashes Don't Affect External Jobs**

Because polling happens in the Triggerer (a separate, stable service), worker crashes during the wait phase
don't affect job execution:

.. code-block:: text

    Worker 1: Submits job (job_id=123) -> Crashes
    Triggerer: Continues polling job_id=123 status
    Worker 2: Wait task restarted -> Reconnects to same Triggerer polling
    Result: External job completes normally, status detected by Triggerer

**2. Clear Separation of User Intent**

The teardown task only runs when explicitly triggered by failure or cancellation:

.. code-block:: python

    cancel = PythonOperator(
        task_id="cancel_job",
        python_callable=cancel_job,
        trigger_rule=TriggerRule.ONE_FAILED,  # Only on explicit failure
    ).as_teardown(setups=submit)

**Scenarios**:

- **Worker Crash**: Wait task fails and retries, teardown NOT triggered
- **User Cancellation**: User marks task as failed, teardown IS triggered
- **Infrastructure Event**: Worker evicted, wait task reschedules, teardown NOT triggered

**3. Stateless Polling in Triggerer**

The Triggerer maintains minimal state (job_id) and polls external status:

.. code-block:: python

    class JobCompletionTrigger(BaseTrigger):
        def __init__(self, job_id: str):
            self.job_id = job_id  # Only state needed
        
        async def run(self):
            while True:
                status = await self.check_job_status(self.job_id)
                if status in ('complete', 'failed'):
                    yield TriggerEvent({"status": status})
                    return
                await asyncio.sleep(60)

**Benefits**:

- Triggerer crashes/restarts don't cancel jobs
- Multiple triggerers can poll the same job (high availability)
- No worker slots consumed during polling

**4. Unix Signal Handling Simplified**

Each task has simple, unambiguous signal handling:

**Submit Task**: Short-lived (seconds), crashes are okay (will retry submission)

.. code-block:: python

    def submit_job(config):
        # If this crashes, retry will submit again (use idempotency tokens if needed)
        job_id = external_api.submit_job(config, idempotency_token=config['token'])
        return job_id

**Wait Task**: Runs in Triggerer, doesn't receive worker SIGTERM

.. code-block:: python

    # Triggerer is a stable service, not subject to pod evictions
    # If Triggerer restarts, polling resumes automatically

**Cancel Task**: Only runs when explicitly needed, clear intent

.. code-block:: python

    def cancel_job(job_id):
        # Only called when user explicitly cancels or task group fails
        external_api.cancel_job(job_id)

Comparison: Traditional vs Factory Pattern on Unix
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Scenario: Kubernetes Pod Eviction**

**Traditional Operator**:

.. code-block:: text

    1. Worker pod receives SIGTERM (30s grace period)
    2. Worker propagates SIGTERM to task executor
    3. Task executor calls operator.on_kill()
    4. on_kill() doesn't know if it should cancel external job
    5. Option A: Cancels job -> 9 hours of training wasted
    6. Option B: Doesn't cancel -> Job becomes zombie

**Factory Pattern**:

.. code-block:: text

    1. Worker pod receives SIGTERM (30s grace period)
    2. If submit task running: Task fails, will retry (safe)
    3. If wait task running: Task fails, will retry (safe)
    4. Triggerer: Continues polling job status (unaffected)
    5. New worker pod: Wait task reconnects to Triggerer
    6. Result: External job completes, success detected

**Scenario: User Cancellation**

**Traditional Operator**:

.. code-block:: text

    1. User clicks "Mark Failed" in UI
    2. Scheduler sends SIGTERM to worker
    3. Worker calls operator.on_kill()
    4. If on_kill() cancels: Job stops (correct)
    5. If on_kill() doesn't cancel: Job continues (wrong)

**Factory Pattern**:

.. code-block:: text

    1. User clicks "Mark Failed" in UI
    2. Task marked as failed
    3. Teardown trigger rule evaluates: ONE_FAILED = True
    4. Cancel task executes: external_api.cancel_job(job_id)
    5. Result: Job cancelled as intended

Triggerer vs Worker Execution
------------------------------

Understanding the difference between Worker and Triggerer execution is crucial for the Factory Pattern.

Worker Execution Model
^^^^^^^^^^^^^^^^^^^^^^^

**Characteristics**:

- **Slot Occupancy**: Each running task occupies a worker slot (limited resource)
- **Process-Based**: Each task runs in a process (or subprocess)
- **Stateful**: Task state lives in worker memory
- **Blocking**: Worker cannot execute other tasks while one is running
- **Resource Intensive**: Consumes CPU, memory for task duration

**Unix Process Structure**:

.. code-block:: text

    Worker Process (PID 100)
      ├─ Task 1 Executor (PID 101) [OCCUPIES SLOT 1]
      ├─ Task 2 Executor (PID 102) [OCCUPIES SLOT 2]
      └─ Task 3 Executor (PID 103) [OCCUPIES SLOT 3]
      
    Max workers: 3 (slots exhausted)

**Problem for Long-Running Jobs**: If Task 1 waits for a 10-hour training job, Slot 1 is blocked for 10 hours.

Triggerer Execution Model
^^^^^^^^^^^^^^^^^^^^^^^^^^

**Characteristics**:

- **No Slot Occupancy**: Triggers don't occupy worker slots
- **Async-Based**: Runs in an asyncio event loop, thousands of triggers per process
- **Stateless**: Minimal state (job_id, poll interval), easily recoverable
- **Non-Blocking**: Can monitor thousands of jobs concurrently
- **Resource Efficient**: Low CPU and memory footprint per trigger

**Async Event Loop Structure**:

.. code-block:: text

    Triggerer Process (PID 200)
      └─ Asyncio Event Loop
           ├─ Trigger 1: Polling job_id=123 every 60s
           ├─ Trigger 2: Polling job_id=456 every 60s
           ├─ Trigger 3: Polling job_id=789 every 60s
           └─ ... thousands more triggers
           
    Worker slots: 0 (triggers don't consume slots)

**Benefit**: Monitoring 1000 jobs requires:

- **Worker Execution**: 1000 worker slots (infeasible)
- **Triggerer Execution**: 1 triggerer process (lightweight)

Unix Process Comparison
^^^^^^^^^^^^^^^^^^^^^^^^

**Worker Task Process** (long-running):

.. code-block:: bash

    # Worker task process
    $ ps aux | grep task
    airflow  101  80.0  5.0  500M  task-executor --task-id=train_model
    
    # Long-lived, high resource usage, blocks slot

**Triggerer Async Task** (lightweight):

.. code-block:: bash

    # Triggerer process
    $ ps aux | grep triggerer
    airflow  200  0.5  0.2  50M  airflow-triggerer
    
    # Many triggers in one process, low resource usage

**System Signals**:

- **Worker**: Receives signals from scheduler, Kubernetes, system

  .. code-block:: bash

      kill -SIGTERM 101  # Ambiguous: User? System? Eviction?

- **Triggerer**: Service process, not subject to per-task signals

  .. code-block:: bash

      # Triggerer restarts gracefully, triggers resume polling

Deferral Mechanism on Unix
^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Deferral Process**:

1. **Worker Executes Operator**: Operator's ``execute()`` method runs on worker

   .. code-block:: python

       def execute(self, context):
           job_id = context['ti'].xcom_pull(task_ids='submit_job')
           self.defer(
               trigger=JobCompletionTrigger(job_id=job_id),
               method_name="execute_complete",
           )

2. **Operator Defers**: Raises ``TaskDeferred`` exception with trigger info

   .. code-block:: python

       # Internal Airflow behavior
       raise TaskDeferred(trigger=trigger, method_name="execute_complete")

3. **Worker Releases Slot**: Task instance state set to "deferred", worker slot freed

   .. code-block:: text

       Worker Slot: OCCUPIED -> FREED
       Task State: running -> deferred
       Trigger: Registered with Triggerer

4. **Triggerer Polls**: Async polling begins in Triggerer event loop

   .. code-block:: python

       # In Triggerer process
       async def run_trigger(trigger):
           async for event in trigger.run():
               mark_task_for_resume(event)

5. **Trigger Fires**: When condition met, trigger emits event

   .. code-block:: python

       # Trigger emits event
       yield TriggerEvent({"status": "complete", "job_id": job_id})

6. **Worker Resumes Task**: Task scheduled back to worker, ``execute_complete()`` called

   .. code-block:: python

       def execute_complete(self, context, event):
           if event['status'] == 'complete':
               return "Job completed successfully"

**Unix Process Lifecycle**:

.. code-block:: text

    Worker Process:
    ├─ execute() runs [OCCUPIES SLOT] (2 seconds)
    ├─ defer() called -> Slot released [FREES SLOT]
    └─ execute_complete() runs [OCCUPIES SLOT] (2 seconds)
    
    Total slot occupancy: 4 seconds (instead of 10 hours)

Resilience to Unix System Events
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Node Drain / Maintenance**:

.. code-block:: bash

    # Kubernetes node drain
    kubectl drain node-1 --ignore-daemonsets --delete-emptydir-data
    
    # Worker pods on node-1 evicted
    # Triggers continue on other triggerer instances
    # Wait tasks reschedule on different nodes

**Triggerer High Availability**:

.. code-block:: text

    Triggerer-1 (node-1): Monitoring triggers 1-500
    Triggerer-2 (node-2): Monitoring triggers 501-1000
    Triggerer-3 (node-3): Hot standby
    
    Node-1 fails:
    - Triggerer-1 terminates
    - Triggerer-2 and Triggerer-3 redistribute triggers 1-500
    - No jobs affected, polling continues

**Worker Pool Scaling**:

.. code-block:: text

    # Scale down workers (deployments, cost optimization)
    kubectl scale deployment airflow-worker --replicas=5
    
    Traditional Operators:
    - 100 long-running tasks x 3 workers = Only 3 jobs monitored
    - Scaling down kills 2 workers, jobs may be cancelled
    
    Factory Pattern:
    - 100 jobs deferred to Triggerer
    - Workers freed immediately
    - Scaling down has no impact on monitoring

Summary: Why Factory Pattern on Unix
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Deferrable Factory Pattern is particularly important on Unix-based systems (Linux, Kubernetes) because:

1. **Signal Handling**: Unix signals (SIGTERM, SIGKILL) are ambiguous and cannot reliably distinguish
   between user intent and infrastructure events.

2. **Process Lifecycle**: Container orchestrators (Kubernetes, Docker Swarm) aggressively manage pod lifecycles,
   frequently sending SIGTERM for evictions, drains, and scaling operations.

3. **Resource Efficiency**: Unix systems have finite process limits and resources. The Factory Pattern's
   triggerer model aligns with Unix's async I/O model (epoll, kqueue) for efficient waiting.

4. **Fault Tolerance**: Unix systems (especially cloud infrastructure) fail frequently. The Factory Pattern's
   architecture naturally handles process crashes, network partitions, and node failures.

5. **Observability**: Separating submission, waiting, and cancellation provides clear Unix process boundaries
   and audit trails in system logs.

Implementation Guidelines
-------------------------

Creating a Factory Operator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Follow these steps to create a factory operator for your external system:

**Step 1: Define the Job Submission Function**

.. code-block:: python

    def submit_external_job(config: dict) -> str:
        """
        Submit a job to external system and return job_id.
        
        Should be idempotent (use idempotency tokens if possible).
        Should return quickly (no waiting).
        """
        import external_api
        
        job_id = external_api.submit(
            config=config,
            idempotency_token=config.get('token')  # For retry safety
        )
        return job_id

**Step 2: Implement a Custom Trigger**

.. code-block:: python

    from airflow.triggers.base import BaseTrigger, TriggerEvent
    import asyncio
    
    class ExternalJobTrigger(BaseTrigger):
        """Trigger that polls for external job completion."""
        
        def __init__(self, job_id: str, poll_interval: int = 60):
            super().__init__()
            self.job_id = job_id
            self.poll_interval = poll_interval
        
        def serialize(self):
            """Serialize trigger for storage."""
            return {
                "job_id": self.job_id,
                "poll_interval": self.poll_interval,
            }
        
        async def run(self):
            """Poll for job completion."""
            import external_api
            
            while True:
                status = await external_api.get_job_status(self.job_id)
                
                if status['state'] == 'complete':
                    yield TriggerEvent({
                        "status": "complete",
                        "job_id": self.job_id,
                        "result": status.get('result'),
                    })
                    return
                elif status['state'] == 'failed':
                    yield TriggerEvent({
                        "status": "failed",
                        "job_id": self.job_id,
                        "error": status.get('error'),
                    })
                    return
                
                await asyncio.sleep(self.poll_interval)

**Step 3: Create a Deferrable Wait Operator**

.. code-block:: python

    from airflow.sensors.base import BaseSensorOperator
    from airflow.exceptions import AirflowException
    
    class ExternalJobSensor(BaseSensorOperator):
        """Sensor that waits for external job completion."""
        
        def __init__(
            self,
            job_id: str,
            poll_interval: int = 60,
            deferrable: bool = True,
            **kwargs
        ):
            super().__init__(**kwargs)
            self.job_id = job_id
            self.poll_interval = poll_interval
            self.deferrable = deferrable
        
        def execute(self, context):
            """Defer immediately to trigger."""
            if self.deferrable:
                self.defer(
                    trigger=ExternalJobTrigger(
                        job_id=self.job_id,
                        poll_interval=self.poll_interval,
                    ),
                    method_name="execute_complete",
                )
            else:
                # Fallback: blocking poll (not recommended)
                return self.poke(context)
        
        def execute_complete(self, context, event):
            """Handle trigger event."""
            if event['status'] == 'complete':
                self.log.info(f"Job {event['job_id']} completed successfully")
                return event.get('result')
            elif event['status'] == 'failed':
                raise AirflowException(f"Job {event['job_id']} failed: {event['error']}")
        
        def poke(self, context):
            """Fallback synchronous check."""
            import external_api
            status = external_api.get_job_status(self.job_id)
            return status['state'] == 'complete'

**Step 4: Create the Factory Function**

.. code-block:: python

    from airflow.utils.task_group import TaskGroup
    from airflow.operators.python import PythonOperator
    from airflow.utils.trigger_rule import TriggerRule
    
    def external_job_factory(
        task_id: str,
        job_config: dict,
        poll_interval: int = 60,
        **kwargs
    ) -> TaskGroup:
        """
        Factory that creates a TaskGroup for managing an external job.
        
        Returns a TaskGroup with:
        - submit task: Submits job and returns job_id
        - wait task: Defers to trigger for polling
        - cancel task: Teardown that cancels on failure
        """
        with TaskGroup(group_id=task_id) as task_group:
            # Submit task
            submit = PythonOperator(
                task_id=f"{task_id}_submit",
                python_callable=submit_external_job,
                op_kwargs={"config": job_config},
            )
            
            # Wait task (deferrable)
            wait = ExternalJobSensor(
                task_id=f"{task_id}_wait",
                job_id="{{ task_instance.xcom_pull(task_ids='" + f"{task_id}_submit" + "') }}",
                poll_interval=poll_interval,
                deferrable=True,
            )
            
            # Cancel task (teardown)
            cancel = PythonOperator(
                task_id=f"{task_id}_cancel",
                python_callable=lambda job_id: external_api.cancel_job(job_id),
                op_kwargs={"job_id": "{{ task_instance.xcom_pull(task_ids='" + f"{task_id}_submit" + "') }}"},
                trigger_rule=TriggerRule.ONE_FAILED,
            ).as_teardown(setups=submit)
            
            submit >> wait
        
        return task_group

**Step 5: Use the Factory in a DAG**

.. code-block:: python

    from airflow.decorators import dag
    from datetime import datetime
    
    @dag(start_date=datetime(2024, 1, 1), schedule=None)
    def my_external_job_dag():
        job_task = external_job_factory(
            task_id="process_data",
            job_config={
                "input": "s3://bucket/input.csv",
                "output": "s3://bucket/output.csv",
                "token": "{{ run_id }}",  # Idempotency token
            },
            poll_interval=30,
        )
    
    my_external_job_dag()

Testing Factory Operators
^^^^^^^^^^^^^^^^^^^^^^^^^^

**Unit Tests**: Test each component independently:

.. code-block:: python

    import pytest
    from unittest.mock import Mock, patch
    
    def test_submit_external_job():
        """Test job submission returns job_id."""
        config = {"input": "test.csv"}
        with patch('external_api.submit') as mock_submit:
            mock_submit.return_value = "job-123"
            job_id = submit_external_job(config)
            assert job_id == "job-123"
    
    @pytest.mark.asyncio
    async def test_external_job_trigger_success():
        """Test trigger emits completion event."""
        trigger = ExternalJobTrigger(job_id="job-123")
        with patch('external_api.get_job_status') as mock_status:
            mock_status.return_value = {"state": "complete", "result": "success"}
            
            async for event in trigger.run():
                assert event.payload['status'] == 'complete'
                assert event.payload['job_id'] == 'job-123'
    
    def test_factory_creates_taskgroup():
        """Test factory returns TaskGroup with correct tasks."""
        task_group = external_job_factory(
            task_id="test_job",
            job_config={"input": "test.csv"},
        )
        
        assert isinstance(task_group, TaskGroup)
        assert "test_job_submit" in [t.task_id for t in task_group.children.values()]
        assert "test_job_wait" in [t.task_id for t in task_group.children.values()]
        assert "test_job_cancel" in [t.task_id for t in task_group.children.values()]

**Integration Tests**: Test the full DAG execution:

.. code-block:: python

    from airflow.models import DagBag
    from airflow.utils.state import DagRunState
    from datetime import datetime
    
    def test_external_job_dag_execution():
        """Test full DAG execution with factory pattern."""
        dagbag = DagBag(dag_folder="dags/", include_examples=False)
        dag = dagbag.get_dag("my_external_job_dag")
        
        # Test DAG structure
        assert dag is not None
        assert len(dag.tasks) == 3  # submit, wait, cancel
        
        # Test execution (requires test environment)
        with patch('external_api.submit') as mock_submit, \
             patch('external_api.get_job_status') as mock_status:
            
            mock_submit.return_value = "test-job-123"
            mock_status.return_value = {"state": "complete"}
            
            dag.test(
                execution_date=datetime(2024, 1, 1),
                run_conf={"input": "test.csv"},
            )

Monitoring and Debugging
^^^^^^^^^^^^^^^^^^^^^^^^^

**Trigger Monitoring**: Use Airflow's trigger monitoring views:

.. code-block:: bash

    # List active triggers
    airflow triggers list
    
    # View trigger details
    airflow triggers details --trigger-id 123
    
    # Triggerer logs
    airflow triggerer logs

**Task Instance Logs**: Each component has separate logs:

.. code-block:: text

    Task: my_job_submit
    Logs: Shows job submission details, returned job_id
    
    Task: my_job_wait
    Logs: Shows deferral message, trigger events, completion status
    
    Task: my_job_cancel
    Logs: Only appears if task group fails (teardown triggered)

**Debugging Worker Crashes**: Use Kubernetes logs and events:

.. code-block:: bash

    # Pod logs (before eviction)
    kubectl logs airflow-worker-abc --previous
    
    # Pod events
    kubectl describe pod airflow-worker-abc
    
    # Check for eviction reason
    kubectl get events --field-selector involvedObject.name=airflow-worker-abc

**Debugging Trigger Issues**: Check Triggerer service health:

.. code-block:: bash

    # Triggerer health
    kubectl logs airflow-triggerer-0
    
    # Check trigger event queue
    airflow db check-triggers

Migration Guide
^^^^^^^^^^^^^^^

**Converting Existing Operators**: Migrate from traditional to factory pattern:

**Before (Traditional)**:

.. code-block:: python

    class MyJobOperator(BaseOperator):
        def execute(self, context):
            job_id = self.submit_job()
            while not self.is_complete(job_id):
                time.sleep(60)
            return self.get_result(job_id)
        
        def on_kill(self):
            self.cancel_job()  # Ambiguous!

**After (Factory Pattern)**:

.. code-block:: python

    def my_job_factory(task_id: str, config: dict) -> TaskGroup:
        with TaskGroup(group_id=task_id) as tg:
            submit = PythonOperator(
                task_id=f"{task_id}_submit",
                python_callable=submit_job,
                op_kwargs={"config": config},
            )
            wait = MyJobSensor(
                task_id=f"{task_id}_wait",
                job_id="{{ ti.xcom_pull(task_ids='" + f"{task_id}_submit" + "') }}",
                deferrable=True,
            )
            cancel = PythonOperator(
                task_id=f"{task_id}_cancel",
                python_callable=cancel_job,
            ).as_teardown(setups=submit)
            
            submit >> wait
        return tg

**DAG Migration**: Update DAG to use factory:

**Before**:

.. code-block:: python

    @dag(start_date=datetime(2024, 1, 1))
    def my_dag():
        job = MyJobOperator(task_id="process_data", config={...})

**After**:

.. code-block:: python

    @dag(start_date=datetime(2024, 1, 1))
    def my_dag():
        job = my_job_factory(task_id="process_data", config={...})

Limitations and Considerations
-------------------------------

Resource Requirements
^^^^^^^^^^^^^^^^^^^^^

**Triggerer Service**: Requires a separate Triggerer service to be running:

.. code-block:: yaml

    # Kubernetes deployment
    apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: airflow-triggerer
    spec:
      replicas: 2  # For high availability
      template:
        spec:
          containers:
          - name: triggerer
            image: apache/airflow:2.7.0
            command: ["airflow", "triggerer"]

**Resource Scaling**: Plan triggerer capacity based on job count:

.. code-block:: text

    Capacity Planning:
    - 1 Triggerer: ~1000 concurrent triggers (low poll frequency)
    - 2 Triggerers: ~2000 concurrent triggers (high availability)
    - Poll interval impacts: 10s interval = more CPU than 60s interval

Increased Task Count
^^^^^^^^^^^^^^^^^^^^

Factory pattern creates 3 tasks per logical operation:

.. code-block:: text

    Traditional: 1 job = 1 task
    Factory: 1 job = 3 tasks (submit, wait, cancel)
    
    Impact on Database:
    - More task instances in DB
    - More XCom entries (job_id passing)
    - Longer scheduler loop times for large DAGs

**Mitigation**: Use task instance cleanup policies:

.. code-block:: python

    # In airflow.cfg
    [core]
    max_tis_per_query = 512
    
    [scheduler]
    task_instance_mutation_hook = my_cleanup_hook

Trigger Serialization
^^^^^^^^^^^^^^^^^^^^^

Triggers must be serializable (stored in database):

.. code-block:: python

    class MyTrigger(BaseTrigger):
        def __init__(self, job_id: str, client_config: dict):
            self.job_id = job_id
            self.client_config = client_config  # Must be JSON-serializable
        
        def serialize(self):
            return {
                "job_id": self.job_id,
                "client_config": self.client_config,  # Must be dict/list/str/int/float
            }

**Limitation**: Cannot pass complex objects (connections, hooks) to triggers directly.

**Workaround**: Pass connection IDs and reconstruct in trigger:

.. code-block:: python

    class MyTrigger(BaseTrigger):
        def __init__(self, job_id: str, conn_id: str):
            self.job_id = job_id
            self.conn_id = conn_id  # Pass ID, not connection object
        
        async def run(self):
            # Reconstruct connection in trigger
            hook = MyHook(conn_id=self.conn_id)
            status = await hook.get_job_status(self.job_id)

References
----------

Related Airflow Documentation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- `Deferrable Operators <https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html>`_
- `Custom Triggers <https://airflow.apache.org/docs/apache-airflow/stable/howto/custom-trigger.html>`_
- `TaskGroups <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#taskgroups>`_
- `Teardown Tasks <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#teardowns>`_

Community Discussions
^^^^^^^^^^^^^^^^^^^^^

- `Apache Mailing List: Worker crashes shouldn't restart healthy jobs <https://lists.apache.org/thread/g4jhd0vz71x6jm4z8p6hjv9z7rmtb3jk>`_
- `Airflow Wiki: Persist XCom Through Retry <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=399278333>`_

Related Patterns
^^^^^^^^^^^^^^^^

- **Saga Pattern**: Distributed transactions with compensating actions (similar to teardown tasks)
- **Polling Pattern**: Async polling without blocking resources (triggerer model)
- **Circuit Breaker**: Fault tolerance for external systems (applicable to job submissions)

Glossary
--------

**Deferrable Operator**: An operator that suspends itself and frees the worker slot while waiting for
an external event, resuming when the event occurs.

**Factory Pattern**: A design pattern where a function creates and returns a configured object or group
of objects. In this context, a factory function that returns a TaskGroup.

**Infrastructure Instability**: The phenomenon where Airflow workers crash, are evicted, or fail due
to infrastructure issues (not application logic errors).

**on_kill() Dilemma**: The problem where the ``on_kill()`` method cannot distinguish between user-initiated
cancellation and infrastructure-triggered termination.

**SIGTERM**: Unix signal for process termination. Sent by systems for graceful shutdown before SIGKILL.

**Task Group**: A grouping of tasks in Airflow that provides organizational structure and allows tasks
to be treated as a logical unit.

**Teardown Task**: A task that runs cleanup logic when its associated setup tasks fail or are cancelled.
Implemented via Airflow's ``@teardown`` decorator or ``.as_teardown()`` method.

**Trigger**: A lightweight, asynchronous unit of work that runs in the Triggerer service and polls for
external events without occupying worker slots.

**Triggerer**: An Airflow service that runs Triggers in an asyncio event loop. Separate from the Scheduler
and Workers.

**Worker Slot**: A limited resource representing the capacity to execute one task. Traditional operators
occupy a slot for their entire execution duration.

**XCom**: Cross-communication mechanism in Airflow for passing data between tasks (e.g., job_id from
submit to wait task).

**Zombie Job**: An external job that continues running after Airflow has lost track of it (e.g., due to
worker crash without cleanup).
