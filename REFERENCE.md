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

# SkyPilot Provider for Apache Airflow 3.1+ Design Document

This document outlines the design and implementation plan for a comprehensive SkyPilot Provider for Apache Airflow 3.1+. The goal is to enable users to seamlessly leverage SkyPilot's powerful cloud infrastructure management capabilities directly within Airflow, while adhering to Airflow's best practices for long-running asynchronous workloads.

## Motivation & Use Cases

This integration brings the synergy of **Airflow (Orchestration)** and **SkyPilot (Compute Abstraction)** to solve critical challenges in MLOps and data engineering workflows, especially for AI workloads that require dynamic cloud resource management.

### 1. Cost Optimization & Market Arbitrage
*   **Automatic Cloud Arbitrage**: Dynamically routes tasks to the cheapest cloud provider (AWS, GCP, Azure, or specialized clouds like Lambda/RunPod) at runtime without changing the DAG code.
*   **Managed Spot Instance Pipelines**: Safely utilizes Spot/Preemptible instances (saving ~70-80%) for long-running batch jobs. SkyPilot handles the auto-recovery of preempted nodes, while Airflow manages the overall workflow state.

### 2. High Availability & GPU Scarcity Mitigation
*   **Zero-Touch Failover**: Eliminates bottlenecks caused by GPU shortages (e.g., H100 scarcity) in a specific region or cloud. If the primary cloud is out of stock, the system automatically fails over to secondary providers.
*   **Vendor Agnostic Operations**: Decouples the ML pipeline logic from the underlying infrastructure, preventing vendor lock-in and allowing the organization to pivot providers instantly.

### 3. Granular Resource "Right-Sizing"
*   **Heterogeneous Hardware DAGs**: Optimizes spend by assigning the exact hardware required for each step of the pipeline (e.g., High-RAM CPU for Spark/Preprocessing → H100 GPU for Training → T4 GPU for Inference).
*   **Just-in-Time Provisioning**: Automatically spins up expensive GPU clusters only when the specific Airflow task is ready to run and terminates them immediately upon completion, eliminating idle costs.

### 4. Hybrid Cloud & Data Sovereignty
*   **On-Premise to Cloud Bursting**: Maintains sensitive data processing or lightweight tasks on local/on-prem clusters (via Airflow) while "bursting" to the public cloud solely for heavy compute tasks via SkyPilot.
*   **Unified Interface**: Provides a single control plane (Airflow) to manage workloads across both on-premise data centers and multiple public clouds.

### 5. Automated Developer Efficiency (Platform Engineering)
*   **Ephemeral Dev Environments**: Automates the lifecycle of development clusters based on work schedules (e.g., auto-start at 9 AM, auto-stop at 7 PM), preventing "zombie" instances from draining the budget.
*   **Self-Service ML Platform**: Enables a "Model-as-a-Service" internal platform where data scientists can trigger complex training or fine-tuning jobs (like LLMs) via Airflow parameters without needing to manage Kubernetes or Terraform.

### 6. Reproducibility & Governance
*   **Infrastructure Versioning**: Encapsulates the compute environment (Docker image, CUDA version, setup commands) alongside the pipeline logic. Re-running an old DAG guarantees the exact same hardware environment, aiding in compliance and research reproducibility.

## 2. Design Strategy: Handling Long-Running AI Workloads

Most SkyPilot workloads (AI training, batch inference) are long-running external processes.

### 2.1 The "Infrastructure Instability" Problem
Standard Airflow Operators (like `PythonOperator` or `KubernetesPodOperator`) running on standard Executors (Local, Celery, Kubernetes) face specific challenges when handling long-running remote jobs:
*   **Worker Crashes & Evictions**: If the Airflow worker executing the task crashes (or is evicted by K8s), the task fails. If the operator's `on_kill` logic handles cleanup, the remote job might be killed unnecessarily. If it doesn't, the job creates a "zombie" that Airflow loses track of.
*   **Ambiguous Signals**: Airflow signals (`SIGTERM`) do not distinguish between "User click cancel" and "System shutdown", making it hard to implement safe cancellation logic inside a monolithic operator.
*   **Inconsistent State Tracking**: Which might cause the long running external job is still running but Airflow TaskInstance was marked as failed, or vice versa.
*   See **Appendix A** for a detailed analysis of the `on_kill` dilemma.

### 2.2 Implementation Strategy: The Factory Pattern
To achieve robust resilience while supporting user cancellation, we implement a **Factory Operator** (returning a `TaskGroup`) composed of three distinct tasks. This isolates the polling logic from the submission and cancellation logic.

*   **1. Submit Task**: Synchronously submits the job to SkyPilot and returns the `job_id`.
*   **2. Wait Trigger**: A generic Deferrable Operator that simply waits for the external job/process to complete. Since polling happens in the Triggerer Service, it is resilient to Worker crashes. (**Triggerer itself crashes will not impact the job at all; it will simply retry polling later.**)
*   **3. Cancel Teardown**: A `@teardown` task that specifically handles cleanup (`sky jobs cancel`). It is invoked when the `TaskGroup` fails or is manually cancelled by the user.

This approach ensures that **Infrastructure Instability** (e.g., worker crash) does not kill the job (since the TaskInstances reside in Triggerer instead of Worker), while **User Intent** (UI Cancellation) is correctly handled by the teardown task.

![Factory Pattern Diagram](factory_pattern_dag-graph.png)

## 3. Core Components

### 3.1 Operators (The Primary Interface)

We support multiple lifecycle modes via the `execution_mode` parameter:

1.  **Dedicated / Long-Lived Cluster**: Submit to manually managed clusters (`sky launch`): `execution_mode="cluster_job"`.
2.  **Pool**: Submit to a pre-warmed pool (`sky jobs launch -p <pool-name>`): `execution_mode="pool"`.
3.  **Serverless (Per-Run & Per-Task)**: `@task.skypilot` decorator with `execution_mode="managed_job"` or managed via Cluster Context Manager: `SkyPilotClusterContext`

*   **Factory Operators**: Return a `TaskGroup` with submission, waiting, and cancellation tasks, suitable for AI workloads.
    *   These operators will return a `TaskGroup` containing:
        * `{task_id}_submit_job`: Submits the job and returns `job_id` as XCom then passes it to the wait task.
        * `{task_id}_wait_for_completion`: Deferrable Operator that waits for job completion.
            *   **`SkypilotJobTrigger`**: The Deferrable Trigger that polls the SkyPilot API for job status until completion.
        * `{task_id}_cancel_job`: Teardown task that cancels the job if user explicitly cancel the task on UI.
    *   **`BaseSkyPilotJobOperator`**: base factory interface with shared parameters for all SkyPilot job operators.
        *   **Parameters**:
            *   `sky_config` (dict): SkyPilot yaml configuration as a dictionary.
            *   `execution_mode` (str, Enum): `managed_job` | `cluster_job` | `pool`
            *   `cluster_name` (str, optional): Name of the SkyPilot cluster (for `cluster_job` mode).
            *   `pool_name` (str, optional): Name of the SkyPilot pool (for `pool` mode).
            *   `cluster_context_ref` (`ClusterContextReference`, optional): Reference to a `SkyPilotClusterContext` for using shared clusters.
            *   `poll_interval` (int): Frequency of status checks.
            *   `enable_logs` (bool): Whether to stream logs from SkyPilot to Airflow. (Airflow will store the SkyPilot logs again, but this provides real-time visibility on the Airflow UI.)
    *   **`SkypilotJobOperator`**: Submits a managed job.
        *   **Parameters**:
            *   `python_callable` (callable): Python function to execute as the job payload.
            *   will omit the `run` field from `sky_config` and use the provided callable instead.
    *   **`SkypilotJobCommandOperator`**: Submits a job with a custom command.
        *   **Parameters**:
            *   `command` (str | Tuple[str,str] | `sky.Task`): Command to execute as the job payload.
                *   str: use `textwrap.dedent` to format multi-line commands.
                *   Tuple[str,str]: (setup_commands, run_commands) pair.
                *   `sky.Task` instance: allows users to define a task from yaml or programmatically define a task with further customization.
            * will omit the `run` field from `sky_config` and use the provided command instead.
*   **Normal Operators**: Same as other common Airflow operators instead of factory function returning TaskGroup.
    *   **`SkypilotCreateJobOperator`**: Submits a SkyPilot job and returns the `job_id`.
    *   **`SkypilotCancelJobOperator`**: Explicitly cancels a running job.
    *   **`SkypilotJobGroupOperator`**: (optional, as we can implement the parallel execution in Airflow) For executing job groups in parallel.
    *   **`SkypilotClusterSetupOperator` / `SkypilotTeardownOperator`**: For explicit cluster lifecycle management.
*   **Context Manger Operators**:
    *   **`SkyPilotClusterContext`**: Context manager for automatic cluster setup/teardown around tasks.
        *   Wraps Airflow's `@setup` and `@teardown` decorators with `SkyPilotClusterSetupOperator` and `SkypilotTeardownOperator` for just-in-time cluster provisioning.


### 3.2 Decorators (`@task.skypilot`, `@task.skypilot_cmd`)

Wraps around the Factory Operators to support the TaskFlow API.
*   The decorated function body is treated as the payload for the SkyPilot job.
*   Uses `SkypilotJobOperator` and `SkypilotJobCommandOperator` internally to ensure efficient, stable, deferrable execution.

### 3.3 Executor (`SkyPilotExecutor`)

**Important Note!!!**
Even if we implement a `SkyPilotExecutor`, we **still will encounter the "infrastructure instability" problem** described in Section 2.1 if we run long-running SkyPilot jobs directly on the Executor. To really resolve this, we would need to refactor the Airflow TaskSDK runtime or implement custom heartbeat and signal handling in the SkyPilot runtime, which is out of scope for this initial integration.

Therefore, **we recommend using the Factory Operators and Decorators for AI workloads instead of relying on the Executor.**

> **Comparison of Approaches:**
> *   **`@task.skypilot` (Decorator/Factory)**: Executed by standard executors (Local/Celery/K8s) and Triggerer but decomposes into 3 tasks (submit, wait, teardown). **Recommended for AI Workloads** as it naturally handles the `on_kill` instability problem via the architecture itself.
> *   **`@task(executor="SkypilotExecutor")`**: Runs the python callable directly on the SkyPilot executor. While simpler, it suffers from the "fire-and-monitor" pitfalls. If the Airflow infrastructure sends a SIGTERM (e.g., during recovery), **it might inadvertently kill the remote job depending on how the executor handles heartbeats and signals.**

While Factory Operators are preferred for efficiency and stability for AI workloads, a `SkyPilotExecutor` is still valuable for:
*   Users who want *all* tasks in a DAG to inherently run on SkyPilot without changing task definitions.
*   Short-lived tasks where slot occupation is acceptable.

**Research TODO for Executor:**
*   Investigate SIGTERM handling in SkyPilot runtime.
*   Verify cross-cloud disaster recovery impacts on Airflow Executor, TaskInstance, Workload status.
*   Check SkyPilot checkpoint compatibility with standard PythonOperator execution.


### 3.5 UI & Integration
*   **Connection Type**: Custom Airflow Connection Form for SkyPilot API credentials.
*   **Operator Extra Links**:
    *   *Open in VS Code*
    *   *Open Cluster Page*
    *   *Open Job Page*
*   **External View**: `external_view_with_metadata` for rich context.

## 4. Example Usage

### 4.1 Use `@task.skypilot` Decorators for common AI Workloads

The following example demonstrates a typical AI workflow using the `@task.skypilot` and `@task.skypilot_cmd` decorators to define tasks with varying resource requirements. Each task is automatically submitted to SkyPilot with the specified configuration, and the underlying Factory Operator handles submission, waiting, and teardown for resilience execution.

```python
import textwrap
from airflow.sdk import dag, task


@dag(
    dag_id="skypilot_taskflow_example",
    schedule=None,
    start_date=None,
    catchup=False,
)
def skypilot_taskflow_example():
    # Each task will be submitted to SkyPilot with different resource requirements
    # and will be TaskGroup with 3 sub-tasks (submit, wait, teardown) under the hood
    @task.skypilot_cmd(
        sky_config={"resources": {"cpu": 16, "memory": "64GiB"}},
        execution_mode="managed_job",
        executor="LocalExecutor",  # This can be any standard executor since the actual waiting happens in Triggerer
    )
    def data_preprocessing():
        # for user who prefer to define the command in yaml
        # we can also support returning a sky.Task object from the decorated function
        return sky.Task.from_yaml("/path/to/data_preprocessing.yaml")

    @task.skypilot_cmd(
        sky_config={"resources": {"accelerators": {"H100": 1}}},
        execution_mode="cluster_job",
        # we must specify the cluster name if we choose `cluster_job` execution mode
        cluster_name="training-cluster",
        executor="KubernetesExecutor",  # This can be any standard executor since the actual waiting happens in Triggerer
    )
    def train_model():
        # Or users can define the commands to run directly in the function body and return as a string
        # Same as the root `run` field in sky yaml config
        return textwrap.dedent(
            """
            echo "Running batch inference on SkyPilot with A100x4 cluster"
            python run_batch_inference.py --input data.csv --output results.csv
            """
        )

    @task.skypilot_cmd(
        sky_config={"resources": {"accelerators": {"T4": 1}}},
        execution_mode="pool",
        # we must specify the pool name if we choose `pool` execution mode
        pool_name="inference-pool",
        executor="CeleryExecutor",
    )
    def define_both_setup_and_run_commands():
        # For more complex scenarios where users want to define both setup and run commands,
        # we can support returning a tuple of (setup_commands, run_commands) as strings.
        setup_commands = textwrap.dedent(
            """
            echo "Setting up environment for batch inference"
            pip install -r requirements.txt
            """
        )
        run_commands = textwrap.dedent(
            """
            echo "Running batch inference on SkyPilot with T4 pool"
            python run_batch_inference.py --input data.csv --output results.csv
            """
        )
        return setup_commands, run_commands

    @task.skypilot(
        sky_config={"resources": {"accelerators": {"A100": 4}}},
    )
    def batch_inference():
        # Or users can directly return a python callable with `@task.skypilot` decorator,
        # and the function body will be executed by SkyPilot
        from my_company.inference import run_batch_inference

        print("Starting batch inference job on SkyPilot A100x4 cluster")
        run_batch_inference()

    data_preprocessing() >> train_model() >> batch_inference()


skypilot_taskflow_example()
```

### 4.2 `SkyPilotClusterContext` for using a shared cluster across multiple tasks

Wraps Airflow's `@setup` and `@teardown` to provision resources just-in-time for set of tasks. This is ideal for AI workloads that require specific cluster configurations for multiple tasks within the same cluster without provisioning separate clusters for each task.

```python
@dag(
    dag_id="skypilot_cluster_context_example",
    schedule=None,
    start_date=None,
    catchup=False,
)
def skypilot_cluster_context_example():
    # Return the setup_teardown_ops and the cluster_context_ref from the factory
    h100_cluster_scope, h100_cluster_ref = SkyPilotClusterContext(
        task_id="h100_scope",
        cluster_config=ClusterConfig(
            cluster_name=None,
            # cluster_name will be filled in by the CreateClusterOperator
            # and passed to the CancelClusterOperator via XCom
            resources=ResourceConfig(
                cpus=16,
                memory="64Gi",
                accelerators={"h100": 4},
            ),
        ),
    )

    # The same SkyPilot cluster will be used for all tasks within this context
    with h100_cluster_scope:
        # The cluster_context_ref will be used by the SkypilotJobOperator to pull the ClusterContext from XCom at runtime
        # and use the information to connect to the provisioned cluster
        training_task = SkypilotJobOperator(task_id="training_job", cluster_context_ref=h100_cluster_ref)
        evaluation_task = SkypilotJobOperator(task_id="evaluation_job", cluster_context_ref=h100_cluster_ref)
        training_task >> evaluation_task
        # and the cluster will be torn down automatically afterward if any task fails or when the context block exits.


skypilot_cluster_context_example()
```

Here's an example of using `SkyPilotClusterContextFactory` with dynamic cluster configuration based on `dag_run.conf` parameters when triggering the Dag:

```python
@dag(
    dag_id="skypilot_cluster_context_from_dag_run_conf",
    schedule=None,
    start_date=None,
    catchup=False,
)
def skypilot_cluster_context_from_dag_run_conf():
    # The cluster spec will depend on the dag_run.conf
    # which allows for dynamic cluster provisioning based on user input at runtime when triggering the Dag
    cluster_scope, cluster_ref = SkyPilotClusterContextFactory(
        task_id="based_on_dag_run_conf",
        cluster_config="{{ dag_run.conf['cluster_config'] | tojson }}",
    )
    # Fill the following conf when triggering the Dag:
    """
    {
        "cluster_config": {
            "cluster_name": null,
            "resources": {
                "cpus": 16,
                "memory": "64",
                "accelerators": {
                    "h100": 4
                }
            }
        }
    }
    """
    with cluster_scope:
        training_task = SkypilotJobOperator(task_id="training_job", cluster_context_ref=cluster_ref)

        @task.skypilot(cluster_context_ref=cluster_ref)
        def evaluation_task(): ...

        training_task >> evaluation_task


skypilot_cluster_context_from_dag_run_conf()
```

## 4.3 `SkyPilotExecutor` for running tasks directly on SkyPilot (not recommended for long-running AI workloads and Spot instances due to the "infrastructure instability" problem)

```python
@task(executor="SkyPilotExecutor")
def run_quick_inference_job():
    # This task will run directly on SkyPilot using the SkyPilotExecutor.
    # Note: This is not recommended for long-running jobs due to the "infrastructure instability" problem described in Section 2.1.
    # Unless your provisioned hardware is very stable (not on spot instances) or your job is short-lived.
    ...
```

## 5. Implementation Roadmap

### Step 1: Foundation
*   Basic SkyPilot API client integration (`SkyPilotHook`).
*   Finish **Factory Operators**: `SkypilotJobOperator`, `SkypilotJobCommandOperator`, `SkypilotCancelJobOperator` and its dependencies.

### Step 3: Decorators & Context Managers
*   Implement `@task.skypilot` decorators (using the Operator underlying).
*   Implement `SkyPilotClusterContext`.


### Step 4: UI & Advanced Features
*   Implement Airflow Connection Form.
*   Add Extra Links and External Views.

### Step 5: Executor (Optional)
*   Implement `SkyPilotExecutor` (optional/secondary priority).
    *  Research and implement signal handling and heartbeat logic to mitigate the "infrastructure instability" problem if possible.

## Appendix A: Challenges with Infrastructure Instability and `on_kill`

### The "Worker Crash vs. Cancellation" Dilemma

A critical problem for long-running AI workloads in Airflow is distinguishing between infrastructure failures and intentional user cancellations.

*   **Problem Statement**:
    *   Airflow workers cannot reliably distinguish whether a `SIGTERM` signal was triggered explicitly by a user (e.g., marking a task as failed in the UI) or by infrastructure instability (e.g., Kubernetes Pod eviction, node maintenance, spot instance preemption).
    *   Since the `on_kill` hook in an Operator is triggered by `SIGTERM`, infrastructure instability can inadvertently trigger the cancellation logic meant for user actions.
*   **Consequences**:
    1.  **Wasted Resources**: A long-running external job (e.g., 5 hours into training) might be killed because the Airflow worker pod was evicted, even though the remote SkyPilot job was healthy.
    2.  **Unstoppable Zombies**: Conversely, if we simply disable `on_kill` logic to prevent accidental kills, users lose the ability to stop a job via the Airflow UI, leading to "zombie" jobs that must be killed manually via CLI.
*   **Relevance**: This issue affects any system where the Airflow Worker is just a proxy for a remote job (SkyPilot, Databricks, etc.).
*   **References**:
    *   [Apache Airflow Mail List: Worker crashes shouldn't restart healthy jobs](https://lists.apache.org/thread/g4jhd0vz71x6jm4z8p6hjv9z7rmtb3jk)
    *   [[WIP] Add "persist_xcom_through_retry" Parameter to Airflow Operators](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=399278333)

This fundamental challenge is why we have adopted the **Factory Pattern (TaskGroup)** approach in Section 2.1, which decouples the "Wait" logic (in the Triggerer) from the "Cancel" logic (in a Teardown task).

## Appendix B: Airflow Architecture & Terminology

To understand the design choices (specifically regarding Executors vs. Deferrable Operators), we verify the following Airflow definitions:

*   **DAG**: A multi-step workflow consisting of multiple tasks.
*   **Task / Operator**:
    *   **Operator**: A pre-defined template for a Task (e.g., `SkypilotJobOperator`).
    *   **Task**: The basic unit of execution in a DAG.
*   **DagRun / TaskInstance**:
    *   **DagRun**: An instantiation of a DAG for a specific time.
    *   **TaskInstance**: An execution run of a specific Task.
*   **Scheduler**: The component responsible for determining when tasks need to run.
*   **Executor**:
    *   Responsible for taking TaskInstances and running them.
    *   Tracks the state of tasks (queued, running, etc.).
    *   *Constraint*: Each running task typically occupies a **pool slot**. Using a standard executor for a 2-day training job locks up a worker slot for 2 days.
*   **Trigger / Triggerer**:
    *   **Trigger**: A special, lightweight unit of work that runs in an asyncio loop, responsible solely for polling external events (e.g., "Is the SkyPilot job finished?").
    *   **Triggerer**: The component that runs Triggers. It does **not** occupy a pool slot. One Triggerer can handle thousands of concurrent waits.
    *   *Best Practice*: For long-running external tasks (like AI training), use Triggers (Deferrable Operators) to wait for completion.
*   **Deferrable Operator**: An operator that suspends itself (freeing the worker slot) while waiting for a Trigger, and resumes when the external condition is met.
