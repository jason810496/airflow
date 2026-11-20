/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Generated from providers/standard/_generated/lang_sdk_operators.json by
// scripts/ci/lang_sdk_providers/generate_java_wrappers.py. Do not edit by hand.

package org.apache.airflow.providers.standard.sensors.external_task;

import java.time.Duration;
import java.util.List;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Waits for a different DAG, task group, or task to complete for a specific logical date.
 *
 * Runs {@code airflow.providers.standard.sensors.external_task.ExternalTaskSensor} of {@code
 * apache-airflow-providers-standard} on a Python worker, so give the task a queue that a
 * Python worker serves.
 *
 * Not available from Java, because JSON cannot carry them: {@code execution_date_fn}.
 */
public final class ExternalTaskSensor {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.standard.sensors.external_task.ExternalTaskSensor";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.standard.sensors.external_task", "ExternalTaskSensor")
          .params(
              "external_dag_id",
              "external_task_id",
              "external_task_ids",
              "external_task_group_id",
              "allowed_states",
              "skipped_states",
              "failed_states",
              "execution_delta",
              "check_existence",
              "poll_interval",
              "deferrable",
              "poke_interval",
              "timeout",
              "soft_fail",
              "mode",
              "exponential_backoff",
              "max_wait",
              "silent_fail",
              "never_fail")
          .required("external_dag_id")
          .templated(
              "external_dag_id",
              "external_task_id",
              "external_task_ids",
              "external_task_group_id")
          .uiColor("#4db7db")
          .sensor();

  private ExternalTaskSensor() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code ExternalTaskSensor}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The dag_id that contains the task you want to wait for. (templated) Templated. */
    public Builder externalDagId(String value) {
      return param("external_dag_id", value);
    }

    /** The dag_id that contains the task you want to wait for. (templated) Templated. */
    public Builder externalDagId(TaskRef<?> value) {
      return param("external_dag_id", value);
    }

    /** The task_id that contains the task you want to wait for. (templated) Templated. */
    public Builder externalTaskId(String value) {
      return param("external_task_id", value);
    }

    /** The task_id that contains the task you want to wait for. (templated) Templated. */
    public Builder externalTaskId(TaskRef<?> value) {
      return param("external_task_id", value);
    }

    /**
     * The list of task_ids that you want to wait for. (templated) If `None` (default value)
     * the sensor waits for the DAG. Either external_task_id or external_task_ids can be passed
     * to ExternalTaskSensor, but not both. Templated.
     */
    public Builder externalTaskIds(List<String> value) {
      return param("external_task_ids", value);
    }

    /**
     * The list of task_ids that you want to wait for. (templated) If `None` (default value)
     * the sensor waits for the DAG. Either external_task_id or external_task_ids can be passed
     * to ExternalTaskSensor, but not both. Templated.
     */
    public Builder externalTaskIds(TaskRef<?> value) {
      return param("external_task_ids", value);
    }

    /** The task_group_id that contains the task you want to wait for. (templated) Templated. */
    public Builder externalTaskGroupId(String value) {
      return param("external_task_group_id", value);
    }

    /** The task_group_id that contains the task you want to wait for. (templated) Templated. */
    public Builder externalTaskGroupId(TaskRef<?> value) {
      return param("external_task_group_id", value);
    }

    /** Iterable of allowed states, default is `['success']` */
    public Builder allowedStates(List<String> value) {
      return param("allowed_states", value);
    }

    /** Iterable of states to make this task mark as skipped, default is `None` */
    public Builder skippedStates(List<String> value) {
      return param("skipped_states", value);
    }

    /** Iterable of failed or dis-allowed states, default is `None` */
    public Builder failedStates(List<String> value) {
      return param("failed_states", value);
    }

    /**
     * time difference with the previous execution to look at, the default is the same logical
     * date as the current task or DAG. For yesterday, use [positive!]
     * datetime.timedelta(days=1). Either execution_delta or execution_date_fn can be passed to
     * ExternalTaskSensor, but not both.
     */
    public Builder executionDelta(Duration value) {
      return param("execution_delta", value);
    }

    /**
     * Set to `True` to check if the external task exists (when external_task_id is not None)
     * or check if the DAG to wait for exists (when external_task_id is None), and immediately
     * cease waiting if the external task or DAG does not exist (default value: False).
     * Defaults to false.
     */
    public Builder checkExistence(boolean value) {
      return param("check_existence", value);
    }

    /** (DEPRECATED) use `poke_interval` instead */
    public Builder pollInterval(double value) {
      return param("poll_interval", value);
    }

    /** Run sensor in deferrable mode Defaults to false. */
    public Builder deferrable(boolean value) {
      return param("deferrable", value);
    }

    /** polling period in seconds to check for the status Defaults to 60. */
    public Builder pokeInterval(double value) {
      return param("poke_interval", value);
    }

    /**
     * Time elapsed before the task times out and fails. Can be `timedelta` or `float` seconds.
     * This should not be confused with `execution_timeout` of the `BaseOperator` class.
     * `timeout` measures the time elapsed between the first poke and the current time (taking
     * into account any reschedule delay between each poke), while `execution_timeout` checks
     * the **running** time of the task (leaving out any reschedule delay). In case that the
     * `mode` is `poke` (see below), both of them are equivalent (as the sensor is never
     * rescheduled), which is not the case in `reschedule` mode.
     */
    public Builder timeout(double value) {
      return param("timeout", value);
    }

    /**
     * Set to true to mark the task as SKIPPED on failure. Mutually exclusive with never_fail.
     * Defaults to false.
     */
    public Builder softFail(boolean value) {
      return param("soft_fail", value);
    }

    /**
     * How the sensor operates. Options are: `{ poke | reschedule }`, default is `poke`. When
     * set to `poke` the sensor is taking up a worker slot for its whole execution time and
     * sleeps between pokes. Use this mode if the expected runtime of the sensor is short or if
     * a short poke interval is required. Note that the sensor will hold onto a worker slot and
     * a pool slot for the duration of the sensor's runtime in this mode. When set to
     * `reschedule` the sensor task frees the worker slot when the criteria is not yet met and
     * it's rescheduled at a later time. Use this mode if the time before the criteria is met
     * is expected to be quite long. The poke interval should be more than one minute to
     * prevent too much load on the scheduler. Defaults to "poke".
     */
    public Builder mode(String value) {
      return param("mode", value);
    }

    /**
     * allow progressive longer waits between pokes by using exponential backoff algorithm
     * Defaults to false.
     */
    public Builder exponentialBackoff(boolean value) {
      return param("exponential_backoff", value);
    }

    /** maximum wait interval between pokes, can be `timedelta` or `float` seconds */
    public Builder maxWait(double value) {
      return param("max_wait", value);
    }

    /**
     * If true, and poke method raises an exception different from AirflowSensorTimeout,
     * AirflowTaskTimeout, AirflowSkipException and AirflowFailException, the sensor will log
     * the error and continue its execution. Otherwise, the sensor task fails, and it can be
     * retried based on the provided `retries` parameter. Defaults to false.
     */
    public Builder silentFail(boolean value) {
      return param("silent_fail", value);
    }

    /**
     * If true, and poke method raises an exception, sensor will be skipped. Mutually exclusive
     * with soft_fail. Defaults to false.
     */
    public Builder neverFail(boolean value) {
      return param("never_fail", value);
    }
  }
}
