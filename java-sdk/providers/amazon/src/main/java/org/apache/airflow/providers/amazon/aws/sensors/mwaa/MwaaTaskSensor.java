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

// Generated from providers/amazon/_generated/lang_sdk_operators.json by
// scripts/ci/lang_sdk_providers/generate_java_wrappers.py. Do not edit by hand.

package org.apache.airflow.providers.amazon.aws.sensors.mwaa;

import java.util.List;
import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Waits for a task in an MWAA Environment to complete.
 *
 * Runs {@code airflow.providers.amazon.aws.sensors.mwaa.MwaaTaskSensor} of {@code
 * apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a Python
 * worker serves.
 */
public final class MwaaTaskSensor {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.sensors.mwaa.MwaaTaskSensor";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.sensors.mwaa", "MwaaTaskSensor")
          .params(
              "external_env_name",
              "external_dag_id",
              "external_dag_run_id",
              "external_task_id",
              "success_states",
              "failure_states",
              "airflow_version",
              "deferrable",
              "poke_interval",
              "max_retries",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region",
              "timeout",
              "soft_fail",
              "mode",
              "exponential_backoff",
              "max_wait",
              "silent_fail",
              "never_fail")
          .required("external_env_name", "external_dag_id", "external_task_id")
          .templated(
              "airflow_version",
              "aws_conn_id",
              "deferrable",
              "external_dag_id",
              "external_dag_run_id",
              "external_env_name",
              "external_task_id",
              "failure_states",
              "max_retries",
              "poke_interval",
              "region_name",
              "success_states",
              "verify")
          .uiColor("#e6f1f2")
          .sensor();

  private MwaaTaskSensor() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code MwaaTaskSensor}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /**
     * The external MWAA environment name that contains the Task Instance you want to wait for
     * (templated) Templated.
     */
    public Builder externalEnvName(String value) {
      return param("external_env_name", value);
    }

    /**
     * The external MWAA environment name that contains the Task Instance you want to wait for
     * (templated) Templated.
     */
    public Builder externalEnvName(TaskRef<?> value) {
      return param("external_env_name", value);
    }

    /**
     * The Dag ID in the external MWAA environment that contains the Task Instance you want to
     * wait for (templated) Templated.
     */
    public Builder externalDagId(String value) {
      return param("external_dag_id", value);
    }

    /**
     * The Dag ID in the external MWAA environment that contains the Task Instance you want to
     * wait for (templated) Templated.
     */
    public Builder externalDagId(TaskRef<?> value) {
      return param("external_dag_id", value);
    }

    /**
     * The Dag Run ID in the external MWAA environment that you want to wait for (templated)
     * Templated.
     */
    public Builder externalDagRunId(String value) {
      return param("external_dag_run_id", value);
    }

    /**
     * The Dag Run ID in the external MWAA environment that you want to wait for (templated)
     * Templated.
     */
    public Builder externalDagRunId(TaskRef<?> value) {
      return param("external_dag_run_id", value);
    }

    /**
     * The Task ID in the external MWAA environment that you want to wait for (templated)
     * Templated.
     */
    public Builder externalTaskId(String value) {
      return param("external_task_id", value);
    }

    /**
     * The Task ID in the external MWAA environment that you want to wait for (templated)
     * Templated.
     */
    public Builder externalTaskId(TaskRef<?> value) {
      return param("external_task_id", value);
    }

    /**
     * Collection of task instance states that would make this task marked as successful,
     * default is `{airflow.utils.state.TaskInstanceState.SUCCESS}` (templated) Templated.
     */
    public Builder successStates(List<String> value) {
      return param("success_states", value);
    }

    /**
     * Collection of task instance states that would make this task marked as successful,
     * default is `{airflow.utils.state.TaskInstanceState.SUCCESS}` (templated) Templated.
     */
    public Builder successStates(TaskRef<?> value) {
      return param("success_states", value);
    }

    /**
     * Collection of task instance states that would make this task marked as failed and raise
     * an AirflowException, default is `{airflow.utils.state.TaskInstanceState.FAILED}`
     * (templated) Templated.
     */
    public Builder failureStates(List<String> value) {
      return param("failure_states", value);
    }

    /**
     * Collection of task instance states that would make this task marked as failed and raise
     * an AirflowException, default is `{airflow.utils.state.TaskInstanceState.FAILED}`
     * (templated) Templated.
     */
    public Builder failureStates(TaskRef<?> value) {
      return param("failure_states", value);
    }

    /**
     * The Airflow major version the MWAA environment runs. This parameter is only used if the
     * local web token method is used to call Airflow API. (templated) Templated.
     */
    public Builder airflowVersion(Object value) {
      return param("airflow_version", value);
    }

    /**
     * If True, the sensor will operate in deferrable mode. This mode requires aiobotocore
     * module to be installed. (default: False, but can be overridden in config file by setting
     * default_deferrable to True) Defaults to false. Templated.
     */
    public Builder deferrable(boolean value) {
      return param("deferrable", value);
    }

    /**
     * If True, the sensor will operate in deferrable mode. This mode requires aiobotocore
     * module to be installed. (default: False, but can be overridden in config file by setting
     * default_deferrable to True) Defaults to false. Templated.
     */
    public Builder deferrable(TaskRef<?> value) {
      return param("deferrable", value);
    }

    /**
     * Polling period in seconds to check for the status of the job. (default: 60) Defaults to
     * 60. Templated.
     */
    public Builder pokeInterval(long value) {
      return param("poke_interval", value);
    }

    /**
     * Polling period in seconds to check for the status of the job. (default: 60) Defaults to
     * 60. Templated.
     */
    public Builder pokeInterval(TaskRef<?> value) {
      return param("poke_interval", value);
    }

    /**
     * Number of times before returning the current state. (default: 720) Defaults to 720.
     * Templated.
     */
    public Builder maxRetries(long value) {
      return param("max_retries", value);
    }

    /**
     * Number of times before returning the current state. (default: 720) Defaults to 720.
     * Templated.
     */
    public Builder maxRetries(TaskRef<?> value) {
      return param("max_retries", value);
    }

    /**
     * The Airflow connection used for AWS credentials. If this is `None` or empty then the
     * default boto3 behaviour is used. If running Airflow in a distributed manner and
     * aws_conn_id is None or empty, then default boto3 configuration would be used (and must
     * be maintained on each worker node). Defaults to "aws_default". Templated.
     */
    public Builder awsConnId(String value) {
      return param("aws_conn_id", value);
    }

    /**
     * The Airflow connection used for AWS credentials. If this is `None` or empty then the
     * default boto3 behaviour is used. If running Airflow in a distributed manner and
     * aws_conn_id is None or empty, then default boto3 configuration would be used (and must
     * be maintained on each worker node). Defaults to "aws_default". Templated.
     */
    public Builder awsConnId(TaskRef<?> value) {
      return param("aws_conn_id", value);
    }

    /** AWS region_name. If not specified then the default boto3 behaviour is used. Templated. */
    public Builder regionName(String value) {
      return param("region_name", value);
    }

    /** AWS region_name. If not specified then the default boto3 behaviour is used. Templated. */
    public Builder regionName(TaskRef<?> value) {
      return param("region_name", value);
    }

    /**
     * Whether or not to verify SSL certificates. See:
     * https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
     * Templated.
     */
    public Builder verify(Object value) {
      return param("verify", value);
    }

    /**
     * Configuration dictionary (key-values) for botocore client. See:
     * https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
     */
    public Builder botocoreConfig(Map<String, Object> value) {
      return param("botocore_config", value);
    }

    public Builder region(String value) {
      return param("region", value);
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
