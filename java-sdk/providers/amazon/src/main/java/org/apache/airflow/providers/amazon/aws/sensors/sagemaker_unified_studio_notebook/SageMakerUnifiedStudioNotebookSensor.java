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

package org.apache.airflow.providers.amazon.aws.sensors.sagemaker_unified_studio_notebook;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Polls a SageMaker Unified Studio notebook execution until it reaches a terminal state.
 *
 * Runs {@code
 * airflow.providers.amazon.aws.sensors.sagemaker_unified_studio_notebook.SageMakerUnifiedStudioNotebookSensor}
 * of {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that
 * a Python worker serves.
 */
public final class SageMakerUnifiedStudioNotebookSensor {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.sensors.sagemaker_unified_studio_notebook.SageMakerUnifiedStudioNotebookSensor";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.sensors.sagemaker_unified_studio_notebook", "SageMakerUnifiedStudioNotebookSensor")
          .params(
              "domain_identifier",
              "owning_project_identifier",
              "notebook_run_id",
              "notebook_identifier",
              "endpoint_url",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region",
              "poke_interval",
              "timeout",
              "soft_fail",
              "mode",
              "exponential_backoff",
              "max_wait",
              "silent_fail",
              "never_fail")
          .required(
              "domain_identifier",
              "owning_project_identifier",
              "notebook_run_id",
              "notebook_identifier")
          .templated(
              "aws_conn_id",
              "domain_identifier",
              "endpoint_url",
              "notebook_identifier",
              "notebook_run_id",
              "owning_project_identifier",
              "region_name",
              "verify")
          .uiColor("#e6f1f2")
          .sensor();

  private SageMakerUnifiedStudioNotebookSensor() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code SageMakerUnifiedStudioNotebookSensor}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The ID of the SageMaker Unified Studio domain containing the notebook. Templated. */
    public Builder domainIdentifier(String value) {
      return param("domain_identifier", value);
    }

    /** The ID of the SageMaker Unified Studio domain containing the notebook. Templated. */
    public Builder domainIdentifier(TaskRef<?> value) {
      return param("domain_identifier", value);
    }

    /** The ID of the SageMaker Unified Studio project containing the notebook. Templated. */
    public Builder owningProjectIdentifier(String value) {
      return param("owning_project_identifier", value);
    }

    /** The ID of the SageMaker Unified Studio project containing the notebook. Templated. */
    public Builder owningProjectIdentifier(TaskRef<?> value) {
      return param("owning_project_identifier", value);
    }

    /**
     * The ID of the notebook run to monitor. This is returned by the
     * `SageMakerUnifiedStudioNotebookOperator`. Templated.
     */
    public Builder notebookRunId(String value) {
      return param("notebook_run_id", value);
    }

    /**
     * The ID of the notebook run to monitor. This is returned by the
     * `SageMakerUnifiedStudioNotebookOperator`. Templated.
     */
    public Builder notebookRunId(TaskRef<?> value) {
      return param("notebook_run_id", value);
    }

    /**
     * The ID of the notebook that was executed. Required to read notebook outputs from S3
     * after the run completes. Templated.
     */
    public Builder notebookIdentifier(String value) {
      return param("notebook_identifier", value);
    }

    /**
     * The ID of the notebook that was executed. Required to read notebook outputs from S3
     * after the run completes. Templated.
     */
    public Builder notebookIdentifier(TaskRef<?> value) {
      return param("notebook_identifier", value);
    }

    /** Templated. */
    public Builder endpointUrl(String value) {
      return param("endpoint_url", value);
    }

    /** Templated. */
    public Builder endpointUrl(TaskRef<?> value) {
      return param("endpoint_url", value);
    }

    /**
     * The Airflow connection used for AWS credentials. If this is None or empty then the
     * default boto3 behaviour is used. If running Airflow in a distributed manner and
     * aws_conn_id is None or empty, then default boto3 configuration would be used (and must
     * be maintained on each worker node). Defaults to "aws_default". Templated.
     */
    public Builder awsConnId(String value) {
      return param("aws_conn_id", value);
    }

    /**
     * The Airflow connection used for AWS credentials. If this is None or empty then the
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
     * Time that the job should wait in between each try. Can be `timedelta` or `float`
     * seconds. Defaults to 60.
     */
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
