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

package org.apache.airflow.providers.amazon.aws.operators.emr;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Operator to start EMR Serverless job.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.emr.EmrServerlessStartJobOperator} of
 * {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class EmrServerlessStartJobOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.emr.EmrServerlessStartJobOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.emr", "EmrServerlessStartJobOperator")
          .params(
              "application_id",
              "execution_role_arn",
              "job_driver",
              "configuration_overrides",
              "client_request_token",
              "config",
              "wait_for_completion",
              "name",
              "waiter_max_attempts",
              "waiter_delay",
              "deferrable",
              "enable_application_ui_links",
              "cancel_on_kill",
              "openlineage_inject_parent_job_info",
              "openlineage_inject_transport_info",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("application_id", "execution_role_arn", "job_driver")
          .templated(
              "application_id",
              "aws_conn_id",
              "config",
              "configuration_overrides",
              "execution_role_arn",
              "job_driver",
              "name",
              "region_name",
              "verify")
          .templateFieldsRenderers(Map.of("config", "json", "configuration_overrides", "json"));

  private EmrServerlessStartJobOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code EmrServerlessStartJobOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** ID of the EMR Serverless application to start. Templated. */
    public Builder applicationId(String value) {
      return param("application_id", value);
    }

    /** ID of the EMR Serverless application to start. Templated. */
    public Builder applicationId(TaskRef<?> value) {
      return param("application_id", value);
    }

    /** ARN of role to perform action. Templated. */
    public Builder executionRoleArn(String value) {
      return param("execution_role_arn", value);
    }

    /** ARN of role to perform action. Templated. */
    public Builder executionRoleArn(TaskRef<?> value) {
      return param("execution_role_arn", value);
    }

    /** Driver that the job runs on. Templated. */
    public Builder jobDriver(Map<String, Object> value) {
      return param("job_driver", value);
    }

    /** Driver that the job runs on. Templated. */
    public Builder jobDriver(TaskRef<?> value) {
      return param("job_driver", value);
    }

    /** Configuration specifications to override existing configurations. Templated. */
    public Builder configurationOverrides(Map<String, Object> value) {
      return param("configuration_overrides", value);
    }

    /** Configuration specifications to override existing configurations. Templated. */
    public Builder configurationOverrides(TaskRef<?> value) {
      return param("configuration_overrides", value);
    }

    /**
     * The client idempotency token of the application to create. Its value must be unique for
     * each request. Defaults to "".
     */
    public Builder clientRequestToken(String value) {
      return param("client_request_token", value);
    }

    /**
     * Optional dictionary for arbitrary parameters to the boto API start_job_run call.
     * Templated.
     */
    public Builder config(Map<String, Object> value) {
      return param("config", value);
    }

    /**
     * Optional dictionary for arbitrary parameters to the boto API start_job_run call.
     * Templated.
     */
    public Builder config(TaskRef<?> value) {
      return param("config", value);
    }

    /**
     * If true, waits for the job to start before returning. Defaults to True. If set to False,
     * `waiter_max_attempts` and `waiter_delay` will only be applied when waiting for the
     * application to be in the `STARTED` state. Defaults to true.
     */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /**
     * Name for the EMR Serverless job. If not provided, a default name will be assigned.
     * Templated.
     */
    public Builder name(String value) {
      return param("name", value);
    }

    /**
     * Name for the EMR Serverless job. If not provided, a default name will be assigned.
     * Templated.
     */
    public Builder name(TaskRef<?> value) {
      return param("name", value);
    }

    /**
     * Number of times the waiter should poll the application to check the state. Defaults to
     * 25 if not set.
     */
    public Builder waiterMaxAttempts(long value) {
      return param("waiter_max_attempts", value);
    }

    /**
     * Number of seconds between polling the state of the job run. Defaults to 60 seconds if
     * not set.
     */
    public Builder waiterDelay(long value) {
      return param("waiter_delay", value);
    }

    /**
     * If True, the operator will wait asynchronously for the job run to complete. When
     * `wait_for_completion` is False, this only affects waiting for the application to start.
     * This mode requires aiobotocore module to be installed. (default: False, but can be
     * overridden in config file by setting default_deferrable to True) Defaults to false.
     */
    public Builder deferrable(boolean value) {
      return param("deferrable", value);
    }

    /**
     * If True, the operator will generate one-time links to EMR Serverless application UIs.
     * The generated links will allow any user with access to the Dag to see the Spark or Tez
     * UI or Spark stdout logs. Defaults to False. Defaults to false.
     */
    public Builder enableApplicationUiLinks(boolean value) {
      return param("enable_application_ui_links", value);
    }

    /**
     * If True, the EMR Serverless job will be cancelled when the task is killed while in
     * deferrable mode. This ensures that orphan jobs are not left running in EMR Serverless
     * when an Airflow task is cancelled. Defaults to True. Defaults to true.
     */
    public Builder cancelOnKill(boolean value) {
      return param("cancel_on_kill", value);
    }

    /**
     * If True, injects OpenLineage parent job information into the EMR Serverless
     * `spark-defaults` configuration so the Spark job emits a `parentRunFacet` linking back to
     * the Airflow task. Defaults to the `openlineage.spark_inject_parent_job_info` config
     * value. Defaults to false.
     */
    public Builder openlineageInjectParentJobInfo(boolean value) {
      return param("openlineage_inject_parent_job_info", value);
    }

    /**
     * If True, injects OpenLineage transport configuration into the EMR Serverless
     * `spark-defaults` configuration so the Spark job sends OL events to the same backend as
     * Airflow. Defaults to the `openlineage.spark_inject_transport_info` config value.
     * Defaults to false.
     */
    public Builder openlineageInjectTransportInfo(boolean value) {
      return param("openlineage_inject_transport_info", value);
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
  }
}
