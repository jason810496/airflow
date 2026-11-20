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
 * An operator that submits jobs to EMR on EKS virtual clusters.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.emr.EmrContainerOperator} of {@code
 * apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a Python
 * worker serves.
 */
public final class EmrContainerOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.emr.EmrContainerOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.emr", "EmrContainerOperator")
          .params(
              "name",
              "virtual_cluster_id",
              "execution_role_arn",
              "release_label",
              "job_driver",
              "configuration_overrides",
              "client_request_token",
              "wait_for_completion",
              "poll_interval",
              "tags",
              "max_polling_attempts",
              "job_retry_max_attempts",
              "deferrable",
              "cancel_on_kill",
              "openlineage_inject_parent_job_info",
              "openlineage_inject_transport_info",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required(
              "name",
              "virtual_cluster_id",
              "execution_role_arn",
              "release_label",
              "job_driver")
          .templated(
              "aws_conn_id",
              "configuration_overrides",
              "execution_role_arn",
              "job_driver",
              "name",
              "region_name",
              "release_label",
              "verify",
              "virtual_cluster_id")
          .uiColor("#f9c915");

  private EmrContainerOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code EmrContainerOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The name of the job run. Templated. */
    public Builder name(String value) {
      return param("name", value);
    }

    /** The name of the job run. Templated. */
    public Builder name(TaskRef<?> value) {
      return param("name", value);
    }

    /** The EMR on EKS virtual cluster ID Templated. */
    public Builder virtualClusterId(String value) {
      return param("virtual_cluster_id", value);
    }

    /** The EMR on EKS virtual cluster ID Templated. */
    public Builder virtualClusterId(TaskRef<?> value) {
      return param("virtual_cluster_id", value);
    }

    /** The IAM role ARN associated with the job run. Templated. */
    public Builder executionRoleArn(String value) {
      return param("execution_role_arn", value);
    }

    /** The IAM role ARN associated with the job run. Templated. */
    public Builder executionRoleArn(TaskRef<?> value) {
      return param("execution_role_arn", value);
    }

    /** The Amazon EMR release version to use for the job run. Templated. */
    public Builder releaseLabel(String value) {
      return param("release_label", value);
    }

    /** The Amazon EMR release version to use for the job run. Templated. */
    public Builder releaseLabel(TaskRef<?> value) {
      return param("release_label", value);
    }

    /** Job configuration details, e.g. the Spark job parameters. Templated. */
    public Builder jobDriver(Map<String, Object> value) {
      return param("job_driver", value);
    }

    /** Job configuration details, e.g. the Spark job parameters. Templated. */
    public Builder jobDriver(TaskRef<?> value) {
      return param("job_driver", value);
    }

    /**
     * The configuration overrides for the job run, specifically either application
     * configuration or monitoring configuration. Templated.
     */
    public Builder configurationOverrides(Map<String, Object> value) {
      return param("configuration_overrides", value);
    }

    /**
     * The configuration overrides for the job run, specifically either application
     * configuration or monitoring configuration. Templated.
     */
    public Builder configurationOverrides(TaskRef<?> value) {
      return param("configuration_overrides", value);
    }

    /**
     * The client idempotency token of the job run request. Pass an explicit value to make
     * repeated submissions idempotent: EMR on EKS treats a resubmission with the same token as
     * the original run, so task retries return that run instead of starting a new one. If no
     * token is provided, a fresh UUIDv4 is generated on every task attempt, so each retry
     * starts a genuinely new job run.
     */
    public Builder clientRequestToken(String value) {
      return param("client_request_token", value);
    }

    /** Whether or not to wait in the operator for the job to complete. Defaults to true. */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /**
     * Time (in seconds) to wait between two consecutive calls to check query status on EMR
     * Defaults to 30.
     */
    public Builder pollInterval(long value) {
      return param("poll_interval", value);
    }

    /** The tags assigned to job runs. Defaults to None */
    public Builder tags(Map<String, Object> value) {
      return param("tags", value);
    }

    /**
     * Maximum number of times to wait for the job run to finish. Defaults to None, which will
     * poll until the job is *not* in a pending, submitted, or running state.
     */
    public Builder maxPollingAttempts(long value) {
      return param("max_polling_attempts", value);
    }

    /**
     * Maximum number of times to retry when the EMR job fails. Defaults to None, which disable
     * the retry.
     */
    public Builder jobRetryMaxAttempts(long value) {
      return param("job_retry_max_attempts", value);
    }

    /**
     * Run operator in the deferrable mode. Deferring when `wait_for_completion` is False is
     * deprecated; from the next major release this has no effect when `wait_for_completion` is
     * False. Defaults to false.
     */
    public Builder deferrable(boolean value) {
      return param("deferrable", value);
    }

    /**
     * Flag to indicate whether to cancel the job when the task is killed while in deferrable
     * mode. Defaults to true.
     */
    public Builder cancelOnKill(boolean value) {
      return param("cancel_on_kill", value);
    }

    /**
     * If True, injects OpenLineage parent job information into the EMR on EKS `spark-defaults`
     * configuration so the Spark job emits a `parentRunFacet` linking back to the Airflow
     * task. Defaults to the `openlineage.spark_inject_parent_job_info` config value. Defaults
     * to false.
     */
    public Builder openlineageInjectParentJobInfo(boolean value) {
      return param("openlineage_inject_parent_job_info", value);
    }

    /**
     * If True, injects OpenLineage transport configuration into the EMR on EKS
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
