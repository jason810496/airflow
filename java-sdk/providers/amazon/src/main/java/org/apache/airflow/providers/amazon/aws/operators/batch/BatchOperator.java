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

package org.apache.airflow.providers.amazon.aws.operators.batch;

import java.time.Duration;
import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Execute a job on AWS Batch.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.batch.BatchOperator} of {@code
 * apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a Python
 * worker serves.
 */
public final class BatchOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.batch.BatchOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.batch", "BatchOperator")
          .params(
              "job_name",
              "job_definition",
              "job_queue",
              "container_overrides",
              "array_properties",
              "ecs_properties_override",
              "eks_properties_override",
              "node_overrides",
              "share_identifier",
              "scheduling_priority_override",
              "parameters",
              "retry_strategy",
              "job_id",
              "waiters",
              "max_retries",
              "status_retries",
              "tags",
              "wait_for_completion",
              "deferrable",
              "poll_interval",
              "awslogs_enabled",
              "awslogs_fetch_interval",
              "submit_job_timeout",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("job_name", "job_definition", "job_queue")
          .templated(
              "array_properties",
              "aws_conn_id",
              "awslogs_enabled",
              "awslogs_fetch_interval",
              "container_overrides",
              "ecs_properties_override",
              "eks_properties_override",
              "job_definition",
              "job_id",
              "job_name",
              "job_queue",
              "node_overrides",
              "parameters",
              "region_name",
              "retry_strategy",
              "tags",
              "verify",
              "wait_for_completion",
              "waiters")
          .templateFieldsRenderers(Map.of("container_overrides", "json", "parameters", "json", "ecs_properties_override", "json", "eks_properties_override", "json", "node_overrides", "json", "retry_strategy", "json"))
          .uiColor("#c3dae0");

  private BatchOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code BatchOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** the name for the job that will run on AWS Batch (templated) Templated. */
    public Builder jobName(String value) {
      return param("job_name", value);
    }

    /** the name for the job that will run on AWS Batch (templated) Templated. */
    public Builder jobName(TaskRef<?> value) {
      return param("job_name", value);
    }

    /** the job definition name on AWS Batch Templated. */
    public Builder jobDefinition(String value) {
      return param("job_definition", value);
    }

    /** the job definition name on AWS Batch Templated. */
    public Builder jobDefinition(TaskRef<?> value) {
      return param("job_definition", value);
    }

    /** the queue name on AWS Batch Templated. */
    public Builder jobQueue(String value) {
      return param("job_queue", value);
    }

    /** the queue name on AWS Batch Templated. */
    public Builder jobQueue(TaskRef<?> value) {
      return param("job_queue", value);
    }

    /** the `containerOverrides` parameter for boto3 (templated) Templated. */
    public Builder containerOverrides(Map<String, Object> value) {
      return param("container_overrides", value);
    }

    /** the `containerOverrides` parameter for boto3 (templated) Templated. */
    public Builder containerOverrides(TaskRef<?> value) {
      return param("container_overrides", value);
    }

    /** the `arrayProperties` parameter for boto3 Templated. */
    public Builder arrayProperties(Map<String, Object> value) {
      return param("array_properties", value);
    }

    /** the `arrayProperties` parameter for boto3 Templated. */
    public Builder arrayProperties(TaskRef<?> value) {
      return param("array_properties", value);
    }

    /** the `ecsPropertiesOverride` parameter for boto3 (templated) Templated. */
    public Builder ecsPropertiesOverride(Map<String, Object> value) {
      return param("ecs_properties_override", value);
    }

    /** the `ecsPropertiesOverride` parameter for boto3 (templated) Templated. */
    public Builder ecsPropertiesOverride(TaskRef<?> value) {
      return param("ecs_properties_override", value);
    }

    /** the `eksPropertiesOverride` parameter for boto3 (templated) Templated. */
    public Builder eksPropertiesOverride(Map<String, Object> value) {
      return param("eks_properties_override", value);
    }

    /** the `eksPropertiesOverride` parameter for boto3 (templated) Templated. */
    public Builder eksPropertiesOverride(TaskRef<?> value) {
      return param("eks_properties_override", value);
    }

    /** the `nodeOverrides` parameter for boto3 (templated) Templated. */
    public Builder nodeOverrides(Map<String, Object> value) {
      return param("node_overrides", value);
    }

    /** the `nodeOverrides` parameter for boto3 (templated) Templated. */
    public Builder nodeOverrides(TaskRef<?> value) {
      return param("node_overrides", value);
    }

    /**
     * The share identifier for the job. Don't specify this parameter if the job queue doesn't
     * have a scheduling policy.
     */
    public Builder shareIdentifier(String value) {
      return param("share_identifier", value);
    }

    /**
     * The scheduling priority for the job. Jobs with a higher scheduling priority are
     * scheduled before jobs with a lower scheduling priority. This overrides any scheduling
     * priority in the job definition
     */
    public Builder schedulingPriorityOverride(long value) {
      return param("scheduling_priority_override", value);
    }

    /** the `parameters` for boto3 (templated) Templated. */
    public Builder parameters(Map<String, Object> value) {
      return param("parameters", value);
    }

    /** the `parameters` for boto3 (templated) Templated. */
    public Builder parameters(TaskRef<?> value) {
      return param("parameters", value);
    }

    /** Templated. */
    public Builder retryStrategy(Map<String, Object> value) {
      return param("retry_strategy", value);
    }

    /** Templated. */
    public Builder retryStrategy(TaskRef<?> value) {
      return param("retry_strategy", value);
    }

    /**
     * the job ID, usually unknown (None) until the submit_job operation gets the jobId defined
     * by AWS Batch Templated.
     */
    public Builder jobId(String value) {
      return param("job_id", value);
    }

    /**
     * the job ID, usually unknown (None) until the submit_job operation gets the jobId defined
     * by AWS Batch Templated.
     */
    public Builder jobId(TaskRef<?> value) {
      return param("job_id", value);
    }

    /**
     * an .BatchWaiters object (see note below); if None, polling is used with max_retries and
     * status_retries. Templated.
     */
    public Builder waiters(Object value) {
      return param("waiters", value);
    }

    /**
     * exponential back-off retries, 4200 = 48 hours; polling is only used when waiters is None
     * Defaults to 4200.
     */
    public Builder maxRetries(long value) {
      return param("max_retries", value);
    }

    /** number of HTTP retries to get job status, 10; polling is only used when waiters is None */
    public Builder statusRetries(long value) {
      return param("status_retries", value);
    }

    /**
     * collection of tags to apply to the AWS Batch job submission if None, no tags are
     * submitted Templated.
     */
    public Builder tags(Map<String, Object> value) {
      return param("tags", value);
    }

    /**
     * collection of tags to apply to the AWS Batch job submission if None, no tags are
     * submitted Templated.
     */
    public Builder tags(TaskRef<?> value) {
      return param("tags", value);
    }

    /** Defaults to true. Templated. */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /** Defaults to true. Templated. */
    public Builder waitForCompletion(TaskRef<?> value) {
      return param("wait_for_completion", value);
    }

    /**
     * Run operator in the deferrable mode. Deferring when `wait_for_completion` is False is
     * deprecated; from the next major release this has no effect when `wait_for_completion` is
     * False. Defaults to false.
     */
    public Builder deferrable(boolean value) {
      return param("deferrable", value);
    }

    /** (Deferrable mode only) Time in seconds to wait between polling. Defaults to 30. */
    public Builder pollInterval(long value) {
      return param("poll_interval", value);
    }

    /**
     * Specifies whether logs from CloudWatch should be printed or not, False. If it is an
     * array job, only the logs of the first task will be printed. Defaults to false.
     * Templated.
     */
    public Builder awslogsEnabled(boolean value) {
      return param("awslogs_enabled", value);
    }

    /**
     * Specifies whether logs from CloudWatch should be printed or not, False. If it is an
     * array job, only the logs of the first task will be printed. Defaults to false.
     * Templated.
     */
    public Builder awslogsEnabled(TaskRef<?> value) {
      return param("awslogs_enabled", value);
    }

    /** The interval with which cloudwatch logs are to be fetched, 30 sec. Templated. */
    public Builder awslogsFetchInterval(Duration value) {
      return param("awslogs_fetch_interval", value);
    }

    /** The interval with which cloudwatch logs are to be fetched, 30 sec. Templated. */
    public Builder awslogsFetchInterval(TaskRef<?> value) {
      return param("awslogs_fetch_interval", value);
    }

    /** Execution timeout in seconds for submitted batch job. */
    public Builder submitJobTimeout(long value) {
      return param("submit_job_timeout", value);
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
