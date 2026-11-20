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

package org.apache.airflow.providers.amazon.aws.operators.bedrock;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Create a batch inference job to invoke a model on multiple prompts.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.bedrock.BedrockBatchInferenceOperator} of
 * {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class BedrockBatchInferenceOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.bedrock.BedrockBatchInferenceOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.bedrock", "BedrockBatchInferenceOperator")
          .params(
              "job_name",
              "role_arn",
              "model_id",
              "input_uri",
              "output_uri",
              "invoke_kwargs",
              "wait_for_completion",
              "success_state",
              "waiter_delay",
              "waiter_max_attempts",
              "deferrable",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("job_name", "role_arn", "model_id", "input_uri", "output_uri")
          .templated(
              "aws_conn_id",
              "input_uri",
              "invoke_kwargs",
              "job_name",
              "model_id",
              "output_uri",
              "region_name",
              "role_arn",
              "verify");

  private BedrockBatchInferenceOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code BedrockBatchInferenceOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** A name to give the batch inference job. (templated) Templated. */
    public Builder jobName(String value) {
      return param("job_name", value);
    }

    /** A name to give the batch inference job. (templated) Templated. */
    public Builder jobName(TaskRef<?> value) {
      return param("job_name", value);
    }

    /**
     * The ARN of the IAM role with permissions to create the knowledge base. (templated)
     * Templated.
     */
    public Builder roleArn(String value) {
      return param("role_arn", value);
    }

    /**
     * The ARN of the IAM role with permissions to create the knowledge base. (templated)
     * Templated.
     */
    public Builder roleArn(TaskRef<?> value) {
      return param("role_arn", value);
    }

    /**
     * Name or ARN of the model to associate with this provisioned throughput. (templated)
     * Templated.
     */
    public Builder modelId(String value) {
      return param("model_id", value);
    }

    /**
     * Name or ARN of the model to associate with this provisioned throughput. (templated)
     * Templated.
     */
    public Builder modelId(TaskRef<?> value) {
      return param("model_id", value);
    }

    /** The S3 location of the input data. (templated) Templated. */
    public Builder inputUri(String value) {
      return param("input_uri", value);
    }

    /** The S3 location of the input data. (templated) Templated. */
    public Builder inputUri(TaskRef<?> value) {
      return param("input_uri", value);
    }

    /** The S3 location of the output data. (templated) Templated. */
    public Builder outputUri(String value) {
      return param("output_uri", value);
    }

    /** The S3 location of the output data. (templated) Templated. */
    public Builder outputUri(TaskRef<?> value) {
      return param("output_uri", value);
    }

    /** Additional keyword arguments to pass to the API call. (templated) Templated. */
    public Builder invokeKwargs(Map<String, Object> value) {
      return param("invoke_kwargs", value);
    }

    /** Additional keyword arguments to pass to the API call. (templated) Templated. */
    public Builder invokeKwargs(TaskRef<?> value) {
      return param("invoke_kwargs", value);
    }

    /**
     * Whether to wait for the job to reach `success_state`. (default: True) NOTE: The way
     * batch inference jobs work, your jobs are added to a queue and done "eventually" so using
     * deferrable mode is much more practical than using wait_for_completion. Defaults to true.
     */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /**
     * The job state to wait for, either `"completed"` or `"scheduled"`. Use `"scheduled"` to
     * stop waiting once Bedrock has queued the job. Accepts the
     * `BedrockBatchInferenceSensor.SuccessState` values. (default: "completed") Defaults to
     * "completed".
     */
    public Builder successState(String value) {
      return param("success_state", value);
    }

    /** Time in seconds to wait between status checks. (default: 60) Defaults to 60. */
    public Builder waiterDelay(long value) {
      return param("waiter_delay", value);
    }

    /** Maximum number of attempts to check for job completion. (default: 20) Defaults to 20. */
    public Builder waiterMaxAttempts(long value) {
      return param("waiter_max_attempts", value);
    }

    /**
     * If True, the operator will wait asynchronously for the job to reach `success_state`.
     * Deferring when `wait_for_completion` is False is deprecated; from the next major release
     * this has no effect when `wait_for_completion` is False. This mode requires aiobotocore
     * module to be installed. (default: False) Defaults to false.
     */
    public Builder deferrable(boolean value) {
      return param("deferrable", value);
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
