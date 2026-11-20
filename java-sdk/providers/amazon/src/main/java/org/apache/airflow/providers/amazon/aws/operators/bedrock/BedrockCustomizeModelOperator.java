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
 * Create a fine-tuning job to customize a base model.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.bedrock.BedrockCustomizeModelOperator} of
 * {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class BedrockCustomizeModelOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.bedrock.BedrockCustomizeModelOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.bedrock", "BedrockCustomizeModelOperator")
          .params(
              "job_name",
              "custom_model_name",
              "role_arn",
              "base_model_id",
              "training_data_uri",
              "output_data_uri",
              "hyperparameters",
              "ensure_unique_job_name",
              "customization_job_kwargs",
              "wait_for_completion",
              "waiter_delay",
              "waiter_max_attempts",
              "deferrable",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required(
              "job_name",
              "custom_model_name",
              "role_arn",
              "base_model_id",
              "training_data_uri",
              "output_data_uri",
              "hyperparameters")
          .templated(
              "aws_conn_id",
              "base_model_id",
              "custom_model_name",
              "customization_job_kwargs",
              "ensure_unique_job_name",
              "hyperparameters",
              "job_name",
              "region_name",
              "role_arn",
              "verify");

  private BedrockCustomizeModelOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code BedrockCustomizeModelOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** A unique name for the fine-tuning job. Templated. */
    public Builder jobName(String value) {
      return param("job_name", value);
    }

    /** A unique name for the fine-tuning job. Templated. */
    public Builder jobName(TaskRef<?> value) {
      return param("job_name", value);
    }

    /** A name for the custom model being created. Templated. */
    public Builder customModelName(String value) {
      return param("custom_model_name", value);
    }

    /** A name for the custom model being created. Templated. */
    public Builder customModelName(TaskRef<?> value) {
      return param("custom_model_name", value);
    }

    /**
     * The Amazon Resource Name (ARN) of an IAM role that Amazon Bedrock can assume to perform
     * tasks on your behalf. Templated.
     */
    public Builder roleArn(String value) {
      return param("role_arn", value);
    }

    /**
     * The Amazon Resource Name (ARN) of an IAM role that Amazon Bedrock can assume to perform
     * tasks on your behalf. Templated.
     */
    public Builder roleArn(TaskRef<?> value) {
      return param("role_arn", value);
    }

    /** Name of the base model. Templated. */
    public Builder baseModelId(String value) {
      return param("base_model_id", value);
    }

    /** Name of the base model. Templated. */
    public Builder baseModelId(TaskRef<?> value) {
      return param("base_model_id", value);
    }

    /** The S3 URI where the training data is stored. */
    public Builder trainingDataUri(String value) {
      return param("training_data_uri", value);
    }

    /** The S3 URI where the output data is stored. */
    public Builder outputDataUri(String value) {
      return param("output_data_uri", value);
    }

    /** Parameters related to tuning the model. Templated. */
    public Builder hyperparameters(Map<String, String> value) {
      return param("hyperparameters", value);
    }

    /** Parameters related to tuning the model. Templated. */
    public Builder hyperparameters(TaskRef<?> value) {
      return param("hyperparameters", value);
    }

    /**
     * If set to true, operator will check whether a model customization job already exists for
     * the name in the config and append the current timestamp if there is a name conflict.
     * (Default: True) Defaults to true. Templated.
     */
    public Builder ensureUniqueJobName(boolean value) {
      return param("ensure_unique_job_name", value);
    }

    /**
     * If set to true, operator will check whether a model customization job already exists for
     * the name in the config and append the current timestamp if there is a name conflict.
     * (Default: True) Defaults to true. Templated.
     */
    public Builder ensureUniqueJobName(TaskRef<?> value) {
      return param("ensure_unique_job_name", value);
    }

    /** Any optional parameters to pass to the API. Templated. */
    public Builder customizationJobKwargs(Map<String, Object> value) {
      return param("customization_job_kwargs", value);
    }

    /** Any optional parameters to pass to the API. Templated. */
    public Builder customizationJobKwargs(TaskRef<?> value) {
      return param("customization_job_kwargs", value);
    }

    /** Whether to wait for cluster to stop. (default: True) Defaults to true. */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /** Time in seconds to wait between status checks. (default: 120) Defaults to 120. */
    public Builder waiterDelay(long value) {
      return param("waiter_delay", value);
    }

    /** Maximum number of attempts to check for job completion. (default: 75) Defaults to 75. */
    public Builder waiterMaxAttempts(long value) {
      return param("waiter_max_attempts", value);
    }

    /**
     * If True, the operator will wait asynchronously for the cluster to stop. This mode
     * requires aiobotocore module to be installed. (default: False) Deferring when
     * `wait_for_completion` is False is deprecated; from the next major release this has no
     * effect when `wait_for_completion` is False. Defaults to false.
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
