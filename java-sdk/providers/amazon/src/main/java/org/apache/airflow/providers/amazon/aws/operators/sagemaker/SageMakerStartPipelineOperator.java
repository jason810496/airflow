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

package org.apache.airflow.providers.amazon.aws.operators.sagemaker;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Starts a SageMaker pipeline execution.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.sagemaker.SageMakerStartPipelineOperator}
 * of {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that
 * a Python worker serves.
 */
public final class SageMakerStartPipelineOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.sagemaker.SageMakerStartPipelineOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.sagemaker", "SageMakerStartPipelineOperator")
          .params(
              "pipeline_name",
              "display_name",
              "pipeline_params",
              "wait_for_completion",
              "check_interval",
              "waiter_max_attempts",
              "verbose",
              "deferrable",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("pipeline_name")
          .templated(
              "aws_conn_id",
              "display_name",
              "pipeline_name",
              "pipeline_params",
              "region_name",
              "verify")
          .templateFieldsRenderers(Map.of("config", "json"))
          .uiColor("#ededed");

  private SageMakerStartPipelineOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code SageMakerStartPipelineOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** Name of the pipeline to start. Templated. */
    public Builder pipelineName(String value) {
      return param("pipeline_name", value);
    }

    /** Name of the pipeline to start. Templated. */
    public Builder pipelineName(TaskRef<?> value) {
      return param("pipeline_name", value);
    }

    /**
     * The name this pipeline execution will have in the UI. Doesn't need to be unique.
     * Defaults to "airflow-triggered-execution". Templated.
     */
    public Builder displayName(String value) {
      return param("display_name", value);
    }

    /**
     * The name this pipeline execution will have in the UI. Doesn't need to be unique.
     * Defaults to "airflow-triggered-execution". Templated.
     */
    public Builder displayName(TaskRef<?> value) {
      return param("display_name", value);
    }

    /**
     * Optional parameters for the pipeline. All parameters supplied need to already be present
     * in the pipeline definition. Templated.
     */
    public Builder pipelineParams(Map<String, Object> value) {
      return param("pipeline_params", value);
    }

    /**
     * Optional parameters for the pipeline. All parameters supplied need to already be present
     * in the pipeline definition. Templated.
     */
    public Builder pipelineParams(TaskRef<?> value) {
      return param("pipeline_params", value);
    }

    /**
     * If true, this operator will only complete once the pipeline is complete. Defaults to
     * false.
     */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /**
     * How long to wait between checks for pipeline status when waiting for completion.
     * Defaults to 30.
     */
    public Builder checkInterval(long value) {
      return param("check_interval", value);
    }

    /** How many times to check the status before failing. Defaults to 9999. */
    public Builder waiterMaxAttempts(long value) {
      return param("waiter_max_attempts", value);
    }

    /**
     * Whether to print steps details when waiting for completion. Defaults to true, consider
     * turning off for pipelines that have thousands of steps. Defaults to true.
     */
    public Builder verbose(boolean value) {
      return param("verbose", value);
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
