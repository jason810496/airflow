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
 * Starts a transform job.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.sagemaker.SageMakerTransformOperator} of
 * {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class SageMakerTransformOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.sagemaker.SageMakerTransformOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.sagemaker", "SageMakerTransformOperator")
          .params(
              "config",
              "wait_for_completion",
              "check_interval",
              "max_attempts",
              "max_ingestion_time",
              "check_if_job_exists",
              "action_if_job_exists",
              "check_if_model_exists",
              "action_if_model_exists",
              "deferrable",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("config")
          .templated("aws_conn_id", "config", "region_name", "verify")
          .templateFieldsRenderers(Map.of("config", "json"))
          .uiColor("#ededed");

  private SageMakerTransformOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code SageMakerTransformOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The configuration necessary to start a transform job (templated). Templated. */
    public Builder config(Map<String, Object> value) {
      return param("config", value);
    }

    /** The configuration necessary to start a transform job (templated). Templated. */
    public Builder config(TaskRef<?> value) {
      return param("config", value);
    }

    /** Set to True to wait until the transform job finishes. Defaults to true. */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /**
     * If wait is set to True, the time interval, in seconds, that this operation waits to
     * check the status of the transform job. Defaults to 30.
     */
    public Builder checkInterval(long value) {
      return param("check_interval", value);
    }

    /**
     * Number of times to poll for query state before returning the current state, defaults to
     * None.
     */
    public Builder maxAttempts(long value) {
      return param("max_attempts", value);
    }

    /**
     * If wait is set to True, the operation fails if the transform job doesn't finish within
     * max_ingestion_time seconds. If you set this parameter to None, the operation does not
     * timeout.
     */
    public Builder maxIngestionTime(long value) {
      return param("max_ingestion_time", value);
    }

    /**
     * If set to true, then the operator will check whether a transform job already exists for
     * the name in the config. Defaults to true.
     */
    public Builder checkIfJobExists(boolean value) {
      return param("check_if_job_exists", value);
    }

    /**
     * Behaviour if the job name already exists. Possible options are "timestamp" (default) and
     * "fail". This is only relevant if check_if_job_exists is True. Defaults to "timestamp".
     */
    public Builder actionIfJobExists(String value) {
      return param("action_if_job_exists", value);
    }

    /** Defaults to true. */
    public Builder checkIfModelExists(boolean value) {
      return param("check_if_model_exists", value);
    }

    /** Defaults to "timestamp". */
    public Builder actionIfModelExists(String value) {
      return param("action_if_model_exists", value);
    }

    /** Defaults to false. */
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
