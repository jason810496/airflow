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

import java.util.List;
import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Use Amazon SageMaker Processing to analyze data and evaluate machine learning models on
 * Amazon SageMaker.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.sagemaker.SageMakerProcessingOperator} of
 * {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class SageMakerProcessingOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.sagemaker.SageMakerProcessingOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.sagemaker", "SageMakerProcessingOperator")
          .params(
              "config",
              "wait_for_completion",
              "print_log",
              "check_interval",
              "max_attempts",
              "max_ingestion_time",
              "action_if_job_exists",
              "deferrable",
              "output_files_to_xcom",
              "cancel_on_kill",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("config")
          .templated("aws_conn_id", "config", "region_name", "verify", "output_files_to_xcom")
          .templateFieldsRenderers(Map.of("config", "json"))
          .uiColor("#ededed");

  private SageMakerProcessingOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code SageMakerProcessingOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /**
     * The configuration necessary to start a processing job (templated). For details of the
     * configuration parameter see SageMaker.Client.create_processing_job Templated.
     */
    public Builder config(Map<String, Object> value) {
      return param("config", value);
    }

    /**
     * The configuration necessary to start a processing job (templated). For details of the
     * configuration parameter see SageMaker.Client.create_processing_job Templated.
     */
    public Builder config(TaskRef<?> value) {
      return param("config", value);
    }

    /**
     * If wait is set to True, the time interval, in seconds, that the operation waits to check
     * the status of the processing job. Defaults to true.
     */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /** if the operator should print the cloudwatch log during processing Defaults to true. */
    public Builder printLog(boolean value) {
      return param("print_log", value);
    }

    /**
     * if wait is set to be true, this is the time interval in seconds which the operator will
     * check the status of the processing job Defaults to 30.
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
     * If wait is set to True, the operation fails if the processing job doesn't finish within
     * max_ingestion_time seconds. If you set this parameter to None, the operation does not
     * timeout.
     */
    public Builder maxIngestionTime(long value) {
      return param("max_ingestion_time", value);
    }

    /**
     * Behaviour if the job name already exists. Possible options are "timestamp" (default) and
     * "fail". Defaults to "timestamp".
     */
    public Builder actionIfJobExists(String value) {
      return param("action_if_job_exists", value);
    }

    /**
     * Run operator in the deferrable mode. This is only effective if wait_for_completion is
     * set to True. Defaults to false.
     */
    public Builder deferrable(boolean value) {
      return param("deferrable", value);
    }

    /**
     * Read small JSON output files from S3 after the job completes and push their parsed
     * contents to XCom useful for feeding metrics into downstream tasks. Each entry is a dict
     * with: `output_name` (matches an output you declared in `ProcessingOutputConfig.Outputs`
     * - the operator uses it to locate that output's S3 URI), `file_name` (the file your
     * processing script wrote inside that output, and `result_name` (a label you choose for
     * the parsed contents in the returned `OutputFiles` dict). Intended for small JSON summary
     * files (metrics, evaluation reports), not large datasets. Optional; if omitted, no
     * post-job S3 reading occurs. Example:: Templated.
     */
    public Builder outputFilesToXcom(List<Map<String, Object>> value) {
      return param("output_files_to_xcom", value);
    }

    /**
     * Read small JSON output files from S3 after the job completes and push their parsed
     * contents to XCom useful for feeding metrics into downstream tasks. Each entry is a dict
     * with: `output_name` (matches an output you declared in `ProcessingOutputConfig.Outputs`
     * - the operator uses it to locate that output's S3 URI), `file_name` (the file your
     * processing script wrote inside that output, and `result_name` (a label you choose for
     * the parsed contents in the returned `OutputFiles` dict). Intended for small JSON summary
     * files (metrics, evaluation reports), not large datasets. Optional; if omitted, no
     * post-job S3 reading occurs. Example:: Templated.
     */
    public Builder outputFilesToXcom(TaskRef<?> value) {
      return param("output_files_to_xcom", value);
    }

    /**
     * If True, the processing job is stopped when the task is killed. Without it the job keeps
     * running, and billing, after the task is gone. A deferred task is stopped from the
     * Triggerer, which needs Airflow 3.3 or later; below that the flag only applies while the
     * task holds a worker slot. It does not cover a deferral that expires on its own
     * `timeout`: the Triggerer kills the trigger without calling `on_kill`, and no event
     * reaches the task, so nothing runs to stop the job. Defaults to false.
     */
    public Builder cancelOnKill(boolean value) {
      return param("cancel_on_kill", value);
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
