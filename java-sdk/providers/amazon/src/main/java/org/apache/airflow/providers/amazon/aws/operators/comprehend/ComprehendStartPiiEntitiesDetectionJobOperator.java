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

package org.apache.airflow.providers.amazon.aws.operators.comprehend;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Create a comprehend pii entities detection job for a collection of documents.
 *
 * Runs {@code
 * airflow.providers.amazon.aws.operators.comprehend.ComprehendStartPiiEntitiesDetectionJobOperator}
 * of {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that
 * a Python worker serves.
 */
public final class ComprehendStartPiiEntitiesDetectionJobOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.comprehend.ComprehendStartPiiEntitiesDetectionJobOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.comprehend", "ComprehendStartPiiEntitiesDetectionJobOperator")
          .params(
              "input_data_config",
              "output_data_config",
              "mode",
              "data_access_role_arn",
              "language_code",
              "start_pii_entities_kwargs",
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
              "input_data_config",
              "output_data_config",
              "mode",
              "data_access_role_arn",
              "language_code")
          .templated(
              "aws_conn_id",
              "data_access_role_arn",
              "input_data_config",
              "language_code",
              "output_data_config",
              "region_name",
              "verify")
          .templateFieldsRenderers(Map.of("input_data_config", "json", "output_data_config", "json"));

  private ComprehendStartPiiEntitiesDetectionJobOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code ComprehendStartPiiEntitiesDetectionJobOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The input properties for a PII entities detection job. (templated) Templated. */
    public Builder inputDataConfig(Map<String, Object> value) {
      return param("input_data_config", value);
    }

    /** The input properties for a PII entities detection job. (templated) Templated. */
    public Builder inputDataConfig(TaskRef<?> value) {
      return param("input_data_config", value);
    }

    /**
     * Provides `con&#64257;guration` parameters for the output of PII entity detection jobs.
     * (templated) Templated.
     */
    public Builder outputDataConfig(Map<String, Object> value) {
      return param("output_data_config", value);
    }

    /**
     * Provides `con&#64257;guration` parameters for the output of PII entity detection jobs.
     * (templated) Templated.
     */
    public Builder outputDataConfig(TaskRef<?> value) {
      return param("output_data_config", value);
    }

    /**
     * Specifies whether the output provides the locations (offsets) of PII entities or a file
     * in which PII entities are redacted. If you set the mode parameter to ONLY_REDACTION. In
     * that case you must provide a RedactionConfig in start_pii_entities_kwargs.
     */
    public Builder mode(String value) {
      return param("mode", value);
    }

    /**
     * The Amazon Resource Name (ARN) of the IAM role that grants Amazon Comprehend read access
     * to your input data. (templated) Templated.
     */
    public Builder dataAccessRoleArn(String value) {
      return param("data_access_role_arn", value);
    }

    /**
     * The Amazon Resource Name (ARN) of the IAM role that grants Amazon Comprehend read access
     * to your input data. (templated) Templated.
     */
    public Builder dataAccessRoleArn(TaskRef<?> value) {
      return param("data_access_role_arn", value);
    }

    /** The language of the input documents. (templated) Templated. */
    public Builder languageCode(String value) {
      return param("language_code", value);
    }

    /** The language of the input documents. (templated) Templated. */
    public Builder languageCode(TaskRef<?> value) {
      return param("language_code", value);
    }

    /**
     * Any optional parameters to pass to the job. If JobName is not provided in
     * start_pii_entities_kwargs, operator will create.
     */
    public Builder startPiiEntitiesKwargs(Map<String, Object> value) {
      return param("start_pii_entities_kwargs", value);
    }

    /** Whether to wait for job to stop. (default: True) Defaults to true. */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
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
     * If True, the operator will wait asynchronously for the job to stop. Deferring when
     * `wait_for_completion` is False is deprecated; from the next major release this has no
     * effect when `wait_for_completion` is False. This mode requires aiobotocore module to be
     * installed. (default: False) Defaults to false.
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
     * Whether to verify SSL certificates. See:
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
