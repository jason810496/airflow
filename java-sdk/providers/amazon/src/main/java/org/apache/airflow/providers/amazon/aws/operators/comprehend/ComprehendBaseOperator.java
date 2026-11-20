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
 * This is the base operator for Comprehend Service operators (not supposed to be used directly
 * in Dags).
 *
 * Runs {@code airflow.providers.amazon.aws.operators.comprehend.ComprehendBaseOperator} of
 * {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class ComprehendBaseOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.comprehend.ComprehendBaseOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.comprehend", "ComprehendBaseOperator")
          .params(
              "input_data_config",
              "output_data_config",
              "data_access_role_arn",
              "language_code",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required(
              "input_data_config",
              "output_data_config",
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

  private ComprehendBaseOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code ComprehendBaseOperator}, to pass to {@code DagDef.task}. */
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
