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

package org.apache.airflow.providers.amazon.aws.operators.s3;

import java.util.List;
import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Copies data from a source S3 location to a temporary location on the local filesystem.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.s3.S3FileTransformOperator} of {@code
 * apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a Python
 * worker serves.
 */
public final class S3FileTransformOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.s3.S3FileTransformOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.s3", "S3FileTransformOperator")
          .params(
              "source_s3_key",
              "dest_s3_key",
              "transform_script",
              "select_expression",
              "select_expr_serialization_config",
              "script_args",
              "source_aws_conn_id",
              "source_verify",
              "dest_aws_conn_id",
              "dest_verify",
              "replace",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("source_s3_key", "dest_s3_key")
          .templated(
              "aws_conn_id",
              "dest_aws_conn_id",
              "dest_s3_key",
              "region_name",
              "script_args",
              "source_aws_conn_id",
              "source_s3_key",
              "verify")
          .uiColor("#f9c915");

  private S3FileTransformOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code S3FileTransformOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The key to be retrieved from S3. (templated) Templated. */
    public Builder sourceS3Key(String value) {
      return param("source_s3_key", value);
    }

    /** The key to be retrieved from S3. (templated) Templated. */
    public Builder sourceS3Key(TaskRef<?> value) {
      return param("source_s3_key", value);
    }

    /** The key to be written from S3. (templated) Templated. */
    public Builder destS3Key(String value) {
      return param("dest_s3_key", value);
    }

    /** The key to be written from S3. (templated) Templated. */
    public Builder destS3Key(TaskRef<?> value) {
      return param("dest_s3_key", value);
    }

    /** location of the executable transformation script */
    public Builder transformScript(String value) {
      return param("transform_script", value);
    }

    /** S3 Select expression */
    public Builder selectExpression(Object value) {
      return param("select_expression", value);
    }

    /** A dictionary that contains input and output serialization configurations for S3 Select. */
    public Builder selectExprSerializationConfig(Map<String, Map<String, Map<String, Object>>> value) {
      return param("select_expr_serialization_config", value);
    }

    /** arguments for transformation script (templated) Templated. */
    public Builder scriptArgs(List<String> value) {
      return param("script_args", value);
    }

    /** arguments for transformation script (templated) Templated. */
    public Builder scriptArgs(TaskRef<?> value) {
      return param("script_args", value);
    }

    /** source s3 connection Defaults to "aws_default". Templated. */
    public Builder sourceAwsConnId(String value) {
      return param("source_aws_conn_id", value);
    }

    /** source s3 connection Defaults to "aws_default". Templated. */
    public Builder sourceAwsConnId(TaskRef<?> value) {
      return param("source_aws_conn_id", value);
    }

    /**
     * Whether or not to verify SSL certificates for S3 connection. By default SSL certificates
     * are verified. You can provide the following values:
     */
    public Builder sourceVerify(Object value) {
      return param("source_verify", value);
    }

    /** destination s3 connection Defaults to "aws_default". Templated. */
    public Builder destAwsConnId(String value) {
      return param("dest_aws_conn_id", value);
    }

    /** destination s3 connection Defaults to "aws_default". Templated. */
    public Builder destAwsConnId(TaskRef<?> value) {
      return param("dest_aws_conn_id", value);
    }

    /** Whether or not to verify SSL certificates for S3 connection. See: `source_verify` */
    public Builder destVerify(Object value) {
      return param("dest_verify", value);
    }

    /** Replace dest S3 key if it already exists Defaults to false. */
    public Builder replace(boolean value) {
      return param("replace", value);
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
