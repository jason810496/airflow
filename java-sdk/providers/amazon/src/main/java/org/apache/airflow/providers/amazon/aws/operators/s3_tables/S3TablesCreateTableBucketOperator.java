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

package org.apache.airflow.providers.amazon.aws.operators.s3_tables;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Create an Amazon S3 Tables table bucket.
 *
 * Runs {@code
 * airflow.providers.amazon.aws.operators.s3_tables.S3TablesCreateTableBucketOperator} of
 * {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class S3TablesCreateTableBucketOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.s3_tables.S3TablesCreateTableBucketOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.s3_tables", "S3TablesCreateTableBucketOperator")
          .params(
              "table_bucket_name",
              "encryption_configuration",
              "tags",
              "if_exists",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("table_bucket_name")
          .templated("aws_conn_id", "region_name", "table_bucket_name", "verify")
          .templateFieldsRenderers(Map.of("encryption_configuration", "json"));

  private S3TablesCreateTableBucketOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code S3TablesCreateTableBucketOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The name of the table bucket. (templated) Templated. */
    public Builder tableBucketName(String value) {
      return param("table_bucket_name", value);
    }

    /** The name of the table bucket. (templated) Templated. */
    public Builder tableBucketName(TaskRef<?> value) {
      return param("table_bucket_name", value);
    }

    /**
     * Optional encryption configuration dict with `sseAlgorithm` and optional `kmsKeyArn`.
     * (templated)
     */
    public Builder encryptionConfiguration(Map<String, String> value) {
      return param("encryption_configuration", value);
    }

    public Builder tags(Map<String, String> value) {
      return param("tags", value);
    }

    /**
     * Behavior when a table bucket with the same name already exists. `"fail"` raises an
     * error, `"skip"` returns the existing bucket ARN. One of "fail", "skip". Defaults to
     * "skip".
     */
    public Builder ifExists(String value) {
      return param("if_exists", value);
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
