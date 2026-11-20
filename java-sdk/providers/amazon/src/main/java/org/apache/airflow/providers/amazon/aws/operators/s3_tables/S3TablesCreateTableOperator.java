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
 * Create a new table in an Amazon S3 Tables namespace.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.s3_tables.S3TablesCreateTableOperator} of
 * {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class S3TablesCreateTableOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.s3_tables.S3TablesCreateTableOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.s3_tables", "S3TablesCreateTableOperator")
          .params(
              "table_bucket_arn",
              "namespace",
              "table_name",
              "format",
              "metadata",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("table_bucket_arn", "namespace", "table_name")
          .templated(
              "aws_conn_id",
              "format",
              "metadata",
              "namespace",
              "region_name",
              "table_bucket_arn",
              "table_name",
              "verify")
          .templateFieldsRenderers(Map.of("metadata", "json"));

  private S3TablesCreateTableOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code S3TablesCreateTableOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The ARN of the table bucket to create the table in. (templated) Templated. */
    public Builder tableBucketArn(String value) {
      return param("table_bucket_arn", value);
    }

    /** The ARN of the table bucket to create the table in. (templated) Templated. */
    public Builder tableBucketArn(TaskRef<?> value) {
      return param("table_bucket_arn", value);
    }

    /** The namespace to associate with the table. (templated) Templated. */
    public Builder namespace(String value) {
      return param("namespace", value);
    }

    /** The namespace to associate with the table. (templated) Templated. */
    public Builder namespace(TaskRef<?> value) {
      return param("namespace", value);
    }

    /** The name of the table. (templated) Templated. */
    public Builder tableName(String value) {
      return param("table_name", value);
    }

    /** The name of the table. (templated) Templated. */
    public Builder tableName(TaskRef<?> value) {
      return param("table_name", value);
    }

    /**
     * The table format. (templated) Currently only `ICEBERG` is supported. Defaults to
     * "ICEBERG". Templated.
     */
    public Builder format(String value) {
      return param("format", value);
    }

    /**
     * The table format. (templated) Currently only `ICEBERG` is supported. Defaults to
     * "ICEBERG". Templated.
     */
    public Builder format(TaskRef<?> value) {
      return param("format", value);
    }

    /**
     * Optional Iceberg schema metadata. (templated) Example: `{"iceberg": {"schema":
     * {"fields": [{"name": "id", "type": "int", "required": True}]}}}` Templated.
     */
    public Builder metadata(Map<String, Object> value) {
      return param("metadata", value);
    }

    /**
     * Optional Iceberg schema metadata. (templated) Example: `{"iceberg": {"schema":
     * {"fields": [{"name": "id", "type": "int", "required": True}]}}}` Templated.
     */
    public Builder metadata(TaskRef<?> value) {
      return param("metadata", value);
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
