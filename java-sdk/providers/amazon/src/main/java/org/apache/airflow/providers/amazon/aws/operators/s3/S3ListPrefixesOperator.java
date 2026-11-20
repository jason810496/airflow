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

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * List all subfolders from the bucket with the given string prefix in name.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.s3.S3ListPrefixesOperator} of {@code
 * apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a Python
 * worker serves.
 */
public final class S3ListPrefixesOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.s3.S3ListPrefixesOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.s3", "S3ListPrefixesOperator")
          .params(
              "bucket",
              "prefix",
              "delimiter",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("bucket", "prefix", "delimiter")
          .templated("aws_conn_id", "bucket", "delimiter", "prefix", "region_name", "verify")
          .uiColor("#ffd700");

  private S3ListPrefixesOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code S3ListPrefixesOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The S3 bucket where to find the subfolders. (templated) Templated. */
    public Builder bucket(String value) {
      return param("bucket", value);
    }

    /** The S3 bucket where to find the subfolders. (templated) Templated. */
    public Builder bucket(TaskRef<?> value) {
      return param("bucket", value);
    }

    /**
     * Prefix string to filter the subfolders whose name begin with such prefix. (templated)
     * Templated.
     */
    public Builder prefix(String value) {
      return param("prefix", value);
    }

    /**
     * Prefix string to filter the subfolders whose name begin with such prefix. (templated)
     * Templated.
     */
    public Builder prefix(TaskRef<?> value) {
      return param("prefix", value);
    }

    /** the delimiter marks subfolder hierarchy. (templated) Templated. */
    public Builder delimiter(String value) {
      return param("delimiter", value);
    }

    /** the delimiter marks subfolder hierarchy. (templated) Templated. */
    public Builder delimiter(TaskRef<?> value) {
      return param("delimiter", value);
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
