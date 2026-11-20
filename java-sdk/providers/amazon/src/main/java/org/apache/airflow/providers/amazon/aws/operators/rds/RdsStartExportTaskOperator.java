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

package org.apache.airflow.providers.amazon.aws.operators.rds;

import java.util.List;
import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Starts an export of a snapshot to Amazon S3. The provided IAM role must have access to the
 * S3 bucket.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.rds.RdsStartExportTaskOperator} of {@code
 * apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a Python
 * worker serves.
 */
public final class RdsStartExportTaskOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.rds.RdsStartExportTaskOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.rds", "RdsStartExportTaskOperator")
          .params(
              "export_task_identifier",
              "source_arn",
              "s3_bucket_name",
              "iam_role_arn",
              "kms_key_id",
              "s3_prefix",
              "export_only",
              "wait_for_completion",
              "waiter_interval",
              "waiter_max_attempts",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required(
              "export_task_identifier",
              "source_arn",
              "s3_bucket_name",
              "iam_role_arn",
              "kms_key_id")
          .templated(
              "aws_conn_id",
              "export_only",
              "export_task_identifier",
              "iam_role_arn",
              "kms_key_id",
              "region_name",
              "s3_bucket_name",
              "s3_prefix",
              "source_arn",
              "verify")
          .uiColor("#eeaa88")
          .uiFgcolor("#ffffff");

  private RdsStartExportTaskOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code RdsStartExportTaskOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** A unique identifier for the snapshot export task. Templated. */
    public Builder exportTaskIdentifier(String value) {
      return param("export_task_identifier", value);
    }

    /** A unique identifier for the snapshot export task. Templated. */
    public Builder exportTaskIdentifier(TaskRef<?> value) {
      return param("export_task_identifier", value);
    }

    /** The Amazon Resource Name (ARN) of the snapshot to export to Amazon S3. Templated. */
    public Builder sourceArn(String value) {
      return param("source_arn", value);
    }

    /** The Amazon Resource Name (ARN) of the snapshot to export to Amazon S3. Templated. */
    public Builder sourceArn(TaskRef<?> value) {
      return param("source_arn", value);
    }

    /** The name of the Amazon S3 bucket to export the snapshot to. Templated. */
    public Builder s3BucketName(String value) {
      return param("s3_bucket_name", value);
    }

    /** The name of the Amazon S3 bucket to export the snapshot to. Templated. */
    public Builder s3BucketName(TaskRef<?> value) {
      return param("s3_bucket_name", value);
    }

    /** The name of the IAM role to use for writing to the Amazon S3 bucket. Templated. */
    public Builder iamRoleArn(String value) {
      return param("iam_role_arn", value);
    }

    /** The name of the IAM role to use for writing to the Amazon S3 bucket. Templated. */
    public Builder iamRoleArn(TaskRef<?> value) {
      return param("iam_role_arn", value);
    }

    /** The ID of the Amazon Web Services KMS key to use to encrypt the snapshot. Templated. */
    public Builder kmsKeyId(String value) {
      return param("kms_key_id", value);
    }

    /** The ID of the Amazon Web Services KMS key to use to encrypt the snapshot. Templated. */
    public Builder kmsKeyId(TaskRef<?> value) {
      return param("kms_key_id", value);
    }

    /**
     * The Amazon S3 bucket prefix to use as the file name and path of the exported snapshot.
     * Defaults to "". Templated.
     */
    public Builder s3Prefix(String value) {
      return param("s3_prefix", value);
    }

    /**
     * The Amazon S3 bucket prefix to use as the file name and path of the exported snapshot.
     * Defaults to "". Templated.
     */
    public Builder s3Prefix(TaskRef<?> value) {
      return param("s3_prefix", value);
    }

    /** The data to be exported from the snapshot. Templated. */
    public Builder exportOnly(List<String> value) {
      return param("export_only", value);
    }

    /** The data to be exported from the snapshot. Templated. */
    public Builder exportOnly(TaskRef<?> value) {
      return param("export_only", value);
    }

    /** If True, waits for the DB snapshot export to complete. (default: True) Defaults to true. */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /**
     * The number of seconds to wait before checking the export status. (default: 30) Defaults
     * to 30.
     */
    public Builder waiterInterval(long value) {
      return param("waiter_interval", value);
    }

    /** The number of attempts to make before failing. (default: 40) Defaults to 40. */
    public Builder waiterMaxAttempts(long value) {
      return param("waiter_max_attempts", value);
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
