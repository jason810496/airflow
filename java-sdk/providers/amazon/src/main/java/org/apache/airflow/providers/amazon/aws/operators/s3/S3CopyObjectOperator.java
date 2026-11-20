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
 * Creates a copy of an object that is already stored in S3.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.s3.S3CopyObjectOperator} of {@code
 * apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a Python
 * worker serves.
 */
public final class S3CopyObjectOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.s3.S3CopyObjectOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.s3", "S3CopyObjectOperator")
          .params(
              "source_bucket_key",
              "dest_bucket_key",
              "source_bucket_name",
              "dest_bucket_name",
              "source_version_id",
              "acl_policy",
              "meta_data_directive",
              "kms_key_id",
              "kms_encryption_type",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("source_bucket_key", "dest_bucket_key")
          .templated(
              "aws_conn_id",
              "dest_bucket_key",
              "dest_bucket_name",
              "region_name",
              "source_bucket_key",
              "source_bucket_name",
              "verify");

  private S3CopyObjectOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code S3CopyObjectOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The key of the source object. (templated) Templated. */
    public Builder sourceBucketKey(String value) {
      return param("source_bucket_key", value);
    }

    /** The key of the source object. (templated) Templated. */
    public Builder sourceBucketKey(TaskRef<?> value) {
      return param("source_bucket_key", value);
    }

    /** The key of the object to copy to. (templated) Templated. */
    public Builder destBucketKey(String value) {
      return param("dest_bucket_key", value);
    }

    /** The key of the object to copy to. (templated) Templated. */
    public Builder destBucketKey(TaskRef<?> value) {
      return param("dest_bucket_key", value);
    }

    /** Name of the S3 bucket where the source object is in. (templated) Templated. */
    public Builder sourceBucketName(String value) {
      return param("source_bucket_name", value);
    }

    /** Name of the S3 bucket where the source object is in. (templated) Templated. */
    public Builder sourceBucketName(TaskRef<?> value) {
      return param("source_bucket_name", value);
    }

    /** Name of the S3 bucket to where the object is copied. (templated) Templated. */
    public Builder destBucketName(String value) {
      return param("dest_bucket_name", value);
    }

    /** Name of the S3 bucket to where the object is copied. (templated) Templated. */
    public Builder destBucketName(TaskRef<?> value) {
      return param("dest_bucket_name", value);
    }

    /** Version ID of the source object (OPTIONAL) */
    public Builder sourceVersionId(String value) {
      return param("source_version_id", value);
    }

    /** String specifying the canned ACL policy for the file being uploaded to the S3 bucket. */
    public Builder aclPolicy(String value) {
      return param("acl_policy", value);
    }

    /**
     * Whether to `COPY` the metadata from the source object or `REPLACE` it with metadata
     * that's provided in the request.
     */
    public Builder metaDataDirective(String value) {
      return param("meta_data_directive", value);
    }

    /**
     * The ARN, id or alias of the AWS KMS key to use for encrypting the destination object.
     * Required if using KMS-based server-side encryption with a non-default key.
     */
    public Builder kmsKeyId(String value) {
      return param("kms_key_id", value);
    }

    /**
     * Type of KMS encryption to use for the object. Can be either "aws:kms" (standard KMS) or
     * "aws:kms:dsse" (double-shielded KMS).
     */
    public Builder kmsEncryptionType(String value) {
      return param("kms_encryption_type", value);
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
