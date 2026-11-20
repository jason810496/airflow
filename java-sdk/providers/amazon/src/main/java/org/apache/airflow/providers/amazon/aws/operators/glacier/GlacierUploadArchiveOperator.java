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

package org.apache.airflow.providers.amazon.aws.operators.glacier;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * This operator add an archive to an Amazon S3 Glacier vault.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.glacier.GlacierUploadArchiveOperator} of
 * {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class GlacierUploadArchiveOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.glacier.GlacierUploadArchiveOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.glacier", "GlacierUploadArchiveOperator")
          .params(
              "vault_name",
              "body",
              "checksum",
              "archive_description",
              "account_id",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("vault_name", "body")
          .templated("aws_conn_id", "region_name", "vault_name", "verify");

  private GlacierUploadArchiveOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code GlacierUploadArchiveOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The name of the vault Templated. */
    public Builder vaultName(String value) {
      return param("vault_name", value);
    }

    /** The name of the vault Templated. */
    public Builder vaultName(TaskRef<?> value) {
      return param("vault_name", value);
    }

    /** A bytes or seekable file-like object. The data to upload. */
    public Builder body(Object value) {
      return param("body", value);
    }

    /**
     * The SHA256 tree hash of the data being uploaded. This parameter is automatically
     * populated if it is not provided
     */
    public Builder checksum(String value) {
      return param("checksum", value);
    }

    /** The description of the archive you are uploading */
    public Builder archiveDescription(String value) {
      return param("archive_description", value);
    }

    /**
     * (Optional) AWS account ID of the account that owns the vault. Defaults to the
     * credentials used to sign the request
     */
    public Builder accountId(String value) {
      return param("account_id", value);
    }

    /** The reference to the AWS connection details Defaults to "aws_default". Templated. */
    public Builder awsConnId(String value) {
      return param("aws_conn_id", value);
    }

    /** The reference to the AWS connection details Defaults to "aws_default". Templated. */
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
