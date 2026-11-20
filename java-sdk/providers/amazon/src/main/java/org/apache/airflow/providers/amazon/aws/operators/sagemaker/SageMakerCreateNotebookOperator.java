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

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Create a SageMaker notebook.
 *
 * Runs {@code
 * airflow.providers.amazon.aws.operators.sagemaker.SageMakerCreateNotebookOperator} of {@code
 * apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a Python
 * worker serves.
 */
public final class SageMakerCreateNotebookOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.sagemaker.SageMakerCreateNotebookOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.sagemaker", "SageMakerCreateNotebookOperator")
          .params(
              "instance_name",
              "instance_type",
              "role_arn",
              "volume_size_in_gb",
              "volume_kms_key_id",
              "lifecycle_config_name",
              "direct_internet_access",
              "root_access",
              "create_instance_kwargs",
              "wait_for_completion",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("instance_name", "instance_type", "role_arn")
          .templated(
              "aws_conn_id",
              "create_instance_kwargs",
              "direct_internet_access",
              "instance_name",
              "instance_type",
              "lifecycle_config_name",
              "region_name",
              "role_arn",
              "root_access",
              "verify",
              "volume_kms_key_id",
              "volume_size_in_gb",
              "wait_for_completion")
          .uiColor("#ff7300");

  private SageMakerCreateNotebookOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code SageMakerCreateNotebookOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The name of the notebook instance. Templated. */
    public Builder instanceName(String value) {
      return param("instance_name", value);
    }

    /** The name of the notebook instance. Templated. */
    public Builder instanceName(TaskRef<?> value) {
      return param("instance_name", value);
    }

    /** The type of instance to create. Templated. */
    public Builder instanceType(String value) {
      return param("instance_type", value);
    }

    /** The type of instance to create. Templated. */
    public Builder instanceType(TaskRef<?> value) {
      return param("instance_type", value);
    }

    /**
     * The Amazon Resource Name (ARN) of the IAM role that SageMaker can assume to access
     * Templated.
     */
    public Builder roleArn(String value) {
      return param("role_arn", value);
    }

    /**
     * The Amazon Resource Name (ARN) of the IAM role that SageMaker can assume to access
     * Templated.
     */
    public Builder roleArn(TaskRef<?> value) {
      return param("role_arn", value);
    }

    /** Size in GB of the EBS root device volume of the notebook instance. Templated. */
    public Builder volumeSizeInGb(long value) {
      return param("volume_size_in_gb", value);
    }

    /** Size in GB of the EBS root device volume of the notebook instance. Templated. */
    public Builder volumeSizeInGb(TaskRef<?> value) {
      return param("volume_size_in_gb", value);
    }

    /** The KMS key ID for the EBS root device volume. Templated. */
    public Builder volumeKmsKeyId(String value) {
      return param("volume_kms_key_id", value);
    }

    /** The KMS key ID for the EBS root device volume. Templated. */
    public Builder volumeKmsKeyId(TaskRef<?> value) {
      return param("volume_kms_key_id", value);
    }

    /** The name of the lifecycle configuration to associate with the notebook Templated. */
    public Builder lifecycleConfigName(String value) {
      return param("lifecycle_config_name", value);
    }

    /** The name of the lifecycle configuration to associate with the notebook Templated. */
    public Builder lifecycleConfigName(TaskRef<?> value) {
      return param("lifecycle_config_name", value);
    }

    /** Whether to enable direct internet access for the notebook instance. Templated. */
    public Builder directInternetAccess(String value) {
      return param("direct_internet_access", value);
    }

    /** Whether to enable direct internet access for the notebook instance. Templated. */
    public Builder directInternetAccess(TaskRef<?> value) {
      return param("direct_internet_access", value);
    }

    /** Whether to give the notebook instance root access to the Amazon S3 bucket. Templated. */
    public Builder rootAccess(String value) {
      return param("root_access", value);
    }

    /** Whether to give the notebook instance root access to the Amazon S3 bucket. Templated. */
    public Builder rootAccess(TaskRef<?> value) {
      return param("root_access", value);
    }

    /** Additional configuration options for the create call. Templated. */
    public Builder createInstanceKwargs(Map<String, Object> value) {
      return param("create_instance_kwargs", value);
    }

    /** Additional configuration options for the create call. Templated. */
    public Builder createInstanceKwargs(TaskRef<?> value) {
      return param("create_instance_kwargs", value);
    }

    /**
     * Whether or not to wait for the notebook to be InService before returning Defaults to
     * true. Templated.
     */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /**
     * Whether or not to wait for the notebook to be InService before returning Defaults to
     * true. Templated.
     */
    public Builder waitForCompletion(TaskRef<?> value) {
      return param("wait_for_completion", value);
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
