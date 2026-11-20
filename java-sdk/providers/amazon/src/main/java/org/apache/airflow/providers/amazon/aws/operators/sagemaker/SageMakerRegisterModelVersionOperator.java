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
 * Register a SageMaker model by creating a model version that specifies the model group to
 * which it belongs.
 *
 * Runs {@code
 * airflow.providers.amazon.aws.operators.sagemaker.SageMakerRegisterModelVersionOperator} of
 * {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a
 * Python worker serves.
 *
 * Not available from Java, because JSON cannot carry them: {@code model_approval}.
 */
public final class SageMakerRegisterModelVersionOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.sagemaker.SageMakerRegisterModelVersionOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.sagemaker", "SageMakerRegisterModelVersionOperator")
          .params(
              "image_uri",
              "model_url",
              "package_group_name",
              "package_group_desc",
              "package_desc",
              "extras",
              "config",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("image_uri", "model_url", "package_group_name")
          .templated(
              "aws_conn_id",
              "image_uri",
              "model_approval",
              "model_url",
              "package_desc",
              "package_group_desc",
              "package_group_name",
              "region_name",
              "verify")
          .templateFieldsRenderers(Map.of("config", "json"))
          .uiColor("#ededed");

  private SageMakerRegisterModelVersionOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code SageMakerRegisterModelVersionOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /**
     * The Amazon EC2 Container Registry (Amazon ECR) path where inference code is stored.
     * Templated.
     */
    public Builder imageUri(String value) {
      return param("image_uri", value);
    }

    /**
     * The Amazon EC2 Container Registry (Amazon ECR) path where inference code is stored.
     * Templated.
     */
    public Builder imageUri(TaskRef<?> value) {
      return param("image_uri", value);
    }

    /**
     * The Amazon S3 path where the model artifacts (the trained weights of the model), which
     * result from model training, are stored. This path must point to a single gzip compressed
     * tar archive (.tar.gz suffix). Templated.
     */
    public Builder modelUrl(String value) {
      return param("model_url", value);
    }

    /**
     * The Amazon S3 path where the model artifacts (the trained weights of the model), which
     * result from model training, are stored. This path must point to a single gzip compressed
     * tar archive (.tar.gz suffix). Templated.
     */
    public Builder modelUrl(TaskRef<?> value) {
      return param("model_url", value);
    }

    /**
     * The name of the model package group that the model is going to be registered to. Will be
     * created if it doesn't already exist. Templated.
     */
    public Builder packageGroupName(String value) {
      return param("package_group_name", value);
    }

    /**
     * The name of the model package group that the model is going to be registered to. Will be
     * created if it doesn't already exist. Templated.
     */
    public Builder packageGroupName(TaskRef<?> value) {
      return param("package_group_name", value);
    }

    /**
     * Description of the model package group, if it was to be created (optional). Defaults to
     * "". Templated.
     */
    public Builder packageGroupDesc(String value) {
      return param("package_group_desc", value);
    }

    /**
     * Description of the model package group, if it was to be created (optional). Defaults to
     * "". Templated.
     */
    public Builder packageGroupDesc(TaskRef<?> value) {
      return param("package_group_desc", value);
    }

    /** Description of the model package (optional). Defaults to "". Templated. */
    public Builder packageDesc(String value) {
      return param("package_desc", value);
    }

    /** Description of the model package (optional). Defaults to "". Templated. */
    public Builder packageDesc(TaskRef<?> value) {
      return param("package_desc", value);
    }

    /**
     * Can contain extra parameters for the boto call to create_model_package, and/or overrides
     * for any parameter defined above. For a complete list of available parameters, see
     * https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.create_model_package
     */
    public Builder extras(Map<String, Object> value) {
      return param("extras", value);
    }

    /** The configuration necessary to start a training job (templated) */
    public Builder config(Map<String, Object> value) {
      return param("config", value);
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
