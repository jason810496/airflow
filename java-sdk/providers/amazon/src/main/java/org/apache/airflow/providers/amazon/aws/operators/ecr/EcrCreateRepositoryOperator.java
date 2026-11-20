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

package org.apache.airflow.providers.amazon.aws.operators.ecr;

import java.util.List;
import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Create an Amazon ECR repository.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.ecr.EcrCreateRepositoryOperator} of
 * {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class EcrCreateRepositoryOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.ecr.EcrCreateRepositoryOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.ecr", "EcrCreateRepositoryOperator")
          .params(
              "repository_name",
              "registry_id",
              "tags",
              "image_tag_mutability",
              "image_tag_mutability_exclusion_filters",
              "image_scanning_configuration",
              "encryption_configuration",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("repository_name")
          .templated(
              "aws_conn_id",
              "encryption_configuration",
              "image_scanning_configuration",
              "image_tag_mutability",
              "image_tag_mutability_exclusion_filters",
              "region_name",
              "registry_id",
              "repository_name",
              "tags",
              "verify")
          .templateFieldsRenderers(Map.of("tags", "json", "image_tag_mutability_exclusion_filters", "json", "image_scanning_configuration", "json", "encryption_configuration", "json"));

  private EcrCreateRepositoryOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code EcrCreateRepositoryOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The name of the repository to create. (templated) Templated. */
    public Builder repositoryName(String value) {
      return param("repository_name", value);
    }

    /** The name of the repository to create. (templated) Templated. */
    public Builder repositoryName(TaskRef<?> value) {
      return param("repository_name", value);
    }

    /** The AWS account ID associated with the registry. (templated) Templated. */
    public Builder registryId(String value) {
      return param("registry_id", value);
    }

    /** The AWS account ID associated with the registry. (templated) Templated. */
    public Builder registryId(TaskRef<?> value) {
      return param("registry_id", value);
    }

    /** Metadata to apply to the repository. (templated) Templated. */
    public Builder tags(List<Map<String, String>> value) {
      return param("tags", value);
    }

    /** Metadata to apply to the repository. (templated) Templated. */
    public Builder tags(TaskRef<?> value) {
      return param("tags", value);
    }

    /** The tag mutability setting for the repository. (templated) Templated. */
    public Builder imageTagMutability(String value) {
      return param("image_tag_mutability", value);
    }

    /** The tag mutability setting for the repository. (templated) Templated. */
    public Builder imageTagMutability(TaskRef<?> value) {
      return param("image_tag_mutability", value);
    }

    /**
     * Filters that override the repository's image tag mutability setting. (templated)
     * Templated.
     */
    public Builder imageTagMutabilityExclusionFilters(List<Map<String, String>> value) {
      return param("image_tag_mutability_exclusion_filters", value);
    }

    /**
     * Filters that override the repository's image tag mutability setting. (templated)
     * Templated.
     */
    public Builder imageTagMutabilityExclusionFilters(TaskRef<?> value) {
      return param("image_tag_mutability_exclusion_filters", value);
    }

    /** The image scanning configuration for the repository. (templated) Templated. */
    public Builder imageScanningConfiguration(Map<String, Boolean> value) {
      return param("image_scanning_configuration", value);
    }

    /** The image scanning configuration for the repository. (templated) Templated. */
    public Builder imageScanningConfiguration(TaskRef<?> value) {
      return param("image_scanning_configuration", value);
    }

    /** The encryption configuration for the repository. (templated) Templated. */
    public Builder encryptionConfiguration(Map<String, String> value) {
      return param("encryption_configuration", value);
    }

    /** The encryption configuration for the repository. (templated) Templated. */
    public Builder encryptionConfiguration(TaskRef<?> value) {
      return param("encryption_configuration", value);
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
