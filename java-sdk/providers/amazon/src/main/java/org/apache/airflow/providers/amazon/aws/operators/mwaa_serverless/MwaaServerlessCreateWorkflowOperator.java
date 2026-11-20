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

package org.apache.airflow.providers.amazon.aws.operators.mwaa_serverless;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Create an Amazon MWAA Serverless workflow.
 *
 * Runs {@code
 * airflow.providers.amazon.aws.operators.mwaa_serverless.MwaaServerlessCreateWorkflowOperator}
 * of {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that
 * a Python worker serves.
 */
public final class MwaaServerlessCreateWorkflowOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.mwaa_serverless.MwaaServerlessCreateWorkflowOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.mwaa_serverless", "MwaaServerlessCreateWorkflowOperator")
          .params(
              "workflow_name",
              "definition_s3_location",
              "code",
              "role_arn",
              "description",
              "environment_variables",
              "tags",
              "create_workflow_kwargs",
              "if_exists",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("workflow_name", "definition_s3_location", "role_arn")
          .templated(
              "aws_conn_id",
              "code",
              "create_workflow_kwargs",
              "definition_s3_location",
              "description",
              "environment_variables",
              "region_name",
              "role_arn",
              "verify",
              "workflow_name")
          .templateFieldsRenderers(Map.of("definition_s3_location", "json", "code", "json", "environment_variables", "json", "create_workflow_kwargs", "json"));

  private MwaaServerlessCreateWorkflowOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code MwaaServerlessCreateWorkflowOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The name of the workflow. (templated) Templated. */
    public Builder workflowName(String value) {
      return param("workflow_name", value);
    }

    /** The name of the workflow. (templated) Templated. */
    public Builder workflowName(TaskRef<?> value) {
      return param("workflow_name", value);
    }

    /** Dict with `Bucket` and `ObjectKey` for the YAML definition. (templated) Templated. */
    public Builder definitionS3Location(Map<String, String> value) {
      return param("definition_s3_location", value);
    }

    /** Dict with `Bucket` and `ObjectKey` for the YAML definition. (templated) Templated. */
    public Builder definitionS3Location(TaskRef<?> value) {
      return param("definition_s3_location", value);
    }

    /**
     * Optional code location for `PythonOperator` and `BashOperator` tasks, as a dict with an
     * `S3Location` key containing `Bucket`, `ObjectKey`, and optionally `VersionId`.
     * (templated) Templated.
     */
    public Builder code(Map<String, Object> value) {
      return param("code", value);
    }

    /**
     * Optional code location for `PythonOperator` and `BashOperator` tasks, as a dict with an
     * `S3Location` key containing `Bucket`, `ObjectKey`, and optionally `VersionId`.
     * (templated) Templated.
     */
    public Builder code(TaskRef<?> value) {
      return param("code", value);
    }

    /** The execution role ARN. (templated) Templated. */
    public Builder roleArn(String value) {
      return param("role_arn", value);
    }

    /** The execution role ARN. (templated) Templated. */
    public Builder roleArn(TaskRef<?> value) {
      return param("role_arn", value);
    }

    /** Optional description. (templated) Templated. */
    public Builder description(String value) {
      return param("description", value);
    }

    /** Optional description. (templated) Templated. */
    public Builder description(TaskRef<?> value) {
      return param("description", value);
    }

    /**
     * Optional mapping of environment variable names to values to set for tasks in the
     * workflow. Sent to the API as `Environment.Variables`. If not specified, the workflow has
     * no environment variables. Requires `botocore&gt;=1.43.124`. (templated) Templated.
     */
    public Builder environmentVariables(Map<String, String> value) {
      return param("environment_variables", value);
    }

    /**
     * Optional mapping of environment variable names to values to set for tasks in the
     * workflow. Sent to the API as `Environment.Variables`. If not specified, the workflow has
     * no environment variables. Requires `botocore&gt;=1.43.124`. (templated) Templated.
     */
    public Builder environmentVariables(TaskRef<?> value) {
      return param("environment_variables", value);
    }

    /** Optional tags dict. */
    public Builder tags(Map<String, String> value) {
      return param("tags", value);
    }

    /**
     * Optional parameters to pass to the `CreateWorkflow` API for inputs this operator does
     * not expose, such as `NetworkConfiguration`, `LoggingConfiguration`, `EngineVersion`,
     * `TriggerMode`, `EncryptionConfiguration` and `ClientToken`. (templated) Templated.
     */
    public Builder createWorkflowKwargs(Map<String, Object> value) {
      return param("create_workflow_kwargs", value);
    }

    /**
     * Optional parameters to pass to the `CreateWorkflow` API for inputs this operator does
     * not expose, such as `NetworkConfiguration`, `LoggingConfiguration`, `EngineVersion`,
     * `TriggerMode`, `EncryptionConfiguration` and `ClientToken`. (templated) Templated.
     */
    public Builder createWorkflowKwargs(TaskRef<?> value) {
      return param("create_workflow_kwargs", value);
    }

    /**
     * Behavior when the workflow already exists. `"fail"` raises an error, `"skip"` returns
     * the existing ARN. One of "fail", "skip". Defaults to "skip".
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
