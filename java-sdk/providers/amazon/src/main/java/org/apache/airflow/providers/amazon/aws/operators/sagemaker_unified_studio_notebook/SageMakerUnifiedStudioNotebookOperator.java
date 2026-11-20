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

package org.apache.airflow.providers.amazon.aws.operators.sagemaker_unified_studio_notebook;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Execute a notebook in SageMaker Unified Studio.
 *
 * Runs {@code
 * airflow.providers.amazon.aws.operators.sagemaker_unified_studio_notebook.SageMakerUnifiedStudioNotebookOperator}
 * of {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that
 * a Python worker serves.
 */
public final class SageMakerUnifiedStudioNotebookOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.sagemaker_unified_studio_notebook.SageMakerUnifiedStudioNotebookOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.sagemaker_unified_studio_notebook", "SageMakerUnifiedStudioNotebookOperator")
          .params(
              "notebook_identifier",
              "domain_identifier",
              "owning_project_identifier",
              "client_token",
              "notebook_parameters",
              "compute_configuration",
              "timeout_configuration",
              "wait_for_completion",
              "waiter_delay",
              "deferrable",
              "endpoint_url",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("notebook_identifier", "domain_identifier", "owning_project_identifier")
          .templated(
              "aws_conn_id",
              "client_token",
              "compute_configuration",
              "domain_identifier",
              "notebook_identifier",
              "notebook_parameters",
              "owning_project_identifier",
              "region_name",
              "timeout_configuration",
              "verify");

  private SageMakerUnifiedStudioNotebookOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code SageMakerUnifiedStudioNotebookOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The ID of the notebook to execute. Templated. */
    public Builder notebookIdentifier(String value) {
      return param("notebook_identifier", value);
    }

    /** The ID of the notebook to execute. Templated. */
    public Builder notebookIdentifier(TaskRef<?> value) {
      return param("notebook_identifier", value);
    }

    /** The ID of the SageMaker Unified Studio domain containing the notebook. Templated. */
    public Builder domainIdentifier(String value) {
      return param("domain_identifier", value);
    }

    /** The ID of the SageMaker Unified Studio domain containing the notebook. Templated. */
    public Builder domainIdentifier(TaskRef<?> value) {
      return param("domain_identifier", value);
    }

    /** The ID of the SageMaker Unified Studio project containing the notebook. Templated. */
    public Builder owningProjectIdentifier(String value) {
      return param("owning_project_identifier", value);
    }

    /** The ID of the SageMaker Unified Studio project containing the notebook. Templated. */
    public Builder owningProjectIdentifier(TaskRef<?> value) {
      return param("owning_project_identifier", value);
    }

    /** Optional idempotency token. Auto-generated if not provided. Templated. */
    public Builder clientToken(String value) {
      return param("client_token", value);
    }

    /** Optional idempotency token. Auto-generated if not provided. Templated. */
    public Builder clientToken(TaskRef<?> value) {
      return param("client_token", value);
    }

    /** Optional dict of parameters to pass to the notebook. Templated. */
    public Builder notebookParameters(Map<String, Object> value) {
      return param("notebook_parameters", value);
    }

    /** Optional dict of parameters to pass to the notebook. Templated. */
    public Builder notebookParameters(TaskRef<?> value) {
      return param("notebook_parameters", value);
    }

    /** Optional compute config. Example: {"instanceType": "sc.m5.large"} Templated. */
    public Builder computeConfiguration(Map<String, Object> value) {
      return param("compute_configuration", value);
    }

    /** Optional compute config. Example: {"instanceType": "sc.m5.large"} Templated. */
    public Builder computeConfiguration(TaskRef<?> value) {
      return param("compute_configuration", value);
    }

    /** Optional timeout settings. Example: {"runTimeoutInMinutes": 1440} Templated. */
    public Builder timeoutConfiguration(Map<String, Object> value) {
      return param("timeout_configuration", value);
    }

    /** Optional timeout settings. Example: {"runTimeoutInMinutes": 1440} Templated. */
    public Builder timeoutConfiguration(TaskRef<?> value) {
      return param("timeout_configuration", value);
    }

    /**
     * If True, wait for the notebook run to finish before completing the task. If False, the
     * operator returns immediately after starting the run. (default: True) Defaults to true.
     */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /** Interval in seconds to poll the notebook run status (default: 10). Defaults to 10. */
    public Builder waiterDelay(long value) {
      return param("waiter_delay", value);
    }

    /**
     * If True, the operator will defer polling to the trigger, freeing up the worker slot
     * while waiting. Deferring when `wait_for_completion` is False is deprecated; from the
     * next major release this has no effect when `wait_for_completion` is False. (default:
     * False) Defaults to false.
     */
    public Builder deferrable(boolean value) {
      return param("deferrable", value);
    }

    /** Optional custom endpoint URL for the DataZone API. */
    public Builder endpointUrl(String value) {
      return param("endpoint_url", value);
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
