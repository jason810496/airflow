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

package org.apache.airflow.providers.amazon.aws.operators.sagemaker_unified_studio;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Provides Jupyter notebook artifact execution functionality for Sagemaker Unified Studio
 * Workflows.
 *
 * Runs {@code
 * airflow.providers.amazon.aws.operators.sagemaker_unified_studio.SageMakerNotebookOperator}
 * of {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that
 * a Python worker serves.
 */
public final class SageMakerNotebookOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.sagemaker_unified_studio.SageMakerNotebookOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.sagemaker_unified_studio", "SageMakerNotebookOperator")
          .params(
              "task_id",
              "input_config",
              "domain_id",
              "project_id",
              "output_config",
              "domain_region",
              "compute",
              "termination_condition",
              "tags",
              "wait_for_completion",
              "waiter_delay",
              "waiter_max_attempts",
              "deferrable")
          .required("task_id", "input_config")
          .templated(
              "domain_id",
              "project_id",
              "domain_region",
              "input_config",
              "output_config",
              "compute",
              "termination_condition",
              "tags");

  private SageMakerNotebookOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code SageMakerNotebookOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** A unique, meaningful id for the task. */
    public Builder taskId(String value) {
      return param("task_id", value);
    }

    /**
     * Configuration for the input file. Input path should be specified as a relative path. The
     * provided relative path will be automatically resolved to an absolute path within the
     * context of the user's home directory in the IDE. Input params should be a dict. Example:
     * {'input_path': 'folder/input/notebook.ipynb', 'input_params':{'key': 'value'}}
     * Templated.
     */
    public Builder inputConfig(Map<String, Object> value) {
      return param("input_config", value);
    }

    /**
     * Configuration for the input file. Input path should be specified as a relative path. The
     * provided relative path will be automatically resolved to an absolute path within the
     * context of the user's home directory in the IDE. Input params should be a dict. Example:
     * {'input_path': 'folder/input/notebook.ipynb', 'input_params':{'key': 'value'}}
     * Templated.
     */
    public Builder inputConfig(TaskRef<?> value) {
      return param("input_config", value);
    }

    /**
     * The domain ID for Amazon SageMaker Unified Studio. Optional - if not provided, the SDK
     * will attempt to resolve it from the environment. Templated.
     */
    public Builder domainId(String value) {
      return param("domain_id", value);
    }

    /**
     * The domain ID for Amazon SageMaker Unified Studio. Optional - if not provided, the SDK
     * will attempt to resolve it from the environment. Templated.
     */
    public Builder domainId(TaskRef<?> value) {
      return param("domain_id", value);
    }

    /**
     * The project ID for Amazon SageMaker Unified Studio. Optional - if not provided, the SDK
     * will attempt to resolve it from the environment. Templated.
     */
    public Builder projectId(String value) {
      return param("project_id", value);
    }

    /**
     * The project ID for Amazon SageMaker Unified Studio. Optional - if not provided, the SDK
     * will attempt to resolve it from the environment. Templated.
     */
    public Builder projectId(TaskRef<?> value) {
      return param("project_id", value);
    }

    /**
     * Configuration for the output format. It should include an output_format parameter to
     * control the format of the notebook execution output. Example: {"output_formats":
     * ["NOTEBOOK"]} Templated.
     */
    public Builder outputConfig(Map<String, Object> value) {
      return param("output_config", value);
    }

    /**
     * Configuration for the output format. It should include an output_format parameter to
     * control the format of the notebook execution output. Example: {"output_formats":
     * ["NOTEBOOK"]} Templated.
     */
    public Builder outputConfig(TaskRef<?> value) {
      return param("output_config", value);
    }

    /**
     * The AWS region for the domain. If not provided, the default AWS region will be used.
     * Templated.
     */
    public Builder domainRegion(String value) {
      return param("domain_region", value);
    }

    /**
     * The AWS region for the domain. If not provided, the default AWS region will be used.
     * Templated.
     */
    public Builder domainRegion(TaskRef<?> value) {
      return param("domain_region", value);
    }

    /**
     * compute configuration to use for the artifact execution. This is a required attribute if
     * the execution is on a remote compute. Example:: Templated.
     */
    public Builder compute(Map<String, Object> value) {
      return param("compute", value);
    }

    /**
     * compute configuration to use for the artifact execution. This is a required attribute if
     * the execution is on a remote compute. Example:: Templated.
     */
    public Builder compute(TaskRef<?> value) {
      return param("compute", value);
    }

    /**
     * conditions to match to terminate the remote execution. Example: `{"MaxRuntimeInSeconds":
     * 3600}` Templated.
     */
    public Builder terminationCondition(Map<String, Object> value) {
      return param("termination_condition", value);
    }

    /**
     * conditions to match to terminate the remote execution. Example: `{"MaxRuntimeInSeconds":
     * 3600}` Templated.
     */
    public Builder terminationCondition(TaskRef<?> value) {
      return param("termination_condition", value);
    }

    /**
     * tags to be associated with the remote execution runs. Example: `{"md_analytics":
     * "logs"}` Templated.
     */
    public Builder tags(Map<String, Object> value) {
      return param("tags", value);
    }

    /**
     * tags to be associated with the remote execution runs. Example: `{"md_analytics":
     * "logs"}` Templated.
     */
    public Builder tags(TaskRef<?> value) {
      return param("tags", value);
    }

    /**
     * Indicates whether to wait for the notebook execution to complete. If True, wait for
     * completion; if False, don't wait. Defaults to true.
     */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /** Interval in seconds to check the notebook execution status. Defaults to 10. */
    public Builder waiterDelay(long value) {
      return param("waiter_delay", value);
    }

    /** Number of attempts to wait before returning FAILED. Defaults to 1440. */
    public Builder waiterMaxAttempts(long value) {
      return param("waiter_max_attempts", value);
    }

    /**
     * If True, the operator will wait asynchronously for the job to complete. Deferring when
     * `wait_for_completion` is False is deprecated; from the next major release this has no
     * effect when `wait_for_completion` is False. This mode requires aiobotocore module to be
     * installed. (default: False) Defaults to false.
     */
    public Builder deferrable(boolean value) {
      return param("deferrable", value);
    }
  }
}
