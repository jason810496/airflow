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

package org.apache.airflow.providers.amazon.aws.operators.emr;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Creates an EMR JobFlow, reading the config from the EMR connection.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.emr.EmrCreateJobFlowOperator} of {@code
 * apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a Python
 * worker serves.
 */
public final class EmrCreateJobFlowOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.emr.EmrCreateJobFlowOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.emr", "EmrCreateJobFlowOperator")
          .params(
              "emr_conn_id",
              "job_flow_overrides",
              "wait_for_completion",
              "wait_policy",
              "waiter_max_attempts",
              "waiter_delay",
              "deferrable",
              "terminate_job_flow_on_failure",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .templated(
              "aws_conn_id",
              "emr_conn_id",
              "job_flow_overrides",
              "region_name",
              "verify",
              "waiter_delay",
              "waiter_max_attempts")
          .templateExt(".json")
          .templateFieldsRenderers(Map.of("job_flow_overrides", "json"))
          .uiColor("#f9c915");

  private EmrCreateJobFlowOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code EmrCreateJobFlowOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /**
     * Amazon Elastic MapReduce Connection. Use to receive an initial Amazon EMR cluster
     * configuration: `boto3.client('emr').run_job_flow` request body. If this is None or empty
     * or the connection does not exist, then an empty initial configuration is used. Defaults
     * to "emr_default". Templated.
     */
    public Builder emrConnId(String value) {
      return param("emr_conn_id", value);
    }

    /**
     * Amazon Elastic MapReduce Connection. Use to receive an initial Amazon EMR cluster
     * configuration: `boto3.client('emr').run_job_flow` request body. If this is None or empty
     * or the connection does not exist, then an empty initial configuration is used. Defaults
     * to "emr_default". Templated.
     */
    public Builder emrConnId(TaskRef<?> value) {
      return param("emr_conn_id", value);
    }

    /**
     * boto3 style arguments or reference to an arguments file (must be '.json') to override
     * specific `emr_conn_id` extra parameters. (templated) Templated.
     */
    public Builder jobFlowOverrides(Object value) {
      return param("job_flow_overrides", value);
    }

    /**
     * Whether to finish task immediately after creation (False) or wait for jobflow completion
     * (True) (default: None)
     */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /**
     * Whether to finish the task immediately after creation (None) or: - wait for the jobflow
     * completion (WaitPolicy.WAIT_FOR_COMPLETION) - wait for the jobflow completion and
     * cluster to terminate (WaitPolicy.WAIT_FOR_STEPS_COMPLETION) (default: None)
     */
    public Builder waitPolicy(String value) {
      return param("wait_policy", value);
    }

    /** Maximum number of tries before failing. Templated. */
    public Builder waiterMaxAttempts(long value) {
      return param("waiter_max_attempts", value);
    }

    /** Maximum number of tries before failing. Templated. */
    public Builder waiterMaxAttempts(TaskRef<?> value) {
      return param("waiter_max_attempts", value);
    }

    /** Number of seconds between polling the state of the notebook. Templated. */
    public Builder waiterDelay(long value) {
      return param("waiter_delay", value);
    }

    /** Number of seconds between polling the state of the notebook. Templated. */
    public Builder waiterDelay(TaskRef<?> value) {
      return param("waiter_delay", value);
    }

    /**
     * If True, the operator will wait asynchronously for the job flow to complete. Has no
     * effect when `wait_for_completion` is False. This mode requires aiobotocore module to be
     * installed. (default: False) Defaults to false.
     */
    public Builder deferrable(boolean value) {
      return param("deferrable", value);
    }

    /**
     * If True, attempts best-effort termination of the EMR job flow when a failure occurs
     * after the job flow has been created. Cleanup failures do not mask the original
     * exception. (default: True) Defaults to true.
     */
    public Builder terminateJobFlowOnFailure(boolean value) {
      return param("terminate_job_flow_on_failure", value);
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
