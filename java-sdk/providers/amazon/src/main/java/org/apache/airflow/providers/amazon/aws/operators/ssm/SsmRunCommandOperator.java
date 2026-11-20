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

package org.apache.airflow.providers.amazon.aws.operators.ssm;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Executes the SSM Run Command to perform actions on managed instances.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.ssm.SsmRunCommandOperator} of {@code
 * apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a Python
 * worker serves.
 */
public final class SsmRunCommandOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.ssm.SsmRunCommandOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.ssm", "SsmRunCommandOperator")
          .params(
              "document_name",
              "run_command_kwargs",
              "wait_for_completion",
              "waiter_delay",
              "waiter_max_attempts",
              "fail_on_nonzero_exit",
              "deferrable",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("document_name")
          .templated("aws_conn_id", "document_name", "region_name", "run_command_kwargs", "verify");

  private SsmRunCommandOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code SsmRunCommandOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /**
     * The name of the Amazon Web Services Systems Manager document (SSM document) to run.
     * Templated.
     */
    public Builder documentName(String value) {
      return param("document_name", value);
    }

    /**
     * The name of the Amazon Web Services Systems Manager document (SSM document) to run.
     * Templated.
     */
    public Builder documentName(TaskRef<?> value) {
      return param("document_name", value);
    }

    /** Optional parameters to pass to the send_command API. Templated. */
    public Builder runCommandKwargs(Map<String, Object> value) {
      return param("run_command_kwargs", value);
    }

    /** Optional parameters to pass to the send_command API. Templated. */
    public Builder runCommandKwargs(TaskRef<?> value) {
      return param("run_command_kwargs", value);
    }

    /** Whether to wait for cluster to stop. (default: True) Defaults to true. */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /** Time in seconds to wait between status checks. (default: 120) Defaults to 120. */
    public Builder waiterDelay(long value) {
      return param("waiter_delay", value);
    }

    /** Maximum number of attempts to check for job completion. (default: 75) Defaults to 75. */
    public Builder waiterMaxAttempts(long value) {
      return param("waiter_max_attempts", value);
    }

    /**
     * If True (default), the operator will fail when the command returns a non-zero exit code.
     * If False, the operator will complete successfully regardless of the command exit code,
     * allowing downstream tasks to handle exit codes for workflow routing. Note that AWS-level
     * failures (Cancelled, TimedOut) will still raise exceptions even when this is False.
     * (default: True) Defaults to true.
     */
    public Builder failOnNonzeroExit(boolean value) {
      return param("fail_on_nonzero_exit", value);
    }

    /**
     * If True, the operator will wait asynchronously for the cluster to stop. This mode
     * requires aiobotocore module to be installed. (default: False) Deferring when
     * `wait_for_completion` is False is deprecated; from the next major release this has no
     * effect when `wait_for_completion` is False. Defaults to false.
     */
    public Builder deferrable(boolean value) {
      return param("deferrable", value);
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
