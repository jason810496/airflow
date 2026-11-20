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

package org.apache.airflow.providers.amazon.aws.operators.lambda_function;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Creates an AWS Lambda function.
 *
 * Runs {@code
 * airflow.providers.amazon.aws.operators.lambda_function.LambdaCreateFunctionOperator} of
 * {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class LambdaCreateFunctionOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.lambda_function.LambdaCreateFunctionOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.lambda_function", "LambdaCreateFunctionOperator")
          .params(
              "function_name",
              "runtime",
              "role",
              "handler",
              "code",
              "description",
              "timeout",
              "config",
              "wait_for_completion",
              "waiter_max_attempts",
              "waiter_delay",
              "deferrable",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("function_name", "role", "code")
          .templated(
              "aws_conn_id",
              "code",
              "config",
              "function_name",
              "handler",
              "region_name",
              "role",
              "runtime",
              "verify")
          .uiColor("#ff7300");

  private LambdaCreateFunctionOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code LambdaCreateFunctionOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The name of the AWS Lambda function, version, or alias. Templated. */
    public Builder functionName(String value) {
      return param("function_name", value);
    }

    /** The name of the AWS Lambda function, version, or alias. Templated. */
    public Builder functionName(TaskRef<?> value) {
      return param("function_name", value);
    }

    /**
     * The identifier of the function's runtime. Runtime is required if the deployment package
     * is a .zip file archive. Templated.
     */
    public Builder runtime(String value) {
      return param("runtime", value);
    }

    /**
     * The identifier of the function's runtime. Runtime is required if the deployment package
     * is a .zip file archive. Templated.
     */
    public Builder runtime(TaskRef<?> value) {
      return param("runtime", value);
    }

    /** The Amazon Resource Name (ARN) of the function's execution role. Templated. */
    public Builder role(String value) {
      return param("role", value);
    }

    /** The Amazon Resource Name (ARN) of the function's execution role. Templated. */
    public Builder role(TaskRef<?> value) {
      return param("role", value);
    }

    /**
     * The name of the method within your code that Lambda calls to run your function. Handler
     * is required if the deployment package is a .zip file archive. Templated.
     */
    public Builder handler(String value) {
      return param("handler", value);
    }

    /**
     * The name of the method within your code that Lambda calls to run your function. Handler
     * is required if the deployment package is a .zip file archive. Templated.
     */
    public Builder handler(TaskRef<?> value) {
      return param("handler", value);
    }

    /** The code for the function. Templated. */
    public Builder code(Map<String, Object> value) {
      return param("code", value);
    }

    /** The code for the function. Templated. */
    public Builder code(TaskRef<?> value) {
      return param("code", value);
    }

    /** A description of the function. */
    public Builder description(String value) {
      return param("description", value);
    }

    /** The amount of time (in seconds) that Lambda allows a function to run before stopping it. */
    public Builder timeout(long value) {
      return param("timeout", value);
    }

    /**
     * Optional dictionary for arbitrary parameters to the boto API create_lambda call.
     * Templated.
     */
    public Builder config(Map<String, Object> value) {
      return param("config", value);
    }

    /**
     * Optional dictionary for arbitrary parameters to the boto API create_lambda call.
     * Templated.
     */
    public Builder config(TaskRef<?> value) {
      return param("config", value);
    }

    /** If True, the operator will wait until the function is active. Defaults to false. */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /** Maximum number of attempts to poll the creation. Defaults to 60. */
    public Builder waiterMaxAttempts(long value) {
      return param("waiter_max_attempts", value);
    }

    /** Number of seconds between polling the state of the creation. Defaults to 15. */
    public Builder waiterDelay(long value) {
      return param("waiter_delay", value);
    }

    /**
     * If True, the operator will wait asynchronously for the creation to complete. This mode
     * requires aiobotocore module to be installed. (default: False, but can be overridden in
     * config file by setting default_deferrable to True) Deferring when `wait_for_completion`
     * is False is deprecated; from the next major release this has no effect when
     * `wait_for_completion` is False. Defaults to false.
     */
    public Builder deferrable(boolean value) {
      return param("deferrable", value);
    }

    /** The AWS connection ID to use Defaults to "aws_default". Templated. */
    public Builder awsConnId(String value) {
      return param("aws_conn_id", value);
    }

    /** The AWS connection ID to use Defaults to "aws_default". Templated. */
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
