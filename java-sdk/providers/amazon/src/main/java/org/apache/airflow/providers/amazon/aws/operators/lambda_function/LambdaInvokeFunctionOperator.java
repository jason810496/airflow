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
 * Invokes an AWS Lambda function.
 *
 * Runs {@code
 * airflow.providers.amazon.aws.operators.lambda_function.LambdaInvokeFunctionOperator} of
 * {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class LambdaInvokeFunctionOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.lambda_function.LambdaInvokeFunctionOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.lambda_function", "LambdaInvokeFunctionOperator")
          .params(
              "function_name",
              "log_type",
              "keep_empty_log_lines",
              "qualifier",
              "invocation_type",
              "client_context",
              "payload",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("function_name")
          .templated(
              "aws_conn_id",
              "function_name",
              "invocation_type",
              "payload",
              "qualifier",
              "region_name",
              "verify")
          .uiColor("#ff7300");

  private LambdaInvokeFunctionOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code LambdaInvokeFunctionOperator}, to pass to {@code DagDef.task}. */
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
     * Set to Tail to include the execution log in the response and task logs. Otherwise, set
     * to "None". Applies to synchronously invoked functions only, and returns the last 4 KB of
     * the execution log.
     */
    public Builder logType(String value) {
      return param("log_type", value);
    }

    /** Whether or not keep empty lines in the execution log. Defaults to true. */
    public Builder keepEmptyLogLines(boolean value) {
      return param("keep_empty_log_lines", value);
    }

    /** Specify a version or alias to invoke a published version of the function. Templated. */
    public Builder qualifier(String value) {
      return param("qualifier", value);
    }

    /** Specify a version or alias to invoke a published version of the function. Templated. */
    public Builder qualifier(TaskRef<?> value) {
      return param("qualifier", value);
    }

    /** AWS Lambda invocation type (RequestResponse, Event, DryRun) Templated. */
    public Builder invocationType(String value) {
      return param("invocation_type", value);
    }

    /** AWS Lambda invocation type (RequestResponse, Event, DryRun) Templated. */
    public Builder invocationType(TaskRef<?> value) {
      return param("invocation_type", value);
    }

    /** Data about the invoking client to pass to the function in the context object */
    public Builder clientContext(String value) {
      return param("client_context", value);
    }

    /** JSON provided as input to the Lambda function Templated. */
    public Builder payload(String value) {
      return param("payload", value);
    }

    /** JSON provided as input to the Lambda function Templated. */
    public Builder payload(TaskRef<?> value) {
      return param("payload", value);
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
