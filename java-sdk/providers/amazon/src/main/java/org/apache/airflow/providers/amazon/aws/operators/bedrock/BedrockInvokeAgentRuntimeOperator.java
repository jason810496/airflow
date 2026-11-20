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

package org.apache.airflow.providers.amazon.aws.operators.bedrock;

import java.util.List;
import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Invoke an Amazon Bedrock AgentCore Runtime.
 *
 * Runs {@code
 * airflow.providers.amazon.aws.operators.bedrock.BedrockInvokeAgentRuntimeOperator} of {@code
 * apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a Python
 * worker serves.
 */
public final class BedrockInvokeAgentRuntimeOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.bedrock.BedrockInvokeAgentRuntimeOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.bedrock", "BedrockInvokeAgentRuntimeOperator")
          .params(
              "agent_runtime_arn",
              "payload",
              "content_type",
              "accept",
              "invoke_agent_runtime_kwargs",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("agent_runtime_arn", "payload")
          .templated(
              "accept",
              "agent_runtime_arn",
              "aws_conn_id",
              "content_type",
              "invoke_agent_runtime_kwargs",
              "payload",
              "region_name",
              "verify");

  private BedrockInvokeAgentRuntimeOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code BedrockInvokeAgentRuntimeOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The ARN of the AgentCore Runtime to invoke. (templated) Templated. */
    public Builder agentRuntimeArn(String value) {
      return param("agent_runtime_arn", value);
    }

    /** The ARN of the AgentCore Runtime to invoke. (templated) Templated. */
    public Builder agentRuntimeArn(TaskRef<?> value) {
      return param("agent_runtime_arn", value);
    }

    /**
     * The invocation payload. Dict and list payloads are JSON serialized. (templated)
     * Templated.
     */
    public Builder payload(Object value) {
      return param("payload", value);
    }

    /**
     * The MIME type of the input payload. (templated) Default: application/json Defaults to
     * "application/json". Templated.
     */
    public Builder contentType(String value) {
      return param("content_type", value);
    }

    /**
     * The MIME type of the input payload. (templated) Default: application/json Defaults to
     * "application/json". Templated.
     */
    public Builder contentType(TaskRef<?> value) {
      return param("content_type", value);
    }

    /**
     * The desired MIME type of the response. (templated) Default: application/json Defaults to
     * "application/json". Templated.
     */
    public Builder accept(String value) {
      return param("accept", value);
    }

    /**
     * The desired MIME type of the response. (templated) Default: application/json Defaults to
     * "application/json". Templated.
     */
    public Builder accept(TaskRef<?> value) {
      return param("accept", value);
    }

    /** Any optional parameters to pass to the API. (templated) Templated. */
    public Builder invokeAgentRuntimeKwargs(Map<String, Object> value) {
      return param("invoke_agent_runtime_kwargs", value);
    }

    /** Any optional parameters to pass to the API. (templated) Templated. */
    public Builder invokeAgentRuntimeKwargs(TaskRef<?> value) {
      return param("invoke_agent_runtime_kwargs", value);
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
