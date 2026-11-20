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

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Update an Amazon Bedrock guardrail configuration.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.bedrock.BedrockUpdateGuardrailOperator}
 * of {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that
 * a Python worker serves.
 */
public final class BedrockUpdateGuardrailOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.bedrock.BedrockUpdateGuardrailOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.bedrock", "BedrockUpdateGuardrailOperator")
          .params(
              "guardrail_identifier",
              "guardrail_name",
              "blocked_input_messaging",
              "blocked_outputs_messaging",
              "description",
              "topic_policy_config",
              "content_policy_config",
              "word_policy_config",
              "sensitive_information_policy_config",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required(
              "guardrail_identifier",
              "guardrail_name",
              "blocked_input_messaging",
              "blocked_outputs_messaging")
          .templated(
              "aws_conn_id",
              "blocked_input_messaging",
              "blocked_outputs_messaging",
              "guardrail_identifier",
              "guardrail_name",
              "region_name",
              "verify");

  private BedrockUpdateGuardrailOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code BedrockUpdateGuardrailOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The ID or ARN of the guardrail to update. (templated) Templated. */
    public Builder guardrailIdentifier(String value) {
      return param("guardrail_identifier", value);
    }

    /** The ID or ARN of the guardrail to update. (templated) Templated. */
    public Builder guardrailIdentifier(TaskRef<?> value) {
      return param("guardrail_identifier", value);
    }

    /** The new name for the guardrail. (templated) Templated. */
    public Builder guardrailName(String value) {
      return param("guardrail_name", value);
    }

    /** The new name for the guardrail. (templated) Templated. */
    public Builder guardrailName(TaskRef<?> value) {
      return param("guardrail_name", value);
    }

    /** Message returned when input is blocked. (templated) Templated. */
    public Builder blockedInputMessaging(String value) {
      return param("blocked_input_messaging", value);
    }

    /** Message returned when input is blocked. (templated) Templated. */
    public Builder blockedInputMessaging(TaskRef<?> value) {
      return param("blocked_input_messaging", value);
    }

    /** Message returned when output is blocked. (templated) Templated. */
    public Builder blockedOutputsMessaging(String value) {
      return param("blocked_outputs_messaging", value);
    }

    /** Message returned when output is blocked. (templated) Templated. */
    public Builder blockedOutputsMessaging(TaskRef<?> value) {
      return param("blocked_outputs_messaging", value);
    }

    /** Optional description. (templated) */
    public Builder description(String value) {
      return param("description", value);
    }

    /** Optional topic policy configuration dict. */
    public Builder topicPolicyConfig(Map<String, Object> value) {
      return param("topic_policy_config", value);
    }

    /** Optional content filter policy configuration dict. */
    public Builder contentPolicyConfig(Map<String, Object> value) {
      return param("content_policy_config", value);
    }

    /** Optional word filter policy configuration dict. */
    public Builder wordPolicyConfig(Map<String, Object> value) {
      return param("word_policy_config", value);
    }

    /** Optional sensitive information policy dict. */
    public Builder sensitiveInformationPolicyConfig(Map<String, Object> value) {
      return param("sensitive_information_policy_config", value);
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
