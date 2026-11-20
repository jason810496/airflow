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
 * Create an Amazon Bedrock guardrail to implement safeguards for generative AI applications.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.bedrock.BedrockCreateGuardrailOperator}
 * of {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that
 * a Python worker serves.
 */
public final class BedrockCreateGuardrailOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.bedrock.BedrockCreateGuardrailOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.bedrock", "BedrockCreateGuardrailOperator")
          .params(
              "guardrail_name",
              "blocked_input_messaging",
              "blocked_outputs_messaging",
              "description",
              "topic_policy_config",
              "content_policy_config",
              "word_policy_config",
              "sensitive_information_policy_config",
              "contextual_grounding_policy_config",
              "kms_key_id",
              "tags",
              "if_exists",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("guardrail_name", "blocked_input_messaging", "blocked_outputs_messaging")
          .templated(
              "aws_conn_id",
              "blocked_input_messaging",
              "blocked_outputs_messaging",
              "description",
              "guardrail_name",
              "region_name",
              "verify")
          .templateFieldsRenderers(Map.of("topic_policy_config", "json", "content_policy_config", "json", "word_policy_config", "json", "sensitive_information_policy_config", "json"));

  private BedrockCreateGuardrailOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code BedrockCreateGuardrailOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The name of the guardrail (1-50 chars). Templated. */
    public Builder guardrailName(String value) {
      return param("guardrail_name", value);
    }

    /** The name of the guardrail (1-50 chars). Templated. */
    public Builder guardrailName(TaskRef<?> value) {
      return param("guardrail_name", value);
    }

    /** Message returned when the guardrail blocks an input prompt. Templated. */
    public Builder blockedInputMessaging(String value) {
      return param("blocked_input_messaging", value);
    }

    /** Message returned when the guardrail blocks an input prompt. Templated. */
    public Builder blockedInputMessaging(TaskRef<?> value) {
      return param("blocked_input_messaging", value);
    }

    /** Message returned when the guardrail blocks a model response. Templated. */
    public Builder blockedOutputsMessaging(String value) {
      return param("blocked_outputs_messaging", value);
    }

    /** Message returned when the guardrail blocks a model response. Templated. */
    public Builder blockedOutputsMessaging(TaskRef<?> value) {
      return param("blocked_outputs_messaging", value);
    }

    /** A description of the guardrail. Templated. */
    public Builder description(String value) {
      return param("description", value);
    }

    /** A description of the guardrail. Templated. */
    public Builder description(TaskRef<?> value) {
      return param("description", value);
    }

    /** Topic policy configuration dict. */
    public Builder topicPolicyConfig(Map<String, Object> value) {
      return param("topic_policy_config", value);
    }

    /** Content filter policy configuration dict. */
    public Builder contentPolicyConfig(Map<String, Object> value) {
      return param("content_policy_config", value);
    }

    /** Word filter policy configuration dict. */
    public Builder wordPolicyConfig(Map<String, Object> value) {
      return param("word_policy_config", value);
    }

    /** Sensitive information policy configuration dict. */
    public Builder sensitiveInformationPolicyConfig(Map<String, Object> value) {
      return param("sensitive_information_policy_config", value);
    }

    /** Contextual grounding policy configuration dict. */
    public Builder contextualGroundingPolicyConfig(Map<String, Object> value) {
      return param("contextual_grounding_policy_config", value);
    }

    /** ARN of the KMS key to encrypt the guardrail. */
    public Builder kmsKeyId(String value) {
      return param("kms_key_id", value);
    }

    /** Tags to attach to the guardrail. */
    public Builder tags(List<Map<String, String>> value) {
      return param("tags", value);
    }

    /**
     * Behavior when a guardrail with the same name already exists. `"fail"` raises an error,
     * `"skip"` returns the existing guardrail ID. One of "fail", "skip". Defaults to "skip".
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
