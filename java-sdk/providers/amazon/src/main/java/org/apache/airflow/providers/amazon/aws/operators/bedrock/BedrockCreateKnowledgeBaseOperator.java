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
 * Create a knowledge base that contains data sources used by Amazon Bedrock LLMs and Agents.
 *
 * Runs {@code
 * airflow.providers.amazon.aws.operators.bedrock.BedrockCreateKnowledgeBaseOperator} of {@code
 * apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a Python
 * worker serves.
 */
public final class BedrockCreateKnowledgeBaseOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.bedrock.BedrockCreateKnowledgeBaseOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.bedrock", "BedrockCreateKnowledgeBaseOperator")
          .params(
              "name",
              "embedding_model_arn",
              "role_arn",
              "storage_config",
              "create_knowledge_base_kwargs",
              "wait_for_indexing",
              "indexing_error_retry_delay",
              "indexing_error_max_attempts",
              "wait_for_completion",
              "waiter_delay",
              "waiter_max_attempts",
              "deferrable",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("name", "embedding_model_arn", "role_arn", "storage_config")
          .templated(
              "aws_conn_id",
              "create_knowledge_base_kwargs",
              "embedding_model_arn",
              "indexing_error_max_attempts",
              "indexing_error_retry_delay",
              "name",
              "region_name",
              "role_arn",
              "storage_config",
              "verify",
              "wait_for_indexing");

  private BedrockCreateKnowledgeBaseOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code BedrockCreateKnowledgeBaseOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The name of the knowledge base. (templated) Templated. */
    public Builder name(String value) {
      return param("name", value);
    }

    /** The name of the knowledge base. (templated) Templated. */
    public Builder name(TaskRef<?> value) {
      return param("name", value);
    }

    /**
     * ARN of the model used to create vector embeddings for the knowledge base. (templated)
     * Templated.
     */
    public Builder embeddingModelArn(String value) {
      return param("embedding_model_arn", value);
    }

    /**
     * ARN of the model used to create vector embeddings for the knowledge base. (templated)
     * Templated.
     */
    public Builder embeddingModelArn(TaskRef<?> value) {
      return param("embedding_model_arn", value);
    }

    /**
     * The ARN of the IAM role with permissions to create the knowledge base. (templated)
     * Templated.
     */
    public Builder roleArn(String value) {
      return param("role_arn", value);
    }

    /**
     * The ARN of the IAM role with permissions to create the knowledge base. (templated)
     * Templated.
     */
    public Builder roleArn(TaskRef<?> value) {
      return param("role_arn", value);
    }

    /**
     * Configuration details of the vector database used for the knowledge base. (templated)
     * Templated.
     */
    public Builder storageConfig(Map<String, Object> value) {
      return param("storage_config", value);
    }

    /**
     * Configuration details of the vector database used for the knowledge base. (templated)
     * Templated.
     */
    public Builder storageConfig(TaskRef<?> value) {
      return param("storage_config", value);
    }

    /** Any additional optional parameters to pass to the API call. (templated) Templated. */
    public Builder createKnowledgeBaseKwargs(Map<String, Object> value) {
      return param("create_knowledge_base_kwargs", value);
    }

    /** Any additional optional parameters to pass to the API call. (templated) Templated. */
    public Builder createKnowledgeBaseKwargs(TaskRef<?> value) {
      return param("create_knowledge_base_kwargs", value);
    }

    /**
     * Vector indexing can take some time and there is no apparent way to check the state
     * before trying to create the Knowledge Base. If this is True, and creation fails due to
     * the index not being available, the operator will wait and retry. (default: True)
     * (templated) Defaults to true. Templated.
     */
    public Builder waitForIndexing(boolean value) {
      return param("wait_for_indexing", value);
    }

    /**
     * Vector indexing can take some time and there is no apparent way to check the state
     * before trying to create the Knowledge Base. If this is True, and creation fails due to
     * the index not being available, the operator will wait and retry. (default: True)
     * (templated) Defaults to true. Templated.
     */
    public Builder waitForIndexing(TaskRef<?> value) {
      return param("wait_for_indexing", value);
    }

    /**
     * Seconds between retries if an index error is encountered. (default 5) (templated)
     * Defaults to 5. Templated.
     */
    public Builder indexingErrorRetryDelay(long value) {
      return param("indexing_error_retry_delay", value);
    }

    /**
     * Seconds between retries if an index error is encountered. (default 5) (templated)
     * Defaults to 5. Templated.
     */
    public Builder indexingErrorRetryDelay(TaskRef<?> value) {
      return param("indexing_error_retry_delay", value);
    }

    /**
     * Maximum number of times to retry when encountering an index error. (default 20)
     * (templated) Defaults to 20. Templated.
     */
    public Builder indexingErrorMaxAttempts(long value) {
      return param("indexing_error_max_attempts", value);
    }

    /**
     * Maximum number of times to retry when encountering an index error. (default 20)
     * (templated) Defaults to 20. Templated.
     */
    public Builder indexingErrorMaxAttempts(TaskRef<?> value) {
      return param("indexing_error_max_attempts", value);
    }

    /** Whether to wait for cluster to stop. (default: True) Defaults to true. */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /** Time in seconds to wait between status checks. (default: 60) Defaults to 60. */
    public Builder waiterDelay(long value) {
      return param("waiter_delay", value);
    }

    /** Maximum number of attempts to check for job completion. (default: 20) Defaults to 20. */
    public Builder waiterMaxAttempts(long value) {
      return param("waiter_max_attempts", value);
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
