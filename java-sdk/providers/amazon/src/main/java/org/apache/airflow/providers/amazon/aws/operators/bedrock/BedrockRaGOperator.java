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
 * Query a knowledge base and generate responses based on the retrieved results with sources
 * citations.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.bedrock.BedrockRaGOperator} of {@code
 * apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a Python
 * worker serves.
 */
public final class BedrockRaGOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.bedrock.BedrockRaGOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.bedrock", "BedrockRaGOperator")
          .params(
              "input",
              "source_type",
              "model_arn",
              "prompt_template",
              "knowledge_base_id",
              "vector_search_config",
              "sources",
              "rag_kwargs",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("input", "source_type", "model_arn")
          .templated(
              "aws_conn_id",
              "input",
              "knowledge_base_id",
              "model_arn",
              "prompt_template",
              "rag_kwargs",
              "region_name",
              "source_type",
              "sources",
              "vector_search_config",
              "verify");

  private BedrockRaGOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code BedrockRaGOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The query to be made to the knowledge base. (templated) Templated. */
    public Builder input(String value) {
      return param("input", value);
    }

    /** The query to be made to the knowledge base. (templated) Templated. */
    public Builder input(TaskRef<?> value) {
      return param("input", value);
    }

    /**
     * The type of resource that is queried by the request. (templated) Must be one of
     * 'KNOWLEDGE_BASE' or 'EXTERNAL_SOURCES', and the appropriate config values must also be
     * provided. If set to 'KNOWLEDGE_BASE' then `knowledge_base_id` must be provided, and
     * `vector_search_config` may be. If set to `EXTERNAL_SOURCES` then `sources` must also be
     * provided. NOTE: Support for EXTERNAL SOURCES was added in botocore 1.34.90 Templated.
     */
    public Builder sourceType(String value) {
      return param("source_type", value);
    }

    /**
     * The type of resource that is queried by the request. (templated) Must be one of
     * 'KNOWLEDGE_BASE' or 'EXTERNAL_SOURCES', and the appropriate config values must also be
     * provided. If set to 'KNOWLEDGE_BASE' then `knowledge_base_id` must be provided, and
     * `vector_search_config` may be. If set to `EXTERNAL_SOURCES` then `sources` must also be
     * provided. NOTE: Support for EXTERNAL SOURCES was added in botocore 1.34.90 Templated.
     */
    public Builder sourceType(TaskRef<?> value) {
      return param("source_type", value);
    }

    /** The ARN of the foundation model used to generate a response. (templated) Templated. */
    public Builder modelArn(String value) {
      return param("model_arn", value);
    }

    /** The ARN of the foundation model used to generate a response. (templated) Templated. */
    public Builder modelArn(TaskRef<?> value) {
      return param("model_arn", value);
    }

    /**
     * The template for the prompt that's sent to the model for response generation. You can
     * include prompt placeholders, which are replaced before the prompt is sent to the model
     * to provide instructions and context to the model. In addition, you can include XML tags
     * to delineate meaningful sections of the prompt template. (templated) Templated.
     */
    public Builder promptTemplate(String value) {
      return param("prompt_template", value);
    }

    /**
     * The template for the prompt that's sent to the model for response generation. You can
     * include prompt placeholders, which are replaced before the prompt is sent to the model
     * to provide instructions and context to the model. In addition, you can include XML tags
     * to delineate meaningful sections of the prompt template. (templated) Templated.
     */
    public Builder promptTemplate(TaskRef<?> value) {
      return param("prompt_template", value);
    }

    /**
     * The unique identifier of the knowledge base that is queried. (templated) Can only be
     * specified if source_type='KNOWLEDGE_BASE'. Templated.
     */
    public Builder knowledgeBaseId(String value) {
      return param("knowledge_base_id", value);
    }

    /**
     * The unique identifier of the knowledge base that is queried. (templated) Can only be
     * specified if source_type='KNOWLEDGE_BASE'. Templated.
     */
    public Builder knowledgeBaseId(TaskRef<?> value) {
      return param("knowledge_base_id", value);
    }

    /**
     * How the results from the vector search should be returned. (templated) Can only be
     * specified if source_type='KNOWLEDGE_BASE'. For more information, see
     * https://docs.aws.amazon.com/bedrock/latest/userguide/kb-test-config.html. Templated.
     */
    public Builder vectorSearchConfig(Map<String, Object> value) {
      return param("vector_search_config", value);
    }

    /**
     * How the results from the vector search should be returned. (templated) Can only be
     * specified if source_type='KNOWLEDGE_BASE'. For more information, see
     * https://docs.aws.amazon.com/bedrock/latest/userguide/kb-test-config.html. Templated.
     */
    public Builder vectorSearchConfig(TaskRef<?> value) {
      return param("vector_search_config", value);
    }

    /**
     * The documents used as reference for the response. (templated) Can only be specified if
     * source_type='EXTERNAL_SOURCES' NOTE: Support for EXTERNAL SOURCES was added in botocore
     * 1.34.90 Templated.
     */
    public Builder sources(List<Map<String, Object>> value) {
      return param("sources", value);
    }

    /**
     * The documents used as reference for the response. (templated) Can only be specified if
     * source_type='EXTERNAL_SOURCES' NOTE: Support for EXTERNAL SOURCES was added in botocore
     * 1.34.90 Templated.
     */
    public Builder sources(TaskRef<?> value) {
      return param("sources", value);
    }

    /** Additional keyword arguments to pass to the API call. (templated) Templated. */
    public Builder ragKwargs(Map<String, Object> value) {
      return param("rag_kwargs", value);
    }

    /** Additional keyword arguments to pass to the API call. (templated) Templated. */
    public Builder ragKwargs(TaskRef<?> value) {
      return param("rag_kwargs", value);
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
