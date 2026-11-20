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
 * Query a knowledge base and retrieve results with source citations.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.bedrock.BedrockRetrieveOperator} of
 * {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class BedrockRetrieveOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.bedrock.BedrockRetrieveOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.bedrock", "BedrockRetrieveOperator")
          .params(
              "retrieval_query",
              "knowledge_base_id",
              "vector_search_config",
              "retrieve_kwargs",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("retrieval_query", "knowledge_base_id")
          .templated(
              "aws_conn_id",
              "knowledge_base_id",
              "region_name",
              "retrieval_query",
              "retrieve_kwargs",
              "vector_search_config",
              "verify");

  private BedrockRetrieveOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code BedrockRetrieveOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The query to be made to the knowledge base. (templated) Templated. */
    public Builder retrievalQuery(String value) {
      return param("retrieval_query", value);
    }

    /** The query to be made to the knowledge base. (templated) Templated. */
    public Builder retrievalQuery(TaskRef<?> value) {
      return param("retrieval_query", value);
    }

    /** The unique identifier of the knowledge base that is queried. (templated) Templated. */
    public Builder knowledgeBaseId(String value) {
      return param("knowledge_base_id", value);
    }

    /** The unique identifier of the knowledge base that is queried. (templated) Templated. */
    public Builder knowledgeBaseId(TaskRef<?> value) {
      return param("knowledge_base_id", value);
    }

    /**
     * How the results from the vector search should be returned. (templated) For more
     * information, see
     * https://docs.aws.amazon.com/bedrock/latest/userguide/kb-test-config.html. Templated.
     */
    public Builder vectorSearchConfig(Map<String, Object> value) {
      return param("vector_search_config", value);
    }

    /**
     * How the results from the vector search should be returned. (templated) For more
     * information, see
     * https://docs.aws.amazon.com/bedrock/latest/userguide/kb-test-config.html. Templated.
     */
    public Builder vectorSearchConfig(TaskRef<?> value) {
      return param("vector_search_config", value);
    }

    /** Additional keyword arguments to pass to the API call. (templated) Templated. */
    public Builder retrieveKwargs(Map<String, Object> value) {
      return param("retrieve_kwargs", value);
    }

    /** Additional keyword arguments to pass to the API call. (templated) Templated. */
    public Builder retrieveKwargs(TaskRef<?> value) {
      return param("retrieve_kwargs", value);
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
