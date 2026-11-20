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

package org.apache.airflow.providers.amazon.aws.operators.s3_vectors;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Query vectors by similarity in an Amazon S3 Vectors index.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.s3_vectors.S3VectorsQueryVectorsOperator}
 * of {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that
 * a Python worker serves.
 */
public final class S3VectorsQueryVectorsOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.s3_vectors.S3VectorsQueryVectorsOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.s3_vectors", "S3VectorsQueryVectorsOperator")
          .params(
              "vector_bucket_name",
              "index_name",
              "top_k",
              "query_vector",
              "filter",
              "return_metadata",
              "return_distance",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("vector_bucket_name", "index_name", "top_k", "query_vector")
          .templated(
              "aws_conn_id",
              "region_name",
              "verify",
              "vector_bucket_name",
              "index_name",
              "top_k");

  private S3VectorsQueryVectorsOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code S3VectorsQueryVectorsOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The name of the vector bucket. (templated) Templated. */
    public Builder vectorBucketName(String value) {
      return param("vector_bucket_name", value);
    }

    /** The name of the vector bucket. (templated) Templated. */
    public Builder vectorBucketName(TaskRef<?> value) {
      return param("vector_bucket_name", value);
    }

    /** The name of the index. (templated) Templated. */
    public Builder indexName(String value) {
      return param("index_name", value);
    }

    /** The name of the index. (templated) Templated. */
    public Builder indexName(TaskRef<?> value) {
      return param("index_name", value);
    }

    /** The number of results to return. Templated. */
    public Builder topK(long value) {
      return param("top_k", value);
    }

    /** The number of results to return. Templated. */
    public Builder topK(TaskRef<?> value) {
      return param("top_k", value);
    }

    /** The query vector dict (e.g. `{"float32": [0.1, 0.2, ...]}`) */
    public Builder queryVector(Map<String, Object> value) {
      return param("query_vector", value);
    }

    /** Optional filter expression dict. */
    public Builder filter(Map<String, Object> value) {
      return param("filter", value);
    }

    /** Whether to return metadata with results. Defaults to true. */
    public Builder returnMetadata(boolean value) {
      return param("return_metadata", value);
    }

    /** Whether to return distance scores. Defaults to true. */
    public Builder returnDistance(boolean value) {
      return param("return_distance", value);
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
