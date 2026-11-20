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
 * Create an index in an Amazon S3 Vectors vector bucket.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.s3_vectors.S3VectorsCreateIndexOperator}
 * of {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that
 * a Python worker serves.
 */
public final class S3VectorsCreateIndexOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.s3_vectors.S3VectorsCreateIndexOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.s3_vectors", "S3VectorsCreateIndexOperator")
          .params(
              "vector_bucket_name",
              "index_name",
              "data_type",
              "dimension",
              "distance_metric",
              "metadata_configuration",
              "if_exists",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("vector_bucket_name", "index_name", "data_type", "dimension")
          .templated(
              "aws_conn_id",
              "region_name",
              "verify",
              "vector_bucket_name",
              "index_name",
              "data_type",
              "distance_metric")
          .templateFieldsRenderers(Map.of("metadata_configuration", "json"));

  private S3VectorsCreateIndexOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code S3VectorsCreateIndexOperator}, to pass to {@code DagDef.task}. */
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

    /** The name of the index to create. (templated) Templated. */
    public Builder indexName(String value) {
      return param("index_name", value);
    }

    /** The name of the index to create. (templated) Templated. */
    public Builder indexName(TaskRef<?> value) {
      return param("index_name", value);
    }

    /** The data type for vectors (e.g. `float32`). (templated) Templated. */
    public Builder dataType(String value) {
      return param("data_type", value);
    }

    /** The data type for vectors (e.g. `float32`). (templated) Templated. */
    public Builder dataType(TaskRef<?> value) {
      return param("data_type", value);
    }

    /** The number of dimensions for each vector. */
    public Builder dimension(long value) {
      return param("dimension", value);
    }

    /**
     * The distance metric for similarity search (e.g. `cosine`, `euclidean`). Defaults to
     * "cosine". Templated.
     */
    public Builder distanceMetric(String value) {
      return param("distance_metric", value);
    }

    /**
     * The distance metric for similarity search (e.g. `cosine`, `euclidean`). Defaults to
     * "cosine". Templated.
     */
    public Builder distanceMetric(TaskRef<?> value) {
      return param("distance_metric", value);
    }

    /** Optional metadata configuration dict. */
    public Builder metadataConfiguration(Map<String, Object> value) {
      return param("metadata_configuration", value);
    }

    /**
     * Behavior when the index already exists. `"fail"` raises an error, `"skip"` returns the
     * existing index ARN. One of "fail", "skip". Defaults to "skip".
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
