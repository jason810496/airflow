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

package org.apache.airflow.providers.amazon.aws.operators.opensearch_serverless;

import java.util.List;
import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Create an Amazon OpenSearch Serverless collection.
 *
 * Runs {@code
 * airflow.providers.amazon.aws.operators.opensearch_serverless.OpenSearchServerlessCreateCollectionOperator}
 * of {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that
 * a Python worker serves.
 */
public final class OpenSearchServerlessCreateCollectionOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.opensearch_serverless.OpenSearchServerlessCreateCollectionOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.opensearch_serverless", "OpenSearchServerlessCreateCollectionOperator")
          .params(
              "collection_name",
              "collection_type",
              "description",
              "standby_replicas",
              "tags",
              "if_exists",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("collection_name")
          .templated(
              "aws_conn_id",
              "collection_name",
              "collection_type",
              "description",
              "region_name",
              "verify");

  private OpenSearchServerlessCreateCollectionOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code OpenSearchServerlessCreateCollectionOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The name of the collection. (templated) Templated. */
    public Builder collectionName(String value) {
      return param("collection_name", value);
    }

    /** The name of the collection. (templated) Templated. */
    public Builder collectionName(TaskRef<?> value) {
      return param("collection_name", value);
    }

    /**
     * The type of collection (SEARCH, TIMESERIES, VECTORSEARCH). (templated) Defaults to
     * "SEARCH". Templated.
     */
    public Builder collectionType(String value) {
      return param("collection_type", value);
    }

    /**
     * The type of collection (SEARCH, TIMESERIES, VECTORSEARCH). (templated) Defaults to
     * "SEARCH". Templated.
     */
    public Builder collectionType(TaskRef<?> value) {
      return param("collection_type", value);
    }

    /** Optional description. (templated) Templated. */
    public Builder description(String value) {
      return param("description", value);
    }

    /** Optional description. (templated) Templated. */
    public Builder description(TaskRef<?> value) {
      return param("description", value);
    }

    /** Whether to use standby replicas (ENABLED or DISABLED). */
    public Builder standbyReplicas(String value) {
      return param("standby_replicas", value);
    }

    /** Optional list of tag dicts. */
    public Builder tags(List<Map<String, String>> value) {
      return param("tags", value);
    }

    /**
     * Behavior when the collection already exists. `"fail"` raises an error, `"skip"` logs and
     * returns. One of "fail", "skip". Defaults to "skip".
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
