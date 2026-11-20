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

package org.apache.airflow.providers.amazon.aws.operators.neptune_analytics;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Creates a Neptune Graph and imports data into it.
 *
 * Runs {@code
 * airflow.providers.amazon.aws.operators.neptune_analytics.NeptuneCreateGraphWithImportOperator}
 * of {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that
 * a Python worker serves.
 */
public final class NeptuneCreateGraphWithImportOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.neptune_analytics.NeptuneCreateGraphWithImportOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.neptune_analytics", "NeptuneCreateGraphWithImportOperator")
          .params(
              "graph_name",
              "vector_search_config",
              "source",
              "role_arn",
              "blank_node_handling",
              "parquet_type",
              "format",
              "min_provisioned_memory",
              "max_provisioned_memory",
              "fail_on_error",
              "public_connectivity",
              "replica_count",
              "deletion_protection",
              "kms_key_id",
              "tags",
              "import_options",
              "wait_for_completion",
              "waiter_delay",
              "waiter_max_attempts",
              "deferrable",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("graph_name", "vector_search_config", "source", "role_arn")
          .templated(
              "aws_conn_id",
              "graph_name",
              "region_name",
              "role_arn",
              "source",
              "vector_search_config",
              "verify")
          .templateFieldsRenderers(Map.of("vector_search_config", "json"));

  private NeptuneCreateGraphWithImportOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code NeptuneCreateGraphWithImportOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** Name of Neptune graph to create Templated. */
    public Builder graphName(String value) {
      return param("graph_name", value);
    }

    /** Name of Neptune graph to create Templated. */
    public Builder graphName(TaskRef<?> value) {
      return param("graph_name", value);
    }

    /**
     * Specifies the number of dimensions for vector embeddings that will be loaded into the
     * graph. Templated.
     */
    public Builder vectorSearchConfig(Map<String, Object> value) {
      return param("vector_search_config", value);
    }

    /**
     * Specifies the number of dimensions for vector embeddings that will be loaded into the
     * graph. Templated.
     */
    public Builder vectorSearchConfig(TaskRef<?> value) {
      return param("vector_search_config", value);
    }

    /**
     * The source from which to import data. Can be an S3 URI or Neptune database snapshot.
     * Templated.
     */
    public Builder source(String value) {
      return param("source", value);
    }

    /**
     * The source from which to import data. Can be an S3 URI or Neptune database snapshot.
     * Templated.
     */
    public Builder source(TaskRef<?> value) {
      return param("source", value);
    }

    /**
     * The ARN of the IAM role that Neptune Analytics can assume to access the data source.
     * Templated.
     */
    public Builder roleArn(String value) {
      return param("role_arn", value);
    }

    /**
     * The ARN of the IAM role that Neptune Analytics can assume to access the data source.
     * Templated.
     */
    public Builder roleArn(TaskRef<?> value) {
      return param("role_arn", value);
    }

    /**
     * The method to handle blank nodes in the dataset. Options include 'convertToIri' or other
     * handling strategies.
     */
    public Builder blankNodeHandling(String value) {
      return param("blank_node_handling", value);
    }

    /** The type of Parquet files in the data source (if applicable). */
    public Builder parquetType(String value) {
      return param("parquet_type", value);
    }

    /**
     * The format of the data to be imported (e.g., 'csv', 'opencypher', 'ntriples', 'nquads',
     * 'rdfxml', 'turtle').
     */
    public Builder format(String value) {
      return param("format", value);
    }

    /** The minimum provisioned memory for the graph in GBs. */
    public Builder minProvisionedMemory(long value) {
      return param("min_provisioned_memory", value);
    }

    /** The maximum provisioned memory for the graph in GBs. */
    public Builder maxProvisionedMemory(long value) {
      return param("max_provisioned_memory", value);
    }

    /**
     * If True, the import will fail if any errors are encountered. If False, the import will
     * continue despite errors.
     */
    public Builder failOnError(boolean value) {
      return param("fail_on_error", value);
    }

    /** Specifies whether or not the graph can be reachable over the internet. */
    public Builder publicConnectivity(boolean value) {
      return param("public_connectivity", value);
    }

    /** The number of replicas in other AZs. */
    public Builder replicaCount(long value) {
      return param("replica_count", value);
    }

    /**
     * Indicates whether or not to enable deletion protection on the graph. The graph can't be
     * deleted when deletion protection is enabled. (default: False)
     */
    public Builder deletionProtection(boolean value) {
      return param("deletion_protection", value);
    }

    /** Specifies a KMS key to use to encrypt data in the new graph. */
    public Builder kmsKeyId(String value) {
      return param("kms_key_id", value);
    }

    /** Specifies metadata tags to add to the graph. */
    public Builder tags(Map<String, Object> value) {
      return param("tags", value);
    }

    /** Contains options for controlling the import process. */
    public Builder importOptions(Map<String, Object> value) {
      return param("import_options", value);
    }

    /**
     * Whether to wait for the graph to be created and data imported. (default: True) Defaults
     * to true.
     */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /** Time in seconds to wait between status checks. (default: 30) Defaults to 30. */
    public Builder waiterDelay(long value) {
      return param("waiter_delay", value);
    }

    /** Maximum number of attempts to check for job completion. (default: 60) Defaults to 60. */
    public Builder waiterMaxAttempts(long value) {
      return param("waiter_max_attempts", value);
    }

    /**
     * If True, the operator will wait asynchronously for the graph to be created and data
     * imported. Deferring when `wait_for_completion` is False is deprecated; from the next
     * major release this has no effect when `wait_for_completion` is False. This mode requires
     * aiobotocore module to be installed. (default: False) Defaults to false.
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
