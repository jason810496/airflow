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

package org.apache.airflow.providers.amazon.aws.operators.dms;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Creates an AWS DMS Serverless replication configuration.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.dms.DmsCreateReplicationConfigOperator}
 * of {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that
 * a Python worker serves.
 */
public final class DmsCreateReplicationConfigOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.dms.DmsCreateReplicationConfigOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.dms", "DmsCreateReplicationConfigOperator")
          .params(
              "replication_config_id",
              "source_endpoint_arn",
              "target_endpoint_arn",
              "compute_config",
              "replication_type",
              "table_mappings",
              "additional_config_kwargs",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required(
              "replication_config_id",
              "source_endpoint_arn",
              "target_endpoint_arn",
              "compute_config",
              "replication_type",
              "table_mappings")
          .templated(
              "aws_conn_id",
              "compute_config",
              "region_name",
              "replication_config_id",
              "replication_type",
              "source_endpoint_arn",
              "table_mappings",
              "target_endpoint_arn",
              "verify")
          .templateFieldsRenderers(Map.of("compute_config", "json", "tableMappings", "json"));

  private DmsCreateReplicationConfigOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code DmsCreateReplicationConfigOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** Unique identifier used to create a ReplicationConfigArn. Templated. */
    public Builder replicationConfigId(String value) {
      return param("replication_config_id", value);
    }

    /** Unique identifier used to create a ReplicationConfigArn. Templated. */
    public Builder replicationConfigId(TaskRef<?> value) {
      return param("replication_config_id", value);
    }

    /** ARN of the source endpoint Templated. */
    public Builder sourceEndpointArn(String value) {
      return param("source_endpoint_arn", value);
    }

    /** ARN of the source endpoint Templated. */
    public Builder sourceEndpointArn(TaskRef<?> value) {
      return param("source_endpoint_arn", value);
    }

    /** ARN of the target endpoint Templated. */
    public Builder targetEndpointArn(String value) {
      return param("target_endpoint_arn", value);
    }

    /** ARN of the target endpoint Templated. */
    public Builder targetEndpointArn(TaskRef<?> value) {
      return param("target_endpoint_arn", value);
    }

    /** Parameters for provisioning an DMS Serverless replication. Templated. */
    public Builder computeConfig(Map<String, Object> value) {
      return param("compute_config", value);
    }

    /** Parameters for provisioning an DMS Serverless replication. Templated. */
    public Builder computeConfig(TaskRef<?> value) {
      return param("compute_config", value);
    }

    /** type of DMS Serverless replication Templated. */
    public Builder replicationType(String value) {
      return param("replication_type", value);
    }

    /** type of DMS Serverless replication Templated. */
    public Builder replicationType(TaskRef<?> value) {
      return param("replication_type", value);
    }

    /** JSON table mappings Templated. */
    public Builder tableMappings(String value) {
      return param("table_mappings", value);
    }

    /** JSON table mappings Templated. */
    public Builder tableMappings(TaskRef<?> value) {
      return param("table_mappings", value);
    }

    /**
     * Additional configuration parameters for DMS Serverless replication. Passed directly to
     * the API
     */
    public Builder additionalConfigKwargs(Map<String, Object> value) {
      return param("additional_config_kwargs", value);
    }

    /**
     * The Airflow connection used for AWS credentials. If this is `None` or empty then the
     * default boto3 behaviour is used. If running Airflow in a distributed manner and
     * aws_conn_id is None or empty, then default boto3 configuration would be used (and must
     * be Defaults to "aws_default". Templated.
     */
    public Builder awsConnId(String value) {
      return param("aws_conn_id", value);
    }

    /**
     * The Airflow connection used for AWS credentials. If this is `None` or empty then the
     * default boto3 behaviour is used. If running Airflow in a distributed manner and
     * aws_conn_id is None or empty, then default boto3 configuration would be used (and must
     * be Defaults to "aws_default". Templated.
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
