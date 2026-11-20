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

package org.apache.airflow.providers.amazon.aws.operators.batch;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Create an AWS Batch compute environment.
 *
 * Runs {@code
 * airflow.providers.amazon.aws.operators.batch.BatchCreateComputeEnvironmentOperator} of
 * {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class BatchCreateComputeEnvironmentOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.batch.BatchCreateComputeEnvironmentOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.batch", "BatchCreateComputeEnvironmentOperator")
          .params(
              "compute_environment_name",
              "environment_type",
              "state",
              "compute_resources",
              "unmanaged_v_cpus",
              "service_role",
              "tags",
              "poll_interval",
              "max_retries",
              "deferrable",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("compute_environment_name", "environment_type", "state", "compute_resources")
          .templated(
              "aws_conn_id",
              "compute_environment_name",
              "compute_resources",
              "region_name",
              "service_role",
              "verify")
          .templateFieldsRenderers(Map.of("compute_resources", "json"));

  private BatchCreateComputeEnvironmentOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code BatchCreateComputeEnvironmentOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** Name of the AWS batch compute environment (templated). Templated. */
    public Builder computeEnvironmentName(String value) {
      return param("compute_environment_name", value);
    }

    /** Name of the AWS batch compute environment (templated). Templated. */
    public Builder computeEnvironmentName(TaskRef<?> value) {
      return param("compute_environment_name", value);
    }

    /** Type of the compute-environment. */
    public Builder environmentType(String value) {
      return param("environment_type", value);
    }

    /** State of the compute-environment. */
    public Builder state(String value) {
      return param("state", value);
    }

    /**
     * Details about the resources managed by the compute-environment (templated). More
     * details:
     * https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch.html#Batch.Client.create_compute_environment
     * Templated.
     */
    public Builder computeResources(Map<String, Object> value) {
      return param("compute_resources", value);
    }

    /**
     * Details about the resources managed by the compute-environment (templated). More
     * details:
     * https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch.html#Batch.Client.create_compute_environment
     * Templated.
     */
    public Builder computeResources(TaskRef<?> value) {
      return param("compute_resources", value);
    }

    /**
     * Maximum number of vCPU for an unmanaged compute environment. This parameter is only
     * supported when the `type` parameter is set to `UNMANAGED`.
     */
    public Builder unmanagedVCpus(long value) {
      return param("unmanaged_v_cpus", value);
    }

    /**
     * IAM role that allows Batch to make calls to other AWS services on your behalf
     * (templated). Templated.
     */
    public Builder serviceRole(String value) {
      return param("service_role", value);
    }

    /**
     * IAM role that allows Batch to make calls to other AWS services on your behalf
     * (templated). Templated.
     */
    public Builder serviceRole(TaskRef<?> value) {
      return param("service_role", value);
    }

    /**
     * Tags that you apply to the compute-environment to help you categorize and organize your
     * resources.
     */
    public Builder tags(Map<String, Object> value) {
      return param("tags", value);
    }

    /**
     * How long to wait in seconds between 2 polls at the environment status. Only useful when
     * deferrable is True. Defaults to 30.
     */
    public Builder pollInterval(long value) {
      return param("poll_interval", value);
    }

    /** How many times to poll for the environment status. Only useful when deferrable is True. */
    public Builder maxRetries(long value) {
      return param("max_retries", value);
    }

    /**
     * If True, the operator will wait asynchronously for the environment to be created. This
     * mode requires aiobotocore module to be installed. (default: False) Defaults to false.
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
