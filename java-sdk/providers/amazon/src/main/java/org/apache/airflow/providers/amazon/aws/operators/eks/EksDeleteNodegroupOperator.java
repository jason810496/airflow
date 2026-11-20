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

package org.apache.airflow.providers.amazon.aws.operators.eks;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Deletes an Amazon EKS managed node group from an Amazon EKS Cluster.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.eks.EksDeleteNodegroupOperator} of {@code
 * apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a Python
 * worker serves.
 */
public final class EksDeleteNodegroupOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.eks.EksDeleteNodegroupOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.eks", "EksDeleteNodegroupOperator")
          .params(
              "cluster_name",
              "nodegroup_name",
              "region",
              "wait_for_completion",
              "waiter_delay",
              "waiter_max_attempts",
              "deferrable",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config")
          .required("cluster_name", "nodegroup_name")
          .templated(
              "aws_conn_id",
              "cluster_name",
              "nodegroup_name",
              "region_name",
              "verify",
              "wait_for_completion");

  private EksDeleteNodegroupOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code EksDeleteNodegroupOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /**
     * The name of the Amazon EKS Cluster associated with your nodegroup. (templated)
     * Templated.
     */
    public Builder clusterName(String value) {
      return param("cluster_name", value);
    }

    /**
     * The name of the Amazon EKS Cluster associated with your nodegroup. (templated)
     * Templated.
     */
    public Builder clusterName(TaskRef<?> value) {
      return param("cluster_name", value);
    }

    /** The name of the nodegroup to delete. (templated) Templated. */
    public Builder nodegroupName(String value) {
      return param("nodegroup_name", value);
    }

    /** The name of the nodegroup to delete. (templated) Templated. */
    public Builder nodegroupName(TaskRef<?> value) {
      return param("nodegroup_name", value);
    }

    public Builder region(String value) {
      return param("region", value);
    }

    /**
     * If True, waits for operator to complete. (default: False) (templated) Defaults to false.
     * Templated.
     */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /**
     * If True, waits for operator to complete. (default: False) (templated) Defaults to false.
     * Templated.
     */
    public Builder waitForCompletion(TaskRef<?> value) {
      return param("wait_for_completion", value);
    }

    /**
     * Time (in seconds) to wait between two consecutive calls to check nodegroup state
     * Defaults to 30.
     */
    public Builder waiterDelay(long value) {
      return param("waiter_delay", value);
    }

    /** The maximum number of attempts to check nodegroup state Defaults to 40. */
    public Builder waiterMaxAttempts(long value) {
      return param("waiter_max_attempts", value);
    }

    /**
     * If True, the operator will wait asynchronously for the nodegroup to be deleted.
     * Deferring when `wait_for_completion` is False is deprecated; from the next major release
     * this has no effect when `wait_for_completion` is False. This mode requires aiobotocore
     * module to be installed. (default: False) Defaults to false.
     */
    public Builder deferrable(boolean value) {
      return param("deferrable", value);
    }

    /**
     * The Airflow connection used for AWS credentials. (templated) If this is None or empty
     * then the default boto3 behaviour is used. If running Airflow in a distributed manner and
     * aws_conn_id is None or empty, then the default boto3 configuration would be used (and
     * must be maintained on each worker node). Defaults to "aws_default". Templated.
     */
    public Builder awsConnId(String value) {
      return param("aws_conn_id", value);
    }

    /**
     * The Airflow connection used for AWS credentials. (templated) If this is None or empty
     * then the default boto3 behaviour is used. If running Airflow in a distributed manner and
     * aws_conn_id is None or empty, then the default boto3 configuration would be used (and
     * must be maintained on each worker node). Defaults to "aws_default". Templated.
     */
    public Builder awsConnId(TaskRef<?> value) {
      return param("aws_conn_id", value);
    }

    /**
     * Which AWS region the connection should use. (templated) If this is None or empty then
     * the default boto3 behaviour is used. Templated.
     */
    public Builder regionName(String value) {
      return param("region_name", value);
    }

    /**
     * Which AWS region the connection should use. (templated) If this is None or empty then
     * the default boto3 behaviour is used. Templated.
     */
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
  }
}
