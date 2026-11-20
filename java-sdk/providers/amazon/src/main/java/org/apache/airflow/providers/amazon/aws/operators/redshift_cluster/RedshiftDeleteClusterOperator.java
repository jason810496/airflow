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

package org.apache.airflow.providers.amazon.aws.operators.redshift_cluster;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Delete an AWS Redshift cluster.
 *
 * Runs {@code
 * airflow.providers.amazon.aws.operators.redshift_cluster.RedshiftDeleteClusterOperator} of
 * {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class RedshiftDeleteClusterOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.redshift_cluster.RedshiftDeleteClusterOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.redshift_cluster", "RedshiftDeleteClusterOperator")
          .params(
              "cluster_identifier",
              "skip_final_cluster_snapshot",
              "final_cluster_snapshot_identifier",
              "wait_for_completion",
              "poll_interval",
              "deferrable",
              "max_attempts",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("cluster_identifier")
          .templated("aws_conn_id", "cluster_identifier", "region_name", "verify")
          .uiColor("#eeaa11")
          .uiFgcolor("#ffffff");

  private RedshiftDeleteClusterOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code RedshiftDeleteClusterOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** unique identifier of a cluster Templated. */
    public Builder clusterIdentifier(String value) {
      return param("cluster_identifier", value);
    }

    /** unique identifier of a cluster Templated. */
    public Builder clusterIdentifier(TaskRef<?> value) {
      return param("cluster_identifier", value);
    }

    /** determines cluster snapshot creation Defaults to true. */
    public Builder skipFinalClusterSnapshot(boolean value) {
      return param("skip_final_cluster_snapshot", value);
    }

    /** name of final cluster snapshot */
    public Builder finalClusterSnapshotIdentifier(String value) {
      return param("final_cluster_snapshot_identifier", value);
    }

    /** Whether wait for cluster deletion or not The default value is `True` Defaults to true. */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /**
     * Time (in seconds) to wait between two consecutive calls to check cluster state Defaults
     * to 30.
     */
    public Builder pollInterval(long value) {
      return param("poll_interval", value);
    }

    /**
     * Run operator in the deferrable mode. Deferring when `wait_for_completion` is False is
     * deprecated; from the next major release this has no effect when `wait_for_completion` is
     * False. Defaults to false.
     */
    public Builder deferrable(boolean value) {
      return param("deferrable", value);
    }

    /**
     * The maximum number of attempts to be made, both when retrying the delete while the
     * cluster is busy and when waiting for deletion to complete; combined with `poll_interval`
     * the default gives a ~15 minute window, long enough to outlast a pause/resize. Defaults
     * to 30.
     */
    public Builder maxAttempts(long value) {
      return param("max_attempts", value);
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
