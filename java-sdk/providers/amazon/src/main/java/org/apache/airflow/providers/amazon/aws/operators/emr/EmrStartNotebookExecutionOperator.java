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

package org.apache.airflow.providers.amazon.aws.operators.emr;

import java.util.List;
import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * An operator that starts an EMR notebook execution.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.emr.EmrStartNotebookExecutionOperator} of
 * {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class EmrStartNotebookExecutionOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.emr.EmrStartNotebookExecutionOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.emr", "EmrStartNotebookExecutionOperator")
          .params(
              "editor_id",
              "relative_path",
              "cluster_id",
              "service_role",
              "notebook_execution_name",
              "notebook_params",
              "notebook_instance_security_group_id",
              "master_instance_security_group_id",
              "tags",
              "wait_for_completion",
              "waiter_max_attempts",
              "waiter_delay",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("editor_id", "relative_path", "cluster_id", "service_role")
          .templated(
              "aws_conn_id",
              "cluster_id",
              "editor_id",
              "master_instance_security_group_id",
              "notebook_execution_name",
              "notebook_instance_security_group_id",
              "notebook_params",
              "region_name",
              "relative_path",
              "service_role",
              "tags",
              "verify",
              "waiter_delay",
              "waiter_max_attempts");

  private EmrStartNotebookExecutionOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code EmrStartNotebookExecutionOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The unique identifier of the EMR notebook to use for notebook execution. Templated. */
    public Builder editorId(String value) {
      return param("editor_id", value);
    }

    /** The unique identifier of the EMR notebook to use for notebook execution. Templated. */
    public Builder editorId(TaskRef<?> value) {
      return param("editor_id", value);
    }

    /**
     * The path and file name of the notebook file for this execution, relative to the path
     * specified for the EMR notebook. Templated.
     */
    public Builder relativePath(String value) {
      return param("relative_path", value);
    }

    /**
     * The path and file name of the notebook file for this execution, relative to the path
     * specified for the EMR notebook. Templated.
     */
    public Builder relativePath(TaskRef<?> value) {
      return param("relative_path", value);
    }

    /** The unique identifier of the EMR cluster the notebook is attached to. Templated. */
    public Builder clusterId(String value) {
      return param("cluster_id", value);
    }

    /** The unique identifier of the EMR cluster the notebook is attached to. Templated. */
    public Builder clusterId(TaskRef<?> value) {
      return param("cluster_id", value);
    }

    /**
     * The name or ARN of the IAM role that is used as the service role for Amazon EMR (the EMR
     * role) for the notebook execution. Templated.
     */
    public Builder serviceRole(String value) {
      return param("service_role", value);
    }

    /**
     * The name or ARN of the IAM role that is used as the service role for Amazon EMR (the EMR
     * role) for the notebook execution. Templated.
     */
    public Builder serviceRole(TaskRef<?> value) {
      return param("service_role", value);
    }

    /** Optional name for the notebook execution. Templated. */
    public Builder notebookExecutionName(String value) {
      return param("notebook_execution_name", value);
    }

    /** Optional name for the notebook execution. Templated. */
    public Builder notebookExecutionName(TaskRef<?> value) {
      return param("notebook_execution_name", value);
    }

    /**
     * Input parameters in JSON format passed to the EMR notebook at runtime for execution.
     * Templated.
     */
    public Builder notebookParams(String value) {
      return param("notebook_params", value);
    }

    /**
     * Input parameters in JSON format passed to the EMR notebook at runtime for execution.
     * Templated.
     */
    public Builder notebookParams(TaskRef<?> value) {
      return param("notebook_params", value);
    }

    /**
     * The unique identifier of the Amazon EC2 security group to associate with the EMR
     * notebook for this notebook execution. Templated.
     */
    public Builder notebookInstanceSecurityGroupId(String value) {
      return param("notebook_instance_security_group_id", value);
    }

    /**
     * The unique identifier of the Amazon EC2 security group to associate with the EMR
     * notebook for this notebook execution. Templated.
     */
    public Builder notebookInstanceSecurityGroupId(TaskRef<?> value) {
      return param("notebook_instance_security_group_id", value);
    }

    /**
     * Optional unique ID of an EC2 security group to associate with the master instance of the
     * EMR cluster for this notebook execution. Templated.
     */
    public Builder masterInstanceSecurityGroupId(String value) {
      return param("master_instance_security_group_id", value);
    }

    /**
     * Optional unique ID of an EC2 security group to associate with the master instance of the
     * EMR cluster for this notebook execution. Templated.
     */
    public Builder masterInstanceSecurityGroupId(TaskRef<?> value) {
      return param("master_instance_security_group_id", value);
    }

    /** Optional list of key value pair to associate with the notebook execution. Templated. */
    public Builder tags(List<Object> value) {
      return param("tags", value);
    }

    /** Optional list of key value pair to associate with the notebook execution. Templated. */
    public Builder tags(TaskRef<?> value) {
      return param("tags", value);
    }

    /** Defaults to false. */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /** Maximum number of tries before failing. Templated. */
    public Builder waiterMaxAttempts(long value) {
      return param("waiter_max_attempts", value);
    }

    /** Maximum number of tries before failing. Templated. */
    public Builder waiterMaxAttempts(TaskRef<?> value) {
      return param("waiter_max_attempts", value);
    }

    /** Number of seconds between polling the state of the notebook. Templated. */
    public Builder waiterDelay(long value) {
      return param("waiter_delay", value);
    }

    /** Number of seconds between polling the state of the notebook. Templated. */
    public Builder waiterDelay(TaskRef<?> value) {
      return param("waiter_delay", value);
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
