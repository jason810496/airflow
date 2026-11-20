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

package org.apache.airflow.providers.amazon.aws.operators.datasync;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Find, Create, Update, Execute and Delete AWS DataSync Tasks.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.datasync.DataSyncOperator} of {@code
 * apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a Python
 * worker serves.
 */
public final class DataSyncOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.datasync.DataSyncOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.datasync", "DataSyncOperator")
          .params(
              "wait_interval_seconds",
              "max_iterations",
              "wait_for_completion",
              "task_arn",
              "source_location_uri",
              "destination_location_uri",
              "allow_random_task_choice",
              "allow_random_location_choice",
              "create_task_kwargs",
              "create_source_location_kwargs",
              "create_destination_location_kwargs",
              "update_task_kwargs",
              "task_execution_kwargs",
              "delete_task_after_execution",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .templated(
              "aws_conn_id",
              "create_destination_location_kwargs",
              "create_source_location_kwargs",
              "create_task_kwargs",
              "destination_location_uri",
              "region_name",
              "source_location_uri",
              "task_arn",
              "task_execution_kwargs",
              "update_task_kwargs",
              "verify")
          .templateFieldsRenderers(Map.of("create_task_kwargs", "json", "create_source_location_kwargs", "json", "create_destination_location_kwargs", "json", "update_task_kwargs", "json", "task_execution_kwargs", "json"))
          .uiColor("#44b5e2");

  private DataSyncOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code DataSyncOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /**
     * Time to wait between two consecutive calls to check TaskExecution status. Defaults to
     * 30.
     */
    public Builder waitIntervalSeconds(long value) {
      return param("wait_interval_seconds", value);
    }

    /** Maximum number of consecutive calls to check TaskExecution status. Defaults to 60. */
    public Builder maxIterations(long value) {
      return param("max_iterations", value);
    }

    /** If True, wait for the task execution to reach a final state Defaults to true. */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /**
     * AWS DataSync TaskArn to use. If None, then this operator will attempt to either search
     * for an existing Task or attempt to create a new Task. Templated.
     */
    public Builder taskArn(String value) {
      return param("task_arn", value);
    }

    /**
     * AWS DataSync TaskArn to use. If None, then this operator will attempt to either search
     * for an existing Task or attempt to create a new Task. Templated.
     */
    public Builder taskArn(TaskRef<?> value) {
      return param("task_arn", value);
    }

    /**
     * Source location URI to search for. All DataSync Tasks with a LocationArn with this URI
     * will be considered. Example: `smb://server/subdir` Templated.
     */
    public Builder sourceLocationUri(String value) {
      return param("source_location_uri", value);
    }

    /**
     * Source location URI to search for. All DataSync Tasks with a LocationArn with this URI
     * will be considered. Example: `smb://server/subdir` Templated.
     */
    public Builder sourceLocationUri(TaskRef<?> value) {
      return param("source_location_uri", value);
    }

    /**
     * Destination location URI to search for. All DataSync Tasks with a LocationArn with this
     * URI will be considered. Example: `s3://airflow_bucket/stuff` Templated.
     */
    public Builder destinationLocationUri(String value) {
      return param("destination_location_uri", value);
    }

    /**
     * Destination location URI to search for. All DataSync Tasks with a LocationArn with this
     * URI will be considered. Example: `s3://airflow_bucket/stuff` Templated.
     */
    public Builder destinationLocationUri(TaskRef<?> value) {
      return param("destination_location_uri", value);
    }

    /**
     * If multiple Tasks match, one must be chosen to execute. If allow_random_task_choice is
     * True then a random one is chosen. Defaults to false.
     */
    public Builder allowRandomTaskChoice(boolean value) {
      return param("allow_random_task_choice", value);
    }

    /**
     * If multiple Locations match, one must be chosen when creating a task. If
     * allow_random_location_choice is True then a random one is chosen. Defaults to false.
     */
    public Builder allowRandomLocationChoice(boolean value) {
      return param("allow_random_location_choice", value);
    }

    /**
     * If no suitable TaskArn is identified, it will be created if `create_task_kwargs` is
     * defined. `create_task_kwargs` is then used internally like this:
     * `boto3.create_task(**create_task_kwargs)` Example: `{'Name': 'xyz', 'Options': ...,
     * 'Excludes': ..., 'Tags': ...}` Templated.
     */
    public Builder createTaskKwargs(Map<String, Object> value) {
      return param("create_task_kwargs", value);
    }

    /**
     * If no suitable TaskArn is identified, it will be created if `create_task_kwargs` is
     * defined. `create_task_kwargs` is then used internally like this:
     * `boto3.create_task(**create_task_kwargs)` Example: `{'Name': 'xyz', 'Options': ...,
     * 'Excludes': ..., 'Tags': ...}` Templated.
     */
    public Builder createTaskKwargs(TaskRef<?> value) {
      return param("create_task_kwargs", value);
    }

    /**
     * If no suitable LocationArn is found, a Location will be created if
     * `create_source_location_kwargs` is defined. `create_source_location_kwargs` is then used
     * internally like this: `boto3.create_location_xyz(**create_source_location_kwargs)` The
     * xyz is determined from the prefix of source_location_uri, eg `smb:/...` or `s3:/...`
     * Example: `{'Subdirectory': ..., 'ServerHostname': ..., ...}` Templated.
     */
    public Builder createSourceLocationKwargs(Map<String, Object> value) {
      return param("create_source_location_kwargs", value);
    }

    /**
     * If no suitable LocationArn is found, a Location will be created if
     * `create_source_location_kwargs` is defined. `create_source_location_kwargs` is then used
     * internally like this: `boto3.create_location_xyz(**create_source_location_kwargs)` The
     * xyz is determined from the prefix of source_location_uri, eg `smb:/...` or `s3:/...`
     * Example: `{'Subdirectory': ..., 'ServerHostname': ..., ...}` Templated.
     */
    public Builder createSourceLocationKwargs(TaskRef<?> value) {
      return param("create_source_location_kwargs", value);
    }

    /**
     * If no suitable LocationArn is found, a Location will be created if
     * `create_destination_location_kwargs` is defined. `create_destination_location_kwargs` is
     * used internally like this:
     * `boto3.create_location_xyz(**create_destination_location_kwargs)` The xyz is determined
     * from the prefix of destination_location_uri, eg `smb:/...` or `s3:/...` Example:
     * `{'S3BucketArn': ..., 'S3Config': {'BucketAccessRoleArn': ...}, ...}` Templated.
     */
    public Builder createDestinationLocationKwargs(Map<String, Object> value) {
      return param("create_destination_location_kwargs", value);
    }

    /**
     * If no suitable LocationArn is found, a Location will be created if
     * `create_destination_location_kwargs` is defined. `create_destination_location_kwargs` is
     * used internally like this:
     * `boto3.create_location_xyz(**create_destination_location_kwargs)` The xyz is determined
     * from the prefix of destination_location_uri, eg `smb:/...` or `s3:/...` Example:
     * `{'S3BucketArn': ..., 'S3Config': {'BucketAccessRoleArn': ...}, ...}` Templated.
     */
    public Builder createDestinationLocationKwargs(TaskRef<?> value) {
      return param("create_destination_location_kwargs", value);
    }

    /**
     * If a suitable TaskArn is found or created, it will be updated if `update_task_kwargs` is
     * defined. `update_task_kwargs` is used internally like this:
     * `boto3.update_task(TaskArn=task_arn, **update_task_kwargs)` Example: `{'Name': 'xyz',
     * 'Options': ..., 'Excludes': ...}` Templated.
     */
    public Builder updateTaskKwargs(Map<String, Object> value) {
      return param("update_task_kwargs", value);
    }

    /**
     * If a suitable TaskArn is found or created, it will be updated if `update_task_kwargs` is
     * defined. `update_task_kwargs` is used internally like this:
     * `boto3.update_task(TaskArn=task_arn, **update_task_kwargs)` Example: `{'Name': 'xyz',
     * 'Options': ..., 'Excludes': ...}` Templated.
     */
    public Builder updateTaskKwargs(TaskRef<?> value) {
      return param("update_task_kwargs", value);
    }

    /**
     * Additional kwargs passed directly when starting the Task execution, used internally like
     * this: `boto3.start_task_execution(TaskArn=task_arn, **task_execution_kwargs)` Templated.
     */
    public Builder taskExecutionKwargs(Map<String, Object> value) {
      return param("task_execution_kwargs", value);
    }

    /**
     * Additional kwargs passed directly when starting the Task execution, used internally like
     * this: `boto3.start_task_execution(TaskArn=task_arn, **task_execution_kwargs)` Templated.
     */
    public Builder taskExecutionKwargs(TaskRef<?> value) {
      return param("task_execution_kwargs", value);
    }

    /**
     * If True then the TaskArn which was executed will be deleted from AWS DataSync on
     * successful completion. Defaults to false.
     */
    public Builder deleteTaskAfterExecution(boolean value) {
      return param("delete_task_after_execution", value);
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
