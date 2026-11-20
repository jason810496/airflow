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

package org.apache.airflow.providers.amazon.aws.operators.ecs;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Execute a task on AWS ECS (Elastic Container Service).
 *
 * Runs {@code airflow.providers.amazon.aws.operators.ecs.EcsRunTaskOperator} of {@code
 * apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a Python
 * worker serves.
 */
public final class EcsRunTaskOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.ecs.EcsRunTaskOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.ecs", "EcsRunTaskOperator")
          .params(
              "task_definition",
              "cluster",
              "overrides",
              "launch_type",
              "capacity_provider_strategy",
              "volume_configurations",
              "group",
              "placement_constraints",
              "placement_strategy",
              "platform_version",
              "network_configuration",
              "tags",
              "awslogs_group",
              "awslogs_region",
              "awslogs_stream_prefix",
              "awslogs_fetch_interval",
              "container_name",
              "propagate_tags",
              "quota_retry",
              "reattach",
              "number_logs_exception",
              "wait_for_completion",
              "waiter_delay",
              "waiter_max_attempts",
              "deferrable",
              "skip_on_exit_code",
              "fail_on_exit_code",
              "stop_task_on_failure",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("task_definition", "cluster", "overrides")
          .templated(
              "task_definition",
              "cluster",
              "overrides",
              "launch_type",
              "capacity_provider_strategy",
              "volume_configurations",
              "group",
              "placement_constraints",
              "placement_strategy",
              "platform_version",
              "network_configuration",
              "tags",
              "awslogs_group",
              "awslogs_region",
              "awslogs_stream_prefix",
              "awslogs_fetch_interval",
              "container_name",
              "propagate_tags",
              "reattach",
              "number_logs_exception",
              "wait_for_completion",
              "deferrable")
          .templateFieldsRenderers(Map.of("overrides", "json", "network_configuration", "json", "tags", "json"))
          .uiColor("#f0ede4");

  private EcsRunTaskOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code EcsRunTaskOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** the task definition name on Elastic Container Service Templated. */
    public Builder taskDefinition(String value) {
      return param("task_definition", value);
    }

    /** the task definition name on Elastic Container Service Templated. */
    public Builder taskDefinition(TaskRef<?> value) {
      return param("task_definition", value);
    }

    /** the cluster name on Elastic Container Service Templated. */
    public Builder cluster(String value) {
      return param("cluster", value);
    }

    /** the cluster name on Elastic Container Service Templated. */
    public Builder cluster(TaskRef<?> value) {
      return param("cluster", value);
    }

    /**
     * the same parameter that boto3 will receive (templated):
     * https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.run_task
     * Note that `containerOverrides[].environment` is merged into the environment defined by
     * the task definition, while the other container override fields replace the task
     * definition's value. See howto/operator:EcsRunTaskOperator. Templated.
     */
    public Builder overrides(Map<String, Object> value) {
      return param("overrides", value);
    }

    /**
     * the same parameter that boto3 will receive (templated):
     * https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.run_task
     * Note that `containerOverrides[].environment` is merged into the environment defined by
     * the task definition, while the other container override fields replace the task
     * definition's value. See howto/operator:EcsRunTaskOperator. Templated.
     */
    public Builder overrides(TaskRef<?> value) {
      return param("overrides", value);
    }

    /**
     * the launch type on which to run your task ('EC2', 'EXTERNAL', or 'FARGATE') Defaults to
     * "EC2". Templated.
     */
    public Builder launchType(String value) {
      return param("launch_type", value);
    }

    /**
     * the launch type on which to run your task ('EC2', 'EXTERNAL', or 'FARGATE') Defaults to
     * "EC2". Templated.
     */
    public Builder launchType(TaskRef<?> value) {
      return param("launch_type", value);
    }

    /**
     * the capacity provider strategy to use for the task. When capacity_provider_strategy is
     * specified, the launch_type parameter is omitted. If no capacity_provider_strategy or
     * launch_type is specified, the default capacity provider strategy for the cluster is
     * used. Templated.
     */
    public Builder capacityProviderStrategy(List<Object> value) {
      return param("capacity_provider_strategy", value);
    }

    /**
     * the capacity provider strategy to use for the task. When capacity_provider_strategy is
     * specified, the launch_type parameter is omitted. If no capacity_provider_strategy or
     * launch_type is specified, the default capacity provider strategy for the cluster is
     * used. Templated.
     */
    public Builder capacityProviderStrategy(TaskRef<?> value) {
      return param("capacity_provider_strategy", value);
    }

    /**
     * the volume configurations to use when using capacity provider. The name of the volume
     * must match the name from the task definition. You can configure the settings like size,
     * volume type, IOPS, throughput and others mentioned in
     * (https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_TaskManagedEBSVolumeConfiguration.html)
     * Templated.
     */
    public Builder volumeConfigurations(List<Object> value) {
      return param("volume_configurations", value);
    }

    /**
     * the volume configurations to use when using capacity provider. The name of the volume
     * must match the name from the task definition. You can configure the settings like size,
     * volume type, IOPS, throughput and others mentioned in
     * (https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_TaskManagedEBSVolumeConfiguration.html)
     * Templated.
     */
    public Builder volumeConfigurations(TaskRef<?> value) {
      return param("volume_configurations", value);
    }

    /** the name of the task group associated with the task Templated. */
    public Builder group(String value) {
      return param("group", value);
    }

    /** the name of the task group associated with the task Templated. */
    public Builder group(TaskRef<?> value) {
      return param("group", value);
    }

    /** an array of placement constraint objects to use for the task Templated. */
    public Builder placementConstraints(List<Object> value) {
      return param("placement_constraints", value);
    }

    /** an array of placement constraint objects to use for the task Templated. */
    public Builder placementConstraints(TaskRef<?> value) {
      return param("placement_constraints", value);
    }

    /** an array of placement strategy objects to use for the task Templated. */
    public Builder placementStrategy(List<Object> value) {
      return param("placement_strategy", value);
    }

    /** an array of placement strategy objects to use for the task Templated. */
    public Builder placementStrategy(TaskRef<?> value) {
      return param("placement_strategy", value);
    }

    /** the platform version on which your task is running Templated. */
    public Builder platformVersion(String value) {
      return param("platform_version", value);
    }

    /** the platform version on which your task is running Templated. */
    public Builder platformVersion(TaskRef<?> value) {
      return param("platform_version", value);
    }

    /** the network configuration for the task Templated. */
    public Builder networkConfiguration(Map<String, Object> value) {
      return param("network_configuration", value);
    }

    /** the network configuration for the task Templated. */
    public Builder networkConfiguration(TaskRef<?> value) {
      return param("network_configuration", value);
    }

    /** a dictionary of tags in the form of {'tagKey': 'tagValue'}. Templated. */
    public Builder tags(Map<String, Object> value) {
      return param("tags", value);
    }

    /** a dictionary of tags in the form of {'tagKey': 'tagValue'}. Templated. */
    public Builder tags(TaskRef<?> value) {
      return param("tags", value);
    }

    /**
     * the CloudWatch group where your ECS container logs are stored. Only required if you want
     * logs to be shown in the Airflow UI after your job has finished. Templated.
     */
    public Builder awslogsGroup(String value) {
      return param("awslogs_group", value);
    }

    /**
     * the CloudWatch group where your ECS container logs are stored. Only required if you want
     * logs to be shown in the Airflow UI after your job has finished. Templated.
     */
    public Builder awslogsGroup(TaskRef<?> value) {
      return param("awslogs_group", value);
    }

    /**
     * the region in which your CloudWatch logs are stored. If None, this is the same as the
     * `region` parameter. If that is also None, this is the default AWS region based on your
     * connection settings. Templated.
     */
    public Builder awslogsRegion(String value) {
      return param("awslogs_region", value);
    }

    /**
     * the region in which your CloudWatch logs are stored. If None, this is the same as the
     * `region` parameter. If that is also None, this is the default AWS region based on your
     * connection settings. Templated.
     */
    public Builder awslogsRegion(TaskRef<?> value) {
      return param("awslogs_region", value);
    }

    /**
     * the stream prefix that is used for the CloudWatch logs. This should match the prefix
     * specified in the log configuration of the task definition. Only required if you want
     * logs to be shown in the Airflow UI after your job has finished. Templated.
     */
    public Builder awslogsStreamPrefix(String value) {
      return param("awslogs_stream_prefix", value);
    }

    /**
     * the stream prefix that is used for the CloudWatch logs. This should match the prefix
     * specified in the log configuration of the task definition. Only required if you want
     * logs to be shown in the Airflow UI after your job has finished. Templated.
     */
    public Builder awslogsStreamPrefix(TaskRef<?> value) {
      return param("awslogs_stream_prefix", value);
    }

    /**
     * the interval that the ECS task log fetcher should wait in between each Cloudwatch logs
     * fetches. If deferrable is set to True, that parameter is ignored and waiter_delay is
     * used instead. Templated.
     */
    public Builder awslogsFetchInterval(Duration value) {
      return param("awslogs_fetch_interval", value);
    }

    /**
     * the interval that the ECS task log fetcher should wait in between each Cloudwatch logs
     * fetches. If deferrable is set to True, that parameter is ignored and waiter_delay is
     * used instead. Templated.
     */
    public Builder awslogsFetchInterval(TaskRef<?> value) {
      return param("awslogs_fetch_interval", value);
    }

    /**
     * The name of the container to fetch logs from. If not set, the first container is used.
     * Templated.
     */
    public Builder containerName(String value) {
      return param("container_name", value);
    }

    /**
     * The name of the container to fetch logs from. If not set, the first container is used.
     * Templated.
     */
    public Builder containerName(TaskRef<?> value) {
      return param("container_name", value);
    }

    /** Templated. */
    public Builder propagateTags(String value) {
      return param("propagate_tags", value);
    }

    /** Templated. */
    public Builder propagateTags(TaskRef<?> value) {
      return param("propagate_tags", value);
    }

    /** Config if and how to retry the launch of a new ECS task, to handle transient errors. */
    public Builder quotaRetry(Map<String, Object> value) {
      return param("quota_retry", value);
    }

    /**
     * If set to True, will check if the task previously launched by the task_instance is
     * already running. If so, the operator will attach to it instead of starting a new task.
     * This is to avoid relaunching a new task when the connection drops between Airflow and
     * ECS while the task is running (when the Airflow worker is restarted for example).
     * Defaults to false. Templated.
     */
    public Builder reattach(boolean value) {
      return param("reattach", value);
    }

    /**
     * If set to True, will check if the task previously launched by the task_instance is
     * already running. If so, the operator will attach to it instead of starting a new task.
     * This is to avoid relaunching a new task when the connection drops between Airflow and
     * ECS while the task is running (when the Airflow worker is restarted for example).
     * Defaults to false. Templated.
     */
    public Builder reattach(TaskRef<?> value) {
      return param("reattach", value);
    }

    /**
     * Number of lines from the last Cloudwatch logs to return in the AirflowException if an
     * ECS task is stopped (to receive Airflow alerts with the logs of what failed in the code
     * running in ECS). Defaults to 10. Templated.
     */
    public Builder numberLogsException(long value) {
      return param("number_logs_exception", value);
    }

    /**
     * Number of lines from the last Cloudwatch logs to return in the AirflowException if an
     * ECS task is stopped (to receive Airflow alerts with the logs of what failed in the code
     * running in ECS). Defaults to 10. Templated.
     */
    public Builder numberLogsException(TaskRef<?> value) {
      return param("number_logs_exception", value);
    }

    /**
     * If True, waits for creation of the cluster to complete. (default: True) Defaults to
     * true. Templated.
     */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /**
     * If True, waits for creation of the cluster to complete. (default: True) Defaults to
     * true. Templated.
     */
    public Builder waitForCompletion(TaskRef<?> value) {
      return param("wait_for_completion", value);
    }

    /**
     * The amount of time in seconds to wait between attempts, if not set then the default
     * waiter value will be used. Defaults to 6.
     */
    public Builder waiterDelay(long value) {
      return param("waiter_delay", value);
    }

    /**
     * The maximum number of attempts to be made, if not set then the default waiter value will
     * be used. Defaults to 1000000.
     */
    public Builder waiterMaxAttempts(long value) {
      return param("waiter_max_attempts", value);
    }

    /**
     * If True, the operator will wait asynchronously for the job to complete. Deferring when
     * `wait_for_completion` is False is deprecated; from the next major release this has no
     * effect when `wait_for_completion` is False. This mode requires aiobotocore module to be
     * installed. (default: False) Defaults to false. Templated.
     */
    public Builder deferrable(boolean value) {
      return param("deferrable", value);
    }

    /**
     * If True, the operator will wait asynchronously for the job to complete. Deferring when
     * `wait_for_completion` is False is deprecated; from the next major release this has no
     * effect when `wait_for_completion` is False. This mode requires aiobotocore module to be
     * installed. (default: False) Defaults to false. Templated.
     */
    public Builder deferrable(TaskRef<?> value) {
      return param("deferrable", value);
    }

    /**
     * If task exits with this exit code, leave the task in `skipped` state (default: None). If
     * set to `None`, any non-zero exit code will be treated as a failure. Can be an int or a
     * container of ints.
     */
    public Builder skipOnExitCode(long value) {
      return param("skip_on_exit_code", value);
    }

    /**
     * Non-zero exit codes that fail the task without retrying. Can be an int or a container of
     * ints (default: None). A matching code reported by a stopped container takes precedence
     * over `skip_on_exit_code` and other container failures. Missing exit codes do not match
     * this option.
     */
    public Builder failOnExitCode(long value) {
      return param("fail_on_exit_code", value);
    }

    /**
     * If True, attempt to stop the ECS task if the Airflow task fails after the ECS task has
     * started. (default: True) Defaults to true.
     */
    public Builder stopTaskOnFailure(boolean value) {
      return param("stop_task_on_failure", value);
    }

    /**
     * connection id of AWS credentials / region name. If None, credential boto3 strategy will
     * be used
     * (https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html).
     * Defaults to "aws_default".
     */
    public Builder awsConnId(String value) {
      return param("aws_conn_id", value);
    }

    /** AWS region_name. If not specified then the default boto3 behaviour is used. */
    public Builder regionName(String value) {
      return param("region_name", value);
    }

    /**
     * Whether or not to verify SSL certificates. See:
     * https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
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

    /** region name to use in AWS Hook. Override the region in connection (if provided) */
    public Builder region(String value) {
      return param("region", value);
    }
  }
}
