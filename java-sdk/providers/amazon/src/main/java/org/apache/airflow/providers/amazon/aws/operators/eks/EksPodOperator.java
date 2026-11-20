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

import java.util.List;
import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Executes a task in a Kubernetes pod on the specified Amazon EKS Cluster.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.eks.EksPodOperator} of {@code
 * apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a Python
 * worker serves.
 *
 * Not available from Java, because JSON cannot carry them: {@code ports}, {@code
 * volume_mounts}, {@code volumes}, {@code env_from}, {@code secrets}, {@code
 * container_resources}, {@code affinity}, {@code image_pull_secrets}, {@code host_aliases},
 * {@code tolerations}, {@code dns_config}, {@code full_pod_spec}, {@code init_containers},
 * {@code pod_runtime_info_envs}, {@code callbacks}, {@code progress_callback}, {@code
 * log_formatter}.
 */
public final class EksPodOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.eks.EksPodOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.eks", "EksPodOperator")
          .params(
              "cluster_name",
              "in_cluster",
              "namespace",
              "pod_name",
              "aws_conn_id",
              "region",
              "on_finish_action",
              "kubernetes_conn_id",
              "image",
              "random_name_suffix",
              "cmds",
              "arguments",
              "env_vars",
              "cluster_context",
              "labels",
              "reattach_on_restart",
              "durable",
              "startup_timeout_seconds",
              "startup_check_interval_seconds",
              "schedule_timeout_seconds",
              "get_logs",
              "base_container_name",
              "base_container_status_polling_interval",
              "init_container_logs",
              "container_logs",
              "image_pull_policy",
              "annotations",
              "config_file",
              "runtime_class_name",
              "node_selector",
              "service_account_name",
              "automount_service_account_token",
              "hostnetwork",
              "security_context",
              "container_security_context",
              "xcom_sidecar_container_security_context",
              "dnspolicy",
              "hostname",
              "subdomain",
              "schedulername",
              "log_events_on_failure",
              "log_events_on_startup",
              "do_xcom_push",
              "pod_template_file",
              "pod_template_dict",
              "priority_class_name",
              "termination_grace_period",
              "configmaps",
              "skip_on_exit_code",
              "deferrable",
              "poll_interval",
              "log_pod_spec_on_failure",
              "on_kill_action",
              "is_delete_operator_pod",
              "termination_message_policy",
              "active_deadline_seconds",
              "logging_interval",
              "container_name_log_prefix_enabled")
          .required("cluster_name")
          .templated(
              "labels",
              "namespace",
              "hostname",
              "annotations",
              "trigger_kwargs",
              "configmaps",
              "aws_conn_id",
              "volume_mounts",
              "env_vars",
              "node_selector",
              "image",
              "env_from",
              "config_file",
              "in_cluster",
              "volumes",
              "container_resources",
              "name",
              "base_container_name",
              "cmds",
              "pod_template_dict",
              "arguments",
              "pod_name",
              "pod_template_file",
              "cluster_context",
              "kubernetes_conn_id",
              "cluster_name",
              "region")
          .templateFieldsRenderers(Map.of("env_vars", "py"));

  private EksPodOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code EksPodOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The name of the Amazon EKS Cluster to execute the task on. (templated) Templated. */
    public Builder clusterName(String value) {
      return param("cluster_name", value);
    }

    /** The name of the Amazon EKS Cluster to execute the task on. (templated) Templated. */
    public Builder clusterName(TaskRef<?> value) {
      return param("cluster_name", value);
    }

    /**
     * If True, look for config inside the cluster; if False look for a local file path.
     * Defaults to false. Templated.
     */
    public Builder inCluster(boolean value) {
      return param("in_cluster", value);
    }

    /**
     * If True, look for config inside the cluster; if False look for a local file path.
     * Defaults to false. Templated.
     */
    public Builder inCluster(TaskRef<?> value) {
      return param("in_cluster", value);
    }

    /** The namespace in which to execute the pod. (templated) Defaults to "default". Templated. */
    public Builder namespace(String value) {
      return param("namespace", value);
    }

    /** The namespace in which to execute the pod. (templated) Defaults to "default". Templated. */
    public Builder namespace(TaskRef<?> value) {
      return param("namespace", value);
    }

    /** The unique name to give the pod. (templated) Templated. */
    public Builder podName(String value) {
      return param("pod_name", value);
    }

    /** The unique name to give the pod. (templated) Templated. */
    public Builder podName(TaskRef<?> value) {
      return param("pod_name", value);
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
    public Builder region(String value) {
      return param("region", value);
    }

    /**
     * Which AWS region the connection should use. (templated) If this is None or empty then
     * the default boto3 behaviour is used. Templated.
     */
    public Builder region(TaskRef<?> value) {
      return param("region", value);
    }

    /**
     * What to do when the pod reaches its final state, or the execution is interrupted. If
     * "delete_pod", the pod will be deleted regardless its state; if "delete_succeeded_pod",
     * only succeeded pod will be deleted. You can set to "keep_pod" to keep the pod. Current
     * default is `keep_pod`, but this will be changed in the next major release of this
     * provider.
     */
    public Builder onFinishAction(String value) {
      return param("on_finish_action", value);
    }

    /**
     * The kubernetes connection id for the Kubernetes cluster. (templated) Defaults to
     * "kubernetes_default". Templated.
     */
    public Builder kubernetesConnId(String value) {
      return param("kubernetes_conn_id", value);
    }

    /**
     * The kubernetes connection id for the Kubernetes cluster. (templated) Defaults to
     * "kubernetes_default". Templated.
     */
    public Builder kubernetesConnId(TaskRef<?> value) {
      return param("kubernetes_conn_id", value);
    }

    /**
     * Container image you wish to launch. Defaults to hub.docker.com, but fully qualified URLS
     * will point to custom repositories. (templated) Templated.
     */
    public Builder image(String value) {
      return param("image", value);
    }

    /**
     * Container image you wish to launch. Defaults to hub.docker.com, but fully qualified URLS
     * will point to custom repositories. (templated) Templated.
     */
    public Builder image(TaskRef<?> value) {
      return param("image", value);
    }

    /** if True, will generate a random suffix. Defaults to true. */
    public Builder randomNameSuffix(boolean value) {
      return param("random_name_suffix", value);
    }

    /**
     * entrypoint of the container. The container images's entrypoint is used if this is not
     * provided. (templated) Templated.
     */
    public Builder cmds(List<String> value) {
      return param("cmds", value);
    }

    /**
     * entrypoint of the container. The container images's entrypoint is used if this is not
     * provided. (templated) Templated.
     */
    public Builder cmds(TaskRef<?> value) {
      return param("cmds", value);
    }

    /**
     * arguments of the entrypoint. The container image's CMD is used if this is not provided.
     * (templated) Templated.
     */
    public Builder arguments(List<String> value) {
      return param("arguments", value);
    }

    /**
     * arguments of the entrypoint. The container image's CMD is used if this is not provided.
     * (templated) Templated.
     */
    public Builder arguments(TaskRef<?> value) {
      return param("arguments", value);
    }

    /** Environment variables initialized in the container. (templated) Templated. */
    public Builder envVars(Map<String, String> value) {
      return param("env_vars", value);
    }

    /** Environment variables initialized in the container. (templated) Templated. */
    public Builder envVars(TaskRef<?> value) {
      return param("env_vars", value);
    }

    /**
     * context that points to kubernetes cluster. Ignored when in_cluster is True. If None,
     * current-context is used. (templated) Templated.
     */
    public Builder clusterContext(String value) {
      return param("cluster_context", value);
    }

    /**
     * context that points to kubernetes cluster. Ignored when in_cluster is True. If None,
     * current-context is used. (templated) Templated.
     */
    public Builder clusterContext(TaskRef<?> value) {
      return param("cluster_context", value);
    }

    /** labels to apply to the Pod. (templated) Templated. */
    public Builder labels(Map<String, Object> value) {
      return param("labels", value);
    }

    /** labels to apply to the Pod. (templated) Templated. */
    public Builder labels(TaskRef<?> value) {
      return param("labels", value);
    }

    /**
     * deprecated, use `durable` instead. If the worker dies while the pod is running, reattach
     * and monitor during the next try. If False, always create a new pod for each try. Below
     * Airflow 3.3, this remains the only way to control that behavior, since `durable` has no
     * effect there.
     */
    public Builder reattachOnRestart(boolean value) {
      return param("reattach_on_restart", value);
    }

    /**
     * if the worker dies while the pod is running, reattach and monitor during the next try
     * instead of creating a duplicate pod. If False, always create a new pod for each try.
     * Supersedes `reattach_on_restart` on Airflow 3.3+, where the reconnection uses a
     * persisted pod identity in task state store instead of a label search. Defaults to `True`
     * on Airflow 3.3+. Below 3.3, `durable` has no effect -- setting it explicitly only emits
     * a warning -- and `reattach_on_restart` (default `True`) is the only lever, using the
     * same label-search reattach mechanism it always has.
     */
    public Builder durable(boolean value) {
      return param("durable", value);
    }

    /** timeout in seconds to startup the pod after pod was scheduled. Defaults to 120. */
    public Builder startupTimeoutSeconds(long value) {
      return param("startup_timeout_seconds", value);
    }

    /** interval in seconds to check if the pod has already started Defaults to 5. */
    public Builder startupCheckIntervalSeconds(long value) {
      return param("startup_check_interval_seconds", value);
    }

    /** timeout in seconds to schedule pod in cluster. */
    public Builder scheduleTimeoutSeconds(long value) {
      return param("schedule_timeout_seconds", value);
    }

    /** get the stdout of the base container as logs of the tasks. Defaults to true. */
    public Builder getLogs(boolean value) {
      return param("get_logs", value);
    }

    /**
     * The name of the base container in the pod. This container's logs will appear as part of
     * this task's logs if get_logs is True. Defaults to None. If None, will consult the class
     * variable BASE_CONTAINER_NAME (which defaults to "base") for the base container name to
     * use. (templated) Templated.
     */
    public Builder baseContainerName(String value) {
      return param("base_container_name", value);
    }

    /**
     * The name of the base container in the pod. This container's logs will appear as part of
     * this task's logs if get_logs is True. Defaults to None. If None, will consult the class
     * variable BASE_CONTAINER_NAME (which defaults to "base") for the base container name to
     * use. (templated) Templated.
     */
    public Builder baseContainerName(TaskRef<?> value) {
      return param("base_container_name", value);
    }

    /**
     * Polling period in seconds to check for the pod base container status. Default to 1s.
     * Defaults to 1.
     */
    public Builder baseContainerStatusPollingInterval(double value) {
      return param("base_container_status_polling_interval", value);
    }

    /**
     * list of init containers whose logs will be published to stdout Takes a sequence of
     * containers, a single container name or True. If True, all the containers logs are
     * published.
     */
    public Builder initContainerLogs(Object value) {
      return param("init_container_logs", value);
    }

    /**
     * list of containers whose logs will be published to stdout Takes a sequence of
     * containers, a single container name or True. If True, all the containers logs are
     * published. Works in conjunction with get_logs param. The default value is the base
     * container.
     */
    public Builder containerLogs(Object value) {
      return param("container_logs", value);
    }

    /** Specify a policy to cache or always pull an image. */
    public Builder imagePullPolicy(String value) {
      return param("image_pull_policy", value);
    }

    /**
     * non-identifying metadata you can attach to the Pod. Can be a large range of data, and
     * can include characters that are not permitted by labels. (templated) Templated.
     */
    public Builder annotations(Map<String, Object> value) {
      return param("annotations", value);
    }

    /**
     * non-identifying metadata you can attach to the Pod. Can be a large range of data, and
     * can include characters that are not permitted by labels. (templated) Templated.
     */
    public Builder annotations(TaskRef<?> value) {
      return param("annotations", value);
    }

    /**
     * The path to the Kubernetes config file. (templated) If not specified, default value is
     * `~/.kube/config` Templated.
     */
    public Builder configFile(String value) {
      return param("config_file", value);
    }

    /**
     * The path to the Kubernetes config file. (templated) If not specified, default value is
     * `~/.kube/config` Templated.
     */
    public Builder configFile(TaskRef<?> value) {
      return param("config_file", value);
    }

    /** The name of the RuntimeClass to use for all containers in the pod. */
    public Builder runtimeClassName(String value) {
      return param("runtime_class_name", value);
    }

    /** A dict containing a group of scheduling rules. (templated) Templated. */
    public Builder nodeSelector(Map<String, Object> value) {
      return param("node_selector", value);
    }

    /** A dict containing a group of scheduling rules. (templated) Templated. */
    public Builder nodeSelector(TaskRef<?> value) {
      return param("node_selector", value);
    }

    /** Name of the service account */
    public Builder serviceAccountName(String value) {
      return param("service_account_name", value);
    }

    /**
     * indicates whether pods running as this service account should have an API token
     * automatically mounted
     */
    public Builder automountServiceAccountToken(boolean value) {
      return param("automount_service_account_token", value);
    }

    /** If True enable host networking on the pod. Defaults to false. */
    public Builder hostnetwork(boolean value) {
      return param("hostnetwork", value);
    }

    /** security options the pod should run with (PodSecurityContext). */
    public Builder securityContext(Map<String, Object> value) {
      return param("security_context", value);
    }

    /** security options the container should run with. */
    public Builder containerSecurityContext(Map<String, Object> value) {
      return param("container_security_context", value);
    }

    /**
     * security options the xcom sidecar container should run with. Overrides the value
     * configured on the Kubernetes connection.
     */
    public Builder xcomSidecarContainerSecurityContext(Map<String, Object> value) {
      return param("xcom_sidecar_container_security_context", value);
    }

    /** dnspolicy for the pod. */
    public Builder dnspolicy(String value) {
      return param("dnspolicy", value);
    }

    /** hostname for the pod. (templated) Templated. */
    public Builder hostname(String value) {
      return param("hostname", value);
    }

    /** hostname for the pod. (templated) Templated. */
    public Builder hostname(TaskRef<?> value) {
      return param("hostname", value);
    }

    /** subdomain for the pod. */
    public Builder subdomain(String value) {
      return param("subdomain", value);
    }

    /** Specify a schedulername for the pod */
    public Builder schedulername(String value) {
      return param("schedulername", value);
    }

    /** Log the pod's events if a failure occurs Defaults to false. */
    public Builder logEventsOnFailure(boolean value) {
      return param("log_events_on_failure", value);
    }

    /**
     * Log the pod's Kubernetes events while waiting for it to start. Each poll makes the API
     * server read every event in the namespace; disable on very large clusters. Defaults to
     * true.
     */
    public Builder logEventsOnStartup(boolean value) {
      return param("log_events_on_startup", value);
    }

    /**
     * If True, the content of the file /airflow/xcom/return.json in the container will also be
     * pushed to an XCom when the container completes. Defaults to false.
     */
    public Builder doXcomPush(boolean value) {
      return param("do_xcom_push", value);
    }

    /** path to pod template file (templated) Templated. */
    public Builder podTemplateFile(String value) {
      return param("pod_template_file", value);
    }

    /** path to pod template file (templated) Templated. */
    public Builder podTemplateFile(TaskRef<?> value) {
      return param("pod_template_file", value);
    }

    /** pod template dictionary (templated) Templated. */
    public Builder podTemplateDict(Map<String, Object> value) {
      return param("pod_template_dict", value);
    }

    /** pod template dictionary (templated) Templated. */
    public Builder podTemplateDict(TaskRef<?> value) {
      return param("pod_template_dict", value);
    }

    /** priority class name for the launched Pod */
    public Builder priorityClassName(String value) {
      return param("priority_class_name", value);
    }

    /**
     * Termination grace period (in seconds) for the pod. This sets the pod's
     * `terminationGracePeriodSeconds` and is also used as the grace period when deleting the
     * pod if the task is killed. If not specified, uses the Kubernetes default (30 seconds).
     */
    public Builder terminationGracePeriod(long value) {
      return param("termination_grace_period", value);
    }

    /**
     * (Optional) A list of names of config maps from which it collects ConfigMaps to populate
     * the environment variables with. The contents of the target ConfigMap's Data field will
     * represent the key-value pairs as environment variables. Extends env_from. (templated)
     * Templated.
     */
    public Builder configmaps(List<String> value) {
      return param("configmaps", value);
    }

    /**
     * (Optional) A list of names of config maps from which it collects ConfigMaps to populate
     * the environment variables with. The contents of the target ConfigMap's Data field will
     * represent the key-value pairs as environment variables. Extends env_from. (templated)
     * Templated.
     */
    public Builder configmaps(TaskRef<?> value) {
      return param("configmaps", value);
    }

    /**
     * If task exits with this exit code, leave the task in `skipped` state (default: None). If
     * set to `None`, any non-zero exit code will be treated as a failure.
     */
    public Builder skipOnExitCode(long value) {
      return param("skip_on_exit_code", value);
    }

    /** Run operator in the deferrable mode. Defaults to false. */
    public Builder deferrable(boolean value) {
      return param("deferrable", value);
    }

    /**
     * Polling period in seconds to check for the status. Used only in deferrable mode.
     * Defaults to 2.
     */
    public Builder pollInterval(double value) {
      return param("poll_interval", value);
    }

    /** Log the pod's specification if a failure occurs Defaults to true. */
    public Builder logPodSpecOnFailure(boolean value) {
      return param("log_pod_spec_on_failure", value);
    }

    /**
     * What to do when the task is killed by the user (e.g. manually marked as success/failed
     * from the Airflow UI). If "delete_pod" (default), the pod will be deleted. If "keep_pod",
     * the pod will not be deleted. In deferrable mode this is forwarded to the trigger which
     * controls cleanup when a deferred task is cancelled. Defaults to "delete_pod".
     */
    public Builder onKillAction(String value) {
      return param("on_kill_action", value);
    }

    public Builder isDeleteOperatorPod(boolean value) {
      return param("is_delete_operator_pod", value);
    }

    /**
     * The termination message policy of the base container. Default value is "File" Defaults
     * to "File".
     */
    public Builder terminationMessagePolicy(String value) {
      return param("termination_message_policy", value);
    }

    /** The active_deadline_seconds which translates to active_deadline_seconds in V1PodSpec. */
    public Builder activeDeadlineSeconds(long value) {
      return param("active_deadline_seconds", value);
    }

    /**
     * max time in seconds that task should be in deferred state before resuming to fetch the
     * latest logs. If `None`, then the task will remain in deferred state until pod is done,
     * and no logs will be visible until that time.
     */
    public Builder loggingInterval(long value) {
      return param("logging_interval", value);
    }

    /** if True, will prefix container name to each log line. Default to True. Defaults to true. */
    public Builder containerNameLogPrefixEnabled(boolean value) {
      return param("container_name_log_prefix_enabled", value);
    }
  }
}
