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
 * Execute a command in a running container of an existing Pod on Amazon EKS.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.eks.EksPodExecOperator} of {@code
 * apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a Python
 * worker serves.
 */
public final class EksPodExecOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.eks.EksPodExecOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.eks", "EksPodExecOperator")
          .params(
              "cluster_name",
              "pod_name",
              "command",
              "namespace",
              "container_name",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "kubernetes_conn_id",
              "do_xcom_push",
              "max_xcom_output_size")
          .required("cluster_name", "pod_name", "command")
          .templated(
              "aws_conn_id",
              "cluster_name",
              "command",
              "container_name",
              "kubernetes_conn_id",
              "namespace",
              "pod_name",
              "region_name",
              "verify")
          .templateFieldsRenderers(Map.of("command", "py"));

  private EksPodExecOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code EksPodExecOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The name of the Amazon EKS Cluster containing the Pod. (templated) Templated. */
    public Builder clusterName(String value) {
      return param("cluster_name", value);
    }

    /** The name of the Amazon EKS Cluster containing the Pod. (templated) Templated. */
    public Builder clusterName(TaskRef<?> value) {
      return param("cluster_name", value);
    }

    /** Name of the existing Kubernetes Pod. (templated) Templated. */
    public Builder podName(String value) {
      return param("pod_name", value);
    }

    /** Name of the existing Kubernetes Pod. (templated) Templated. */
    public Builder podName(TaskRef<?> value) {
      return param("pod_name", value);
    }

    /** Command and arguments to execute in the container. (templated) Templated. */
    public Builder command(List<String> value) {
      return param("command", value);
    }

    /** Command and arguments to execute in the container. (templated) Templated. */
    public Builder command(TaskRef<?> value) {
      return param("command", value);
    }

    /**
     * Namespace containing the Pod. Defaults to `default`. (templated) Defaults to "default".
     * Templated.
     */
    public Builder namespace(String value) {
      return param("namespace", value);
    }

    /**
     * Namespace containing the Pod. Defaults to `default`. (templated) Defaults to "default".
     * Templated.
     */
    public Builder namespace(TaskRef<?> value) {
      return param("namespace", value);
    }

    /**
     * Name of the container in which to execute the command. When omitted, the
     * `kubectl.kubernetes.io/default-container` annotation or the first container is used.
     * Defaults to `None`. (templated) Templated.
     */
    public Builder containerName(String value) {
      return param("container_name", value);
    }

    /**
     * Name of the container in which to execute the command. When omitted, the
     * `kubectl.kubernetes.io/default-container` annotation or the first container is used.
     * Defaults to `None`. (templated) Templated.
     */
    public Builder containerName(TaskRef<?> value) {
      return param("container_name", value);
    }

    /**
     * The Airflow connection used for AWS credentials. (templated) Defaults to `aws_default`.
     * If this is `None` or empty, the default boto3 credential strategy is used without an
     * Airflow connection lookup. Defaults to "aws_default". Templated.
     */
    public Builder awsConnId(String value) {
      return param("aws_conn_id", value);
    }

    /**
     * The Airflow connection used for AWS credentials. (templated) Defaults to `aws_default`.
     * If this is `None` or empty, the default boto3 credential strategy is used without an
     * Airflow connection lookup. Defaults to "aws_default". Templated.
     */
    public Builder awsConnId(TaskRef<?> value) {
      return param("aws_conn_id", value);
    }

    /**
     * AWS region containing the Amazon EKS Cluster. (templated) Defaults to `None`, which uses
     * the region from the AWS connection when available and otherwise falls back to the
     * default boto3 region strategy. Templated.
     */
    public Builder regionName(String value) {
      return param("region_name", value);
    }

    /**
     * AWS region containing the Amazon EKS Cluster. (templated) Defaults to `None`, which uses
     * the region from the AWS connection when available and otherwise falls back to the
     * default boto3 region strategy. Templated.
     */
    public Builder regionName(TaskRef<?> value) {
      return param("region_name", value);
    }

    /**
     * Whether to verify SSL certificates, or the path to a CA bundle. Defaults to `None`,
     * which uses the value from the AWS connection when available. (templated) Templated.
     */
    public Builder verify(Object value) {
      return param("verify", value);
    }

    /**
     * Configuration dictionary for the botocore client. Defaults to `None`, which uses
     * `config_kwargs` from the AWS connection when available.
     */
    public Builder botocoreConfig(Map<String, Object> value) {
      return param("botocore_config", value);
    }

    /**
     * Kubernetes connection used for additional client configuration. Defaults to
     * `kubernetes_default`, as with `EksPodOperator`. (templated) Defaults to
     * "kubernetes_default". Templated.
     */
    public Builder kubernetesConnId(String value) {
      return param("kubernetes_conn_id", value);
    }

    /**
     * Kubernetes connection used for additional client configuration. Defaults to
     * `kubernetes_default`, as with `EksPodOperator`. (templated) Defaults to
     * "kubernetes_default". Templated.
     */
    public Builder kubernetesConnId(TaskRef<?> value) {
      return param("kubernetes_conn_id", value);
    }

    /** Return standard output through XCom when `True`. Defaults to `False`. Defaults to false. */
    public Builder doXcomPush(boolean value) {
      return param("do_xcom_push", value);
    }

    /** Maximum UTF-8 byte size retained for XCom. Defaults to 49,344 bytes. Defaults to 49344. */
    public Builder maxXcomOutputSize(long value) {
      return param("max_xcom_output_size", value);
    }
  }
}
