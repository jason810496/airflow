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

import java.util.List;
import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Register a task definition on AWS ECS.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.ecs.EcsRegisterTaskDefinitionOperator} of
 * {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class EcsRegisterTaskDefinitionOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.ecs.EcsRegisterTaskDefinitionOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.ecs", "EcsRegisterTaskDefinitionOperator")
          .params(
              "family",
              "container_definitions",
              "register_task_kwargs",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("family", "container_definitions")
          .templated("family", "container_definitions", "register_task_kwargs");

  private EcsRegisterTaskDefinitionOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code EcsRegisterTaskDefinitionOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The family name of a task definition to create. Templated. */
    public Builder family(String value) {
      return param("family", value);
    }

    /** The family name of a task definition to create. Templated. */
    public Builder family(TaskRef<?> value) {
      return param("family", value);
    }

    /**
     * A list of container definitions in JSON format that describe the different containers
     * that make up your task. Templated.
     */
    public Builder containerDefinitions(List<Map<String, Object>> value) {
      return param("container_definitions", value);
    }

    /**
     * A list of container definitions in JSON format that describe the different containers
     * that make up your task. Templated.
     */
    public Builder containerDefinitions(TaskRef<?> value) {
      return param("container_definitions", value);
    }

    /** Extra arguments for Register Task Definition. Templated. */
    public Builder registerTaskKwargs(Map<String, Object> value) {
      return param("register_task_kwargs", value);
    }

    /** Extra arguments for Register Task Definition. Templated. */
    public Builder registerTaskKwargs(TaskRef<?> value) {
      return param("register_task_kwargs", value);
    }

    /**
     * The Airflow connection used for AWS credentials. If this is `None` or empty then the
     * default boto3 behaviour is used. If running Airflow in a distributed manner and
     * aws_conn_id is None or empty, then default boto3 configuration would be used (and must
     * be maintained on each worker node). Defaults to "aws_default".
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

    public Builder region(String value) {
      return param("region", value);
    }
  }
}
