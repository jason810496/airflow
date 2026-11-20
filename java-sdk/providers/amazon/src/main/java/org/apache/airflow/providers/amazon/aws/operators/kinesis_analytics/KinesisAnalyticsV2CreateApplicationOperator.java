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

package org.apache.airflow.providers.amazon.aws.operators.kinesis_analytics;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Creates an AWS Managed Service for Apache Flink application.
 *
 * Runs {@code
 * airflow.providers.amazon.aws.operators.kinesis_analytics.KinesisAnalyticsV2CreateApplicationOperator}
 * of {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that
 * a Python worker serves.
 */
public final class KinesisAnalyticsV2CreateApplicationOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.kinesis_analytics.KinesisAnalyticsV2CreateApplicationOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.kinesis_analytics", "KinesisAnalyticsV2CreateApplicationOperator")
          .params(
              "application_name",
              "runtime_environment",
              "service_execution_role",
              "create_application_kwargs",
              "application_description",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("application_name", "runtime_environment", "service_execution_role")
          .templated(
              "application_description",
              "application_name",
              "aws_conn_id",
              "create_application_kwargs",
              "region_name",
              "runtime_environment",
              "service_execution_role",
              "verify")
          .templateFieldsRenderers(Map.of("create_application_kwargs", "json"))
          .uiColor("#44b5e2");

  private KinesisAnalyticsV2CreateApplicationOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code KinesisAnalyticsV2CreateApplicationOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The name of application. (templated) Templated. */
    public Builder applicationName(String value) {
      return param("application_name", value);
    }

    /** The name of application. (templated) Templated. */
    public Builder applicationName(TaskRef<?> value) {
      return param("application_name", value);
    }

    /** The runtime environment for the application. (templated) Templated. */
    public Builder runtimeEnvironment(String value) {
      return param("runtime_environment", value);
    }

    /** The runtime environment for the application. (templated) Templated. */
    public Builder runtimeEnvironment(TaskRef<?> value) {
      return param("runtime_environment", value);
    }

    /** The IAM role used by the application to access services. (templated) Templated. */
    public Builder serviceExecutionRole(String value) {
      return param("service_execution_role", value);
    }

    /** The IAM role used by the application to access services. (templated) Templated. */
    public Builder serviceExecutionRole(TaskRef<?> value) {
      return param("service_execution_role", value);
    }

    /** Create application extra properties. (templated) Templated. */
    public Builder createApplicationKwargs(Map<String, Object> value) {
      return param("create_application_kwargs", value);
    }

    /** Create application extra properties. (templated) Templated. */
    public Builder createApplicationKwargs(TaskRef<?> value) {
      return param("create_application_kwargs", value);
    }

    /**
     * A summary description of the application. (templated) Defaults to "Managed Service for
     * Apache Flink application created from Airflow". Templated.
     */
    public Builder applicationDescription(String value) {
      return param("application_description", value);
    }

    /**
     * A summary description of the application. (templated) Defaults to "Managed Service for
     * Apache Flink application created from Airflow". Templated.
     */
    public Builder applicationDescription(TaskRef<?> value) {
      return param("application_description", value);
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
     * Whether to verify SSL certificates. See:
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
