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

package org.apache.airflow.providers.amazon.aws.operators.mwaa;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Trigger a Dag Run for a Dag in an Amazon MWAA environment.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.mwaa.MwaaTriggerDagRunOperator} of {@code
 * apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a Python
 * worker serves.
 */
public final class MwaaTriggerDagRunOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.mwaa.MwaaTriggerDagRunOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.mwaa", "MwaaTriggerDagRunOperator")
          .params(
              "env_name",
              "trigger_dag_id",
              "trigger_run_id",
              "logical_date",
              "data_interval_start",
              "data_interval_end",
              "conf",
              "note",
              "airflow_version",
              "wait_for_completion",
              "waiter_delay",
              "waiter_max_attempts",
              "deferrable",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("env_name", "trigger_dag_id")
          .templated(
              "airflow_version",
              "aws_conn_id",
              "conf",
              "data_interval_end",
              "data_interval_start",
              "env_name",
              "logical_date",
              "note",
              "region_name",
              "trigger_dag_id",
              "trigger_run_id",
              "verify")
          .templateFieldsRenderers(Map.of("conf", "json"));

  private MwaaTriggerDagRunOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code MwaaTriggerDagRunOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The MWAA environment name (templated) Templated. */
    public Builder envName(String value) {
      return param("env_name", value);
    }

    /** The MWAA environment name (templated) Templated. */
    public Builder envName(TaskRef<?> value) {
      return param("env_name", value);
    }

    /** The ID of the Dag to be triggered (templated) Templated. */
    public Builder triggerDagId(String value) {
      return param("trigger_dag_id", value);
    }

    /** The ID of the Dag to be triggered (templated) Templated. */
    public Builder triggerDagId(TaskRef<?> value) {
      return param("trigger_dag_id", value);
    }

    /** The Run ID. This together with trigger_dag_id are a unique key. (templated) Templated. */
    public Builder triggerRunId(String value) {
      return param("trigger_run_id", value);
    }

    /** The Run ID. This together with trigger_dag_id are a unique key. (templated) Templated. */
    public Builder triggerRunId(TaskRef<?> value) {
      return param("trigger_run_id", value);
    }

    /**
     * The logical date (previously called execution date). This is the time or interval
     * covered by this Dag run, according to the Dag definition. This together with
     * trigger_dag_id are a unique key. This field is required if your environment is running
     * with Airflow 3. (templated) Templated.
     */
    public Builder logicalDate(String value) {
      return param("logical_date", value);
    }

    /**
     * The logical date (previously called execution date). This is the time or interval
     * covered by this Dag run, according to the Dag definition. This together with
     * trigger_dag_id are a unique key. This field is required if your environment is running
     * with Airflow 3. (templated) Templated.
     */
    public Builder logicalDate(TaskRef<?> value) {
      return param("logical_date", value);
    }

    /** The beginning of the interval the Dag run covers Templated. */
    public Builder dataIntervalStart(String value) {
      return param("data_interval_start", value);
    }

    /** The beginning of the interval the Dag run covers Templated. */
    public Builder dataIntervalStart(TaskRef<?> value) {
      return param("data_interval_start", value);
    }

    /** The end of the interval the Dag run covers Templated. */
    public Builder dataIntervalEnd(String value) {
      return param("data_interval_end", value);
    }

    /** The end of the interval the Dag run covers Templated. */
    public Builder dataIntervalEnd(TaskRef<?> value) {
      return param("data_interval_end", value);
    }

    /**
     * Additional configuration parameters. The value of this field can be set only when
     * creating the object. (templated) Templated.
     */
    public Builder conf(Map<String, Object> value) {
      return param("conf", value);
    }

    /**
     * Additional configuration parameters. The value of this field can be set only when
     * creating the object. (templated) Templated.
     */
    public Builder conf(TaskRef<?> value) {
      return param("conf", value);
    }

    /** Contains manually entered notes by the user about the DagRun. (templated) Templated. */
    public Builder note(String value) {
      return param("note", value);
    }

    /** Contains manually entered notes by the user about the DagRun. (templated) Templated. */
    public Builder note(TaskRef<?> value) {
      return param("note", value);
    }

    /**
     * The Airflow major version the MWAA environment runs. This parameter is only used if the
     * local web token method is used to call Airflow API. (templated) Templated.
     */
    public Builder airflowVersion(Object value) {
      return param("airflow_version", value);
    }

    /** Whether to wait for Dag run to stop. (default: False) Defaults to false. */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /** Time in seconds to wait between status checks. (default: 60) Defaults to 60. */
    public Builder waiterDelay(long value) {
      return param("waiter_delay", value);
    }

    /**
     * Maximum number of attempts to check for Dag run completion. (default: 20) Defaults to
     * 20.
     */
    public Builder waiterMaxAttempts(long value) {
      return param("waiter_max_attempts", value);
    }

    /**
     * If True, the operator will wait asynchronously for the Dag run to stop. This mode
     * requires aiobotocore module to be installed. (default: False) Deferring when
     * `wait_for_completion` is False is deprecated; from the next major release this has no
     * effect when `wait_for_completion` is False. Defaults to false.
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
