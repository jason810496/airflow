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

package org.apache.airflow.providers.amazon.aws.operators.dms;

import java.util.List;
import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Reload target tables for a running AWS DMS replication task.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.dms.DmsReloadTablesOperator} of {@code
 * apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a Python
 * worker serves.
 */
public final class DmsReloadTablesOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.dms.DmsReloadTablesOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.dms", "DmsReloadTablesOperator")
          .params(
              "replication_task_arn",
              "tables_to_reload",
              "reload_option",
              "wait_for_completion",
              "deferrable",
              "waiter_delay",
              "waiter_max_attempts",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("replication_task_arn", "tables_to_reload")
          .templated(
              "aws_conn_id",
              "region_name",
              "reload_option",
              "replication_task_arn",
              "tables_to_reload",
              "verify")
          .templateFieldsRenderers(Map.of("tables_to_reload", "json"));

  private DmsReloadTablesOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code DmsReloadTablesOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** Replication task ARN. (required) Templated. */
    public Builder replicationTaskArn(String value) {
      return param("replication_task_arn", value);
    }

    /** Replication task ARN. (required) Templated. */
    public Builder replicationTaskArn(TaskRef<?> value) {
      return param("replication_task_arn", value);
    }

    /**
     * Tables to reload. Each item must contain `SchemaName` and `TableName`. (required)
     * Templated.
     */
    public Builder tablesToReload(List<Map<String, String>> value) {
      return param("tables_to_reload", value);
    }

    /**
     * Tables to reload. Each item must contain `SchemaName` and `TableName`. (required)
     * Templated.
     */
    public Builder tablesToReload(TaskRef<?> value) {
      return param("tables_to_reload", value);
    }

    /**
     * Use `data-reload` to reload data and revalidate it when validation is enabled. Use
     * `validate-only` to revalidate without reloading data; this option only applies when
     * validation is enabled. Defaults to `data-reload`. Defaults to "data-reload". Templated.
     */
    public Builder reloadOption(String value) {
      return param("reload_option", value);
    }

    /**
     * Use `data-reload` to reload data and revalidate it when validation is enabled. Use
     * `validate-only` to revalidate without reloading data; this option only applies when
     * validation is enabled. Defaults to `data-reload`. Defaults to "data-reload". Templated.
     */
    public Builder reloadOption(TaskRef<?> value) {
      return param("reload_option", value);
    }

    /**
     * If True, wait until every data reload or validation completes. Defaults to True.
     * Defaults to true.
     */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /**
     * Run the operator in deferrable mode when waiting for completion. Defaults to the
     * `operators.default_deferrable` configuration. Defaults to false.
     */
    public Builder deferrable(boolean value) {
      return param("deferrable", value);
    }

    /** Seconds between table statistics polls (default: 30). Defaults to 30. */
    public Builder waiterDelay(long value) {
      return param("waiter_delay", value);
    }

    /** Maximum table statistics poll attempts per table (default: 60). Defaults to 60. */
    public Builder waiterMaxAttempts(long value) {
      return param("waiter_max_attempts", value);
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
