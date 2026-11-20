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

package org.apache.airflow.providers.amazon.aws.operators.athena;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * An operator that submits a Trino/Presto query to Amazon Athena.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.athena.AthenaOperator} of {@code
 * apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a Python
 * worker serves.
 */
public final class AthenaOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.athena.AthenaOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.athena", "AthenaOperator")
          .params(
              "query",
              "database",
              "output_location",
              "client_request_token",
              "workgroup",
              "query_execution_context",
              "result_configuration",
              "sleep_time",
              "max_polling_attempts",
              "log_query",
              "deferrable",
              "catalog",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("query")
          .templated(
              "aws_conn_id",
              "catalog",
              "database",
              "output_location",
              "query",
              "region_name",
              "verify",
              "workgroup")
          .templateExt(".sql")
          .templateFieldsRenderers(Map.of("query", "sql"))
          .uiColor("#44b5e2");

  private AthenaOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code AthenaOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** Trino/Presto query to be run on Amazon Athena. (templated) Templated. */
    public Builder query(String value) {
      return param("query", value);
    }

    /** Trino/Presto query to be run on Amazon Athena. (templated) Templated. */
    public Builder query(TaskRef<?> value) {
      return param("query", value);
    }

    /**
     * Default database for query execution. (templated) This argument is optional when the
     * query does not require a default database, such as when all referenced table names are
     * fully qualified. If omitted or set to `None`, any `Database` value set in the
     * `query_execution_context` will be used instead. Templated.
     */
    public Builder database(String value) {
      return param("database", value);
    }

    /**
     * Default database for query execution. (templated) This argument is optional when the
     * query does not require a default database, such as when all referenced table names are
     * fully qualified. If omitted or set to `None`, any `Database` value set in the
     * `query_execution_context` will be used instead. Templated.
     */
    public Builder database(TaskRef<?> value) {
      return param("database", value);
    }

    /**
     * s3 path to write the query results into. (templated) To run the query, you must specify
     * the query results location using one of the ways: either for individual queries using
     * either this setting (client-side), or in the workgroup, using WorkGroupConfiguration. If
     * none of them is set, Athena issues an error that no output location is provided
     * Templated.
     */
    public Builder outputLocation(String value) {
      return param("output_location", value);
    }

    /**
     * s3 path to write the query results into. (templated) To run the query, you must specify
     * the query results location using one of the ways: either for individual queries using
     * either this setting (client-side), or in the workgroup, using WorkGroupConfiguration. If
     * none of them is set, Athena issues an error that no output location is provided
     * Templated.
     */
    public Builder outputLocation(TaskRef<?> value) {
      return param("output_location", value);
    }

    /** Unique token created by user to avoid multiple executions of same query */
    public Builder clientRequestToken(String value) {
      return param("client_request_token", value);
    }

    /**
     * Athena workgroup in which query will be run. (templated) Defaults to "primary".
     * Templated.
     */
    public Builder workgroup(String value) {
      return param("workgroup", value);
    }

    /**
     * Athena workgroup in which query will be run. (templated) Defaults to "primary".
     * Templated.
     */
    public Builder workgroup(TaskRef<?> value) {
      return param("workgroup", value);
    }

    /** Context in which query need to be run */
    public Builder queryExecutionContext(Map<String, String> value) {
      return param("query_execution_context", value);
    }

    /** Dict with path to store results in and config related to encryption */
    public Builder resultConfiguration(Map<String, Object> value) {
      return param("result_configuration", value);
    }

    /**
     * Time (in seconds) to wait between two consecutive calls to check query status on Athena
     * Defaults to 30.
     */
    public Builder sleepTime(long value) {
      return param("sleep_time", value);
    }

    /**
     * Number of times to poll for query state before function exits To limit task execution
     * time, use execution_timeout.
     */
    public Builder maxPollingAttempts(long value) {
      return param("max_polling_attempts", value);
    }

    /**
     * Whether to log athena query and other execution params when it's executed. Defaults to
     * *True*. Defaults to true.
     */
    public Builder logQuery(boolean value) {
      return param("log_query", value);
    }

    /** Defaults to false. */
    public Builder deferrable(boolean value) {
      return param("deferrable", value);
    }

    /** Catalog to select. (templated) Defaults to "AwsDataCatalog". Templated. */
    public Builder catalog(String value) {
      return param("catalog", value);
    }

    /** Catalog to select. (templated) Defaults to "AwsDataCatalog". Templated. */
    public Builder catalog(TaskRef<?> value) {
      return param("catalog", value);
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
