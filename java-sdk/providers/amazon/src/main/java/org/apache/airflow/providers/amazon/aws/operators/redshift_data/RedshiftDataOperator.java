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

package org.apache.airflow.providers.amazon.aws.operators.redshift_data;

import java.util.List;
import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Executes SQL Statements against an Amazon Redshift cluster using Redshift Data.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.redshift_data.RedshiftDataOperator} of
 * {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class RedshiftDataOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.redshift_data.RedshiftDataOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.redshift_data", "RedshiftDataOperator")
          .params(
              "sql",
              "database",
              "cluster_identifier",
              "db_user",
              "parameters",
              "secret_arn",
              "statement_name",
              "with_event",
              "wait_for_completion",
              "poll_interval",
              "return_sql_result",
              "workgroup_name",
              "deferrable",
              "session_id",
              "session_keep_alive_seconds",
              "cancel_on_kill",
              "durable",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("sql")
          .templated(
              "aws_conn_id",
              "cluster_identifier",
              "database",
              "db_user",
              "parameters",
              "region_name",
              "session_id",
              "sql",
              "statement_name",
              "verify",
              "workgroup_name")
          .templateExt(".sql")
          .templateFieldsRenderers(Map.of("sql", "sql"));

  private RedshiftDataOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code RedshiftDataOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** the SQL statement or list of SQL statement to run Templated. */
    public Builder sql(Object value) {
      return param("sql", value);
    }

    /** the name of the database Templated. */
    public Builder database(String value) {
      return param("database", value);
    }

    /** the name of the database Templated. */
    public Builder database(TaskRef<?> value) {
      return param("database", value);
    }

    /** unique identifier of a cluster Templated. */
    public Builder clusterIdentifier(String value) {
      return param("cluster_identifier", value);
    }

    /** unique identifier of a cluster Templated. */
    public Builder clusterIdentifier(TaskRef<?> value) {
      return param("cluster_identifier", value);
    }

    /** the database username Templated. */
    public Builder dbUser(String value) {
      return param("db_user", value);
    }

    /** the database username Templated. */
    public Builder dbUser(TaskRef<?> value) {
      return param("db_user", value);
    }

    /** the parameters for the SQL statement Templated. */
    public Builder parameters(List<Object> value) {
      return param("parameters", value);
    }

    /** the parameters for the SQL statement Templated. */
    public Builder parameters(TaskRef<?> value) {
      return param("parameters", value);
    }

    /** the name or ARN of the secret that enables db access */
    public Builder secretArn(String value) {
      return param("secret_arn", value);
    }

    /** the name of the SQL statement Templated. */
    public Builder statementName(String value) {
      return param("statement_name", value);
    }

    /** the name of the SQL statement Templated. */
    public Builder statementName(TaskRef<?> value) {
      return param("statement_name", value);
    }

    /** indicates whether to send an event to EventBridge Defaults to false. */
    public Builder withEvent(boolean value) {
      return param("with_event", value);
    }

    /**
     * indicates whether to wait for a result, if True wait, if False don't wait Defaults to
     * true.
     */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /** how often in seconds to check the query status Defaults to 10. */
    public Builder pollInterval(long value) {
      return param("poll_interval", value);
    }

    /**
     * if True will return the result of an SQL statement, if False (default) will return
     * statement ID Defaults to false.
     */
    public Builder returnSqlResult(boolean value) {
      return param("return_sql_result", value);
    }

    /**
     * name of the Redshift Serverless workgroup. Mutually exclusive with `cluster_identifier`.
     * Specify this parameter to query Redshift Serverless. More info
     * https://docs.aws.amazon.com/redshift/latest/mgmt/working-with-serverless.html Templated.
     */
    public Builder workgroupName(String value) {
      return param("workgroup_name", value);
    }

    /**
     * name of the Redshift Serverless workgroup. Mutually exclusive with `cluster_identifier`.
     * Specify this parameter to query Redshift Serverless. More info
     * https://docs.aws.amazon.com/redshift/latest/mgmt/working-with-serverless.html Templated.
     */
    public Builder workgroupName(TaskRef<?> value) {
      return param("workgroup_name", value);
    }

    /** Defaults to false. */
    public Builder deferrable(boolean value) {
      return param("deferrable", value);
    }

    /** the session identifier of the query Templated. */
    public Builder sessionId(String value) {
      return param("session_id", value);
    }

    /** the session identifier of the query Templated. */
    public Builder sessionId(TaskRef<?> value) {
      return param("session_id", value);
    }

    /**
     * duration in seconds to keep the session alive after the query finishes. The maximum time
     * a session can keep alive is 24 hours
     */
    public Builder sessionKeepAliveSeconds(long value) {
      return param("session_keep_alive_seconds", value);
    }

    /**
     * If True (default), cancel the running Redshift statement when the task is killed. This
     * applies both while the operator is running and, for a deferred task, while it waits in
     * the triggerer. Defaults to true.
     */
    public Builder cancelOnKill(boolean value) {
      return param("cancel_on_kill", value);
    }

    /**
     * When `True` (the default), the Redshift statement id is persisted to task state before
     * polling begins. A worker crash on retry reconnects to the existing statement instead of
     * resubmitting the SQL. Set to `False` to always submit fresh SQL on retry. Requires
     * Airflow 3.3+; ignored silently on earlier versions.
     */
    public Builder durable(boolean value) {
      return param("durable", value);
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
