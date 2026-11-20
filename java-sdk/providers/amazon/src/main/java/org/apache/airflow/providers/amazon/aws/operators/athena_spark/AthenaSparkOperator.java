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

package org.apache.airflow.providers.amazon.aws.operators.athena_spark;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Run an Apache Spark calculation in an Amazon Athena session.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.athena_spark.AthenaSparkOperator} of
 * {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class AthenaSparkOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.athena_spark.AthenaSparkOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.athena_spark", "AthenaSparkOperator")
          .params(
              "session_id",
              "code_block",
              "description",
              "client_request_token",
              "waiter_delay",
              "waiter_max_attempts",
              "log_query",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("session_id", "code_block")
          .templated(
              "aws_conn_id",
              "code_block",
              "description",
              "region_name",
              "session_id",
              "verify")
          .templateExt(".py")
          .templateFieldsRenderers(Map.of("code_block", "python"))
          .uiColor("#44b5e2");

  private AthenaSparkOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code AthenaSparkOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The Athena session ID in which to run the calculation. (templated) Templated. */
    public Builder sessionId(String value) {
      return param("session_id", value);
    }

    /** The Athena session ID in which to run the calculation. (templated) Templated. */
    public Builder sessionId(TaskRef<?> value) {
      return param("session_id", value);
    }

    /** The calculation code, such as PySpark, to execute. (templated) Templated. */
    public Builder codeBlock(String value) {
      return param("code_block", value);
    }

    /** The calculation code, such as PySpark, to execute. (templated) Templated. */
    public Builder codeBlock(TaskRef<?> value) {
      return param("code_block", value);
    }

    /** Optional description of the calculation. Defaults to None. Templated. */
    public Builder description(String value) {
      return param("description", value);
    }

    /** Optional description of the calculation. Defaults to None. Templated. */
    public Builder description(TaskRef<?> value) {
      return param("description", value);
    }

    /** Optional idempotency token for the submission. Defaults to None. */
    public Builder clientRequestToken(String value) {
      return param("client_request_token", value);
    }

    /** Seconds to wait between status checks. Defaults to 30. Defaults to 30. */
    public Builder waiterDelay(long value) {
      return param("waiter_delay", value);
    }

    /**
     * Maximum number of polling attempts before timing out. Defaults to 120. To limit total
     * task time, use execution_timeout on the task as well. Defaults to 120.
     */
    public Builder waiterMaxAttempts(long value) {
      return param("waiter_max_attempts", value);
    }

    /** Whether to log submission details. Defaults to True. Defaults to true. */
    public Builder logQuery(boolean value) {
      return param("log_query", value);
    }

    /**
     * The Airflow connection used for AWS credentials. Defaults to `aws_default`. Defaults to
     * "aws_default". Templated.
     */
    public Builder awsConnId(String value) {
      return param("aws_conn_id", value);
    }

    /**
     * The Airflow connection used for AWS credentials. Defaults to `aws_default`. Defaults to
     * "aws_default". Templated.
     */
    public Builder awsConnId(TaskRef<?> value) {
      return param("aws_conn_id", value);
    }

    /** AWS region. If not set, default boto3 behavior is used. Defaults to None. Templated. */
    public Builder regionName(String value) {
      return param("region_name", value);
    }

    /** AWS region. If not set, default boto3 behavior is used. Defaults to None. Templated. */
    public Builder regionName(TaskRef<?> value) {
      return param("region_name", value);
    }

    /** Whether to verify SSL certificates. Defaults to None. Templated. */
    public Builder verify(Object value) {
      return param("verify", value);
    }

    /** Optional botocore configuration dict. Defaults to None. */
    public Builder botocoreConfig(Map<String, Object> value) {
      return param("botocore_config", value);
    }

    public Builder region(String value) {
      return param("region", value);
    }
  }
}
