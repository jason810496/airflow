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

package org.apache.airflow.providers.amazon.aws.operators.appflow;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Execute an AppFlow run after updating the filters to select only a single day.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.appflow.AppflowRunDailyOperator} of
 * {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class AppflowRunDailyOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.appflow.AppflowRunDailyOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.appflow", "AppflowRunDailyOperator")
          .params(
              "source",
              "flow_name",
              "source_field",
              "filter_date",
              "poll_interval",
              "wait_for_completion",
              "max_attempts",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("source", "flow_name", "source_field", "filter_date")
          .templated(
              "aws_conn_id",
              "filter_date",
              "flow_name",
              "region_name",
              "source",
              "source_field",
              "verify")
          .uiColor("#2bccbd");

  private AppflowRunDailyOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code AppflowRunDailyOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The source name (Supported: salesforce) Templated. */
    public Builder source(String value) {
      return param("source", value);
    }

    /** The source name (Supported: salesforce) Templated. */
    public Builder source(TaskRef<?> value) {
      return param("source", value);
    }

    /** The flow name Templated. */
    public Builder flowName(String value) {
      return param("flow_name", value);
    }

    /** The flow name Templated. */
    public Builder flowName(TaskRef<?> value) {
      return param("flow_name", value);
    }

    /** The field name to apply filters Templated. */
    public Builder sourceField(String value) {
      return param("source_field", value);
    }

    /** The field name to apply filters Templated. */
    public Builder sourceField(TaskRef<?> value) {
      return param("source_field", value);
    }

    /** The date value (or template) to be used in filters. Templated. */
    public Builder filterDate(String value) {
      return param("filter_date", value);
    }

    /** The date value (or template) to be used in filters. Templated. */
    public Builder filterDate(TaskRef<?> value) {
      return param("filter_date", value);
    }

    /** how often in seconds to check the query status Defaults to 20. */
    public Builder pollInterval(long value) {
      return param("poll_interval", value);
    }

    /** whether to wait for the run to end to return Defaults to true. */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /** how many times to check for status before timing out Defaults to 60. */
    public Builder maxAttempts(long value) {
      return param("max_attempts", value);
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
