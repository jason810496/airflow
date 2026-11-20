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

package org.apache.airflow.providers.amazon.aws.operators.emr;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Operator to create Serverless EMR Application.
 *
 * Runs {@code
 * airflow.providers.amazon.aws.operators.emr.EmrServerlessCreateApplicationOperator} of {@code
 * apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a Python
 * worker serves.
 */
public final class EmrServerlessCreateApplicationOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.emr.EmrServerlessCreateApplicationOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.emr", "EmrServerlessCreateApplicationOperator")
          .params(
              "release_label",
              "job_type",
              "client_request_token",
              "config",
              "wait_for_completion",
              "waiter_max_attempts",
              "waiter_delay",
              "deferrable",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("release_label", "job_type")
          .templated("aws_conn_id", "region_name", "verify");

  private EmrServerlessCreateApplicationOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code EmrServerlessCreateApplicationOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The EMR release version associated with the application. */
    public Builder releaseLabel(String value) {
      return param("release_label", value);
    }

    /** The type of application you want to start, such as Spark or Hive. */
    public Builder jobType(String value) {
      return param("job_type", value);
    }

    /**
     * The client idempotency token of the application to create. Its value must be unique for
     * each request. Defaults to "".
     */
    public Builder clientRequestToken(String value) {
      return param("client_request_token", value);
    }

    /** Optional dictionary for arbitrary parameters to the boto API create_application call. */
    public Builder config(Map<String, Object> value) {
      return param("config", value);
    }

    /**
     * If true, wait for the Application to start before returning. Defaults to True. If set to
     * False, `waiter_max_attempts` and `waiter_delay` will only be applied when waiting for
     * the application to be in the `CREATED` state. Defaults to true.
     */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /**
     * Number of times the waiter should poll the application to check the state. Defaults to
     * 25 if not set.
     */
    public Builder waiterMaxAttempts(long value) {
      return param("waiter_max_attempts", value);
    }

    /**
     * Number of seconds between polling the state of the application. Defaults to 60 seconds
     * if not set.
     */
    public Builder waiterDelay(long value) {
      return param("waiter_delay", value);
    }

    /**
     * If True, the operator will wait asynchronously for application to be created. Deferring
     * when `wait_for_completion` is False is deprecated; from the next major release this has
     * no effect when `wait_for_completion` is False. This mode requires aiobotocore module to
     * be installed. (default: False, but can be overridden in config file by setting
     * default_deferrable to True) Defaults to false.
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
