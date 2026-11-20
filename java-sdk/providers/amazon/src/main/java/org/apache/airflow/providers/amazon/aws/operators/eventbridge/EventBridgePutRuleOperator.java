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

package org.apache.airflow.providers.amazon.aws.operators.eventbridge;

import java.util.List;
import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Create or update a specified EventBridge rule.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.eventbridge.EventBridgePutRuleOperator}
 * of {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that
 * a Python worker serves.
 */
public final class EventBridgePutRuleOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.eventbridge.EventBridgePutRuleOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.eventbridge", "EventBridgePutRuleOperator")
          .params(
              "name",
              "description",
              "event_bus_name",
              "event_pattern",
              "role_arn",
              "schedule_expression",
              "state",
              "tags",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("name")
          .templated(
              "aws_conn_id",
              "description",
              "event_bus_name",
              "event_pattern",
              "name",
              "region_name",
              "role_arn",
              "schedule_expression",
              "state",
              "tags",
              "verify");

  private EventBridgePutRuleOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code EventBridgePutRuleOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** name of the rule to create or update (required) Templated. */
    public Builder name(String value) {
      return param("name", value);
    }

    /** name of the rule to create or update (required) Templated. */
    public Builder name(TaskRef<?> value) {
      return param("name", value);
    }

    /** description of the rule Templated. */
    public Builder description(String value) {
      return param("description", value);
    }

    /** description of the rule Templated. */
    public Builder description(TaskRef<?> value) {
      return param("description", value);
    }

    /** name or ARN of the event bus to associate with this rule Templated. */
    public Builder eventBusName(String value) {
      return param("event_bus_name", value);
    }

    /** name or ARN of the event bus to associate with this rule Templated. */
    public Builder eventBusName(TaskRef<?> value) {
      return param("event_bus_name", value);
    }

    /** pattern of events to be matched to this rule Templated. */
    public Builder eventPattern(String value) {
      return param("event_pattern", value);
    }

    /** pattern of events to be matched to this rule Templated. */
    public Builder eventPattern(TaskRef<?> value) {
      return param("event_pattern", value);
    }

    /** the Amazon Resource Name of the IAM role associated with the rule Templated. */
    public Builder roleArn(String value) {
      return param("role_arn", value);
    }

    /** the Amazon Resource Name of the IAM role associated with the rule Templated. */
    public Builder roleArn(TaskRef<?> value) {
      return param("role_arn", value);
    }

    /** the scheduling expression (for example, a cron or rate expression) Templated. */
    public Builder scheduleExpression(String value) {
      return param("schedule_expression", value);
    }

    /** the scheduling expression (for example, a cron or rate expression) Templated. */
    public Builder scheduleExpression(TaskRef<?> value) {
      return param("schedule_expression", value);
    }

    /** indicates whether rule is set to be "ENABLED" or "DISABLED" Templated. */
    public Builder state(String value) {
      return param("state", value);
    }

    /** indicates whether rule is set to be "ENABLED" or "DISABLED" Templated. */
    public Builder state(TaskRef<?> value) {
      return param("state", value);
    }

    /** list of key-value pairs to associate with the rule Templated. */
    public Builder tags(List<Object> value) {
      return param("tags", value);
    }

    /** list of key-value pairs to associate with the rule Templated. */
    public Builder tags(TaskRef<?> value) {
      return param("tags", value);
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
     * https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.htmlt
     */
    public Builder botocoreConfig(Map<String, Object> value) {
      return param("botocore_config", value);
    }

    public Builder region(String value) {
      return param("region", value);
    }
  }
}
