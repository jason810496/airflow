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

package org.apache.airflow.providers.amazon.aws.operators.rds;

import java.util.List;
import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Creates an RDS event notification subscription.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.rds.RdsCreateEventSubscriptionOperator}
 * of {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that
 * a Python worker serves.
 */
public final class RdsCreateEventSubscriptionOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.rds.RdsCreateEventSubscriptionOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.rds", "RdsCreateEventSubscriptionOperator")
          .params(
              "subscription_name",
              "sns_topic_arn",
              "source_type",
              "event_categories",
              "source_ids",
              "enabled",
              "tags",
              "wait_for_completion",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("subscription_name", "sns_topic_arn")
          .templated(
              "aws_conn_id",
              "event_categories",
              "region_name",
              "sns_topic_arn",
              "source_ids",
              "source_type",
              "subscription_name",
              "tags",
              "verify")
          .uiColor("#eeaa88")
          .uiFgcolor("#ffffff");

  private RdsCreateEventSubscriptionOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code RdsCreateEventSubscriptionOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The name of the subscription (must be less than 255 characters) Templated. */
    public Builder subscriptionName(String value) {
      return param("subscription_name", value);
    }

    /** The name of the subscription (must be less than 255 characters) Templated. */
    public Builder subscriptionName(TaskRef<?> value) {
      return param("subscription_name", value);
    }

    /** The ARN of the SNS topic created for event notification Templated. */
    public Builder snsTopicArn(String value) {
      return param("sns_topic_arn", value);
    }

    /** The ARN of the SNS topic created for event notification Templated. */
    public Builder snsTopicArn(TaskRef<?> value) {
      return param("sns_topic_arn", value);
    }

    /**
     * The type of source that is generating the events. Valid values: db-instance | db-cluster
     * | db-parameter-group | db-security-group | db-snapshot | db-cluster-snapshot | db-proxy
     * Defaults to "". Templated.
     */
    public Builder sourceType(String value) {
      return param("source_type", value);
    }

    /**
     * The type of source that is generating the events. Valid values: db-instance | db-cluster
     * | db-parameter-group | db-security-group | db-snapshot | db-cluster-snapshot | db-proxy
     * Defaults to "". Templated.
     */
    public Builder sourceType(TaskRef<?> value) {
      return param("source_type", value);
    }

    /**
     * A list of event categories for a source type that you want to subscribe to `USER Events
     * &lt;https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_Events.Messages.html&gt;`__
     * Templated.
     */
    public Builder eventCategories(List<String> value) {
      return param("event_categories", value);
    }

    /**
     * A list of event categories for a source type that you want to subscribe to `USER Events
     * &lt;https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_Events.Messages.html&gt;`__
     * Templated.
     */
    public Builder eventCategories(TaskRef<?> value) {
      return param("event_categories", value);
    }

    /** The list of identifiers of the event sources for which events are returned Templated. */
    public Builder sourceIds(List<String> value) {
      return param("source_ids", value);
    }

    /** The list of identifiers of the event sources for which events are returned Templated. */
    public Builder sourceIds(TaskRef<?> value) {
      return param("source_ids", value);
    }

    /**
     * A value that indicates whether to activate the subscription (default True)l Defaults to
     * true.
     */
    public Builder enabled(boolean value) {
      return param("enabled", value);
    }

    /**
     * A dictionary of tags or a list of tags in format `[{"Key": "...", "Value": "..."},]`
     * `USER Tagging
     * &lt;https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_Tagging.html&gt;`__
     * Templated.
     */
    public Builder tags(Object value) {
      return param("tags", value);
    }

    /**
     * If True, waits for creation of the subscription to complete. (default: True) Defaults to
     * true.
     */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
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
