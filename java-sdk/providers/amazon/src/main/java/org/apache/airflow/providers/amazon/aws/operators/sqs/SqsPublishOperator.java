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

package org.apache.airflow.providers.amazon.aws.operators.sqs;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Publish a message to an Amazon SQS queue.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.sqs.SqsPublishOperator} of {@code
 * apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a Python
 * worker serves.
 */
public final class SqsPublishOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.sqs.SqsPublishOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.sqs", "SqsPublishOperator")
          .params(
              "sqs_queue",
              "message_content",
              "message_attributes",
              "delay_seconds",
              "message_group_id",
              "message_deduplication_id",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("sqs_queue", "message_content")
          .templated(
              "aws_conn_id",
              "delay_seconds",
              "message_attributes",
              "message_content",
              "message_deduplication_id",
              "message_group_id",
              "region_name",
              "sqs_queue",
              "verify")
          .templateFieldsRenderers(Map.of("message_attributes", "json"))
          .uiColor("#6ad3fa");

  private SqsPublishOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code SqsPublishOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The SQS queue url (templated) Templated. */
    public Builder sqsQueue(String value) {
      return param("sqs_queue", value);
    }

    /** The SQS queue url (templated) Templated. */
    public Builder sqsQueue(TaskRef<?> value) {
      return param("sqs_queue", value);
    }

    /** The message content (templated) Templated. */
    public Builder messageContent(String value) {
      return param("message_content", value);
    }

    /** The message content (templated) Templated. */
    public Builder messageContent(TaskRef<?> value) {
      return param("message_content", value);
    }

    /**
     * additional attributes for the message (default: None) For details of the attributes
     * parameter see botocore.client.SQS.send_message Templated.
     */
    public Builder messageAttributes(Map<String, Object> value) {
      return param("message_attributes", value);
    }

    /**
     * additional attributes for the message (default: None) For details of the attributes
     * parameter see botocore.client.SQS.send_message Templated.
     */
    public Builder messageAttributes(TaskRef<?> value) {
      return param("message_attributes", value);
    }

    /** message delay (templated) (default: 0 second) Defaults to 0. Templated. */
    public Builder delaySeconds(long value) {
      return param("delay_seconds", value);
    }

    /** message delay (templated) (default: 0 second) Defaults to 0. Templated. */
    public Builder delaySeconds(TaskRef<?> value) {
      return param("delay_seconds", value);
    }

    /**
     * This parameter applies only to FIFO (first-in-first-out) queues. (default: None) For
     * details of the attributes parameter see botocore.client.SQS.send_message Templated.
     */
    public Builder messageGroupId(String value) {
      return param("message_group_id", value);
    }

    /**
     * This parameter applies only to FIFO (first-in-first-out) queues. (default: None) For
     * details of the attributes parameter see botocore.client.SQS.send_message Templated.
     */
    public Builder messageGroupId(TaskRef<?> value) {
      return param("message_group_id", value);
    }

    /**
     * This applies only to FIFO (first-in-first-out) queues. For details of the attributes
     * parameter see botocore.client.SQS.send_message Templated.
     */
    public Builder messageDeduplicationId(String value) {
      return param("message_deduplication_id", value);
    }

    /**
     * This applies only to FIFO (first-in-first-out) queues. For details of the attributes
     * parameter see botocore.client.SQS.send_message Templated.
     */
    public Builder messageDeduplicationId(TaskRef<?> value) {
      return param("message_deduplication_id", value);
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
