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

package org.apache.airflow.providers.amazon.aws.operators.sns;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Publish a message to Amazon SNS.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.sns.SnsPublishOperator} of {@code
 * apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a Python
 * worker serves.
 */
public final class SnsPublishOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.sns.SnsPublishOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.sns", "SnsPublishOperator")
          .params(
              "target_arn",
              "message",
              "subject",
              "message_attributes",
              "message_deduplication_id",
              "message_group_id",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("target_arn", "message")
          .templated(
              "aws_conn_id",
              "message",
              "message_attributes",
              "message_deduplication_id",
              "message_group_id",
              "region_name",
              "subject",
              "target_arn",
              "verify")
          .templateFieldsRenderers(Map.of("message_attributes", "json"));

  private SnsPublishOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code SnsPublishOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** either a TopicArn or an EndpointArn Templated. */
    public Builder targetArn(String value) {
      return param("target_arn", value);
    }

    /** either a TopicArn or an EndpointArn Templated. */
    public Builder targetArn(TaskRef<?> value) {
      return param("target_arn", value);
    }

    /** the default message you want to send (templated) Templated. */
    public Builder message(String value) {
      return param("message", value);
    }

    /** the default message you want to send (templated) Templated. */
    public Builder message(TaskRef<?> value) {
      return param("message", value);
    }

    /** the message subject you want to send (templated) Templated. */
    public Builder subject(String value) {
      return param("subject", value);
    }

    /** the message subject you want to send (templated) Templated. */
    public Builder subject(TaskRef<?> value) {
      return param("subject", value);
    }

    /**
     * the message attributes you want to send as a flat dict (data type will be determined
     * automatically) Templated.
     */
    public Builder messageAttributes(Map<String, Object> value) {
      return param("message_attributes", value);
    }

    /**
     * the message attributes you want to send as a flat dict (data type will be determined
     * automatically) Templated.
     */
    public Builder messageAttributes(TaskRef<?> value) {
      return param("message_attributes", value);
    }

    /**
     * Every message must have a unique message_deduplication_id. This parameter applies only
     * to FIFO (first-in-first-out) topics. Templated.
     */
    public Builder messageDeduplicationId(String value) {
      return param("message_deduplication_id", value);
    }

    /**
     * Every message must have a unique message_deduplication_id. This parameter applies only
     * to FIFO (first-in-first-out) topics. Templated.
     */
    public Builder messageDeduplicationId(TaskRef<?> value) {
      return param("message_deduplication_id", value);
    }

    /**
     * Tag that specifies that a message belongs to a specific message group. This parameter
     * applies only to FIFO (first-in-first-out) topics. Templated.
     */
    public Builder messageGroupId(String value) {
      return param("message_group_id", value);
    }

    /**
     * Tag that specifies that a message belongs to a specific message group. This parameter
     * applies only to FIFO (first-in-first-out) topics. Templated.
     */
    public Builder messageGroupId(TaskRef<?> value) {
      return param("message_group_id", value);
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
