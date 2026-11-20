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

package org.apache.airflow.providers.amazon.aws.operators.ses;

import java.util.List;
import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Send an email using Amazon Simple Email Service (SES).
 *
 * Runs {@code airflow.providers.amazon.aws.operators.ses.SesEmailOperator} of {@code
 * apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a Python
 * worker serves.
 */
public final class SesEmailOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.ses.SesEmailOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.ses", "SesEmailOperator")
          .params(
              "mail_from",
              "to",
              "subject",
              "html_content",
              "files",
              "cc",
              "bcc",
              "mime_subtype",
              "mime_charset",
              "reply_to",
              "return_path",
              "custom_headers",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("mail_from", "to", "subject", "html_content")
          .templated(
              "aws_conn_id",
              "bcc",
              "cc",
              "custom_headers",
              "html_content",
              "mail_from",
              "mime_charset",
              "mime_subtype",
              "region_name",
              "reply_to",
              "return_path",
              "subject",
              "to",
              "verify")
          .templateFieldsRenderers(Map.of("custom_headers", "json"));

  private SesEmailOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code SesEmailOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** Email address to set as email's from (templated) Templated. */
    public Builder mailFrom(String value) {
      return param("mail_from", value);
    }

    /** Email address to set as email's from (templated) Templated. */
    public Builder mailFrom(TaskRef<?> value) {
      return param("mail_from", value);
    }

    /** List of email addresses to set as email's to (templated) Templated. */
    public Builder to(Object value) {
      return param("to", value);
    }

    /** Email's subject (templated) Templated. */
    public Builder subject(String value) {
      return param("subject", value);
    }

    /** Email's subject (templated) Templated. */
    public Builder subject(TaskRef<?> value) {
      return param("subject", value);
    }

    /** Content of email in HTML format (templated) Templated. */
    public Builder htmlContent(String value) {
      return param("html_content", value);
    }

    /** Content of email in HTML format (templated) Templated. */
    public Builder htmlContent(TaskRef<?> value) {
      return param("html_content", value);
    }

    /** List of paths of files to be attached */
    public Builder files(List<String> value) {
      return param("files", value);
    }

    /** List of email addresses to set as email's CC (templated) Templated. */
    public Builder cc(Object value) {
      return param("cc", value);
    }

    /** List of email addresses to set as email's BCC (templated) Templated. */
    public Builder bcc(Object value) {
      return param("bcc", value);
    }

    /**
     * Can be used to specify the subtype of the message. Default = mixed Defaults to "mixed".
     * Templated.
     */
    public Builder mimeSubtype(String value) {
      return param("mime_subtype", value);
    }

    /**
     * Can be used to specify the subtype of the message. Default = mixed Defaults to "mixed".
     * Templated.
     */
    public Builder mimeSubtype(TaskRef<?> value) {
      return param("mime_subtype", value);
    }

    /** Email's charset. Default = UTF-8 Defaults to "utf-8". Templated. */
    public Builder mimeCharset(String value) {
      return param("mime_charset", value);
    }

    /** Email's charset. Default = UTF-8 Defaults to "utf-8". Templated. */
    public Builder mimeCharset(TaskRef<?> value) {
      return param("mime_charset", value);
    }

    /** The email address to which replies will be sent Templated. */
    public Builder replyTo(String value) {
      return param("reply_to", value);
    }

    /** The email address to which replies will be sent Templated. */
    public Builder replyTo(TaskRef<?> value) {
      return param("reply_to", value);
    }

    /** The email address to which message bounces and complaints should be sent Templated. */
    public Builder returnPath(String value) {
      return param("return_path", value);
    }

    /** The email address to which message bounces and complaints should be sent Templated. */
    public Builder returnPath(TaskRef<?> value) {
      return param("return_path", value);
    }

    /** Additional headers to add to the MIME message Templated. */
    public Builder customHeaders(Map<String, Object> value) {
      return param("custom_headers", value);
    }

    /** Additional headers to add to the MIME message Templated. */
    public Builder customHeaders(TaskRef<?> value) {
      return param("custom_headers", value);
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
