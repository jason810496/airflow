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

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Cancels an export task in progress that is exporting a snapshot to Amazon S3.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.rds.RdsCancelExportTaskOperator} of
 * {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class RdsCancelExportTaskOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.rds.RdsCancelExportTaskOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.rds", "RdsCancelExportTaskOperator")
          .params(
              "export_task_identifier",
              "wait_for_completion",
              "check_interval",
              "max_attempts",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("export_task_identifier")
          .templated("aws_conn_id", "export_task_identifier", "region_name", "verify")
          .uiColor("#eeaa88")
          .uiFgcolor("#ffffff");

  private RdsCancelExportTaskOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code RdsCancelExportTaskOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The identifier of the snapshot export task to cancel Templated. */
    public Builder exportTaskIdentifier(String value) {
      return param("export_task_identifier", value);
    }

    /** The identifier of the snapshot export task to cancel Templated. */
    public Builder exportTaskIdentifier(TaskRef<?> value) {
      return param("export_task_identifier", value);
    }

    /** If True, waits for DB snapshot export to cancel. (default: True) Defaults to true. */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /** The amount of time in seconds to wait between attempts Defaults to 30. */
    public Builder checkInterval(long value) {
      return param("check_interval", value);
    }

    /** The maximum number of attempts to be made Defaults to 40. */
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
