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
 * Copies the specified DB instance or DB cluster snapshot.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.rds.RdsCopyDbSnapshotOperator} of {@code
 * apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a Python
 * worker serves.
 */
public final class RdsCopyDbSnapshotOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.rds.RdsCopyDbSnapshotOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.rds", "RdsCopyDbSnapshotOperator")
          .params(
              "db_type",
              "source_db_snapshot_identifier",
              "target_db_snapshot_identifier",
              "kms_key_id",
              "tags",
              "copy_tags",
              "pre_signed_url",
              "option_group_name",
              "target_custom_availability_zone",
              "source_region",
              "wait_for_completion",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("db_type", "source_db_snapshot_identifier", "target_db_snapshot_identifier")
          .templated(
              "aws_conn_id",
              "option_group_name",
              "pre_signed_url",
              "region_name",
              "source_db_snapshot_identifier",
              "tags",
              "target_db_snapshot_identifier",
              "verify")
          .uiColor("#eeaa88")
          .uiFgcolor("#ffffff");

  private RdsCopyDbSnapshotOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code RdsCopyDbSnapshotOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** Type of the DB - either "instance" or "cluster" */
    public Builder dbType(String value) {
      return param("db_type", value);
    }

    /** The identifier of the source snapshot Templated. */
    public Builder sourceDbSnapshotIdentifier(String value) {
      return param("source_db_snapshot_identifier", value);
    }

    /** The identifier of the source snapshot Templated. */
    public Builder sourceDbSnapshotIdentifier(TaskRef<?> value) {
      return param("source_db_snapshot_identifier", value);
    }

    /** The identifier of the target snapshot Templated. */
    public Builder targetDbSnapshotIdentifier(String value) {
      return param("target_db_snapshot_identifier", value);
    }

    /** The identifier of the target snapshot Templated. */
    public Builder targetDbSnapshotIdentifier(TaskRef<?> value) {
      return param("target_db_snapshot_identifier", value);
    }

    /** The AWS KMS key identifier for an encrypted DB snapshot Defaults to "". */
    public Builder kmsKeyId(String value) {
      return param("kms_key_id", value);
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
     * Whether to copy all tags from the source snapshot to the target snapshot (default False)
     * Defaults to false.
     */
    public Builder copyTags(boolean value) {
      return param("copy_tags", value);
    }

    /** The URL that contains a Signature Version 4 signed request Defaults to "". Templated. */
    public Builder preSignedUrl(String value) {
      return param("pre_signed_url", value);
    }

    /** The URL that contains a Signature Version 4 signed request Defaults to "". Templated. */
    public Builder preSignedUrl(TaskRef<?> value) {
      return param("pre_signed_url", value);
    }

    /**
     * The name of an option group to associate with the copy of the snapshot Only when
     * db_type='instance' Defaults to "". Templated.
     */
    public Builder optionGroupName(String value) {
      return param("option_group_name", value);
    }

    /**
     * The name of an option group to associate with the copy of the snapshot Only when
     * db_type='instance' Defaults to "". Templated.
     */
    public Builder optionGroupName(TaskRef<?> value) {
      return param("option_group_name", value);
    }

    /**
     * The external custom Availability Zone identifier for the target Only when
     * db_type='instance' Defaults to "".
     */
    public Builder targetCustomAvailabilityZone(String value) {
      return param("target_custom_availability_zone", value);
    }

    /** The ID of the region that contains the snapshot to be copied Defaults to "". */
    public Builder sourceRegion(String value) {
      return param("source_region", value);
    }

    /** If True, waits for snapshot copy to complete. (default: True) Defaults to true. */
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
