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

package org.apache.airflow.providers.amazon.aws.operators.redshift_cluster;

import java.util.List;
import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Creates a new cluster with the specified parameters.
 *
 * Runs {@code
 * airflow.providers.amazon.aws.operators.redshift_cluster.RedshiftCreateClusterOperator} of
 * {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class RedshiftCreateClusterOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.redshift_cluster.RedshiftCreateClusterOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.redshift_cluster", "RedshiftCreateClusterOperator")
          .params(
              "cluster_identifier",
              "node_type",
              "master_username",
              "master_user_password",
              "cluster_type",
              "db_name",
              "number_of_nodes",
              "cluster_security_groups",
              "vpc_security_group_ids",
              "cluster_subnet_group_name",
              "availability_zone",
              "preferred_maintenance_window",
              "cluster_parameter_group_name",
              "automated_snapshot_retention_period",
              "manual_snapshot_retention_period",
              "port",
              "cluster_version",
              "allow_version_upgrade",
              "publicly_accessible",
              "encrypted",
              "hsm_client_certificate_identifier",
              "hsm_configuration_identifier",
              "elastic_ip",
              "tags",
              "kms_key_id",
              "enhanced_vpc_routing",
              "additional_info",
              "iam_roles",
              "maintenance_track_name",
              "snapshot_schedule_identifier",
              "availability_zone_relocation",
              "aqua_configuration_status",
              "default_iam_role_arn",
              "wait_for_completion",
              "max_attempt",
              "poll_interval",
              "deferrable",
              "delete_cluster_on_failure",
              "cleanup_timeout_seconds",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("cluster_identifier", "node_type", "master_username", "master_user_password")
          .templated(
              "additional_info",
              "allow_version_upgrade",
              "aqua_configuration_status",
              "automated_snapshot_retention_period",
              "availability_zone",
              "availability_zone_relocation",
              "aws_conn_id",
              "cluster_identifier",
              "cluster_parameter_group_name",
              "cluster_security_groups",
              "cluster_subnet_group_name",
              "cluster_type",
              "cluster_version",
              "db_name",
              "default_iam_role_arn",
              "elastic_ip",
              "encrypted",
              "enhanced_vpc_routing",
              "hsm_client_certificate_identifier",
              "hsm_configuration_identifier",
              "iam_roles",
              "kms_key_id",
              "maintenance_track_name",
              "manual_snapshot_retention_period",
              "master_user_password",
              "master_username",
              "node_type",
              "number_of_nodes",
              "port",
              "preferred_maintenance_window",
              "publicly_accessible",
              "region_name",
              "snapshot_schedule_identifier",
              "tags",
              "verify",
              "vpc_security_group_ids")
          .uiColor("#eeaa11")
          .uiFgcolor("#ffffff");

  private RedshiftCreateClusterOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code RedshiftCreateClusterOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** A unique identifier for the cluster. Templated. */
    public Builder clusterIdentifier(String value) {
      return param("cluster_identifier", value);
    }

    /** A unique identifier for the cluster. Templated. */
    public Builder clusterIdentifier(TaskRef<?> value) {
      return param("cluster_identifier", value);
    }

    /**
     * The node type to be provisioned for the cluster. Refer
     * https://docs.aws.amazon.com/redshift/latest/mgmt/working-with-clusters.html#rs-node-type-info
     * for the list of available node types. Templated.
     */
    public Builder nodeType(String value) {
      return param("node_type", value);
    }

    /**
     * The node type to be provisioned for the cluster. Refer
     * https://docs.aws.amazon.com/redshift/latest/mgmt/working-with-clusters.html#rs-node-type-info
     * for the list of available node types. Templated.
     */
    public Builder nodeType(TaskRef<?> value) {
      return param("node_type", value);
    }

    /**
     * The username associated with the admin user account for the cluster that is being
     * created. Templated.
     */
    public Builder masterUsername(String value) {
      return param("master_username", value);
    }

    /**
     * The username associated with the admin user account for the cluster that is being
     * created. Templated.
     */
    public Builder masterUsername(TaskRef<?> value) {
      return param("master_username", value);
    }

    /**
     * The password associated with the admin user account for the cluster that is being
     * created. Templated.
     */
    public Builder masterUserPassword(String value) {
      return param("master_user_password", value);
    }

    /**
     * The password associated with the admin user account for the cluster that is being
     * created. Templated.
     */
    public Builder masterUserPassword(TaskRef<?> value) {
      return param("master_user_password", value);
    }

    /**
     * The type of the cluster `single-node` or `multi-node`. The default value is
     * `multi-node`. Defaults to "multi-node". Templated.
     */
    public Builder clusterType(String value) {
      return param("cluster_type", value);
    }

    /**
     * The type of the cluster `single-node` or `multi-node`. The default value is
     * `multi-node`. Defaults to "multi-node". Templated.
     */
    public Builder clusterType(TaskRef<?> value) {
      return param("cluster_type", value);
    }

    /**
     * The name of the first database to be created when the cluster is created. Defaults to
     * "dev". Templated.
     */
    public Builder dbName(String value) {
      return param("db_name", value);
    }

    /**
     * The name of the first database to be created when the cluster is created. Defaults to
     * "dev". Templated.
     */
    public Builder dbName(TaskRef<?> value) {
      return param("db_name", value);
    }

    /**
     * The number of compute nodes in the cluster. This param require when `cluster_type` is
     * `multi-node`. Defaults to 1. Templated.
     */
    public Builder numberOfNodes(long value) {
      return param("number_of_nodes", value);
    }

    /**
     * The number of compute nodes in the cluster. This param require when `cluster_type` is
     * `multi-node`. Defaults to 1. Templated.
     */
    public Builder numberOfNodes(TaskRef<?> value) {
      return param("number_of_nodes", value);
    }

    /** A list of security groups to be associated with this cluster. Templated. */
    public Builder clusterSecurityGroups(List<String> value) {
      return param("cluster_security_groups", value);
    }

    /** A list of security groups to be associated with this cluster. Templated. */
    public Builder clusterSecurityGroups(TaskRef<?> value) {
      return param("cluster_security_groups", value);
    }

    /** A list of VPC security groups to be associated with the cluster. Templated. */
    public Builder vpcSecurityGroupIds(List<String> value) {
      return param("vpc_security_group_ids", value);
    }

    /** A list of VPC security groups to be associated with the cluster. Templated. */
    public Builder vpcSecurityGroupIds(TaskRef<?> value) {
      return param("vpc_security_group_ids", value);
    }

    /** The name of a cluster subnet group to be associated with this cluster. Templated. */
    public Builder clusterSubnetGroupName(String value) {
      return param("cluster_subnet_group_name", value);
    }

    /** The name of a cluster subnet group to be associated with this cluster. Templated. */
    public Builder clusterSubnetGroupName(TaskRef<?> value) {
      return param("cluster_subnet_group_name", value);
    }

    /** The EC2 Availability Zone (AZ). Templated. */
    public Builder availabilityZone(String value) {
      return param("availability_zone", value);
    }

    /** The EC2 Availability Zone (AZ). Templated. */
    public Builder availabilityZone(TaskRef<?> value) {
      return param("availability_zone", value);
    }

    /** The time range (in UTC) during which automated cluster maintenance can occur. Templated. */
    public Builder preferredMaintenanceWindow(String value) {
      return param("preferred_maintenance_window", value);
    }

    /** The time range (in UTC) during which automated cluster maintenance can occur. Templated. */
    public Builder preferredMaintenanceWindow(TaskRef<?> value) {
      return param("preferred_maintenance_window", value);
    }

    /** The name of the parameter group to be associated with this cluster. Templated. */
    public Builder clusterParameterGroupName(String value) {
      return param("cluster_parameter_group_name", value);
    }

    /** The name of the parameter group to be associated with this cluster. Templated. */
    public Builder clusterParameterGroupName(TaskRef<?> value) {
      return param("cluster_parameter_group_name", value);
    }

    /**
     * The number of days that automated snapshots are retained. The default value is `1`.
     * Defaults to 1. Templated.
     */
    public Builder automatedSnapshotRetentionPeriod(long value) {
      return param("automated_snapshot_retention_period", value);
    }

    /**
     * The number of days that automated snapshots are retained. The default value is `1`.
     * Defaults to 1. Templated.
     */
    public Builder automatedSnapshotRetentionPeriod(TaskRef<?> value) {
      return param("automated_snapshot_retention_period", value);
    }

    /** The default number of days to retain a manual snapshot. Templated. */
    public Builder manualSnapshotRetentionPeriod(long value) {
      return param("manual_snapshot_retention_period", value);
    }

    /** The default number of days to retain a manual snapshot. Templated. */
    public Builder manualSnapshotRetentionPeriod(TaskRef<?> value) {
      return param("manual_snapshot_retention_period", value);
    }

    /**
     * The port number on which the cluster accepts incoming connections. The Default value is
     * `5439`. Defaults to 5439. Templated.
     */
    public Builder port(long value) {
      return param("port", value);
    }

    /**
     * The port number on which the cluster accepts incoming connections. The Default value is
     * `5439`. Defaults to 5439. Templated.
     */
    public Builder port(TaskRef<?> value) {
      return param("port", value);
    }

    /**
     * The version of a Redshift engine software that you want to deploy on the cluster.
     * Defaults to "1.0". Templated.
     */
    public Builder clusterVersion(String value) {
      return param("cluster_version", value);
    }

    /**
     * The version of a Redshift engine software that you want to deploy on the cluster.
     * Defaults to "1.0". Templated.
     */
    public Builder clusterVersion(TaskRef<?> value) {
      return param("cluster_version", value);
    }

    /**
     * Whether major version upgrades can be applied during the maintenance window. The Default
     * value is `True`. Defaults to true. Templated.
     */
    public Builder allowVersionUpgrade(boolean value) {
      return param("allow_version_upgrade", value);
    }

    /**
     * Whether major version upgrades can be applied during the maintenance window. The Default
     * value is `True`. Defaults to true. Templated.
     */
    public Builder allowVersionUpgrade(TaskRef<?> value) {
      return param("allow_version_upgrade", value);
    }

    /** Whether cluster can be accessed from a public network. Defaults to true. Templated. */
    public Builder publiclyAccessible(boolean value) {
      return param("publicly_accessible", value);
    }

    /** Whether cluster can be accessed from a public network. Defaults to true. Templated. */
    public Builder publiclyAccessible(TaskRef<?> value) {
      return param("publicly_accessible", value);
    }

    /**
     * Whether data in the cluster is encrypted at rest. The default value is `False`. Defaults
     * to false. Templated.
     */
    public Builder encrypted(boolean value) {
      return param("encrypted", value);
    }

    /**
     * Whether data in the cluster is encrypted at rest. The default value is `False`. Defaults
     * to false. Templated.
     */
    public Builder encrypted(TaskRef<?> value) {
      return param("encrypted", value);
    }

    /**
     * Name of the HSM client certificate the Amazon Redshift cluster uses to retrieve the
     * data. Templated.
     */
    public Builder hsmClientCertificateIdentifier(String value) {
      return param("hsm_client_certificate_identifier", value);
    }

    /**
     * Name of the HSM client certificate the Amazon Redshift cluster uses to retrieve the
     * data. Templated.
     */
    public Builder hsmClientCertificateIdentifier(TaskRef<?> value) {
      return param("hsm_client_certificate_identifier", value);
    }

    /** Name of the HSM configuration Templated. */
    public Builder hsmConfigurationIdentifier(String value) {
      return param("hsm_configuration_identifier", value);
    }

    /** Name of the HSM configuration Templated. */
    public Builder hsmConfigurationIdentifier(TaskRef<?> value) {
      return param("hsm_configuration_identifier", value);
    }

    /** The Elastic IP (EIP) address for the cluster. Templated. */
    public Builder elasticIp(String value) {
      return param("elastic_ip", value);
    }

    /** The Elastic IP (EIP) address for the cluster. Templated. */
    public Builder elasticIp(TaskRef<?> value) {
      return param("elastic_ip", value);
    }

    /** A list of tag instances Templated. */
    public Builder tags(List<Object> value) {
      return param("tags", value);
    }

    /** A list of tag instances Templated. */
    public Builder tags(TaskRef<?> value) {
      return param("tags", value);
    }

    /** KMS key id of encryption key. Templated. */
    public Builder kmsKeyId(String value) {
      return param("kms_key_id", value);
    }

    /** KMS key id of encryption key. Templated. */
    public Builder kmsKeyId(TaskRef<?> value) {
      return param("kms_key_id", value);
    }

    /**
     * Whether to create the cluster with enhanced VPC routing enabled Default value is
     * `False`. Defaults to false. Templated.
     */
    public Builder enhancedVpcRouting(boolean value) {
      return param("enhanced_vpc_routing", value);
    }

    /**
     * Whether to create the cluster with enhanced VPC routing enabled Default value is
     * `False`. Defaults to false. Templated.
     */
    public Builder enhancedVpcRouting(TaskRef<?> value) {
      return param("enhanced_vpc_routing", value);
    }

    /** Reserved Templated. */
    public Builder additionalInfo(String value) {
      return param("additional_info", value);
    }

    /** Reserved Templated. */
    public Builder additionalInfo(TaskRef<?> value) {
      return param("additional_info", value);
    }

    /**
     * A list of IAM roles that can be used by the cluster to access other AWS services.
     * Templated.
     */
    public Builder iamRoles(List<String> value) {
      return param("iam_roles", value);
    }

    /**
     * A list of IAM roles that can be used by the cluster to access other AWS services.
     * Templated.
     */
    public Builder iamRoles(TaskRef<?> value) {
      return param("iam_roles", value);
    }

    /** Name of the maintenance track for the cluster. Templated. */
    public Builder maintenanceTrackName(String value) {
      return param("maintenance_track_name", value);
    }

    /** Name of the maintenance track for the cluster. Templated. */
    public Builder maintenanceTrackName(TaskRef<?> value) {
      return param("maintenance_track_name", value);
    }

    /** A unique identifier for the snapshot schedule. Templated. */
    public Builder snapshotScheduleIdentifier(String value) {
      return param("snapshot_schedule_identifier", value);
    }

    /** A unique identifier for the snapshot schedule. Templated. */
    public Builder snapshotScheduleIdentifier(TaskRef<?> value) {
      return param("snapshot_schedule_identifier", value);
    }

    /**
     * Enable relocation for a Redshift cluster between Availability Zones after the cluster is
     * created. Templated.
     */
    public Builder availabilityZoneRelocation(boolean value) {
      return param("availability_zone_relocation", value);
    }

    /**
     * Enable relocation for a Redshift cluster between Availability Zones after the cluster is
     * created. Templated.
     */
    public Builder availabilityZoneRelocation(TaskRef<?> value) {
      return param("availability_zone_relocation", value);
    }

    /** The cluster is configured to use AQUA . Templated. */
    public Builder aquaConfigurationStatus(String value) {
      return param("aqua_configuration_status", value);
    }

    /** The cluster is configured to use AQUA . Templated. */
    public Builder aquaConfigurationStatus(TaskRef<?> value) {
      return param("aqua_configuration_status", value);
    }

    /** ARN for the IAM role. Templated. */
    public Builder defaultIamRoleArn(String value) {
      return param("default_iam_role_arn", value);
    }

    /** ARN for the IAM role. Templated. */
    public Builder defaultIamRoleArn(TaskRef<?> value) {
      return param("default_iam_role_arn", value);
    }

    /** Whether wait for the cluster to be in `available` state Defaults to false. */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /** The maximum number of attempts to be made. Default: 5 Defaults to 5. */
    public Builder maxAttempt(long value) {
      return param("max_attempt", value);
    }

    /** The amount of time in seconds to wait between attempts. Default: 60 Defaults to 60. */
    public Builder pollInterval(long value) {
      return param("poll_interval", value);
    }

    /**
     * If True, the operator will run in deferrable mode. Deferring when `wait_for_completion`
     * is False is deprecated; from the next major release this has no effect when
     * `wait_for_completion` is False. Defaults to false.
     */
    public Builder deferrable(boolean value) {
      return param("deferrable", value);
    }

    /**
     * If True, best-effort deletion of the redshift cluster will be attempted after
     * post-creation failure. Default: True. Defaults to true.
     */
    public Builder deleteClusterOnFailure(boolean value) {
      return param("delete_cluster_on_failure", value);
    }

    /**
     * Maximum time in seconds to attempt best-effort deletion of the cluster when
     * post-creation failure occurs. Default: 300 seconds. Defaults to 300.
     */
    public Builder cleanupTimeoutSeconds(long value) {
      return param("cleanup_timeout_seconds", value);
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
