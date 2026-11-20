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

package org.apache.airflow.providers.amazon.aws.sensors.glue_catalog_partition;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Waits for a partition to show up in AWS Glue Catalog.
 *
 * Runs {@code
 * airflow.providers.amazon.aws.sensors.glue_catalog_partition.GlueCatalogPartitionSensor} of
 * {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class GlueCatalogPartitionSensor {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.sensors.glue_catalog_partition.GlueCatalogPartitionSensor";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.sensors.glue_catalog_partition", "GlueCatalogPartitionSensor")
          .params(
              "table_name",
              "expression",
              "database_name",
              "poke_interval",
              "deferrable",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region",
              "timeout",
              "soft_fail",
              "mode",
              "exponential_backoff",
              "max_wait",
              "silent_fail",
              "never_fail")
          .required("table_name")
          .templated(
              "aws_conn_id",
              "database_name",
              "expression",
              "region_name",
              "table_name",
              "verify")
          .uiColor("#C5CAE9")
          .sensor();

  private GlueCatalogPartitionSensor() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code GlueCatalogPartitionSensor}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /**
     * The name of the table to wait for, supports the dot notation (my_database.my_table)
     * Templated.
     */
    public Builder tableName(String value) {
      return param("table_name", value);
    }

    /**
     * The name of the table to wait for, supports the dot notation (my_database.my_table)
     * Templated.
     */
    public Builder tableName(TaskRef<?> value) {
      return param("table_name", value);
    }

    /**
     * The partition clause to wait for. This is passed as is to the AWS Glue Catalog API's
     * get_partitions function, and supports SQL like notation as in `ds='2015-01-01' AND
     * type='value'` and comparison operators as in `"ds&gt;=2015-01-01"`. See
     * https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html
     * #aws-glue-api-catalog-partitions-GetPartitions Defaults to "ds='{{ ds }}'". Templated.
     */
    public Builder expression(String value) {
      return param("expression", value);
    }

    /**
     * The partition clause to wait for. This is passed as is to the AWS Glue Catalog API's
     * get_partitions function, and supports SQL like notation as in `ds='2015-01-01' AND
     * type='value'` and comparison operators as in `"ds&gt;=2015-01-01"`. See
     * https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html
     * #aws-glue-api-catalog-partitions-GetPartitions Defaults to "ds='{{ ds }}'". Templated.
     */
    public Builder expression(TaskRef<?> value) {
      return param("expression", value);
    }

    /**
     * The name of the catalog database where the partitions reside. Defaults to "default".
     * Templated.
     */
    public Builder databaseName(String value) {
      return param("database_name", value);
    }

    /**
     * The name of the catalog database where the partitions reside. Defaults to "default".
     * Templated.
     */
    public Builder databaseName(TaskRef<?> value) {
      return param("database_name", value);
    }

    /** Time in seconds that the job should wait in between each tries Defaults to 180. */
    public Builder pokeInterval(long value) {
      return param("poke_interval", value);
    }

    /**
     * If true, then the sensor will wait asynchronously for the partition to show up in the
     * AWS Glue Catalog. (default: False, but can be overridden in config file by setting
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

    /**
     * Time elapsed before the task times out and fails. Can be `timedelta` or `float` seconds.
     * This should not be confused with `execution_timeout` of the `BaseOperator` class.
     * `timeout` measures the time elapsed between the first poke and the current time (taking
     * into account any reschedule delay between each poke), while `execution_timeout` checks
     * the **running** time of the task (leaving out any reschedule delay). In case that the
     * `mode` is `poke` (see below), both of them are equivalent (as the sensor is never
     * rescheduled), which is not the case in `reschedule` mode.
     */
    public Builder timeout(double value) {
      return param("timeout", value);
    }

    /**
     * Set to true to mark the task as SKIPPED on failure. Mutually exclusive with never_fail.
     * Defaults to false.
     */
    public Builder softFail(boolean value) {
      return param("soft_fail", value);
    }

    /**
     * How the sensor operates. Options are: `{ poke | reschedule }`, default is `poke`. When
     * set to `poke` the sensor is taking up a worker slot for its whole execution time and
     * sleeps between pokes. Use this mode if the expected runtime of the sensor is short or if
     * a short poke interval is required. Note that the sensor will hold onto a worker slot and
     * a pool slot for the duration of the sensor's runtime in this mode. When set to
     * `reschedule` the sensor task frees the worker slot when the criteria is not yet met and
     * it's rescheduled at a later time. Use this mode if the time before the criteria is met
     * is expected to be quite long. The poke interval should be more than one minute to
     * prevent too much load on the scheduler. Defaults to "poke".
     */
    public Builder mode(String value) {
      return param("mode", value);
    }

    /**
     * allow progressive longer waits between pokes by using exponential backoff algorithm
     * Defaults to false.
     */
    public Builder exponentialBackoff(boolean value) {
      return param("exponential_backoff", value);
    }

    /** maximum wait interval between pokes, can be `timedelta` or `float` seconds */
    public Builder maxWait(double value) {
      return param("max_wait", value);
    }

    /**
     * If true, and poke method raises an exception different from AirflowSensorTimeout,
     * AirflowTaskTimeout, AirflowSkipException and AirflowFailException, the sensor will log
     * the error and continue its execution. Otherwise, the sensor task fails, and it can be
     * retried based on the provided `retries` parameter. Defaults to false.
     */
    public Builder silentFail(boolean value) {
      return param("silent_fail", value);
    }

    /**
     * If true, and poke method raises an exception, sensor will be skipped. Mutually exclusive
     * with soft_fail. Defaults to false.
     */
    public Builder neverFail(boolean value) {
      return param("never_fail", value);
    }
  }
}
