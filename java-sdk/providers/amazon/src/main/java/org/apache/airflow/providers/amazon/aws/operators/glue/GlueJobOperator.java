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

package org.apache.airflow.providers.amazon.aws.operators.glue;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Create an AWS Glue Job.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.glue.GlueJobOperator} of {@code
 * apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a Python
 * worker serves.
 */
public final class GlueJobOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.glue.GlueJobOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.glue", "GlueJobOperator")
          .params(
              "job_name",
              "job_desc",
              "script_location",
              "concurrent_run_limit",
              "script_args",
              "retry_limit",
              "num_of_dpus",
              "s3_bucket",
              "iam_role_name",
              "iam_role_arn",
              "create_job_kwargs",
              "run_job_kwargs",
              "wait_for_completion",
              "deferrable",
              "verbose",
              "replace_script_file",
              "update_config",
              "stop_job_run_on_kill",
              "sleep_before_return",
              "job_poll_interval",
              "waiter_delay",
              "waiter_max_attempts",
              "resume_glue_job_on_retry",
              "durable",
              "openlineage_inject_parent_job_info",
              "openlineage_inject_transport_info",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .templated(
              "aws_conn_id",
              "create_job_kwargs",
              "iam_role_arn",
              "iam_role_name",
              "job_name",
              "region_name",
              "run_job_kwargs",
              "s3_bucket",
              "script_args",
              "script_location",
              "verify")
          .templateFieldsRenderers(Map.of("script_args", "json", "create_job_kwargs", "json", "run_job_kwargs", "json"))
          .uiColor("#ededed");

  private GlueJobOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code GlueJobOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** unique job name per AWS Account Defaults to "aws_glue_default_job". Templated. */
    public Builder jobName(String value) {
      return param("job_name", value);
    }

    /** unique job name per AWS Account Defaults to "aws_glue_default_job". Templated. */
    public Builder jobName(TaskRef<?> value) {
      return param("job_name", value);
    }

    /** job description details Defaults to "AWS Glue Job with Airflow". */
    public Builder jobDesc(String value) {
      return param("job_desc", value);
    }

    /** location of ETL script. Must be a local or S3 path Templated. */
    public Builder scriptLocation(String value) {
      return param("script_location", value);
    }

    /** location of ETL script. Must be a local or S3 path Templated. */
    public Builder scriptLocation(TaskRef<?> value) {
      return param("script_location", value);
    }

    /** The maximum number of concurrent runs allowed for a job */
    public Builder concurrentRunLimit(long value) {
      return param("concurrent_run_limit", value);
    }

    /** etl script arguments and AWS Glue arguments (templated) Templated. */
    public Builder scriptArgs(Map<String, Object> value) {
      return param("script_args", value);
    }

    /** etl script arguments and AWS Glue arguments (templated) Templated. */
    public Builder scriptArgs(TaskRef<?> value) {
      return param("script_args", value);
    }

    /** The maximum number of times to retry this job if it fails Defaults to 0. */
    public Builder retryLimit(long value) {
      return param("retry_limit", value);
    }

    /** Number of AWS Glue DPUs to allocate to this Job. */
    public Builder numOfDpus(double value) {
      return param("num_of_dpus", value);
    }

    /** S3 bucket where logs and local etl script will be uploaded Templated. */
    public Builder s3Bucket(String value) {
      return param("s3_bucket", value);
    }

    /** S3 bucket where logs and local etl script will be uploaded Templated. */
    public Builder s3Bucket(TaskRef<?> value) {
      return param("s3_bucket", value);
    }

    /** AWS IAM Role for Glue Job Execution. If set `iam_role_arn` must equal None. Templated. */
    public Builder iamRoleName(String value) {
      return param("iam_role_name", value);
    }

    /** AWS IAM Role for Glue Job Execution. If set `iam_role_arn` must equal None. Templated. */
    public Builder iamRoleName(TaskRef<?> value) {
      return param("iam_role_name", value);
    }

    /** AWS IAM ARN for Glue Job Execution. If set `iam_role_name` must equal None. Templated. */
    public Builder iamRoleArn(String value) {
      return param("iam_role_arn", value);
    }

    /** AWS IAM ARN for Glue Job Execution. If set `iam_role_name` must equal None. Templated. */
    public Builder iamRoleArn(TaskRef<?> value) {
      return param("iam_role_arn", value);
    }

    /** Extra arguments for Glue Job Creation Templated. */
    public Builder createJobKwargs(Map<String, Object> value) {
      return param("create_job_kwargs", value);
    }

    /** Extra arguments for Glue Job Creation Templated. */
    public Builder createJobKwargs(TaskRef<?> value) {
      return param("create_job_kwargs", value);
    }

    /** Extra arguments for Glue Job Run Templated. */
    public Builder runJobKwargs(Map<String, Object> value) {
      return param("run_job_kwargs", value);
    }

    /** Extra arguments for Glue Job Run Templated. */
    public Builder runJobKwargs(TaskRef<?> value) {
      return param("run_job_kwargs", value);
    }

    /** Whether to wait for job run completion. (default: True) Defaults to true. */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /**
     * If True, the operator will wait asynchronously for the job to complete. Deferring when
     * `wait_for_completion` is False is deprecated; from the next major release this has no
     * effect when `wait_for_completion` is False. This mode requires aiobotocore module to be
     * installed. (default: False) Defaults to false.
     */
    public Builder deferrable(boolean value) {
      return param("deferrable", value);
    }

    /**
     * If True, Glue Job Run logs show in the Airflow Task Logs. (default: False) Defaults to
     * false.
     */
    public Builder verbose(boolean value) {
      return param("verbose", value);
    }

    /** If True, the script file will be replaced in S3. (default: False) Defaults to false. */
    public Builder replaceScriptFile(boolean value) {
      return param("replace_script_file", value);
    }

    /** If True, Operator will update job configuration. (default: False) Defaults to false. */
    public Builder updateConfig(boolean value) {
      return param("update_config", value);
    }

    /**
     * If True, Operator will stop the job run when task is killed. In deferrable mode this
     * requires Airflow 3.3+, and with `durable` a retried or cleared task stops its previous
     * run and waits for it to end before starting a new one, instead of reconnecting to it.
     * Defaults to false.
     */
    public Builder stopJobRunOnKill(boolean value) {
      return param("stop_job_run_on_kill", value);
    }

    /**
     * time in seconds to wait before returning final status. This is meaningful in case of
     * limiting concurrency, Glue needs 5-10 seconds to clean up resources. Thus if status is
     * returned immediately it might end up in case of more than 1 concurrent run. It is
     * recommended to set this parameter to 10 when you are using concurrency=1. For more
     * information see:
     * https://repost.aws/questions/QUaKgpLBMPSGWO0iq2Fob_bw/glue-run-concurrent-jobs#ANFpCL2fRnQRqgDFuIU_rpvA
     * Defaults to 0.
     */
    public Builder sleepBeforeReturn(long value) {
      return param("sleep_before_return", value);
    }

    /** Defaults to 6. */
    public Builder jobPollInterval(double value) {
      return param("job_poll_interval", value);
    }

    /** Time in seconds to wait between status checks. (default: 60) Defaults to 60. */
    public Builder waiterDelay(long value) {
      return param("waiter_delay", value);
    }

    /** Maximum number of attempts to check for job completion. (default: 75) Defaults to 75. */
    public Builder waiterMaxAttempts(long value) {
      return param("waiter_max_attempts", value);
    }

    /** deprecated, use `durable` instead. */
    public Builder resumeGlueJobOnRetry(boolean value) {
      return param("resume_glue_job_on_retry", value);
    }

    /**
     * When `True`, the Glue job run id is persisted to task state before polling begins. A
     * worker crash on retry reconnects to the existing run instead of starting a duplicate.
     * Defaults to `True` on Airflow 3.3+, which uses task state store for the persisted
     * lookup; on earlier versions it defaults to `False` and, if set explicitly, recovers the
     * run by searching job runs for the task's `--airflow_task_uuid` argument.
     */
    public Builder durable(boolean value) {
      return param("durable", value);
    }

    /**
     * If True, injects OpenLineage parent job information into the Glue job's `--conf`
     * argument so the Glue Spark job emits a `parentRunFacet` linking back to the Airflow
     * task. Defaults to the `openlineage.spark_inject_parent_job_info` config value. Defaults
     * to false.
     */
    public Builder openlineageInjectParentJobInfo(boolean value) {
      return param("openlineage_inject_parent_job_info", value);
    }

    /**
     * If True, injects OpenLineage transport configuration into the Glue job's `--conf`
     * argument so the Glue Spark job sends OL events to the same backend as Airflow. Defaults
     * to the `openlineage.spark_inject_transport_info` config value. Defaults to false.
     */
    public Builder openlineageInjectTransportInfo(boolean value) {
      return param("openlineage_inject_transport_info", value);
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
