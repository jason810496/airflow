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

import java.util.List;
import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Evaluate a ruleset against a data source (Glue table).
 *
 * Runs {@code
 * airflow.providers.amazon.aws.operators.glue.GlueDataQualityRuleSetEvaluationRunOperator} of
 * {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class GlueDataQualityRuleSetEvaluationRunOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.glue.GlueDataQualityRuleSetEvaluationRunOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.glue", "GlueDataQualityRuleSetEvaluationRunOperator")
          .params(
              "datasource",
              "role",
              "rule_set_names",
              "number_of_workers",
              "timeout",
              "verify_result_status",
              "show_results",
              "rule_set_evaluation_run_kwargs",
              "wait_for_completion",
              "waiter_delay",
              "waiter_max_attempts",
              "deferrable",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("datasource", "role", "rule_set_names")
          .templated(
              "aws_conn_id",
              "datasource",
              "region_name",
              "role",
              "rule_set_evaluation_run_kwargs",
              "rule_set_names",
              "verify")
          .templateFieldsRenderers(Map.of("datasource", "json", "rule_set_evaluation_run_kwargs", "json"))
          .uiColor("#ededed");

  private GlueDataQualityRuleSetEvaluationRunOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code GlueDataQualityRuleSetEvaluationRunOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The data source (Glue table) associated with this run. (templated) Templated. */
    public Builder datasource(Map<String, Object> value) {
      return param("datasource", value);
    }

    /** The data source (Glue table) associated with this run. (templated) Templated. */
    public Builder datasource(TaskRef<?> value) {
      return param("datasource", value);
    }

    /** IAM role supplied for job execution. (templated) Templated. */
    public Builder role(String value) {
      return param("role", value);
    }

    /** IAM role supplied for job execution. (templated) Templated. */
    public Builder role(TaskRef<?> value) {
      return param("role", value);
    }

    /** A list of ruleset names for evaluation. (templated) Templated. */
    public Builder ruleSetNames(List<String> value) {
      return param("rule_set_names", value);
    }

    /** A list of ruleset names for evaluation. (templated) Templated. */
    public Builder ruleSetNames(TaskRef<?> value) {
      return param("rule_set_names", value);
    }

    /** The number of G.1X workers to be used in the run. (default: 5) Defaults to 5. */
    public Builder numberOfWorkers(long value) {
      return param("number_of_workers", value);
    }

    /**
     * The timeout for a run in minutes. This is the maximum time that a run can consume
     * resources before it is terminated and enters TIMEOUT status. (default: 2,880) Defaults
     * to 2880.
     */
    public Builder timeout(long value) {
      return param("timeout", value);
    }

    /**
     * Validate all the ruleset rules evaluation run results, If any of the rule status is Fail
     * or Error then an exception is thrown. (default: True) Defaults to true.
     */
    public Builder verifyResultStatus(boolean value) {
      return param("verify_result_status", value);
    }

    /** Displays all the ruleset rules evaluation run results. (default: True) Defaults to true. */
    public Builder showResults(boolean value) {
      return param("show_results", value);
    }

    /** Extra arguments for evaluation run. (templated) Templated. */
    public Builder ruleSetEvaluationRunKwargs(Map<String, Object> value) {
      return param("rule_set_evaluation_run_kwargs", value);
    }

    /** Extra arguments for evaluation run. (templated) Templated. */
    public Builder ruleSetEvaluationRunKwargs(TaskRef<?> value) {
      return param("rule_set_evaluation_run_kwargs", value);
    }

    /** Whether to wait for job to stop. (default: True) Defaults to true. */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /** Time in seconds to wait between status checks. (default: 60) Defaults to 60. */
    public Builder waiterDelay(long value) {
      return param("waiter_delay", value);
    }

    /** Maximum number of attempts to check for job completion. (default: 20) Defaults to 20. */
    public Builder waiterMaxAttempts(long value) {
      return param("waiter_max_attempts", value);
    }

    /**
     * If True, the operator will wait asynchronously for the job to stop. Deferring when
     * `wait_for_completion` is False is deprecated; from the next major release this has no
     * effect when `wait_for_completion` is False. This mode requires aiobotocore module to be
     * installed. (default: False) Defaults to false.
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
  }
}
