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
 * Creates a data quality ruleset with DQDL rules applied to a specified Glue table.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.glue.GlueDataQualityOperator} of {@code
 * apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a Python
 * worker serves.
 */
public final class GlueDataQualityOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.glue.GlueDataQualityOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.glue", "GlueDataQualityOperator")
          .params(
              "name",
              "ruleset",
              "description",
              "update_rule_set",
              "data_quality_ruleset_kwargs",
              "aws_conn_id",
              "region_name",
              "verify",
              "botocore_config",
              "region")
          .required("name", "ruleset")
          .templated(
              "aws_conn_id",
              "data_quality_ruleset_kwargs",
              "description",
              "name",
              "region_name",
              "ruleset",
              "verify")
          .templateFieldsRenderers(Map.of("data_quality_ruleset_kwargs", "json"))
          .uiColor("#ededed");

  private GlueDataQualityOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code GlueDataQualityOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** A unique name for the data quality ruleset. Templated. */
    public Builder name(String value) {
      return param("name", value);
    }

    /** A unique name for the data quality ruleset. Templated. */
    public Builder name(TaskRef<?> value) {
      return param("name", value);
    }

    /**
     * A Data Quality Definition Language (DQDL) ruleset. For more information, see the Glue
     * developer guide. Templated.
     */
    public Builder ruleset(String value) {
      return param("ruleset", value);
    }

    /**
     * A Data Quality Definition Language (DQDL) ruleset. For more information, see the Glue
     * developer guide. Templated.
     */
    public Builder ruleset(TaskRef<?> value) {
      return param("ruleset", value);
    }

    /**
     * A description of the data quality ruleset. Defaults to "AWS Glue Data Quality Rule Set
     * With Airflow". Templated.
     */
    public Builder description(String value) {
      return param("description", value);
    }

    /**
     * A description of the data quality ruleset. Defaults to "AWS Glue Data Quality Rule Set
     * With Airflow". Templated.
     */
    public Builder description(TaskRef<?> value) {
      return param("description", value);
    }

    /** To update existing ruleset, Set this flag to True. (default: False) Defaults to false. */
    public Builder updateRuleSet(boolean value) {
      return param("update_rule_set", value);
    }

    /** Extra arguments for RuleSet. Templated. */
    public Builder dataQualityRulesetKwargs(Map<String, Object> value) {
      return param("data_quality_ruleset_kwargs", value);
    }

    /** Extra arguments for RuleSet. Templated. */
    public Builder dataQualityRulesetKwargs(TaskRef<?> value) {
      return param("data_quality_ruleset_kwargs", value);
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
