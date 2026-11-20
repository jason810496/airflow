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

package org.apache.airflow.providers.amazon.aws.operators.sagemaker;

import java.util.List;
import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Evaluates a single condition or a list of conditions, and routes tasks based on the result.
 *
 * Runs {@code airflow.providers.amazon.aws.operators.sagemaker.SageMakerConditionOperator} of
 * {@code apache-airflow-providers-amazon} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class SageMakerConditionOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.amazon.aws.operators.sagemaker.SageMakerConditionOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.amazon.aws.operators.sagemaker", "SageMakerConditionOperator")
          .params(
              "condition_type",
              "left_value",
              "right_value",
              "conditions",
              "if_task_ids",
              "else_task_ids")
          .required("if_task_ids")
          .templated(
              "condition_type",
              "left_value",
              "right_value",
              "conditions",
              "if_task_ids",
              "else_task_ids")
          .canSkipDownstream();

  private SageMakerConditionOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code SageMakerConditionOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /**
     * Condition type for the simple (flat) interface. Valid types: `Equals`, `GreaterThan`,
     * `GreaterThanOrEqualTo`, `LessThan`, `LessThanOrEqualTo`, `In`. Mutually exclusive with
     * `conditions`. Templated.
     */
    public Builder conditionType(String value) {
      return param("condition_type", value);
    }

    /**
     * Condition type for the simple (flat) interface. Valid types: `Equals`, `GreaterThan`,
     * `GreaterThanOrEqualTo`, `LessThan`, `LessThanOrEqualTo`, `In`. Mutually exclusive with
     * `conditions`. Templated.
     */
    public Builder conditionType(TaskRef<?> value) {
      return param("condition_type", value);
    }

    /**
     * Left operand for the flat interface. For `In` conditions this is the value to check
     * membership of. Templated.
     */
    public Builder leftValue(Object value) {
      return param("left_value", value);
    }

    /**
     * Right operand for the flat interface. For `In` conditions this is the list of allowed
     * values. Templated.
     */
    public Builder rightValue(Object value) {
      return param("right_value", value);
    }

    /**
     * List of condition dicts to evaluate (AND-ed together). Each dict must have a `type` key.
     * Must not be empty. Mutually exclusive with `condition_type`/`left_value`/`right_value`.
     * Templated.
     */
    public Builder conditions(List<Map<String, Object>> value) {
      return param("conditions", value);
    }

    /**
     * List of condition dicts to evaluate (AND-ed together). Each dict must have a `type` key.
     * Must not be empty. Mutually exclusive with `condition_type`/`left_value`/`right_value`.
     * Templated.
     */
    public Builder conditions(TaskRef<?> value) {
      return param("conditions", value);
    }

    /** Task ID(s) to execute when all conditions are True. Templated. */
    public Builder ifTaskIds(Object value) {
      return param("if_task_ids", value);
    }

    /**
     * Task ID(s) to execute when any condition is False. If omitted, the task fails with
     * `AirflowFailException` when conditions are not met. Templated.
     */
    public Builder elseTaskIds(Object value) {
      return param("else_task_ids", value);
    }
  }
}
