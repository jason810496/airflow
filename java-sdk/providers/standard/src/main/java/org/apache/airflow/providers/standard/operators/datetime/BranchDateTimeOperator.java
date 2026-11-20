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

// Generated from providers/standard/_generated/lang_sdk_operators.json by
// scripts/ci/lang_sdk_providers/generate_java_wrappers.py. Do not edit by hand.

package org.apache.airflow.providers.standard.operators.datetime;

import java.time.OffsetDateTime;
import java.util.List;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Branches into one of two lists of tasks depending on the current datetime.
 *
 * Runs {@code airflow.providers.standard.operators.datetime.BranchDateTimeOperator} of {@code
 * apache-airflow-providers-standard} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class BranchDateTimeOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.standard.operators.datetime.BranchDateTimeOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.standard.operators.datetime", "BranchDateTimeOperator")
          .params(
              "follow_task_ids_if_true",
              "follow_task_ids_if_false",
              "target_lower",
              "target_upper",
              "use_task_logical_date")
          .required(
              "follow_task_ids_if_true",
              "follow_task_ids_if_false",
              "target_lower",
              "target_upper")
          .canSkipDownstream();

  private BranchDateTimeOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code BranchDateTimeOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /**
     * task_id, task_group_id, or a list of task_ids and/or task_group_ids to follow if
     * `datetime.datetime.now()` falls above target_lower and below target_upper.
     */
    public Builder followTaskIdsIfTrue(Object value) {
      return param("follow_task_ids_if_true", value);
    }

    /**
     * task_id, task_group_id, or a list of task_ids and/or task_group_ids to follow if
     * `datetime.datetime.now()` falls below target_lower or above target_upper.
     */
    public Builder followTaskIdsIfFalse(Object value) {
      return param("follow_task_ids_if_false", value);
    }

    /** target lower bound. */
    public Builder targetLower(OffsetDateTime value) {
      return param("target_lower", value);
    }

    /** target upper bound. */
    public Builder targetUpper(OffsetDateTime value) {
      return param("target_upper", value);
    }

    /**
     * If `True`, uses task's logical date to compare with targets. Logical date is useful for
     * backfilling. If `False`, uses system's date. Defaults to false.
     */
    public Builder useTaskLogicalDate(boolean value) {
      return param("use_task_logical_date", value);
    }
  }
}
