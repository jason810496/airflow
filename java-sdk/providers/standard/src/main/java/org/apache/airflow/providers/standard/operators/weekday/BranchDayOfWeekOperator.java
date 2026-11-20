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

package org.apache.airflow.providers.standard.operators.weekday;

import java.util.List;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Branches into one of two lists of tasks depending on the current day.
 *
 * Runs {@code airflow.providers.standard.operators.weekday.BranchDayOfWeekOperator} of {@code
 * apache-airflow-providers-standard} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class BranchDayOfWeekOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.standard.operators.weekday.BranchDayOfWeekOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.standard.operators.weekday", "BranchDayOfWeekOperator")
          .params(
              "follow_task_ids_if_true",
              "follow_task_ids_if_false",
              "week_day",
              "use_task_logical_date")
          .required("follow_task_ids_if_true", "follow_task_ids_if_false", "week_day")
          .canSkipDownstream();

  private BranchDayOfWeekOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code BranchDayOfWeekOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /**
     * task_id, task_group_id, or a list of task_ids and/or task_group_ids to follow if
     * criteria met.
     */
    public Builder followTaskIdsIfTrue(Object value) {
      return param("follow_task_ids_if_true", value);
    }

    /**
     * task_id, task_group_id, or a list of task_ids and/or task_group_ids to follow if
     * criteria not met.
     */
    public Builder followTaskIdsIfFalse(Object value) {
      return param("follow_task_ids_if_false", value);
    }

    /**
     * Day of the week to check (full name). Optionally, a set of days can also be provided
     * using a set. Example values:
     */
    public Builder weekDay(Object value) {
      return param("week_day", value);
    }

    /**
     * If `True`, uses task's logical date to compare with is_today. Execution Date is Useful
     * for backfilling. If `False`, uses system's day of the week. Defaults to false.
     */
    public Builder useTaskLogicalDate(boolean value) {
      return param("use_task_logical_date", value);
    }
  }
}
