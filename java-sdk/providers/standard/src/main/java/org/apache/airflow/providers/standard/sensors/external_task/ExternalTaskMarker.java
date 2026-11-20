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

package org.apache.airflow.providers.standard.sensors.external_task;

import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Use this operator to indicate that a task on a different DAG depends on this task.
 *
 * Runs {@code airflow.providers.standard.sensors.external_task.ExternalTaskMarker} of {@code
 * apache-airflow-providers-standard} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class ExternalTaskMarker {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.standard.sensors.external_task.ExternalTaskMarker";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.standard.sensors.external_task", "ExternalTaskMarker")
          .params("external_dag_id", "external_task_id", "logical_date", "recursion_depth")
          .required("external_dag_id", "external_task_id")
          .templated("external_dag_id", "external_task_id", "logical_date")
          .uiColor("#4db7db");

  private ExternalTaskMarker() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code ExternalTaskMarker}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The dag_id that contains the dependent task that needs to be cleared. Templated. */
    public Builder externalDagId(String value) {
      return param("external_dag_id", value);
    }

    /** The dag_id that contains the dependent task that needs to be cleared. Templated. */
    public Builder externalDagId(TaskRef<?> value) {
      return param("external_dag_id", value);
    }

    /** The task_id of the dependent task that needs to be cleared. Templated. */
    public Builder externalTaskId(String value) {
      return param("external_task_id", value);
    }

    /** The task_id of the dependent task that needs to be cleared. Templated. */
    public Builder externalTaskId(TaskRef<?> value) {
      return param("external_task_id", value);
    }

    /**
     * The logical date of the dependent task execution that needs to be cleared. Defaults to
     * the logical date of this task's own run; a run without one clears nothing. Templated.
     */
    public Builder logicalDate(String value) {
      return param("logical_date", value);
    }

    /**
     * The logical date of the dependent task execution that needs to be cleared. Defaults to
     * the logical date of this task's own run; a run without one clears nothing. Templated.
     */
    public Builder logicalDate(TaskRef<?> value) {
      return param("logical_date", value);
    }

    /**
     * The maximum level of transitive dependencies allowed. Default is 10. This is mostly used
     * for preventing cyclic dependencies. It is fine to increase this number if necessary.
     * However, too many levels of transitive dependencies will make it slower to clear tasks
     * in the web UI. Defaults to 10.
     */
    public Builder recursionDepth(long value) {
      return param("recursion_depth", value);
    }
  }
}
