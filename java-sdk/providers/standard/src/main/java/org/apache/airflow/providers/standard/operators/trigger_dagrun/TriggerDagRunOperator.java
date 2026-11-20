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

package org.apache.airflow.providers.standard.operators.trigger_dagrun;

import java.util.List;
import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Triggers a DAG run for a specified DAG ID.
 *
 * Runs {@code airflow.providers.standard.operators.trigger_dagrun.TriggerDagRunOperator} of
 * {@code apache-airflow-providers-standard} on a Python worker, so give the task a queue that
 * a Python worker serves.
 */
public final class TriggerDagRunOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.standard.operators.trigger_dagrun.TriggerDagRunOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.standard.operators.trigger_dagrun", "TriggerDagRunOperator")
          .params(
              "trigger_dag_id",
              "trigger_run_id",
              "conf",
              "logical_date",
              "run_after",
              "reset_dag_run",
              "wait_for_completion",
              "poke_interval",
              "allowed_states",
              "failed_states",
              "skip_when_already_exists",
              "fail_when_dag_is_paused",
              "note",
              "deferrable",
              "openlineage_inject_parent_info")
          .required("trigger_dag_id")
          .templated(
              "trigger_dag_id",
              "trigger_run_id",
              "logical_date",
              "conf",
              "wait_for_completion",
              "skip_when_already_exists")
          .templateFieldsRenderers(Map.of("conf", "py"))
          .uiColor("#ffefeb");

  private TriggerDagRunOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code TriggerDagRunOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The `dag_id` of the DAG to trigger (templated). Templated. */
    public Builder triggerDagId(String value) {
      return param("trigger_dag_id", value);
    }

    /** The `dag_id` of the DAG to trigger (templated). Templated. */
    public Builder triggerDagId(TaskRef<?> value) {
      return param("trigger_dag_id", value);
    }

    /**
     * The run ID to use for the triggered DAG run (templated). If not provided, a run ID will
     * be automatically generated. Templated.
     */
    public Builder triggerRunId(String value) {
      return param("trigger_run_id", value);
    }

    /**
     * The run ID to use for the triggered DAG run (templated). If not provided, a run ID will
     * be automatically generated. Templated.
     */
    public Builder triggerRunId(TaskRef<?> value) {
      return param("trigger_run_id", value);
    }

    /** Configuration for the DAG run (templated). Templated. */
    public Builder conf(Map<String, Object> value) {
      return param("conf", value);
    }

    /** Configuration for the DAG run (templated). Templated. */
    public Builder conf(TaskRef<?> value) {
      return param("conf", value);
    }

    /** Logical date for the triggered DAG (templated). Templated. */
    public Builder logicalDate(String value) {
      return param("logical_date", value);
    }

    /** Logical date for the triggered DAG (templated). Templated. */
    public Builder logicalDate(TaskRef<?> value) {
      return param("logical_date", value);
    }

    /** The date before which the triggered DAG should not run. */
    public Builder runAfter(String value) {
      return param("run_after", value);
    }

    /**
     * Whether clear existing DAG run if already exists. This is useful when backfill or rerun
     * an existing DAG run. This only resets (not recreates) the DAG run. DAG run conf is
     * immutable and will not be reset on rerun of an existing DAG run. When
     * reset_dag_run=False and dag run exists, DagRunAlreadyExists will be raised. When
     * reset_dag_run=True and dag run exists, existing DAG run will be cleared to rerun.
     * Defaults to false.
     */
    public Builder resetDagRun(boolean value) {
      return param("reset_dag_run", value);
    }

    /**
     * Whether or not wait for DAG run completion. (default: False) Defaults to false.
     * Templated.
     */
    public Builder waitForCompletion(boolean value) {
      return param("wait_for_completion", value);
    }

    /**
     * Whether or not wait for DAG run completion. (default: False) Defaults to false.
     * Templated.
     */
    public Builder waitForCompletion(TaskRef<?> value) {
      return param("wait_for_completion", value);
    }

    /**
     * Poke interval to check DAG run status when wait_for_completion=True. (default: 60)
     * Defaults to 60.
     */
    public Builder pokeInterval(long value) {
      return param("poke_interval", value);
    }

    /**
     * Optional list of allowed DAG run states of the triggered DAG. This is useful when
     * setting `wait_for_completion` to True. Must be a valid DagRunState. Default is
     * `[DagRunState.SUCCESS]`.
     */
    public Builder allowedStates(List<String> value) {
      return param("allowed_states", value);
    }

    /**
     * Optional list of failed or disallowed DAG run states of the triggered DAG. This is
     * useful when setting `wait_for_completion` to True. Must be a valid DagRunState. Default
     * is `[DagRunState.FAILED]`.
     */
    public Builder failedStates(List<String> value) {
      return param("failed_states", value);
    }

    /**
     * Set to true to mark the task as SKIPPED if a DAG run of the triggered DAG for the same
     * logical date already exists. Defaults to false. Templated.
     */
    public Builder skipWhenAlreadyExists(boolean value) {
      return param("skip_when_already_exists", value);
    }

    /**
     * Set to true to mark the task as SKIPPED if a DAG run of the triggered DAG for the same
     * logical date already exists. Defaults to false. Templated.
     */
    public Builder skipWhenAlreadyExists(TaskRef<?> value) {
      return param("skip_when_already_exists", value);
    }

    /**
     * If the dag to trigger is paused, DagIsPaused will be raised. On Airflow 3.x this
     * requires Airflow 3.2.0+ (it relies on the task-SDK DAG state endpoint added then); on
     * Airflow 3.0/3.1 setting this raises `NotImplementedError`. Defaults to false.
     */
    public Builder failWhenDagIsPaused(boolean value) {
      return param("fail_when_dag_is_paused", value);
    }

    public Builder note(String value) {
      return param("note", value);
    }

    /**
     * If waiting for completion, whether to defer the task until done, default is `False`.
     * Defaults to false.
     */
    public Builder deferrable(boolean value) {
      return param("deferrable", value);
    }

    /**
     * whether to include OpenLineage metadata about the parent task in the triggered DAG run's
     * conf, enabling improved lineage tracking. The metadata is only injected if OpenLineage
     * is enabled and running. This option does not modify any other part of the conf, and
     * existing OpenLineage-related settings in the conf will not be overwritten. The injection
     * process is safeguarded against exceptions - if any error occurs during metadata
     * injection, it is gracefully handled and the conf remains unchanged - so it's safe to
     * use. Default is `True` Defaults to true.
     */
    public Builder openlineageInjectParentInfo(boolean value) {
      return param("openlineage_inject_parent_info", value);
    }
  }
}
