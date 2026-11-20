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

package org.apache.airflow.providers.standard.operators.hitl;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * BranchOperator based on Human-in-the-loop Response.
 *
 * Runs {@code airflow.providers.standard.operators.hitl.HITLBranchOperator} of {@code
 * apache-airflow-providers-standard} on a Python worker, so give the task a queue that a
 * Python worker serves.
 *
 * Not available from Java, because JSON cannot carry them: {@code notifiers}.
 */
public final class HITLBranchOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.standard.operators.hitl.HITLBranchOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.standard.operators.hitl", "HITLBranchOperator")
          .params(
              "options_mapping",
              "subject",
              "options",
              "body",
              "defaults",
              "multiple",
              "params",
              "assigned_users",
              "response_timeout")
          .required("subject", "options")
          .templated("subject", "body")
          .canSkipDownstream();

  private HITLBranchOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code HITLBranchOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    public Builder optionsMapping(Map<String, String> value) {
      return param("options_mapping", value);
    }

    /** Headline/subject presented to the user for the interaction task. Templated. */
    public Builder subject(String value) {
      return param("subject", value);
    }

    /** Headline/subject presented to the user for the interaction task. Templated. */
    public Builder subject(TaskRef<?> value) {
      return param("subject", value);
    }

    /** List of options that the an user can select from to complete the task. */
    public Builder options(List<String> value) {
      return param("options", value);
    }

    /**
     * Descriptive text (with Markdown support) that gives the details that are needed to
     * decide. Templated.
     */
    public Builder body(String value) {
      return param("body", value);
    }

    /**
     * Descriptive text (with Markdown support) that gives the details that are needed to
     * decide. Templated.
     */
    public Builder body(TaskRef<?> value) {
      return param("body", value);
    }

    /** The default options and the options that are taken if timeout is passed. */
    public Builder defaults(Object value) {
      return param("defaults", value);
    }

    /** Whether the user can select one or multiple options. Defaults to false. */
    public Builder multiple(boolean value) {
      return param("multiple", value);
    }

    /**
     * dictionary of parameter definitions that are in the format of Dag params such that a
     * Form Field can be rendered. Entered data is validated (schema, required fields) like for
     * a Dag run and added to XCom of the task result.
     */
    public Builder params(Map<String, Object> value) {
      return param("params", value);
    }

    public Builder assignedUsers(Object value) {
      return param("assigned_users", value);
    }

    /**
     * Maximum time to wait for a human response. On Airflow 3.3+ this is enforced by the
     * scheduler's `awaiting_input` timeout sweep; on older versions it is passed to the
     * triggerer. This is separate from `execution_timeout`, which controls the execution phase
     * before the task starts waiting. If not set, no timeout is applied to the human response
     * wait.
     */
    public Builder responseTimeout(Duration value) {
      return param("response_timeout", value);
    }
  }
}
