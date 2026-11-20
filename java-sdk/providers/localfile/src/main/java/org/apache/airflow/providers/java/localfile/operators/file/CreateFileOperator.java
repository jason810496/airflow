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

// Generated from providers/java/localfile/_generated/lang_sdk_operators.json by
// scripts/ci/lang_sdk_providers/generate_java_wrappers.py. Do not edit by hand.

package org.apache.airflow.providers.java.localfile.operators.file;

import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Create an empty file.
 *
 * Runs {@code airflow.providers.java.localfile.operators.file.CreateFileOperator} of {@code
 * apache-airflow-providers-java-localfile} in the Java runtime, which a Java coordinator or a
 * Dag in Java provides.
 */
public final class CreateFileOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.java.localfile.operators.file.CreateFileOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.java.localfile.operators.file", "CreateFileOperator")
          .params("path", "exist_ok")
          .required("path")
          .templated("path")
          .implementationModule("java-localfile/org.apache.airflow.providers.localfile.CreateFileOperator");

  private CreateFileOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code CreateFileOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The file to create. Missing parent directories are created too. Templated. */
    public Builder path(String value) {
      return param("path", value);
    }

    /** The file to create. Missing parent directories are created too. Templated. */
    public Builder path(TaskRef<?> value) {
      return param("path", value);
    }

    /**
     * Succeed when the file exists already. When false, an existing file fails the task.
     * Defaults to true.
     */
    public Builder existOk(boolean value) {
      return param("exist_ok", value);
    }
  }
}
