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
 * Write text to a file.
 *
 * Runs {@code airflow.providers.java.localfile.operators.file.WriteFileOperator} of {@code
 * apache-airflow-providers-java-localfile} in the Java runtime, which a Java coordinator or a
 * Dag in Java provides.
 */
public final class WriteFileOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.java.localfile.operators.file.WriteFileOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.java.localfile.operators.file", "WriteFileOperator")
          .params("path", "content", "mode", "encoding")
          .required("path", "content")
          .templated("path", "content")
          .implementationModule("java-localfile/org.apache.airflow.providers.localfile.WriteFileOperator");

  private WriteFileOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code WriteFileOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /** The file to write. Missing parent directories are created. Templated. */
    public Builder path(String value) {
      return param("path", value);
    }

    /** The file to write. Missing parent directories are created. Templated. */
    public Builder path(TaskRef<?> value) {
      return param("path", value);
    }

    /** The text to write. Templated. */
    public Builder content(String value) {
      return param("content", value);
    }

    /** The text to write. Templated. */
    public Builder content(TaskRef<?> value) {
      return param("content", value);
    }

    /**
     * Replace what the file holds, or add to its end. One of "overwrite", "append". Defaults
     * to "overwrite".
     */
    public Builder mode(String value) {
      return param("mode", value);
    }

    /**
     * How the text is encoded in the file. One of "utf-8", "ascii", "latin1". Defaults to
     * "utf-8".
     */
    public Builder encoding(String value) {
      return param("encoding", value);
    }
  }
}
