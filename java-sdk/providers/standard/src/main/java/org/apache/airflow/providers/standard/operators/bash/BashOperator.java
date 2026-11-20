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

package org.apache.airflow.providers.standard.operators.bash;

import java.util.Map;
import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Execute a Bash script, command or set of commands.
 *
 * Runs {@code airflow.providers.standard.operators.bash.BashOperator} of {@code
 * apache-airflow-providers-standard} on a Python worker, so give the task a queue that a
 * Python worker serves.
 *
 * Not available from Java, because JSON cannot carry them: {@code output_processor}.
 */
public final class BashOperator {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.standard.operators.bash.BashOperator";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.standard.operators.bash", "BashOperator")
          .params(
              "bash_command",
              "env",
              "append_env",
              "output_encoding",
              "skip_on_exit_code",
              "cwd")
          .required("bash_command")
          .templated("bash_command", "env", "cwd")
          .templateExt(".sh", ".bash")
          .templateFieldsRenderers(Map.of("bash_command", "bash", "env", "json"))
          .uiColor("#f0ede4");

  private BashOperator() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code BashOperator}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /**
     * The command, set of commands or reference to a Bash script (must be '.sh' or '.bash') to
     * be executed. (templated) Templated.
     */
    public Builder bashCommand(String value) {
      return param("bash_command", value);
    }

    /**
     * The command, set of commands or reference to a Bash script (must be '.sh' or '.bash') to
     * be executed. (templated) Templated.
     */
    public Builder bashCommand(TaskRef<?> value) {
      return param("bash_command", value);
    }

    /**
     * If env is not None, it must be a dict that defines the environment variables for the new
     * process; these are used instead of inheriting the current process environment, which is
     * the default behavior. (templated) Templated.
     */
    public Builder env(Map<String, String> value) {
      return param("env", value);
    }

    /**
     * If env is not None, it must be a dict that defines the environment variables for the new
     * process; these are used instead of inheriting the current process environment, which is
     * the default behavior. (templated) Templated.
     */
    public Builder env(TaskRef<?> value) {
      return param("env", value);
    }

    /**
     * If False(default) uses the environment variables passed in env params and does not
     * inherit the current process environment. If True, inherits the environment variables
     * from current passes and then environment variable passed by the user will either update
     * the existing inherited environment variables or the new variables gets appended to it
     * Defaults to false.
     */
    public Builder appendEnv(boolean value) {
      return param("append_env", value);
    }

    /** Output encoding of Bash command Defaults to "utf-8". */
    public Builder outputEncoding(String value) {
      return param("output_encoding", value);
    }

    /**
     * If task exits with this exit code, leave the task in `skipped` state (default: 99). If
     * set to `None`, any non-zero exit code will be treated as a failure. Defaults to 99.
     */
    public Builder skipOnExitCode(long value) {
      return param("skip_on_exit_code", value);
    }

    /**
     * Working directory to execute the command in (templated). If None (default), the command
     * is run in a temporary directory. To use current DAG folder as the working directory, you
     * might set template `{{ task.dag.folder }}`. When bash_command is a '.sh' or '.bash'
     * file, Airflow must have write access to the working directory. The script will be
     * rendered (Jinja template) into a new temporary file in this directory. Templated.
     */
    public Builder cwd(String value) {
      return param("cwd", value);
    }

    /**
     * Working directory to execute the command in (templated). If None (default), the command
     * is run in a temporary directory. To use current DAG folder as the working directory, you
     * might set template `{{ task.dag.folder }}`. When bash_command is a '.sh' or '.bash'
     * file, Airflow must have write access to the working directory. The script will be
     * rendered (Jinja template) into a new temporary file in this directory. Templated.
     */
    public Builder cwd(TaskRef<?> value) {
      return param("cwd", value);
    }
  }
}
