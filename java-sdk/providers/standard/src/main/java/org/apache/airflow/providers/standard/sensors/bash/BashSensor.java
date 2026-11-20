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

package org.apache.airflow.providers.standard.sensors.bash;

import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Executes a bash command/script.
 *
 * Runs {@code airflow.providers.standard.sensors.bash.BashSensor} of {@code
 * apache-airflow-providers-standard} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class BashSensor {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.standard.sensors.bash.BashSensor";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.standard.sensors.bash", "BashSensor")
          .params(
              "bash_command",
              "env",
              "output_encoding",
              "retry_exit_code",
              "poke_interval",
              "timeout",
              "soft_fail",
              "mode",
              "exponential_backoff",
              "max_wait",
              "silent_fail",
              "never_fail")
          .required("bash_command")
          .templated("bash_command", "env")
          .uiColor("#e6f1f2")
          .sensor();

  private BashSensor() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code BashSensor}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /**
     * The command, set of commands or reference to a bash script (must be '.sh') to be
     * executed. Templated.
     */
    public Builder bashCommand(Object value) {
      return param("bash_command", value);
    }

    /**
     * If env is not None, it must be a mapping that defines the environment variables for the
     * new process; these are used instead of inheriting the current process environment, which
     * is the default behavior. (templated) Templated.
     */
    public Builder env(Object value) {
      return param("env", value);
    }

    /** output encoding of bash command. Defaults to "utf-8". */
    public Builder outputEncoding(Object value) {
      return param("output_encoding", value);
    }

    /**
     * If task exits with this code, treat the sensor as not-yet-complete and retry the check
     * later according to the usual retry/timeout settings. Any other non-zero return code will
     * be treated as an error, and cause the sensor to fail. If set to `None` (the default),
     * any non-zero exit code will cause a retry and the task will never raise an error except
     * on time-out.
     */
    public Builder retryExitCode(long value) {
      return param("retry_exit_code", value);
    }

    /**
     * Time that the job should wait in between each try. Can be `timedelta` or `float`
     * seconds. Defaults to 60.
     */
    public Builder pokeInterval(double value) {
      return param("poke_interval", value);
    }

    /**
     * Time elapsed before the task times out and fails. Can be `timedelta` or `float` seconds.
     * This should not be confused with `execution_timeout` of the `BaseOperator` class.
     * `timeout` measures the time elapsed between the first poke and the current time (taking
     * into account any reschedule delay between each poke), while `execution_timeout` checks
     * the **running** time of the task (leaving out any reschedule delay). In case that the
     * `mode` is `poke` (see below), both of them are equivalent (as the sensor is never
     * rescheduled), which is not the case in `reschedule` mode.
     */
    public Builder timeout(double value) {
      return param("timeout", value);
    }

    /**
     * Set to true to mark the task as SKIPPED on failure. Mutually exclusive with never_fail.
     * Defaults to false.
     */
    public Builder softFail(boolean value) {
      return param("soft_fail", value);
    }

    /**
     * How the sensor operates. Options are: `{ poke | reschedule }`, default is `poke`. When
     * set to `poke` the sensor is taking up a worker slot for its whole execution time and
     * sleeps between pokes. Use this mode if the expected runtime of the sensor is short or if
     * a short poke interval is required. Note that the sensor will hold onto a worker slot and
     * a pool slot for the duration of the sensor's runtime in this mode. When set to
     * `reschedule` the sensor task frees the worker slot when the criteria is not yet met and
     * it's rescheduled at a later time. Use this mode if the time before the criteria is met
     * is expected to be quite long. The poke interval should be more than one minute to
     * prevent too much load on the scheduler. Defaults to "poke".
     */
    public Builder mode(String value) {
      return param("mode", value);
    }

    /**
     * allow progressive longer waits between pokes by using exponential backoff algorithm
     * Defaults to false.
     */
    public Builder exponentialBackoff(boolean value) {
      return param("exponential_backoff", value);
    }

    /** maximum wait interval between pokes, can be `timedelta` or `float` seconds */
    public Builder maxWait(double value) {
      return param("max_wait", value);
    }

    /**
     * If true, and poke method raises an exception different from AirflowSensorTimeout,
     * AirflowTaskTimeout, AirflowSkipException and AirflowFailException, the sensor will log
     * the error and continue its execution. Otherwise, the sensor task fails, and it can be
     * retried based on the provided `retries` parameter. Defaults to false.
     */
    public Builder silentFail(boolean value) {
      return param("silent_fail", value);
    }

    /**
     * If true, and poke method raises an exception, sensor will be skipped. Mutually exclusive
     * with soft_fail. Defaults to false.
     */
    public Builder neverFail(boolean value) {
      return param("never_fail", value);
    }
  }
}
