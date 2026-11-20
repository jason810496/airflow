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

package org.apache.airflow.providers.standard.sensors.time_delta;

import org.apache.airflow.sdk.TaskRef;
import org.apache.airflow.sdk.provider.OperatorBuilder;
import org.apache.airflow.sdk.provider.OperatorInfo;

/**
 * Waits for a timedelta.
 *
 * Runs {@code airflow.providers.standard.sensors.time_delta.TimeDeltaSensor} of {@code
 * apache-airflow-providers-standard} on a Python worker, so give the task a queue that a
 * Python worker serves.
 */
public final class TimeDeltaSensor {
  /** The Python class path of the operator. */
  public static final String CLASSPATH = "airflow.providers.standard.sensors.time_delta.TimeDeltaSensor";

  private static final OperatorInfo INFO =
      OperatorInfo.of("airflow.providers.standard.sensors.time_delta", "TimeDeltaSensor")
          .params(
              "delta",
              "deferrable",
              "end_from_trigger",
              "poke_interval",
              "timeout",
              "soft_fail",
              "mode",
              "exponential_backoff",
              "max_wait",
              "silent_fail",
              "never_fail")
          .required("delta")
          .templated("delta")
          .uiColor("#e6f1f2")
          .sensor();

  private TimeDeltaSensor() {}

  /** Starts building a task of this operator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builds a task of {@code TimeDeltaSensor}, to pass to {@code DagDef.task}. */
  public static final class Builder extends OperatorBuilder<Builder, Object> {
    private Builder() {
      super(INFO);
    }

    /**
     * time to wait before succeeding. If a templated value is provided, it will be cast to an
     * integer representing the duration in minutes. Templated.
     */
    public Builder delta(long value) {
      return param("delta", value);
    }

    /**
     * time to wait before succeeding. If a templated value is provided, it will be cast to an
     * integer representing the duration in minutes. Templated.
     */
    public Builder delta(TaskRef<?> value) {
      return param("delta", value);
    }

    /**
     * Run sensor in deferrable mode. If set to True, task will defer itself to avoid taking up
     * a worker slot while it is waiting. Defaults to false.
     */
    public Builder deferrable(boolean value) {
      return param("deferrable", value);
    }

    /** Defaults to false. */
    public Builder endFromTrigger(boolean value) {
      return param("end_from_trigger", value);
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
