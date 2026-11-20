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

package org.apache.airflow.sdk.provider

import org.apache.airflow.sdk.Client
import org.apache.airflow.sdk.Context

/**
 * Marks a class as the implementation of a provider operator, so that a task of the
 * operator runs in the Java runtime, whichever language its Dag is written in.
 *
 * The class implements [Operator] and has a public no-argument constructor. Each
 * constructor argument of the operator is a field marked with [Param]:
 *
 * ```java
 * @ProviderOperator(module = "airflow.providers.java.localfile.operators.file", name = "HasFileOperator")
 * public class HasFileOperator implements Operator<Boolean> {
 *   @Param(required = true, templated = true)
 *   public String path;
 *
 *   @Override
 *   public Boolean execute(OperatorContext context) { return Files.exists(Path.of(path)); }
 * }
 * ```
 *
 * The operator is known to Airflow by the Python class path `<module>.<name>`, which the Python
 * wrapper of the provider also has. Package the class in a JAR that a Java coordinator finds
 * under its `java_providers_path`, or put it on the classpath of a Java Dag.
 *
 * @property module Python module of the operator's class, such as
 *    `airflow.providers.java.localfile.operators.file`.
 * @property name Python class name. The simple name of the annotated class when blank.
 * @property description What the operator does, shown in the provider's generated documentation.
 * @property canSkipDownstream Whether the operator may skip its direct downstream tasks with
 *    [OperatorContext.skipDownstream], as Python's `SkipMixin` marks it.
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@MustBeDocumented
annotation class ProviderOperator(
  val module: String,
  val name: String = "",
  val description: String = "",
  val canSkipDownstream: Boolean = false,
)

/**
 * Marks a field of a [ProviderOperator] class as a constructor argument of the operator.
 *
 * Before [Operator.execute] runs, the runtime sets the field from the argument the Dag passed.
 * The field's type says how the value is read: `String`, `boolean`, a number, [java.time.Duration]
 * for a number of seconds, [java.time.OffsetDateTime] or [java.time.Instant] for an ISO 8601
 * string, a `List`, a `Map` with string keys, or `Object` for any JSON value. A field the Dag did
 * not pass keeps the value it was initialized with, which is the operator's default.
 *
 * @property name The argument's name in the Python constructor. Snake case of the field's name when
 *    blank, so `existOk` is `exist_ok`.
 * @property required Whether the Dag must pass the argument.
 * @property templated Whether the argument takes a Jinja template, which the supervisor renders
 *    before the operator runs, or the result of another task.
 * @property description What the argument means, shown in the provider's generated documentation.
 * @property choices The values a `String` argument may take, when it takes only some.
 */
@Target(AnnotationTarget.FIELD)
@Retention(AnnotationRetention.RUNTIME)
@MustBeDocumented
annotation class Param(
  val name: String = "",
  val required: Boolean = false,
  val templated: Boolean = false,
  val description: String = "",
  val choices: Array<String> = [],
)

/**
 * The body of a [ProviderOperator].
 *
 * @param R Type of the operator's result. The runtime pushes it as the task's `return_value` XCom,
 *    so it has to be JSON: a string, a number, a boolean, a `List`, a `Map`, or an object that
 *    Jackson writes as one. A `null` result pushes nothing.
 */
interface Operator<R> {
  /**
   * Runs the operator with its arguments set.
   *
   * Throwing fails the task, or retries it when it has retries left.
   *
   * @param context What the operator may read of the run and call on Airflow.
   * @return The task's `return_value` XCom.
   */
  @Throws(Exception::class)
  fun execute(context: OperatorContext): R
}

/**
 * What an [Operator] is told about the task it runs, and what it can ask Airflow for.
 *
 * @property context The Dag run and the task instance, as a [org.apache.airflow.sdk.Task] gets them.
 * @property client Client for Airflow API calls scoped to this task instance, such as reading a
 *    Variable.
 * @param onSkipDownstream What [skipDownstream] does. The runtime builds the context with the
 *    skipping of the direct downstream tasks; a test of an operator may build its own.
 */
class OperatorContext(
  @JvmField val context: Context,
  @JvmField val client: Client,
  private val onSkipDownstream: Runnable = Runnable {},
) {
  /**
   * Skips the direct downstream tasks of this task, as a branch operator does.
   *
   * Only an operator that declares [ProviderOperator.canSkipDownstream] should call it.
   */
  fun skipDownstream() = onSkipDownstream.run()
}
