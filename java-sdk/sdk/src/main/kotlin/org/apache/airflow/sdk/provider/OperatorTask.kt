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
import org.apache.airflow.sdk.Task
import org.apache.airflow.sdk.TaskRef
import java.time.Duration
import java.time.Instant
import java.time.OffsetDateTime

/**
 * What a generated operator wrapper knows of its Python operator class.
 *
 * A wrapper builds one with [of] and the methods below, and hands it to [OperatorBuilder]. Authors
 * do not write these, because the wrappers are generated from the provider's spec.
 */
class OperatorInfo private constructor(
  internal val module: String,
  internal val name: String,
  internal val params: List<String>,
  internal val required: Set<String>,
  internal val templated: Set<String>,
  internal val implementationModule: String?,
  internal val templateExt: List<String>,
  internal val templateFieldsRenderers: Map<String, String>,
  internal val uiColor: String?,
  internal val uiFgcolor: String?,
  internal val operatorName: String?,
  internal val isSensor: Boolean,
  internal val canSkipDownstream: Boolean,
) {
  companion object {
    /**
     * Starts the description of the operator class [name] of the Python [module].
     */
    @JvmStatic
    fun of(
      module: String,
      name: String,
    ): OperatorInfo =
      OperatorInfo(module, name, emptyList(), emptySet(), emptySet(), null, emptyList(), emptyMap(), null, null, null, false, false)
  }

  internal val classpath: String get() = "$module.$name"

  /** The constructor arguments by their Python names, in the order the operator declares them. */
  fun params(vararg names: String): OperatorInfo = copy(params = names.toList())

  /** The arguments the Dag has to pass. */
  fun required(vararg names: String): OperatorInfo = copy(required = names.toSet())

  /** The arguments that take a Jinja template, or the result of another task. */
  fun templated(vararg names: String): OperatorInfo = copy(templated = names.toSet())

  /** The module of the Java runtime that implements the operator, as `<provider dir>/<class name>`. */
  fun implementationModule(value: String): OperatorInfo = copy(implementationModule = value)

  fun templateExt(vararg values: String): OperatorInfo = copy(templateExt = values.toList())

  fun templateFieldsRenderers(value: Map<String, String>): OperatorInfo = copy(templateFieldsRenderers = value.toMap())

  fun uiColor(value: String): OperatorInfo = copy(uiColor = value)

  fun uiFgcolor(value: String): OperatorInfo = copy(uiFgcolor = value)

  fun operatorName(value: String): OperatorInfo = copy(operatorName = value)

  /** Marks the operator as a sensor. */
  fun sensor(): OperatorInfo = copy(isSensor = true)

  /** Marks the operator as one that may skip its downstream tasks. */
  fun canSkipDownstream(): OperatorInfo = copy(canSkipDownstream = true)

  private fun copy(
    params: List<String> = this.params,
    required: Set<String> = this.required,
    templated: Set<String> = this.templated,
    implementationModule: String? = this.implementationModule,
    templateExt: List<String> = this.templateExt,
    templateFieldsRenderers: Map<String, String> = this.templateFieldsRenderers,
    uiColor: String? = this.uiColor,
    uiFgcolor: String? = this.uiFgcolor,
    operatorName: String? = this.operatorName,
    isSensor: Boolean = this.isSensor,
    canSkipDownstream: Boolean = this.canSkipDownstream,
  ) = OperatorInfo(
    module,
    name,
    params,
    required,
    templated,
    implementationModule,
    templateExt,
    templateFieldsRenderers,
    uiColor,
    uiFgcolor,
    operatorName,
    isSensor,
    canSkipDownstream,
  )
}

/**
 * A task that runs a provider operator, in place of a task class.
 *
 * The task carries the operator's Python class path and its constructor arguments, and no code.
 * Pass one to [org.apache.airflow.sdk.DagDef.task]:
 *
 * ```java
 * dag.task("write_report",
 *     S3CreateObjectOperator.builder().s3Bucket("reports").s3Key("report.json").data(report).build())
 *     .config("queue", "default");
 * ```
 *
 * Where the task runs depends on its `queue`, as for any task. A Python operator needs a queue
 * that a Python worker serves, such as `default`, because the Dag's own queue routes to the Java
 * runtime, which cannot run a Python operator. An operator implemented in Java, such as one of
 * `apache-airflow-providers-java-localfile`, runs in the Java runtime.
 *
 * The wrappers of a provider build these with their `builder()`. [useOperator] reaches an operator
 * that has no wrapper.
 *
 * @param R Type of the operator's result, which a downstream task reads as an XCom.
 */
class OperatorTask<R> internal constructor(
  internal val info: OperatorInfo,
  /** The arguments in the order they are written, as plain JSON, [Duration], [OffsetDateTime] or [TaskRef]. */
  internal val kwargs: Map<String, Any?>,
) {
  /** The tasks whose results the arguments take. */
  internal val upstream: List<TaskRef<*>> = kwargs.values.filterIsInstance<TaskRef<*>>().distinct()

  companion object {
    /**
     * Uses the operator [classpath] without a generated wrapper, with its arguments under their
     * Python names.
     *
     * ```java
     * dag.task("write_report", OperatorTask.useOperator(
     *     "airflow.providers.amazon.aws.operators.s3.S3CreateObjectOperator",
     *     Map.of("s3_bucket", "reports", "s3_key", "report.json", "data", report)));
     * ```
     *
     * Nothing checks the class path, the argument names or their types before the task runs, so a
     * mistake fails the task. A [TaskRef] argument resolves only when the operator templates that
     * argument.
     *
     * @param classpath Python class path of the operator, such as
     *    `airflow.providers.standard.operators.bash.BashOperator`.
     * @param kwargs The constructor arguments, in the order given.
     * @throws IllegalArgumentException if [classpath] has no module or an argument has no JSON form.
     */
    @JvmStatic
    fun <R> useOperator(
      classpath: String,
      kwargs: Map<String, Any?>,
    ): OperatorTask<R> = useOperator(classpath, null, kwargs)

    /**
     * Uses the operator [classpath], which the Java runtime implements in [implementationModule].
     *
     * @param implementationModule `<provider dir>/<class name>`, as the provider's spec names it.
     */
    @JvmStatic
    fun <R> useOperator(
      classpath: String,
      implementationModule: String?,
      kwargs: Map<String, Any?>,
    ): OperatorTask<R> {
      val dot = classpath.lastIndexOf('.')
      require(dot > 0 && dot < classpath.length - 1) {
        "'$classpath' is not the class path of an operator; it names the module and the class, as in " +
          "airflow.providers.standard.operators.bash.BashOperator"
      }
      var info = OperatorInfo.of(classpath.substring(0, dot), classpath.substring(dot + 1)).params(*kwargs.keys.toTypedArray())
      if (implementationModule != null) info = info.implementationModule(implementationModule)
      val checked =
        kwargs.entries.associateTo(linkedMapOf()) { (name, value) ->
          name to checkArgument(info.classpath, name, value, takesTaskRef = true)
        }
      return OperatorTask(info, checked)
    }
  }
}

/**
 * Base of the generated builder of an operator wrapper.
 *
 * A generated subclass has one method per constructor argument, which calls [param], and the
 * operator's [OperatorInfo]. [build] returns the task to pass to
 * [org.apache.airflow.sdk.DagDef.task].
 *
 * @param B The subclass, so that the argument methods chain.
 * @param R Type of the operator's result.
 */
abstract class OperatorBuilder<B : OperatorBuilder<B, R>, R> protected constructor(
  private val info: OperatorInfo,
) {
  private val values = linkedMapOf<String, Any?>()

  /**
   * Sets the argument [name], by its Python name.
   *
   * @throws IllegalArgumentException if the operator has no such argument, or [value] has no JSON
   *    form, or is a task's result for an argument that takes none.
   */
  @Suppress("UNCHECKED_CAST")
  protected fun param(
    name: String,
    value: Any?,
  ): B {
    require(name in info.params) { "${info.name} has no argument '$name'" }
    values[name] = checkArgument(info.classpath, name, value, takesTaskRef = name in info.templated)
    return this as B
  }

  /**
   * Builds the task.
   *
   * @throws IllegalStateException if a required argument is not set.
   */
  fun build(): OperatorTask<R> {
    val missing = info.required.filter { it !in values }
    check(missing.isEmpty()) {
      "${info.name} needs the argument(s) ${missing.joinToString { "'$it'" }}"
    }
    val ordered = linkedMapOf<String, Any?>()
    // In the order the operator declares its parameters, whatever order the caller named them in.
    info.params.filter { it in values }.forEach { ordered[it] = values.getValue(it) }
    return OperatorTask(info, ordered)
  }
}

/**
 * Copies [value] as an argument of the operator [classpath], so that a later change to what the
 * author passed does not change the Dag.
 *
 * A [Duration], [OffsetDateTime] or [Instant] is allowed only as a whole argument, and so is the
 * result of a task: the worker resolves it when it renders templates, and nothing nested in a
 * list or a map would be.
 */
internal fun checkArgument(
  classpath: String,
  name: String,
  value: Any?,
  takesTaskRef: Boolean,
): Any? {
  val label = "Argument '$name' of $classpath"

  fun copy(
    item: Any?,
    nested: Boolean,
  ): Any? =
    when (item) {
      null, is String, is Boolean, is Int, is Long, is Short, is Byte -> item
      is Double ->
        item.takeIf { it.isFinite() }
          ?: throw IllegalArgumentException("$label is $item, which JSON has no form for; pass it as a string")
      is Float -> copy(item.toDouble(), nested)
      is Duration, is OffsetDateTime, is Instant -> {
        require(!nested) { "$label holds a ${item.javaClass.simpleName} inside a list or a map; pass it as a string" }
        if (item is Instant) item.atOffset(java.time.ZoneOffset.UTC) else item
      }
      is TaskRef<*> -> {
        require(!nested) {
          "$label holds the result of task '${item.def.id}' inside a list or a map; only a whole argument can " +
            "take a task's result"
        }
        require(takesTaskRef) {
          "$label was given the result of task '${item.def.id}', but it is not a templated argument, so the " +
            "worker would not resolve it"
        }
        item
      }
      is Collection<*> -> item.map { copy(it, nested = true) }
      is Array<*> -> item.map { copy(it, nested = true) }
      is Map<*, *> ->
        item.entries.associateTo(linkedMapOf<String, Any?>()) { (key, entry) ->
          require(key is String) { "$label has a map key that is not a string" }
          key to copy(entry, nested = true)
        }
      else ->
        throw IllegalArgumentException(
          "$label holds a ${item.javaClass.name}, which JSON cannot carry; pass a string, a number, a boolean, " +
            "null, a list, a map, a Duration, an OffsetDateTime or a task's result",
        )
    }
  return copy(value, nested = false)
}

/**
 * Stands in for the class of a task that runs a provider operator. The runtime runs such a task
 * itself and never instantiates this, so it exists only so that every task definition names a class.
 */
internal class OperatorTaskPlaceholder : Task {
  override fun execute(
    context: Context,
    client: Client,
  ): Unit =
    throw IllegalStateException(
      "A task that runs a provider operator is run by the SDK, so its placeholder is never executed",
    )
}
