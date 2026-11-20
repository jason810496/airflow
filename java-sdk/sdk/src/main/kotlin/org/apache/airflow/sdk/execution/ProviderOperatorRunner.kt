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

package org.apache.airflow.sdk.execution

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.json.JsonMapper
import kotlinx.coroutines.CancellationException
import org.apache.airflow.sdk.Client
import org.apache.airflow.sdk.Context
import org.apache.airflow.sdk.SKIPMIXIN_SKIPPED
import org.apache.airflow.sdk.SKIPMIXIN_XCOM_KEY
import org.apache.airflow.sdk.execution.comm.StartupDetails
import org.apache.airflow.sdk.internal.ArgValues
import org.apache.airflow.sdk.provider.Operator
import org.apache.airflow.sdk.provider.OperatorContext
import org.apache.airflow.sdk.provider.OperatorDefinition
import org.apache.airflow.sdk.provider.ParamDefinition
import org.apache.airflow.sdk.provider.readOperatorDefinition
import java.io.File
import java.net.URLClassLoader
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.time.OffsetDateTime
import kotlin.math.roundToLong

/**
 * The operator cannot run in this runtime, whatever the task does next: the Dag names an operator
 * the runtime does not have, or leaves out an argument it needs. Retrying cannot change that.
 */
internal class ProviderOperatorException(
  message: String,
  cause: Throwable? = null,
) : RuntimeException(message, cause)

/** `TIRunContext.provider_operator`, which the supervisor decodes into a plain map. */
internal class ProviderRequest(
  val classpath: String,
  val module: String?,
  val kwargs: List<ArgBinding>,
  val downstreamTaskIds: Map<String, List<String>>,
) {
  companion object {
    fun decode(raw: Any?): ProviderRequest {
      check(raw is Map<*, *>) { "provider_operator payload is not a map: ${raw?.javaClass?.name}" }
      val downstream =
        (raw["downstream_task_ids"] as? Map<*, *>).orEmpty().entries.associate { (key, ids) ->
          key.toString() to (ids as? List<*>).orEmpty().map { it.toString() }
        }
      return ProviderRequest(
        classpath = checkNotNull(raw["classpath"] as? String) { "provider_operator has no classpath: $raw" },
        module = raw["module"] as? String,
        kwargs = decodeArgBindings(raw["kwargs"]),
        downstreamTaskIds = downstream,
      )
    }
  }
}

/**
 * Finds the class of an operator the task names, in a class loader of its own per provider.
 *
 * The task names the class as `<provider dir>/<class name>`. With a [providersPath], each
 * `<provider dir>` under it gets one [URLClassLoader] over its JARs, whose parent is the loader of the
 * SDK. So the SDK classes are shared with every provider, and a provider's own dependencies stay
 * apart from the other providers'. Without one, the Dag's own classpath holds the provider, so the
 * class is found there.
 */
internal class ProviderLoader(
  private val providersPath: Path?,
) {
  private val loaders = hashMapOf<String, ClassLoader>()

  /**
   * @throws ProviderOperatorException if the task names no module, or the module cannot be loaded, or
   *    holds something other than the operator.
   */
  fun find(request: ProviderRequest): OperatorDefinition {
    val module =
      request.module
        ?: throw ProviderOperatorException(
          "${request.classpath} is implemented in Python, so a Python worker has to run it, but this task's " +
            "queue is routed to the Java coordinator. Give the task a queue that a Python worker serves, and " +
            "check [sdk] queue_to_coordinator.",
        )
    val separator = module.indexOf('/')
    if (separator <= 0 || separator == module.length - 1) {
      throw ProviderOperatorException("'$module' is not a module of the Java runtime, which is <provider dir>/<class name>")
    }
    val directory = module.substring(0, separator)
    val className = module.substring(separator + 1)
    val type =
      try {
        Class.forName(className, false, getLoader(directory, request.classpath))
      } catch (e: ClassNotFoundException) {
        throw ProviderOperatorException(
          "Cannot load '$className' to run ${request.classpath}: " +
            if (providersPath == null) {
              "it is not on the classpath of the Dag. Add the provider's JAR to the Dag's dependencies."
            } else {
              "it is not in a JAR of '$directory' under $providersPath. Install the provider there."
            },
          e,
        )
      } catch (e: LinkageError) {
        throw ProviderOperatorException("Cannot load '$className' to run ${request.classpath}: $e", e)
      }
    val definition =
      try {
        readOperatorDefinition(type)
      } catch (e: IllegalArgumentException) {
        throw ProviderOperatorException("'$module' is not an operator: ${e.message}", e)
      }
    if (definition.classpath != request.classpath) {
      throw ProviderOperatorException(
        "'$module' implements ${definition.classpath}, not ${request.classpath}",
      )
    }
    return definition
  }

  private fun getLoader(
    directory: String,
    classpath: String,
  ): ClassLoader {
    val root = providersPath ?: return Thread.currentThread().contextClassLoader ?: ProviderLoader::class.java.classLoader
    if (!DIRECTORY_NAME.matches(directory) || directory == SDK_DIRECTORY || directory == "." || directory == "..") {
      throw ProviderOperatorException("'$directory' is not the directory of a provider, so $classpath cannot be loaded from it")
    }
    return loaders.getOrPut(directory) {
      val jars =
        root
          .resolve(directory)
          .toFile()
          .listFiles { file -> file.isFile && file.name.endsWith(".jar") }
          .orEmpty()
      URLClassLoader(
        jars.sortedBy(File::getName).map { it.toURI().toURL() }.toTypedArray(),
        ProviderLoader::class.java.classLoader,
      )
    }
  }

  companion object {
    /** The directory of the SDK's own JARs, which is not a provider. */
    const val SDK_DIRECTORY = "sdk"

    private val DIRECTORY_NAME = Regex("[A-Za-z0-9._-]+")
  }
}

/** Runs the task of a provider operator that this runtime implements. */
internal object ProviderOperatorRunner {
  private val logger = Logger(ProviderOperatorRunner::class)
  private val mapper: ObjectMapper = JsonMapper.builder().build().findAndRegisterModules()

  /**
   * Runs the operator [raw] names, in the way every other task of the SDK ends: success with the
   * operator's result as the `return_value` XCom, or a failure that retries when the task has
   * retries left.
   */
  fun run(
    request: StartupDetails,
    client: Client,
    raw: Any?,
    loader: ProviderLoader,
  ): Any =
    try {
      val provider = ProviderRequest.decode(raw)
      val definition = loader.find(provider)
      val instance =
        definition.type
          .getDeclaredConstructor()
          .apply { isAccessible = true }
          .newInstance()
      bindArguments(definition, instance, provider, client)
      val downstream = provider.downstreamTaskIds[request.ti.taskId].orEmpty()
      val context = OperatorContext(Context.from(request), client) { skipTasks(client, downstream) }

      @Suppress("UNCHECKED_CAST")
      val result = (instance as Operator<Any?>).execute(context)
      toJson(result)?.let { client.setXCom(value = it) }
      TaskResult.success()
    } catch (e: CancellationException) {
      throw e
    } catch (e: ProviderOperatorException) {
      logger.error(e.message ?: "The provider operator cannot run", mapOf("ti" to request.ti))
      // Retrying runs the same Dag against the same installation.
      TaskResult.failure(shouldRetry = false)
    } catch (e: Throwable) {
      logger.error(
        "Error executing provider operator",
        mapOf("ti" to request.ti, "error" to e, "trace" to e.stackTraceToString()),
      )
      TaskResult.failure(request.tiContext.shouldRetry)
    }

  private fun skipTasks(
    client: Client,
    taskIds: List<String>,
  ) {
    if (taskIds.isEmpty()) return
    // Written before the skip so a cleared downstream task is skipped again, as SkipMixin does.
    client.setXCom(SKIPMIXIN_XCOM_KEY, mapOf(SKIPMIXIN_SKIPPED to taskIds))
    client.impl.skipDownstreamTasks(taskIds)
  }

  /**
   * Sets each field from the argument the task passed: the literals of the template fields come
   * rendered by the supervisor, and an upstream's result is pulled as it is.
   */
  private fun bindArguments(
    definition: OperatorDefinition,
    instance: Any,
    provider: ProviderRequest,
    client: Client,
  ) {
    val byName = definition.params.associateBy { it.name }
    val (known, unknown) = provider.kwargs.partition { it.name in byName }
    if (unknown.isNotEmpty()) {
      logger.info(
        "Ignoring arguments that the operator does not define",
        mapOf("operator" to definition.classpath, "arguments" to unknown.map { it.name }),
      )
    }
    val given = known.mapTo(mutableSetOf()) { it.name }
    definition.params.firstOrNull { it.required && it.name !in given }?.let {
      throw ProviderOperatorException("${definition.classpath} needs the argument '${it.name}', which the task lacks")
    }

    val toRender =
      known.filterIsInstance<ArgBinding.Literal>().filter { byName.getValue(it.name).templated }.associate { it.name to it.value }
    val rendered = if (toRender.isEmpty()) emptyMap() else client.impl.renderTemplateFields(toRender)
    for (binding in known) {
      val param = byName.getValue(binding.name)
      val value =
        when {
          binding is ArgBinding.XCom ->
            client.resolveBinding(binding)
              ?: throw ProviderOperatorException(
                "Argument '${binding.name}' takes the output of upstream task '${binding.taskId}', which pushed " +
                  "no return_value XCom; a task that returns nothing pushes none",
              )
          binding.name in toRender -> {
            if (binding.name !in rendered) {
              throw ProviderOperatorException(
                "The supervisor did not render the template field '${binding.name}' of ${definition.classpath}",
              )
            }
            rendered[binding.name]
          }
          else -> (binding as ArgBinding.Literal).value
        }
      assign(instance, param, value, definition)
    }
  }

  private fun assign(
    instance: Any,
    param: ParamDefinition,
    value: Any?,
    definition: OperatorDefinition,
  ) {
    // A null leaves the field as it is initialized, which is the operator's default, and the only
    // thing a primitive field can take.
    if (value == null) return
    val where = "Argument '${param.name}' of ${definition.classpath}"
    if (param.choices.isNotEmpty() && value !in param.choices) {
      throw ProviderOperatorException("$where is '$value', not one of ${param.choices.joinToString { "'$it'" }}")
    }
    val type = param.field.genericType
    val converted =
      try {
        when (type) {
          Duration::class.java -> Duration.ofNanos(((value as Number).toDouble() * NANOS_PER_SECOND).roundToLong())
          OffsetDateTime::class.java -> OffsetDateTime.parse(value as String)
          Instant::class.java -> OffsetDateTime.parse(value as String).toInstant()
          else -> ArgValues.decode(value, if (type is Class<*> && type.isPrimitive) type.kotlin.javaObjectType else type)
        }
      } catch (e: RuntimeException) {
        throw ProviderOperatorException("$where cannot be read as ${param.field.type.simpleName}: ${e.message}", e)
      }
    param.field.isAccessible = true
    param.field.set(instance, converted)
  }

  /** The operator's result as the plain JSON the supervisor accepts, or null when it has none. */
  private fun toJson(result: Any?): Any? =
    when (result) {
      null -> null
      is String, is Number, is Boolean, is Map<*, *>, is Collection<*> -> result
      else -> mapper.convertValue(result, Any::class.java)
    }

  private const val NANOS_PER_SECOND = 1e9
}
