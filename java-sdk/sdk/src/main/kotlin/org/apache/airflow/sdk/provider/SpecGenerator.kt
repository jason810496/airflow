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

import com.fasterxml.jackson.databind.ObjectMapper
import com.xenomachina.argparser.ArgParser
import java.io.File
import java.lang.reflect.GenericArrayType
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.time.Duration
import java.time.Instant
import java.time.OffsetDateTime
import kotlin.system.exitProcess

private class SpecArgs(
  parser: ArgParser,
) {
  val classes by parser.adding("--classes", help = "Directory of compiled classes to scan for operators") { File(this) }
  val providerId by parser.storing("--provider-id", help = "Provider id, such as java.localfile")
  val packageName by parser.storing("--package", help = "Python distribution of the provider")
  val version by parser.storing("--version", help = "Provider version")
  val providerDir by parser.storing("--provider-dir", help = "Directory of the provider under java_providers_path")
  val output by parser.storing("--output", help = "Spec file to write") { File(this) }
  val check by parser.flagging("--check", help = "Write nothing, and fail when the spec file is out of date")
}

/** The provider a spec describes. */
internal class SpecProvider(
  val id: String,
  val packageName: String,
  val version: String,
  /** Where the provider's JARs sit under `java_providers_path`. */
  val directory: String,
)

/**
 * @suppress
 *
 * Writes the spec of a provider implemented in Java, the file Airflow's other generators read to make
 * the wrappers in every language, at `providers/<provider>/_generated/lang_sdk_operators.json`. It
 * follows `airflow-core/src/airflow/lang_sdk_provider_operators.schema.json`.
 *
 * It reads the classes that carry [ProviderOperator]: the arguments from their [Param] fields, and
 * the defaults from what a new instance holds. Run by a Gradle task of the provider, not by hand.
 */
object SpecGenerator {
  @JvmStatic
  fun main(args: Array<String>) {
    val status = run(args)
    if (status != 0) exitProcess(status)
  }

  /** Runs the generator and returns the exit status: 1 when `--check` finds the spec file out of date. */
  @JvmStatic
  fun run(args: Array<String>): Int {
    val parsed = ArgParser(args).parseInto(::SpecArgs)
    val loader = Thread.currentThread().contextClassLoader ?: SpecGenerator::class.java.classLoader
    val definitions =
      parsed.classes
        .flatMap { root -> root.walkTopDown().filter { it.isFile && it.name.endsWith(".class") }.map { root to it } }
        .filter { (_, file) -> file.name != "module-info.class" }
        .map { (root, file) ->
          file
            .relativeTo(root)
            .path
            .removeSuffix(".class")
            .replace(File.separatorChar, '.')
        }.map { Class.forName(it, false, loader) }
        .filter { it.isAnnotationPresent(ProviderOperator::class.java) }
        .map(::readOperatorDefinition)
    val provider = SpecProvider(parsed.providerId, parsed.packageName, parsed.version, parsed.providerDir)
    val text = buildSpec(provider, definitions)
    if (parsed.check) {
      if (!parsed.output.isFile || parsed.output.readText() != text) {
        System.err.println(
          "${parsed.output} is out of date. Re-run the generateOperatorSpec task of the provider's Gradle project.",
        )
        return 1
      }
      return 0
    }
    parsed.output.absoluteFile.parentFile
      ?.mkdirs()
    parsed.output.writeText(text)
    return 0
  }
}

private val json = ObjectMapper()

/** The spec of [definitions] as the text of the spec file. */
internal fun buildSpec(
  provider: SpecProvider,
  definitions: List<OperatorDefinition>,
): String {
  val spec =
    linkedMapOf<String, Any?>(
      "spec_version" to "1.0",
      "provider" to linkedMapOf("id" to provider.id, "package" to provider.packageName, "version" to provider.version),
      "implemented_in" to listOf("Java"),
      "operators" to definitions.sortedBy { it.name }.map { specOperator(it, provider) },
      "skipped" to emptyList<Any>(),
    )
  return StringBuilder().also { writeValue(it, spec, 0, inline = false) }.append('\n').toString()
}

private fun specOperator(
  definition: OperatorDefinition,
  provider: SpecProvider,
): Map<String, Any?> {
  val instance =
    definition.type
      .getDeclaredConstructor()
      .apply { isAccessible = true }
      .newInstance()
  val operator =
    linkedMapOf<String, Any?>(
      "name" to definition.name,
      "module" to definition.module,
      "implementation_module" to "${provider.directory}/${definition.type.name}",
      "kind" to "operator",
    )
  if (definition.description.isNotBlank()) operator["description"] = definition.description
  operator["template_fields"] = definition.params.filter { it.templated }.map { it.name }
  if (definition.canSkipDownstream) operator["can_skip_downstream"] = true
  operator["params"] = definition.params.map { specParam(definition, it, instance) }
  return operator
}

private fun specParam(
  definition: OperatorDefinition,
  param: ParamDefinition,
  instance: Any,
): Map<String, Any?> {
  val spec = linkedMapOf<String, Any?>("name" to param.name)
  spec["type"] =
    try {
      specType(param.field.genericType, param.choices)
    } catch (e: IllegalArgumentException) {
      throw IllegalArgumentException("${definition.type.name}.${param.field.name}: ${e.message}", e)
    }
  if (param.required) spec["required"] = true
  if (param.templated) spec["templated"] = true
  if (!param.required) {
    param.field.isAccessible = true
    specDefault(param.field.get(instance))?.let { spec["default"] = it }
  }
  if (param.description.isNotBlank()) spec["description"] = param.description
  return spec
}

/** A default a spec can carry: JSON that is not null. A duration or a date has none. */
private fun specDefault(value: Any?): Any? =
  when (value) {
    is String, is Boolean, is Number -> value
    is Collection<*> -> value.map { specDefault(it) ?: return null }
    else -> null
  }

private fun specType(
  type: Type,
  choices: List<String>,
): Map<String, Any?> {
  fun kind(name: String) = linkedMapOf<String, Any?>("kind" to name)

  if (type is ParameterizedType) {
    val raw = type.rawType as Class<*>
    return when {
      Map::class.java.isAssignableFrom(raw) -> {
        require(type.actualTypeArguments[0] == String::class.java) { "a map needs String keys" }
        kind("object").also { it["values"] = specType(type.actualTypeArguments[1], emptyList()) }
      }
      Collection::class.java.isAssignableFrom(raw) ->
        kind("array").also { it["items"] = specType(type.actualTypeArguments[0], emptyList()) }
      else -> throw IllegalArgumentException("${type.typeName} has no JSON form")
    }
  }
  if (type is GenericArrayType) return kind("array").also { it["items"] = specType(type.genericComponentType, emptyList()) }
  require(type is Class<*>) { "${type.typeName} has no JSON form" }
  return when {
    type == String::class.java ->
      if (choices.isEmpty()) kind("string") else kind("literal").also { it["literals"] = choices }
    type == java.lang.Boolean.TYPE || type == java.lang.Boolean::class.java -> kind("boolean")
    type in INTEGERS -> kind("integer")
    type in NUMBERS -> kind("number")
    type == Duration::class.java -> kind("timedelta")
    type == OffsetDateTime::class.java || type == Instant::class.java -> kind("datetime")
    type == Any::class.java -> kind("any")
    Map::class.java.isAssignableFrom(type) || Collection::class.java.isAssignableFrom(type) ->
      throw IllegalArgumentException("${type.name} needs its type arguments, as in List<String>")
    type.isArray -> kind("array").also { it["items"] = specType(type.componentType, emptyList()) }
    else -> throw IllegalArgumentException(
      "${type.name} has no JSON form; use String, a number, boolean, Duration, OffsetDateTime, List, Map or Object",
    )
  }
}

private val INTEGERS: Set<Class<*>> =
  setOf(
    java.lang.Byte.TYPE,
    java.lang.Byte::class.java,
    java.lang.Short.TYPE,
    java.lang.Short::class.java,
    java.lang.Integer.TYPE,
    java.lang.Integer::class.java,
    java.lang.Long.TYPE,
    java.lang.Long::class.java,
  )

private val NUMBERS: Set<Class<*>> =
  setOf(java.lang.Float.TYPE, java.lang.Float::class.java, java.lang.Double.TYPE, java.lang.Double::class.java)

/**
 * Writes [value] the way the other SDKs write their specs: two-space indentation, one item to a
 * line, except that an argument and its type are one line each.
 */
private fun writeValue(
  out: StringBuilder,
  value: Any?,
  depth: Int,
  inline: Boolean,
) {
  val pad = "  ".repeat(depth + 1)
  when (value) {
    is Map<*, *> ->
      if (value.isEmpty()) {
        out.append("{}")
      } else if (inline) {
        out.append(value.entries.joinToString(", ", "{", "}") { (k, v) -> "${json.writeValueAsString(k)}: ${inlineText(v)}" })
      } else {
        out.append("{\n")
        out.append(
          value.entries.joinToString(",\n") { (k, v) ->
            StringBuilder(pad).append(json.writeValueAsString(k)).append(": ").also {
              writeValue(it, v, depth + 1, inline = k == "type")
            }
          },
        )
        out.append("\n").append("  ".repeat(depth)).append("}")
      }
    is List<*> ->
      if (value.isEmpty()) {
        out.append("[]")
      } else if (inline) {
        out.append(value.joinToString(", ", "[", "]") { inlineText(it) })
      } else {
        out.append("[\n")
        out.append(
          value.joinToString(",\n") { item ->
            StringBuilder(pad).also { writeValue(it, item, depth + 1, inline = item is Map<*, *> && "type" in item) }
          },
        )
        out.append("\n").append("  ".repeat(depth)).append("]")
      }
    else -> out.append(json.writeValueAsString(value))
  }
}

private fun inlineText(value: Any?): String = StringBuilder().also { writeValue(it, value, 0, inline = true) }.toString()
