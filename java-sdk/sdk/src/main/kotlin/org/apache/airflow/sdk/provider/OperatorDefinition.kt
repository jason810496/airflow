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

import java.lang.reflect.Field
import java.lang.reflect.Modifier

/** One constructor argument of an operator: a field marked with [Param]. */
internal class ParamDefinition(
  val field: Field,
  /** The argument's name in the Python constructor. */
  val name: String,
  val required: Boolean,
  val templated: Boolean,
  val description: String,
  val choices: List<String>,
)

/** What a [ProviderOperator] class declares, read once from its annotations. */
internal class OperatorDefinition(
  val type: Class<*>,
  val module: String,
  val name: String,
  val description: String,
  val canSkipDownstream: Boolean,
  val params: List<ParamDefinition>,
) {
  /** The Python class path, which Airflow knows the operator by. */
  val classpath: String get() = "$module.$name"
}

/** `existOk` is `exist_ok`. A name that this does not reach is given to [Param.name]. */
internal fun toSnakeCase(name: String): String =
  buildString {
    name.forEachIndexed { index, char ->
      if (char.isUpperCase()) {
        if (index > 0) append('_')
        append(char.lowercaseChar())
      } else {
        append(char)
      }
    }
  }

/**
 * Reads what [type] declares as an operator.
 *
 * @throws IllegalArgumentException if [type] is not an [Operator] that carries [ProviderOperator],
 *    or cannot be built and filled by the runtime.
 */
internal fun readOperatorDefinition(type: Class<*>): OperatorDefinition {
  val annotation =
    requireNotNull(type.getAnnotation(ProviderOperator::class.java)) {
      "${type.name} is not annotated with @ProviderOperator"
    }
  require(Operator::class.java.isAssignableFrom(type)) {
    "${type.name} carries @ProviderOperator, so it has to implement org.apache.airflow.sdk.provider.Operator"
  }
  require(!type.isInterface && !Modifier.isAbstract(type.modifiers)) { "${type.name} has to be a concrete class" }
  require(annotation.module.isNotBlank()) { "@ProviderOperator on ${type.name} needs the Python module of the class" }
  try {
    type.getDeclaredConstructor()
  } catch (e: NoSuchMethodException) {
    throw IllegalArgumentException("${type.name} needs a no-argument constructor", e)
  }

  val fields = generateSequence(type) { it.superclass }.toList().reversed().flatMap { it.declaredFields.toList() }
  val params =
    fields.filter { it.isAnnotationPresent(Param::class.java) }.map { field ->
      require(!Modifier.isStatic(field.modifiers) && !Modifier.isFinal(field.modifiers)) {
        "@Param field '${field.name}' of ${type.name} must not be static or final, so that the runtime can set it"
      }
      val param = field.getAnnotation(Param::class.java)
      ParamDefinition(
        field,
        param.name.ifBlank { toSnakeCase(field.name) },
        param.required,
        param.templated,
        param.description,
        param.choices.toList(),
      )
    }
  params.groupBy { it.name }.entries.firstOrNull { it.value.size > 1 }?.let { (name, same) ->
    throw IllegalArgumentException(
      "Fields ${same.joinToString { "'${it.field.name}'" }} of ${type.name} are all the argument '$name'; " +
        "name one of them with @Param(name = ...)",
    )
  }
  return OperatorDefinition(
    type,
    annotation.module,
    annotation.name.ifBlank { type.simpleName },
    annotation.description,
    annotation.canSkipDownstream,
    params,
  )
}
