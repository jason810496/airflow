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

import org.apache.airflow.sdk.Bundle
import org.apache.airflow.sdk.Client
import org.apache.airflow.sdk.Context
import org.apache.airflow.sdk.DagDef
import org.apache.airflow.sdk.Task
import org.apache.airflow.sdk.TaskRef
import org.apache.airflow.sdk.execution.serializeDag
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.time.OffsetDateTime

private val NOTIFY =
  OperatorInfo
    .of("airflow.providers.demo.operators.notify", "NotifyOperator")
    .params("subject", "channel", "response_timeout", "not_before", "extra")
    .required("subject")
    .templated("subject")
    .uiColor("#e6f1f2")
    .templateExt(".txt")

/** What a generated wrapper is: the arguments of one operator, typed. */
private class NotifyBuilder : OperatorBuilder<NotifyBuilder, Any>(NOTIFY) {
  fun subject(value: String) = param("subject", value)

  fun subject(value: TaskRef<*>) = param("subject", value)

  fun channel(value: String) = param("channel", value)

  fun responseTimeout(value: Duration) = param("response_timeout", value)

  fun notBefore(value: Any) = param("not_before", value)

  fun extra(value: Map<String, Any?>) = param("extra", value)

  fun unknown() = param("missing", "x")
}

private class Report : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

private fun tasksOf(dag: DagDef): Map<String, Map<String, Any?>> {
  Bundle().register(dag)
  val serialized = serializeDag(dag, "/bundles/app.jar", "app.jar")

  @Suppress("UNCHECKED_CAST")
  val tasks = serialized["tasks"] as List<Map<String, Any?>>

  @Suppress("UNCHECKED_CAST")
  return tasks.associate { task ->
    val data = task["__var"] as Map<String, Any?>
    data["task_id"] as String to data
  }
}

class OperatorTaskTest {
  @Test
  @DisplayName("Should serialize the task as the Python operator class with its constructor arguments")
  fun shouldSerializeAsPythonClass() {
    val dag = DagDef("d")
    dag
      .task(
        "notify",
        NotifyBuilder()
          .extra(mapOf("tags" to listOf("a", "b")))
          .notBefore(OffsetDateTime.parse("2026-01-02T03:04:05Z"))
          .responseTimeout(Duration.ofSeconds(90))
          .channel("ops")
          .subject("Run {{ ds }}")
          .build(),
      ).config("retries", 2)
      .config("retry_delay", Duration.ofSeconds(30))
      .config("pool", "notifications")

    val task = tasksOf(dag).getValue("notify")

    assertEquals("NotifyOperator", task["task_type"])
    assertEquals("airflow.providers.demo.operators.notify", task["_task_module"])
    assertEquals("java", task["language"])
    assertEquals(true, task["is_stub"])
    assertEquals("provider_operator", task["stub_kind"])
    assertEquals(listOf("subject"), task["template_fields"])
    assertEquals(listOf(".txt"), task["template_ext"])
    assertEquals("#e6f1f2", task["ui_color"])
    assertFalse("_operator_module" in task)
    assertEquals(2, task["retries"])
    val duration = mapOf("type" to "number", "format" to "duration-seconds")
    // In the order the operator declares its parameters, then the task options in the order of the schema.
    assertEquals(
      listOf(
        mapOf("name" to "subject", "kind" to "literal", "value" to "Run {{ ds }}"),
        mapOf("name" to "channel", "kind" to "literal", "value" to "ops"),
        mapOf("name" to "response_timeout", "kind" to "literal", "value" to 90L, "value_schema" to duration),
        mapOf(
          "name" to "not_before",
          "kind" to "literal",
          "value" to "2026-01-02T03:04:05Z",
          "value_schema" to mapOf("type" to "string", "format" to "date-time"),
        ),
        mapOf("name" to "extra", "kind" to "literal", "value" to mapOf("tags" to listOf("a", "b"))),
        mapOf("name" to "retries", "kind" to "literal", "value" to 2),
        mapOf("name" to "pool", "kind" to "literal", "value" to "notifications"),
        mapOf("name" to "retry_delay", "kind" to "literal", "value" to 30L, "value_schema" to duration),
      ),
      task["_operator_kwargs"],
    )
  }

  @Test
  @DisplayName("Should write an Instant as a date-time and a fractional duration as seconds")
  fun shouldWriteInstantAndFractionalDuration() {
    val dag = DagDef("d")
    dag.task(
      "notify",
      NotifyBuilder()
        .subject("s")
        .notBefore(Instant.parse("2026-01-02T03:04:05Z"))
        .responseTimeout(Duration.ofMillis(1500))
        .build(),
    )

    val kwargs = tasksOf(dag).getValue("notify")["_operator_kwargs"] as List<*>

    assertEquals(
      mapOf(
        "name" to "response_timeout",
        "kind" to "literal",
        "value" to 1.5,
        "value_schema" to mapOf("type" to "number", "format" to "duration-seconds"),
      ),
      kwargs[1],
    )
    assertEquals("2026-01-02T03:04:05Z", (kwargs[2] as Map<*, *>)["value"])
  }

  @Test
  @DisplayName("Should bind the result of a task as an xcom argument and make it an upstream")
  fun shouldBindTaskResultAsXCom() {
    val dag = DagDef("d")
    val report = dag.task<String>("build_report", Report::class.java)
    dag.task("notify", NotifyBuilder().subject(report).build())

    val tasks = tasksOf(dag)

    assertEquals(
      listOf(mapOf("name" to "subject", "kind" to "xcom", "task_id" to "build_report")),
      tasks.getValue("notify")["_operator_kwargs"],
    )
    assertEquals(listOf("notify"), tasks.getValue("build_report")["downstream_task_ids"])
    assertEquals("dag_definition", tasks.getValue("build_report")["stub_kind"])
  }

  @Test
  @DisplayName("Should take the Dag's queue unless the task sets its own, and omit the default queue")
  fun shouldTakeTheDagQueue() {
    val dag = DagDef("d").config("queue", "java")
    dag.task("inherits", NotifyBuilder().subject("s").build())
    dag.task("python", NotifyBuilder().subject("s").build()).config("queue", "default")

    val tasks = tasksOf(dag)

    assertEquals("java", tasks.getValue("inherits")["queue"])
    assertEquals(
      listOf(mapOf("name" to "queue", "kind" to "literal", "value" to "java")),
      (tasks.getValue("inherits")["_operator_kwargs"] as List<*>).drop(1),
    )
    assertFalse("queue" in tasks.getValue("python"))
    assertEquals(1, (tasks.getValue("python")["_operator_kwargs"] as List<*>).size)
  }

  @Test
  @DisplayName("Should name the implementing module, and mark an operator that can skip downstream tasks")
  fun shouldNameTheImplementingModule() {
    val info =
      OperatorInfo
        .of("airflow.providers.java.localfile.operators.file", "HasFileOperator")
        .params("path")
        .implementationModule("java-localfile/org.apache.airflow.providers.localfile.HasFileOperator")
        .canSkipDownstream()
        .sensor()
    val dag = DagDef("d")
    dag.task("has_file", OperatorTask<Any>(info, mapOf("path" to "/tmp/a")))

    val task = tasksOf(dag).getValue("has_file")

    assertEquals("java-localfile/org.apache.airflow.providers.localfile.HasFileOperator", task["_operator_module"])
    assertEquals(true, task["_can_skip_downstream"])
    assertEquals(true, task["_is_sensor"])
  }

  @Test
  @DisplayName("Should reject what a wrapper cannot write")
  fun shouldRejectWhatCannotBeWritten() {
    assertEquals(
      "NotifyOperator has no argument 'missing'",
      assertThrows(IllegalArgumentException::class.java) { NotifyBuilder().unknown() }.message,
    )
    assertEquals(
      "NotifyOperator needs the argument(s) 'subject'",
      assertThrows(IllegalStateException::class.java) { NotifyBuilder().channel("c").build() }.message,
    )
    val dag = DagDef("d")
    val report = dag.task<String>("build_report", Report::class.java)
    val taskRef =
      assertThrows(IllegalArgumentException::class.java) { OperatorTask.useOperator<Any>("a.b.C", mapOf("x" to listOf(report))) }
    assertTrue(taskRef.message!!.contains("inside a list or a map"), taskRef.message)
    val notTemplated = assertThrows(IllegalArgumentException::class.java) { NotifyBuilderChannel().channel(report) }
    assertTrue(notTemplated.message!!.contains("not a templated argument"), notTemplated.message)
    val notJson = assertThrows(IllegalArgumentException::class.java) { NotifyBuilder().notBefore(Object()) }
    assertTrue(notJson.message!!.contains("JSON cannot carry"), notJson.message)
    assertThrows(IllegalArgumentException::class.java) { NotifyBuilder().notBefore(Double.NaN) }
    assertThrows(IllegalArgumentException::class.java) { NotifyBuilder().extra(mapOf("a" to Duration.ofSeconds(1))) }
  }

  @Test
  @DisplayName("Should reach an operator with no wrapper through useOperator")
  fun shouldUseAnOperatorWithoutAWrapper() {
    val dag = DagDef("d")
    val report = dag.task<String>("build_report", Report::class.java)
    dag.task(
      "bash",
      OperatorTask.useOperator<Any>(
        "airflow.providers.standard.operators.bash.BashOperator",
        linkedMapOf("bash_command" to report, "env" to mapOf("A" to "1")),
      ),
    )

    val task = tasksOf(dag).getValue("bash")

    assertEquals("BashOperator", task["task_type"])
    assertEquals("airflow.providers.standard.operators.bash", task["_task_module"])
    assertEquals(
      listOf(
        mapOf("name" to "bash_command", "kind" to "xcom", "task_id" to "build_report"),
        mapOf("name" to "env", "kind" to "literal", "value" to mapOf("A" to "1")),
      ),
      task["_operator_kwargs"],
    )
    assertThrows(IllegalArgumentException::class.java) { OperatorTask.useOperator<Any>("BashOperator", emptyMap()) }
  }

  @Test
  @DisplayName("Should keep a provider task from being a case of a switch")
  fun shouldNotBeASwitchCase() {
    val dag = DagDef("d")
    val pick = dag.Switch("pick", PickNothing::class.java)
    val operator = dag.task("notify", NotifyBuilder().subject("s").build())

    val error = assertThrows(IllegalArgumentException::class.java) { pick.Case(operator) }

    assertTrue(error.message!!.contains("runs a provider operator"), error.message)
  }
}

private class NotifyBuilderChannel : OperatorBuilder<NotifyBuilderChannel, Any>(NOTIFY) {
  fun channel(value: TaskRef<*>) = param("channel", value)
}

private class PickNothing : org.apache.airflow.sdk.SwitchTask {
  override fun choose(
    context: Context,
    client: Client,
  ): Class<out Task> = Report::class.java
}
