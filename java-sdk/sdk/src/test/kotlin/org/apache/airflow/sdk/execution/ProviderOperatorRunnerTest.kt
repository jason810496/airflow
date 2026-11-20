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

import org.apache.airflow.sdk.Bundle
import org.apache.airflow.sdk.Client
import org.apache.airflow.sdk.execution.comm.ConnectionResult
import org.apache.airflow.sdk.execution.comm.StartupDetails
import org.apache.airflow.sdk.execution.comm.SucceedTask
import org.apache.airflow.sdk.execution.comm.TIRunContext
import org.apache.airflow.sdk.execution.comm.TaskInstance
import org.apache.airflow.sdk.execution.comm.TaskState
import org.apache.airflow.sdk.execution.comm.VariableResult
import org.apache.airflow.sdk.execution.comm.XComResult
import org.apache.airflow.sdk.provider.Operator
import org.apache.airflow.sdk.provider.OperatorContext
import org.apache.airflow.sdk.provider.Param
import org.apache.airflow.sdk.provider.ProviderOperator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.OffsetDateTime
import java.util.UUID

private const val MODULE = "airflow.providers.demo.operators.demo"

/** Reads everything it is given, and skips the downstream tasks when told to. */
@ProviderOperator(module = MODULE, name = "EchoOperator", canSkipDownstream = true)
class EchoOperator : Operator<Map<String, Any?>> {
  @JvmField
  @field:Param(required = true, templated = true)
  var message: String = ""

  @JvmField @field:Param
  var repeat: Int = 1

  @JvmField @field:Param
  var timeout: Duration? = null

  @JvmField @field:Param
  var notBefore: OffsetDateTime? = null

  @JvmField
  @field:Param(choices = ["a", "b"])
  var mode: String = "a"

  @JvmField
  @field:Param(name = "tags")
  var labels: List<String> = emptyList()

  @JvmField @field:Param
  var skipAfter: Boolean = false

  override fun execute(context: OperatorContext): Map<String, Any?> {
    if (skipAfter) context.skipDownstream()
    return linkedMapOf(
      "message" to message,
      "repeat" to repeat,
      "timeout" to timeout?.seconds,
      "notBefore" to notBefore?.toString(),
      "mode" to mode,
      "tags" to labels,
      "task" to context.context.ti.taskId,
    )
  }
}

@ProviderOperator(module = MODULE, name = "VoidOperator")
class VoidOperator : Operator<Void?> {
  override fun execute(context: OperatorContext): Void? = null
}

@ProviderOperator(module = MODULE, name = "FailingOperator")
class FailingOperator : Operator<Void?> {
  override fun execute(context: OperatorContext): Void? = throw IllegalStateException("boom")
}

class NotAnOperator

/** Records what the runtime sends and serves canned values. */
private class RecordingClient(
  val xcoms: Map<String, Any?> = emptyMap(),
) : org.apache.airflow.sdk.execution.Client {
  val rendered = mutableListOf<Map<String, Any?>>()
  val pushed = mutableMapOf<String, Any>()
  val skipped = mutableListOf<List<String>>()

  override fun getConnection(id: String): ConnectionResult = throw NotImplementedError()

  override fun getVariable(key: String): VariableResult? = null

  override fun setVariable(
    key: String,
    value: String,
    description: String?,
  ) = throw NotImplementedError()

  override fun deleteVariable(key: String) = throw NotImplementedError()

  override fun getXCom(
    key: String,
    dagId: String,
    taskId: String,
    runId: String,
    mapIndex: Int?,
    includePriorDates: Boolean,
  ): XComResult =
    XComResult().also {
      it.key = key
      it.value = xcoms[taskId]
    }

  override fun setXCom(
    key: String,
    value: Any,
    dagId: String,
    taskId: String,
    runId: String,
    mapIndex: Int,
  ) {
    pushed[key] = value
  }

  override fun getTaskStateStore(
    tiId: UUID,
    key: String,
  ) = throw NotImplementedError()

  override fun setTaskStateStore(
    tiId: UUID,
    key: String,
    value: Any,
    expiresAt: OffsetDateTime?,
  ) = throw NotImplementedError()

  override fun deleteTaskStateStore(
    tiId: UUID,
    key: String,
  ) = throw NotImplementedError()

  override fun clearTaskStateStore(tiId: UUID) = throw NotImplementedError()

  override fun skipDownstreamTasks(taskIds: List<String>) {
    skipped += taskIds
  }

  override fun triggerDagRun(
    dagId: String,
    runId: String,
    logicalDate: OffsetDateTime?,
    runAfter: OffsetDateTime?,
    conf: Map<String, Any?>?,
    resetDagRun: Boolean,
    note: String?,
  ): Boolean = throw NotImplementedError()

  override fun getDagRunState(
    dagId: String,
    runId: String,
  ): String = throw NotImplementedError()

  override fun isDagPaused(dagId: String): Boolean = throw NotImplementedError()

  override fun renderTemplateFields(values: Map<String, Any?>): Map<String, Any?> {
    rendered += values
    return values.mapValues { (_, value) -> (value as? String)?.replace("{{ run_id }}", "run-1") ?: value }
  }
}

private fun literal(
  name: String,
  value: Any?,
) = mapOf("name" to name, "kind" to "literal", "value" to value)

private fun request(
  className: String,
  kwargs: List<Map<String, Any?>>,
  module: String? = "demo/org.apache.airflow.sdk.execution.$className",
  shouldRetry: Boolean = false,
  classpath: String = "$MODULE.$className",
): StartupDetails =
  StartupDetails().also { details ->
    details.ti =
      TaskInstance().also {
        it.dagId = "d"
        it.runId = "r"
        it.taskId = "echo"
        it.tryNumber = 1
      }
    details.tiContext =
      TIRunContext().also {
        it.dagRun =
          org.apache.airflow.sdk.execution.comm.DagRun().also { run ->
            run.dagId = "d"
            run.runId = "r"
          }
        it.shouldRetry = shouldRetry
        it.providerOperator =
          mapOf(
            "classpath" to classpath,
            "module" to module,
            "kwargs" to kwargs,
            "downstream_task_ids" to mapOf("echo" to listOf("after"), "after" to emptyList<String>()),
          )
      }
  }

class ProviderOperatorRunnerTest {
  private fun run(
    details: StartupDetails,
    transport: RecordingClient = RecordingClient(),
  ): Any = TaskRunner.runTask(Bundle(), details, Client(details, transport), ProviderLoader(null))

  private fun runOk(
    details: StartupDetails,
    transport: RecordingClient = RecordingClient(),
  ): Any =
    run(details, transport).also {
      if (it !is SucceedTask) {
        val logs = LogCapture.drain().map { message -> message.event + " " + message.arguments }
        throw AssertionError("The task did not succeed: " + it + "; logs: " + logs)
      }
    }

  @BeforeEach
  fun drainLogs() {
    LogCapture.drain()
  }

  @Test
  @DisplayName("Should set each field from its argument and push the result as the return value")
  fun shouldRunTheOperator() {
    val transport = RecordingClient()
    val details =
      request(
        "EchoOperator",
        listOf(
          literal("message", "run {{ run_id }}"),
          literal("repeat", 3),
          literal("timeout", 90),
          literal("not_before", "2026-01-02T03:04:05Z"),
          literal("mode", "b"),
          literal("tags", listOf("x", "y")),
          literal("skip_after", false),
        ),
      )

    val result = runOk(details, transport)

    assertInstanceOf(SucceedTask::class.java, result)
    assertEquals(setOf("message"), transport.rendered.single().keys, "only the template fields are rendered")
    assertEquals(
      mapOf(
        "message" to "run run-1",
        "repeat" to 3,
        "timeout" to 90L,
        "notBefore" to "2026-01-02T03:04:05Z",
        "mode" to "b",
        "tags" to listOf("x", "y"),
        "task" to "echo",
      ),
      transport.pushed["return_value"],
    )
  }

  @Test
  @DisplayName("Should pull the result of an upstream task and leave it unrendered")
  fun shouldPullAnUpstreamResult() {
    val transport = RecordingClient(mapOf("build" to "{{ run_id }} as it is"))
    val details = request("EchoOperator", listOf(mapOf("name" to "message", "kind" to "xcom", "task_id" to "build")))

    val result = run(details, transport)

    assertInstanceOf(SucceedTask::class.java, result)
    assertTrue(transport.rendered.isEmpty())
    assertEquals("{{ run_id }} as it is", (transport.pushed["return_value"] as Map<*, *>)["message"])
  }

  @Test
  @DisplayName("Should skip the direct downstream tasks the operator names")
  fun shouldSkipDownstream() {
    val transport = RecordingClient()

    run(request("EchoOperator", listOf(literal("message", "m"), literal("skip_after", true))), transport)

    assertEquals(listOf(listOf("after")), transport.skipped)
    assertEquals(mapOf("skipped" to listOf("after")), transport.pushed["skipmixin_key"])
  }

  @Test
  @DisplayName("Should push nothing for an operator with no result")
  fun shouldPushNothingForNoResult() {
    val transport = RecordingClient()

    val result = run(request("VoidOperator", emptyList()), transport)

    assertInstanceOf(SucceedTask::class.java, result)
    assertTrue(transport.pushed.isEmpty())
  }

  @Test
  @DisplayName("Should ignore an argument the operator does not define")
  fun shouldIgnoreUnknownArguments() {
    val result = run(request("EchoOperator", listOf(literal("message", "m"), literal("queue", "java"))))

    assertInstanceOf(SucceedTask::class.java, result)
    assertTrue(LogCapture.drain().any { it.event == "Ignoring arguments that the operator does not define" })
  }

  @Test
  @DisplayName("Should fail without a retry when the operator cannot run here")
  fun shouldFailWithoutARetry() {
    val missing = run(request("EchoOperator", emptyList(), shouldRetry = true))
    val choice = run(request("EchoOperator", listOf(literal("message", "m"), literal("mode", "c")), shouldRetry = true))
    val type = run(request("EchoOperator", listOf(literal("message", "m"), literal("repeat", "many")), shouldRetry = true))
    val other = run(request("EchoOperator", listOf(literal("message", "m")), classpath = "$MODULE.Other", shouldRetry = true))
    val notAnOperator =
      run(
        request(
          "EchoOperator",
          listOf(literal("message", "m")),
          module = "demo/org.apache.airflow.sdk.execution.NotAnOperator",
          shouldRetry = true,
        ),
      )
    val absent = run(request("EchoOperator", listOf(literal("message", "m")), module = "demo/org.example.Absent", shouldRetry = true))

    listOf(missing, choice, type, other, notAnOperator, absent).forEach {
      assertEquals(TaskState.State.FAILED, (it as TaskState).state)
    }
    val events = LogCapture.drain().map { it.event }
    assertEquals("$MODULE.EchoOperator needs the argument 'message', which the task lacks", events[0])
    assertTrue(events[1].startsWith("Argument 'mode' of $MODULE.EchoOperator is 'c', not one of 'a', 'b'"), events[1])
    assertTrue(events[2].contains("Argument 'repeat'") && events[2].contains("cannot be read as"), events[2])
    assertTrue(events[3].contains("implements $MODULE.EchoOperator, not $MODULE.Other"), events[3])
    assertTrue(events[4].contains("is not annotated with @ProviderOperator"), events[4])
    assertTrue(events[5].contains("it is not on the classpath of the Dag"), events[5])
  }

  @Test
  @DisplayName("Should name the queue setting when a Python operator reaches the Java runtime")
  fun shouldNameTheQueueForAPythonOperator() {
    val result = run(request("EchoOperator", emptyList(), module = null))

    assertEquals(TaskState.State.FAILED, (result as TaskState).state)
    val event = LogCapture.drain().single().event
    assertTrue(event.contains("is implemented in Python") && event.contains("[sdk] queue_to_coordinator"), event)
  }

  @Test
  @DisplayName("Should retry an operator that throws when the task has retries left")
  fun shouldRetryAFailingOperator() {
    val retry = run(request("FailingOperator", emptyList(), shouldRetry = true))
    val fail = run(request("FailingOperator", emptyList(), shouldRetry = false))

    assertEquals(org.apache.airflow.sdk.execution.comm.RetryTask::class.java, retry.javaClass)
    assertEquals(TaskState.State.FAILED, (fail as TaskState).state)
  }

  @Test
  @DisplayName("Should not look the task up in the bundle when the run context carries a provider operator")
  fun shouldNotAskTheBundle() {
    val details = request("VoidOperator", emptyList())

    assertNull(Bundle().taskDef("d", "echo"))
    assertInstanceOf(SucceedTask::class.java, run(details))
  }
}
