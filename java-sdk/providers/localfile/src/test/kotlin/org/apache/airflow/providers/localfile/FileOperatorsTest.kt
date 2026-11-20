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

package org.apache.airflow.providers.localfile

import io.mockk.every
import io.mockk.mockk
import org.apache.airflow.sdk.Client
import org.apache.airflow.sdk.Context
import org.apache.airflow.sdk.DagRun
import org.apache.airflow.sdk.TaskInstance
import org.apache.airflow.sdk.provider.OperatorContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest

class FileOperatorsTest {
  @TempDir
  lateinit var dir: Path

  private val client = mockk<Client>()

  private var skipped = 0

  private fun context() =
    OperatorContext(
      Context(DagRun("d", "r", null, null, null, null, null, emptyMap()), TaskInstance("d", "r", "t", null, 1)),
      client,
    ) { skipped++ }

  private fun sha256(text: String) = MessageDigest.getInstance("SHA-256").digest(text.toByteArray()).joinToString("") { "%02x".format(it) }

  @Test
  @DisplayName("Should create the file and its parent directories, and leave an existing file alone")
  fun shouldCreateAFile() {
    val file = dir.resolve("a/b/report.txt")
    val operator = CreateFileOperator().also { it.path = file.toString() }

    val created = operator.execute(context())
    Files.writeString(file, "kept")
    val again = operator.execute(context())

    assertEquals(true, created.created)
    assertEquals(false, again.created)
    assertEquals(file.toString(), again.path)
    assertEquals("kept", Files.readString(file))
  }

  @Test
  @DisplayName("Should fail on an existing file when existOk is false")
  fun shouldFailOnAnExistingFile() {
    val file = Files.createFile(dir.resolve("a.txt"))
    val operator =
      CreateFileOperator().also {
        it.path = file.toString()
        it.existOk = false
      }

    assertTrue(assertThrows(IOException::class.java) { operator.execute(context()) }.message!!.endsWith("exists already"))
  }

  @Test
  @DisplayName("Should write the text and describe the file as it is on disk")
  fun shouldWriteAFile() {
    val file = dir.resolve("out/report.txt")
    val text = "localfile report\nnaïve café, 3 orders\n"

    val overwritten =
      WriteFileOperator()
        .also {
          it.path = file.toString()
          it.content = "old"
        }.execute(context())
    val result =
      WriteFileOperator()
        .also {
          it.path = file.toString()
          it.content = text
        }.execute(context())

    assertEquals(3L, overwritten.bytes)
    assertEquals(text.toByteArray().size.toLong(), result.bytes)
    assertEquals(sha256(text), result.sha256)
    assertEquals(text, Files.readString(file))
  }

  @Test
  @DisplayName("Should append, and describe the whole file")
  fun shouldAppend() {
    val file = dir.resolve("log.txt")
    WriteFileOperator()
      .also {
        it.path = file.toString()
        it.content = "one\n"
      }.execute(context())

    val result =
      WriteFileOperator()
        .also {
          it.path = file.toString()
          it.content = "two\n"
          it.mode = "append"
        }.execute(context())

    assertEquals("one\ntwo\n", Files.readString(file))
    assertEquals(sha256("one\ntwo\n"), result.sha256)
  }

  @Test
  @DisplayName("Should encode the text as asked")
  fun shouldEncode() {
    val file = dir.resolve("latin.txt")

    WriteFileOperator()
      .also {
        it.path = file.toString()
        it.content = "café"
        it.encoding = "latin1"
      }.execute(context())

    assertEquals(4, Files.size(file).toInt())
  }

  @Test
  @DisplayName("Should say whether the file exists, and skip the downstream tasks only when asked and missing")
  fun shouldCheckForAFile() {
    val file = Files.createFile(dir.resolve("here.txt"))
    val missing = "$file.gone"

    assertTrue(
      HasFileOperator()
        .also {
          it.path = file.toString()
          it.skipDownstreamIfMissing = true
        }.execute(context()),
    )
    assertFalse(HasFileOperator().also { it.path = missing }.execute(context()))
    assertEquals(0, skipped)

    assertFalse(
      HasFileOperator()
        .also {
          it.path = missing
          it.skipDownstreamIfMissing = true
        }.execute(context()),
    )
    assertEquals(1, skipped)
  }

  @Test
  @DisplayName("Should delete the file, and fail on a missing one only when asked")
  fun shouldDeleteAFile() {
    val file = Files.createFile(dir.resolve("gone.txt"))
    val operator = DeleteFileOperator().also { it.path = file.toString() }

    assertTrue(operator.execute(context()).deleted)
    assertFalse(operator.execute(context()).deleted)
    operator.missingOk = false
    assertTrue(assertThrows(IOException::class.java) { operator.execute(context()) }.message!!.endsWith("is missing"))
    assertTrue(
      assertThrows(IOException::class.java) {
        DeleteFileOperator().also { it.path = dir.toString() }.execute(context())
      }.message!!.contains("directory"),
    )
  }

  @Test
  @DisplayName("Should write the value of an Airflow Variable, which it reads through the client")
  fun shouldWriteAVariable() {
    val file = dir.resolve("var/value.txt")
    every { client.getVariable("greeting") } returns "greetings from e2e"
    every { client.getVariable("unset") } returns null

    val result =
      WriteVariableOperator()
        .also {
          it.path = file.toString()
          it.key = "greeting"
        }.execute(context())

    assertEquals("greetings from e2e", Files.readString(file))
    assertEquals(sha256("greetings from e2e"), result.sha256)
    assertEquals(
      "The Airflow Variable 'unset' is not set",
      assertThrows(IllegalStateException::class.java) {
        WriteVariableOperator()
          .also {
            it.path = file.toString()
            it.key = "unset"
          }.execute(context())
      }.message,
    )
  }
}
