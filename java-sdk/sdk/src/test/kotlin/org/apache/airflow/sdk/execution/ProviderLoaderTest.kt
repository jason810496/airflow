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

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.io.FileOutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.jar.JarEntry
import java.util.jar.JarOutputStream
import javax.tools.ToolProvider

private const val OPERATOR_SOURCE = """
package demo;

import org.apache.airflow.sdk.provider.*;

@ProviderOperator(module = "airflow.providers.demo.operators.demo", name = "WhoOperator")
public class WhoOperator implements Operator<String> {
  public String execute(OperatorContext context) { return Helper.value(); }
}
"""

private fun helperSource(value: String) = "package demo; public class Helper { public static String value() { return \"$value\"; } }"

class ProviderLoaderTest {
  @TempDir
  lateinit var root: Path

  /** A provider directory with one JAR, holding an operator and its own copy of [Helper]. */
  private fun installProvider(
    directory: String,
    helperValue: String,
  ) {
    val sources = Files.createDirectories(root.resolve("src-$directory"))
    val classes = Files.createDirectories(root.resolve("classes-$directory"))
    val files =
      mapOf("WhoOperator" to OPERATOR_SOURCE, "Helper" to helperSource(helperValue)).map { (name, text) ->
        sources.resolve("$name.java").also { Files.writeString(it, text) }.toFile()
      }
    val compiler = ToolProvider.getSystemJavaCompiler()
    val status =
      compiler.run(
        null,
        null,
        null,
        "-cp",
        System.getProperty("java.class.path"),
        "-d",
        classes.toString(),
        *files
          .map {
            it.path
          }.toTypedArray(),
      )
    assertEquals(0, status, "the provider fixture compiles")
    val jarDir = Files.createDirectories(root.resolve("providers").resolve(directory))
    JarOutputStream(FileOutputStream(jarDir.resolve("$directory.jar").toFile())).use { jar ->
      classes.toFile().walkTopDown().filter { it.isFile }.forEach { file ->
        jar.putNextEntry(JarEntry(file.relativeTo(classes.toFile()).path.replace(File.separatorChar, '/')))
        jar.write(file.readBytes())
        jar.closeEntry()
      }
    }
  }

  private fun request(directory: String) =
    ProviderRequest("airflow.providers.demo.operators.demo.WhoOperator", "$directory/demo.WhoOperator", emptyList(), emptyMap())

  @Test
  @DisplayName("Should load each provider in a class loader of its own, under the SDK's loader")
  fun shouldIsolateProviders() {
    installProvider("first", "one")
    installProvider("second", "two")
    val loader = ProviderLoader(root.resolve("providers"))

    val first = loader.find(request("first"))
    val second = loader.find(request("second"))

    assertNotSame(first.type.classLoader, second.type.classLoader)
    assertSame(ProviderLoader::class.java.classLoader, first.type.classLoader.parent)
    assertSame(ProviderLoader::class.java.classLoader, second.type.classLoader.parent)
    // The SDK's classes are the ones the runtime itself uses, so the provider's Operator is ours.
    assertTrue(org.apache.airflow.sdk.provider.Operator::class.java.isAssignableFrom(first.type))
    assertEquals(
      "one",
      first.type
        .getMethod(
          "execute",
          org.apache.airflow.sdk.provider.OperatorContext::class.java,
        ).invoke(first.type.getDeclaredConstructor().newInstance(), null),
    )
    assertEquals(
      "two",
      second.type
        .getMethod(
          "execute",
          org.apache.airflow.sdk.provider.OperatorContext::class.java,
        ).invoke(second.type.getDeclaredConstructor().newInstance(), null),
    )
    assertSame(first.type, loader.find(request("first")).type, "a provider's loader is made once")
  }

  @Test
  @DisplayName("Should refuse a directory that is not a provider's")
  fun shouldRefuseOtherDirectories() {
    installProvider("first", "one")
    val loader = ProviderLoader(root.resolve("providers"))

    for (directory in listOf("sdk", "..", ".")) {
      val module = "$directory/demo.WhoOperator"
      val error =
        assertThrows(ProviderOperatorException::class.java) { loader.find(ProviderRequest("x.Y", module, emptyList(), emptyMap())) }
      assertTrue(
        error.message!!.contains("is not the directory of a provider") || error.message!!.contains("cannot be loaded"),
        error.message,
      )
    }
    val absent = assertThrows(ProviderOperatorException::class.java) { loader.find(request("absent")) }
    assertTrue(absent.message!!.contains("it is not in a JAR of 'absent'"), absent.message)
    val malformed =
      assertThrows(ProviderOperatorException::class.java) { loader.find(ProviderRequest("x.Y", "nodirectory", emptyList(), emptyMap())) }
    assertTrue(malformed.message!!.contains("<provider dir>/<class name>"), malformed.message)
  }
}
