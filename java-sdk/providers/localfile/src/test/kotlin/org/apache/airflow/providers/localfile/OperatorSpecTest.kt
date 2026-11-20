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

import org.apache.airflow.sdk.provider.SpecGenerator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.io.File

class OperatorSpecTest {
  @Test
  @DisplayName("Should match the committed spec, which the Python wrapper and the other SDKs are generated from")
  fun shouldMatchTheCommittedSpec() {
    val classesDirs = System.getProperty("airflow.operatorClassesDirs").split(File.pathSeparator)
    val specFile = System.getProperty("airflow.operatorSpecFile")
    val args =
      classesDirs.flatMap { listOf("--classes", it) } +
        listOf(
          "--provider-id",
          "java.localfile",
          "--package",
          "apache-airflow-providers-java-localfile",
          "--version",
          "1.0.0",
          "--provider-dir",
          "java-localfile",
          "--output",
          specFile,
          "--check",
        )

    // 0 means the committed file is what the operators generate; if not, run the generateOperatorSpec task.
    assertEquals(0, SpecGenerator.run(args.toTypedArray()))
  }
}
