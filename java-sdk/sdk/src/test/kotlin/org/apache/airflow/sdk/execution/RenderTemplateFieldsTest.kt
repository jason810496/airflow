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

import org.apache.airflow.sdk.execution.comm.XComResult
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.msgpack.core.MessagePack

private fun bytesOf(
  id: Int,
  body: Map<String, Any?>,
): ByteArray = Frame.encodeRequest(id, body).fold(ByteArray(0)) { acc, buffer -> acc + buffer.toByteArray() }

class RenderTemplateFieldsTest {
  @Test
  @DisplayName("Should send the request as the plain map it is, and keep the values it carries")
  fun shouldEncodeTheRequestAsAMap() {
    val values = mapOf("path" to "/tmp/{{ run_id }}", "count" to 3, "nested" to mapOf("a" to listOf("b", null)))

    val unpacker = MessagePack.newDefaultUnpacker(bytesOf(7, mapOf("type" to "RenderTemplateFields", "values" to values)))

    assertEquals(2, unpacker.unpackArrayHeader())
    assertEquals(7, unpacker.unpackInt())
    assertEquals(
      mapOf("type" to "RenderTemplateFields", "values" to values),
      unpacker
        .unpackValue()
        .asMapValue()
        .map()
        .entries
        .associate { (k, v) -> k.asStringValue().asString() to v.toPlain() },
    )
  }

  @Test
  @DisplayName("Should leave the rendered values as a map, which the generated model would drop")
  fun shouldDecodeTheReplyAsAMap() {
    val values = mapOf("path" to "/tmp/run-1", "count" to 3L)

    val frame = Frame.decode(bytesOf(7, mapOf("type" to "RenderedTemplateFieldsResult", "values" to values)))

    assertEquals(mapOf("type" to "RenderedTemplateFieldsResult", "values" to values), frame.body)
  }

  @Test
  @DisplayName("Should still decode every other reply into its model")
  fun shouldDecodeOtherRepliesIntoModels() {
    val frame = Frame.decode(bytesOf(7, mapOf("type" to "XComResult", "key" to "return_value", "value" to "x")))

    assertInstanceOf(XComResult::class.java, frame.body)
    assertEquals("x", (frame.body as XComResult).value)
  }
}

private fun org.msgpack.value.Value.toPlain(): Any? =
  when {
    isNilValue -> null
    isStringValue -> asStringValue().asString()
    isIntegerValue -> asIntegerValue().toInt()
    isArrayValue -> asArrayValue().list().map { it.toPlain() }
    isMapValue -> asMapValue().map().entries.associate { (k, v) -> k.asStringValue().asString() to v.toPlain() }
    else -> error("unexpected value $this")
  }
