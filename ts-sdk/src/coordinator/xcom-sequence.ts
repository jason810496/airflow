/*!
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

// A reduce: a mapped task's whole output, read on demand through the supervisor, which decides
// which rows are live and in what order (ADR-0016, *Reduce*).

import type { CoordinatorClient } from "./client.js";
import type { JsonValue } from "../sdk/client-types.js";
import type { XComSequence } from "../sdk/mapping.js";

/** Internal: the `XComSequence` a `sequence` binding hands a handler. Nothing is read until asked. */
export function makeXComSequence(
  client: CoordinatorClient,
  taskId: string,
): XComSequence<JsonValue> {
  let length: number | undefined;
  const getLength = async (): Promise<number> => (length ??= await client.getXComCount(taskId));
  const sequence: XComSequence<JsonValue> = {
    length: getLength,
    async at(i: number): Promise<JsonValue> {
      if (!Number.isInteger(i))
        throw new TypeError(`An XComSequence index has to be an integer, not ${i}`);
      const size = await getLength();
      const offset = i < 0 ? size + i : i;
      if (offset < 0 || offset >= size) {
        throw new RangeError(`Index ${i} is outside the ${size} elements of task "${taskId}"`);
      }
      return client.getXComSequenceItem(taskId, offset);
    },
    async slice(start?: number, stop?: number): Promise<JsonValue[]> {
      return client.getXComSequenceSlice(taskId, start ?? null, stop ?? null);
    },
    async *[Symbol.asyncIterator]() {
      const size = await getLength();
      for (let offset = 0; offset < size; offset++) {
        yield await client.getXComSequenceItem(taskId, offset);
      }
    },
  };
  return Object.freeze(sequence);
}
