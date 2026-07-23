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

/**
 * Reads an NDJSON response body incrementally, invoking `onLines` with the
 * complete (newline-terminated) non-empty lines of each received chunk.
 *
 * Aborting the signal cancels the underlying reader; callers only need to
 * abort their controller for cleanup.
 */
export const readNdjsonStream = async ({
  body,
  onLines,
  signal,
}: {
  body: ReadableStream<Uint8Array>;
  onLines: (lines: Array<string>) => void;
  signal: AbortSignal;
}): Promise<void> => {
  const reader = body.getReader();

  const cancelReader = () => {
    reader.cancel().catch(() => {
      // Ignore cancellation errors
    });
  };

  signal.addEventListener("abort", cancelReader, { once: true });
  if (signal.aborted) {
    // the abort predates listener registration; cancelling ends the read loop
    cancelReader();
  }

  try {
    const decoder = new TextDecoder();
    let buffer = "";

    // eslint-disable-next-line no-await-in-loop -- sequential reads required; each chunk depends on the previous buffer state
    for (let result = await reader.read(); !result.done; result = await reader.read()) {
      if (signal.aborted) {
        break;
      }

      buffer += decoder.decode(result.value, { stream: true });

      const lines = buffer.split("\n");

      buffer = lines.pop() ?? "";

      const completeLines = lines.filter((line) => line.trim());

      if (completeLines.length > 0) {
        onLines(completeLines);
      }
    }
  } finally {
    signal.removeEventListener("abort", cancelReader);
  }
};
