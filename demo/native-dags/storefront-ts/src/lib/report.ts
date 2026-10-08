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

// Plain-text tables for the task log.

export function renderTable(
  headers: readonly string[],
  rows: readonly (readonly string[])[],
  rightAligned: readonly number[] = [],
): string {
  const widths = headers.map((header, column) =>
    Math.max(header.length, ...rows.map((row) => (row[column] ?? "").length)),
  );
  const format = (cells: readonly string[]): string =>
    cells
      .map((cell, column) =>
        rightAligned.includes(column)
          ? cell.padStart(widths[column] ?? 0)
          : cell.padEnd(widths[column] ?? 0),
      )
      .join("  ")
      .trimEnd();
  const rule = widths.map((width) => "-".repeat(width)).join("  ");
  return [format(headers), rule, ...rows.map(format)].join("\n");
}
