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

package com.coceuretail.risk.lib;

import java.util.ArrayList;
import java.util.List;

/** Plain-text tables for the task log. */
public final class Table {
  private Table() {}

  public static String render(List<String> headers, List<List<String>> rows, int... rightAligned) {
    int columns = headers.size();
    int[] widths = new int[columns];
    for (int c = 0; c < columns; c++) {
      widths[c] = headers.get(c).length();
      for (var row : rows) {
        widths[c] = Math.max(widths[c], row.get(c).length());
      }
    }
    var lines = new ArrayList<String>();
    lines.add(format(headers, widths, rightAligned));
    var rule = new ArrayList<String>();
    for (int width : widths) {
      rule.add("-".repeat(width));
    }
    lines.add(String.join("  ", rule));
    for (var row : rows) {
      lines.add(format(row, widths, rightAligned));
    }
    return String.join("\n", lines);
  }

  private static String format(List<String> cells, int[] widths, int[] rightAligned) {
    var out = new ArrayList<String>();
    for (int c = 0; c < widths.length; c++) {
      boolean right = false;
      for (int r : rightAligned) {
        right |= r == c;
      }
      var cell = cells.get(c);
      var pad = " ".repeat(widths[c] - cell.length());
      out.add(right ? pad + cell : cell + pad);
    }
    return String.join("  ", out).replaceAll("\\s+$", "");
  }
}
