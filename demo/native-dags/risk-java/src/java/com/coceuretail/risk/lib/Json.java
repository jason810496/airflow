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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A small JSON reader and writer for the lake files, so the bundle needs nothing beyond the SDK.
 * Objects read as {@code LinkedHashMap}, arrays as {@code ArrayList} and numbers as {@code BigDecimal}.
 */
public final class Json {
  private final String text;
  private int pos;

  private Json(String text) {
    this.text = text;
  }

  public static Object parse(String text) {
    var parser = new Json(text);
    var value = parser.value();
    parser.skipWhitespace();
    if (parser.pos != text.length()) {
      throw parser.error("unexpected trailing content");
    }
    return value;
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Object> parseObject(String text) {
    return (Map<String, Object>) parse(text);
  }

  public static String write(Object value) {
    var out = new StringBuilder();
    write(out, value, 0);
    return out.append('\n').toString();
  }

  private Object value() {
    skipWhitespace();
    if (pos >= text.length()) {
      throw error("unexpected end of input");
    }
    char c = text.charAt(pos);
    switch (c) {
      case '{':
        return object();
      case '[':
        return array();
      case '"':
        return string();
      case 't':
        return literal("true", Boolean.TRUE);
      case 'f':
        return literal("false", Boolean.FALSE);
      case 'n':
        return literal("null", null);
      default:
        return number();
    }
  }

  private Map<String, Object> object() {
    var map = new LinkedHashMap<String, Object>();
    pos++;
    skipWhitespace();
    if (peek() == '}') {
      pos++;
      return map;
    }
    while (true) {
      skipWhitespace();
      var key = string();
      skipWhitespace();
      expect(':');
      map.put(key, value());
      skipWhitespace();
      if (peek() == ',') {
        pos++;
      } else {
        expect('}');
        return map;
      }
    }
  }

  private List<Object> array() {
    var list = new ArrayList<Object>();
    pos++;
    skipWhitespace();
    if (peek() == ']') {
      pos++;
      return list;
    }
    while (true) {
      list.add(value());
      skipWhitespace();
      if (peek() == ',') {
        pos++;
      } else {
        expect(']');
        return list;
      }
    }
  }

  private String string() {
    expect('"');
    var out = new StringBuilder();
    while (true) {
      if (pos >= text.length()) {
        throw error("unterminated string");
      }
      char c = text.charAt(pos++);
      if (c == '"') {
        return out.toString();
      }
      if (c != '\\') {
        out.append(c);
        continue;
      }
      char escaped = text.charAt(pos++);
      switch (escaped) {
        case 'n':
          out.append('\n');
          break;
        case 't':
          out.append('\t');
          break;
        case 'r':
          out.append('\r');
          break;
        case 'b':
          out.append('\b');
          break;
        case 'f':
          out.append('\f');
          break;
        case 'u':
          out.append((char) Integer.parseInt(text.substring(pos, pos + 4), 16));
          pos += 4;
          break;
        default:
          out.append(escaped);
      }
    }
  }

  private Object literal(String word, Object value) {
    if (!text.startsWith(word, pos)) {
      throw error("expected " + word);
    }
    pos += word.length();
    return value;
  }

  private BigDecimal number() {
    int start = pos;
    while (pos < text.length() && "+-0123456789.eE".indexOf(text.charAt(pos)) >= 0) {
      pos++;
    }
    if (start == pos) {
      throw error("unexpected character '" + text.charAt(pos) + "'");
    }
    return new BigDecimal(text.substring(start, pos));
  }

  private void skipWhitespace() {
    while (pos < text.length() && Character.isWhitespace(text.charAt(pos))) {
      pos++;
    }
  }

  private char peek() {
    return pos < text.length() ? text.charAt(pos) : '\0';
  }

  private void expect(char c) {
    if (peek() != c) {
      throw error("expected '" + c + "'");
    }
    pos++;
  }

  private IllegalArgumentException error(String message) {
    return new IllegalArgumentException("Invalid JSON at offset " + pos + ": " + message);
  }

  private static void write(StringBuilder out, Object value, int depth) {
    if (value == null) {
      out.append("null");
    } else if (value instanceof String) {
      quote(out, (String) value);
    } else if (value instanceof BigDecimal) {
      out.append(((BigDecimal) value).toPlainString());
    } else if (value instanceof Double || value instanceof Float) {
      out.append(BigDecimal.valueOf(((Number) value).doubleValue()).toPlainString());
    } else if (value instanceof Number || value instanceof Boolean) {
      out.append(value);
    } else if (value instanceof Map) {
      writeObject(out, (Map<?, ?>) value, depth);
    } else if (value instanceof Iterable) {
      writeArray(out, (Iterable<?>) value, depth);
    } else {
      throw new IllegalArgumentException("Cannot write " + value.getClass().getName() + " as JSON");
    }
  }

  private static void writeObject(StringBuilder out, Map<?, ?> map, int depth) {
    if (map.isEmpty()) {
      out.append("{}");
      return;
    }
    out.append("{\n");
    var first = true;
    for (var entry : map.entrySet()) {
      if (!first) {
        out.append(",\n");
      }
      first = false;
      indent(out, depth + 1);
      quote(out, String.valueOf(entry.getKey()));
      out.append(": ");
      write(out, entry.getValue(), depth + 1);
    }
    out.append('\n');
    indent(out, depth);
    out.append('}');
  }

  private static void writeArray(StringBuilder out, Iterable<?> items, int depth) {
    if (!items.iterator().hasNext()) {
      out.append("[]");
      return;
    }
    out.append("[\n");
    var first = true;
    for (var item : items) {
      if (!first) {
        out.append(",\n");
      }
      first = false;
      indent(out, depth + 1);
      write(out, item, depth + 1);
    }
    out.append('\n');
    indent(out, depth);
    out.append(']');
  }

  private static void indent(StringBuilder out, int depth) {
    for (int i = 0; i < depth; i++) {
      out.append("  ");
    }
  }

  private static void quote(StringBuilder out, String s) {
    out.append('"');
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '"':
          out.append("\\\"");
          break;
        case '\\':
          out.append("\\\\");
          break;
        case '\n':
          out.append("\\n");
          break;
        case '\r':
          out.append("\\r");
          break;
        case '\t':
          out.append("\\t");
          break;
        default:
          if (c < 0x20) {
            out.append(String.format("\\u%04x", (int) c));
          } else {
            out.append(c);
          }
      }
    }
    out.append('"');
  }
}
