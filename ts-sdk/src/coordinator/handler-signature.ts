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

// The argument names a handler declares, read off its own source.
//
// TypeScript's parameter types are erased, so the only declaration of what a
// handler expects that survives to run time is the destructuring pattern it was
// written with. Reading it is what lets the runtime compare the Dag's call
// against the handler before the task runs, rather than inferring it afterwards
// from what the handler happened to touch.
//
// Bundlers keep the pattern intact: property keys are part of the object's
// shape, so `({ regionCode })` minifies to `({ regionCode: a })` and the key is
// still there.

import { unwrapArgNames } from "../sdk/arg-names.js";

/** What a handler declares, or `null` when its source does not say. */
export interface DeclaredArgs {
  /** Names destructured from the first parameter, in source order. */
  readonly names: readonly string[];
  /** Whether the pattern ends in `...rest`, which takes whatever is left. */
  readonly takesRest: boolean;
}

/**
 * The arguments `handler` destructures from its first parameter.
 *
 * Returns `null` when the handler takes the whole object instead of
 * destructuring it, or declares no parameter at all: it has then narrowed
 * nothing, so there is no declaration to compare a call against.
 */
export function declaredArgs(handler: unknown): DeclaredArgs | null {
  const fn = unwrapArgNames(handler);
  if (typeof fn !== "function") return null;
  let source: string;
  try {
    source = Function.prototype.toString.call(fn);
  } catch {
    // A bound or native function has no readable source.
    return null;
  }
  const pattern = firstParamPattern(source);
  if (pattern === null) return null;
  return readPattern(pattern);
}

/**
 * The text inside the braces of a first parameter written as `{ ... }`, or
 * `null` for any other shape.
 */
function firstParamPattern(source: string): string | null {
  const open = source.indexOf("(");
  const brace = source.indexOf("{");
  // `async x => ...` has no parameter parens at all, and a `{` that comes
  // before them is the pattern of an arrow like `({a}) => ...` only when the
  // parens open first. Anything else is the body, or an undestructured name.
  if (brace === -1) return null;
  if (open !== -1 && open < brace) {
    // Between the parens and the first brace there must be nothing but space:
    // `(args, extra) => { ... }` opens its body brace, not a pattern.
    if (source.slice(open + 1, brace).trim() !== "") return null;
  } else {
    return null;
  }
  const close = matchingBrace(source, brace);
  return close === -1 ? null : source.slice(brace + 1, close);
}

/** Index of the `}` closing the `{` at `start`, respecting nesting and strings. */
function matchingBrace(source: string, start: number): number {
  let depth = 0;
  for (let i = start; i < source.length; i++) {
    const ch = source[i];
    if (ch === '"' || ch === "'" || ch === "`") {
      i = skipString(source, i);
      continue;
    }
    if (ch === "{") depth++;
    else if (ch === "}") {
      depth--;
      if (depth === 0) return i;
    }
  }
  return -1;
}

/** Index of the quote closing the one at `start`, or the end of the source. */
function skipString(source: string, start: number): number {
  const quote = source[start];
  for (let i = start + 1; i < source.length; i++) {
    if (source[i] === "\\") {
      i++;
      continue;
    }
    if (source[i] === quote) return i;
  }
  return source.length;
}

/** The declared names in one destructuring pattern's inner text. */
function readPattern(pattern: string): DeclaredArgs {
  const names: string[] = [];
  let takesRest = false;
  for (const part of splitTopLevel(pattern)) {
    const entry = part.trim();
    if (entry === "") continue;
    if (entry.startsWith("...")) {
      takesRest = true;
      continue;
    }
    // A computed key resolves at run time, so its name is not in the source.
    if (entry.startsWith("[")) continue;
    const name = entry.split(/[:=]/, 1)[0]?.trim();
    // Quoted keys (`"run-label": x`) carry the wire name inside the quotes.
    if (name) names.push(name.replace(/^["'`]|["'`]$/g, ""));
  }
  return { names, takesRest };
}

/** Split on commas that are not inside nested braces, brackets, parens or strings. */
function splitTopLevel(pattern: string): string[] {
  const parts: string[] = [];
  let depth = 0;
  let from = 0;
  for (let i = 0; i < pattern.length; i++) {
    const ch = pattern[i];
    if (ch === '"' || ch === "'" || ch === "`") {
      i = skipString(pattern, i);
      continue;
    }
    if (ch === "{" || ch === "[" || ch === "(") depth++;
    else if (ch === "}" || ch === "]" || ch === ")") depth--;
    else if (ch === "," && depth === 0) {
      parts.push(pattern.slice(from, i));
      from = i + 1;
    }
  }
  parts.push(pattern.slice(from));
  return parts;
}
