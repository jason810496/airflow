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

// Task mapping: what `expand`, `expandKwargs` and `partial` take, as ts-sdk ADR-0004 spells them.
//
// A source built with `map`, `zip` or `concat` is a tree of the references it reads. Airflow's
// scheduler decides how many instances it gives, and its Python sidecar picks each instance's
// element, so the SDK only records the tree and serializes it as Python's `expand_input` does.

import { brand, hasBrand } from "./brand.js";
import type { JsonValue } from "./client-types.js";
import type { Node, RETURN_TYPE, TaskRef } from "./dag.js";

/** Marks a mapped task's element type, as `RETURN_TYPE` marks a `TaskRef`'s value. */
declare const ELEMENT_TYPE: unique symbol;

/** A function `map` applies to one element. It may not be async. */
export type MapCallable<T, U> = (element: T) => U extends PromiseLike<unknown> ? never : U;

/**
 * A mapped task: one instance per element it expands over.
 *
 * It goes on edges and into `expand`. Passed to an argument typed `XComSequence<T>`, it is read
 * as a reduce: every instance's result, in map-index order.
 */
export interface MappedTaskRef<T = unknown> extends Node {
  readonly taskId: string;
  /** @internal Never set; the element type, one instance's return value. */
  readonly [ELEMENT_TYPE]?: T;
  before(...downstream: readonly Node[]): MappedTaskRef<T>;
  after(...upstream: readonly Node[]): MappedTaskRef<T>;
  map<U>(fn: MapCallable<T, U>): MapSource<U>;
  zip<const S extends readonly Expandable[]>(...others: S): ZipSource<[T, ...ElementsOf<S>]>;
  zip<const S extends readonly Expandable[], F extends JsonValue>(
    ...others: [...S, ZipOptions<F>]
  ): ZipSource<[T | F, ...ElementsOrFill<S, F>]>;
  concat(...others: readonly Expandable<T>[]): ConcatSource<T>;
}

/** What `map` returns: only `map` again, so a `map()` stays outermost. */
export interface MapSource<T> {
  /** @internal Never set. */
  readonly [ELEMENT_TYPE]?: T;
  map<U>(fn: MapCallable<T, U>): MapSource<U>;
}

/** What `zip` returns: only `map`, so a `zip()` never goes inside another. */
export interface ZipSource<T> {
  /** @internal Never set. */
  readonly [ELEMENT_TYPE]?: T;
  map<U>(fn: MapCallable<T, U>): MapSource<U>;
}

/** What `concat` returns. */
export interface ConcatSource<T> {
  /** @internal Never set. */
  readonly [ELEMENT_TYPE]?: T;
  map<U>(fn: MapCallable<T, U>): MapSource<U>;
  concat(...others: readonly Expandable<T>[]): ConcatSource<T>;
}

/** `padding` makes a `zip` as long as its longest source, standing in for the missing items. */
export interface ZipOptions<F extends JsonValue> {
  padding: F;
}

/** An array's element, or a record's `[key, value]` pair. */
export type ElementOf<R> = R extends readonly (infer E)[]
  ? E
  : R extends Readonly<Record<infer K extends string, infer V>>
    ? [K, V]
    : never;

/** What `zip` and `concat` take: something with elements of `T`. A `zip` or `map` result is not. */
export type Expandable<T = unknown> = TaskRef<readonly T[]> | MappedTaskRef<T> | ConcatSource<T>;

/**
 * Each `zip` source's element type, in order. Read off the brands rather than the reference types,
 * whose own `zip` would make this type refer to itself.
 */
export type ElementsOf<S extends readonly unknown[]> = {
  [I in keyof S]: S[I] extends { readonly [RETURN_TYPE]?: infer R }
    ? ElementOf<R>
    : S[I] extends { readonly [ELEMENT_TYPE]?: infer E }
      ? E
      : never;
};

/** Each `zip` source's element type, or the padding type where that source runs out. */
export type ElementsOrFill<S extends readonly unknown[], F> = {
  [I in keyof S]: ElementsOf<[S[I]]>[0] | F;
};

/**
 * A mapped task's whole output, read on demand, as Python's `LazyXComSequence` reads it.
 *
 * Elements are in map-index order, and an instance that returned `null` or nothing is not one of
 * them. It is valid only in the handler call that received it.
 */
export interface XComSequence<T> extends AsyncIterable<T> {
  length(): Promise<number>;
  /** A negative `i` counts from the end. Throws `RangeError` outside the sequence. */
  at(i: number): Promise<T>;
  slice(start?: number, stop?: number): Promise<T[]>;
}

/** Internal: the tree a source records, by what it reads. */
export type SourceNode =
  | { readonly kind: "ref"; readonly taskId: string }
  | {
      readonly kind: "map";
      readonly arg: SourceNode;
      readonly fns: readonly ((value: never) => unknown)[];
    }
  | {
      readonly kind: "zip";
      readonly args: readonly SourceNode[];
      readonly padding?: { value: JsonValue };
    }
  | { readonly kind: "concat"; readonly args: readonly SourceNode[] };

const SOURCE_BRAND = "ExpandSource";
const MAPPED_TASK_REF_BRAND = "MappedTaskRef";
const sourceNodes = new WeakMap<object, SourceNode>();

/** Internal: whether `value` is a mapped task's reference. */
export function isMappedTaskRef(value: unknown): value is MappedTaskRef {
  return hasBrand(value, MAPPED_TASK_REF_BRAND);
}

/** Internal: whether `value` is a source `map`, `zip` or `concat` built. */
export function isSource(value: unknown): boolean {
  return hasBrand(value, SOURCE_BRAND);
}

/** Internal: the tree of a source or a reference, or `undefined` for anything else. */
export function getSourceNode(value: unknown): SourceNode | undefined {
  if (typeof value !== "object" || value === null) return undefined;
  return sourceNodes.get(value);
}

/** Internal: brand a mapped task's reference, and give it the methods a source has. */
export function makeMappedTaskRef<T>(
  ref: Omit<MappedTaskRef<T>, "map" | "zip" | "concat">,
): MappedTaskRef<T> {
  const mapped = { ...ref } as MappedTaskRef<T>;
  addSourceMethods(mapped, { kind: "ref", taskId: ref.taskId });
  brand(mapped, MAPPED_TASK_REF_BRAND);
  return mapped;
}

/** Internal: give a `TaskRef` the `map`, `zip` and `concat` of a reference whose value has elements. */
export function addSourceMethods(target: object, node: SourceNode): void {
  sourceNodes.set(target, node);
  Object.assign(target, {
    map: (fn: (value: never) => unknown) => makeSource("map", mapNode(node, fn)),
    zip: (...others: unknown[]) => makeSource("zip", zipNode(node, others)),
    concat: (...others: unknown[]) => makeSource("concat", concatNode(node, others)),
  });
}

function makeSource(kind: "map" | "zip" | "concat", node: SourceNode): object {
  const source: Record<string, unknown> = {
    map: (fn: (value: never) => unknown) => makeSource("map", mapNode(node, fn)),
  };
  if (kind === "concat") {
    source["concat"] = (...others: unknown[]) => makeSource("concat", concatNode(node, others));
  }
  sourceNodes.set(source, node);
  brand(source, SOURCE_BRAND);
  return Object.freeze(source);
}

function mapNode(node: SourceNode, fn: unknown): SourceNode {
  if (typeof fn !== "function") {
    throw new TypeError(`map() takes a function of one element, not ${typeof fn}`);
  }
  if (fn.constructor?.name === "AsyncFunction") {
    throw new TypeError("map() takes a synchronous function; a map callable cannot be async");
  }
  const callable = fn as (value: never) => unknown;
  // Chained map() calls are one wrapper with several callables, applied in order.
  if (node.kind === "map") return { kind: "map", arg: node.arg, fns: [...node.fns, callable] };
  return { kind: "map", arg: node, fns: [callable] };
}

function zipNode(node: SourceNode, others: unknown[]): SourceNode {
  const last = others.at(-1);
  let padding: { value: JsonValue } | undefined;
  if (typeof last === "object" && last !== null && !getSourceNode(last) && "padding" in last) {
    others = others.slice(0, -1);
    const value = (last as { padding: unknown }).padding;
    if (value === undefined) {
      throw new TypeError(
        "zip() padding cannot be undefined, which has no JSON form; pad with null",
      );
    }
    padding = { value: value as JsonValue };
  }
  if (others.length === 0) throw new TypeError("zip() needs at least one other source to zip with");
  const args = [node, ...others.map((other) => expandableNode(other, "zip()"))];
  return padding === undefined ? { kind: "zip", args } : { kind: "zip", args, padding };
}

function concatNode(node: SourceNode, others: unknown[]): SourceNode {
  if (others.length === 0) throw new TypeError("concat() needs at least one other source");
  const head = node.kind === "concat" ? node.args : [node];
  return {
    kind: "concat",
    args: [...head, ...others.map((other) => expandableNode(other, "concat()"))],
  };
}

function expandableNode(value: unknown, method: string): SourceNode {
  const node = getSourceNode(value);
  if (node === undefined || node.kind === "map" || node.kind === "zip") {
    throw new TypeError(
      `${method} takes a task's reference or a concat() result, not a map() or zip() result; ` +
        "call map() last",
    );
  }
  return node;
}

/** Internal: the id of every task a source reads. */
export function getSourceTaskIds(node: SourceNode): string[] {
  switch (node.kind) {
    case "ref":
      return [node.taskId];
    case "map":
      return getSourceTaskIds(node.arg);
    case "zip":
    case "concat":
      return node.args.flatMap(getSourceTaskIds);
  }
}
