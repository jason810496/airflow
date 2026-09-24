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

import { describe, expect, it } from "vitest";

import { declaredArgs } from "../../src/coordinator/handler-signature.js";
import { withArgNames } from "../../src/sdk/arg-names.js";

describe("declaredArgs", () => {
  it("reads the names an arrow handler destructures", () => {
    const handler = async ({ regionCode, threshold }: { regionCode: string; threshold: number }) =>
      `${regionCode}${threshold}`;
    expect(declaredArgs(handler)).toEqual({
      names: ["regionCode", "threshold"],
      takesRest: false,
    });
  });

  it("reads them from a function declaration too", () => {
    async function report({ runLabel, transformed }: { runLabel: string; transformed: number }) {
      return `${runLabel}${transformed}`;
    }
    expect(declaredArgs(report)).toEqual({ names: ["runLabel", "transformed"], takesRest: false });
  });

  it("keeps a name that carries a destructuring default", () => {
    const handler = async ({ runId = "manual", region }: { runId?: string; region: string }) =>
      `${runId}${region}`;
    expect(declaredArgs(handler)).toEqual({ names: ["runId", "region"], takesRest: false });
  });

  it("is not confused by a default containing a comma or a brace", () => {
    const handler = async ({
      opts = { a: 1, b: 2 },
      label = "x,y",
    }: {
      opts?: { a: number; b: number };
      label?: string;
    }) => `${opts.a}${label}`;
    expect(declaredArgs(handler)).toEqual({ names: ["opts", "label"], takesRest: false });
  });

  it("takes the outer key of a nested pattern", () => {
    const handler = async ({ totals: { orders } }: { totals: { orders: number } }) => orders;
    expect(declaredArgs(handler)).toEqual({ names: ["totals"], takesRest: false });
  });

  it("reports a rest element, which claims whatever is left", () => {
    const handler = async ({ region, ...rest }: { region: string; [k: string]: unknown }) =>
      `${region}${Object.keys(rest).length}`;
    expect(declaredArgs(handler)).toEqual({ names: ["region"], takesRest: true });
  });

  it("sees through a withArgNames wrapper to the handler it wraps", () => {
    // The wrapper's own source is `(args) => handler(args)`, which declares nothing.
    interface ReportArgs {
      label: string;
      threshold: number;
    }
    const report = withArgNames(
      { label: "run_label" },
      async ({ label, threshold }: ReportArgs) => `${label}${threshold}`,
    );
    expect(declaredArgs(report)).toEqual({ names: ["label", "threshold"], takesRest: false });
  });

  it.each([
    ["a whole-object parameter", async (args: Record<string, unknown>) => Object.keys(args)],
    ["no parameter at all", async () => "nothing"],
    [
      "a second parameter",
      async (args: Record<string, unknown>, _extra?: unknown) => Object.keys(args),
    ],
  ])("declares nothing for %s", (_label, handler) => {
    expect(declaredArgs(handler)).toBeNull();
  });

  it("declares nothing for a value that is not a function", () => {
    expect(declaredArgs(null)).toBeNull();
    expect(declaredArgs({ regionCode: 1 })).toBeNull();
  });
});
