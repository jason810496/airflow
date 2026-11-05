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

// Task loops declared in TypeScript: a task group that runs again and again
// until a gate says stop. See ts-sdk/adr/0003-task-loop-authoring-interface.md.
//
// `refine` is Newton's method for the square root of two. Each iteration
// improves the previous estimate and measures its error, and the loop stops
// once the error is small enough, or at ten iterations. `poll` is a loop that
// only counts, so it needs no handler to decide.

import {
  Dag,
  getClient,
  getLoop,
  type LoopDecision,
  type XComSequence,
} from "@apache-airflow/ts-sdk";

type Metrics = { estimate: number; error: number };

export const dag = new Dag("typescript_loop_example", {
  schedule: "@daily",
  catchup: false,
  tags: ["typescript", "loop"],
  description: "Task loops declared in TypeScript: one decided by a handler, one by a count.",
  queue: "typescript",
});

// --- A loop with a gate ---------------------------------------------------

const refine = dag.taskGroup("refine");

const improve = refine.task(async function improve(): Promise<number> {
  const loop = await getLoop();
  console.log(`iteration ${loop.index} of ${loop.maxIterations}`);
  // The terminal task's result of the iteration before; null at the first.
  const estimate = loop.getPrevious<Metrics>()?.estimate ?? 1.0;
  return (estimate + 2.0 / estimate) / 2.0;
});

const evaluate = refine.task(async function evaluate({
  estimate,
}: {
  estimate: number;
}): Promise<Metrics> {
  return { estimate, error: Math.abs(estimate * estimate - 2) };
});

// The gate. It reads the terminal task as wired data, so the compiler checks
// it against what `evaluate` returns.
async function accurateEnough({ result }: { result: Metrics }): Promise<LoopDecision> {
  return { stop: result.error < 1e-6, reason: `error ${result.error.toExponential(1)}` };
}

const report = dag.task(async function report({ history }: { history: XComSequence<Metrics> }) {
  const iterations = await history.length();
  const last = await history.at(-1);
  console.log(`settled after ${iterations} iterations at ${last.estimate}`);
  return {
    iterations,
    estimate: last.estimate,
    errors: (await history.slice()).map((m) => m.error),
  };
});

const cleanup = dag.task(async function cleanup() {
  await getClient().setXCom({ key: "loop_finished", value: true });
});

const estimate = improve();
const evaluated = evaluate({ estimate });

const refinement = refine.loop(accurateEnough, { result: evaluated }, { maxIterations: 10 });

// `evaluated` is one iteration's Metrics inside the loop, and every
// iteration's Metrics from `report`, outside it.
report({ history: evaluated });
// Order only: after the whole loop, not after one iteration.
refinement.before(cleanup());

// --- A loop that only counts ---------------------------------------------

const poll = dag.taskGroup("poll");

const checkOnce = poll.task(async function checkOnce(): Promise<number> {
  return (await getLoop()).index;
});

checkOnce();
poll.loop({ maxIterations: 3 }).after(refinement);
