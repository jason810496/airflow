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

import { Dag, DagRegistry, serveDags, type TaskHandlerArgs } from "@apache-airflow/ts-sdk";
import { report, summarize } from "./taskflow.js";

const dag = new Dag("typescript_example");

/** What the Dag's `build_message(python_start(), "uk")` call binds. */
interface BuildMessageArgs {
  /** python_start's return value, pulled before this handler is called. */
  upstream: string;
  /** A literal from the Dag file. */
  country: string;
}

export async function buildMessage({
  client,
  upstream,
  country,
}: TaskHandlerArgs & BuildMessageArgs) {
  const greeting = await client.getVariable("typescript_example_greeting");
  const message = `${greeting ?? "hello from TypeScript"} (${country}); upstream=${upstream}`;

  await client.setXCom({ key: "typescript_message", value: message });

  return { message, country };
}

/**
 * A call argument binds an upstream task's return value, so anything else — a
 * custom XCom key, another Dag's output — is still read through the client.
 */
export async function readMessage({ client }: TaskHandlerArgs) {
  const message = await client.getXCom<string>({
    key: "typescript_message",
    taskId: "build_message",
  });

  return { message: message ?? "missing" };
}

export async function readConnection({ client }: TaskHandlerArgs) {
  const connection = await client.getConnection("typescript_example_http");

  return {
    id: connection?.id ?? null,
    type: connection?.type ?? null,
    host: connection?.host ?? null,
    login: connection?.login ?? null,
    hasPassword: connection?.password != null,
  };
}

dag.task("build_message", buildMessage);
dag.task("read_message", readMessage);
dag.task("read_connection", readConnection);

// A second Dag in the same bundle, dedicated to argument binding: `summarize`
// types its arguments flat, `report` declares one interface. See taskflow.ts.
const taskflowDag = new Dag("typescript_taskflow_example");
taskflowDag.task("summarize", summarize);
taskflowDag.task("report", report);

await serveDags(new DagRegistry(dag, taskflowDag));
