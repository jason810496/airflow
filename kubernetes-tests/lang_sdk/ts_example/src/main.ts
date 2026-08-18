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

import {
  Dag,
  DagRegistry,
  serveDags,
  type TaskHandlerArgs,
} from "@apache-airflow/ts-sdk";

// Must match the dag_id of the Python stub Dag and the Go/Java bundles.
const dag = new Dag("lang_sdk_combined");

/** What the Dag's `ts_build_message(python_task_1(), "uk")` call binds. */
interface BuildMessageArgs {
  /** python_task_1's return value, pulled from its XCom before this handler is called. */
  upstream: string;
  /** A literal from the Dag file. */
  country: string;
}

/**
 * Return both bound arguments so the system test can assert on them.
 *
 * The return value is the task's `return_value` XCom, which is the only
 * evidence of what the handler was called with that outlives the pod.
 */
export async function tsBuildMessage({
  upstream,
  country,
}: TaskHandlerArgs & BuildMessageArgs) {
  return { upstream, country, message: `${upstream} (${country})` };
}

dag.task("ts_build_message", tsBuildMessage);

await serveDags(new DagRegistry(dag));
