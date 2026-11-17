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

// A bundle with the native Dag `ts_localfile`, which uses the operators of the `localfile` provider,
// implemented in TypeScript, and runs them from the bundle.
//
// A Python Dag that uses the generated `airflow.providers.localfile` wrapper on the `typescript`
// queue does not need this bundle. The Node coordinator runs it in the SDK's provider runner, from
// the `node_modules` directory it is configured with.
//
//     build_content -> create -> write -> has_file -> delete -> has_after_delete -> after_skip
//
// `has_after_delete` finds the file gone and skips its direct downstream task, `after_skip`.
//
// Pack it into a bundle the Node coordinator parses:
//
//     airflow-ts-pack src/localfile.ts --outfile dist/localfile.min.mjs

import { Bundle, Dag } from "@apache-airflow/ts-sdk";
import {
  CreateFileOperator,
  DeleteFileOperator,
  HasFileOperator,
  WriteFileOperator,
} from "@apache-airflow/providers-localfile/operators/file";

/** The queue the deployment's `[sdk] queue_to_coordinator` routes to the Node coordinator. */
const QUEUE = "typescript";
/** Rendered by the Python supervisor, as it renders a Python Dag's templates. */
const REPORT_PATH = "/tmp/airflow-localfile/ts_localfile/{{ run_id }}/report.txt";

const dag = new Dag("ts_localfile", { queue: QUEUE, tags: ["typescript", "providers", "localfile"] });

// The TypeScript tasks take the Dag's queue. A provider task does not, so each one names its own.
const buildContent = dag.task(
  "build_content",
  async (): Promise<string> => "localfile report\nnaïve café, 3 orders\n",
)();
const create = dag.task("create", CreateFileOperator({ path: REPORT_PATH }), { queue: QUEUE })();
const write = dag.task("write", WriteFileOperator({ path: REPORT_PATH, content: buildContent }), {
  queue: QUEUE,
})();
const hasFile = dag.task("has_file", HasFileOperator({ path: REPORT_PATH }), { queue: QUEUE })();
const remove = dag.task("delete", DeleteFileOperator({ path: REPORT_PATH }), { queue: QUEUE })();
const hasAfterDelete = dag.task(
  "has_after_delete",
  HasFileOperator({ path: REPORT_PATH, skipDownstreamIfMissing: true }),
  { queue: QUEUE },
)();
const afterSkip = dag.task("after_skip", async (): Promise<string> => "unreachable")();

create.after(buildContent);
write.after(create);
hasFile.after(write);
remove.after(hasFile);
hasAfterDelete.after(remove);
afterSkip.after(hasAfterDelete);

await new Bundle(dag).serve();
