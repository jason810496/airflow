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

// Writes this SDK's answer to a DagFileParseRequest over the shared fixtures,
// so Airflow's own deserializer can be pointed at it:
//
//   pnpm exec tsx tests/conformance/serialize_typescript.ts
//   uv run --project airflow-core python ts-sdk/tests/conformance/serialize_python.py \
//     --verify ts-sdk/tests/conformance/serialized_typescript.json
//
// conformance.test.ts already compares this output against Python's field by
// field; the round trip is the other half — it proves the scheduler can read
// what the comparison says is right. The output is not committed, because the
// test builds it in memory anyway.

import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { parseDags } from "../../src/coordinator/serde.js";
import { buildFixtureDags, FIXTURE_DAGS } from "./fixtures.js";
import { DagRegistry } from "../../src/sdk/registry.js";

const OUTPUT = fileURLToPath(new URL("./serialized_typescript.json", import.meta.url));

const registry = new DagRegistry(...buildFixtureDags(FIXTURE_DAGS));
const result = parseDags(registry, {
  file: "/bundles/app/bundle.mjs",
  bundle_path: "/bundles/app",
});

writeFileSync(OUTPUT, `${JSON.stringify(result, null, 2)}\n`);
process.stdout.write(`wrote ${OUTPUT} (${result.serialized_dags.length} Dags)\n`);
