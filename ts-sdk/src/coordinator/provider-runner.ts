#!/usr/bin/env node
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

// Runs the tasks of provider operators that a provider package implements, for a Dag that no bundle
// declares. It serves an empty bundle, so the runtime imports the operator's package from the
// `ti_context.provider_operator` the task starts with. See
// airflow-core/adr/lang-sdk/0018-provider-operators-implemented-in-lang-sdks.md.
//
// The coordinator runs it from the `node_modules` directory it is configured with:
//
//     node node_modules/@apache-airflow/ts-sdk/dist/coordinator/provider-runner.js --comm=host:port --logs=host:port
//
// A provider package installed in the same `node_modules` resolves from here, and shares this copy of
// the SDK.

import { Bundle } from "../sdk/bundle.js";

try {
  await new Bundle().serve();
} catch (error) {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
}
