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

// Finds the implementation of a provider operator that no bundle declares: imports the module
// `ti_context.provider_operator` names and takes the export named after the operator's class. The
// provider runner runs from the `node_modules` directory that holds the provider package, so the
// package resolves from here and shares this copy of the SDK.

import type { TIProviderOperator } from "../generated/supervisor.js";
import {
  getProviderImplementation,
  isProviderOperatorBinding,
  type ProviderImplementation,
} from "../sdk/provider-operator.js";

/** The class name of a Python class path: `airflow.providers.x.WriteOperator` is `WriteOperator`. */
function getClassName(classpath: string): string {
  return classpath.slice(classpath.lastIndexOf(".") + 1);
}

/**
 * Import the implementation of `provider`, or throw why it cannot be used.
 *
 * The export has to be a binding that this copy of the SDK built, with an `execute`, for the same
 * class path that Airflow sent.
 */
export async function importProviderImplementation(
  provider: TIProviderOperator,
): Promise<ProviderImplementation> {
  const { classpath, module } = provider;
  if (!module) {
    throw new Error(
      `${classpath} is not declared by a bundle, and Airflow names no module that implements it; ` +
        "regenerate the Python wrapper of its provider, or pack the operator into the bundle of the Dag",
    );
  }
  let imported: Record<string, unknown>;
  try {
    imported = (await import(module)) as Record<string, unknown>;
  } catch (err) {
    const reason = err instanceof Error ? err.message : String(err);
    throw new Error(
      `Cannot import "${module}" to run ${classpath}: ${reason}. Install the provider package in the ` +
        "node_modules directory the provider runner runs from",
      { cause: err },
    );
  }
  const className = getClassName(classpath);
  const exported = imported[className];
  if (!isProviderOperatorBinding(exported)) {
    throw new Error(`"${module}" does not export a provider operator named "${className}"`);
  }
  const found = getProviderImplementation(exported);
  if (found === undefined) {
    throw new Error(
      `"${module}" exports ${className} built by another copy of @apache-airflow/ts-sdk; install the ` +
        "provider package beside the SDK of the provider runner, so that they share one copy",
    );
  }
  if (found.implementation === null) {
    throw new Error(`"${module}" exports ${className}, which has no execute to run`);
  }
  if (found.classpath !== classpath) {
    throw new Error(
      `"${module}" exports ${className}, which implements ${found.classpath}, not ${classpath}`,
    );
  }
  return found.implementation;
}
