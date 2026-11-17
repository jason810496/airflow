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

import { spawn } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

import { SUPERVISOR_API_VERSION } from "../../src/coordinator/protocol.js";
import { Bundle } from "../../src/sdk/bundle.js";
import { drive, makeStartupDetails, requestsOf } from "./support/supervisor-driver.js";

const SDK_ROOT = fileURLToPath(new URL("../../", import.meta.url));
const RUNNER = fileURLToPath(new URL("../../src/coordinator/provider-runner.ts", import.meta.url));
const FIXTURE = fileURLToPath(new URL("./support/provider-fixture.ts", import.meta.url));

describe("the package manifest", () => {
  const manifest = JSON.parse(readFileSync(`${SDK_ROOT}package.json`, "utf8")) as {
    bin: Record<string, string>;
    airflow: { supervisorSchemaVersion: string };
  };

  it("names the supervisor schema version the coordinator reports for the provider runner", () => {
    expect(manifest.airflow.supervisorSchemaVersion).toBe(SUPERVISOR_API_VERSION);
  });

  it("ships the provider runner where the coordinator looks for it", () => {
    expect(manifest.bin["airflow-ts-provider-runner"]).toBe(
      "./dist/coordinator/provider-runner.js",
    );
  });
});

describe("the provider runner", () => {
  it("serves an empty bundle, so a provider task of a Python Dag runs from the imported module", async () => {
    const context = {
      provider_operator: {
        classpath: "airflow.providers.demo.operators.file.WriteOperator",
        module: FIXTURE,
        kwargs: [{ name: "path", kind: "literal", value: "/tmp/a" }],
        downstream_task_ids: { write: [] },
      },
    };

    const result = await drive(
      new Bundle(),
      makeStartupDetails("write", "py_dag", context),
      (type, body) =>
        type === "RenderTemplateFields"
          ? { body: { type: "RenderedTemplateFieldsResult", values: body["values"] } }
          : undefined,
      ({ commAddr, logsAddr }) =>
        new Promise<void>((resolve, reject) => {
          const child = spawn(
            process.execPath,
            ["--import", "tsx", RUNNER, `--comm=${commAddr}`, `--logs=${logsAddr}`],
            { cwd: SDK_ROOT, stdio: ["ignore", "inherit", "inherit"] },
          );
          child.on("error", reject);
          child.on("exit", (code) =>
            code === 0 ? resolve() : reject(new Error(`provider runner exited with ${code}`)),
          );
        }),
    );

    expect(result.response).toMatchObject({ type: "SucceedTask" });
    expect(requestsOf(result, "RenderTemplateFields")).toEqual([
      { type: "RenderTemplateFields", values: { path: "/tmp/a" } },
    ]);
    expect(requestsOf(result, "SetXCom")[0]).toMatchObject({
      key: "return_value",
      value: { bytes: 3 },
      dag_id: "py_dag",
    });
  });
});
