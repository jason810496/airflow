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

// The shared lake: where each team writes its batches, and the outbox for customer-facing files.
//
//   <lake>/<team>/<batch_id>/<file>.json
//
// Teams hand work to each other by Dag run plus a Variable that names the batch directory.

import { createHash } from "node:crypto";
import { mkdir, readFile, readdir, rename, writeFile } from "node:fs/promises";
import path from "node:path";

import { getContext } from "apache-airflow-ts-sdk";

export const TEAM = "storefront";
export const LATEST_BATCH_VARIABLE = "handoff.storefront.latest_batch";

export function lakeRoot(): string {
  return process.env["COCEU_LAKE_ROOT"] ?? "/files/demo/lake";
}

export function outboxRoot(): string {
  return process.env["COCEU_OUTBOX_ROOT"] ?? "/files/demo/outbox";
}

export interface Batch {
  batchId: string;
  /** The day the batch covers, as `YYYY-MM-DD`. */
  businessDate: string;
  dir: string;
}

/** `scheduled__2026-10-07T00:00:00+00:00` becomes `scheduled__2026-10-07T00-00-00`. */
export function batchIdFromRunId(runId: string): string {
  return runId
    .replace(/\+00:00$/, "")
    .replace(/[^A-Za-z0-9_.-]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

export function batchFor(runId: string): Batch {
  const batchId = batchIdFromRunId(runId);
  const businessDate =
    /\d{4}-\d{2}-\d{2}/.exec(runId)?.[0] ?? new Date().toISOString().slice(0, 10);
  return { batchId, businessDate, dir: path.join(lakeRoot(), TEAM, batchId) };
}

/** The batch of the task that is running now. */
export function currentBatch(): Batch {
  return batchFor(getContext().runId);
}

export interface WrittenFile {
  path: string;
  bytes: number;
  sha256: string;
}

export function sha256Of(content: string | Buffer): string {
  return createHash("sha256").update(content).digest("hex");
}

/** Writes through a temporary file, so a reader never sees half a file. */
export async function writeJson(dir: string, name: string, value: unknown): Promise<WrittenFile> {
  const content = `${JSON.stringify(value, null, 2)}\n`;
  return writeText(dir, name, content);
}

export async function writeText(dir: string, name: string, content: string): Promise<WrittenFile> {
  await mkdir(dir, { recursive: true });
  const target = path.join(dir, name);
  const staging = `${target}.tmp`;
  await writeFile(staging, content);
  await rename(staging, target);
  return { path: target, bytes: Buffer.byteLength(content), sha256: sha256Of(content) };
}

export async function readJson<T>(file: string): Promise<T> {
  return JSON.parse(await readFile(file, "utf-8")) as T;
}

/** Regular files directly inside `dir`, sorted by name. */
export async function listFiles(dir: string): Promise<string[]> {
  const entries = await readdir(dir, { withFileTypes: true });
  return entries
    .filter((entry) => entry.isFile() && !entry.name.endsWith(".tmp"))
    .map((entry) => entry.name)
    .sort();
}

export async function fileDigest(file: string): Promise<WrittenFile> {
  const content = await readFile(file);
  return { path: file, bytes: content.length, sha256: sha256Of(content) };
}

/** Writes `_manifest.json`, listing every other file in the batch with its size and sha256. */
export async function writeManifest(batch: Batch, producedBy: string) {
  const names = (await listFiles(batch.dir)).filter((name) => name !== "_manifest.json");
  const files = await Promise.all(
    names.map(async (name) => {
      const { bytes, sha256 } = await fileDigest(path.join(batch.dir, name));
      return { name, bytes, sha256 };
    }),
  );
  await writeJson(batch.dir, "_manifest.json", {
    team: TEAM,
    batch_id: batch.batchId,
    business_date: batch.businessDate,
    produced_by: producedBy,
    files,
  });
  return files;
}
