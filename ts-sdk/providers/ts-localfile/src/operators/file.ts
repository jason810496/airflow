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

// Implements the operators of `apache-airflow-providers-ts-localfile`. A task of one of them runs in
// the Node runtime, whichever language the Dag is written in. See
// airflow-core/adr/lang-sdk/0018-provider-operators-implemented-in-lang-sdks.md.

import { createHash } from "node:crypto";
import { appendFile, mkdir, open, readFile, stat, unlink, writeFile } from "node:fs/promises";
import { dirname } from "node:path";

import { defineProviderOperator, type Templated } from "@apache-airflow/ts-sdk/provider";

function hasCode(error: unknown, code: string): boolean {
  return error instanceof Error && (error as NodeJS.ErrnoException).code === code;
}

/** Arguments of {@link CreateFileOperator}. */
export interface CreateFileOperatorArgs {
  /** The file to create. Missing parent directories are created too. */
  readonly path: Templated<string>;
  /** Succeed when the file exists already. When false, an existing file fails the task. @default true */
  readonly existOk?: boolean;
}

/** What {@link CreateFileOperator} returns. */
export interface CreateFileResult {
  readonly path: string;
  /** False when the file existed already. */
  readonly created: boolean;
}

/**
 * Create an empty file.
 *
 * An existing file is left as it is.
 */
export const CreateFileOperator = defineProviderOperator<CreateFileOperatorArgs, CreateFileResult>({
  module: "airflow.providers.ts.localfile.operators.file",
  className: "CreateFileOperator",
  implementationModule: "@apache-airflow/providers-ts-localfile/operators/file",
  templateFields: ["path"],
  params: {
    path: { name: "path", required: true },
    existOk: { name: "exist_ok" },
  },
  async execute({ path, existOk = true }): Promise<CreateFileResult> {
    await mkdir(dirname(path), { recursive: true });
    try {
      const handle = await open(path, "wx");
      await handle.close();
      return { path, created: true };
    } catch (error) {
      if (hasCode(error, "EEXIST") && existOk) return { path, created: false };
      if (hasCode(error, "EEXIST")) throw new Error(`${path} exists already`, { cause: error });
      throw error;
    }
  },
});

/** Arguments of {@link WriteFileOperator}. */
export interface WriteFileOperatorArgs {
  /** The file to write. Missing parent directories are created. */
  readonly path: Templated<string>;
  /** The text to write. */
  readonly content: Templated<string>;
  /** Replace what the file holds, or add to its end. @default `"overwrite"` */
  readonly mode?: "overwrite" | "append";
  /** How the text is encoded in the file. @default `"utf-8"` */
  readonly encoding?: "utf-8" | "ascii" | "latin1";
}

/** What {@link WriteFileOperator} returns, read from the file after the write. */
export interface WriteFileResult {
  readonly path: string;
  /** The size of the whole file. */
  readonly bytes: number;
  /** The SHA-256 of the whole file, in hex. */
  readonly sha256: string;
}

const NODE_ENCODINGS = { "utf-8": "utf8", ascii: "ascii", latin1: "latin1" } as const;

/**
 * Write text to a file.
 *
 * The result describes the file as it is on disk after the write.
 */
export const WriteFileOperator = defineProviderOperator<WriteFileOperatorArgs, WriteFileResult>({
  module: "airflow.providers.ts.localfile.operators.file",
  className: "WriteFileOperator",
  implementationModule: "@apache-airflow/providers-ts-localfile/operators/file",
  templateFields: ["path", "content"],
  params: {
    path: { name: "path", required: true },
    content: { name: "content", required: true },
    mode: { name: "mode" },
    encoding: { name: "encoding" },
  },
  async execute({
    path,
    content,
    mode = "overwrite",
    encoding = "utf-8",
  }): Promise<WriteFileResult> {
    const data = Buffer.from(content, NODE_ENCODINGS[encoding]);
    await mkdir(dirname(path), { recursive: true });
    if (mode === "append") await appendFile(path, data);
    else await writeFile(path, data);
    const written = await readFile(path);
    return {
      path,
      bytes: written.length,
      sha256: createHash("sha256").update(written).digest("hex"),
    };
  },
});

/** Arguments of {@link HasFileOperator}. */
export interface HasFileOperatorArgs {
  /** The path to look for. */
  readonly path: Templated<string>;
  /** Skip the direct downstream tasks when nothing is at the path. @default false */
  readonly skipDownstreamIfMissing?: boolean;
}

/**
 * Check that a file exists.
 *
 * The result is true when something is at the path, and false when not.
 */
export const HasFileOperator = defineProviderOperator<HasFileOperatorArgs, boolean>({
  module: "airflow.providers.ts.localfile.operators.file",
  className: "HasFileOperator",
  implementationModule: "@apache-airflow/providers-ts-localfile/operators/file",
  templateFields: ["path"],
  canSkipDownstream: true,
  params: {
    path: { name: "path", required: true },
    skipDownstreamIfMissing: { name: "skip_downstream_if_missing" },
  },
  async execute({ path, skipDownstreamIfMissing = false }, ctx): Promise<boolean> {
    try {
      await stat(path);
      return true;
    } catch (error) {
      if (!hasCode(error, "ENOENT")) throw error;
    }
    if (skipDownstreamIfMissing) await ctx.skipDownstream();
    return false;
  },
});

/** Arguments of {@link DeleteFileOperator}. */
export interface DeleteFileOperatorArgs {
  /** The file to delete. */
  readonly path: Templated<string>;
  /** Succeed when the file is missing. When false, a missing file fails the task. @default true */
  readonly missingOk?: boolean;
}

/** What {@link DeleteFileOperator} returns. */
export interface DeleteFileResult {
  readonly path: string;
  /** False when the file was missing. */
  readonly deleted: boolean;
}

/** Delete a file. */
export const DeleteFileOperator = defineProviderOperator<DeleteFileOperatorArgs, DeleteFileResult>({
  module: "airflow.providers.ts.localfile.operators.file",
  className: "DeleteFileOperator",
  implementationModule: "@apache-airflow/providers-ts-localfile/operators/file",
  templateFields: ["path"],
  params: {
    path: { name: "path", required: true },
    missingOk: { name: "missing_ok" },
  },
  async execute({ path, missingOk = true }): Promise<DeleteFileResult> {
    try {
      await unlink(path);
      return { path, deleted: true };
    } catch (error) {
      if (hasCode(error, "ENOENT") && missingOk) return { path, deleted: false };
      if (hasCode(error, "ENOENT")) throw new Error(`${path} is missing`, { cause: error });
      throw error;
    }
  },
});
