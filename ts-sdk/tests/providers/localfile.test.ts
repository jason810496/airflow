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

import { createHash } from "node:crypto";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  CreateFileOperator,
  DeleteFileOperator,
  HasFileOperator,
  WriteFileOperator,
} from "../../providers/localfile/src/operators/file.js";
import { getProviderImplementation } from "../../src/sdk/provider-operator.js";
import type {
  ProviderImplementation,
  ProviderOperatorContext,
} from "../../src/sdk/provider-operator.js";

function implementationOf(binding: unknown): ProviderImplementation {
  const found = getProviderImplementation(binding);
  if (found?.implementation == null) throw new Error("not an implemented binding");
  return found.implementation;
}

function makeContext(): ProviderOperatorContext & { skipDownstream: ReturnType<typeof vi.fn> } {
  const logger = { debug: vi.fn(), info: vi.fn(), warning: vi.fn(), error: vi.fn() };
  return {
    dagId: "d",
    taskId: "t",
    runId: "r",
    tryNumber: 1,
    mapIndex: -1,
    signal: new AbortController().signal,
    logger,
    skipDownstream: vi.fn(async () => undefined),
  };
}

let dir: string;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "localfile-"));
});

afterEach(() => {
  rmSync(dir, { recursive: true, force: true });
});

const run = (binding: unknown, args: object, ctx = makeContext()) =>
  Promise.resolve(implementationOf(binding).execute(args, ctx));

describe("the localfile operators", () => {
  it("implement their Python class paths", () => {
    expect(
      [CreateFileOperator, WriteFileOperator, HasFileOperator, DeleteFileOperator].map(
        (binding) => getProviderImplementation(binding)?.classpath,
      ),
    ).toEqual([
      "airflow.providers.localfile.operators.file.CreateFileOperator",
      "airflow.providers.localfile.operators.file.WriteFileOperator",
      "airflow.providers.localfile.operators.file.HasFileOperator",
      "airflow.providers.localfile.operators.file.DeleteFileOperator",
    ]);
  });
});

describe("CreateFileOperator", () => {
  it("creates the file and its parent directories", async () => {
    const path = join(dir, "a", "b", "file.txt");

    await expect(run(CreateFileOperator, { path })).resolves.toEqual({ path, created: true });

    expect(readFileSync(path, "utf8")).toBe("");
  });

  it("leaves an existing file as it is when exist_ok is true", async () => {
    const path = join(dir, "file.txt");
    writeFileSync(path, "keep");

    await expect(run(CreateFileOperator, { path })).resolves.toEqual({ path, created: false });

    expect(readFileSync(path, "utf8")).toBe("keep");
  });

  it("fails on an existing file when exist_ok is false", async () => {
    const path = join(dir, "file.txt");
    writeFileSync(path, "keep");

    await expect(run(CreateFileOperator, { path, existOk: false })).rejects.toThrow(
      `${path} exists already`,
    );
    expect(readFileSync(path, "utf8")).toBe("keep");
  });
});

describe("WriteFileOperator", () => {
  it("overwrites by default and reports the file as read back", async () => {
    const path = join(dir, "nested", "file.txt");
    await run(WriteFileOperator, { path, content: "old content" });

    const result = await run(WriteFileOperator, { path, content: "héllo" });

    const bytes = Buffer.from("héllo", "utf8");
    expect(readFileSync(path, "utf8")).toBe("héllo");
    expect(result).toEqual({
      path,
      bytes: bytes.length,
      sha256: createHash("sha256").update(bytes).digest("hex"),
    });
  });

  it("appends, and describes the whole file", async () => {
    const path = join(dir, "file.txt");
    await run(WriteFileOperator, { path, content: "one " });

    const result = await run(WriteFileOperator, { path, content: "two", mode: "append" });

    expect(readFileSync(path, "utf8")).toBe("one two");
    expect(result).toEqual({
      path,
      bytes: 7,
      sha256: createHash("sha256").update("one two").digest("hex"),
    });
  });

  it.each([
    ["utf-8", "é", [0xc3, 0xa9]],
    ["latin1", "é", [0xe9]],
    ["ascii", "A", [0x41]],
  ] as const)("encodes the text as %s", async (encoding, content, expected) => {
    const path = join(dir, "file.txt");

    const result = await run(WriteFileOperator, { path, content, encoding });

    expect([...readFileSync(path)]).toEqual(expected);
    expect(result).toMatchObject({ bytes: expected.length });
  });
});

describe("HasFileOperator", () => {
  it("is true for an existing file and does not skip", async () => {
    const path = join(dir, "file.txt");
    writeFileSync(path, "x");
    const ctx = makeContext();

    await expect(run(HasFileOperator, { path, skipDownstreamIfMissing: true }, ctx)).resolves.toBe(
      true,
    );
    expect(ctx.skipDownstream).not.toHaveBeenCalled();
  });

  it("is false for a missing file and does not skip by default", async () => {
    const ctx = makeContext();

    await expect(run(HasFileOperator, { path: join(dir, "missing") }, ctx)).resolves.toBe(false);
    expect(ctx.skipDownstream).not.toHaveBeenCalled();
  });

  it("skips the downstream tasks of a missing file when asked to", async () => {
    const ctx = makeContext();

    await expect(
      run(HasFileOperator, { path: join(dir, "missing"), skipDownstreamIfMissing: true }, ctx),
    ).resolves.toBe(false);
    expect(ctx.skipDownstream).toHaveBeenCalledOnce();
  });
});

describe("DeleteFileOperator", () => {
  it("deletes the file", async () => {
    const path = join(dir, "file.txt");
    writeFileSync(path, "x");

    await expect(run(DeleteFileOperator, { path })).resolves.toEqual({ path, deleted: true });

    expect(existsSync(path)).toBe(false);
  });

  it("succeeds on a missing file when missing_ok is true", async () => {
    const path = join(dir, "missing");

    await expect(run(DeleteFileOperator, { path })).resolves.toEqual({ path, deleted: false });
  });

  it("fails on a missing file when missing_ok is false", async () => {
    const path = join(dir, "missing");

    await expect(run(DeleteFileOperator, { path, missingOk: false })).rejects.toThrow(
      `${path} is missing`,
    );
  });
});
