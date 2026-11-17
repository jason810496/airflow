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

// Drives `startCoordinator` against a fake supervisor over real sockets: sends one first frame,
// answers the runtime's requests through a responder, and collects what the runtime sent back.

import { createServer, type Server, type Socket } from "node:net";

import { decode, encode } from "@msgpack/msgpack";

import { startCoordinator } from "../../../src/coordinator/runtime.js";
import type { Bundle } from "../../../src/sdk/bundle.js";

export interface DriveResult {
  /** The terminal frame the runtime answered the first frame with. */
  response: Record<string, unknown> | null;
  logRecords: Record<string, unknown>[];
  requests: { type: string; body: Record<string, unknown> }[];
}

/** What a responder answers a request with: a body, or an error frame. */
export interface Reply {
  body?: unknown;
  error?: unknown;
}

export type Responder = (type: string, body: Record<string, unknown>) => Reply | undefined;

/** The error frame the supervisor sends when the API server answers 404. */
export const API_SERVER_404 = {
  error: "API_SERVER_ERROR",
  detail: { status_code: 404, message: "Not found", detail: { detail: { reason: "not_found" } } },
};

function frameBytes(id: number, body: unknown, error?: unknown): Buffer {
  const payload = Buffer.from(encode(error === undefined ? [id, body, null] : [id, body, error]));
  const header = Buffer.alloc(4);
  header.writeUInt32BE(payload.length, 0);
  return Buffer.concat([header, payload]);
}

function readFrames(buf: Buffer): {
  frames: { id: number; body: unknown; arity: number }[];
  rest: Buffer;
} {
  const frames: { id: number; body: unknown; arity: number }[] = [];
  let rest: Buffer = buf;
  while (rest.length >= 4) {
    const length = rest.readUInt32BE(0);
    if (rest.length < 4 + length) break;
    const decoded = decode(Buffer.from(rest.subarray(4, 4 + length))) as unknown[];
    frames.push({ id: decoded[0] as number, body: decoded[1], arity: decoded.length });
    rest = Buffer.from(rest.subarray(4 + length));
  }
  return { frames, rest };
}

function listen(): Promise<{ server: Server; port: number }> {
  return new Promise((resolve) => {
    const server = createServer();
    server.listen(0, "127.0.0.1", () => {
      resolve({ server, port: (server.address() as { port: number }).port });
    });
  });
}

function acceptOne(server: Server): Promise<Socket> {
  return new Promise((resolve) => server.once("connection", resolve));
}

export function makeStartupDetails(
  taskId: string,
  dagId: string,
  tiContext: Record<string, unknown> = {},
): unknown {
  return {
    type: "StartupDetails",
    ti: {
      id: "ti-1",
      dag_version_id: "dag-version-1",
      task_id: taskId,
      dag_id: dagId,
      run_id: "run-1",
      try_number: 1,
      map_index: -1,
      hostname: "test-host",
      queue: "typescript",
    },
    dag_rel_path: "test.py",
    bundle_info: { name: "test", version: null },
    start_date: "2026-04-23T00:00:00Z",
    ti_context: tiContext,
    sentry_integration: "",
  };
}

/** The requests of one type, in the order the runtime sent them. */
export function requestsOf(result: DriveResult, type: string): Record<string, unknown>[] {
  return result.requests.filter((request) => request.type === type).map((request) => request.body);
}

/** Runs the runtime against the supervisor's two addresses, until it has delivered its last frame. */
export type RunRuntime = (addresses: { commAddr: string; logsAddr: string }) => Promise<void>;

export async function drive(
  bundle: Bundle,
  firstFrame: unknown,
  responder?: Responder,
  /** Defaults to `startCoordinator(bundle)` in this process. A test of an entry point runs the entry. */
  run: RunRuntime = (addresses) => startCoordinator(bundle, { ...addresses, argv: [] }),
): Promise<DriveResult> {
  const comm = await listen();
  const logs = await listen();
  const commAccepted = acceptOne(comm.server);
  const logsAccepted = acceptOne(logs.server);
  const runtimeDone = run({
    commAddr: `127.0.0.1:${comm.port}`,
    logsAddr: `127.0.0.1:${logs.port}`,
  });
  const [commSocket, logsSocket] = await Promise.all([commAccepted, logsAccepted]);
  commSocket.write(frameBytes(0, firstFrame));

  const result: DriveResult = { response: null, logRecords: [], requests: [] };
  const logChunks: Buffer[] = [];
  let buffered: Buffer = Buffer.alloc(0);
  commSocket.on("data", (chunk: Buffer) => {
    const taken = readFrames(Buffer.concat([buffered, chunk]));
    buffered = taken.rest;
    for (const frame of taken.frames) {
      if (frame.arity === 2) {
        const body = (frame.body ?? {}) as Record<string, unknown>;
        const type = String(body["type"] ?? "");
        result.requests.push({ type, body });
        const reply = responder?.(type, body);
        commSocket.write(frameBytes(frame.id, reply?.body ?? null, reply?.error));
      } else if (result.response === null) {
        result.response = frame.body as Record<string, unknown>;
      }
    }
  });
  logsSocket.on("data", (chunk: Buffer) => logChunks.push(chunk));
  const commEnded = new Promise<void>((resolve) => commSocket.on("end", () => resolve()));
  await Promise.all([runtimeDone, commEnded]);
  comm.server.close();
  logs.server.close();
  result.logRecords = Buffer.concat(logChunks)
    .toString("utf8")
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line) as Record<string, unknown>);
  return result;
}
