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
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import React from "react";
import { afterEach, beforeEach, describe, expect, it, vi, type Mock } from "vitest";

import { useStreamingLogs } from "./useStreamingLogs";

const CONTROL_KEY = "_airflow_stream_control";

const HOOK_PROPS = {
  dagId: "test_dag",
  dagRunId: "test_run",
  enabled: true,
  mapIndex: -1,
  taskId: "test_task",
  tryNumber: 1,
};

const endOfStream = JSON.stringify({ [CONTROL_KEY]: "end_of_stream", reason: "finished", state: "success" });

const createMockResponse = (lines: Array<string>, { ok = true } = {}) => {
  const encoder = new TextEncoder();
  const stream = new ReadableStream({
    start(controller) {
      lines.forEach((line) => {
        controller.enqueue(encoder.encode(`${line}\n`));
      });
      controller.close();
    },
  });

  return { body: stream, ok } as unknown as Response;
};

const createWrapper =
  (queryClient: QueryClient) =>
  ({ children }: { readonly children: React.ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );

describe("useStreamingLogs", () => {
  let mockFetch: Mock;
  let queryClient: QueryClient;

  beforeEach(() => {
    queryClient = new QueryClient();
    mockFetch = vi.fn().mockImplementation(() => Promise.resolve(createMockResponse([endOfStream])));
    vi.stubGlobal("fetch", mockFetch);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("appends log lines and consumes control records", async () => {
    mockFetch.mockImplementationOnce(() =>
      Promise.resolve(
        createMockResponse([
          JSON.stringify({ event: "line 1" }),
          JSON.stringify({ [CONTROL_KEY]: "heartbeat" }),
          JSON.stringify({ event: "line 2" }),
          JSON.stringify({ [CONTROL_KEY]: "resume", token: "signed-token" }),
          endOfStream,
        ]),
      ),
    );

    const { result } = renderHook(() => useStreamingLogs(HOOK_PROPS), {
      wrapper: createWrapper(queryClient),
    });

    await waitFor(() => {
      expect(result.current.isStreamComplete).toBe(true);
    });

    expect(result.current.content.map((message) => message.event)).toEqual(["line 1", "line 2"]);
    expect(result.current.hasStreamError).toBe(false);
    expect(mockFetch).toHaveBeenCalledTimes(1);
  });

  it("skips malformed lines without dropping valid ones", async () => {
    mockFetch.mockImplementationOnce(() =>
      Promise.resolve(createMockResponse([JSON.stringify({ event: "line 1" }), "{not json", endOfStream])),
    );

    const { result } = renderHook(() => useStreamingLogs(HOOK_PROPS), {
      wrapper: createWrapper(queryClient),
    });

    await waitFor(() => {
      expect(result.current.isStreamComplete).toBe(true);
    });

    expect(result.current.content.map((message) => message.event)).toEqual(["line 1"]);
    expect(mockFetch).toHaveBeenCalledTimes(1);
  });

  it("invalidates task instance queries on end of stream", async () => {
    const invalidateSpy = vi.spyOn(queryClient, "invalidateQueries");

    const { result } = renderHook(() => useStreamingLogs(HOOK_PROPS), {
      wrapper: createWrapper(queryClient),
    });

    await waitFor(() => {
      expect(result.current.isStreamComplete).toBe(true);
    });

    expect(invalidateSpy).toHaveBeenCalled();
  });

  it("reconnects immediately with the last resume token when a progressed stream ends early", async () => {
    mockFetch
      .mockImplementationOnce(() =>
        Promise.resolve(
          createMockResponse([
            JSON.stringify({ event: "line 1" }),
            JSON.stringify({ [CONTROL_KEY]: "resume", token: "resume-1" }),
          ]),
        ),
      )
      .mockImplementationOnce(() =>
        Promise.resolve(createMockResponse([JSON.stringify({ event: "line 2" }), endOfStream])),
      );

    const { result } = renderHook(() => useStreamingLogs(HOOK_PROPS), {
      wrapper: createWrapper(queryClient),
    });

    await waitFor(() => {
      expect(result.current.isStreamComplete).toBe(true);
    });

    expect(result.current.content.map((message) => message.event)).toEqual(["line 1", "line 2"]);
    expect(mockFetch).toHaveBeenCalledTimes(2);

    const secondUrl = String(mockFetch.mock.calls[1]?.[0]);

    expect(secondUrl).toContain("resume_token=resume-1");
    expect(result.current.hasStreamError).toBe(false);
  });

  it("encodes path segments in the stream URL", async () => {
    const { result } = renderHook(
      () => useStreamingLogs({ ...HOOK_PROPS, dagRunId: "manual/2024-01-01T00:00:00+00:00" }),
      { wrapper: createWrapper(queryClient) },
    );

    await waitFor(() => {
      expect(result.current.isStreamComplete).toBe(true);
    });

    const url = String(mockFetch.mock.calls[0]?.[0]);

    expect(url).toContain("manual%2F2024-01-01T00%3A00%3A00%2B00%3A00");
  });

  it("applies the failure budget to streams that close without delivering anything", async () => {
    vi.useFakeTimers();
    try {
      // 200 OK with an immediately-closing empty body: no progress was made, so
      // this must back off and eventually give up rather than busy-loop.
      mockFetch.mockImplementation(() => Promise.resolve(createMockResponse([])));

      const { result } = renderHook(() => useStreamingLogs(HOOK_PROPS), {
        wrapper: createWrapper(queryClient),
      });

      // Six empty connects (initial + 5 retries) exhaust the failure budget.
      for (let attempt = 0; attempt < 6; attempt += 1) {
        // eslint-disable-next-line no-await-in-loop -- each retry timer must fire and settle before the next
        await act(async () => {
          await vi.runAllTimersAsync();
        });
      }

      expect(result.current.hasStreamError).toBe(true);
      expect(result.current.isStreamComplete).toBe(false);
      expect(mockFetch).toHaveBeenCalledTimes(6);
    } finally {
      vi.useRealTimers();
    }
  });

  it("sets hasStreamError after repeated connection failures", async () => {
    vi.useFakeTimers();
    try {
      mockFetch.mockImplementation(() => Promise.resolve(createMockResponse([], { ok: false })));

      const { result } = renderHook(() => useStreamingLogs(HOOK_PROPS), {
        wrapper: createWrapper(queryClient),
      });

      // Six failed connects (initial + 5 retries) exhaust the failure budget.
      for (let attempt = 0; attempt < 6; attempt += 1) {
        // eslint-disable-next-line no-await-in-loop -- each retry timer must fire and settle before the next
        await act(async () => {
          await vi.runAllTimersAsync();
        });
      }

      expect(result.current.hasStreamError).toBe(true);
      expect(result.current.isStreamComplete).toBe(false);
    } finally {
      vi.useRealTimers();
    }
  });

  it("does not fetch when disabled", () => {
    renderHook(() => useStreamingLogs({ ...HOOK_PROPS, enabled: false }), {
      wrapper: createWrapper(queryClient),
    });

    expect(mockFetch).not.toHaveBeenCalled();
  });
});
