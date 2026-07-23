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
import { useQueryClient } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";

import {
  useTaskInstanceServiceGetMappedTaskInstanceKey,
  useTaskInstanceServiceGetTaskInstanceKey,
} from "openapi/queries";
import type { StructuredLogMessage } from "openapi/requests";
import { OpenAPI } from "openapi/requests/core/OpenAPI";
import { readNdjsonStream } from "src/utils/ndjsonStream";

// Control records are told apart from log lines by this reserved key (log lines
// can never carry it — the server strips it from log records).
const CONTROL_KEY = "_airflow_stream_control";

const RECONNECT_BASE_DELAY_MS = 1000;
const RECONNECT_MAX_DELAY_MS = 30_000;
const MAX_CONSECUTIVE_FAILURES = 5;

type ControlRecord = {
  // state/reason are informational; the canonical final state comes from the TI refetch
  [CONTROL_KEY]: "end_of_stream" | "heartbeat" | "resume";
  reason?: string;
  state?: string | null;
  token?: string;
};

/**
 * Tails a running task instance try over the UI task log stream endpoint (NDJSON).
 *
 * Log lines are appended incrementally as the server delivers them; in-band control
 * records are consumed here: `resume` tokens are remembered so a dropped or
 * server-rotated connection reconnects where it left off, `heartbeat` records are
 * discarded, and `end_of_stream` sets `isStreamComplete` and invalidates the task
 * instance queries so callers refetch the final state and canonical log. A stream
 * that closes early after making progress reconnects immediately; one that closes
 * without delivering anything counts against a failure budget with exponential
 * backoff, and exhausting the budget sets `hasStreamError` so callers can fall
 * back to the polling endpoint.
 */
export const useStreamingLogs = ({
  dagId,
  dagRunId,
  enabled,
  mapIndex,
  taskId,
  tryNumber,
}: {
  dagId: string;
  dagRunId: string;
  enabled: boolean;
  mapIndex: number;
  taskId: string;
  tryNumber: number;
}) => {
  const queryClient = useQueryClient();
  const [content, setContent] = useState<Array<StructuredLogMessage>>([]);
  const [isStreamComplete, setIsStreamComplete] = useState(false);
  const [hasStreamError, setHasStreamError] = useState(false);
  const [reconnectTick, setReconnectTick] = useState(0);
  const resumeTokenRef = useRef<string | undefined>(undefined);
  const consecutiveFailuresRef = useRef(0);

  // A different task instance try is a different stream: drop the buffer and token.
  useEffect(() => {
    setContent([]);
    setIsStreamComplete(false);
    setHasStreamError(false);
    resumeTokenRef.current = undefined;
    consecutiveFailuresRef.current = 0;
  }, [dagId, dagRunId, taskId, mapIndex, tryNumber]);

  useEffect(() => {
    if (!enabled) {
      return undefined;
    }

    const abortController = new AbortController();
    let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
    let sawEndOfStream = false;
    // Any record received proves the endpoint works, so an early close is a
    // connection rotation/drop (reconnect immediately with the resume token),
    // not a failure. A close with NO records goes through the failure budget.
    let madeProgress = false;

    const scheduleReconnect = () => {
      consecutiveFailuresRef.current += 1;
      if (consecutiveFailuresRef.current > MAX_CONSECUTIVE_FAILURES) {
        setHasStreamError(true);

        return;
      }
      const delay = Math.min(
        RECONNECT_BASE_DELAY_MS * 2 ** (consecutiveFailuresRef.current - 1),
        RECONNECT_MAX_DELAY_MS,
      );

      reconnectTimer = setTimeout(() => setReconnectTick((tick) => tick + 1), delay);
    };

    const handleControl = (record: ControlRecord) => {
      const control = record[CONTROL_KEY];

      if (control === "end_of_stream") {
        sawEndOfStream = true;
        setIsStreamComplete(true);
        void queryClient.invalidateQueries({ queryKey: [useTaskInstanceServiceGetTaskInstanceKey] });
        void queryClient.invalidateQueries({
          queryKey: [useTaskInstanceServiceGetMappedTaskInstanceKey],
        });
      } else if (control === "resume") {
        resumeTokenRef.current = record.token;
      }
      // heartbeat records carry nothing to act on
    };

    const parseLine = (line: string): unknown => {
      try {
        return JSON.parse(line);
      } catch {
        // a malformed line (partial proxy flush) must not kill the valid ones
        return undefined;
      }
    };

    const handleLines = (lines: Array<string>) => {
      madeProgress = true;
      const newMessages: Array<StructuredLogMessage> = [];

      for (const line of lines) {
        const record = parseLine(line);

        if (typeof record === "object" && record !== null) {
          if (CONTROL_KEY in record) {
            handleControl(record as ControlRecord);
          } else {
            newMessages.push(record as StructuredLogMessage);
          }
        }
      }

      if (newMessages.length > 0) {
        consecutiveFailuresRef.current = 0;
        setContent((prev) => [...prev, ...newMessages]);
      }
    };

    const fetchStream = async () => {
      try {
        const params = new URLSearchParams({ map_index: String(mapIndex) });

        if (resumeTokenRef.current !== undefined) {
          params.set("resume_token", resumeTokenRef.current);
        }

        const path = [
          "/ui/dags",
          encodeURIComponent(dagId),
          "dagRuns",
          encodeURIComponent(dagRunId),
          "taskInstances",
          encodeURIComponent(taskId),
          "logs",
          String(tryNumber),
          "stream",
        ].join("/");
        const response = await fetch(`${OpenAPI.BASE}${path}?${params}`, {
          signal: abortController.signal,
        });

        if (!response.ok || !response.body) {
          scheduleReconnect();

          return;
        }

        await readNdjsonStream({
          body: response.body,
          onLines: handleLines,
          signal: abortController.signal,
        });

        if (!abortController.signal.aborted && !sawEndOfStream) {
          if (madeProgress) {
            consecutiveFailuresRef.current = 0;
            reconnectTimer = setTimeout(() => setReconnectTick((tick) => tick + 1), 0);
          } else {
            scheduleReconnect();
          }
        }
      } catch (error) {
        if ((error as Error).name !== "AbortError") {
          // eslint-disable-next-line no-console
          console.error("Task log stream error:", error);
          scheduleReconnect();
        }
      }
    };

    void fetchStream();

    return () => {
      abortController.abort();
      if (reconnectTimer !== undefined) {
        clearTimeout(reconnectTimer);
      }
    };
  }, [enabled, dagId, dagRunId, taskId, mapIndex, tryNumber, reconnectTick, queryClient]);

  return { content, hasStreamError, isStreamComplete };
};
