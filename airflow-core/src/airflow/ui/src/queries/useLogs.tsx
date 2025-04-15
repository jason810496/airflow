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
import { chakra } from "@chakra-ui/react";
import type { UseQueryOptions } from "@tanstack/react-query";
import { useQuery } from "@tanstack/react-query";
import dayjs from "dayjs";
import { useState, useEffect } from "react";
import innerText from "react-innertext";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import { renderStructuredLog } from "src/components/renderStructuredLog";
import { isStatePending, useAutoRefresh } from "src/utils";
import { getTaskInstanceLink } from "src/utils/links";
import { TOKEN_STORAGE_KEY, getTokenFromCookies } from "src/utils/tokenHandler";

// Update TaskInstancesLogResponse type to include headers and body
interface TaskInstancesLogResponse {
  content?: Array<any>;
  headers?: Headers;
  body?: ReadableStream<Uint8Array>;
}

type Props = {
  dagId: string;
  logLevelFilters?: Array<string>;
  sourceFilters?: Array<string>;
  taskInstance?: TaskInstanceResponse;
  tryNumber?: number;
};

type ParseLogsProps = {
  data: TaskInstancesLogResponse["content"];
  logLevelFilters?: Array<string>;
  sourceFilters?: Array<string>;
  taskInstance?: TaskInstanceResponse;
  tryNumber: number;
};

const parseLogs = ({ data, logLevelFilters, sourceFilters, taskInstance, tryNumber }: ParseLogsProps) => {
  let warning;
  let parsedLines;
  let startGroup = false;
  let groupLines: Array<JSX.Element | ""> = [];
  let groupName = "";
  const sources: Array<string> = [];

  // open the summary when hash is present since the link might have a hash linking to a line
  const open = Boolean(location.hash);
  const logLink = taskInstance ? `${getTaskInstanceLink(taskInstance)}?try_number=${tryNumber}` : "";

  try {
    parsedLines = data.map((datum, index) => {
      if (typeof datum !== "string" && "logger" in datum) {
        const source = datum.logger as string;

        if (!sources.includes(source)) {
          sources.push(source);
        }
      }

      return renderStructuredLog({ index, logLevelFilters, logLink, logMessage: datum, sourceFilters });
    });
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : "An error occurred.";

    // eslint-disable-next-line no-console
    console.warn(`Error parsing logs: ${errorMessage}`);
    warning = "Unable to show logs. There was an error parsing logs.";

    return { data, warning };
  }

  // TODO: Add support for nested groups

  parsedLines = parsedLines.map((line) => {
    const text = innerText(line);

    if (text.includes("::group::") && !startGroup) {
      startGroup = true;
      groupName = text.split("::group::")[1] as string;
    } else if (text.includes("::endgroup::")) {
      startGroup = false;
      const group = (
        <details key={groupName} open={open} style={{ width: "100%" }}>
          <summary data-testid={`summary-${groupName}`}>
            <chakra.span color="fg.info" cursor="pointer">
              {groupName}
            </chakra.span>
          </summary>
          {groupLines}
        </details>
      );

      groupLines = [];

      return group;
    }

    if (startGroup) {
      groupLines.push(line);

      return undefined;
    } else {
      return line;
    }
  });

  return {
    parsedLogs: parsedLines,
    sources,
    warning,
  };
};

const parseNdjsonStream = async (stream: ReadableStream<Uint8Array>, onData: (line: string) => void) => {
  const reader = stream.getReader();
  const decoder = new TextDecoder("utf-8");
  let buffer = "";

  while (true) {
    const { done, value } = await reader.read();

    if (done) {break;}

    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split("\n");

    buffer = lines.pop() || ""; // Keep the last incomplete line in the buffer

    for (const line of lines) {
      if (line.trim()) {
        onData(line);
      }
    }
  }

  if (buffer.trim()) {
    onData(buffer); // Process any remaining data
  }
};

export const useTaskInstanceServiceGetLog = (
  {
    accept = "application/x-ndjson",
    dagId,
    dagRunId,
    taskId,
    tryNumber,
    mapIndex = -1,
  }: {
    accept?: "application/json" | "*/*" | "application/x-ndjson";
    dagId: string;
    dagRunId: string;
    taskId: string;
    tryNumber: number;
    mapIndex?: number;
    token?: string;
  },
  options?: Omit<UseQueryOptions<TaskInstancesLogResponse>, "queryFn" | "queryKey">
) => {
  const token = localStorage.getItem(TOKEN_STORAGE_KEY) ?? getTokenFromCookies();
  console.log(
    `Fetching logs for DAG ID: ${dagId}, DAG Run ID: ${dagRunId}, Task ID: ${taskId}, Try Number: ${tryNumber}, Map Index: ${mapIndex}`
  )
  return useQuery<TaskInstancesLogResponse>({
    queryKey: ["taskInstanceLog", dagId, dagRunId, taskId, tryNumber, mapIndex],
    queryFn: async () => {
      const response = await fetch(
        `/api/v2/dags/${dagId}/dagRuns/${dagRunId}/taskInstances/${taskId}/logs/${tryNumber}?mapIndex=${mapIndex}`,
        {
          headers: {
            Accept: accept,
            Authorization: `Bearer ${token}`,
          },
        }
      );

      if (!response.ok) {
        throw new Error(`Failed to fetch logs: ${response.statusText}`);
      }

      if (accept === "application/x-ndjson") {
        return response.body as ReadableStream<Uint8Array>;
      }

      return response.json();
    },
    ...options,
  });
};

export const useLogs = (
  { dagId, logLevelFilters, sourceFilters, taskInstance, tryNumber = 1 }: Props,
  options?: Omit<UseQueryOptions<TaskInstancesLogResponse>, "queryFn" | "queryKey">,
) => {
  const refetchInterval = useAutoRefresh({ dagId });
  const [parsedLogs, setParsedLogs] = useState<Array<JSX.Element | "">>([]);
  const [sources, setSources] = useState<Array<string>>([]);
  const [warning, setWarning] = useState<string | undefined>();

  console.log(`Fetching logs for DAG ID: ${dagId}, Task Instance: ${taskInstance?.task_id}, Try Number: ${tryNumber}`);

  const { data, ...rest } = useTaskInstanceServiceGetLog(
    {
      accept: "application/x-ndjson",
      dagId,
      dagRunId: taskInstance?.dag_run_id ?? "",
      mapIndex: taskInstance?.map_index ?? -1,
      taskId: taskInstance?.task_id ?? "",
      tryNumber,
    },
    undefined,
    {
      enabled: Boolean(taskInstance),
      refetchInterval: (query: { state: { dataUpdatedAt: number } }) =>
        isStatePending(taskInstance?.state) ||
        dayjs(query.state.dataUpdatedAt).isBefore(taskInstance?.end_date)
          ? refetchInterval
          : false,
      ...options,
    },
  );

  useEffect(() => {
    if (!data) {return;}

    const contentType = data.headers?.get("content-type");

    if (contentType === "application/x-ndjson") {
      const stream = data.body as ReadableStream<Uint8Array>;

      parseNdjsonStream(stream, (line) => {
        try {
          const logEntry = JSON.parse(line);
          const { parsedLogs: newLogs, sources: newSources } = parseLogs({
            data: [logEntry],
            logLevelFilters,
            sourceFilters,
            taskInstance,
            tryNumber,
          });

          // Ensure state updates handle undefined values
          setParsedLogs((prevLogs) => [...prevLogs, ...(newLogs || [])]);
          setSources((prevSources) => [...new Set([...(prevSources || []), ...(newSources || [])])]);
        } catch (error) {
          console.warn("Error parsing ndjson log line:", error);
          setWarning("Unable to show logs. There was an error parsing logs.");
        }
      });
    } else {
      const {
        parsedLogs: newLogs,
        sources: newSources,
        warning: newWarning,
      } = parseLogs({
        data: data.content ?? [],
        logLevelFilters,
        sourceFilters,
        taskInstance,
        tryNumber,
      });

      // Ensure state updates handle undefined values
      setParsedLogs(newLogs || []);
      setSources(newSources || []);
      setWarning(newWarning);
    }
  }, [data, logLevelFilters, sourceFilters, taskInstance?.task_id, taskInstance?.dag_run_id, tryNumber]);

  return { data: { parsedLogs, sources, warning }, ...rest };
};
