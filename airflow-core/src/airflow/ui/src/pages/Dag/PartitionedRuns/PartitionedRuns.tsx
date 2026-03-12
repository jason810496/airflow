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
import { Badge, Box, Link, Text } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";
import { Link as RouterLink, useParams } from "react-router-dom";

import { usePartitionedDagRunServiceGetPartitionedDagRuns } from "openapi/queries";
import type { DagRunState, PartitionedDagRunResponse } from "openapi/requests/types.gen";
import { AssetProgressCell } from "src/components/AssetProgressCell";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";

const dagRunStates = new Set<string>(["queued", "running", "success", "failed"]);

const StateCell = ({ state }: { readonly state: string | null }) => {
  if (state !== null && dagRunStates.has(state)) {
    return <StateBadge state={state as DagRunState}>{state}</StateBadge>;
  }

  return (
    <Badge borderRadius="full" fontSize="sm" px={2} py={1} variant="outline">
      {state ?? ""}
    </Badge>
  );
};

const getColumns = (
  translate: TFunction,
  dagId: string,
): Array<ColumnDef<PartitionedDagRunResponse>> => [
  {
    accessorKey: "partition_key",
    enableSorting: false,
    header: translate("dagRun.mappedPartitionKey"),
  },
  {
    accessorKey: "state",
    cell: ({ row: { original } }) => <StateCell state={original.state} />,
    enableSorting: false,
    header: translate("state"),
  },
  {
    accessorKey: "created_at",
    cell: ({ row }) => (
      <Text>
        <Time datetime={row.original.created_at} />
      </Text>
    ),
    enableSorting: false,
    header: translate("table.createdAt"),
  },
  {
    accessorKey: "total_received",
    cell: ({ row }) =>
      // eslint-disable-next-line @typescript-eslint/strict-boolean-expressions
      row.original.created_dag_run_id ? (
        <Text>{`${String(row.original.total_received)} / ${String(row.original.total_required)}`}</Text>
      ) : (
        <AssetProgressCell
          dagId={dagId}
          partitionKey={row.original.partition_key}
          totalReceived={row.original.total_received}
          totalRequired={row.original.total_required}
        />
      ),
    enableSorting: false,
    header: translate("partitionedDagRunDetail.receivedAssetEvents"),
  },
  {
    accessorKey: "created_dag_run_id",
    cell: ({ row: { original } }) =>
      // eslint-disable-next-line @typescript-eslint/strict-boolean-expressions
      original.created_dag_run_id ? (
        <Link asChild color="fg.info">
          <RouterLink to={`/dags/${original.dag_id}/runs/${original.created_dag_run_id}`}>
            {original.created_dag_run_id}
          </RouterLink>
        </Link>
      ) : undefined,
    enableSorting: false,
    header: translate("dagRunId"),
  },
];

export const PartitionedRuns = () => {
  const { t: translate } = useTranslation();
  const { dagId } = useParams();
  const resolvedDagId = dagId ?? "";
  const { setTableURLState, tableURLState } = useTableURLState();

  const { data, error, isFetching, isLoading } = usePartitionedDagRunServiceGetPartitionedDagRuns({
    dagId: resolvedDagId,
  });

  const partitionedDagRuns = data?.partitioned_dag_runs ?? [];
  const total = data?.total ?? 0;
  const columns = getColumns(translate, resolvedDagId);

  return (
    <Box>
      <ErrorAlert error={error} />
      <DataTable
        columns={columns}
        data={partitionedDagRuns}
        initialState={tableURLState}
        isFetching={isFetching}
        isLoading={isLoading}
        modelName="common:partitionedDagRun"
        onStateChange={setTableURLState}
        total={total}
      />
    </Box>
  );
};
