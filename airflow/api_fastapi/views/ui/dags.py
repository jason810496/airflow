# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

from fastapi import Depends
from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session
from typing_extensions import Annotated

from airflow.api_fastapi.db.common import get_session
from airflow.api_fastapi.parameters import (
    QueryDagIdsFilter,
)
from airflow.api_fastapi.serializers.ui.dags import RecentDAGRunsResponse
from airflow.api_fastapi.views.router import AirflowRouter
from airflow.models import DagRun

dags_router = AirflowRouter(prefix="/dags", tags=["Dags"])


@dags_router.get("/recent_dag_runs", include_in_schema=False)
async def recent_dag_runs(
    filter_dag_ids: QueryDagIdsFilter,
    session: Annotated[Session, Depends(get_session)],
) -> RecentDAGRunsResponse:
    """Get recent DAG runs."""
    last_runs_subquery = (
        select(
            DagRun.dag_id,
            func.max(DagRun.execution_date).label("max_execution_date"),
        )
        .group_by(DagRun.dag_id)
        .where(DagRun.dag_id.in_(filter_dag_ids))  # Only include accessible/selected DAGs.
        .subquery("last_runs")
    )
    query = session.execute(
        select(
            DagRun.dag_id,
            DagRun.start_date,
            DagRun.end_date,
            DagRun.state,
            DagRun.execution_date,
            DagRun.data_interval_start,
            DagRun.data_interval_end,
        ).join(
            last_runs_subquery,
            and_(
                last_runs_subquery.c.dag_id == DagRun.dag_id,
                last_runs_subquery.c.max_execution_date == DagRun.execution_date,
            ),
        )
    )
    print(query)
    return RecentDAGRunsResponse(dag_id="1", dag_runs=[])
