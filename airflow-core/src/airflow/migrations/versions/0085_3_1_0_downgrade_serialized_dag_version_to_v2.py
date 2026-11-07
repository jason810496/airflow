#
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

"""
Add backward compatibility for serialized DAG format v3 to v2.

Revision ID: cc92b33c6709
Revises: eaf332f43c7c
Create Date: 2025-09-22 22:50:48.035121

"""

from __future__ import annotations

from textwrap import dedent

from alembic import context, op

from airflow.migrations.utils import downgrade_serialized_dags

# revision identifiers, used by Alembic.
revision = "cc92b33c6709"
down_revision = "eaf332f43c7c"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    """Apply Downgrade Serialized Dag version to v2."""
    # No-op: Server handles v1/v2/v3 DAGs at runtime via conversion functions
    pass


def downgrade():
    """Convert v3 serialized DAGs back to v2 format for compatibility with older Airflow versions."""
    if context.is_offline_mode():
        print(
            dedent("""
            ------------
            --  Manual v3 to v2 DAG conversion required (offline mode)
            --
            --  PostgreSQL:
            --  UPDATE serialized_dag SET data = jsonb_set((data::jsonb - 'client_defaults'), '{__version}', '2')::json
            --  WHERE id IN (SELECT id FROM serialized_dag WHERE data->>'__version' = '3' AND data_compressed IS NULL);
            --
            --  MySQL/SQLite:
            --  UPDATE serialized_dag SET data = JSON_SET(JSON_REMOVE(data, '$.client_defaults'), '$.__version', 2)
            --  WHERE JSON_EXTRACT(data, '$.__version') = '3' AND data_compressed IS NULL;
            --
            --  For compressed DAGs: run online migration.
            ------------
            """)
        )
        return

    downgrade_serialized_dags(op.get_bind(), to=2)
