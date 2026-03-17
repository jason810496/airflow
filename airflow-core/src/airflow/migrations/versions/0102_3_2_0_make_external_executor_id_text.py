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
Make external_executor_id TEXT to allow for longer external_executor_ids.

Revision ID: a5a3e5eb9b8d
Revises: 55297ae24532
Create Date: 2026-01-28 16:35:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import context, op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "a5a3e5eb9b8d"
down_revision = "55297ae24532"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Change external_executor_id column from VARCHAR(250) to TEXT."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.alter_column(
            "external_executor_id",
            existing_type=sa.VARCHAR(length=250),
            type_=sa.Text(),
            existing_nullable=True,
        )

    with op.batch_alter_table("task_instance_history", schema=None) as batch_op:
        batch_op.alter_column(
            "external_executor_id",
            existing_type=sa.VARCHAR(length=250),
            type_=sa.Text(),
            existing_nullable=True,
        )


def downgrade():
    """Revert external_executor_id column from TEXT to VARCHAR(250)."""
    conn = op.get_bind()

    if conn.dialect.name != "postgresql":
        with op.batch_alter_table("task_instance_history", schema=None) as batch_op:
            batch_op.alter_column(
                "external_executor_id",
                existing_type=sa.Text(),
                type_=sa.VARCHAR(length=250),
                existing_nullable=True,
            )

        with op.batch_alter_table("task_instance", schema=None) as batch_op:
            batch_op.alter_column(
                "external_executor_id",
                existing_type=sa.Text(),
                type_=sa.VARCHAR(length=250),
                existing_nullable=True,
            )
        return

    if context.is_offline_mode():
        print(
            "WARNING: Unable to validate external_executor_id length in offline mode. "
            "Downgrade may fail if values exceed 250 characters."
        )
    else:
        for table_name in ("task_instance", "task_instance_history"):
            too_long_count = conn.execute(
                text(
                    f"""
                    SELECT count(*)
                    FROM {table_name}
                    WHERE external_executor_id IS NOT NULL
                    AND length(external_executor_id) > 250
                    """
                )
            ).scalar_one()
            if too_long_count:
                raise ValueError(
                    f"Cannot downgrade: {table_name}.external_executor_id has {too_long_count} rows longer "
                    "than 250 characters."
                )

    for table_name in ("task_instance_history", "task_instance"):
        temp_column = "external_executor_id_varchar"
        op.add_column(table_name, sa.Column(temp_column, sa.VARCHAR(length=250), nullable=True))
        conn.execute(
            text(
                f"""
                UPDATE {table_name}
                SET {temp_column} = external_executor_id
                WHERE external_executor_id IS NOT NULL
                """
            )
        )
        with op.batch_alter_table(table_name, schema=None) as batch_op:
            batch_op.drop_column("external_executor_id")
            batch_op.alter_column(temp_column, new_column_name="external_executor_id")
