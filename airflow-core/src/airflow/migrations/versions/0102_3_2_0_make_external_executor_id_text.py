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
    dialect = conn.dialect.name

    _downgrade_external_executor_id_table("task_instance_history", dialect)
    _downgrade_external_executor_id_table("task_instance", dialect)


def _downgrade_external_executor_id_table(table_name: str, dialect: str) -> None:
    if dialect == "postgresql":
        if context.is_offline_mode():
            # In offline mode we cannot safely validate data length prior to narrowing the column.
            # Emit direct SQL so generated scripts still include the expected downgrade behavior.
            op.execute(text(f"ALTER TABLE {table_name} ALTER COLUMN external_executor_id TYPE VARCHAR(250)"))
            return

        _validate_external_executor_id_length(table_name)

        new_column_name = "external_executor_id_varchar_250"
        op.add_column(table_name, sa.Column(new_column_name, sa.String(length=250), nullable=True))
        op.execute(
            text(
                f"""
                UPDATE {table_name}
                SET {new_column_name} = external_executor_id
                WHERE external_executor_id IS NOT NULL
                """
            )
        )
        op.drop_column(table_name, "external_executor_id")
        op.alter_column(
            table_name,
            new_column_name,
            existing_type=sa.String(length=250),
            nullable=True,
            new_column_name="external_executor_id",
        )
        return

    with op.batch_alter_table(table_name, schema=None) as batch_op:
        batch_op.alter_column(
            "external_executor_id",
            existing_type=sa.Text(),
            type_=sa.VARCHAR(length=250),
            existing_nullable=True,
        )


def _validate_external_executor_id_length(table_name: str) -> None:
    conn = op.get_bind()
    too_long_rows = conn.execute(
        text(
            f"""
            SELECT COUNT(*)
            FROM {table_name}
            WHERE external_executor_id IS NOT NULL
              AND LENGTH(external_executor_id) > 250
            """
        )
    ).scalar()
    if too_long_rows:
        raise RuntimeError(
            f"Cannot downgrade {table_name}.external_executor_id to VARCHAR(250): "
            f"found {too_long_rows} rows exceeding 250 characters."
        )
