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

import contextlib
import json
import re
from collections.abc import Iterable
from contextlib import contextmanager
from textwrap import dedent
from typing import Any

import sqlalchemy as sa


@contextmanager
def disable_sqlite_fkeys(op):
    if op.get_bind().dialect.name == "sqlite":
        op.execute("PRAGMA foreign_keys=off")
        yield op
        op.execute("PRAGMA foreign_keys=on")
    else:
        yield op


# Unlike disable_sqlite_fkeys, a failed SQLite upgrade rolls back atomically and foreign_keys is always restored.
@contextmanager
def sqlite_rebuilds(op):
    if op.get_bind().dialect.name != "sqlite":
        yield
        return
    if op.get_context().as_sql:
        raise RuntimeError("SQLite offline SQL cannot render this migration's table rebuilds")
    enabled = op.get_bind().exec_driver_sql("PRAGMA foreign_keys").scalar()
    with op.get_context().autocommit_block():
        op.execute("PRAGMA foreign_keys=OFF")
    try:
        with op.get_bind().begin_nested():
            yield
    finally:
        with op.get_context().autocommit_block():
            op.execute(f"PRAGMA foreign_keys={int(enabled)}")


def mysql_drop_foreignkey_if_exists(constraint_name, table_name, op):
    """Older Mysql versions do not support DROP FOREIGN KEY IF EXISTS."""
    op.execute(f"""
    CREATE PROCEDURE DropForeignKeyIfExists()
    BEGIN
        IF EXISTS (
            SELECT 1
            FROM information_schema.TABLE_CONSTRAINTS
            WHERE
                CONSTRAINT_SCHEMA = DATABASE() AND
                TABLE_NAME = '{table_name}' AND
                CONSTRAINT_NAME = '{constraint_name}' AND
                CONSTRAINT_TYPE = 'FOREIGN KEY'
        ) THEN
            ALTER TABLE `{table_name}`
            DROP CONSTRAINT `{constraint_name}`;
        ELSE
            SELECT 1;
        END IF;
    END;
    CALL DropForeignKeyIfExists();
    DROP PROCEDURE DropForeignKeyIfExists;
    """)


def raise_if_rows_exist(select_sql: str, message: str, op) -> None:
    """
    Abort the migration when ``select_sql`` returns a row, using SQL so it also runs offline.

    Call it before any DDL: MySQL DDL is not transactional, so a late failure leaves a half-applied
    migration. ``message`` must not contain single quotes or percent signs and, for MySQL, must be at
    most 128 characters. PostgreSQL and MySQL only.
    """
    dialect = op.get_context().dialect.name
    if dialect == "postgresql":
        op.execute(
            dedent("""
                DO $$
                BEGIN
                    IF EXISTS ({select_sql}) THEN
                        RAISE EXCEPTION '{message}';
                    END IF;
                END $$
                """).format(select_sql=select_sql, message=message)
        )
    elif dialect == "mysql":
        create = (
            dedent("""
            CREATE PROCEDURE AirflowMigrationGuard()
            BEGIN
                IF EXISTS ({select_sql}) THEN
                    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '{message}';
                END IF;
            END
            """)
            .strip()
            .format(select_sql=select_sql, message=message)
        )
        context = op.get_context()
        if context.as_sql:
            # The mysql client splits on ';' unless the procedure body is fenced by DELIMITER.
            context.output_buffer.write(
                "DROP PROCEDURE IF EXISTS AirflowMigrationGuard;\n"
                f"DELIMITER //\n{create}//\nDELIMITER ;\n"
                "CALL AirflowMigrationGuard();\nDROP PROCEDURE AirflowMigrationGuard;\n\n"
            )
        else:
            op.execute("DROP PROCEDURE IF EXISTS AirflowMigrationGuard")
            op.execute(create)
            try:
                op.execute("CALL AirflowMigrationGuard()")
            finally:
                op.execute("DROP PROCEDURE AirflowMigrationGuard")
    else:
        raise NotImplementedError(f"No SQL guard for dialect {dialect}")


def ignore_sqlite_value_error():
    from alembic import op

    if op.get_bind().dialect.name == "sqlite":
        return contextlib.suppress(ValueError)
    return contextlib.nullcontext()


_SERIALIZED_DAG_BATCH_SIZE = 1000


def _sort_serialized_dag_dict(serialized_dag: Any) -> Any:
    if isinstance(serialized_dag, dict):
        return {k: _sort_serialized_dag_dict(v) for k, v in sorted(serialized_dag.items())}
    if isinstance(serialized_dag, list):
        if all(isinstance(i, dict) for i in serialized_dag):
            if all(
                isinstance(i.get("__var", {}), Iterable) and "task_id" in i.get("__var", {})
                for i in serialized_dag
            ):
                return sorted(
                    [_sort_serialized_dag_dict(i) for i in serialized_dag],
                    key=lambda x: x["__var"]["task_id"],
                )
        elif all(isinstance(item, str) for item in serialized_dag):
            return sorted(serialized_dag)
        return [_sort_serialized_dag_dict(i) for i in serialized_dag]
    return serialized_dag


def hash_serialized_dag(data: dict[str, Any]) -> str:
    """
    Compute ``serialized_dag.dag_hash`` the way the release that reads *data* does.

    A frozen copy of ``SerializedDagModel.hash``: migrations must not import the model, which changes
    over time. Releases that write a date stamp leave ``__version`` out of the hash; releases up to
    3.4, which write an integer stamp, include it.
    """
    from airflow.utils.hashlib_wrapper import md5

    data_ = _sort_serialized_dag_dict(data)
    data_["dag"].pop("fileloc", None)
    data_["dag"].pop("bundle_name", None)
    if not isinstance(data_.get("__version"), int):
        data_.pop("__version", None)
    return md5(json.dumps(data_, sort_keys=True).encode("utf-8")).hexdigest()


def _get_destination_airflow_version() -> str | None:
    """Return the Airflow release of alembic's downgrade destination, or None below every revision."""
    from alembic import context

    script = context.script
    destination = context.get_revision_argument()
    if isinstance(destination, (list, tuple)):
        if len(destination) != 1:
            raise ValueError(f"Cannot downgrade serialized Dags to several revisions {destination!r}")
        destination = destination[0]
    if destination in (None, "base"):
        return None
    if isinstance(destination, str) and re.fullmatch(r"-\d+", destination):
        # A relative step, such as "-1", counts down from the revision the database is at.
        revision = script.get_revision(context.get_context().get_current_revision())
        for _ in range(int(destination[1:])):
            if revision is None or revision.down_revision is None:
                return None
            revision = script.get_revision(revision.down_revision)
    else:
        revision = script.get_revision(destination)
    if revision is None:
        return None
    return revision.module.airflow_version


def get_serialized_dag_downgrade_target(*, below: int | str | None = None) -> int | str:
    """
    Return the serialized Dag version an alembic downgrade must bring stored Dags to.

    It is the version the destination release reads, but a version above it holding a change with
    ``requires_migration`` stops the downgrade there: that change's own, older migration converts its
    part and then continues the downgrade, passing its version as *below* so the stop is not hit again.
    """
    from airflow.serialization import versions

    airflow_version = _get_destination_airflow_version()
    min_index = versions.VERSIONS.index(versions.get_version(versions.MIN_DOWNGRADE_VERSION))
    # Below 3.0.0 migration 0047 deletes every serialized Dag, so version 2 is far enough.
    target_index = (
        min_index
        if airflow_version is None
        else max(versions.VERSIONS.index(versions.get_version_for_airflow(airflow_version)), min_index)
    )
    stop_index = (
        len(versions.VERSIONS) if below is None else versions.VERSIONS.index(versions.get_version(below))
    )
    for index in range(stop_index - 1, target_index, -1):
        if any(change.requires_migration for change in versions.VERSIONS[index].changes):
            return versions.VERSIONS[index].id
    return versions.VERSIONS[target_index].id


def _get_serialized_dag_data_type(conn) -> str | None:
    if conn.dialect.name != "postgresql":
        return None
    # 0089 changed the column to jsonb in 3.2.0; downgrading past it turns it back into json.
    return conn.execute(
        sa.text(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = 'serialized_dag' AND column_name = 'data'"
        )
    ).scalar()


def downgrade_serialized_dags(conn, *, to: int | str) -> None:
    """
    Downgrade every stored serialized Dag to format version *to*, and recompute its ``dag_hash``.

    Uses plain SQL on the columns every release has (``id``, ``data``, ``data_compressed``,
    ``dag_hash``), so it can run from any migration. A row whose format this release does not know is
    left as it is and logged.
    """
    import zlib

    import structlog

    from airflow.serialization import versions

    log = structlog.get_logger(__name__)
    data_type = _get_serialized_dag_data_type(conn)
    set_data = f"CAST(:data AS {data_type})" if data_type else ":data"
    select_ids = sa.text("SELECT id FROM serialized_dag WHERE id > :last_id ORDER BY id LIMIT :batch_size")
    select_rows = sa.text(
        "SELECT id, data, data_compressed, dag_hash FROM serialized_dag WHERE id IN :ids"
    ).bindparams(sa.bindparam("ids", expanding=True))
    update_data = sa.text(f"UPDATE serialized_dag SET data = {set_data}, dag_hash = :dag_hash WHERE id = :id")
    update_compressed = sa.text(
        "UPDATE serialized_dag SET data_compressed = :data, dag_hash = :dag_hash WHERE id = :id"
    )

    last_id = "00000000-0000-0000-0000-000000000000"
    downgraded = 0
    while ids := [
        row.id
        for row in conn.execute(select_ids, {"last_id": last_id, "batch_size": _SERIALIZED_DAG_BATCH_SIZE})
    ]:
        last_id = str(ids[-1])
        for row in conn.execute(select_rows, {"ids": ids}):
            if row.data_compressed is not None:
                blob = json.loads(zlib.decompress(row.data_compressed))
            elif row.data is not None:
                blob = row.data if isinstance(row.data, dict) else json.loads(row.data)
            else:
                continue
            original = json.dumps(blob, sort_keys=True)
            try:
                versions.downgrade(blob, to=to)
            except ValueError as e:
                log.warning("Cannot downgrade serialized Dag", serialized_dag_id=str(row.id), error=str(e))
                continue
            if json.dumps(blob, sort_keys=True) == original:
                continue
            params = {"id": row.id, "dag_hash": hash_serialized_dag(blob)}
            if row.data_compressed is not None:
                conn.execute(
                    update_compressed, {**params, "data": zlib.compress(json.dumps(blob).encode("utf-8"))}
                )
            else:
                conn.execute(update_data, {**params, "data": json.dumps(blob)})
            downgraded += 1
    log.info("Downgraded serialized Dags", to=to, count=downgraded)
