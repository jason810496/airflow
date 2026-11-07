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
"""Tests for the shared serialized Dag downgrade used by alembic migrations."""

from __future__ import annotations

import json
import zlib
from typing import Any
from unittest import mock

import alembic.context
import pytest
import sqlalchemy as sa
from alembic.script import ScriptDirectory

from airflow import settings
from airflow.migrations.utils import (
    downgrade_serialized_dags,
    get_serialized_dag_downgrade_target,
    hash_serialized_dag,
)
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils.db import _REVISION_HEADS_MAP, _get_alembic_config, downgrade, upgradedb

HEAD_STAMP = "2027-02-28"


def _build_blob(dag_id: str) -> dict[str, Any]:
    return {
        "__version": HEAD_STAMP,
        "dag": {
            "dag_id": dag_id,
            "fileloc": f"/dags/{dag_id}.py",
            "bundle_name": "dags-folder",
            "rerun_with_latest_version": True,
            "tasks": [{"__type": "operator", "__var": {"task_id": "t", "returns_dag_result": True}}],
        },
    }


@pytest.fixture
def connection():
    engine = sa.create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "CREATE TABLE serialized_dag ("
                "id VARCHAR(32) PRIMARY KEY, data JSON, data_compressed BLOB, dag_hash VARCHAR(32))"
            )
        )
        yield conn


def _insert(conn, row_id: str, blob: dict[str, Any] | None, *, compressed: bool) -> None:
    conn.execute(
        sa.text("INSERT INTO serialized_dag VALUES (:id, :data, :data_compressed, 'stale')"),
        {
            "id": row_id,
            "data": None if compressed or blob is None else json.dumps(blob),
            "data_compressed": zlib.compress(json.dumps(blob).encode()) if compressed and blob else None,
        },
    )


def _read(conn, row_id: str) -> tuple[dict[str, Any] | None, str]:
    row = conn.execute(
        sa.text("SELECT data, data_compressed, dag_hash FROM serialized_dag WHERE id = :id"), {"id": row_id}
    ).one()
    if row.data_compressed is not None:
        return json.loads(zlib.decompress(row.data_compressed)), row.dag_hash
    return (json.loads(row.data) if row.data is not None else None), row.dag_hash


class TestDowngradeSerializedDags:
    @pytest.mark.parametrize("compressed", [False, True], ids=["raw", "compressed"])
    def test_downgrades_row_and_recomputes_hash(self, connection, compressed):
        _insert(connection, "a" * 32, _build_blob("a"), compressed=compressed)

        downgrade_serialized_dags(connection, to="2026-06-30")

        blob, dag_hash = _read(connection, "a" * 32)
        assert blob == {
            "__version": 3,
            "dag": {
                "dag_id": "a",
                "fileloc": "/dags/a.py",
                "rerun_with_latest_version": True,
                "tasks": [{"__type": "operator", "__var": {"task_id": "t"}}],
            },
        }
        assert dag_hash == hash_serialized_dag(blob)

    def test_processes_rows_in_batches(self, connection):
        row_ids = [f"{i:032x}" for i in range(5)]
        for row_id in row_ids:
            _insert(connection, row_id, _build_blob(row_id), compressed=False)

        with mock.patch("airflow.migrations.utils._SERIALIZED_DAG_BATCH_SIZE", 2):
            downgrade_serialized_dags(connection, to=3)

        assert {_read(connection, row_id)[0]["__version"] for row_id in row_ids} == {3}

    def test_leaves_unchanged_and_unknown_rows_alone(self, connection):
        _insert(connection, "a" * 32, {"__version": 2, "dag": {"dag_id": "old"}}, compressed=False)
        _insert(connection, "b" * 32, {"__version": 99, "dag": {"dag_id": "future"}}, compressed=False)
        _insert(connection, "c" * 32, None, compressed=False)

        downgrade_serialized_dags(connection, to=3)

        assert _read(connection, "a" * 32) == ({"__version": 2, "dag": {"dag_id": "old"}}, "stale")
        assert _read(connection, "b" * 32) == ({"__version": 99, "dag": {"dag_id": "future"}}, "stale")
        assert _read(connection, "c" * 32) == (None, "stale")


class TestHashSerializedDag:
    def test_matches_model_hash_for_date_stamp(self):
        blob = _build_blob("a")

        assert hash_serialized_dag(blob) == SerializedDagModel.hash(blob)

    def test_keeps_integer_stamp(self):
        # Releases up to 3.4 hash __version, so their first parse after a downgrade finds no change.
        blob = {**_build_blob("a"), "__version": 3}

        assert hash_serialized_dag(blob) != hash_serialized_dag({**blob, "__version": 2})
        assert hash_serialized_dag(blob) != SerializedDagModel.hash(blob)


@pytest.fixture
def script() -> ScriptDirectory:
    return ScriptDirectory.from_config(_get_alembic_config())


def _resolve(script, destination, *, below=None, current=None):
    migration_context = mock.Mock(**{"get_current_revision.return_value": current})
    with (
        mock.patch.object(alembic.context, "script", script, create=True),
        mock.patch.object(alembic.context, "get_revision_argument", return_value=destination, create=True),
        mock.patch.object(alembic.context, "get_context", return_value=migration_context, create=True),
    ):
        return get_serialized_dag_downgrade_target(below=below)


@pytest.mark.parametrize(
    ("destination", "below", "expected"),
    [
        pytest.param(_REVISION_HEADS_MAP["3.4.0"], None, "2026-10-30", id="3.4.0"),
        pytest.param(_REVISION_HEADS_MAP["3.3.0"], None, "2026-06-30", id="3.3.0"),
        pytest.param(_REVISION_HEADS_MAP["3.2.0"], None, "2026-04-06", id="3.2.0"),
        # Migration 0101 holds a change of 2026-04-06, so the 3.5 migration stops there.
        pytest.param(_REVISION_HEADS_MAP["3.1.8"], None, "2026-04-06", id="3.1.8-stops"),
        pytest.param(_REVISION_HEADS_MAP["3.1.0"], None, "2026-04-06", id="3.1.0-stops"),
        pytest.param(_REVISION_HEADS_MAP["3.0.0"], None, "2026-04-06", id="3.0.0-stops"),
        # ... and 0101 then finishes the downgrade.
        pytest.param(_REVISION_HEADS_MAP["3.1.8"], "2026-04-06", "2025-11-05", id="3.1.8-from-0101"),
        pytest.param(_REVISION_HEADS_MAP["3.1.0"], "2026-04-06", 3, id="3.1.0-from-0101"),
        pytest.param(_REVISION_HEADS_MAP["3.0.0"], "2026-04-06", 2, id="3.0.0-from-0101"),
        pytest.param(_REVISION_HEADS_MAP["2.10.3"], "2026-04-06", 2, id="airflow-2-from-0101"),
        pytest.param("base", "2026-04-06", 2, id="base-from-0101"),
        pytest.param((_REVISION_HEADS_MAP["3.4.0"],), None, "2026-10-30", id="tuple"),
    ],
)
def test_get_serialized_dag_downgrade_target(script, destination, below, expected):
    assert _resolve(script, destination, below=below) == expected


def test_get_serialized_dag_downgrade_target_resolves_relative_step(script):
    current = _REVISION_HEADS_MAP["3.4.0"]
    one_down = script.get_revision(current).down_revision

    assert _resolve(script, "-1", current=current) == "2026-10-30"
    assert script.get_revision(one_down).module.airflow_version == "3.4.0"


def test_get_serialized_dag_downgrade_target_rejects_several_destinations(script):
    with pytest.raises(ValueError, match="several revisions"):
        _resolve(script, ("a", "b"))


@pytest.mark.db_test
class TestDowngradeMigration:
    @pytest.fixture
    def stored_row_id(self, dag_maker, session):
        with dag_maker("serialized_dag_downgrade", session=session):
            pass
        session.commit()
        row_id = session.scalar(
            sa.select(SerializedDagModel.id).where(SerializedDagModel.dag_id == "serialized_dag_downgrade")
        )
        session.execute(
            sa.update(SerializedDagModel.__table__)
            .where(SerializedDagModel.__table__.c.id == row_id)
            .values(data=_build_blob("serialized_dag_downgrade"), data_compressed=None)
        )
        session.commit()
        return row_id

    def test_downgrade_to_3_3_drops_newer_fields(self, stored_row_id):
        original_async_conn = settings.SQL_ALCHEMY_CONN_ASYNC
        settings.SQL_ALCHEMY_CONN_ASYNC = ""
        settings.dispose_orm(do_log=False)
        settings.configure_orm(disable_connection_pool=True)
        try:
            downgrade(to_revision=_REVISION_HEADS_MAP["3.3.0"])
            with settings.engine.connect() as conn:
                row = conn.execute(
                    sa.text("SELECT data, dag_hash FROM serialized_dag WHERE id = :id"),
                    {"id": stored_row_id.hex},
                ).one()
        finally:
            upgradedb()
            settings.SQL_ALCHEMY_CONN_ASYNC = original_async_conn
            settings.reconfigure_orm()

        blob = json.loads(row.data) if isinstance(row.data, str) else row.data
        assert blob["__version"] == 3
        assert "bundle_name" not in blob["dag"]
        assert "returns_dag_result" not in blob["dag"]["tasks"][0]["__var"]
        assert blob["dag"]["rerun_with_latest_version"] is True
        assert row.dag_hash == hash_serialized_dag(blob)
