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

from datetime import timedelta
from unittest import mock

import pytest
from sqlalchemy import func, select

from airflow._shared.timezones import timezone
from airflow.models.dag import DagModel
from airflow.models.dag_version import DagVersion
from airflow.models.dagbundle import DagBundleModel
from airflow.providers.standard.operators.empty import EmptyOperator

from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.db import clear_db_dag_bundles, clear_db_dags

pytestmark = pytest.mark.db_test


class TestDagVersion:
    def setup_method(self):
        clear_db_dags()

    def teardown_method(self):
        # clear_db_dags() first: DagModel.bundle_name has an FK to dag_bundle.
        clear_db_dags()
        clear_db_dag_bundles()

    @pytest.mark.need_serialized_dag
    def test_writing_dag_version(self, dag_maker, session):
        with dag_maker("test_writing_dag_version") as dag:
            pass

        latest_version = DagVersion.get_latest_version(dag.dag_id)
        assert latest_version.version_number == 1
        assert latest_version.dag_id == dag.dag_id

    def test_writing_dag_version_with_changes(self, dag_maker, session):
        """This also tested the get_latest_version method"""
        with dag_maker("test1") as dag:
            EmptyOperator(task_id="task1")
        sync_dag_to_db(dag)
        dag_maker.create_dagrun()
        # Add extra task to change the dag
        with dag_maker("test1") as dag2:
            EmptyOperator(task_id="task1")
            EmptyOperator(task_id="task2")
        sync_dag_to_db(dag2)
        latest_version = DagVersion.get_latest_version(dag.dag_id)
        assert latest_version.version_number == 2
        assert session.scalar(select(func.count()).where(DagVersion.dag_id == dag.dag_id)) == 2

    @staticmethod
    def _seed_two_versions_with_inverted_created_at(session, *, dag_id):
        """Create versions 1 and 2 where version 2 has an *earlier* created_at than version 1.

        This makes created_at ordering disagree with version_number ordering, modelling the
        timestamp tie / clock-skew case the ordering must be robust to. Returns the bundle name.
        """
        bundle_name = f"bundle-{dag_id}"
        session.add(DagBundleModel(name=bundle_name))
        session.flush()
        session.add(DagModel(dag_id=dag_id, bundle_name=bundle_name))
        session.flush()

        base = timezone.utcnow()
        for version_number, created_at in ((1, base), (2, base - timedelta(minutes=1))):
            session.add(
                DagVersion(
                    dag_id=dag_id,
                    version_number=version_number,
                    bundle_name=bundle_name,
                    created_at=created_at,
                    last_updated=created_at,
                )
            )
        session.commit()
        return bundle_name

    def test_latest_version_uses_version_number_not_created_at(self, session):
        """The latest version is the one with the highest version_number, not the latest created_at."""
        dag_id = "test_latest_ordering"
        self._seed_two_versions_with_inverted_created_at(session, dag_id=dag_id)

        assert DagVersion.get_latest_version(dag_id, session=session).version_number == 2
        assert DagVersion.get_version(dag_id, session=session).version_number == 2

    def test_write_dag_increments_from_max_version_number(self, session):
        """write_dag must increment from the max version_number, not the latest-created row.

        Otherwise, when created_at ordering disagrees with version_number ordering, it would
        recompute an already-used version_number and violate the (dag_id, version_number) unique
        constraint.
        """
        dag_id = "test_write_dag_increment"
        bundle_name = self._seed_two_versions_with_inverted_created_at(session, dag_id=dag_id)

        new_version = DagVersion.write_dag(dag_id=dag_id, bundle_name=bundle_name, session=session)
        session.commit()

        assert new_version.version_number == 3
        assert session.scalar(select(func.count()).where(DagVersion.dag_id == dag_id)) == 3

    @pytest.mark.need_serialized_dag
    def test_get_version(self, dag_maker, session):
        """The two dags have the same version name and number but different dag ids"""
        dag1_id = "test1"
        with dag_maker(dag1_id):
            EmptyOperator(task_id="task1")

        with dag_maker("test2"):
            EmptyOperator(task_id="task1")

        with dag_maker("test3"):
            EmptyOperator(task_id="task1")

        version = DagVersion.get_version(dag1_id)
        assert version.version_number == 1
        assert version.dag_id == dag1_id
        assert version.version == f"{dag1_id}-1"

    @pytest.mark.need_serialized_dag
    def test_version_property(self, dag_maker):
        with dag_maker("test1") as dag:
            pass

        latest_version = DagVersion.get_latest_version(dag.dag_id)
        assert latest_version.version == f"{dag.dag_id}-1"

    @pytest.mark.db_test
    def test_write_dag_with_version_data(self, dag_maker, session):
        """Test that version_data is stored and retrievable."""
        with dag_maker("test_version_data"):
            pass

        manifest = {"schema_version": 1, "files": {"dags/my_dag.py": "S3VersionId123"}}
        DagVersion.write_dag(
            dag_id="test_version_data",
            bundle_name="testing",
            bundle_version="sha256abc",
            version_data=manifest,
            session=session,
        )
        session.flush()

        retrieved = DagVersion.get_latest_version("test_version_data", session=session)
        assert retrieved.version_data == manifest
        assert retrieved.bundle_version == "sha256abc"

    @pytest.mark.db_test
    def test_write_dag_without_version_data(self, dag_maker, session):
        """Test that version_data defaults to None for bundles that don't use it."""
        with dag_maker("test_no_version_data"):
            pass

        DagVersion.write_dag(
            dag_id="test_no_version_data",
            bundle_name="testing",
            bundle_version="abc123",
            session=session,
        )
        session.flush()

        retrieved = DagVersion.get_latest_version("test_no_version_data", session=session)
        assert retrieved.version_data is None
        assert retrieved.bundle_version == "abc123"


class TestResolveVersionData:
    """Unit tests for the _resolve_version_data guard.

    The invariant under test: the returned manifest describes the ``bundle_version`` passed in, or
    is None. It is never some other version's manifest.
    """

    @pytest.mark.parametrize(
        ("hint", "bundle_version", "expected"),
        [
            pytest.param(
                mock.Mock(bundle_version="abc123", version_data={"schema_version": 1}),
                "abc123",
                {"schema_version": 1},
                id="hint-matches-pin",
            ),
            pytest.param(
                mock.Mock(bundle_version="abc123", version_data={"schema_version": 1}),
                None,
                None,
                id="unpinned-suppresses-present-data",
            ),
            pytest.param(
                mock.Mock(bundle_version="def456", version_data={"schema_version": 1}),
                "abc123",
                None,
                id="hint-pointer-moved-on",
            ),
            pytest.param(
                mock.Mock(bundle_version=None, version_data={"schema_version": 1}),
                "abc123",
                None,
                id="hint-pointer-unreadable-on-transient-object",
            ),
            pytest.param(None, "abc123", None, id="missing-dag-version"),
            pytest.param(None, None, None, id="unpinned-and-missing"),
        ],
    )
    def test_resolve_version_data_without_session_is_hint_only(self, hint, bundle_version, expected):
        from airflow.models.dag_version import _resolve_version_data

        assert _resolve_version_data("some_dag", bundle_version, hint=hint) == expected

    def test_unpinned_run_does_not_query(self):
        """An unpinned run short-circuits before the recovery lookup, even with a session in hand."""
        from airflow.models.dag_version import _resolve_version_data

        session = mock.Mock()
        assert _resolve_version_data("some_dag", None, session=session) is None
        session.scalar.assert_not_called()

    def test_matching_hint_does_not_query(self):
        """The fast path must avoid a query per call; this runs per task instance in the scheduler."""
        from airflow.models.dag_version import _resolve_version_data

        session = mock.Mock()
        hint = mock.Mock(bundle_version="abc123", version_data={"schema_version": 1})

        assert _resolve_version_data("some_dag", "abc123", hint=hint, session=session) == {
            "schema_version": 1
        }
        session.scalar.assert_not_called()


class TestGetVersionData:
    """Unit tests for DagVersion.get_version_data recovery lookups."""

    def setup_method(self):
        clear_db_dags()

    def teardown_method(self):
        clear_db_dags()

    def test_recovers_manifest_from_historical_row(self, dag_maker, session):
        """The latest row's pointer is refreshed in place when the bundle advances with no Dag
        change, but an older row still records the version an in-flight pinned run needs.
        """
        v1_manifest = {"schema_version": 1, "files": {"dags/my_dag.py": "v1-object-id"}}
        with dag_maker("test_recover_manifest"):
            pass

        latest = DagVersion.get_latest_version("test_recover_manifest", session=session)
        latest.bundle_version = "v1hash"
        latest.version_data = v1_manifest
        session.flush()

        # A Dag change mints version 2 at v2hash; version 1 keeps v1hash's manifest.
        DagVersion.write_dag(
            dag_id="test_recover_manifest",
            bundle_name="testing",
            bundle_version="v2hash",
            version_data={"schema_version": 1, "files": {"dags/my_dag.py": "v2-object-id"}},
            session=session,
        )
        session.flush()

        assert DagVersion.get_version_data("test_recover_manifest", "v1hash", session=session) == v1_manifest

    def test_returns_none_when_no_row_records_the_version(self, dag_maker, session):
        """Recovery is best-effort: if the only row holding the version moved on, nothing holds it."""
        with dag_maker("test_manifest_lost"):
            pass

        latest = DagVersion.get_latest_version("test_manifest_lost", session=session)
        latest.bundle_version = "v2hash"
        latest.version_data = {"schema_version": 1, "files": {"dags/my_dag.py": "v2-object-id"}}
        session.flush()

        assert DagVersion.get_version_data("test_manifest_lost", "v1hash", session=session) is None

    def test_skips_rows_without_a_manifest(self, dag_maker, session):
        """Bundles that record no manifest (every in-tree bundle today) resolve to None, not a row."""
        with dag_maker("test_no_manifest"):
            pass

        latest = DagVersion.get_latest_version("test_no_manifest", session=session)
        latest.bundle_version = "v1hash"
        session.flush()

        assert DagVersion.get_version_data("test_no_manifest", "v1hash", session=session) is None

    def test_scoped_to_the_requested_dag(self, dag_maker, session):
        """A manifest recorded for another Dag at the same bundle version must not leak across."""
        with dag_maker("test_other_dag"):
            pass
        other = DagVersion.get_latest_version("test_other_dag", session=session)
        other.bundle_version = "v1hash"
        other.version_data = {"schema_version": 1, "files": {"dags/other.py": "v1-object-id"}}
        session.flush()

        assert DagVersion.get_version_data("test_missing_dag", "v1hash", session=session) is None
