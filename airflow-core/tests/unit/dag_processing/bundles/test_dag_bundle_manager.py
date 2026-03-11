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

import json
import os
from contextlib import nullcontext
from unittest import mock
from unittest.mock import patch

import pytest
from sqlalchemy import func, select

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.exceptions import AirflowConfigException
from airflow.models.dag import DagModel
from airflow.models.dagbundle import DagBundleModel
from airflow.models.errors import ParseImportError

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dag_bundles, clear_db_dags

# ---------------------------------------------------------------------------
# Test bundle implementations
# ---------------------------------------------------------------------------


class BasicBundle(BaseDagBundle):
    def refresh(self):
        pass

    def get_current_version(self):
        pass

    def path(self):
        pass


class BundleWithTemplate(BaseDagBundle):
    """Test bundle that provides a URL template."""

    def __init__(self, *, subdir: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self.subdir = subdir

    def refresh(self):
        pass

    def get_current_version(self):
        return "v1.0"

    @property
    def path(self):
        return "/tmp/test"


class FailingBundle(BaseDagBundle):
    """Test bundle that raises an exception during initialization."""

    def __init__(self, *, should_fail: bool = True, **kwargs):
        super().__init__(**kwargs)
        if should_fail:
            raise ValueError("Bundle creation failed for testing")

    def refresh(self):
        pass

    def get_current_version(self):
        return None

    @property
    def path(self):
        return "/tmp/failing"


# ---------------------------------------------------------------------------
# Shared bundle config dicts
# ---------------------------------------------------------------------------

BASIC_BUNDLE_CONFIG = [
    {
        "name": "my-test-bundle",
        "classpath": "unit.dag_processing.bundles.test_dag_bundle_manager.BasicBundle",
        "kwargs": {"refresh_interval": 1},
    }
]

SECOND_BUNDLE_CONFIG = [
    {
        "name": "second-bundle",
        "classpath": "unit.dag_processing.bundles.test_dag_bundle_manager.BasicBundle",
        "kwargs": {"refresh_interval": 1},
    }
]

TEMPLATE_BUNDLE_CONFIG = [
    {
        "name": "template-bundle",
        "classpath": "unit.dag_processing.bundles.test_dag_bundle_manager.BundleWithTemplate",
        "kwargs": {
            "view_url_template": "https://github.com/example/repo/tree/{version}/{subdir}",
            "subdir": "dags",
            "refresh_interval": 1,
        },
    }
]

FAILING_BUNDLE_CONFIG = [
    {
        "name": "failing-bundle",
        "classpath": "unit.dag_processing.bundles.test_dag_bundle_manager.FailingBundle",
        "kwargs": {"should_fail": True, "refresh_interval": 1},
    }
]


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def clear_db():
    clear_db_dag_bundles()
    yield
    clear_db_dag_bundles()


@pytest.fixture
def clear_dags_and_bundles():
    clear_db_dags()
    clear_db_dag_bundles()
    yield
    clear_db_dags()
    clear_db_dag_bundles()


def _add_dag(session, dag_id: str, bundle_name: str) -> DagModel:
    dag = DagModel(dag_id=dag_id, bundle_name=bundle_name, fileloc=f"/tmp/{dag_id}.py")
    session.add(dag)
    session.flush()
    return dag


# ---------------------------------------------------------------------------
# TestParseConfig
# ---------------------------------------------------------------------------


class TestParseConfig:
    """Tests for DagBundlesManager.parse_config()."""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            pytest.param(None, {"dags-folder"}, id="default"),
            pytest.param(
                json.dumps(
                    [
                        {
                            "name": "my-bundle",
                            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                            "kwargs": {"path": "/tmp/hihi", "refresh_interval": 1},
                        }
                    ]
                ),
                {"my-bundle"},
                id="remove_dags_folder_default_add_bundle",
            ),
            pytest.param(
                json.dumps(
                    [
                        {
                            "name": "my-bundle",
                            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                            "kwargs": {"path": "/tmp/hihi", "refresh_interval": 1},
                            "team_name": "test",
                        }
                    ]
                ),
                "cannot have a team name when multi-team mode is disabled.",
                id="add_bundle_with_team",
            ),
            pytest.param("1", "key `dag_bundle_config_list` must be list", id="int"),
            pytest.param("abc", "Unable to parse .* as valid json", id="not_json"),
        ],
    )
    def test_parse_bundle_config(self, value, expected):
        """Test that bundle_configs are read from configuration."""
        envs = {"AIRFLOW__CORE__LOAD_EXAMPLES": "False"}
        if value:
            envs["AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST"] = value
        cm = nullcontext()
        exp_fail = False
        if isinstance(expected, str):
            exp_fail = True
            cm = pytest.raises(AirflowConfigException, match=expected)

        with patch.dict(os.environ, envs), cm:
            bundle_manager = DagBundlesManager()
            names = set(x.name for x in bundle_manager.get_all_dag_bundles())

        if not exp_fail:
            assert names == expected

    @pytest.mark.parametrize(
        "value",
        [
            pytest.param("{}", id="empty_dict"),
            pytest.param("[]", id="empty_list"),
        ],
    )
    def test_parse_config_raises_on_empty(self, value):
        """Empty or falsy dag_bundle_config_list now raises AirflowConfigException."""
        envs = {
            "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
            "AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": value,
        }
        with patch.dict(os.environ, envs):
            with pytest.raises(AirflowConfigException, match="is not set or empty"):
                DagBundlesManager()

    def test_example_dags_bundle_added(self):
        manager = DagBundlesManager()
        manager.parse_config()
        assert "example_dags" in manager._bundle_config

        with conf_vars({("core", "LOAD_EXAMPLES"): "False"}):
            manager = DagBundlesManager()
            manager.parse_config()
            assert "example_dags" not in manager._bundle_config

    def test_example_dags_name_is_reserved(self):
        reserved_name_config = [{"name": "example_dags", "classpath": "yo face", "kwargs": {}}]
        with conf_vars({("dag_processor", "dag_bundle_config_list"): json.dumps(reserved_name_config)}):
            with pytest.raises(
                AirflowConfigException, match="Bundle name 'example_dags' is a reserved name."
            ):
                DagBundlesManager().parse_config()


# ---------------------------------------------------------------------------
# TestBundleNames
# ---------------------------------------------------------------------------


class TestBundleNames:
    """Tests for DagBundlesManager.bundle_names property."""

    def test_bundle_names_returns_configured_names(self):
        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(BASIC_BUNDLE_CONFIG)},
        ):
            manager = DagBundlesManager()
            assert manager.bundle_names == ["my-test-bundle", "example_dags"]

    @conf_vars({("core", "LOAD_EXAMPLES"): "False"})
    def test_bundle_names_without_examples(self):
        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(BASIC_BUNDLE_CONFIG)},
        ):
            manager = DagBundlesManager()
            assert manager.bundle_names == ["my-test-bundle"]

    @conf_vars({("core", "LOAD_EXAMPLES"): "False"})
    def test_bundle_names_preserves_insertion_order(self):
        multi_config = BASIC_BUNDLE_CONFIG + SECOND_BUNDLE_CONFIG
        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(multi_config)},
        ):
            manager = DagBundlesManager()
            assert manager.bundle_names == ["my-test-bundle", "second-bundle"]


# ---------------------------------------------------------------------------
# TestGetBundle
# ---------------------------------------------------------------------------


class TestGetBundle:
    """Tests for DagBundlesManager.get_bundle()."""

    def test_get_bundle(self):
        """Test that get_bundle builds and returns a bundle."""
        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(BASIC_BUNDLE_CONFIG)},
        ):
            bundle_manager = DagBundlesManager()

            with pytest.raises(ValueError, match="'bundle-that-doesn't-exist' is not configured"):
                bundle_manager.get_bundle(name="bundle-that-doesn't-exist", version="hello")
            bundle = bundle_manager.get_bundle(name="my-test-bundle", version="hello")
        assert isinstance(bundle, BasicBundle)
        assert bundle.name == "my-test-bundle"
        assert bundle.version == "hello"
        assert bundle.refresh_interval == 1

    def test_get_bundle_none_version(self):
        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(BASIC_BUNDLE_CONFIG)},
        ):
            bundle_manager = DagBundlesManager()
            bundle = bundle_manager.get_bundle(name="my-test-bundle")
        assert isinstance(bundle, BasicBundle)
        assert bundle.name == "my-test-bundle"
        assert bundle.version is None

    def test_get_all_bundle_names(self):
        assert DagBundlesManager().get_all_bundle_names() == ["dags-folder", "example_dags"]


# ---------------------------------------------------------------------------
# TestViewUrl
# ---------------------------------------------------------------------------


class TestViewUrl:
    """Tests for DagBundlesManager.view_url()."""

    @conf_vars({("dag_processor", "dag_bundle_config_list"): json.dumps(BASIC_BUNDLE_CONFIG)})
    @pytest.mark.parametrize("version", [None, "hello"])
    def test_view_url(self, version):
        """Test that view_url calls the bundle's view_url method."""
        bundle_manager = DagBundlesManager()
        with patch.object(BaseDagBundle, "view_url") as view_url_mock:
            with pytest.warns(DeprecationWarning, match="'view_url' method is deprecated"):
                bundle_manager.view_url("my-test-bundle", version=version)
        view_url_mock.assert_called_once_with(version=version)


# ---------------------------------------------------------------------------
# TestSyncBundlesToDb
# ---------------------------------------------------------------------------


@pytest.mark.db_test
class TestSyncBundlesToDb:
    """Tests for DagBundlesManager.sync_bundles_to_db()."""

    @conf_vars({("core", "LOAD_EXAMPLES"): "False"})
    def test_sync_add_disable_reenable(self, clear_db, session):
        def _get_bundle_names_and_active():
            return session.execute(
                select(DagBundleModel.name, DagBundleModel.active).order_by(DagBundleModel.name)
            ).all()

        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(BASIC_BUNDLE_CONFIG)},
        ):
            manager = DagBundlesManager()
            manager.sync_bundles_to_db()
        assert _get_bundle_names_and_active() == [("my-test-bundle", True)]

        session.add(
            ParseImportError(
                bundle_name="my-test-bundle",
                filename="some_file.py",
                stacktrace="some error",
            )
        )
        session.flush()

        manager = DagBundlesManager()
        manager.sync_bundles_to_db()
        assert _get_bundle_names_and_active() == [
            ("dags-folder", True),
            ("my-test-bundle", False),
        ]
        assert session.scalar(select(func.count(ParseImportError.id))) == 0

        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(BASIC_BUNDLE_CONFIG)},
        ):
            manager = DagBundlesManager()
            manager.sync_bundles_to_db()
        assert _get_bundle_names_and_active() == [
            ("dags-folder", False),
            ("my-test-bundle", True),
        ]

    @conf_vars({("core", "LOAD_EXAMPLES"): "False"})
    def test_sync_with_template(self, clear_db, session):
        """Test that URL templates and parameters are stored in the database during sync."""
        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(TEMPLATE_BUNDLE_CONFIG)},
        ):
            manager = DagBundlesManager()
            manager.sync_bundles_to_db()

        bundle_model = session.scalars(
            select(DagBundleModel).filter_by(name="template-bundle").limit(1)
        ).first()

        session.merge(bundle_model)

        assert bundle_model is not None
        assert bundle_model.render_url(version="v1.0") == "https://github.com/example/repo/tree/v1.0/dags"
        assert bundle_model.template_params == {"subdir": "dags"}
        assert bundle_model.active is True

    @conf_vars({("core", "LOAD_EXAMPLES"): "False"})
    def test_bundle_model_render_url(self, clear_db, session):
        """Test the DagBundleModel render_url method."""
        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(TEMPLATE_BUNDLE_CONFIG)},
        ):
            manager = DagBundlesManager()
            manager.sync_bundles_to_db()
            bundle_model = session.scalars(
                select(DagBundleModel).filter_by(name="template-bundle").limit(1)
            ).first()

            session.merge(bundle_model)
            assert bundle_model is not None

            url = bundle_model.render_url(version="main")
            assert url == "https://github.com/example/repo/tree/main/dags"
            url = bundle_model.render_url()
            assert url == "https://github.com/example/repo/tree/None/dags"

    @conf_vars({("core", "LOAD_EXAMPLES"): "False"})
    def test_template_params_update_on_sync(self, clear_db, session):
        """Test that template parameters are updated when bundle configuration changes."""
        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(TEMPLATE_BUNDLE_CONFIG)},
        ):
            manager = DagBundlesManager()
            manager.sync_bundles_to_db()

        bundle_model = session.scalars(
            select(DagBundleModel).filter_by(name="template-bundle").limit(1)
        ).first()
        url = bundle_model._unsign_url()
        assert url == "https://github.com/example/repo/tree/{version}/{subdir}"
        assert bundle_model.template_params == {"subdir": "dags"}

        updated_config = [
            {
                "name": "template-bundle",
                "classpath": "unit.dag_processing.bundles.test_dag_bundle_manager.BundleWithTemplate",
                "kwargs": {
                    "view_url_template": "https://gitlab.com/example/repo/-/tree/{version}/{subdir}",
                    "subdir": "workflows",
                    "refresh_interval": 1,
                },
            }
        ]

        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(updated_config)},
        ):
            manager = DagBundlesManager()
            manager.sync_bundles_to_db()

        bundle_model = session.scalars(
            select(DagBundleModel).filter_by(name="template-bundle").limit(1)
        ).first()
        url = bundle_model._unsign_url()
        assert url == "https://gitlab.com/example/repo/-/tree/{version}/{subdir}"
        assert bundle_model.template_params == {"subdir": "workflows"}
        assert bundle_model.render_url(version="v1") == "https://gitlab.com/example/repo/-/tree/v1/workflows"

    @conf_vars({("core", "LOAD_EXAMPLES"): "False"})
    def test_template_update_on_sync(self, clear_db, session):
        """Test that templates are updated when bundle configuration changes."""
        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(TEMPLATE_BUNDLE_CONFIG)},
        ):
            manager = DagBundlesManager()
            manager.sync_bundles_to_db()

        bundle_model = session.scalars(
            select(DagBundleModel).filter_by(name="template-bundle").limit(1)
        ).first()
        url = bundle_model._unsign_url()
        assert url == "https://github.com/example/repo/tree/{version}/{subdir}"
        assert bundle_model.render_url(version="v1") == "https://github.com/example/repo/tree/v1/dags"

        updated_config = [
            {
                "name": "template-bundle",
                "classpath": "unit.dag_processing.bundles.test_dag_bundle_manager.BundleWithTemplate",
                "kwargs": {
                    "view_url_template": "https://gitlab.com/example/repo/-/tree/{version}/{subdir}",
                    "subdir": "dags",
                    "refresh_interval": 1,
                },
            }
        ]

        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(updated_config)},
        ):
            manager = DagBundlesManager()
            manager.sync_bundles_to_db()

        bundle_model = session.scalars(
            select(DagBundleModel).filter_by(name="template-bundle").limit(1)
        ).first()
        url = bundle_model._unsign_url()
        assert url == "https://gitlab.com/example/repo/-/tree/{version}/{subdir}"
        assert bundle_model.render_url("v1") == "https://gitlab.com/example/repo/-/tree/v1/dags"

    def test_dag_bundle_model_render_url_with_invalid_template(self):
        """Test that DagBundleModel.render_url handles invalid templates gracefully."""
        bundle_model = DagBundleModel(name="test-bundle")
        bundle_model.signed_url_template = "https://github.com/example/repo/tree/{invalid_placeholder}"
        bundle_model.template_params = {"subdir": "dags"}

        url = bundle_model.render_url("v1")
        assert url is None

    @conf_vars({("core", "LOAD_EXAMPLES"): "False"})
    def test_multiple_bundles_one_fails(self, clear_db, session):
        """Test that when one bundle fails to create, other bundles still load successfully."""
        mix_config = BASIC_BUNDLE_CONFIG + FAILING_BUNDLE_CONFIG

        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(mix_config)},
        ):
            manager = DagBundlesManager()

            bundles = list(manager.get_all_dag_bundles())
            assert len(bundles) == 1
            assert bundles[0].name == "my-test-bundle"
            assert isinstance(bundles[0], BasicBundle)

            manager.sync_bundles_to_db()
            bundle_names = {b.name for b in session.scalars(select(DagBundleModel)).all()}
            assert bundle_names == {"my-test-bundle"}


# ---------------------------------------------------------------------------
# TestReassignDagsWithUnconfiguredBundles
# ---------------------------------------------------------------------------


@pytest.mark.db_test
class TestReassignDagsWithUnconfiguredBundles:
    """Tests for DagBundlesManager.reassign_dags_with_unconfigured_bundles()."""

    @conf_vars({("core", "LOAD_EXAMPLES"): "False"})
    def test_no_stale_dags_returns_zero(self, clear_dags_and_bundles, session):
        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(BASIC_BUNDLE_CONFIG)},
        ):
            manager = DagBundlesManager()
            manager.sync_bundles_to_db(session=session)
            session.flush()

            _add_dag(session, "dag-1", "my-test-bundle")

            count = manager.reassign_dags_with_unconfigured_bundles(session=session)
            assert count == 0
            assert session.get(DagModel, "dag-1").bundle_name == "my-test-bundle"

    @conf_vars({("core", "LOAD_EXAMPLES"): "False"})
    def test_dags_with_unconfigured_bundle_are_reassigned(self, clear_dags_and_bundles, session):
        multi_config = BASIC_BUNDLE_CONFIG + SECOND_BUNDLE_CONFIG
        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(multi_config)},
        ):
            manager = DagBundlesManager()
            manager.sync_bundles_to_db(session=session)
        session.flush()

        _add_dag(session, "dag-1", "my-test-bundle")
        _add_dag(session, "dag-2", "my-test-bundle")
        _add_dag(session, "dag-3", "second-bundle")

        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(SECOND_BUNDLE_CONFIG)},
        ):
            manager = DagBundlesManager()
            manager.sync_bundles_to_db(session=session)
            session.flush()

            count = manager.reassign_dags_with_unconfigured_bundles(session=session)
            assert count == 2

            session.expire_all()
            assert session.get(DagModel, "dag-1").bundle_name == "second-bundle"
            assert session.get(DagModel, "dag-2").bundle_name == "second-bundle"
            assert session.get(DagModel, "dag-3").bundle_name == "second-bundle"

    @conf_vars({("core", "LOAD_EXAMPLES"): "False"})
    def test_first_configured_bundle_is_used_as_default(self, clear_dags_and_bundles, session):
        """The first bundle in the config list is the fallback target."""
        multi_config = BASIC_BUNDLE_CONFIG + SECOND_BUNDLE_CONFIG
        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(multi_config)},
        ):
            manager = DagBundlesManager()
            manager.sync_bundles_to_db(session=session)
        session.flush()

        _add_dag(session, "dag-1", "second-bundle")

        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(BASIC_BUNDLE_CONFIG)},
        ):
            manager = DagBundlesManager()
            manager.sync_bundles_to_db(session=session)
            session.flush()

            count = manager.reassign_dags_with_unconfigured_bundles(session=session)
            assert count == 1

            session.expire_all()
            assert session.get(DagModel, "dag-1").bundle_name == "my-test-bundle"

    @conf_vars({("core", "LOAD_EXAMPLES"): "False"})
    def test_multiple_unconfigured_bundles(self, clear_dags_and_bundles, session):
        third_config = [
            {
                "name": "third-bundle",
                "classpath": "unit.dag_processing.bundles.test_dag_bundle_manager.BasicBundle",
                "kwargs": {"refresh_interval": 1},
            }
        ]
        all_config = BASIC_BUNDLE_CONFIG + SECOND_BUNDLE_CONFIG + third_config
        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(all_config)},
        ):
            manager = DagBundlesManager()
            manager.sync_bundles_to_db(session=session)
        session.flush()

        _add_dag(session, "dag-1", "my-test-bundle")
        _add_dag(session, "dag-2", "second-bundle")
        _add_dag(session, "dag-3", "third-bundle")

        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(third_config)},
        ):
            manager = DagBundlesManager()
            manager.sync_bundles_to_db(session=session)
            session.flush()

            count = manager.reassign_dags_with_unconfigured_bundles(session=session)
            assert count == 2

            session.expire_all()
            assert session.get(DagModel, "dag-1").bundle_name == "third-bundle"
            assert session.get(DagModel, "dag-2").bundle_name == "third-bundle"
            assert session.get(DagModel, "dag-3").bundle_name == "third-bundle"

    @conf_vars({("core", "LOAD_EXAMPLES"): "False"})
    def test_reassign_is_idempotent(self, clear_dags_and_bundles, session):
        """Calling reassign twice produces the same result and returns 0 the second time."""
        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(BASIC_BUNDLE_CONFIG)},
        ):
            manager = DagBundlesManager()
            manager.sync_bundles_to_db(session=session)
        session.flush()

        session.add(DagBundleModel(name="old-bundle"))
        session.flush()
        _add_dag(session, "dag-1", "old-bundle")

        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(BASIC_BUNDLE_CONFIG)},
        ):
            manager = DagBundlesManager()
            first_count = manager.reassign_dags_with_unconfigured_bundles(session=session)
            assert first_count == 1

            second_count = manager.reassign_dags_with_unconfigured_bundles(session=session)
            assert second_count == 0


# ---------------------------------------------------------------------------
# TestSyncBundlesIntegration — sync_bundles_to_db + reassign combined
# ---------------------------------------------------------------------------


@pytest.mark.db_test
class TestSyncBundlesIntegration:
    """Tests for the full sync flow including DAG reassignment via DagFileProcessorManager.sync_bundles."""

    @conf_vars({("core", "LOAD_EXAMPLES"): "False"})
    def test_sync_reassigns_dags_from_removed_bundle(self, clear_dags_and_bundles, session):
        """sync_bundles_to_db + reassign_dags_with_unconfigured_bundles reassigns DAGs when their bundle is removed."""
        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(BASIC_BUNDLE_CONFIG)},
        ):
            manager = DagBundlesManager()
            manager.sync_bundles_to_db(session=session)
        session.flush()

        _add_dag(session, "dag-1", "my-test-bundle")

        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(SECOND_BUNDLE_CONFIG)},
        ):
            manager = DagBundlesManager()
            manager.sync_bundles_to_db(session=session)
            manager.reassign_dags_with_unconfigured_bundles(session=session)

        session.expire_all()
        dag = session.get(DagModel, "dag-1")
        assert dag.bundle_name == "second-bundle"

    @conf_vars({("core", "LOAD_EXAMPLES"): "False"})
    def test_sync_does_not_reassign_dags_with_active_bundle(self, clear_dags_and_bundles, session):
        """sync_bundles_to_db + reassign leaves DAGs alone when their bundle is still configured."""
        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(BASIC_BUNDLE_CONFIG)},
        ):
            manager = DagBundlesManager()
            manager.sync_bundles_to_db(session=session)
        session.flush()

        _add_dag(session, "dag-1", "my-test-bundle")

        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(BASIC_BUNDLE_CONFIG)},
        ):
            manager = DagBundlesManager()
            manager.sync_bundles_to_db(session=session)
            manager.reassign_dags_with_unconfigured_bundles(session=session)

        dag = session.get(DagModel, "dag-1")
        assert dag.bundle_name == "my-test-bundle"

    @conf_vars({("core", "LOAD_EXAMPLES"): "False"})
    def test_dag_file_processor_manager_sync_bundles_calls_reassign(self, clear_dags_and_bundles, session):
        """DagFileProcessorManager.sync_bundles() calls both sync_bundles_to_db and reassign."""
        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(BASIC_BUNDLE_CONFIG)},
        ):
            mock_manager = mock.MagicMock(spec=DagBundlesManager)
            with mock.patch("airflow.dag_processing.manager.DagBundlesManager", return_value=mock_manager):
                from airflow.dag_processing.manager import DagFileProcessorManager

                processor = DagFileProcessorManager.__new__(DagFileProcessorManager)
                processor.sync_bundles()

            mock_manager.sync_bundles_to_db.assert_called_once()
            mock_manager.reassign_dags_with_unconfigured_bundles.assert_called_once()
