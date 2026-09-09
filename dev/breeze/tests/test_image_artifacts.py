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

import argparse
import io
import zipfile
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import pytest
import requests

from airflow_breeze.global_constants import GithubEvents
from airflow_breeze.utils.image_artifacts import (
    GithubArtifacts,
    artifact_name,
    download,
    fingerprint,
    is_build_input,
    resolve,
    restore_selection,
)
from airflow_breeze.utils.selective_checks import SelectiveChecks


@pytest.fixture
def inputs():
    return {"schema": 1, "kind": "ci", "python": "3.12", "platform": "linux/amd64", "fingerprint": "a" * 64}


@pytest.fixture
def artifact(inputs):
    return {
        "id": 123,
        "name": artifact_name(inputs),
        "expired": False,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "workflow_run": {"id": 456, "head_sha": "b" * 40},
        "digest": "sha256:" + "c" * 64,
    }


@pytest.fixture
def run():
    return {
        "id": 456,
        "repository": {"full_name": "apache/airflow"},
        "head_repository": {"full_name": "apache/airflow"},
        "head_branch": "main",
        "event": "push",
        "path": ".github/workflows/publish-main-images.yml",
        "status": "completed",
        "conclusion": "success",
        "head_sha": "b" * 40,
    }


class TestFingerprint:
    @pytest.mark.parametrize(
        ("path", "ci", "prod"),
        [
            ("airflow-core/src/airflow/api_fastapi/app.py", False, True),
            ("airflow-core/ui/src/App.tsx", False, True),
            ("airflow-core/ui/package.json", True, True),
            ("providers/amazon/pyproject.toml", True, True),
            ("providers/amazon/provider.yaml", True, True),
            ("providers/amazon/src/airflow/providers/amazon/get_provider_info.py", True, True),
            ("shared/logging/src/airflow_shared/logging/foo.py", True, True),
            ("airflow-core/src/airflow/__init__.py", True, True),
            ("uv.lock", True, True),
            ("scripts/docker/install_airflow.sh", True, True),
            ("chart/templates/test.yaml", False, False),
            ("airflow-core/tests/unit/test_api.py", False, False),
        ],
    )
    def test_build_inputs(self, path, ci, prod):
        assert is_build_input(path, "ci") is ci
        assert is_build_input(path, "prod") is prod

    def test_actual_checkout_and_dimensions(self, tmp_path):
        lock = tmp_path / "uv.lock"
        lock.write_text("old")
        with patch("subprocess.check_output", return_value=b"uv.lock\0"):
            original = fingerprint(tmp_path, "ci", "3.12", "linux/amd64")
            lock.write_text("new")
            assert fingerprint(tmp_path, "ci", "3.12", "linux/amd64") != original
            lock.write_text("old")
            assert fingerprint(tmp_path, "ci", "3.13", "linux/amd64") != original
            assert fingerprint(tmp_path, "ci", "3.12", "linux/arm64") != original
            assert fingerprint(tmp_path, "ci", "3.12", "linux/amd64", ("mysql=8",)) != original
            assert fingerprint(tmp_path, "ci", "3.12", "linux/amd64", constraints=b"updated") != original
            lock.unlink()
            assert fingerprint(tmp_path, "ci", "3.12", "linux/amd64") != original


class TestTrustedProducer:
    def test_success(self, artifact, run):
        api = GithubArtifacts()
        api.get = Mock(return_value=run)
        assert api.validate(artifact, artifact["name"])["artifact-id"] == 123
        api.get.assert_called_once_with("actions/runs/456")

    @pytest.mark.parametrize(
        ("key", "value"),
        [
            ("repository", {"full_name": "fork/airflow"}),
            ("head_repository", {"full_name": "fork/airflow"}),
            ("head_branch", "feature"),
            ("event", "pull_request"),
            ("path", ".github/workflows/ci-amd.yml"),
            ("status", "in_progress"),
            ("conclusion", "cancelled"),
            ("head_sha", "wrong"),
        ],
    )
    def test_reject_provenance(self, artifact, run, key, value):
        run[key] = value
        api = GithubArtifacts()
        api.get = Mock(return_value=run)
        with pytest.raises(ValueError, match="producer"):
            api.validate(artifact, artifact["name"])

    @pytest.mark.parametrize("age", [timedelta(hours=49), timedelta(hours=-1)])
    def test_reject_age(self, artifact, age):
        artifact["created_at"] = (datetime.now(timezone.utc) - age).isoformat()
        with pytest.raises(ValueError, match="freshness"):
            GithubArtifacts().validate(artifact, artifact["name"])

    def test_reject_unverifiable_digest(self, artifact, run):
        artifact["digest"] = None
        api = GithubArtifacts()
        api.get = Mock(return_value=run)
        with pytest.raises(ValueError, match="digest"):
            api.validate(artifact, artifact["name"])

    def test_resolve_failure_is_miss(self, inputs):
        api = Mock()
        api.find.side_effect = requests.Timeout()
        assert resolve(inputs, api)["hit"] is False

    def test_disabled_does_not_lookup(self, inputs):
        api = Mock()
        assert resolve(inputs, api, disabled=True)["hit"] is False
        api.find.assert_not_called()

    def test_resolve_skips_bad_producer(self, inputs, artifact):
        api = Mock()
        api.find.return_value = [artifact, artifact]
        api.validate.side_effect = [ValueError("bad producer"), {"hit": True}]
        assert resolve(inputs, api)["hit"] is True


class TestDownload:
    def test_digest_mismatch(self):
        api = GithubArtifacts()
        response = Mock()
        response.iter_content.return_value = [b"corrupt"]
        api.session.get = Mock()
        api.session.get.return_value.__enter__ = Mock(return_value=response)
        api.session.get.return_value.__exit__ = Mock(return_value=False)
        with pytest.raises(ValueError, match="digest mismatch"), api.archive({"id": 1, "digest": "wrong"}):
            pytest.fail("must not open corrupt archive")

    @pytest.mark.parametrize("member", ["../escape", "/tmp/escape"])
    def test_reject_archive_escape(self, inputs, artifact, run, tmp_path, member):
        api = GithubArtifacts()
        api.get = Mock(return_value=run)
        selection = {**inputs, **api.validate(artifact, artifact["name"])}
        api.get = Mock(side_effect=[artifact, run])

        @contextmanager
        def archive(_artifact):
            payload = io.BytesIO()
            with zipfile.ZipFile(payload, "w") as zipped:
                zipped.writestr(member, "bad")
            payload.seek(0)
            with zipfile.ZipFile(payload) as zipped:
                yield zipped

        api.archive = archive
        with patch("airflow_breeze.utils.image_artifacts.GithubArtifacts", return_value=api):
            with pytest.raises(ValueError, match="archive member"):
                download(selection, tmp_path)


class TestReuseEligibility:
    @pytest.mark.parametrize(
        "labels", [(), ("disable image cache",), ("upgrade to newer dependencies",), ("canary",)]
    )
    def test_labels(self, labels):
        checks = SelectiveChecks(files=("airflow-core/src/airflow/api_fastapi/app.py",), pr_labels=labels)
        assert checks.image_reuse_eligible is (not labels)
        assert checks.ci_image_build

    @pytest.mark.parametrize(
        "event", [GithubEvents.PUSH, GithubEvents.SCHEDULE, GithubEvents.WORKFLOW_DISPATCH]
    )
    def test_full_builds(self, event):
        assert not SelectiveChecks(github_event=event).image_reuse_eligible


class TestRestoreSelection:
    def test_missing_selection_uses_existing_build(self, tmp_path):
        api = Mock()
        api.find.return_value = []
        args = argparse.Namespace(
            repository="fork/airflow",
            kind="ci",
            python="3.12",
            platform="linux/amd64",
            run_id=100,
            run_attempt=2,
            output_directory=tmp_path,
        )
        with patch("airflow_breeze.utils.image_artifacts.GithubArtifacts", return_value=api):
            assert restore_selection(args)["hit"] is False
        api.find.assert_called_once_with("selected-ci-3.12-amd64-2", 100)

    def test_lookup_error_must_not_silently_restore_old_image(self, tmp_path):
        api = Mock()
        api.find.side_effect = requests.Timeout("unavailable")
        args = argparse.Namespace(
            repository="fork/airflow",
            kind="ci",
            python="3.12",
            platform="linux/amd64",
            run_id=100,
            run_attempt=2,
            output_directory=tmp_path,
        )
        with patch("airflow_breeze.utils.image_artifacts.GithubArtifacts", return_value=api):
            with pytest.raises(requests.Timeout, match="unavailable"):
                restore_selection(args)
