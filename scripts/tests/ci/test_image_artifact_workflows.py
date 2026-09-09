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

import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
RESOLVE_ACTION = ROOT / ".github/actions/resolve_main_image/action.yml"


class TestResolveMainImageAction:
    @pytest.mark.parametrize(
        ("disabled", "publish", "docker_script", "returncode", "miss"),
        [
            pytest.param("true", "false", "exit 99", 0, True, id="disabled-skips-registry"),
            pytest.param("false", "false", "exit 1", 0, True, id="registry-failure-builds-normally"),
            pytest.param("true", "true", "exit 1", 1, False, id="publisher-requires-base-identity"),
            pytest.param("false", "false", "echo invalid", 0, True, id="invalid-digest-builds-normally"),
        ],
    )
    def test_resolver_failure_modes(
        self, tmp_path: Path, disabled: str, publish: str, docker_script: str, returncode: int, miss: bool
    ) -> None:
        docker = tmp_path / "docker"
        docker.write_text("#!/bin/bash\n" + docker_script + "\n")
        docker.chmod(0o755)
        action = yaml.safe_load(RESOLVE_ACTION.read_text())
        output = tmp_path / "output"
        result = subprocess.run(
            [
                "bash",
                "--noprofile",
                "--norc",
                "-e",
                "-o",
                "pipefail",
                "-c",
                action["runs"]["steps"][0]["run"],
            ],
            env={
                "PATH": f"{tmp_path}:/usr/bin:/bin",
                "RUNNER_TEMP": str(tmp_path),
                "GITHUB_OUTPUT": str(output),
                "GITHUB_RUN_ATTEMPT": "2",
                "IMAGE_KIND": "ci",
                "IMAGE_PYTHON": "3.12",
                "IMAGE_PLATFORM": "linux/amd64",
                "IMAGE_REUSE_DISABLED": disabled,
                "IMAGE_PUBLISH": publish,
                "IMAGE_CONSTRAINTS_FILE": "",
            },
            text=True,
            capture_output=True,
            check=False,
        )
        assert result.returncode == returncode, result.stderr
        values = dict(line.split("=", 1) for line in output.read_text().splitlines())
        assert values["selection-name"] == "selected-ci-3.12-amd64-2"
        assert values["built-image-name"] == "built-ci-3.12-amd64-2"
        assert (values.get("hit") == "false") is miss
        assert "base-image" not in values

    @pytest.mark.parametrize("kind", ["ci", "prod"])
    def test_selected_image_hit_bypasses_build_and_export(self, kind: str) -> None:
        workflow = yaml.safe_load((ROOT / f".github/workflows/{kind}-image-build.yml").read_text())
        steps = workflow["jobs"][f"build-{kind}-images"]["steps"]
        expensive_steps = [
            step
            for step in steps
            if "run" in step
            and (f"breeze {kind}-image build" in step["run"] or f"breeze {kind}-image save" in step["run"])
        ]
        assert len(expensive_steps) == 2
        assert all("steps.main-image.outputs.hit != 'true'" in step["if"] for step in expensive_steps)
        local_upload = next(step for step in steps if step.get("id") == "local-image")
        assert local_upload["with"]["name"] == "${{ steps.main-image.outputs.built-image-name }}"
        selection_step = next(step for step in steps if "select-local" in step.get("run", ""))
        assert steps.index(selection_step) > steps.index(local_upload)
