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
"""Select immutable images from successful trusted main publishers."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import tempfile
import time
import zipfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

SCHEMA_VERSION = 1
RETENTION_DAYS = 7
MAX_AGE = timedelta(hours=48)
TRUSTED_REPOSITORY = "apache/airflow"
PUBLISHER_PATH = ".github/workflows/publish-main-images.yml"
KINDS = ("ci", "prod", "prod-dependencies")


def is_build_input(path: str, kind: str) -> bool:
    """Keep unknown inputs; omit only mounted editable implementations for CI environments."""
    if path.startswith(("chart/", "kubernetes-tests/", "docker-tests/")) or (
        "/tests/" in path and path.startswith(("airflow-core/", "task-sdk/", "airflow-ctl/", "providers/"))
    ):
        return False
    if kind == "prod":
        return True
    file = Path(path)
    # Shared distributions and build-generated metadata remain fingerprinted, even when mounted.
    if (
        path.startswith(("airflow-core/src/", "task-sdk/src/", "airflow-ctl/src/", "providers/"))
        and "/src/" in path
        and file.suffix == ".py"
        and file.name not in {"__init__.py", "get_provider_info.py", "version.py"}
        and "/_generated/" not in path
    ):
        return False
    if path.startswith("airflow-core/ui/src/"):
        return False
    return True


def fingerprint(
    root: Path,
    kind: str,
    python: str,
    platform: str,
    build_args: tuple[str, ...] = (),
    base_image_digest: str = "",
    constraints: bytes = b"",
) -> dict[str, Any]:
    """Hash actual checkout inputs, including file modes, deletions and symlink targets."""
    if kind not in KINDS or platform not in {"linux/amd64", "linux/arm64"}:
        raise ValueError("Unsupported image dimensions")
    dimensions = {
        "schema": SCHEMA_VERSION,
        "kind": kind,
        "python": python,
        "platform": platform,
        "build-args": sorted(build_args),
        "base-image-digest": base_image_digest,
        "constraints-digest": hashlib.sha256(constraints).hexdigest(),
    }
    digest = hashlib.sha256(json.dumps(dimensions, sort_keys=True).encode())
    paths = subprocess.check_output(["git", "ls-files", "-z"], cwd=root).decode().split("\0")
    for name in sorted(set(paths) - {""}):
        if not is_build_input(name, kind):
            continue
        path = root / name
        digest.update(name.encode() + b"\0")
        if path.is_symlink():
            digest.update(b"symlink\0" + os.readlink(path).encode())
        elif path.is_file():
            digest.update(str(path.stat().st_mode & 0o777).encode() + b"\0")
            digest.update(hashlib.sha256(path.read_bytes()).digest())
        else:
            digest.update(b"deleted")
    return {**dimensions, "fingerprint": digest.hexdigest()}


def artifact_name(inputs: dict[str, Any]) -> str:
    return (
        f"main-image-{inputs['kind']}-{inputs['python']}-"
        f"{inputs['platform'].split('/')[-1]}-{inputs['fingerprint']}"
    )


class GithubArtifacts:
    """Access artifacts without trusting producer-controlled manifest provenance."""

    def __init__(self, repository: str = TRUSTED_REPOSITORY):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
            raise ValueError("Invalid repository")
        self.repository = repository
        self.session = requests.Session()
        token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
        self.session.headers.update({"Accept": "application/vnd.github+json"})
        if token:
            self.session.headers["Authorization"] = f"Bearer {token}"

    def get(self, path: str, **params: Any) -> dict[str, Any]:
        response = self.session.get(
            f"https://api.github.com/repos/{self.repository}/{path}", params=params, timeout=30
        )
        response.raise_for_status()
        return response.json()

    def find(self, name: str, run_id: int | None = None) -> list[dict[str, Any]]:
        path = f"actions/runs/{run_id}/artifacts" if run_id else "actions/artifacts"
        result: list[dict[str, Any]] = []
        # Bound requests; a cache miss is preferable to unbounded CI startup latency.
        for page in range(1, 6):
            artifacts = self.get(path, name=name, per_page=100, page=page)["artifacts"]
            result.extend(item for item in artifacts if item["name"] == name and not item["expired"])
            if len(artifacts) < 100:
                break
        return sorted(result, key=lambda item: item["created_at"], reverse=True)

    def validate(self, artifact: dict[str, Any], name: str) -> dict[str, Any]:
        if self.repository != TRUSTED_REPOSITORY or artifact["name"] != name or artifact["expired"]:
            raise ValueError("Artifact is not a trusted main image")
        created = datetime.fromisoformat(artifact["created_at"].replace("Z", "+00:00"))
        age = datetime.now(timezone.utc) - created
        if not timedelta(0) <= age <= MAX_AGE:
            raise ValueError("Artifact is outside the freshness window")
        run = self.get(f"actions/runs/{artifact['workflow_run']['id']}")
        if not (
            run["repository"]["full_name"] == TRUSTED_REPOSITORY
            and run["head_repository"]["full_name"] == TRUSTED_REPOSITORY
            and run["head_branch"] == "main"
            and run["event"] in {"push", "schedule", "workflow_dispatch"}
            and run["path"] == PUBLISHER_PATH
            and run["status"] == "completed"
            and run["conclusion"] == "success"
            and run["head_sha"] == artifact["workflow_run"]["head_sha"]
        ):
            raise ValueError("Artifact producer is not a successful main publisher")
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", artifact.get("digest") or ""):
            raise ValueError("Artifact has no verifiable digest")
        return {
            "hit": True,
            "artifact-id": artifact["id"],
            "artifact-name": name,
            "run-id": run["id"],
            "repository": self.repository,
            "source-sha": run["head_sha"],
            "digest": artifact["digest"],
        }

    @contextmanager
    def archive(self, artifact: dict[str, Any]) -> Iterator[zipfile.ZipFile]:
        with tempfile.TemporaryFile() as payload:
            for attempt in range(3):
                try:
                    payload.seek(0)
                    payload.truncate()
                    digest = hashlib.sha256()
                    with self.session.get(
                        f"https://api.github.com/repos/{self.repository}/actions/artifacts/{artifact['id']}/zip",
                        timeout=120,
                        stream=True,
                    ) as response:
                        response.raise_for_status()
                        for chunk in response.iter_content(chunk_size=1024 * 1024):
                            digest.update(chunk)
                            payload.write(chunk)
                    if "sha256:" + digest.hexdigest() != artifact.get("digest"):
                        raise ValueError("Artifact digest mismatch")
                    break
                except requests.RequestException:
                    if attempt == 2:
                        raise
                    time.sleep(2**attempt)
            payload.seek(0)
            with zipfile.ZipFile(payload) as archive:
                yield archive


def resolve(inputs: dict[str, Any], api: GithubArtifacts, disabled: bool = False) -> dict[str, Any]:
    """Return a cache miss on unavailable, incompatible or untrusted artifacts."""
    if disabled:
        return {"hit": False, "reason": "reuse disabled"}
    try:
        name = artifact_name(inputs)
        for artifact in api.find(name):
            try:
                return {**api.validate(artifact, name), **inputs}
            except (ValueError, KeyError, TypeError):
                continue
    except (requests.RequestException, ValueError, KeyError, TypeError):
        return {"hit": False, "reason": "artifact lookup unavailable"}
    return {"hit": False, "reason": "no compatible fresh main artifact"}


def download(selection: dict[str, Any], directory: Path) -> None:
    """Revalidate immutable provenance and digest immediately before loading an image."""
    api = GithubArtifacts(selection["repository"])
    artifact = api.get(f"actions/artifacts/{int(selection['artifact-id'])}")
    verified = api.validate(artifact, artifact_name(selection))
    if any(selection[key] != value for key, value in verified.items()):
        raise ValueError("Selection does not match immutable artifact provenance")
    with api.archive(artifact) as archive:
        for member in archive.infolist():
            destination = (directory / member.filename).resolve()
            if not destination.is_relative_to(directory.resolve()):
                raise ValueError("Unexpected image archive member")
        archive.extractall(directory)


def restore_selection(args: argparse.Namespace) -> dict[str, Any]:
    api = GithubArtifacts(args.repository)
    name = f"selected-{args.kind}-{args.python}-{args.platform.split('/')[-1]}-{args.run_attempt}"
    artifacts = api.find(name, args.run_id)
    if not artifacts:
        return {"hit": False, "reason": "no selection for this run"}
    with api.archive(artifacts[0]) as archive:
        members = archive.namelist()
        if len(members) != 1 or not members[0].endswith(".json"):
            raise ValueError("Invalid selection archive")
        selection = json.loads(archive.read(members[0]))
    if (selection["kind"], selection["python"], selection["platform"]) != (
        args.kind,
        args.python,
        args.platform,
    ):
        raise ValueError("Selection dimensions do not match consumer")
    download(selection, args.output_directory)
    return selection


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("fingerprint", "resolve", "download", "restore-selection"))
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--kind", choices=KINDS)
    parser.add_argument("--python")
    parser.add_argument("--platform", choices=("linux/amd64", "linux/arm64"))
    parser.add_argument("--repository", default=TRUSTED_REPOSITORY)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--fingerprint-file", type=Path)
    parser.add_argument("--selection-file", type=Path)
    parser.add_argument("--output-directory", type=Path)
    parser.add_argument("--run-id", type=int)
    parser.add_argument("--run-attempt", type=int)
    parser.add_argument("--disabled", action="store_true")
    parser.add_argument("--build-arg", action="append", default=[])
    parser.add_argument("--base-image-digest", default="")
    parser.add_argument("--constraints-file", type=Path)
    args = parser.parse_args()
    if args.command == "restore-selection":
        result = restore_selection(args)
    elif args.command == "download":
        result = json.loads(args.selection_file.read_text())
        download(result, args.output_directory)
    else:
        inputs = (
            json.loads(args.fingerprint_file.read_text())
            if args.fingerprint_file
            else fingerprint(
                args.root,
                args.kind,
                args.python,
                args.platform,
                tuple(args.build_arg),
                args.base_image_digest,
                args.constraints_file.read_bytes() if args.constraints_file else b"",
            )
        )
        result = (
            {**inputs, "artifact-name": artifact_name(inputs)}
            if args.command == "fingerprint"
            else resolve(inputs, GithubArtifacts(args.repository), args.disabled)
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
