#!/usr/bin/env bash
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

# Build the source release tarball and its .sha512 for a language SDK release candidate tag.
#
# The ATR compose workflow runs this in CI, and the release manager runs the same command on
# their own machine to confirm the artifacts uploaded to ATR are reproducible. Everything is
# read from the tag, so the working tree may be on any commit.
#
# Usage:
#   build_source_release.sh <java|ts|go> <tag> <output-dir>
#
# Tags:
#   java-sdk/<VERSION>-rc<N>
#   ts-sdk/<VERSION>-rc<N>
#   go-sdk/v<VERSION>-rc<N>
#
# When GITHUB_OUTPUT is set, writes version, commit, tarball and sha512 to it.

set -euo pipefail

if [[ $# -ne 3 ]]; then
    echo "Usage: $0 <java|ts|go> <tag> <output-dir>" >&2
    exit 2
fi

sdk="$1"
tag="$2"
out_dir="$3"

case "${sdk}" in
    java | ts) tag_version_prefix="" ;;
    # Go resolves module versions from `go-sdk/vX.Y.Z` tags, so Go SDK tags keep the `v`.
    go) tag_version_prefix="v" ;;
    *)
        echo "ERROR: unknown SDK '${sdk}', expected java, ts or go" >&2
        exit 2
        ;;
esac
sdk_dir="${sdk}-sdk"

# The ATR version is the version without the RC suffix: ATR accepts only letters, digits and
# single `+`, `.` or `-` separators, and a failed vote adds a revision to the same ATR version.
semver='[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z]+(\.[0-9A-Za-z]+)*)?'
if ! [[ "${tag}" =~ ^${sdk_dir}/${tag_version_prefix}(${semver})-rc([0-9]+)$ ]]; then
    echo "ERROR: tag '${tag}' does not match ${sdk_dir}/${tag_version_prefix}<VERSION>-rc<N>" >&2
    exit 1
fi
version="${BASH_REMATCH[1]}"

repo_root="$(git rev-parse --show-toplevel)"
if ! commit="$(git -C "${repo_root}" rev-parse --verify --quiet "refs/tags/${tag}^{commit}")"; then
    echo "ERROR: tag '${tag}' does not exist locally; fetch it first" >&2
    exit 1
fi

case "${sdk}" in
    java)
        manifest="java-sdk/gradle.properties"
        manifest_version="$(git -C "${repo_root}" show "${commit}:${manifest}" | sed -n 's/^projectVersion=//p')"
        ;;
    ts)
        manifest="ts-sdk/package.json"
        manifest_version="$(git -C "${repo_root}" show "${commit}:${manifest}" | jq -r '.version')"
        ;;
    go)
        # Go has no version manifest: the tag is the version.
        manifest=""
        manifest_version="${version}"
        ;;
esac
if [[ "${manifest_version}" != "${version}" ]]; then
    echo "ERROR: ${manifest} at ${tag} has version '${manifest_version}', the tag says '${version}'" >&2
    exit 1
fi

mkdir -p "${out_dir}"
out_dir="$(cd "${out_dir}" && pwd)"
tarball="apache-airflow-${sdk_dir}-${version}-src.tar.gz"

# Same command as the Java SDK's Gradle `sourceTarball` task, so both produce identical bytes.
# Run from the SDK directory so only its subtree is archived and its .gitattributes apply.
git -C "${repo_root}/${sdk_dir}" archive \
    --format=tar.gz \
    --prefix="apache-airflow-${sdk_dir}-${version}/" \
    -o "${out_dir}/${tarball}" \
    "${commit}"

(cd "${out_dir}" && sha512sum "${tarball}" > "${tarball}.sha512")
sha512="$(cut -d' ' -f1 "${out_dir}/${tarball}.sha512")"

echo "Built ${out_dir}/${tarball} from ${tag} (${commit})"
echo "SHA-512: ${sha512}"

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    {
        echo "version=${version}"
        echo "commit=${commit}"
        echo "tarball=${tarball}"
        echo "sha512=${sha512}"
    } >> "${GITHUB_OUTPUT}"
fi
