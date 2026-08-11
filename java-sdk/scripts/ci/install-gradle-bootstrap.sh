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

set -euo pipefail

if [ "$#" -ne 2 ] || [ "$1" != "--destination" ]; then
  echo "Usage: install-gradle-bootstrap.sh --destination <directory>" >&2
  exit 2
fi

destination="$2"
script_dir="$(cd "$(dirname "$0")" && pwd)"
properties="$script_dir/../../gradle/wrapper/gradle-wrapper.properties"
distribution_url="$(sed -n 's/^distributionUrl=//p' "$properties")"
distribution_url="${distribution_url//\\:/:}"
distribution_sha256="$(sed -n 's/^distributionSha256Sum=//p' "$properties")"

if [[ ! "$distribution_url" =~ ^https://services\.gradle\.org/distributions/gradle-([0-9]+\.[0-9]+(\.[0-9]+)?)-bin\.zip$ ]]; then
  echo "ERROR: unsupported Gradle distribution URL: $distribution_url" >&2
  exit 1
fi
version="${BASH_REMATCH[1]}"
if [[ ! "$distribution_sha256" =~ ^[0-9a-f]{64}$ ]]; then
  echo "ERROR: invalid Gradle distribution SHA-256" >&2
  exit 1
fi
mkdir -p "$destination"
archive="$destination/gradle-$version-bin.zip"
curl --fail --silent --show-error --location --proto '=https' --proto-redir '=https' \
  --output "$archive" "$distribution_url"

if command -v sha256sum >/dev/null 2>&1; then
  printf '%s  %s\n' "$distribution_sha256" "$archive" | sha256sum -c - >/dev/null
else
  printf '%s  %s\n' "$distribution_sha256" "$archive" | shasum -a 256 -c - >/dev/null
fi

unzip -oq -d "$destination" "$archive"
printf '%s\n' "$destination/gradle-$version/bin"
