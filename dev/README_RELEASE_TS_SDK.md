<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Releasing the Apache Airflow TypeScript SDK](#releasing-the-apache-airflow-typescript-sdk)
  - [Collect ambiguities during the release](#collect-ambiguities-during-the-release)
  - [Prerequisites](#prerequisites)
  - [Choose the version and npm dist-tag](#choose-the-version-and-npm-dist-tag)
  - [Prepare the release commit](#prepare-the-release-commit)
  - [Verify the release commit](#verify-the-release-commit)
  - [Tag the release candidate](#tag-the-release-candidate)
  - [Build and sign the candidate artifacts](#build-and-sign-the-candidate-artifacts)
  - [Stage the candidate in ASF dist](#stage-the-candidate-in-asf-dist)
  - [Call the vote](#call-the-vote)
  - [Verify the release candidate](#verify-the-release-candidate)
  - [Finish a successful vote](#finish-a-successful-vote)
  - [Handle a failed vote](#handle-a-failed-vote)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Releasing the Apache Airflow TypeScript SDK

The TypeScript SDK is versioned and released independently from Apache Airflow core. Under the
[ASF Release Policy](https://www.apache.org/legal/release-policy.html), the signed source archive is the
official Apache release. The package published to npm is a convenience artifact built from that source
archive, and must not be published until the source release passes a PMC vote.

Every public version, including alpha, beta, and release-candidate versions, is a release and follows this
process. The npm version never contains the vote-candidate number: for example, vote candidate 2 for beta 1
uses npm version `1.0.0-beta1` and git tag `ts-sdk/1.0.0-beta1-rc2`.

This guide uses the unscoped package name selected for the first public release, `apache-airflow-ts-sdk`.
Do not cut a candidate until the release-preparation commit has replaced any earlier package name in the SDK,
examples, documentation, and lockfiles.

## Collect ambiguities during the release

Keep notes about unclear or outdated steps outside the repository while running a release. After the
release, turn those notes into a focused documentation PR. This prevents release-manager scratch notes from
being committed with release preparation changes.

## Prerequisites

The release manager needs:

- An ASF committer account and write access to the Airflow `dist` development area.
- A GPG key listed in the Airflow `KEYS` file and available from a public key server, following the
  [ASF release-signing guidance](https://infra.apache.org/release-signing.html).
- Node.js 22 or later, the pnpm version declared by `packageManager` in `ts-sdk/package.json`, `npm`, `git`,
  `gpg`, and `svn`.
- An npm account with two-factor authentication and permission to publish `apache-airflow-ts-sdk`, or the
  community's agreement that this account will claim the package for the initial release.
- `gh` authenticated for the `apache/airflow` repository.

Before the first release, agree on the npm publisher on `dev@airflow.apache.org`. The first publisher claims
the unscoped package name; immediately after publishing, add the other agreed Airflow npm owners so the
package is not controlled by a single account. For later releases, confirm access before cutting a candidate:

```bash
npm whoami
npm owner ls apache-airflow-ts-sdk
```

Never put npm credentials, one-time passwords, or GPG private-key material in the repository, command
transcripts, vote emails, or GitHub issues.

## Choose the version and npm dist-tag

The SDK follows [semantic versioning](https://semver.org/). Choose a version based on changes to its public
TypeScript API and runtime protocol, independently of the Airflow core version. Record the minimum compatible
Airflow version in the vote and announcement when compatibility has changed.

Use a non-`latest` [npm dist-tag](https://docs.npmjs.com/cli/v11/commands/npm-dist-tag/) for every prerelease.
Typical mappings are `alpha` for alpha versions, `beta` for beta versions, and `rc` for release candidates.
Only a stable version is published under `latest`.

Set the release variables from the repository root:

```bash
export TS_SDK_VERSION=1.0.0-beta1
export TS_SDK_RC=1
export TS_SDK_NPM_TAG=beta
export TS_SDK_PACKAGE=apache-airflow-ts-sdk
export TS_SDK_RC_TAG="ts-sdk/${TS_SDK_VERSION}-rc${TS_SDK_RC}"
export TS_SDK_SOURCE_BASENAME="apache-airflow-ts-sdk-${TS_SDK_VERSION}-src"
export TS_SDK_REPO_ROOT="$(pwd -P)"
```

## Prepare the release commit

Open and merge a normal PR that updates every SDK-owned version to `TS_SDK_VERSION`. At minimum, inspect:

- `ts-sdk/package.json`
- `ts-sdk/docs/package.json` and `ts-sdk/docs/package-lock.json`
- `ts-sdk/example/package.json`
- `ts-sdk/pnpm-lock.yaml`
- Version and status text in the SDK and Airflow documentation

Keep the package name, example dependency, documentation imports, and lockfile package keys consistent.
For a prerelease, ensure `publishConfig.tag` names the intended non-`latest` channel. For a stable release,
remove the prerelease setting or change it to `latest`.

After the PR merges, check out the exact commit to release and confirm the worktree is clean. Do not release
from an unmerged branch or a worktree with local changes.

## Verify the release commit

Run the SDK checks from `ts-sdk/` using the toolchain pinned by the repository:

```bash
cd "${TS_SDK_REPO_ROOT}/ts-sdk"
corepack enable
pnpm install --frozen-lockfile
pnpm run lint
pnpm run format:check
pnpm run typecheck
pnpm run test
pnpm run build
npm pack --dry-run
cd "${TS_SDK_REPO_ROOT}"
```

Review the `npm pack --dry-run` file list. It must contain `LICENSE`, `NOTICE`, `README.md`, `package.json`,
and the expected files under `dist/`; it must not contain tests, credentials, local configuration, or build
caches.

Build the API reference from the repository root:

```bash
breeze build-docs --sdk-docs-only --sdk=typescript
```

Confirm `generated/_build/docs/ts-sdk/stable.txt` contains `TS_SDK_VERSION` and review the rendered
landing page.

## Tag the release candidate

Create a signed tag on the verified release commit. The RC number belongs only in the tag and ASF staging
directory, not in `package.json` or the npm tarball:

```bash
git tag -s "${TS_SDK_RC_TAG}" -m "Apache Airflow TypeScript SDK ${TS_SDK_VERSION} RC ${TS_SDK_RC}"
git push upstream "${TS_SDK_RC_TAG}"
git rev-list -n 1 "${TS_SDK_RC_TAG}"
```

Save the full commit hash printed by the final command for the vote email.

## Build and sign the candidate artifacts

Create the source archive directly from the signed tag. Using the `ts-sdk` subtree keeps the source package
focused while retaining everything needed to install, build, and test the SDK:

```bash
mkdir -p dist/ts-sdk
git archive \
  --format=tar.gz \
  --prefix="${TS_SDK_SOURCE_BASENAME}/" \
  --output="dist/ts-sdk/${TS_SDK_SOURCE_BASENAME}.tar.gz" \
  "${TS_SDK_RC_TAG}:ts-sdk"
```

Build the npm convenience artifact from an extracted copy of that source archive, not from the worktree. This
ensures the npm package can be traced to the source package that voters review:

```bash
export TS_SDK_BUILD_DIR="$(mktemp -d)"
tar xzf "dist/ts-sdk/${TS_SDK_SOURCE_BASENAME}.tar.gz" -C "${TS_SDK_BUILD_DIR}"
cd "${TS_SDK_BUILD_DIR}/${TS_SDK_SOURCE_BASENAME}"
corepack enable
pnpm install --frozen-lockfile
pnpm run test
pnpm run build
npm pack
mv "${TS_SDK_PACKAGE}-${TS_SDK_VERSION}.tgz" "${TS_SDK_REPO_ROOT}/dist/ts-sdk/"
cd "${TS_SDK_REPO_ROOT}"
```

Sign and checksum both artifacts:

```bash
cd dist/ts-sdk
for artifact in \
  "${TS_SDK_SOURCE_BASENAME}.tar.gz" \
  "${TS_SDK_PACKAGE}-${TS_SDK_VERSION}.tgz"
do
  gpg --armor --detach-sign "${artifact}"
  sha512sum "${artifact}" > "${artifact}.sha512"
done
cd "${TS_SDK_REPO_ROOT}"
```

Do not rebuild either artifact after this point. The exact bytes staged for the vote are the bytes promoted
after a successful vote.

## Stage the candidate in ASF dist

Check out the Airflow development distribution area if needed, then add a candidate directory containing the
two artifacts and their signatures and checksums:

```bash
[ -d asf-dist-dev-airflow/.svn ] || \
  svn checkout --depth=immediates https://dist.apache.org/repos/dist/dev/airflow asf-dist-dev-airflow
if [ -d asf-dist-dev-airflow/ts-sdk ]; then
  svn update --set-depth=infinity asf-dist-dev-airflow/ts-sdk
else
  mkdir -p asf-dist-dev-airflow/ts-sdk
fi
mkdir -p "asf-dist-dev-airflow/ts-sdk/${TS_SDK_VERSION}-rc${TS_SDK_RC}"
for artifact in \
  "${TS_SDK_SOURCE_BASENAME}.tar.gz" \
  "${TS_SDK_PACKAGE}-${TS_SDK_VERSION}.tgz"
do
  cp "dist/ts-sdk/${artifact}" \
    "dist/ts-sdk/${artifact}.asc" \
    "dist/ts-sdk/${artifact}.sha512" \
    "asf-dist-dev-airflow/ts-sdk/${TS_SDK_VERSION}-rc${TS_SDK_RC}/"
done
svn add --parents "asf-dist-dev-airflow/ts-sdk/${TS_SDK_VERSION}-rc${TS_SDK_RC}"
svn commit "asf-dist-dev-airflow/ts-sdk" \
  -m "Add Apache Airflow TypeScript SDK ${TS_SDK_VERSION} RC ${TS_SDK_RC}"
```

Verify the files at:

```text
https://dist.apache.org/repos/dist/dev/airflow/ts-sdk/<VERSION>-rc<RC>/
```

Optionally publish the candidate API reference to the staging documentation site:

```bash
gh workflow run "Publish Docs to S3" --repo apache/airflow --ref main \
  -f ref="${TS_SDK_RC_TAG}" \
  -f include-docs=ts-sdk \
  -f destination=staging
```

## Call the vote

Send a plain-text vote email to `dev@airflow.apache.org`. Under the normal release process, leave the vote
open for at least 72 hours. A release passes with at least three positive binding votes and more positive
binding votes than negative binding votes. Replace every placeholder before sending.

```text
Subject: [VOTE] Release Apache Airflow TypeScript SDK <VERSION> based on <VERSION>-rc<RC>

Hi,

I would like to call a vote to release Apache Airflow TypeScript SDK <VERSION>,
based on release candidate <VERSION>-rc<RC>.

Changes since <the previous release or "the initial release">:
<one-line summary>

Git information:
- Tag: ts-sdk/<VERSION>-rc<RC>
- Commit: <FULL_COMMIT_SHA>

Candidate artifacts, signatures, and checksums:
https://dist.apache.org/repos/dist/dev/airflow/ts-sdk/<VERSION>-rc<RC>/

The candidate contains:
- apache-airflow-ts-sdk-<VERSION>-src.tar.gz (official source release)
- apache-airflow-ts-sdk-<VERSION>.tgz (npm convenience artifact)

KEYS file:
https://downloads.apache.org/airflow/KEYS

Compatibility:
- Node.js: 22 or later
- Apache Airflow: <MINIMUM_COMPATIBLE_AIRFLOW_VERSION>

Verification instructions:
https://github.com/apache/airflow/blob/ts-sdk%2F<VERSION>-rc<RC>/dev/README_RELEASE_TS_SDK.md#verify-the-release-candidate

Please review and vote. The vote will remain open through at least
<YYYY-MM-DD HH:MM UTC> and, if necessary, longer until the release-vote
criteria are met.

[ ] +1 Release this package as Apache Airflow TypeScript SDK <VERSION>
[ ] +0 No opinion
[ ] -1 Do not release, because ...

Only votes from Airflow PMC members are binding, but everyone is welcome and
encouraged to test the release and vote.

Best,
<YOUR_NAME>
```

Before sending, run `grep '<'` against the rendered email and confirm it produces no output.

## Verify the release candidate

Binding voters must download and verify the signed source archive on hardware they own and control. All
contributors are encouraged to perform the same checks.

### Verify checksums and signatures

Download every file from the candidate directory, including each artifact and its `.asc` and `.sha512`
files, and import the Airflow `KEYS` file. Then run:

```bash
sha512sum -c "${TS_SDK_SOURCE_BASENAME}.tar.gz.sha512"
sha512sum -c "${TS_SDK_PACKAGE}-${TS_SDK_VERSION}.tgz.sha512"
gpg --verify "${TS_SDK_SOURCE_BASENAME}.tar.gz.asc" \
  "${TS_SDK_SOURCE_BASENAME}.tar.gz"
gpg --verify "${TS_SDK_PACKAGE}-${TS_SDK_VERSION}.tgz.asc" \
  "${TS_SDK_PACKAGE}-${TS_SDK_VERSION}.tgz"
```

Both checksum commands must report `OK`, and both signatures must be valid and belong to a key in `KEYS`.

### Compare the source archive with the tag

Extract the source and compare it with a clean checkout of the tagged `ts-sdk` subtree:

```bash
export TS_SDK_VERIFY_DIR="$(mktemp -d)"
tar xzf "${TS_SDK_SOURCE_BASENAME}.tar.gz" -C "${TS_SDK_VERIFY_DIR}"
git clone --branch "${TS_SDK_RC_TAG}" https://github.com/apache/airflow.git \
  "${TS_SDK_VERIFY_DIR}/tag-checkout"
diff -rq \
  "${TS_SDK_VERIFY_DIR}/${TS_SDK_SOURCE_BASENAME}" \
  "${TS_SDK_VERIFY_DIR}/tag-checkout/ts-sdk"
```

The diff must produce no output. Confirm `LICENSE` and `NOTICE` are present at the archive root, review their
contents against what is actually packaged, and inspect the archive for unexpected generated files,
credentials, missing source headers, and binary content before continuing. Dependencies are resolved during
the build and must not be bundled into either candidate artifact.

### Build and test from the source archive

From the extracted source directory, run:

```bash
corepack enable
pnpm install --frozen-lockfile
pnpm run lint
pnpm run format:check
pnpm run typecheck
pnpm run test
pnpm run build
npm pack
```

Extract the voted npm tarball and the locally rebuilt tarball into separate directories and compare their
contents. The content diff must be empty:

```bash
mkdir candidate-package rebuilt-package
tar xzf "/path/to/${TS_SDK_PACKAGE}-${TS_SDK_VERSION}.tgz" -C candidate-package
tar xzf "${TS_SDK_PACKAGE}-${TS_SDK_VERSION}.tgz" -C rebuilt-package
diff -rq candidate-package rebuilt-package
```

Finally, install the voted npm tarball into an empty project and verify its public entry point:

```bash
export TS_SDK_SMOKE_DIR="$(mktemp -d)"
cd "${TS_SDK_SMOKE_DIR}"
npm init -y
npm install "/path/to/${TS_SDK_PACKAGE}-${TS_SDK_VERSION}.tgz"
node --input-type=module -e \
  'import { Dag } from "apache-airflow-ts-sdk"; console.log(new Dag("release_smoke_test").dagId)'
```

## Finish a successful vote

Reply to the vote thread with a `[RESULT][VOTE]` message that lists binding and non-binding votes.

### Promote the ASF artifacts

Move the exact voted candidate directory from the development area to the release area. This requires PMC
permissions:

```bash
svn mv \
  "https://dist.apache.org/repos/dist/dev/airflow/ts-sdk/${TS_SDK_VERSION}-rc${TS_SDK_RC}" \
  "https://dist.apache.org/repos/dist/release/airflow/ts-sdk/${TS_SDK_VERSION}" \
  -m "Release Apache Airflow TypeScript SDK ${TS_SDK_VERSION}"
```

### Create the final tag

Create the final signed tag on the same commit that was voted. Keep the RC tag for traceability:

```bash
git tag -s "ts-sdk/${TS_SDK_VERSION}" "${TS_SDK_RC_TAG}" \
  -m "Apache Airflow TypeScript SDK ${TS_SDK_VERSION}"
git push upstream "ts-sdk/${TS_SDK_VERSION}"
```

### Remove superseded ASF releases

The ASF archive automatically preserves releases after they appear on `downloads.apache.org`. Once the new
release is available from both downloads and the archive, remove SDK versions that are no longer current
from `dist/release`; do not remove a version from a separately maintained release line.

List the published versions and confirm each version selected for removal is present under
`https://archive.apache.org/dist/airflow/ts-sdk/` before running an explicit removal:

```bash
svn list https://dist.apache.org/repos/dist/release/airflow/ts-sdk/
svn rm \
  "https://dist.apache.org/repos/dist/release/airflow/ts-sdk/<SUPERSEDED_VERSION>" \
  -m "Archive superseded Apache Airflow TypeScript SDK <SUPERSEDED_VERSION>"
```

### Publish the voted npm artifact

Publish the exact `.tgz` file from the vote. Never run `npm publish` from the worktree, never rebuild after
the vote, and never use `latest` for a prerelease:

```bash
npm publish "dist/ts-sdk/${TS_SDK_PACKAGE}-${TS_SDK_VERSION}.tgz" \
  --tag "${TS_SDK_NPM_TAG}"
```

npm prompts for a one-time password when the account requires it. Do not place the password on a shared
command line or in shell history.

For the first release, add the agreed backup owners immediately after publishing:

```bash
npm owner add <AIRFLOW_NPM_OWNER> "${TS_SDK_PACKAGE}"
npm owner ls "${TS_SDK_PACKAGE}"
```

Verify the immutable version and dist-tags in the registry:

```bash
npm view "${TS_SDK_PACKAGE}@${TS_SDK_VERSION}" name version dist.tarball dist.integrity
npm dist-tag ls "${TS_SDK_PACKAGE}"
```

If this is a stable release, `latest` must point to `TS_SDK_VERSION`. For a prerelease, the selected prerelease
tag must point to it and `latest` must remain unchanged. npm does not allow a published name/version pair to
be reused; correct mistakes with a new version rather than attempting to overwrite one.

### Publish the GitHub release

Create a GitHub release for the final tag and attach the exact voted artifacts, signatures, and checksums.
Use `--prerelease` for alpha, beta, or release-candidate versions and omit it for a stable version:

```bash
gh release create "ts-sdk/${TS_SDK_VERSION}" \
  --repo apache/airflow \
  --title "Apache Airflow TypeScript SDK ${TS_SDK_VERSION}" \
  --notes "See the TypeScript SDK release guide for verification and compatibility details." \
  --verify-tag \
  --prerelease \
  "dist/ts-sdk/${TS_SDK_SOURCE_BASENAME}.tar.gz" \
  "dist/ts-sdk/${TS_SDK_SOURCE_BASENAME}.tar.gz.asc" \
  "dist/ts-sdk/${TS_SDK_SOURCE_BASENAME}.tar.gz.sha512" \
  "dist/ts-sdk/${TS_SDK_PACKAGE}-${TS_SDK_VERSION}.tgz" \
  "dist/ts-sdk/${TS_SDK_PACKAGE}-${TS_SDK_VERSION}.tgz.asc" \
  "dist/ts-sdk/${TS_SDK_PACKAGE}-${TS_SDK_VERSION}.tgz.sha512"
```

### Publish the API reference

Publish from the final tag, optionally using `staging` first and then `live`:

```bash
gh workflow run "Publish Docs to S3" --repo apache/airflow --ref main \
  -f ref="ts-sdk/${TS_SDK_VERSION}" \
  -f include-docs=ts-sdk \
  -f destination=live
```

When publishing a prerelease after a stable SDK version exists, add
`-f skip-write-to-stable-folder=true` so the prerelease does not replace the stable alias. Confirm the
versioned URL. For a stable release, also confirm the stable redirect:

```text
https://airflow.apache.org/docs/ts-sdk/<VERSION>/
https://airflow.apache.org/docs/ts-sdk/stable/
```

### Announce and record the release

Wait at least one hour after promoting the ASF artifacts. After the download URL, npm package, and
documentation resolve for a fresh consumer:

1. Send an `[ANNOUNCE]` email to `users@airflow.apache.org`, cc `dev@airflow.apache.org`.
2. Record the version and release date in the ASF Committee Report Helper at
   <https://reporter.apache.org/addrelease.html?airflow>.

Use this announcement template:

```text
Subject: [ANNOUNCE] Apache Airflow TypeScript SDK <VERSION> Released

Dear Airflow community,

I'm happy to announce that Apache Airflow TypeScript SDK <VERSION> was just
released.

The signed source release and npm convenience artifact are available at:
https://downloads.apache.org/airflow/ts-sdk/<VERSION>/

The npm package is available at:
https://www.npmjs.com/package/apache-airflow-ts-sdk/v/<VERSION>

The API documentation is available at:
https://airflow.apache.org/docs/ts-sdk/<VERSION>/

Compatibility:
- Node.js: 22 or later
- Apache Airflow: <MINIMUM_COMPATIBLE_AIRFLOW_VERSION>

<Optional: one or two lines on notable changes; omit for the first release.>

Thanks to everyone who contributed to and tested this release.

Cheers,
<YOUR_NAME>
```

Before sending, verify every URL and run `grep '<'` against the rendered email. Fill or remove every remaining
placeholder.

## Handle a failed vote

If a vote fails:

1. Close the vote and summarize why the candidate was rejected.
2. Do not publish anything to npm and do not create a final tag.
3. Remove the failed candidate from `dist/dev`.
4. Fix the issue through the normal PR process.
5. Cut the next candidate with the same `TS_SDK_VERSION` and an incremented `TS_SDK_RC`.

```bash
svn rm \
  "https://dist.apache.org/repos/dist/dev/airflow/ts-sdk/${TS_SDK_VERSION}-rc${TS_SDK_RC}" \
  -m "Remove failed Apache Airflow TypeScript SDK ${TS_SDK_VERSION} RC ${TS_SDK_RC}"
```

Keep failed RC git tags for traceability. Because the npm version was never published, a later successful
candidate can still publish `TS_SDK_VERSION`.
