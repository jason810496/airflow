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

- [Releasing language SDK source packages through ATR](#releasing-language-sdk-source-packages-through-atr)
  - [Versions, release candidates and betas](#versions-release-candidates-and-betas)
  - [One-time setup](#one-time-setup)
  - [Release steps](#release-steps)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Releasing language SDK source packages through ATR

The Java, TypeScript and Go SDKs ship their voted source package through
[Apache Trusted Releases (ATR)](https://releases.apache.org) instead of committing it by hand to
`dist/dev` and moving it to `dist/release` with `svn mv`. A GitHub workflow builds, signs and
uploads each release candidate to a draft on ATR. The release manager then starts the vote, resolves
it and publishes the release from ATR. Convenience binaries (Maven Central, npm, the Go module proxy)
keep their own SDK-specific steps.

| SDK        | ATR project        | Workflow to dispatch                         | RC tag                       | `dist/release` path      |
|------------|--------------------|----------------------------------------------|------------------------------|--------------------------|
| Java       | `airflow-java-sdk` | `.github/workflows/java-sdk-atr-compose.yml` | `java-sdk/<VERSION>-rc<N>`   | `airflow/java-sdk/<VERSION>` |
| TypeScript | `airflow-ts-sdk`   | `.github/workflows/ts-sdk-atr-compose.yml`   | `ts-sdk/<VERSION>-rc<N>`     | `airflow/ts-sdk/<VERSION>`   |
| Go         | `airflow-go-sdk`   | `.github/workflows/go-sdk-atr-compose.yml`   | `go-sdk/v<VERSION>-rc<N>`    | `airflow/go-sdk/<VERSION>`   |

Each caller hands its SDK to the shared
[`lang-sdk-atr-compose.yml`](../.github/workflows/lang-sdk-atr-compose.yml), which runs
[`build_source_release.sh`](lang_sdk_release/build_source_release.sh). ATR picks the project from
the workflow path in the GitHub OIDC token, which is the caller's, and refuses a path registered in
more than one project. That is why every SDK has its own caller.

## Versions, release candidates and betas

* Every version published to a public channel is an ASF release that needs a PMC vote, including
  alphas and betas (see the [ASF release policy](https://www.apache.org/legal/release-policy.html#release-types)).
  A beta such as `1.0.0-beta2` goes through exactly the same steps as `1.0.0`.
* The ATR version is the release version without the RC suffix, for example `1.0.0` or
  `1.0.0-beta2`. The RC number lives only in the git tag. When a vote fails, ATR returns the
  release to its draft, and the next RC adds a new revision to the same ATR version.
* The version in the tag must equal the version in `java-sdk/gradle.properties` (`projectVersion`)
  or `ts-sdk/package.json` at that commit. The Go SDK has no version manifest; its tags keep the `v`
  prefix because Go resolves module versions from `go-sdk/vX.Y.Z` tags.

## One-time setup

The workflows cannot upload anything until all of these are done.

1. **Automated Release Signing key.** Notify the [ASF Security team](https://www.apache.org/security/)
   and open an ASF Infra Jira ticket for a CI signing key, as described in
   [Automated release signing](https://infra.apache.org/release-signing.html#automated-release-signing).
   The request must describe how the release manager rebuilds the artifacts on trusted hardware
   (see [Verify the candidate](#verify-the-candidate)). Ask Infra to:
   * give the key the UID `Apache Airflow Automated Release Signing <private@airflow.apache.org>`,
     which is how ATR recognises it as a committee key and enables CI builds for the committee;
   * store the private key as the `AUTOMATED_RELEASE_SIGNING_KEY` secret of the
     `lang-sdk-atr-compose` environment of `apache/airflow`, rather than as a repository secret, so
     only the signing job can read it;
   * add the public key to the [`KEYS` file](https://downloads.apache.org/airflow/KEYS).

   Then set `SIGNING_KEY_FINGERPRINT` in `lang-sdk-atr-compose.yml` to the key's primary
   fingerprint. Signing fails while it is empty, and if the secret holds any other key.
2. **ATR projects.** `airflow-java-sdk` exists already. A PMC member creates `airflow-ts-sdk` and
   `airflow-go-sdk` under the `airflow` committee on ATR.
3. **Release policy of each project** (project page on ATR):
   * *Trusted Publishing*: repository name `airflow`, and only that SDK's caller under
     *Workflow paths: compose*, for example `.github/workflows/java-sdk-atr-compose.yml`. Leave the
     vote and finish paths empty: those phases are run by the release manager on ATR.
   * *Finish*: default download path suffix `java-sdk/{{VERSION}}` (or `ts-sdk/{{VERSION}}`,
     `go-sdk/{{VERSION}}`). Without it ATR publishes a subproject to `airflow-<sdk>-sdk-<VERSION>`.
4. **CI builds.** The *Permissions* card on the `airflow` committee page on ATR shows
   *CI builds: Enabled*. If it shows *Disabled*, the key from step 1 is not linked to the committee.
5. **GitHub environment.** Ask Infra to require reviewers on the `lang-sdk-atr-compose`
   environment, to prevent self-review, and to limit its deployment refs to the `java-sdk/*`,
   `ts-sdk/*` and `go-sdk/*` tags.
6. **Tag protection.** The *Language SDK release tags* ruleset in [`.asf.yaml`](../.asf.yaml)
   protects the SDK tags. ATR raises a concern, to be acknowledged before the vote, on a release
   whose tag was not protected.

## Release steps

### Tag the release candidate

Tag the commit on `main` or on a `vX-Y-test` / `vX-Y-stable` branch, and push the tag. The workflow
refuses commits that are on none of these. The workflow file and
`dev/lang_sdk_release/build_source_release.sh` must exist at the tagged commit, so backport them
before releasing from an older release branch.

```bash
git tag -s java-sdk/1.0.0-rc1 -m "Java SDK 1.0.0 RC 1"
git push upstream java-sdk/1.0.0-rc1
```

### Compose the candidate on ATR

Dispatch the SDK's caller on the tag. Only an allow-listed release manager may dispatch it, and the
first upload of a new ATR version must come from an Airflow PMC member, whose GitHub account is linked
to their ASF account.

```bash
gh workflow run java-sdk-atr-compose.yml --repo apache/airflow --ref java-sdk/1.0.0-rc1
```

The workflow:

1. builds `apache-airflow-<sdk>-sdk-<VERSION>-src.tar.gz` and its `.sha512` from the tag;
2. waits for approval on the `lang-sdk-atr-compose` environment, then signs the tarball with the
   Automated Release Signing key (this job has no OIDC permission);
3. uploads the three files to the draft of the ATR version with
   [`upload-to-atr`](https://github.com/apache/tooling-actions/tree/main/upload-to-atr), declaring
   the tagged commit as the source commit.

A successful upload does not mean ATR's checks passed. Open the draft on ATR and resolve every
blocker and concern before the vote.

### Verify the candidate

Before starting the vote, rebuild the tarball on your own machine and confirm it is identical to the
one on ATR. Download the draft's files from ATR, then:

```bash
git fetch upstream tag java-sdk/1.0.0-rc1
dev/lang_sdk_release/build_source_release.sh java java-sdk/1.0.0-rc1 /tmp/rebuild
sha512sum /tmp/rebuild/apache-airflow-java-sdk-1.0.0-src.tar.gz \
  <downloaded>/apache-airflow-java-sdk-1.0.0-src.tar.gz
gpg --verify <downloaded>/apache-airflow-java-sdk-1.0.0-src.tar.gz.asc \
  <downloaded>/apache-airflow-java-sdk-1.0.0-src.tar.gz
```

The script calls `git archive`, so differing git or zlib versions can change the gzip bytes. If only
the compressed checksums differ, compare `gzip -dc <file> | sha512sum` and look into the toolchain
before calling the vote.

### Vote, publish and announce

Start the vote from the draft on ATR, which sends the `[VOTE]` email to `dev@airflow.apache.org`
and tabulates the replies. Use the SDK's vote email template for the body. When the vote closes,
resolve it on ATR:

* **Failed or cancelled:** the release returns to its draft. Fix the problem, tag `-rc<N+1>` and
  compose again.
* **Passed:** publish the release from ATR with the download path suffix from the project policy.
  ATR commits it to `dist/release`. Then announce it from ATR.

After publishing, tag the final version on the voted commit and run the SDK's own steps for
convenience binaries, the GitHub release and the docs. The Go final tag keeps the `v` prefix
(`go-sdk/v1.0.0`).
