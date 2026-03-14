---
name: apache-airflow-contributing
description: "Guidelines for contributing to the Apache Airflow project — covers environment setup, test commands, static checks, coding standards, architecture boundaries, PR workflow, and provider development. Use this skill whenever working on the apache/airflow repository, running Airflow tests, setting up Breeze, writing providers, or creating PRs for Airflow. Also triggers for Execution API versioning, the Airflow Registry frontend, and the Common AI provider."
license: Apache-2.0
compatibility: opencode
metadata:
  author: jason810496
  version: "2.0"
  category: development
---

## Environment Setup

- Install prek: `uv tool install prek`
- Enable commit hooks: `prek install`
- **Never run pytest, python, or airflow commands directly on the host** — use `uv run` or `breeze`.
- Place temporary scripts in `dev/` (mounted as `/opt/airflow/dev/` inside Breeze).

### Remote Environment (GitHub Coding Agent, etc.)

Run the bundled setup script from the repo root:

```bash
bash <path-to-skill>/tools/setup_remote_env.sh
```

### Local Environment

Assume `uv`, `prek`, and `breeze` are already installed.

## Commands

`<PROJECT>` is the folder with the package's `pyproject.toml` (e.g., `airflow-core` or `providers/amazon`).
`<target_branch>` is the branch the PR targets — usually `main`, but could be `v3-1-test` for the 3.1 branch.

### Static Checks & Formatting

- **Lint with ruff:** `prek run ruff --from-ref <target_branch>`
- **Format with ruff:** `prek run ruff-format --from-ref <target_branch>`
- **Fast static checks:** `prek run --from-ref <target_branch> --stage pre-commit`
- **Slow manual checks:** `prek run --from-ref <target_branch> --stage manual`

### Mypy Type Checking (per top-level directory)

Run `git add . && prek run mypy-<dir> --stage pre-push` based on changed files:
- `airflow-core/` -> `mypy-airflow-core`
- `providers/` -> `mypy-providers`
- `dev/` -> `mypy-dev`
- `task-sdk/` -> `mypy-task-sdk`

### Running Tests

- **Single test:** `uv run --project <PROJECT> pytest path/to/test.py::TestClass::test_method -xvs`
- **Test file:** `uv run --project <PROJECT> pytest path/to/test.py -xvs`
- **All tests in package:** `uv run --project <PROJECT> pytest path/to/package -xvs`
- **If uv tests fail with missing system dependencies:** `breeze run pytest <tests> -xvs`
- **Run Python script:** `uv run --project <PROJECT> python dev/my_script.py`

### Parallel Test Suites (via Breeze)

- **Core/provider tests in parallel:** `breeze testing <test_group> --run-in-parallel`
  (test groups: `core-tests`, `providers-tests`)
- **DB tests only:** `breeze testing <test_group> --run-db-tests-only --run-in-parallel`
- **Non-DB tests with xdist:** `breeze testing <test_group> --skip-db-tests --use-xdist`
- **Single provider suite:** `breeze testing providers-tests --test-type "Providers[amazon]"`
  (or `"Providers[amazon,google]"`)
- **Helm tests:** `breeze testing helm-tests --use-xdist`
  - With K8s version: `--kubernetes-version 1.35.0`
  - Specific type: `--test-type <type>` (types: `airflow_aux`, `airflow_core`, `apiserver`,
    `dagprocessor`, `other`, `redis`, `security`, `statsd`, `webserver`)
- **Other suites:** `breeze testing <test_group>` (`airflow-ctl-tests`, `docker-compose-tests`,
  `task-sdk-tests`)
- **Selective checks (which tests to run):** `breeze selective-checks --commit-ref <commit>`

SQLite is the default backend. Use `--backend postgres` or `--backend mysql` for integration
tests. If Docker networking fails, run `docker network prune`.

### Other Commands

- **Airflow CLI:** `breeze run airflow dags list`
- **Type-check:** `breeze run mypy path/to/code`
- **Build docs:** `breeze build-docs`

## Repository Structure

UV workspace monorepo. Key paths:

- `airflow-core/src/airflow/` — core scheduler, API, CLI, models
  - `models/` — SQLAlchemy models (DagModel, TaskInstance, DagRun, Asset, etc.)
  - `jobs/` — scheduler, triggerer, DAG processor runners
  - `api_fastapi/core_api/` — public REST API v2, UI endpoints
  - `api_fastapi/execution_api/` — task execution communication API
  - `dag_processing/` — DAG parsing and validation
  - `cli/` — command-line interface
  - `ui/` — React/TypeScript web interface (Vite)
- `task-sdk/` — lightweight SDK for DAG authoring and task execution runtime
  - `src/airflow/sdk/execution_time/` — task runner, supervisor
- `providers/` — 100+ provider packages, each with its own `pyproject.toml`
- `airflow-ctl/` — management CLI tool
- `shared/` — shared libraries symlinked into distributions (airflow-core, task-sdk, etc.)
- `chart/` — Helm chart for Kubernetes deployment
- `registry/` — static site (Eleventy) indexing all providers

## Architecture Boundaries

1. Users author DAGs with the Task SDK (`airflow.sdk`).
2. DAG Processor parses DAG files in isolated processes and stores serialized DAGs in the metadata DB.
3. Scheduler reads serialized DAGs — **never runs user code** — and creates DAG runs / task instances.
4. Workers execute tasks via Task SDK and communicate with the API server through the Execution API — **never access the metadata DB directly**.
5. API Server serves the React UI and handles all client-database interactions.
6. Triggerer evaluates deferred tasks/sensors in isolated processes.
7. Shared libraries in `shared/` are symbolically linked into distributions that use them.
8. Airflow uses `uv workspace` to keep all distributions sharing dependencies and venv.
9. Each distribution declares its needed distributions: `uv --project <FOLDER> sync` acts on the
   selected project with only its declared dependencies.

## Coding Standards

- No `assert` in production code.
- `time.monotonic()` for durations, not `time.time()`.
- In `airflow-core`, functions with a `session` parameter must not call `session.commit()`.
  Use keyword-only `session` parameters.
- Imports at top of file. Valid exceptions: circular imports, lazy loading for worker isolation,
  `TYPE_CHECKING` blocks.
- Guard heavy type-only imports (e.g., `kubernetes.client`) with `TYPE_CHECKING` in multi-process
  code paths.
- Define dedicated exception classes or use existing exceptions such as `ValueError` instead
  of raising the broad `AirflowException` directly. Each error case should have a specific
  exception type that conveys what went wrong.
- Apache License header on all new files (prek enforces this).

## Testing Standards

- Add tests for new behavior — cover success, failure, and edge cases.
- Use pytest patterns, not `unittest.TestCase`.
- Use `spec`/`autospec` when mocking.
- Use `time_machine` for time-dependent tests.
- Use `@pytest.mark.parametrize` for multiple similar inputs.
- Use `@pytest.mark.db_test` for tests that require database access.
- Test fixtures: `devel-common/src/tests_common/pytest_plugin.py`.
- Test location mirrors source: `airflow/cli/cli_parser.py` -> `tests/cli/test_cli_parser.py`.

## Providers

Each provider is an independent package with its own `pyproject.toml`, tests, and docs.

- `provider.yaml` — metadata, dependencies, and configuration for the provider.
- Building blocks: Hooks, Operators, Sensors, Transfers.
- Use `version_compat.py` patterns for cross-version compatibility.
- Keep `provider.yaml` metadata, docs, and tests in sync.
- Don't upper-bound dependencies by default; add limits only with justification.
- Tests mirror source paths in test directories.
- Full guide: `contributing-docs/12_provider_distributions.rst`

## Commits and PRs

Write commit messages focused on user impact, not implementation details.

- **Good:** `Fix airflow dags test command failure without serialized DAGs`
- **Good:** `UI: Fix Grid view not refreshing after task actions`
- **Bad:** `Initialize DAG bundles in CLI get_dag function`

NEVER add Co-Authored-By with yourself as co-author of the commit. Agents cannot be authors,
humans can be, Agents are assistants.

### Newsfragments

Add a newsfragment for user-visible changes:
```bash
echo "Brief description" > airflow-core/newsfragments/{PR_NUMBER}.{bugfix|feature|improvement|doc|misc|significant}.rst
```

### Creating Pull Requests

**Always push to the user's fork**, not upstream `apache/airflow`. Never push directly to `main`.

Determine the fork remote via `git remote -v`:
- If `origin` doesn't point to `apache/airflow`, use `origin`.
- If `origin` points to `apache/airflow`, look for another remote pointing to the user's fork.
- If none exists: `gh repo fork apache/airflow --remote --remote-name fork`

### Self-Review Before Pushing

1. Review the full diff (`git diff main...HEAD`) — every change must be intentional.
2. Read `.github/instructions/code-review.instructions.md` and check your diff against every
   rule — architecture boundaries, database correctness, code quality, testing, API correctness.
3. Confirm compliance with coding standards and architecture boundaries above.
4. Run fast static checks: `prek run --from-ref <target_branch> --stage pre-commit`
5. Run slow manual checks: `prek run --from-ref <target_branch> --stage manual`
6. Run relevant individual tests and confirm they pass.
7. Run `breeze selective-checks --commit-ref <commit>` to find which test suites to run,
   then run them in parallel.
8. Check for security issues — no secrets, injection vulnerabilities, or unsafe patterns.

### Push and Open PR

```bash
git push -u <fork-remote> <branch-name>
gh pr create --web --title "Short title (under 70 chars)" --body "$(cat <<'EOF'
Brief description of the changes.

closes: #ISSUE  (if applicable)

---

##### Was generative AI tooling used to co-author this PR?

- [X] Yes — <Agent Name and Version>

Generated-by: <Agent Name and Version> following [the guidelines](https://github.com/apache/airflow/blob/main/contributing-docs/05_pull_requests.rst#gen-ai-assisted-contributions)

EOF
)"
```

Remind the user to review the PR title, add a description, and reference related issues.

## Boundaries

- **Ask first:** large cross-package refactors, new broad-impact dependencies, destructive
  data or migration changes.
- **Never:** commit secrets/credentials/tokens, edit generated files by hand when a generation
  workflow exists, use destructive git operations unless explicitly requested.

## Specialized Domain References

Read these reference files when working on the corresponding subsystem:

- **Execution API versioning** (Cadwyn, adding new features end-to-end):
  Read `references/execution-api.md` when modifying `airflow-core/src/airflow/api_fastapi/execution_api/`
  or `task-sdk/`.
- **Common AI provider** (pydantic-ai integration, LLM operators, toolsets):
  Read `references/common-ai-provider.md` when modifying `providers/common/ai/`.
- **Registry frontend** (Eleventy site, semantic CSS, data extraction):
  Read `references/registry.md` when modifying `registry/` or `dev/registry/`.

## Available Tools

Run with `uv run <path-to-tool>`:

- `tools/get_latest_main_merged_pr_num.py` — prints `LATEST_MAIN_MERGED_PR_NUM=xxx` to stdout.
  Requires `GITHUB_TOKEN` env var.
- `tools/get_environment_package_version.py <abs-path-to-action.yml>` — prints `PYTHON_VERSION`,
  `UV_VERSION`, `PREK_VERSION` from `.github/actions/install-prek/action.yml`.

## References

- `contributing-docs/03a_contributors_quick_start_beginners.rst`
- `contributing-docs/05_pull_requests.rst`
- `contributing-docs/07_local_virtualenv.rst`
- `contributing-docs/08_static_code_checks.rst`
- `contributing-docs/12_provider_distributions.rst`
- `contributing-docs/19_execution_api_versioning.rst`
