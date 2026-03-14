#!/usr/bin/env bash
# Setup script for remote environments (GitHub Coding Agent, etc.)
# Run from the root of the apache/airflow repository.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ACTION_YML="$REPO_ROOT/.github/actions/install-prek/action.yml"
SKILL_TOOLS="$(dirname "${BASH_SOURCE[0]}")"

echo "==> Resolving environment versions..."
eval "$(uv run "$SKILL_TOOLS/get_environment_package_version.py" "$ACTION_YML")"
eval "$(uv run "$SKILL_TOOLS/get_latest_main_merged_pr_num.py")"

echo "    PYTHON_VERSION=$PYTHON_VERSION"
echo "    UV_VERSION=$UV_VERSION"
echo "    PREK_VERSION=$PREK_VERSION"
echo "    LATEST_MAIN_MERGED_PR_NUM=$LATEST_MAIN_MERGED_PR_NUM"

echo "==> Installing uv $UV_VERSION..."
curl -LsSf "https://astral.sh/uv/${UV_VERSION}/install.sh" | sh
export PATH="$HOME/.local/bin:$PATH"

echo "==> Installing prek $PREK_VERSION..."
uv tool install "prek==${PREK_VERSION}" --with "uv==${UV_VERSION}"

echo "==> Installing breeze..."
bash "$REPO_ROOT/scripts/ci/install_breeze.sh"

CI_IMAGE="ghcr.io/apache/airflow/main/ci/python${PYTHON_VERSION}"
echo "==> Checking for CI image: $CI_IMAGE..."
if docker image inspect "$CI_IMAGE" > /dev/null 2>&1; then
  echo "    Image already exists locally, skipping."
else
  echo "    Image not found locally, pulling $CI_IMAGE..."
  if ! docker pull "$CI_IMAGE"; then
    echo "    Pull failed, loading via breeze (PR #$LATEST_MAIN_MERGED_PR_NUM)..."
    breeze ci-image load \
      --from-pr "$LATEST_MAIN_MERGED_PR_NUM" \
      --python "$PYTHON_VERSION" \
      --github-token "$GITHUB_TOKEN"
  fi
fi

echo "==> Remote environment setup complete."
