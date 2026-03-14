#!/usr/bin/env bash
# Copies the apache-airflow-contributing skill into the current repo's .claude/skills/
# and commits with a warning to drop the commit before raising a PR.
#
# Usage: bash <path-to>/setup_airflow_skills.sh
#   Run from the root of your airflow repo clone.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SKILL_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SKILL_NAME="$(basename "$SKILL_DIR")"
TARGET_DIR="$(pwd)/.claude/skills/$SKILL_NAME"

if ! git rev-parse --is-inside-work-tree > /dev/null 2>&1; then
  echo "ERROR: Not inside a git repository." >&2
  exit 1
fi

echo "==> Copying skill: $SKILL_NAME"
echo "    From: $SKILL_DIR"
echo "    To:   $TARGET_DIR"

mkdir -p "$TARGET_DIR"
cp -r "$SKILL_DIR"/* "$TARGET_DIR"/

echo "==> Staging and committing..."
git add "$TARGET_DIR"
git commit -n -m "DO NOT MERGE — temporary skill setup (drop this commit before PR)

Added .claude/skills/$SKILL_NAME for local development.
This commit must be removed (git rebase -i / git reset) before opening a PR."

echo ""
echo "==> Done. Skill installed at: $TARGET_DIR"
echo ""
echo "    WARNING: A commit was created that you MUST drop before raising a PR."
echo "    Use 'git rebase -i HEAD~1' or 'git reset HEAD~1' to remove it when ready."
