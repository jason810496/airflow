# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "pygithub>=2.1.1",
# ]
# ///
"""Tool to get the latest merged PR number on the main branch from Apache Airflow repository."""

import os
import sys

from github import Auth, Github
from github.GithubException import GithubException


def get_latest_main_merged_pr_num(
    repo_owner: str = "apache",
    repo_name: str = "airflow",
    github_token: str | None = None,
) -> int:
    """
    Get the latest merged PR number on the main branch of the Apache Airflow repository.

    Uses PyGithub to fetch the most recent merged pull request on the default branch.
    Requires GITHUB_TOKEN environment variable or explicit github_token parameter.

    :param repo_owner: GitHub repository owner
    :type repo_owner: str
    :param repo_name: GitHub repository name
    :type repo_name: str
    :param github_token: GitHub authentication token (uses GITHUB_TOKEN env var if not provided)
    :type github_token: str or None
    :return: Latest merged PR number on main branch
    :rtype: int
    :raises EnvironmentError: If GITHUB_TOKEN is not set and not provided
    :raises RuntimeError: If GitHub API request fails or no merged PRs found

    Example::

        pr_num = get_latest_main_merged_pr_num()
        print(f"Latest merged PR: {pr_num}")
        # Output: Latest merged PR: 41234

        # With custom repository
        pr_num = get_latest_main_merged_pr_num(repo_owner="myorg", repo_name="myrepo")
    """
    token = github_token or os.environ.get("GITHUB_TOKEN")

    if not token:
        raise EnvironmentError(
            "GITHUB_TOKEN environment variable is not set. "
            "Please set GITHUB_TOKEN or provide github_token parameter."
        )

    try:
        auth = Auth.Token(token)
        g = Github(auth=auth)

        repo = g.get_repo(f"{repo_owner}/{repo_name}")
        default_branch = repo.default_branch

        # Get the most recent merged PR targeting the default branch
        pulls = repo.get_pulls(state="closed", sort="updated", direction="desc", base=default_branch)

        for pr in pulls:
            if pr.merged:
                return pr.number

        raise RuntimeError(f"No merged PRs found on {default_branch} branch for {repo_owner}/{repo_name}")

    except GithubException as e:
        raise RuntimeError(f"GitHub API error: {e}")
    finally:
        g.close()


if __name__ == "__main__":
    try:
        pr_num = get_latest_main_merged_pr_num()
        print(f"LATEST_MAIN_MERGED_PR_NUM={pr_num}", file=sys.stdout)
    except (EnvironmentError, RuntimeError) as e:
        print(f"Error: {e}")
        exit(1)
