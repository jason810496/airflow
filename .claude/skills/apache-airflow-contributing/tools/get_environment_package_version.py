# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "pyyaml>=6.0.3",
# ]
# ///
"""Tool to extract package versions from install-prek GitHub Action configuration."""


from pathlib import Path

import yaml


def get_environment_package_version(action_yml_path: str) -> dict[str, str]:
    """
    Extract python, uv, and prek versions from the install-prek GitHub Action YAML file.

    :param action_yml_path: Absolute path to the install-prek/action.yml file
    :type action_yml_path: str
    :return: Dictionary containing python, uv, and prek versions
    :rtype: dict[str, str]
    :raises FileNotFoundError: If the action.yml file is not found
    :raises ValueError: If versions cannot be extracted from the file

    Example::

        versions = get_environment_package_version("/path/to/.github/actions/install-prek/action.yml")
        print(versions)
        # Output: {'python': '3.10', 'uv': '0.9.30', 'prek': '0.3.1'}
    """
    action_file = Path(action_yml_path)

    if not action_file.exists():
        raise FileNotFoundError(f"Action file not found: {action_yml_path}")

    content = action_file.read_text()
    try:
        data = yaml.safe_load(content)
    except yaml.YAMLError as e:
        raise ValueError(f"Failed to parse YAML file: {e}")

    versions = {}
    inputs = data.get("inputs", {})

    # Extract python-version
    if python_default := inputs.get("python-version", {}).get("default"):
        versions["python"] = str(python_default)

    # Extract uv-version
    if uv_default := inputs.get("uv-version", {}).get("default"):
        versions["uv"] = str(uv_default)

    # Extract prek-version
    if prek_default := inputs.get("prek-version", {}).get("default"):
        versions["prek"] = str(prek_default)

    if not versions:
        raise ValueError(f"Could not extract version information from {action_yml_path}")

    return versions


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python get_environment_package_version.py <path_to_action_yml>")
        sys.exit(1)

    try:
        result = get_environment_package_version(sys.argv[1])
        print("Extracted versions:")
        for key, value in result.items():
            print(f"{key.upper()}_VERSION={value}", file=sys.stdout)
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}")
        sys.exit(1)
