import os
import sys
from pathlib import Path

import pytest

# Generic pytest runner for the datastream-platform monorepo
# Leverages pytest configuration from pyproject.toml


def main():
    """Run pytest on Databricks using monorepo configuration."""

    # Get the workspace root directory
    workspace_root = Path(__file__).parent.parent

    # Change to workspace root - pytest will use pyproject.toml config from here
    os.chdir(workspace_root)

    # Skip writing .pyc files to the bytecode cache on the cluster
    sys.dont_write_bytecode = True

    # Configure pytest arguments
    pytest_args = []

    # If target directory specified, add it as argument
    if len(sys.argv) > 1:
        target_dir = sys.argv[1]
        # Validate target exists
        target_path = workspace_root / target_dir
        if not target_path.exists():
            print(f"Error: Target directory {target_path} does not exist")
            sys.exit(1)
        pytest_args.append(target_dir)

    # Add any additional arguments passed to the script
    if len(sys.argv) > 2:
        pytest_args.extend(sys.argv[2:])

    # Add default arguments if none provided
    if not pytest_args:
        pytest_args.extend(["-v"])  # Just verbose, let pyproject.toml handle the rest

    print(f"Running pytest from workspace root: {workspace_root}")
    print(f"Pytest arguments: {pytest_args}")
    print("Using configuration from pyproject.toml")

    # Run pytest - it will automatically use pyproject.toml configuration
    retcode = pytest.main(pytest_args)

    # Exit with the same code as pytest
    sys.exit(retcode)


if __name__ == "__main__":
    main()
