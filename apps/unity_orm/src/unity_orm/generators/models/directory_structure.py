"""Directory structure creation utilities for generated models."""

import os
from pathlib import Path
from typing import Optional


def create_directory_structure(
    models_content: str,
    output_directory: Path,
    schema: Optional[str] = None,
    verbose: bool = False,
    split_models_to_files_func=None,
) -> None:
    """Create the directory structure for the generated models.

    Args:
        models_content: Content of the generated models.
        output_directory: Directory to write the models to.
        schema: Schema name.
        verbose: Whether to print verbose output.
        split_models_to_files_func: Function to split models into files.
    """
    # The output directory is already the base directory (which may include the catalog)
    base_dir = output_directory
    os.makedirs(base_dir, exist_ok=True)

    # Create base __init__.py file
    with open(os.path.join(base_dir, "__init__.py"), "w") as f:
        f.write("# Auto-generated SQLAlchemy models\n\n")

    # Get schema name or use default
    schema_name = schema if schema else "default"

    # Create schema directory directly under the base directory
    schema_dir = os.path.join(base_dir, schema_name)
    os.makedirs(schema_dir, exist_ok=True)

    # Create schema __init__.py file
    with open(os.path.join(schema_dir, "__init__.py"), "w") as f:
        f.write(f"# Auto-generated SQLAlchemy models for {schema_name} schema\n\n")

    # Create tables directory under schema
    tables_dir = os.path.join(schema_dir, "tables")
    os.makedirs(tables_dir, exist_ok=True)

    # Create table model files
    if split_models_to_files_func:
        split_models_to_files_func(models_content, tables_dir)

    if verbose:
        print(f"Models written to {tables_dir}")
