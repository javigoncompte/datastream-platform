# ruff: noqa
"""Model generator for creating SQLAlchemy models from existing Databricks tables."""

import importlib.util
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel
from sqlalchemy import inspect
from sqlalchemy.engine import Engine

from unity_orm.logger import get_logger

logger = get_logger(__name__)

from unity_orm.databricks_session import get_spark
from unity_orm.engine import create_engine

spark = get_spark()


class TableInfo(BaseModel):
    """Information about a table in Databricks."""

    catalog: str
    schema: str
    name: str
    comment: Optional[str] = None


class ModelGenerator:
    """Generator for SQLAlchemy models from Databricks tables."""

    def __init__(
        self,
        engine: Engine,
        url: str,
        output_directory: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        verbose: bool = False,
    ):
        """Initialize the model generator.

        Args:
            engine: SQLAlchemy engine instance.
            output_directory: Directory to output generated models.
            catalog: Catalog to generate models for (optional).
            schema: Schema to generate models for (optional).
            verbose: Whether to display verbose debug output.
        """
        self.engine = engine
        self.url = url
        self.output_directory = Path(output_directory)
        self.catalog = catalog
        self.schema = schema
        self.verbose = verbose
        self.inspector = inspect(engine)

        # Check if sqlacodegen is installed
        self.has_sqlacodegen = False
        try:
            self.has_sqlacodegen = (
                importlib.util.find_spec("sqlacodegen") is not None
            )
            if not self.has_sqlacodegen:
                print(
                    "sqlacodegen is not installed. It should be included as a dependency."
                )
                print(
                    "Please check your installation or add sqlacodegen to your dependencies."
                )
        except ImportError:
            print("Error importing sqlacodegen. Make sure it's installed.")

    def get_table_list(self) -> List[TableInfo]:
        """Get a list of tables from Databricks.

        Returns:
            List of TableInfo objects.
        """
        tables: List[TableInfo] = []

        # Get all schemas in a catalog if a specific catalog is provided
        if self.catalog:
            if self.verbose:
                print(f"Fetching schemas for catalog: {self.catalog}")

            try:
                schemas = self.inspector.get_schema_names()

                for schema_name in schemas:
                    if self.schema and schema_name != self.schema:
                        continue

                    if schema_name == "information_schema":
                        # Skip information_schema as it's system tables
                        continue

                    if self.verbose:
                        print(f"Fetching tables for schema: {schema_name}")

                    try:
                        table_names = self.inspector.get_table_names(
                            schema=schema_name
                        )

                        for table_name in table_names:
                            table_info = TableInfo(
                                catalog=self.catalog,
                                schema=schema_name,
                                name=table_name,
                            )
                            tables.append(table_info)

                    except Exception as e:
                        print(
                            f"Error fetching tables for schema {schema_name}: {str(e)}"
                        )

            except Exception as e:
                print(
                    f"Error fetching schemas for catalog {self.catalog}: {str(e)}"
                )

        return tables

    def generate_models(self) -> None:
        """Generate SQLAlchemy models for all tables using sqlacodegen."""
        # Verify tables exist in the schema
        if self.verbose:
            print(f"Getting list of tables in {self.catalog}.{self.schema}")

            # Add debug info about engine
            logger.info(f"Engine details: {self.engine}")
            logger.info(f"Engine driver: {self.engine.driver}")
            logger.info(
                f"Engine dialect: {type(self.engine.dialect).__name__}"
            )

            # Debug the engine's inspection capabilities
            insp = inspect(self.engine)
            logger.info(f"Inspector: {insp}")

            # Debug SQLAlchemy version
            import sqlalchemy

            logger.info(f"SQLAlchemy version: {sqlalchemy.__version__}")

        tables = self.get_table_list()
        if not tables:
            print(f"No tables found in {self.catalog}.{self.schema}")
            return

        print(f"Found {len(tables)} tables in {self.catalog}.{self.schema}")

        if self.verbose:
            # Display table names
            for i, table in enumerate(tables):
                print(f"  {i + 1}. {table.name}")

                # Try to get column info for each table
                try:
                    columns = self.inspector.get_columns(
                        table.name, schema=table.schema
                    )
                    print(f"    Columns for {table.name}:")
                    for col in columns:
                        print(f"      {col['name']}: {col['type']}")
                except Exception as e:
                    print(
                        f"    Error getting columns for {table.name}: {str(e)}"
                    )

            print("\nConfiguring sqlacodegen...")

        # Create a temporary directory
        temp_dir = tempfile.mkdtemp(prefix="sqlacodegen_")
        temp_models_file = os.path.join(temp_dir, "models.py")

        try:
            # Create a clean URL for sqlacodegen
            url_obj = self.engine.url
            db_url = str(url_obj)

            # Ensure it has the parameter for handling empty strings
            if "_handle_empty_strings_for_numeric_types=NULL" not in db_url:
                separator = "&" if "?" in db_url else "?"
                db_url += (
                    f"{separator}_handle_empty_strings_for_numeric_types=NULL"
                )

            if self.verbose:
                logger.info(f"Using database URL: {db_url}")
                logger.info(f"Output file: {temp_models_file}")

            # Create the command to run sqlacodegen directly
            cmd = [
                "sqlacodegen",
                "--generator=declarative",
                f"--outfile={temp_models_file}",
                "--options=noindexes,nojoined,nobidi,nocomments",
                "--noviews",
                f"--schema={self.schema}",
                self.url,
            ]

            if self.verbose:
                print(f"Running command: {' '.join(cmd)}")

            # Run sqlacodegen
            process = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
            )

            # Check for errors
            if process.stderr:
                if self.verbose:
                    print(f"sqlacodegen stderr: {process.stderr}")
                if process.returncode != 0:
                    raise RuntimeError(f"sqlacodegen failed: {process.stderr}")

            # Check if the output file exists and has content
            if not os.path.exists(temp_models_file):
                raise RuntimeError(
                    f"Output file {temp_models_file} not created"
                )

            with open(temp_models_file, "r") as f:
                models_content = f.read()

            if not models_content.strip():
                raise RuntimeError("Empty output generated by sqlacodegen")

            # Create output directory structure
            self._create_directory_structure(models_content)

            print(
                f"Models successfully generated for {self.catalog}.{self.schema}"
            )

        except Exception as e:
            print(f"Error generating models: {str(e)}")
            if self.verbose:
                import traceback

                traceback.print_exc()
        finally:
            # Clean up temporary directory
            try:
                shutil.rmtree(temp_dir)
                if self.verbose:
                    print(f"Temporary directory {temp_dir} removed")
            except Exception as cleanup_error:
                print(
                    f"Warning: Could not clean up temporary directory: {str(cleanup_error)}"
                )

    def _create_directory_structure(self, models_content: str) -> None:
        """Create directory structure and write model files.

        Args:
            models_content: Content of models.py generated by sqlacodegen
        """
        # Create catalog dir
        catalog_dir = self.output_directory
        os.makedirs(catalog_dir, exist_ok=True)

        # Create catalog __init__.py
        with open(os.path.join(catalog_dir, "__init__.py"), "w") as f:
            f.write("# Auto-generated SQLAlchemy models\n\n")

        # Create schema dir
        schema_name = self.schema if self.schema else "default"
        schema_dir = os.path.join(catalog_dir, schema_name)
        os.makedirs(schema_dir, exist_ok=True)

        # Create schema __init__.py
        with open(os.path.join(schema_dir, "__init__.py"), "w") as f:
            f.write(
                f"# Auto-generated SQLAlchemy models for {schema_name} schema\n\n"
            )

        # Create tables dir
        tables_dir = os.path.join(schema_dir, "tables")
        os.makedirs(tables_dir, exist_ok=True)

        # Split models into individual files
        self._split_models_to_files(models_content, tables_dir)

        if self.verbose:
            print(f"Models written to {tables_dir}")

    def _split_models_to_files(
        self, models_content: str, output_dir: str
    ) -> None:
        """Split the combined models file into individual files.

        Args:
            models_content: The content of the models.py file generated by sqlacodegen
            output_dir: Directory to output the individual model files
        """
        # First, process the imports
        lines = models_content.splitlines()
        import_lines = []

        i = 0
        while i < len(lines) and not lines[i].startswith("class "):
            import_lines.append(lines[i])
            i += 1

        # Create __init__.py file for the tables package
        with open(os.path.join(output_dir, "__init__.py"), "w") as f:
            f.write("# Auto-generated SQLAlchemy models\n\n")

            # Find all class definitions
            class_names = []
            for line in lines:
                if line.startswith("class "):
                    # Extract class name - format is "class ClassName(Base):"
                    class_name = (
                        line.split("(")[0].replace("class ", "").strip()
                    )
                    class_names.append(class_name)

            # Add imports for all model classes
            for class_name in class_names:
                # Convert CamelCase to snake_case for filename
                snake_name = ""
                for char in class_name:
                    if char.isupper() and snake_name:
                        snake_name += "_" + char.lower()
                    else:
                        snake_name += char.lower()

                f.write(f"from .{snake_name} import {class_name}\n")

            # Add __all__ list
            f.write("\n__all__ = [\n")
            for class_name in class_names:
                f.write(f"    '{class_name}',\n")
            f.write("]\n")

        # Extract each class definition and write to separate file
        class_start_indices = [
            i for i, line in enumerate(lines) if line.startswith("class ")
        ]

        # Replace Base with ManagedTable in imports
        import_content = "\n".join(import_lines)
        import_content = import_content.replace(
            "from sqlalchemy.ext.declarative import declarative_base",
            "from unity_orm.model_base import ManagedTable",
        )
        import_content = import_content.replace(
            "from sqlalchemy.orm import declarative_base",
            "from unity_orm.model_base import ManagedTable",
        )
        import_content = import_content.replace(
            "Base = declarative_base()", ""
        )

        for i, start_idx in enumerate(class_start_indices):
            # Determine where this class definition ends
            end_idx = (
                class_start_indices[i + 1]
                if i + 1 < len(class_start_indices)
                else len(lines)
            )

            # Get the class content
            class_lines = lines[start_idx:end_idx]
            class_content = "\n".join(class_lines)

            # Extract the class name
            class_name = (
                class_lines[0].split("(")[0].replace("class ", "").strip()
            )

            # Convert CamelCase to snake_case for filename
            snake_name = ""
            for char in class_name:
                if char.isupper() and snake_name:
                    snake_name += "_" + char.lower()
                else:
                    snake_name += char.lower()

            # Replace Base with ManagedTable in class definition
            class_content = class_content.replace("(Base):", "(ManagedTable):")

            # Create the model file
            file_path = os.path.join(output_dir, f"{snake_name}.py")
            with open(file_path, "w") as f:
                f.write(import_content + "\n\n")
                f.write(class_content)

            print(f"Created model file: {file_path}")


def generate_models_for_catalog_schema(
    access_token: str,
    server_hostname: str,
    http_path: str,
    catalog: str,
    schema: str,
    output_directory: str = "./src/unity_orm/dataplatform",
    verbose: bool = False,
):
    """Generate SQLAlchemy models for a specific catalog and schema.

    Args:
        access_token: Databricks access token.
        server_hostname: Databricks server hostname.
        http_path: Databricks SQL warehouse HTTP path.
        catalog: Catalog name.
        schema: Schema name.
        output_directory: Directory to output generated models. Defaults to ./src/unity_orm/dataplatform.
        verbose: Whether to display verbose debug output.
    """
    # Ensure output directory exists
    os.makedirs(output_directory, exist_ok=True)

    if verbose:
        print(f"Creating dedicated engine for {catalog}.{schema}")
        print(f"Output directory: {output_directory}")

    # Create a dedicated engine for this catalog.schema combination
    # Each catalog.schema gets its own isolated engine instance
    engine = None
    try:
        # The engine.py module handles the URL construction and connection
        engine, url = create_engine(
            access_token=access_token,
            server_hostname=server_hostname,
            http_path=http_path,
            catalog=catalog,
            schema=schema,
            connect_args={
                "_handle_empty_strings_for_numeric_types": "NULL",
                "connection_timeout": 300,
                "socket_timeout": 600,
            },
        )

        if verbose:
            print(f"Engine created successfully for {catalog}.{schema}")
    except Exception as e:
        print(
            f"Error creating database engine for {catalog}.{schema}: {str(e)}"
        )
        if verbose:
            import traceback

            traceback.print_exc()
        raise

    try:
        # Create generator with the dedicated engine
        generator = ModelGenerator(
            engine=engine,
            url=url,
            output_directory=output_directory,
            catalog=catalog,
            schema=schema,
            verbose=verbose,
        )

        # Generate models
        generator.generate_models()
    except Exception as e:
        print(
            f"Error in model generation process for {catalog}.{schema}: {str(e)}"
        )
        if verbose:
            import traceback

            traceback.print_exc()
        raise
    finally:
        # Ensure engine is properly disposed to avoid connection leaks
        if engine is not None:
            try:
                engine.dispose()
                if verbose:
                    print(f"Engine for {catalog}.{schema} disposed")
            except Exception as dispose_error:
                print(
                    f"Warning: Error disposing engine for {catalog}.{schema}: {str(dispose_error)}"
                )
