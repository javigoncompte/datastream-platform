"""Model generator for Unity ORM."""

import importlib.util
import os
import shutil
import subprocess
import tempfile
import traceback
from pathlib import Path
from typing import List, Optional

from sqlalchemy import inspect
from sqlalchemy.engine import Engine

from unity_orm.logger import get_logger

logger = get_logger(__name__)


# Import after defining the class to avoid circular imports
class ModelGenerator:
    """Generate SQLAlchemy models from a database."""

    def __init__(
        self,
        engine: Engine,
        url: str,
        output_directory: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        verbose: bool = False,
    ):
        """Initialize ModelGenerator.

        Args:
            engine: SQLAlchemy engine.
            url: Database URL.
            output_directory: Directory to write models to.
            catalog: Catalog name.
            schema: Schema name.
            verbose: Whether to print verbose output.
        """
        self.engine = engine
        self.url = url
        self.output_directory = Path(output_directory)
        self.catalog = catalog
        self.schema = schema
        self.verbose = verbose
        self.inspector = inspect(engine)

        self.has_sqlacodegen = False
        try:
            self.has_sqlacodegen = importlib.util.find_spec("sqlacodegen") is not None
            if not self.has_sqlacodegen:
                print(
                    "sqlacodegen is not installed. It should be included as a dependency."
                )
                print(
                    "Please check your installation or add sqlacodegen to your dependencies."
                )
        except ImportError:
            print("Error importing sqlacodegen. Make sure it's installed.")

    def get_table_list(self) -> List:
        """Get list of tables in the catalog and schema.

        Returns:
            List of TableInfo objects.
        """
        # Import here to avoid circular imports
        from unity_orm.generators.models.table_info import TableInfo

        tables = []

        if self.catalog:
            if self.verbose:
                print(f"Fetching schemas for catalog: {self.catalog}")

            try:
                schemas = self.inspector.get_schema_names()

                for schema_name in schemas:
                    if self.schema and schema_name != self.schema:
                        continue

                    if schema_name == "information_schema":
                        continue

                    if self.verbose:
                        print(f"Fetching tables for schema: {schema_name}")

                    try:
                        table_names = self.inspector.get_table_names(schema=schema_name)

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
                print(f"Error fetching schemas for catalog {self.catalog}: {str(e)}")

        return tables

    def generate_models(self) -> None:
        """Generate SQLAlchemy models from the database."""
        if self.verbose:
            print(f"Getting list of tables in {self.catalog}.{self.schema}")

            logger.info(f"Engine details: {self.engine}")
            logger.info(f"Engine driver: {self.engine.driver}")
            logger.info(f"Engine dialect: {type(self.engine.dialect).__name__}")

            insp = inspect(self.engine)
            logger.info(f"Inspector: {insp}")

            import sqlalchemy

            logger.info(f"SQLAlchemy version: {sqlalchemy.__version__}")

        tables = self.get_table_list()
        if not tables:
            print(f"No tables found in {self.catalog}.{self.schema}")
            return

        print(f"Found {len(tables)} tables in {self.catalog}.{self.schema}")

        if self.verbose:
            for i, table in enumerate(tables):
                print(f"  {i + 1}. {table.name}")

                try:
                    columns = self.inspector.get_columns(
                        table.name, schema=table.schema
                    )
                    print(f"    Columns for {table.name}:")
                    for col in columns:
                        print(f"      {col['name']}: {col['type']}")
                except Exception as e:
                    print(f"    Error getting columns for {table.name}: {str(e)}")

            print("\nConfiguring sqlacodegen...")

        temp_dir = tempfile.mkdtemp(prefix="sqlacodegen_")
        temp_models_file = os.path.join(temp_dir, "models.py")

        try:
            url_obj = self.engine.url
            db_url = str(url_obj)

            if "_handle_empty_strings_for_numeric_types=NULL" not in db_url:
                separator = "&" if "?" in db_url else "?"
                db_url += f"{separator}_handle_empty_strings_for_numeric_types=NULL"

            if self.verbose:
                logger.info(f"Using database URL: {db_url}")
                logger.info(f"Output file: {temp_models_file}")

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

            process = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
            )

            if process.stderr:
                if self.verbose:
                    print(f"sqlacodegen stderr: {process.stderr}")
                if process.returncode != 0:
                    raise RuntimeError(f"sqlacodegen failed: {process.stderr}")

            if not os.path.exists(temp_models_file):
                raise RuntimeError(f"Output file {temp_models_file} not created")

            with open(temp_models_file, "r") as f:
                models_content = f.read()

            if not models_content.strip():
                raise RuntimeError("Empty output generated by sqlacodegen")

            # Import here to avoid circular imports
            from unity_orm.generators.models.directory_structure import (
                create_directory_structure,
            )
            from unity_orm.generators.models.file_splitter import split_models_to_files

            create_directory_structure(
                models_content=models_content,
                output_directory=self.output_directory,
                schema=self.schema,
                verbose=self.verbose,
                split_models_to_files_func=split_models_to_files,
            )

            print(f"Models successfully generated for {self.catalog}.{self.schema}")

        except Exception as e:
            print(f"Error generating models: {str(e)}")
            if self.verbose:
                traceback.print_exc()
        finally:
            try:
                shutil.rmtree(temp_dir)
                if self.verbose:
                    print(f"Temporary directory {temp_dir} removed")
            except Exception as cleanup_error:
                print(
                    f"Warning: Could not clean up temporary directory: {str(cleanup_error)}"
                )
