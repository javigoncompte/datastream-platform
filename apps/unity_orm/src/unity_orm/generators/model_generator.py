# ruff: noqa
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
    catalog: str
    schema: str
    name: str
    comment: Optional[str] = None


class ModelGenerator:
    def __init__(
        self,
        engine: Engine,
        url: str,
        output_directory: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        verbose: bool = False,
    ):
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

    def get_table_list(self) -> List[TableInfo]:
        tables: List[TableInfo] = []

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

            self._create_directory_structure(models_content)

            print(f"Models successfully generated for {self.catalog}.{self.schema}")

        except Exception as e:
            print(f"Error generating models: {str(e)}")
            if self.verbose:
                import traceback

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

    def _create_directory_structure(self, models_content: str) -> None:
        catalog_dir = self.output_directory
        os.makedirs(catalog_dir, exist_ok=True)

        with open(os.path.join(catalog_dir, "__init__.py"), "w") as f:
            f.write("# Auto-generated SQLAlchemy models\n\n")

        schema_name = self.schema if self.schema else "default"
        schema_dir = os.path.join(catalog_dir, schema_name)
        os.makedirs(schema_dir, exist_ok=True)

        with open(os.path.join(schema_dir, "__init__.py"), "w") as f:
            f.write(f"# Auto-generated SQLAlchemy models for {schema_name} schema\n\n")

        tables_dir = os.path.join(schema_dir, "tables")
        os.makedirs(tables_dir, exist_ok=True)

        self._split_models_to_files(models_content, tables_dir)

        if self.verbose:
            print(f"Models written to {tables_dir}")

    def _split_models_to_files(self, models_content: str, output_dir: str) -> None:
        lines = models_content.splitlines()
        import_lines = []

        i = 0
        while i < len(lines) and not lines[i].startswith("class "):
            import_lines.append(lines[i])
            i += 1

        with open(os.path.join(output_dir, "__init__.py"), "w") as f:
            f.write("# Auto-generated SQLAlchemy models\n\n")

            class_names = []
            for line in lines:
                if line.startswith("class "):
                    class_name = line.split("(")[0].replace("class ", "").strip()
                    class_names.append(class_name)

            for class_name in class_names:
                snake_name = ""
                for char in class_name:
                    if char.isupper() and snake_name:
                        snake_name += "_" + char.lower()
                    else:
                        snake_name += char.lower()

                f.write(f"from .{snake_name} import {class_name}\n")

            f.write("\n__all__ = [\n")
            for class_name in class_names:
                f.write(f"    '{class_name}',\n")
            f.write("]\n")

        class_start_indices = [
            i for i, line in enumerate(lines) if line.startswith("class ")
        ]

        import_content = "\n".join(import_lines)
        import_content = import_content.replace(
            "from sqlalchemy.ext.declarative import declarative_base",
            "from unity_orm.model_base import ManagedTable",
        )
        import_content = import_content.replace(
            "from sqlalchemy.orm import declarative_base",
            "from unity_orm.model_base import ManagedTable",
        )
        import_content = import_content.replace("Base = declarative_base()", "")

        for i, start_idx in enumerate(class_start_indices):
            end_idx = (
                class_start_indices[i + 1]
                if i + 1 < len(class_start_indices)
                else len(lines)
            )

            class_lines = lines[start_idx:end_idx]
            class_content = "\n".join(class_lines)

            class_name = class_lines[0].split("(")[0].replace("class ", "").strip()

            snake_name = ""
            for char in class_name:
                if char.isupper() and snake_name:
                    snake_name += "_" + char.lower()
                else:
                    snake_name += char.lower()

            class_content = class_content.replace("(Base):", "(ManagedTable):")

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
    os.makedirs(output_directory, exist_ok=True)

    if verbose:
        print(f"Creating dedicated engine for {catalog}.{schema}")
        print(f"Output directory: {output_directory}")

    engine = None
    try:
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
        print(f"Error creating database engine for {catalog}.{schema}: {str(e)}")
        if verbose:
            import traceback

            traceback.print_exc()
        raise

    try:
        generator = ModelGenerator(
            engine=engine,
            url=url,
            output_directory=output_directory,
            catalog=catalog,
            schema=schema,
            verbose=verbose,
        )

        generator.generate_models()
    except Exception as e:
        print(f"Error in model generation process for {catalog}.{schema}: {str(e)}")
        if verbose:
            import traceback

            traceback.print_exc()
        raise
    finally:
        if engine is not None:
            try:
                engine.dispose()
                if verbose:
                    print(f"Engine for {catalog}.{schema} disposed")
            except Exception as dispose_error:
                print(
                    f"Warning: Error disposing engine for {catalog}.{schema}: {str(dispose_error)}"
                )
