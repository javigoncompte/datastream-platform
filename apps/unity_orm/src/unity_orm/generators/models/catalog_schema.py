"""Functions for generating models for a specific catalog and schema."""

import os
import traceback

from unity_orm.engine import create_engine


def generate_models_for_catalog_schema(
    access_token: str,
    server_hostname: str,
    http_path: str,
    catalog: str,
    schema: str,
    output_directory: str = "./src/unity_orm/dataplatform",
    verbose: bool = False,
) -> None:
    """Generate models for a specific catalog and schema.

    Args:
        access_token: Access token for Databricks.
        server_hostname: Databricks server hostname.
        http_path: HTTP path for Databricks SQL.
        catalog: Catalog name.
        schema: Schema name.
        output_directory: Directory to write the models to.
        verbose: Whether to print verbose output.
    """
    # Import here to avoid circular imports
    from unity_orm.generators.models.generator import ModelGenerator

    # Construct a proper directory path that includes the catalog name
    catalog_dir = os.path.join(output_directory, catalog)
    os.makedirs(catalog_dir, exist_ok=True)

    if verbose:
        print(f"Creating dedicated engine for {catalog}.{schema}")
        print(f"Output directory: {catalog_dir}")

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
            traceback.print_exc()
        raise

    try:
        generator = ModelGenerator(
            engine=engine,
            url=url,
            output_directory=catalog_dir,  # Pass the catalog directory as output directory
            catalog=catalog,
            schema=schema,
            verbose=verbose,
        )

        generator.generate_models()
    except Exception as e:
        print(f"Error in model generation process for {catalog}.{schema}: {str(e)}")
        if verbose:
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
