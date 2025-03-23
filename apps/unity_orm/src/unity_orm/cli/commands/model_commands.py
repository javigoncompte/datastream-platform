"""Model generation commands for Unity ORM CLI."""

import os
from pathlib import Path
from typing import Optional

import click
from click import style
from databricks.connect import DatabricksSession
from databricks.sdk.core import Config

from unity_orm.generators import generate_models_for_catalog_schema


def register_commands(cli):
    """Register model generation commands with the CLI."""
    cli.add_command(generate_models)
    cli.add_command(generate_single)
    cli.add_command(generate_all_catalogs)


@click.command()
@click.option(
    "--access-token",
    envvar="DATABRICKS_TOKEN",
    help="Databricks access token (optional, uses databricks-connect auth if not provided).",
)
@click.option(
    "--server-hostname",
    envvar="DATABRICKS_SERVER_HOSTNAME",
    help="Databricks server hostname (optional, uses databricks-connect auth if not provided).",
)
@click.option(
    "--http-path",
    envvar="DATABRICKS_HTTP_PATH",
    help="Databricks HTTP path (optional, uses databricks-connect auth if not provided).",
)
@click.option(
    "--catalog",
    required=True,
    help="Catalog name in Unity Catalog.",
)
@click.option(
    "--schema",
    help="Schema name in Unity Catalog.",
)
@click.option(
    "--output-directory",
    default="./src/unity_orm/dataplatform",
    help="Directory to store generated models. Default: src/unity_orm/dataplatform",
)
@click.option(
    "--verbose",
    is_flag=True,
    help="Enable verbose output for debugging.",
)
def generate_models(
    access_token: str,
    server_hostname: str,
    http_path: str,
    catalog: str,
    schema: Optional[str] = None,
    output_directory: str = "./src/unity_orm/dataplatform",
    verbose: bool = False,
):
    """Generate SQLAlchemy models from existing Databricks tables."""
    try:
        # Ensure output directory exists
        os.makedirs(output_directory, exist_ok=True)

        # If schema is not provided, use "default" as the default schema
        schema_to_use = schema if schema is not None else "default"

        click.echo(
            style(
                f"Generating models for {catalog}.{schema_to_use}", fg="blue"
            )
        )
        click.echo(
            style(
                f"Creating dedicated engine for {catalog}.{schema_to_use}",
                fg="blue",
            )
        )

        # Generate models with a dedicated engine for this catalog.schema
        generate_models_for_catalog_schema(
            access_token=access_token,
            server_hostname=server_hostname,
            http_path=http_path,
            catalog=catalog,
            schema=schema_to_use,
            output_directory=output_directory,
            verbose=verbose,
        )

        click.echo(
            style(
                f"✅ Models generated successfully for {catalog}.{schema_to_use}",
                fg="green",
            )
        )
        click.echo(f"Output directory: {output_directory}")

    except Exception as e:
        click.echo(
            style(
                f"❌ Error generating models for {catalog}.{schema_to_use if schema is not None else 'default'}: {str(e)}",
                fg="red",
            )
        )
        if verbose:
            import traceback

            click.echo(traceback.format_exc())
        click.echo(
            style(
                "If you're seeing URL or connection errors, please check your Databricks credentials",
                fg="yellow",
            )
        )
        click.echo(
            style(
                "Try running with --verbose for more detailed error information",
                fg="yellow",
            )
        )


@click.command()
@click.option(
    "--access-token",
    envvar="DATABRICKS_TOKEN",
    help="Databricks access token (optional, uses databricks-connect auth if not provided).",
)
@click.option(
    "--server-hostname",
    envvar="DATABRICKS_SERVER_HOSTNAME",
    help="Databricks server hostname (optional, uses databricks-connect auth if not provided).",
)
@click.option(
    "--http-path",
    envvar="DATABRICKS_HTTP_PATH",
    help="Databricks HTTP path (optional, uses databricks-connect auth if not provided).",
)
@click.option(
    "--verbose",
    is_flag=True,
    help="Enable verbose output for debugging.",
)
@click.option(
    "--output-directory",
    default="./src/unity_orm/dataplatform",
    help="Directory to store generated models. Default: src/unity_orm/dataplatform",
)
@click.argument("catalog_schema")
def generate_single(
    access_token: str,
    server_hostname: str,
    http_path: str,
    catalog_schema: str,
    verbose: bool = False,
    output_directory: str = "./src/unity_orm/dataplatform",
):
    """Generate SQLAlchemy models for a single catalog.schema.

    CATALOG_SCHEMA should be in the format 'catalog.schema'.

    Example:
        unity-orm generate-single main.default
    """
    # Parse catalog.schema format
    if "." not in catalog_schema:
        click.echo(
            style(
                "Error: CATALOG_SCHEMA must be in the format 'catalog.schema'",
                fg="red",
            )
        )
        click.echo("Example: unity-orm generate-single main.default")
        return

    catalog, schema = catalog_schema.split(".", 1)

    if not catalog or not schema:
        click.echo(
            style("Error: Both catalog and schema must be provided", fg="red")
        )
        click.echo("Example: unity-orm generate-single main.default")
        return

    click.echo(style(f"Generating models for {catalog}.{schema}", fg="blue"))

    # Set up output directory
    schema_output_dir = os.path.join(output_directory, catalog, schema)
    os.makedirs(schema_output_dir, exist_ok=True)

    if verbose:
        click.echo(style("Verbose mode enabled", fg="yellow"))
        click.echo(f"Output directory: {schema_output_dir}")

    try:
        # Get authentication configuration from current session if not provided
        if not access_token or not server_hostname or not http_path:
            click.echo("Using Databricks Connect authentication...")
            try:
                config = Config()

                if not access_token:
                    access_token = config.token
                    if verbose:
                        click.echo(
                            "Using access token from Databricks Connect"
                        )

                if not server_hostname:
                    server_hostname = config.host
                    if verbose:
                        click.echo(
                            f"Using server hostname from Databricks Connect: {server_hostname}"
                        )

                if not http_path:
                    http_path = click.prompt(
                        "Enter HTTP path for SQL warehouse",
                        default="/sql/1.0/warehouses/ea1d76591241b3a0",
                    )
                    if verbose:
                        click.echo(f"HTTP path: {http_path}")
            except Exception as auth_error:
                click.echo(
                    style(
                        f"Error with Databricks Connect authentication: {str(auth_error)}",
                        fg="red",
                    )
                )
                if verbose:
                    import traceback

                    click.echo(traceback.format_exc())
                return

        # Validate all required parameters are present
        if not access_token or not server_hostname or not http_path:
            click.echo(
                style(
                    "Error: Missing required parameters for connection",
                    fg="red",
                )
            )
            click.echo(
                "Please provide access_token, server_hostname, and http_path"
            )
            return

        click.echo(
            style(
                f"Creating dedicated engine for {catalog}.{schema}", fg="blue"
            )
        )

        # Generate models with a dedicated engine for this catalog.schema
        generate_models_for_catalog_schema(
            access_token=access_token,
            server_hostname=server_hostname,
            http_path=http_path,
            catalog=catalog,
            schema=schema,
            output_directory=schema_output_dir,
            verbose=verbose,
        )

        click.echo(
            style(
                f"✅ Models successfully generated for {catalog}.{schema}",
                fg="green",
            )
        )
        click.echo(f"Output directory: {schema_output_dir}")

    except Exception as e:
        click.echo(
            style(
                f"❌ Error generating models for {catalog}.{schema}: {str(e)}",
                fg="red",
            )
        )
        if verbose:
            import traceback

            click.echo(traceback.format_exc())
        click.echo(
            style(
                "If you're seeing URL or connection errors, please check your Databricks credentials",
                fg="yellow",
            )
        )
        click.echo(
            style(
                "Try running with --verbose for more detailed error information",
                fg="yellow",
            )
        )


def get_filtered_catalogs(all_catalog_names, show_all, debug, no_filter):
    """Filter catalog names based on filtering options."""
    if no_filter:
        click.echo(
            style("No filtering applied (--no-filter option)", fg="green")
        )
        return all_catalog_names, [], []

    # Filter catalogs - use complete list of env prefixes
    env_prefixes = ["qa_", "test_", "dev_", "stage_", "prod_"]
    env_filtered = []
    dunder_filtered = []
    filtered_catalogs = []

    # Apply filtering logic based on flags
    for catalog_name in all_catalog_names:
        if catalog_name.startswith("__") and not debug:
            dunder_filtered.append(catalog_name)
            continue

        if not show_all and any(
            catalog_name.startswith(prefix) for prefix in env_prefixes
        ):
            env_filtered.append(catalog_name)
            continue

        filtered_catalogs.append(catalog_name)

    return filtered_catalogs, dunder_filtered, env_filtered


def get_filtered_schemas(all_schema_names, include_all_schemas):
    """Filter schema names based on filtering options."""
    if include_all_schemas:
        return [s for s in all_schema_names if s != "information_schema"]

    env_prefixes = ["qa_", "test_", "dev_", "stage_", "prod_"]
    schema_names = []

    for schema_name in all_schema_names:
        if (
            not any(schema_name.startswith(prefix) for prefix in env_prefixes)
            and schema_name != "information_schema"
        ):
            schema_names.append(schema_name)

    return schema_names


def process_schema(
    catalog_name,
    schema_name,
    catalog_output_dir,
    access_token,
    server_hostname,
    http_path,
    verbose,
):
    """Process a single schema and generate models."""
    if schema_name == "information_schema":
        # Skip information_schema as it's system tables
        return True

    click.echo(f"Generating models for {catalog_name}.{schema_name}...")

    # Set schema-specific output directory for use with model generator
    schema_output_dir = os.path.join(catalog_output_dir, schema_name)
    os.makedirs(schema_output_dir, exist_ok=True)

    try:
        # Create a dedicated engine for each catalog.schema
        generate_models_for_catalog_schema(
            access_token=access_token,
            server_hostname=server_hostname,
            http_path=http_path,
            catalog=catalog_name,
            schema=schema_name,
            output_directory=schema_output_dir,
            verbose=verbose,
        )
        click.echo(
            style(
                f"✓ Models for {catalog_name}.{schema_name} generated successfully",
                fg="green",
            )
        )
        return True
    except Exception as e:
        click.echo(
            style(
                f"Error generating models for {catalog_name}.{schema_name}: {str(e)}",
                fg="red",
            )
        )
        if verbose:
            import traceback

            click.echo(traceback.format_exc())
        return False


def process_catalog_schemas(
    spark,
    catalog_name,
    catalog_output_dir,
    access_token,
    server_hostname,
    http_path,
    include_all_schemas,
    verbose,
):
    """Process all schemas in a catalog and return success status."""
    click.echo(f"Fetching schemas for catalog: {catalog_name}")

    try:
        schemas_df = fetch_schemas(spark, catalog_name)
    except Exception:
        # Failed to get schemas
        return False

    if not schemas_df:
        click.echo(
            style(
                f"No schemas found in catalog '{catalog_name}'. Skipping.",
                fg="yellow",
            )
        )
        return None  # None means skipped

    # Get all schema names
    all_schema_names = [schema.databaseName for schema in schemas_df]

    # Filter schemas
    schema_names = get_filtered_schemas(all_schema_names, include_all_schemas)

    if not schema_names:
        click.echo(
            style(
                f"No suitable schemas found in catalog '{catalog_name}' after filtering. Skipping.",
                fg="yellow",
            )
        )
        return None  # None means skipped

    click.echo(
        style(
            f"Found {len(schema_names)} schemas in catalog '{catalog_name}'",
            fg="green",
        )
    )

    # Generate models for each schema in this catalog
    catalog_success = True
    for schema_name in schema_names:
        schema_success = process_schema(
            catalog_name,
            schema_name,
            catalog_output_dir,
            access_token,
            server_hostname,
            http_path,
            verbose,
        )
        if not schema_success:
            catalog_success = False

    return catalog_success


def fetch_schemas(spark, catalog_name):
    """Fetch schemas with retry on session failure."""
    try:
        return spark.sql(f"SHOW SCHEMAS IN {catalog_name}").collect()
    except Exception as e:
        click.echo(f"Spark session failed: {str(e)}")
        click.echo("Getting a new Spark session and retrying...")
        from unity_orm.databricks_session import get_spark

        spark = get_spark()
        return spark.sql(f"SHOW SCHEMAS IN {catalog_name}").collect()


def verify_authentication(access_token, server_hostname, http_path, verbose):
    """Verify authentication parameters, using Databricks Connect if needed."""
    if access_token and server_hostname and http_path:
        return access_token, server_hostname, http_path

    click.echo("\nUsing Databricks Connect authentication...")
    try:
        config = Config()

        if not access_token:
            access_token = config.token
            if verbose:
                click.echo("Using access token from Databricks Connect")

        if not server_hostname:
            server_hostname = config.host
            if verbose:
                click.echo(
                    f"Using server hostname from Databricks Connect: {server_hostname}"
                )

        if not http_path:
            # Try to get HTTP path from prompt, falling back to a default
            click.echo(style("Warehouse HTTP path is required", fg="yellow"))
            http_path = click.prompt(
                "Enter HTTP path for SQL warehouse",
                default="/sql/1.0/warehouses/ea1d76591241b3a0",
            )
            if verbose:
                click.echo(f"Using HTTP path: {http_path}")

        return access_token, server_hostname, http_path

    except Exception as auth_error:
        click.echo(
            style(
                f"Error with Databricks Connect authentication: {str(auth_error)}",
                fg="red",
            )
        )
        if verbose:
            import traceback

            click.echo(traceback.format_exc())
        return None, None, None


def print_summary(
    filtered_catalogs,
    successful_catalogs,
    failed_catalogs,
    skipped_catalogs,
    base_output_directory,
):
    """Print a summary of the catalog processing results."""
    click.echo(
        style(
            "\n===== Multi-Catalog Model Generation Summary =====",
            fg="blue",
            bold=True,
        )
    )
    click.echo(
        style(f"Total catalogs processed: {len(filtered_catalogs)}", fg="blue")
    )
    click.echo(
        style(
            f"Successfully processed: {len(successful_catalogs)}", fg="green"
        )
    )

    if successful_catalogs:
        click.echo(style("Successfully processed catalogs:", fg="green"))
        for catalog in successful_catalogs:
            click.echo(f"  - {catalog}")

    if failed_catalogs:
        click.echo(
            style(f"Failed to process: {len(failed_catalogs)}", fg="red")
        )
        click.echo(style("Failed catalogs:", fg="red"))
        for catalog in failed_catalogs:
            click.echo(f"  - {catalog}")

    if skipped_catalogs:
        click.echo(style(f"Skipped: {len(skipped_catalogs)}", fg="yellow"))
        click.echo(
            style("Skipped catalogs (no suitable schemas):", fg="yellow")
        )
        for catalog in skipped_catalogs:
            click.echo(f"  - {catalog}")

    click.echo(
        style(
            "\n✨ Multi-catalog model generation complete!",
            fg="green",
            bold=True,
        )
    )
    click.echo(f"You can find your models in: {base_output_directory}")


@click.command()
@click.option(
    "--access-token",
    envvar="DATABRICKS_TOKEN",
    help="Databricks access token (optional, uses databricks-connect auth if not provided).",
)
@click.option(
    "--server-hostname",
    envvar="DATABRICKS_SERVER_HOSTNAME",
    help="Databricks server hostname (optional, uses databricks-connect auth if not provided).",
)
@click.option(
    "--http-path",
    envvar="DATABRICKS_HTTP_PATH",
    help="Databricks HTTP path (optional, uses databricks-connect auth if not provided).",
)
@click.option(
    "--base-output-directory",
    default="./src/unity_orm/dataplatform",
    help="Base directory to store generated models. Default: src/unity_orm/dataplatform",
)
@click.option(
    "--show-all",
    is_flag=True,
    help="Include environment-specific catalogs (qa_, test_, etc.)",
)
@click.option(
    "--debug",
    is_flag=True,
    help="Include system catalogs (with __ prefix)",
)
@click.option(
    "--no-filter",
    is_flag=True,
    help="Show all catalogs without any filtering (overrides --show-all and --debug)",
)
@click.option(
    "--include-all-schemas",
    is_flag=True,
    default=True,
    help="Include all schemas in each catalog (excluding information_schema)",
)
@click.option(
    "--verbose",
    is_flag=True,
    help="Enable verbose output for debugging.",
)
def generate_all_catalogs(
    access_token: str,
    server_hostname: str,
    http_path: str,
    base_output_directory: str = "./src/unity_orm/dataplatform",
    show_all: bool = False,
    debug: bool = False,
    no_filter: bool = False,
    include_all_schemas: bool = True,
    verbose: bool = False,
):
    """Generate models for all available catalogs based on filtering options.

    This command automatically processes all catalogs that match your filtering criteria.
    By default, it filters out system catalogs (starting with __) and environment-specific
    catalogs (starting with qa_, test_, etc). Use --show-all and --debug to modify filtering.
    """
    click.echo(
        style(
            "\n🔍 Unity ORM Multi-Catalog Model Generator",
            fg="blue",
            bold=True,
        )
    )
    click.echo(style("---------------------------------------", fg="blue"))

    spark = None
    try:
        # Create Databricks session for authentication and queries
        click.echo("\nConnecting to Databricks...")
        spark = (
            DatabricksSession.builder.serverless(True)
            .profile("qa")
            .getOrCreate()
        )
        click.echo(style("✓ Connected to Databricks", fg="green"))

        # Get catalogs from Databricks
        click.echo("\nFetching available catalogs from Databricks...")
        catalogs_df = spark.sql("SHOW CATALOGS").collect()

        if not catalogs_df:
            click.echo(style("No catalogs found!", fg="red"))
            return

        # Process catalog names
        all_catalog_names = [catalog.catalog for catalog in catalogs_df]
        click.echo(
            style(
                f"Total catalogs in Databricks: {len(all_catalog_names)} catalogs found",
                fg="blue",
            )
        )

        # Display raw catalog names in debug mode
        if debug:
            click.echo("\nAll available catalogs (unfiltered):")
            for i, catalog in enumerate(all_catalog_names):
                click.echo(f"  {i + 1}. {catalog}")

        # Get filtered catalogs
        filtered_catalogs, dunder_filtered, env_filtered = (
            get_filtered_catalogs(
                all_catalog_names, show_all, debug, no_filter
            )
        )

        # Report filtering stats
        if dunder_filtered:
            click.echo(
                style(
                    f"Filtered out {len(dunder_filtered)} system catalogs with __ prefix",
                    fg="yellow",
                )
            )
            click.echo(
                style("Use --debug to include system catalogs", fg="yellow")
            )

        if env_filtered:
            click.echo(
                style(
                    f"Filtered out {len(env_filtered)} environment-specific catalogs",
                    fg="yellow",
                )
            )
            click.echo(
                style(
                    "Use --show-all to include environment-specific catalogs",
                    fg="yellow",
                )
            )

        # Sort the catalogs alphabetically
        filtered_catalogs.sort()

        click.echo(
            style(
                f"Catalogs to process after filtering: {len(filtered_catalogs)}",
                fg="green",
            )
        )
        if not filtered_catalogs:
            click.echo(
                style(
                    "No catalogs to process after filtering. Try --show-all or --debug options.",
                    fg="red",
                )
            )
            return

        # Show list of catalogs that will be processed
        click.echo("\nThe following catalogs will be processed:")
        for i, catalog_name in enumerate(filtered_catalogs):
            click.echo(style(f"  {i + 1}. {catalog_name}", fg="cyan"))

        # Verify authentication parameters
        auth_result = verify_authentication(
            access_token, server_hostname, http_path, verbose
        )
        if auth_result[0] is not None:
            access_token, server_hostname, http_path = auth_result

        # Validate all required parameters are present
        if not access_token or not server_hostname or not http_path:
            click.echo(
                style(
                    "Error: Missing required parameters for connection",
                    fg="red",
                )
            )
            click.echo(
                "Please provide access_token, server_hostname, and http_path"
            )
            return

        # Ask for confirmation before proceeding
        if len(filtered_catalogs) > 3:
            click.echo(
                style(
                    f"\nYou are about to generate models for {len(filtered_catalogs)} catalogs.",
                    fg="yellow",
                    bold=True,
                )
            )
            confirm = click.confirm("Do you want to continue?", default=True)
            if not confirm:
                click.echo(style("Operation cancelled by user", fg="yellow"))
                return

        # Track overall status
        successful_catalogs = []
        failed_catalogs = []
        skipped_catalogs = []

        # Process each catalog
        for catalog_name in filtered_catalogs:
            click.echo(
                style(
                    f"\nProcessing catalog: {catalog_name}",
                    fg="blue",
                    bold=True,
                )
            )
            catalog_output_dir = str(
                Path(base_output_directory) / catalog_name
            )

            try:
                result = process_catalog_schemas(
                    spark,
                    catalog_name,
                    catalog_output_dir,
                    access_token,
                    server_hostname,
                    http_path,
                    include_all_schemas,
                    verbose,
                )

                if result is None:  # Skipped
                    skipped_catalogs.append(catalog_name)
                elif result:  # Success
                    successful_catalogs.append(catalog_name)
                else:  # Failed
                    failed_catalogs.append(catalog_name)

            except Exception as e:
                click.echo(
                    style(
                        f"Error processing catalog '{catalog_name}': {str(e)}",
                        fg="red",
                    )
                )
                if verbose:
                    import traceback

                    click.echo(traceback.format_exc())
                failed_catalogs.append(catalog_name)

            click.echo(
                style(
                    f"Completed processing catalog: {catalog_name}", fg="blue"
                )
            )
            click.echo(style("-" * 50, fg="blue"))

        # Show summary of processing
        print_summary(
            filtered_catalogs,
            successful_catalogs,
            failed_catalogs,
            skipped_catalogs,
            base_output_directory,
        )

    except Exception as e:
        click.echo(style(f"Error: {str(e)}", fg="red"))
        if verbose:
            import traceback

            click.echo(traceback.format_exc())
    finally:
        # Close the Spark session if it was created
        try:
            if spark is not None:
                spark.stop()
        except:
            pass
