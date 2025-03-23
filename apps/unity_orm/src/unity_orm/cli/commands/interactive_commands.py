"""Interactive commands for Unity ORM CLI."""

import os

import click
from click import style
from databricks.connect import DatabricksSession
from databricks.sdk.core import Config

from unity_orm.generators import generate_models_for_catalog_schema


def register_commands(cli):
    """Register interactive commands with the CLI."""
    cli.add_command(interactive)


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
    "--select-all",
    is_flag=True,
    help="Select all available catalogs after filtering",
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
    "--base-output-directory",
    default="./src/unity_orm/dataplatform",
    help="Base directory to store generated models. Default: src/unity_orm/dataplatform",
)
@click.option(
    "--verbose",
    is_flag=True,
    help="Enable verbose output for debugging.",
)
def interactive(
    access_token: str,
    server_hostname: str,
    http_path: str,
    show_all: bool = False,
    debug: bool = False,
    select_all: bool = False,
    no_filter: bool = False,
    include_all_schemas: bool = True,
    base_output_directory: str = "./src/unity_orm/dataplatform",
    verbose: bool = False,
):
    """Interactive CLI to select catalogs and schemas for model generation.

    This command allows you to select catalogs and schemas in an interactive manner,
    and then generates SQLAlchemy models for the selected catalog-schema combinations.
    """
    click.echo(
        style(
            "\n🔍 Unity ORM Interactive Model Generator", fg="blue", bold=True
        )
    )
    click.echo(style("---------------------------------------", fg="blue"))

    try:
        # Create Databricks session for authentication and queries
        click.echo("\nConnecting to Databricks...")
        spark = DatabricksSession.builder.serverless(True).getOrCreate()
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

        # Check if we should skip filtering
        if no_filter:
            click.echo(
                style("No filtering applied (--no-filter option)", fg="green")
            )
            filtered_catalogs = all_catalog_names
        else:
            # Filter catalogs
            env_prefixes = ["qa_", "test_", "dev_", "stage_", "prod_"]
            filtered_catalogs = []

            # Apply filtering logic based on flags
            for catalog_name in all_catalog_names:
                if catalog_name.startswith("__") and not debug:
                    continue

                if not show_all and any(
                    catalog_name.startswith(prefix) for prefix in env_prefixes
                ):
                    continue

                filtered_catalogs.append(catalog_name)

        # Sort the catalogs alphabetically
        filtered_catalogs.sort()

        if not filtered_catalogs:
            click.echo(
                style(
                    "No catalogs to select after filtering. Try --show-all or --debug options.",
                    fg="red",
                )
            )
            return

        # Show list of catalogs that are available for selection
        click.echo("\nAvailable catalogs:")
        for i, catalog_name in enumerate(filtered_catalogs):
            click.echo(style(f"  {i + 1}. {catalog_name}", fg="cyan"))

        # Handle --select-all flag
        selected_indices = []
        if select_all:
            click.echo(
                style(
                    "\nSelecting all available catalogs due to --select-all flag",
                    fg="green",
                )
            )
            selected_indices = list(range(len(filtered_catalogs)))
        else:
            # Prompt user to select catalogs
            click.echo(
                "\nSelect catalogs (comma-separated list of numbers, e.g., '1,3,5')"
            )
            click.echo("or enter 'all' to select all catalogs")
            selection = click.prompt("Selection", default="1")

            if selection.lower() == "all":
                selected_indices = list(range(len(filtered_catalogs)))
            else:
                try:
                    # Parse comma-separated selection
                    selected_indices = [
                        int(idx.strip()) - 1 for idx in selection.split(",")
                    ]
                    # Filter out invalid indices
                    selected_indices = [
                        idx
                        for idx in selected_indices
                        if 0 <= idx < len(filtered_catalogs)
                    ]
                except ValueError:
                    click.echo(
                        style(
                            "Invalid selection. Please enter comma-separated numbers.",
                            fg="red",
                        )
                    )
                    return

            if not selected_indices:
                click.echo(
                    style("No valid catalogs selected. Exiting.", fg="red")
                )
                return

        # List selected catalogs
        selected_catalogs = [
            filtered_catalogs[idx] for idx in selected_indices
        ]
        click.echo(style("\nSelected catalogs:", fg="green"))
        for i, catalog_name in enumerate(selected_catalogs):
            click.echo(style(f"  {i + 1}. {catalog_name}", fg="green"))

        # Get authentication configuration from current session if not provided
        if not access_token or not server_hostname or not http_path:
            click.echo("\nUsing Databricks Connect authentication...")
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
                    # Try to get HTTP path from prompt, falling back to a default
                    click.echo(
                        style("Warehouse HTTP path is required", fg="yellow")
                    )
                    http_path = click.prompt(
                        "Enter HTTP path for SQL warehouse",
                        default="/sql/1.0/warehouses/ea1d76591241b3a0",
                    )
                    if verbose:
                        click.echo(f"Using HTTP path: {http_path}")
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

        # Track overall status
        successful_catalogs = []
        failed_catalogs = []
        skipped_catalogs = []

        # Ask for confirmation before proceeding
        if len(selected_catalogs) > 3:
            click.echo(
                style(
                    f"\nYou are about to generate models for {len(selected_catalogs)} catalogs.",
                    fg="yellow",
                    bold=True,
                )
            )
            confirm = click.confirm("Do you want to continue?", default=True)
            if not confirm:
                click.echo(style("Operation cancelled by user", fg="yellow"))
                return

        # Process each selected catalog
        for catalog_name in selected_catalogs:
            click.echo(
                style(
                    f"\nProcessing catalog: {catalog_name}",
                    fg="blue",
                    bold=True,
                )
            )

            try:
                # Get schemas for this catalog
                try:
                    click.echo(f"Fetching schemas for catalog: {catalog_name}")
                    schemas_df = spark.sql(
                        f"SHOW SCHEMAS IN {catalog_name}"
                    ).collect()

                    if not schemas_df:
                        click.echo(
                            style(
                                f"No schemas found in catalog '{catalog_name}'. Skipping.",
                                fg="yellow",
                            )
                        )
                        skipped_catalogs.append(catalog_name)
                        continue

                    # Get all schema names
                    all_schema_names = [
                        schema.databaseName for schema in schemas_df
                    ]

                    # Filter out system schemas and optionally environment-specific schemas
                    if include_all_schemas:
                        schema_names = [
                            s
                            for s in all_schema_names
                            if s != "information_schema"
                        ]
                    else:
                        env_prefixes = [
                            "qa_",
                            "test_",
                            "dev_",
                            "stage_",
                            "prod_",
                        ]
                        schema_names = []

                        for schema_name in all_schema_names:
                            if (
                                not any(
                                    schema_name.startswith(prefix)
                                    for prefix in env_prefixes
                                )
                                and schema_name != "information_schema"
                            ):
                                schema_names.append(schema_name)

                    if not schema_names:
                        click.echo(
                            style(
                                f"No suitable schemas found in catalog '{catalog_name}' after filtering. Skipping.",
                                fg="yellow",
                            )
                        )
                        skipped_catalogs.append(catalog_name)
                        continue

                    # Show available schemas for this catalog
                    click.echo(
                        style(
                            f"\nAvailable schemas in catalog '{catalog_name}':",
                            fg="blue",
                        )
                    )
                    for i, schema_name in enumerate(schema_names):
                        click.echo(
                            style(f"  {i + 1}. {schema_name}", fg="cyan")
                        )

                    # Prompt user to select schemas
                    click.echo(
                        "\nSelect schemas (comma-separated list of numbers, e.g., '1,3,5')"
                    )
                    click.echo("or enter 'all' to select all schemas")

                    selection = click.prompt("Selection", default="all")

                    selected_schema_indices = []
                    if selection.lower() == "all":
                        selected_schema_indices = list(
                            range(len(schema_names))
                        )
                    else:
                        try:
                            # Parse comma-separated selection
                            selected_schema_indices = [
                                int(idx.strip()) - 1
                                for idx in selection.split(",")
                            ]
                            # Filter out invalid indices
                            selected_schema_indices = [
                                idx
                                for idx in selected_schema_indices
                                if 0 <= idx < len(schema_names)
                            ]
                        except ValueError:
                            click.echo(
                                style(
                                    "Invalid selection. Using all schemas.",
                                    fg="yellow",
                                )
                            )
                            selected_schema_indices = list(
                                range(len(schema_names))
                            )

                    if not selected_schema_indices:
                        click.echo(
                            style(
                                f"No valid schemas selected for catalog '{catalog_name}'. Skipping.",
                                fg="yellow",
                            )
                        )
                        skipped_catalogs.append(catalog_name)
                        continue

                    # List selected schemas
                    selected_schemas = [
                        schema_names[idx] for idx in selected_schema_indices
                    ]
                    click.echo(
                        style(
                            f"\nSelected schemas for catalog '{catalog_name}':",
                            fg="green",
                        )
                    )
                    for i, schema_name in enumerate(selected_schemas):
                        click.echo(
                            style(f"  {i + 1}. {schema_name}", fg="green")
                        )

                    # Create output directory for catalog
                    catalog_output_dir = os.path.join(
                        base_output_directory, catalog_name
                    )

                    # Generate models for each selected schema
                    catalog_success = True
                    for schema_name in selected_schemas:
                        click.echo(
                            f"\nGenerating models for {catalog_name}.{schema_name}..."
                        )

                        # Set schema-specific output directory
                        schema_output_dir = os.path.join(
                            catalog_output_dir, schema_name
                        )
                        os.makedirs(schema_output_dir, exist_ok=True)

                        try:
                            # Generate models with a dedicated engine for this catalog.schema
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
                            click.echo(
                                f"  Output directory: {schema_output_dir}"
                            )
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
                            catalog_success = False

                    if catalog_success:
                        successful_catalogs.append(catalog_name)
                    else:
                        failed_catalogs.append(catalog_name)

                except Exception as e:
                    click.echo(
                        style(
                            f"Error fetching schemas for catalog '{catalog_name}': {str(e)}",
                            fg="red",
                        )
                    )
                    if verbose:
                        import traceback

                        click.echo(traceback.format_exc())
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
            # Add a separator for clarity between catalogs
            click.echo(style("-" * 50, fg="blue"))

        # Show summary of processing
        click.echo(
            style(
                "\n===== Interactive Model Generation Summary =====",
                fg="blue",
                bold=True,
            )
        )
        click.echo(
            style(
                f"Total catalogs processed: {len(selected_catalogs)}",
                fg="blue",
            )
        )
        click.echo(
            style(
                f"Successfully processed: {len(successful_catalogs)}",
                fg="green",
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
                "\n✨ Interactive model generation complete!",
                fg="green",
                bold=True,
            )
        )
        click.echo(f"You can find your models in: {base_output_directory}")

    except Exception as e:
        click.echo(style(f"Error: {str(e)}", fg="red"))
        if verbose:
            import traceback

            click.echo(traceback.format_exc())
    finally:
        # Close the Spark session if it was created
        try:
            if "spark" in locals():
                spark.stop()
        except:
            pass
