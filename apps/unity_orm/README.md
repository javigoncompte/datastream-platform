# Unity ORM

Unity ORM is an ORM toolkit for interacting with Databricks Unity Catalog tables using SQLAlchemy.

## Features

- Generate SQLAlchemy models from existing Databricks tables
- Support for Unity Catalog (catalog.schema.table format)
- Interactive CLI for selecting catalogs and schemas
- Batch processing for all available catalogs
- Project initialization tools

## Installation

```bash
pip install unity-orm
```

Or install in development mode:

```bash
git clone https://github.com/your-organization/unity-orm.git
cd unity-orm
pip install -e .
```

## Prerequisites

- Databricks account with Unity Catalog enabled
- Databricks Connect configured (`databricks configure --profile DEFAULT` or set environment variables)
- Python 3.8+

## CLI Usage

Unity ORM provides a command-line interface for common tasks:

```bash
# Show help
unity-orm --help

# Generate models for a specific catalog.schema
unity-orm generate-single catalog.schema --verbose

# Interactive selection of catalogs and schemas
unity-orm interactive

# Generate models for all available catalogs (automatic filtering)
unity-orm generate-all-catalogs

# Initialize a new project
unity-orm init-project --project-name my-unity-project
```

### Authentication

The CLI supports multiple authentication methods:

1. **Databricks Connect Authentication** (default) - Uses your configured Databricks Connect profile
2. **Explicit Parameters** - Provide access token, server hostname, and HTTP path directly

Parameters can be passed via:
- Command-line options: `--access-token`, `--server-hostname`, `--http-path`
- Environment variables: `DATABRICKS_TOKEN`, `DATABRICKS_SERVER_HOSTNAME`, `DATABRICKS_HTTP_PATH`

## Commands

### Model Generation Commands

#### `generate-single`

Generate models for a specific catalog.schema combination:

```bash
unity-orm generate-single catalog.schema [OPTIONS]
```

Options:
- `--access-token` - Databricks access token (optional, uses Databricks Connect auth if not provided)
- `--server-hostname` - Databricks server hostname (optional)
- `--http-path` - Databricks HTTP path for SQL warehouse (optional)
- `--output-directory` - Directory to store generated models (default: `./src/unity_orm/dataplatform`)
- `--verbose` - Enable verbose output for debugging

#### `generate-models`

Generate models for a catalog with optional schema:

```bash
unity-orm generate-models --catalog CATALOG [--schema SCHEMA] [OPTIONS]
```

Options:
- `--catalog` - Catalog name in Unity Catalog (required)
- `--schema` - Schema name in Unity Catalog (optional, defaults to "default")
- `--access-token` - Databricks access token (optional)
- `--server-hostname` - Databricks server hostname (optional)
- `--http-path` - Databricks HTTP path (optional)
- `--output-directory` - Directory to store generated models (default: `./src/unity_orm/dataplatform`)
- `--verbose` - Enable verbose output for debugging

#### `generate-all-catalogs`

Generate models for all available catalogs based on filtering options:

```bash
unity-orm generate-all-catalogs [OPTIONS]
```

Options:
- `--access-token`, `--server-hostname`, `--http-path` - Authentication options
- `--base-output-directory` - Base directory to store generated models
- `--show-all` - Include environment-specific catalogs (qa_, test_, etc.)
- `--debug` - Include system catalogs (with __ prefix)
- `--no-filter` - Show all catalogs without any filtering
- `--include-all-schemas` - Include all schemas in each catalog (excluding information_schema)
- `--verbose` - Enable verbose output for debugging

### Interactive Commands

#### `interactive`

Interactive CLI to select catalogs and schemas for model generation:

```bash
unity-orm interactive [OPTIONS]
```

Options:
- Auth options: `--access-token`, `--server-hostname`, `--http-path`
- Filtering: `--show-all`, `--debug`, `--no-filter`
- `--select-all` - Select all available catalogs after filtering
- `--include-all-schemas` - Include all schemas in each catalog
- `--base-output-directory` - Base directory to store generated models
- `--verbose` - Enable verbose output for debugging

### Project Commands

#### `init-project`

Initialize a new Unity ORM project:

```bash
unity-orm init-project --project-name PROJECT_NAME [OPTIONS]
```

Options:
- `--project-name` - Name of the project to create (required)
- `--project-dir` - Directory to create the project in (default: `./`)

#### `init-migrations`

Initialize Alembic migrations for an existing Unity ORM project:

```bash
unity-orm init-migrations --project-dir PROJECT_DIR
```

Options:
- `--project-dir` - Directory of the project to initialize migrations for (required)

## Project Structure

The Unity ORM CLI is organized into modules:

```
unity_orm/
├── cli/
│   ├── commands/
│   │   ├── __init__.py          - Command module exports
│   │   ├── model_commands.py    - Model generation commands
│   │   ├── interactive_commands.py - Interactive selection
│   │   └── project_commands.py  - Project initialization
│   ├── __init__.py
│   └── main.py                  - CLI entrypoint
```

## Development

### Adding a New Command

To add a new command:

1. Add the command function to an appropriate module in `cli/commands/`
2. Register the command in the module's `register_commands` function
3. Ensure the module is imported and registered in `cli/commands/__init__.py`

### Running Tests

```bash
pytest
```

## License

[MIT License](LICENSE)

