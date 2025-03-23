"""Project initialization commands for Unity ORM CLI."""

import os

import click


def register_commands(cli):
    """Register project initialization commands with the CLI."""
    cli.add_command(init_project)
    cli.add_command(init_migrations)


@click.command()
@click.option(
    "--project-name",
    required=True,
    help="Name of the project to create.",
)
@click.option(
    "--project-dir",
    default="./",
    help="Directory to create the project in. Default: ./",
)
def init_project(
    project_name: str,
    project_dir: str = "./",
):
    """Initialize a new Unity ORM project.

    This command creates a new Unity ORM project with the given name.
    The project contains all the necessary files to get started with Unity ORM.

    Example:
        unity-orm init-project --project-name my_project
    """
    project_path = os.path.join(project_dir, project_name)
    project_name_underscores = project_name.replace("-", "_")

    # Check if the project directory already exists
    if os.path.exists(project_path):
        click.echo(
            click.style(
                f"Error: Project directory '{project_path}' already exists",
                fg="red",
            )
        )
        return

    # Create project directory
    os.makedirs(project_path)

    # Create subdirectories
    os.makedirs(os.path.join(project_path, "src"))
    os.makedirs(os.path.join(project_path, "tests"))
    os.makedirs(os.path.join(project_path, "docs"))
    os.makedirs(os.path.join(project_path, "migrations"))

    # Create __init__.py files
    open(os.path.join(project_path, "src", "__init__.py"), "w").close()
    open(os.path.join(project_path, "tests", "__init__.py"), "w").close()

    # Create setup.py
    with open(os.path.join(project_path, "setup.py"), "w") as f:
        f.write(f'''from setuptools import setup, find_packages

setup(
    name="{project_name}",
    version="0.1.0",
    packages=find_packages("src"),
    package_dir={{"": "src"}},
    install_requires=[
        "unity-orm",
        "sqlalchemy>=2.0.0",
        "alembic",
        "click",
    ],
    entry_points={{
        "console_scripts": [
            "{project_name_underscores}={project_name_underscores}.cli:main",
        ],
    }},
)
''')

    # Create pyproject.toml
    with open(os.path.join(project_path, "pyproject.toml"), "w") as f:
        f.write("""[build-system]
requires = ["setuptools>=42", "wheel"]
build-backend = "setuptools.build_meta"
""")

    # Create project module directory
    project_module_dir = os.path.join(
        project_path, "src", project_name_underscores
    )
    os.makedirs(project_module_dir)

    # Create __init__.py in project module
    with open(os.path.join(project_module_dir, "__init__.py"), "w") as f:
        f.write(f'''"""
{project_name} package.

This project is built using Unity ORM for connecting to Databricks.
"""

__version__ = "0.1.0"
''')

    # Create models directory
    models_dir = os.path.join(project_module_dir, "models")
    os.makedirs(models_dir)
    open(os.path.join(models_dir, "__init__.py"), "w").close()

    # Create CLI module
    cli_dir = os.path.join(project_module_dir, "cli")
    os.makedirs(cli_dir)

    # Create __init__.py in CLI module
    with open(os.path.join(cli_dir, "__init__.py"), "w") as f:
        f.write('''"""CLI for the project."""

import click

@click.group()
def cli():
    """Command-line interface."""
    pass

@cli.command()
def hello():
    """Say hello."""
    click.echo("Hello from your new project!")

def main():
    """Run the CLI."""
    cli()

if __name__ == "__main__":
    main()
''')

    # Create README.md
    with open(os.path.join(project_path, "README.md"), "w") as f:
        f.write(f"""# {project_name}

A project built with Unity ORM for connecting to Databricks.

## Installation

```
pip install -e .
```

## Usage

```
{project_name_underscores} --help
```
""")

    # Create .gitignore
    with open(os.path.join(project_path, ".gitignore"), "w") as f:
        f.write("""# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
env/
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
*.egg-info/
.installed.cfg
*.egg

# Virtual environments
venv/
ENV/
env/

# IDE
.idea/
.vscode/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db
""")

    click.echo(
        click.style(
            f"✨ Project '{project_name}' initialized successfully!",
            fg="green",
        )
    )
    click.echo(f"Project directory: {project_path}")
    click.echo("\nNext steps:")
    click.echo(f"  1. cd {project_path}")
    click.echo("  2. pip install -e .")
    click.echo(f"  3. {project_name_underscores} hello")
    click.echo(f"  4. unity-orm init-migrations --project-dir {project_path}")


@click.command()
@click.option(
    "--project-dir",
    required=True,
    help="Directory of the project to initialize migrations for.",
)
def init_migrations(
    project_dir: str,
):
    """Initialize Alembic migrations for an existing Unity ORM project.

    This command initializes Alembic migrations for an existing Unity ORM project.
    It creates the necessary files and directories for managing database migrations.

    Example:
        unity-orm init-migrations --project-dir ./my_project
    """
    # Check if the project directory exists
    if not os.path.exists(project_dir):
        click.echo(
            click.style(
                f"Error: Project directory '{project_dir}' does not exist",
                fg="red",
            )
        )
        return

    # Check if the migrations directory already exists
    migrations_dir = os.path.join(project_dir, "migrations")
    if not os.path.exists(migrations_dir):
        os.makedirs(migrations_dir)

    # Create alembic.ini
    with open(os.path.join(project_dir, "alembic.ini"), "w") as f:
        f.write("""# A generic, single database configuration.

[alembic]
# path to migration scripts
script_location = migrations

# template used to generate migration files
# file_template = %%(rev)s_%%(slug)s

# timezone to use when rendering the date
# within the migration file as well as the filename.
# string value is passed to dateutil.tz.gettz()
# leave blank for localtime
# timezone =

# max length of characters to apply to the
# "slug" field
# truncate_slug_length = 40

# set to 'true' to run the environment during
# the 'revision' command, regardless of autogenerate
# revision_environment = false

# set to 'true' to allow .pyc and .pyo files without
# a source .py file to be detected as revisions in the
# versions/ directory
# sourceless = false

# version location specification; this defaults
# to migrations/versions.  When using multiple version
# directories, initial revisions must be specified with --version-path
# version_locations = %(here)s/bar %(here)s/bat migrations/versions

# the output encoding used when revision files
# are written from script.py.mako
# output_encoding = utf-8

sqlalchemy.url = driver://user:pass@localhost/dbname


[post_write_hooks]
# post_write_hooks defines scripts or Python functions that are run
# on newly generated revision scripts.  See the documentation for further
# detail and examples

# format using "black" - use the console_scripts runner, against the "black" entrypoint
# hooks=black
# black.type=console_scripts
# black.entrypoint=black
# black.options=-l 79

# Logging configuration
[loggers]
keys = root,sqlalchemy,alembic

[handlers]
keys = console

[formatters]
keys = generic

[logger_root]
level = WARN
handlers = console
qualname =

[logger_sqlalchemy]
level = WARN
handlers =
qualname = sqlalchemy.engine

[logger_alembic]
level = INFO
handlers =
qualname = alembic

[handler_console]
class = StreamHandler
args = (sys.stderr,)
level = NOTSET
formatter = generic

[formatter_generic]
format = %(levelname)-5.5s [%(name)s] %(message)s
datefmt = %H:%M:%S
""")

    # Create migrations/env.py
    with open(os.path.join(migrations_dir, "env.py"), "w") as f:
        f.write('''from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = None

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
''')

    # Create migrations/script.py.mako
    with open(os.path.join(migrations_dir, "script.py.mako"), "w") as f:
        f.write('''"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

"""
from alembic import op
import sqlalchemy as sa
${imports if imports else ""}

# revision identifiers, used by Alembic.
revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}
branch_labels = ${repr(branch_labels)}
depends_on = ${repr(depends_on)}


def upgrade():
    ${upgrades if upgrades else "pass"}


def downgrade():
    ${downgrades if downgrades else "pass"}
''')

    # Create migrations/versions directory
    versions_dir = os.path.join(migrations_dir, "versions")
    if not os.path.exists(versions_dir):
        os.makedirs(versions_dir)

    # Add README to versions directory
    with open(os.path.join(versions_dir, "README"), "w") as f:
        f.write("""This directory contains Alembic migration scripts.

To create a new migration:
    alembic revision -m "description of migration"

To upgrade to the latest version:
    alembic upgrade head

To downgrade to a previous version:
    alembic downgrade -1
""")

    click.echo(
        click.style(
            "✨ Alembic migrations initialized successfully!", fg="green"
        )
    )
    click.echo(f"Migrations directory: {migrations_dir}")
    click.echo("\nNext steps:")
    click.echo("  1. Edit the sqlalchemy.url in alembic.ini")
    click.echo("  2. Update migrations/env.py to import your models")
    click.echo("  3. Run 'alembic revision --autogenerate -m \"initial\"'")
    click.echo("  4. Run 'alembic upgrade head'")
