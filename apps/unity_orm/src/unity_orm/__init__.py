"""Unity ORM - ORM and migration tools for Databricks Unity Catalog."""

__version__ = "0.1.0"

from unity_orm.engine import create_engine
from unity_orm.generators import (
    ModelGenerator,
    TableInfo,
    generate_models_for_catalog_schema,
)
from unity_orm.migrations import (
    MigrationManager,
    initialize_migrations_for_catalog_schema,
)
from unity_orm.model_base import ManagedTable

__all__ = [
    # Engine
    "create_engine",
    # Model base
    "ManagedTable",
    # Generators
    "ModelGenerator",
    "TableInfo",
    "generate_models_for_catalog_schema",
    # Migrations
    "MigrationManager",
    "initialize_migrations_for_catalog_schema",
]


def hello() -> str:
    return "Hello from unity-orm!"
