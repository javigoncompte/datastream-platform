"""Model generator module for Unity ORM (backward compatibility).

This file exists for backward compatibility and re-exports
functionality from the models submodule.
"""

# ruff: noqa

from unity_orm.generators.models.table_info import TableInfo
from unity_orm.generators.models.generator import ModelGenerator
from unity_orm.generators.models.catalog_schema import (
    generate_models_for_catalog_schema,
)

__all__ = [
    "TableInfo",
    "ModelGenerator",
    "generate_models_for_catalog_schema",
]
