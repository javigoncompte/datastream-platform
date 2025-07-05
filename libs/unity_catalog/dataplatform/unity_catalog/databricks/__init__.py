# pyright: reportUnknownVariableType=false
from .table import (
    delete,
    generate_table_manifest,
    get_cdc_df,
    get_last_cdc_version,
    get_metadata_provider,
    get_primary_keys,
    merge,
    read,
    restore,
    set_table_property,
    unset_table_property,
    update,
    write,
)

__all__ = [
    "merge",
    "get_metadata_provider",
    "get_primary_keys",
    "set_table_property",
    "unset_table_property",
    "delete",
    "read",
    "write",
    "update",
    "generate_table_manifest",
    "get_last_cdc_version",
    "get_cdc_df",
    "restore",
]
