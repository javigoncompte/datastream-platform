from dataclasses import dataclass
from enum import Enum

from pyspark.sql import Column


class DeltaWriteMode(Enum):
    """Enum for different write modes in Delta Lake"""

    APPEND = "append"
    OVERWRITE = "overwrite"
    MERGE = "merge"
    MERGE_CDC = "merge_cdc"
    UPSERT = "upsert"


class DeltaDeleteMode(Enum):
    """Enum for different delete modes in Delta Lake"""

    HARD = "hard"
    SOFT = "soft"
    NONE = "none"


@dataclass
class DeltaMergeConfig:
    """Configuration for Delta merge operations"""

    join_fields: list[str]
    update_condition: str | None = None
    delete_mode: DeltaDeleteMode = DeltaDeleteMode.NONE
    custom_set: dict[str, str | Column] | None = None
    ignore_null_updates: bool = False
    move_last_modified: bool = False
    columns_to_ignore: list[str] | None = None
