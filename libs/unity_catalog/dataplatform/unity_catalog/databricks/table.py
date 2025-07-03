from functools import cached_property
from typing import Any, TypeVar

from delta.tables import DeltaTable
from fastcore.all import store_attr
from fastcore.basics import GetAttr
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from .deltalake import DeltaDeleteMode, DeltaMergeConfig, DeltaWriteMode
from .spark import Spark
from .table_metadata import (
    DeltaMetadataProvider,
    TablePropertyType,
)

T = TypeVar("T")


class Table(GetAttr):
    """A wrapper class for Databricks Delta Table operations.

    This class provides an interface for interacting with Delta tables in Databricks,
    with support for reading, writing, merging, and other Delta Lake operations.
    It also provides ORM functionality when a SQLAlchemy model is provided.

    Args:
        table: Either a fully qualified table name (catalog.schema.table) or a SQLAlchemy model class

    Examples:
        ```python
        # Using table name
        table = Table("catalog.schema.my_table")

        ```
    """

    def __init__(self, name: str):
        store_attr()
        self.spark: SparkSession = (  # type: ignore[reportUninitializedInstanceVariable]
            Spark().spark_session
        )

    def __repr__(self) -> str:
        return f"Table(name={self.name})"

    @cached_property
    def metadata(self) -> DeltaMetadataProvider:
        return DeltaMetadataProvider(self.spark, self.name)

    @cached_property
    def delta_table(self) -> DeltaTable:
        """Get the DeltaTable instance."""
        return DeltaTable.forName(self.spark, self.name)

    @property
    def primary_keys(self) -> list[str]:
        """Get primary key columns from ORM model if available, otherwise from metadata."""
        return self.metadata.primary_keys

    def set_property(
        self, key: str, value: Any, property_type: TablePropertyType | None = None
    ) -> None:
        """Set a table property."""
        self.metadata.set_property(key, value, property_type)

    def unset_property(self, key: str) -> None:
        """Unset a table property."""
        self.metadata.unset_property(key)

    def read(self) -> DataFrame:
        """Get a DataFrame representation of this Delta table."""
        return self.delta_table.toDF()

    def merge(
        self,
        source: DataFrame,
        condition: str | Column,
        when_matched_update: dict[str, str | Column] | None = None,
        when_not_matched_insert: dict[str, str | Column] | None = None,
    ) -> None:
        """Merge source DataFrame into the Delta table.

        Args:
            source: Source DataFrame to merge
            condition: Merge condition (e.g. "target.id = source.id")
            when_matched_update: Dict of column updates for matched rows
            when_not_matched_insert: Dict of column values for new rows
        """
        merge_builder = self.delta_table.alias("target").merge(
            source=source.alias("source"), condition=condition
        )

        if when_matched_update:
            merge_builder = merge_builder.whenMatchedUpdate(set=when_matched_update)

        if when_not_matched_insert:
            merge_builder = merge_builder.whenNotMatchedInsert(
                values=when_not_matched_insert
            )

        merge_builder.execute()

    def merge_changes(
        self,
        df: DataFrame,
        config: DeltaMergeConfig,
        options: dict[str, str] | None = None,
    ) -> None:
        """
        Enhanced merge operation with CDC support and various merge strategies.

        Args:
            df: Source DataFrame
            config: Merge configuration
            options: Additional Delta options
        """
        options = options or {"mergeSchema": "true"}

        join_condition = self._build_join_condition(config.join_fields)
        update_condition = self._build_update_condition(
            df, config.join_fields, config.columns_to_ignore, config.ignore_null_updates
        )

        merge_builder = self.delta_table.alias("target").merge(
            source=df.alias("source"), condition=join_condition
        )

        if config.custom_set:
            merge_builder = merge_builder.whenMatchedUpdate(
                condition=config.update_condition, set=config.custom_set
            )
        else:
            merge_builder = merge_builder.whenMatchedUpdateAll(
                condition=update_condition
            )

        if config.delete_mode == DeltaDeleteMode.HARD:
            merge_builder = merge_builder.whenNotMatchedBySourceDelete()
        elif config.delete_mode == DeltaDeleteMode.SOFT:
            merge_builder = merge_builder.whenMatchedUpdate(
                condition="source._change_type = 'delete'",
                set={"is_deleted": "true", "deleted_at": "current_timestamp()"},
            )

        merge_builder = merge_builder.whenNotMatchedInsertAll()

        merge_builder.execute()

    def write(
        self,
        df: DataFrame,
        mode: DeltaWriteMode,
        partition_by: list[str] | None = None,
        options: dict[str, str] | None = None,
    ) -> None:
        """
        Write DataFrame to Delta table with advanced options.

        Args:
            df: Source DataFrame
            mode: Write mode (append/overwrite/merge)
            partition_by: Columns to partition by
            options: Additional Delta options
        """
        options = options or {"mergeSchema": "true", "overwriteSchema": "false"}

        writer = df.write.format("delta").options(**options)

        if partition_by:
            writer = writer.partitionBy(partition_by)

        writer.mode(mode.value).saveAsTable(self.name.lower())

    def update(
        self,
        condition: Column | str | None = None,
        set_expr: dict[str, str | Column] | None = None,
    ) -> None:
        """Update rows in the table that match the condition.

        Args:
            condition: Optional condition for rows to update. Can be a Column object or string expression.
            set_expr: Dict mapping column names to update expressions
        """
        set_dict = set_expr or {}
        if condition is None:
            self.delta_table.update(set=set_dict)
        else:
            self.delta_table.update(condition=condition, set=set_dict)

    def delete(self, condition: Column | str | None = None) -> None:
        """Delete rows from the table that match the condition.

        Args:
            condition: Optional condition for rows to delete
        """
        if condition is None:
            self.delta_table.delete()
        else:
            self.delta_table.delete(condition)

    def get_history(self) -> DataFrame:
        """Get the table's change history."""
        return self.delta_table.history()

    def get_details(self) -> DataFrame:
        """Get detailed metadata about the table."""
        return self.delta_table.detail()

    def generate_manifest(self, mode: str = "symlink_format_manifest") -> None:
        """Generate a manifest for the table."""
        self.delta_table.generate(mode)

    def vacuum(self, retention_hours: float | None = None) -> DataFrame:
        """Clean up files no longer needed by the table.

        Args:
            retention_hours: Optional retention period in hours
        """
        return self.delta_table.vacuum(retention_hours)

    def optimize(self):
        """Start building an OPTIMIZE command for this table.

        Returns:
            A builder object to specify Z-ORDER or COMPACT options
        """
        return self.delta_table.optimize()

    def get_cdc_version(self) -> int:
        """Get the latest CDC version for the table."""
        history_df = self.delta_table.history()
        transaction_df = history_df.filter(
            ~F.col("operation").isin([
                "OPTIMIZE",
                "VACUUM START",
                "VACUUM END",
                "CHANGE COLUMN",
            ])
        )

        if transaction_df.isEmpty():
            transaction_df = history_df

        result = transaction_df.orderBy(F.col("version").desc()).first()
        if result is None:
            return 0  # Return 0 if no version history exists
        return result["version"]

    def _build_join_condition(self, join_fields: list[str]) -> str:
        """Build the join condition for merge operations."""
        return " AND ".join([f"target.{x} = source.{x}" for x in join_fields])

    def _build_update_condition(
        self,
        df: DataFrame,
        join_fields: list[str],
        columns_to_ignore: list[str] | None = None,
        ignore_null_updates: bool = False,
    ) -> str:
        """Build the update condition for merge operations."""
        columns_to_ignore_set = set(columns_to_ignore or [])
        columns_to_ignore_set.update(join_fields)

        update_columns = set(df.columns) - columns_to_ignore_set

        conditions = []
        for column in update_columns:
            if ignore_null_updates:
                conditions.append(
                    f"""(source.`{column}` IS NOT NULL AND target.`{column}` != source.`{column}`)"""
                )
            else:
                conditions.append(
                    f"""(source.`{column}` != target.`{column}` OR 
                    (source.`{column}` IS NULL AND target.`{column}` IS NOT NULL) OR 
                    (source.`{column}` IS NOT NULL AND target.`{column}` IS NULL))"""
                )

        return " OR ".join(conditions)

    @classmethod
    def convert_to_delta(
        cls,
        spark: SparkSession,
        table_identifier: str,
        partition_schema: str | StructType | None = None,
    ) -> "Table":
        """Convert a parquet table to Delta format.

        Args:
            spark: Active SparkSession
            table_identifier: Parquet table identifier ("parquet.`path`")
            partition_schema: Optional partition schema
        """
        delta_table = DeltaTable.convertToDelta(
            spark, table_identifier, partition_schema
        )
        return cls(table_identifier)

    def upgrade_table_protocol(
        self,
        reader_version: int,
        writer_version: int,
    ) -> None:
        """Upgrade the protocol version of the table.

        Args:
            reader_version: Optional minimum reader version of the table
            writer_version: Optional minimum writer version of the table

        Example:
            ```python
            # Upgrade to protocol version 2
            table.upgrade_table_protocol(reader_version=2, writer_version=5)
            ```
        """
        self.delta_table.upgradeTableProtocol(reader_version, writer_version)

    def restore(
        self, version_as_of: int | None = None, timestamp_as_of: str | None = None
    ) -> None:
        """Restore the table to an earlier version.

        Args:
            version_as_of: Optional version number to restore to
            timestamp_as_of: Optional timestamp to restore to (e.g. "2023-01-01 00:00:00")

        Note: Only one of version_as_of or timestamp_as_of should be specified.

        Example:
            ```python
            # Restore to version 5
            table.restore(version_as_of=5)

            # Restore to specific timestamp
            table.restore(timestamp_as_of="2023-01-01 00:00:00")
            ```
        """
        if version_as_of is not None and timestamp_as_of is not None:
            raise ValueError(
                "Only one of version_as_of or timestamp_as_of should be specified"
            )

        if version_as_of is not None:
            self.delta_table.restore(version=version_as_of)
        elif timestamp_as_of is not None:
            self.delta_table.restore(timestamp=timestamp_as_of)
        else:
            raise ValueError(
                "Either version_as_of or timestamp_as_of must be specified"
            )

    def merge_with_keys(
        self,
        source_df: DataFrame,
        update_columns: dict[str, str | Column] | None = None,
        enable_hard_delete: bool = False,
        merge_options: dict[str, str] | None = None,
    ) -> None:
        """
        Merge source DataFrame into table using primary keys with optional hard deletes.

        Args:
            source_df: Source DataFrame to merge
            update_columns: Optional dict of column updates for matched rows. If None, updates all columns
            enable_hard_delete: Whether to delete records not in source
            merge_options: Additional merge options like 'mergeSchema'
        """
        merge_condition = " AND ".join([
            f"target.{pk} = source.{pk}" for pk in self.primary_keys
        ])

        merge_builder = self.delta_table.alias("target").merge(
            source=source_df.alias("source"), condition=merge_condition
        )

        if update_columns:
            merge_builder = merge_builder.whenMatchedUpdate(set=update_columns)
        else:
            merge_builder = merge_builder.whenMatchedUpdateAll()

        if enable_hard_delete:
            merge_builder = merge_builder.whenNotMatchedBySourceDelete()

        merge_builder = merge_builder.whenNotMatchedInsertAll()

        if merge_options:
            for k, v in merge_options.items():
                self.spark.conf.set(k, v)

        merge_builder.execute()
