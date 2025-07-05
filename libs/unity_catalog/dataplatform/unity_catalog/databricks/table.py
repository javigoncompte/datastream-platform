from functools import wraps
from typing import Any, Callable, TypeVar

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType

from .deltalake import DeltaDeleteMode, DeltaMergeConfig, DeltaWriteMode
from .spark import Spark
from .table_metadata import (
    DeltaMetadataProvider,
    TablePropertyType,
)

T = TypeVar("T")


def with_delta_table(func: Callable[..., T]):  # pyright: ignore[reportUnknownParameterType]
    """Decorator that provides DeltaTable instance to the decorated function.

    Args:
        func: Function that takes DeltaTable as first argument after table_name

    Returns:
        Decorated function that automatically creates DeltaTable instance
    """

    @wraps(func)
    def wrapper(table_name: str, *args, **kwargs):
        spark: SparkSession = Spark().spark_session
        delta_table = DeltaTable.forName(spark, table_name)  # pyright: ignore[reportCallIssue]
        wrapper.table_name = table_name
        wrapper.delta_table = delta_table
        return func(delta_table, *args, **kwargs)

    return wrapper  # pyright: ignore[reportUnknownVariableType]


def get_metadata_provider(table_name: str) -> DeltaMetadataProvider:
    """Get DeltaMetadataProvider for a table."""
    spark: SparkSession = Spark().spark_session
    return DeltaMetadataProvider(spark, table_name)


def get_primary_keys(table_name: str) -> list[str]:
    """Get primary key columns from table metadata."""
    metadata = get_metadata_provider(table_name)
    return metadata.primary_keys


def set_table_property(
    table_name: str,
    key: str,
    value: Any,
    property_type: TablePropertyType | None = None,
) -> None:
    """Set a table property."""
    metadata = get_metadata_provider(table_name)
    metadata.set_property(key, value, property_type)


def unset_table_property(table_name: str, key: str) -> None:
    """Unset a table property."""
    metadata = get_metadata_provider(table_name)
    metadata.unset_property(key)


@with_delta_table
def read(
    delta_table: DeltaTable, to_polars: bool = False, to_daft: bool = False
) -> DataFrame:
    """Get a DataFrame representation of this Delta table.

    Args:
        table_name: Fully qualified table name (catalog.schema.table)

    Returns:
        DataFrame representation of the table
    """

    df = delta_table.toDF()
    if to_polars:
        import polars as pl

        arrow_table = df.toArrow()
        return pl.from_arrow(arrow_table)

    if to_daft:
        from daft.pyspark import SparkSession

        url_remote = Spark().spark_session.conf.get("remote")
        spark = SparkSession.builder.remote(url_remote).getOrCreate()
        df = spark.table(delta_table.name)
        return df
    else:
        return df


@with_delta_table
def merge(
    delta_table: DeltaTable,
    source: DataFrame,
    condition: str | Column | None = None,
    config: DeltaMergeConfig | None = None,
    with_schema_evolution: bool = True,
    when_matched_update: dict[str, str | Column] | None = None,
    when_not_matched_insert: dict[str, str | Column] | None = None,
) -> None:
    """Merge source DataFrame into the Delta table.

    Args:
        delta_table: DeltaTable instance
        source: Source DataFrame to merge
        condition: Merge condition (e.g. "target.id = source.id") - optional if using primary keys
        config: DeltaMergeConfig for advanced merge operations
        with_schema_evolution: Whether to enable schema evolution
        when_matched_update: Dict of column updates for matched rows
        when_not_matched_insert: Dict of column values for new rows
    """
    primary_keys = get_primary_keys(delta_table.name)

    # Strategy 1: Use DeltaMergeConfig if provided (for CDC and advanced scenarios)
    if config is not None:
        return _merge_taable_with_delta_merge_config(
            delta_table,
            source,
            config,
            with_schema_evolution=with_schema_evolution,
        )

    # Strategy 2: Use primary keys if available and no explicit condition/custom logic
    if (
        primary_keys
        and condition is None
        and when_matched_update is None
        and when_not_matched_insert is None
    ):
        return _merge_table_with_keys(
            delta_table,
            source,
            with_schema_evolution=with_schema_evolution,
            update_columns=when_matched_update,
            enable_hard_delete=False,
        )

    # Strategy 3: Use merge_table_changes for complex scenarios with primary keys
    if (
        primary_keys
        and condition is None
        and (when_matched_update is not None or when_not_matched_insert is not None)
    ):
        # Auto-create DeltaMergeConfig using primary keys but with custom update/insert logic
        auto_config = DeltaMergeConfig(
            join_fields=primary_keys,
            delete_mode=DeltaDeleteMode.NONE,
            columns_to_ignore=[],
            ignore_null_updates=False,
            custom_set=when_matched_update,
            update_condition=None,
        )
        return _merge_taable_with_delta_merge_config(
            delta_table,
            source,
            auto_config,
            with_schema_evolution=with_schema_evolution,
        )

    # Strategy 4: Basic merge with explicit condition
    if condition is None:
        raise ValueError(
            "Either provide a condition, ensure table has primary keys, or use DeltaMergeConfig"
        )

    merge_builder = delta_table.alias("target").merge(
        source=source.alias("source"), condition=condition
    )

    if with_schema_evolution:
        merge_builder = merge_builder.withSchemaEvolution()

    if when_matched_update:
        merge_builder = merge_builder.whenMatchedUpdate(set=when_matched_update)
    else:
        merge_builder = merge_builder.whenMatchedUpdateAll()

    if when_not_matched_insert:
        merge_builder = merge_builder.whenNotMatchedInsert(
            values=when_not_matched_insert
        )
    else:
        merge_builder = merge_builder.whenNotMatchedInsertAll()

    merge_builder.execute()


def _merge_taable_with_delta_merge_config(
    delta_table: DeltaTable,
    df: DataFrame,
    config: DeltaMergeConfig,
    with_schema_evolution: bool = True,
    options: dict[str, str] | None = None,
) -> None:
    """
    Enhanced merge operation with CDC support and various merge strategies.

    Args:
        delta_table: DeltaTable instance
        df: Source DataFrame
        config: Merge configuration
        with_schema_evolution: Whether to enable schema evolution
        options: Additional Delta options
    """
    options = options or {"mergeSchema": "true"}

    join_condition = _build_join_condition(config.join_fields)
    update_condition = _build_update_condition(
        df, config.join_fields, config.columns_to_ignore, config.ignore_null_updates
    )

    merge_builder = delta_table.alias("target").merge(
        source=df.alias("source"), condition=join_condition
    )

    if with_schema_evolution:
        merge_builder = merge_builder.withSchemaEvolution()

    if config.custom_set:
        merge_builder = merge_builder.whenMatchedUpdate(
            condition=config.update_condition, set=config.custom_set
        )
    else:
        merge_builder = merge_builder.whenMatchedUpdateAll(condition=update_condition)

    if config.delete_mode == DeltaDeleteMode.HARD:
        merge_builder = merge_builder.whenNotMatchedBySourceDelete()
    elif config.delete_mode == DeltaDeleteMode.SOFT:
        merge_builder = merge_builder.whenMatchedUpdate(
            condition="source._change_type = 'delete'",
            set={"is_deleted": "true", "deleted_at": "current_timestamp()"},
        )

    merge_builder = merge_builder.whenNotMatchedInsertAll()

    merge_builder.execute()


@with_delta_table
def write(
    delta_table: DeltaTable,
    df: DataFrame,
    mode: DeltaWriteMode,
    partition_by: list[str] | None = None,
    options: dict[str, str] | None = None,
) -> None:
    """
    Write DataFrame to Delta table with advanced options.

    Args:
        table_name: Fully qualified table name (catalog.schema.table)
        df: Source DataFrame
        mode: Write mode (append/overwrite/merge)
        partition_by: Columns to partition by
        options: Additional Delta options
    """
    table_name = delta_table.name
    options = options or {"mergeSchema": "true", "overwriteSchema": "false"}

    writer = df.write.format("delta").options(**options)

    if partition_by:
        writer = writer.partitionBy(partition_by)

    writer.mode(mode.value).saveAsTable(table_name)


@with_delta_table
def update(
    delta_table: DeltaTable,
    condition: Column | str | None = None,
    set_expr: dict[str, str | Column] | None = None,
) -> None:
    """Update rows in the table that match the condition.

    Args:
        table_name: Fully qualified table name (catalog.schema.table)
        condition: Optional condition for rows to update. Can be a Column object or string expression.
        set_expr: Dict mapping column names to update expressions
    """
    set_dict = set_expr or {}
    if condition is None:
        delta_table.update(set=set_dict)
    else:
        delta_table.update(condition=condition, set=set_dict)


@with_delta_table
def delete(
    delta_table: DeltaTable,
    condition: Column | str | None = None,
) -> None:
    """Delete rows from the table that match the condition.

    Args:
        table_name: Fully qualified table name (catalog.schema.table)
        condition: Optional condition for rows to delete
    """
    if condition is None:
        delta_table.delete()
    else:
        delta_table.delete(condition)


@with_delta_table
def get_table_history(delta_table: DeltaTable, spark: SparkSession) -> DataFrame:
    """Get the table's change history.

    Args:
        table_name: Fully qualified table name (catalog.schema.table)

    Returns:
        DataFrame with table history
    """
    return delta_table.history()


@with_delta_table
def get_table_details(delta_table: DeltaTable, spark: SparkSession) -> DataFrame:
    """Get detailed metadata about the table.

    Args:
        table_name: Fully qualified table name (catalog.schema.table)

    Returns:
        DataFrame with table details
    """
    return delta_table.detail()


@with_delta_table
def generate_table_manifest(
    delta_table: DeltaTable,
    mode: str = "symlink_format_manifest",
) -> None:
    """Generate a manifest for the table.

    Args:
        table_name: Fully qualified table name (catalog.schema.table)
        mode: Manifest generation mode
    """
    delta_table.generate(mode)


@with_delta_table
def vacuum_table(
    delta_table: DeltaTable,
    retention_hours: float | None = None,
) -> DataFrame:
    """Clean up files no longer needed by the table.

    Args:
        table_name: Fully qualified table name (catalog.schema.table)
        retention_hours: Optional retention period in hours

    Returns:
        DataFrame with vacuum results
    """
    return delta_table.vacuum(retention_hours)


@with_delta_table
def optimize_table(
    delta_table: DeltaTable,
) -> DeltaTable.OptimizeBuilder:
    """Start building an OPTIMIZE command for this table.

    Args:
        table_name: Fully qualified table name (catalog.schema.table)

    Returns:
        A builder object to specify Z-ORDER or COMPACT options
    """
    return delta_table.optimize()


@with_delta_table
def get_last_cdc_version(
    delta_table: DeltaTable,
) -> int:
    """Get the latest CDC version for the table.

    Args:
        table_name: Fully qualified table name (catalog.schema.table)

    Returns:
        Latest CDC version number
    """

    history_df = delta_table.history()
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
    version = result.select("version").collect()[0][0]
    if version is None:
        return 0
    return version


def _build_join_condition(join_fields: list[str]) -> str:
    """Build the join condition for merge operations."""
    return " AND ".join([f"target.{x} = source.{x}" for x in join_fields])


def get_cdc_df(
    spark: SparkSession,
    since_date: str | None = None,
    since_version: int | None = None,
    ending_version: int | None = None,
    ending_date: str | None = None,
) -> DataFrame | None:
    """Get a CDC stream reader configured with the appropriate starting point.

    Args:
        spark: SparkSession instance
        since_date: Optional date string to start reading from
        since_version: Optional version number to start reading from

    Returns:
        Configured DataFrameReader for CDC streaming
    """
    startpoint = True if since_date or since_version else False
    endpoint = True if ending_date or ending_version else False
    reader = spark.read.option("readChangeFeed", "true")

    if startpoint:
        if since_date is not None:
            reader = reader.option("startingVersion", since_date)
        elif since_version is not None:
            reader = reader.option("startingVersion", since_version)
    if endpoint:
        if ending_date is not None:
            reader = reader.option("endingVersion", ending_date)
        elif ending_version is not None:
            reader = reader.option("endingVersion", ending_version)

    return reader.load()


def _build_update_condition(
    df: DataFrame,
    join_fields: list[str],
    columns_to_ignore: list[str] | None = None,
    ignore_null_updates: bool = False,
) -> str:
    """Build the update condition for merge operations."""
    columns_to_ignore_set: set[str] = set(columns_to_ignore or [])
    columns_to_ignore_set.update(join_fields)

    update_columns: set[str] = set(df.columns) - columns_to_ignore_set

    conditions: list[str] = []
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


def convert_to_delta_table(
    table_identifier: str,
    partition_schema: str | StructType | None = None,
) -> None:
    """Convert a parquet table to Delta format.

    Args:
        table_identifier: Parquet table identifier ("parquet.`path`")
        partition_schema: Optional partition schema
    """
    spark: SparkSession = Spark().spark_session
    DeltaTable.convertToDelta(spark, table_identifier, partition_schema)


@with_delta_table
def upgrade_table_protocol(
    delta_table: DeltaTable,
    reader_version: int,
    writer_version: int,
) -> None:
    """Upgrade the protocol version of the table.

    Args:
        table_name: Fully qualified table name (catalog.schema.table)
        reader_version: Minimum reader version of the table
        writer_version: Minimum writer version of the table

    Example:
        ```python
        # Upgrade to protocol version 2
        upgrade_table_protocol(
            "catalog.schema.table", reader_version=2, writer_version=5
        )
        ```
    """
    delta_table.upgradeTableProtocol(reader_version, writer_version)


@with_delta_table
def restore(
    delta_table: DeltaTable,
    version_as_of: int | None = None,
    timestamp_as_of: str | None = None,
) -> None:
    """Restore the table to an earlier version.

    Args:
        table_name: Fully qualified table name (catalog.schema.table)
        version_as_of: Optional version number to restore to
        timestamp_as_of: Optional timestamp to restore to (e.g. "2023-01-01 00:00:00")

    Note: Only one of version_as_of or timestamp_as_of should be specified.

    Example:
        ```python
        # Restore to version 5
        restore_table("catalog.schema.table", version_as_of=5)

        # Restore to specific timestamp
        restore_table("catalog.schema.table", timestamp_as_of="2023-01-01 00:00:00")
        ```
    """
    if version_as_of is not None and timestamp_as_of is not None:
        raise ValueError(
            "Only one of version_as_of or timestamp_as_of should be specified"
        )

    if version_as_of is not None:
        delta_table.restore(version=version_as_of)
    elif timestamp_as_of is not None:
        delta_table.restore(timestamp=timestamp_as_of)
    else:
        raise ValueError("Either version_as_of or timestamp_as_of must be specified")


def _merge_table_with_keys(
    delta_table: DeltaTable,
    source_df: DataFrame,
    with_schema_evolution: bool = True,
    update_columns: dict[str, str | Column] | None = None,
    enable_hard_delete: bool = False,
    merge_options: dict[str, str] | None = None,
) -> None:
    """
    Merge source DataFrame into table using primary keys with optional hard deletes.

    Args:
        delta_table: DeltaTable instance
        source_df: Source DataFrame to merge
        with_schema_evolution: Whether to enable schema evolution
        update_columns: Optional dict of column updates for matched rows. If None, updates all columns
        enable_hard_delete: Whether to delete records not in source
        merge_options: Additional merge options like 'mergeSchema'
    """
    spark = Spark().spark_session
    table_name = delta_table.name
    primary_keys = get_primary_keys(table_name)

    if not primary_keys:
        raise ValueError(f"No primary keys found for table {table_name}")

    merge_condition = " AND ".join([
        f"target.{pk} = source.{pk}" for pk in primary_keys
    ])

    merge_builder = delta_table.alias("target").merge(
        source=source_df.alias("source"), condition=merge_condition
    )

    if with_schema_evolution:
        merge_builder = merge_builder.withSchemaEvolution()

    if update_columns:
        merge_builder = merge_builder.whenMatchedUpdate(set=update_columns)
    else:
        merge_builder = merge_builder.whenMatchedUpdateAll()

    if enable_hard_delete:
        merge_builder = merge_builder.whenNotMatchedBySourceDelete()

    merge_builder = merge_builder.whenNotMatchedInsertAll()

    if merge_options:
        for k, v in merge_options.items():
            spark.conf.set(k, v)

    merge_builder.execute()
