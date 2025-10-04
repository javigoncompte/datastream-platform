from typing import List

from delta.tables import DeltaTable
from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame
from dataplatform.core.logger import get_logger
from dataplatform.core.spark_session import SparkSession

spark = SparkSession().get_spark_session()
logger = get_logger()


def create_table(
    table_name: str,
    df: DataFrame,
    partition_cols: List[str] | None = None,
    primary_key_cols: List[str] | None = None,
) -> DeltaTable:
    spark = SparkSession().get_spark_session()
    dt = (
        DeltaTable.createIfNotExists(spark)
        .tableName(table_name)
        .addColumns(df.schema)
        .property("delta.enableChangeDataFeed", "true")
        .property("delta.autoOptimize.optimizeWrite", "true")
        .property("delta.autoOptimize.autoCompact", "true")
        .property("delta.enableTypeWidening", "true")
    )
    if partition_cols:
        dt = dt.partitionedBy(partition_cols)
    dt = dt.execute()

    return dt


def get_tables_last_modified(catalog_name: str, schema_name: str) -> DataFrame:
    """
    Gets all tables from a schema and returns a DataFrame with table names
    and their last modified timestamps.

    Args:
        schema_name: Name of the schema/database to check

    Returns:
        DataFrame with columns 'table_name' and 'last_modified'
    """
    # Get list of all tables in schema
    tables = (
        spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}")
        .select("tableName")
        .collect()
    )

    table_data = []

    for table in tables:
        table_name = table.tableName
        full_table_name = f"{catalog_name}.{schema_name}.{table_name}"

        try:
            # Get last modified timestamp from table history
            history = spark.sql(f"DESCRIBE HISTORY {full_table_name}")
            if not history.isEmpty():
                last_timestamp = history.select(f.max("timestamp")).first()[0]
                table_data.append((table_name, last_timestamp))
            else:
                # If no history, try to get latest timestamp from data
                df = spark.table(full_table_name)
                timestamp_cols = [
                    c
                    for c in df.columns
                    if "timestamp" in c.lower() or "date" in c.lower()
                ]

                if timestamp_cols:
                    # Use the most recent timestamp from any timestamp column
                    max_ts = df.select(*[
                        f.max(f.col(c)).alias(c) for c in timestamp_cols
                    ]).first()
                    if max_ts:
                        values = [v for v in max_ts.asDict().values() if v is not None]
                        last_timestamp = max(values) if values else None
                    else:
                        last_timestamp = None
                    table_data.append((table_name, last_timestamp))
                else:
                    table_data.append((table_name, None))

        except Exception:
            table_data.append((table_name, None))

    return spark.createDataFrame(table_data, ["table_name", "last_modified"])


def get_delta_tables_in_schema(catalog_name: str, schema_name: str) -> List[str]:
    """
    Retrieves a list of all Delta table names in a specified catalog and schema
    using the PySpark DataFrame API.

    Args:
        catalog_name (str): The name of the Unity Catalog.
        schema_name (str): The name of the schema (database).

    Returns:
        List[str]: A list of delta table names in the provided catalog.schema.
    """
    df = spark.table("system.information_schema.tables")

    filtered_df = df.filter(
        (df.table_catalog == catalog_name)
        & (df.table_schema == schema_name)
        & (df.data_source_format == "DELTA")
    ).select("table_catalog", "table_schema", "table_name")

    return [f"{row.table_name}" for row in filtered_df.collect()]
