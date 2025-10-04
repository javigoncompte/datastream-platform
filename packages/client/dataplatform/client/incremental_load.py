import datetime
from collections import ChainMap

import pyspark.sql.functions as f
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from dataplatform.client.managed_table_client import ManagedTableClient
from dataplatform.core.spark_session import SparkSession

table_operations = ("MERGE", "WRITE", "CREATE TABLE", "DELETE", "UPDATE", "TRUNCATE")
default_cdc_options = {"readChangeFeed": "true"}


def get_history_dataframe(table_name: str) -> DataFrame:
    spark = SparkSession().get_spark_session()
    history_df = (
        DeltaTable.forName(spark, table_name)
        .history()
        .filter(f.col("operation").isin(table_operations))
        .select("version", "timestamp", "operation", "operationParameters")
        .orderBy(f.col("timestamp").desc())
    )

    return history_df


def get_latest_ingestion_timestamp(target_table: str) -> datetime.datetime:
    history_df = get_history_dataframe(target_table)
    return history_df.select("timestamp").collect()[0][0]


def filter_history_dataframe_by_timestamp(
    history_df: DataFrame, timestamp: datetime.datetime
) -> DataFrame:
    filtered_history_df = history_df.filter(f.col("timestamp") <= timestamp)

    return filtered_history_df


def get_closest_version_to_timestamp(
    history_df: DataFrame, timestamp: datetime.datetime
) -> str:
    """
    Get the closest version to the timestamp.

    Args:
        history_df: The history dataframe.
        timestamp: The timestamp to get the closest version to.
    """
    df = filter_history_dataframe_by_timestamp(history_df, timestamp)
    df = df.withColumn("timestamp_diff", f.abs(f.col("timestamp") - f.lit(timestamp)))
    window = Window.orderBy(f.col("timestamp_diff"))
    df = df.withColumn("rank", f.row_number().over(window))
    closest_version = df.filter(f.col("rank") == 1).select("version").collect()[0][0]

    return str(closest_version)


def get_source_change_feed(
    source_table: str, target_table: str, client: ManagedTableClient
) -> DataFrame:
    """
    Get the change feed for the source table.

    Args:
        source_table: The name of the source table.
        target_table: The name of the target table.
        client: The client to use to read the change feed.

    Returns:
        The change feed for the source table.
    """
    latest_ingestion_timestamp_target = get_latest_ingestion_timestamp(target_table)
    source_history_df = get_history_dataframe(source_table)
    starting_version = get_closest_version_to_timestamp(
        source_history_df, latest_ingestion_timestamp_target
    )
    options = ChainMap(default_cdc_options, {"startingVersion": starting_version})
    change_feed_df = client.read(source_table, options=dict(options))

    return change_feed_df


def get_incremental_load_dataframe(
    config: dict, target_table: str, client: ManagedTableClient
) -> dict[str, DataFrame]:
    """
    Get the incremental load dataframe.

    Args:
        config: The config to use to get the incremental load dataframe.
        target_table: The name of the target table.
        client: The client to use to read the change feed.

    Returns:
        The incremental load dataframe.
    """
    for sources in config.values():
        source_table = sources["table_name"]
        change_feed_df = get_source_change_feed(source_table, target_table, client)
        sources["dataframe"] = change_feed_df
    return config
