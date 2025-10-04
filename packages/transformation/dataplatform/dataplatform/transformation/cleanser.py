from typing import Any, Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    lower,
    regexp_replace,
    trim,
    upper,
    when,
)
from dataplatform.client.managed_table_client import ManagedTableClient
from dataplatform.core.argparser import is_argument
from dataplatform.core.logger import get_logger

logger = get_logger(__name__)


def clean(df: DataFrame):
    """
    Cleans the DataFrame by trimming whitespace and replacing empty strings with
    None for non-CDC metadata string columns.

    Args:
        df (DataFrame): Input DataFrame to be cleaned

    Returns:
        DataFrame: Cleaned DataFrame
    """
    processed_columns = []

    for field in df.schema.fields:
        if field.dataType.typeName() == "string":
            cleaned_col = (
                when(
                    trim(col(field.name)).isNull() | (trim(col(field.name)) == ""),
                    None,
                )
                .otherwise(trim(col(field.name)))
                .alias(field.name)
            )
            processed_columns.append(cleaned_col)
        else:
            processed_columns.append(field.name)

    return df.select(*processed_columns)


def filter(df: DataFrame):
    """
    Filters out deleted records based on the _fivetran_deleted flag

    Args:
        df (DataFrame): Input DataFrame containing _fivetran_deleted column

    Returns:
        DataFrame: Filtered DataFrame with only non-deleted records
    """
    if "_fivetran_deleted" in df.columns:
        df = df.filter(df["_fivetran_deleted"] == "false").drop("_fivetran_deleted")
    return df


def normalize_column_values(df: DataFrame, columns: list):
    """
    This is meant for fields that are going to be used for joins and filters.
    Normalizes the values of specified columns by converting them
    to lowercase and replacing all non-alphanumeric characters with
    a single underscore

    Args:
        df (DataFrame): Input DataFrame to be normalized
        columns (list): List of column names to be normalized

    Returns:
        DataFrame: Normalized DataFrame
    """
    for column in columns:
        if df.schema[column].dataType.typeName() == "string":
            # Convert to lowercase and replace all non-alphanumeric chars
            # with single underscore
            non_alphanumeric_pattern = r"[^a-zA-Z0-9]"

            # Apply the transformations: first , then replace non-alphanumeric
            df = df.withColumn(
                column,
                regexp_replace(trim(lower(df[column])), non_alphanumeric_pattern, "_"),
            )

    return df


def standardize_date_columns(df: DataFrame, columns: list[dict]) -> DataFrame:
    """
    Standardize date columns by:
    1. Renaming original column to new_name_timestamp
    2. Creating new_name_date column cast to date type

    Args:
        df: Input DataFrame
        columns: List of dictionaries containing original_name and new_name
        mappings

    Returns:
        DataFrame with standardized date columns
    """
    if not columns or columns == [None]:
        return df

    for column in columns:
        if df.schema[column["original_name"]].dataType.typeName() != "timestamp":
            raise ValueError(
                f"Only timestamp type columns can be standardized. "
                f"Column {column['original_name']} must be timestamp type"
            )

        original_name = column["original_name"]
        new_name = column["new_name"]

        if new_name == "created":
            df = df.drop("created_timestamp")

        # First rename to timestamp column
        df = df.withColumnRenamed(original_name, f"{new_name}_timestamp")

        # Create date column from timestamp
        df = df.withColumn(f"{new_name}_date", df[f"{new_name}_timestamp"].cast("date"))

    return df


def uppercase_alpha_columns(df: DataFrame, column_names: list[str]) -> DataFrame:
    """
    Uppercases the specified columns in the DataFrame if they should contain
    only alphabetic characters.

    Args:
        df (DataFrame): The input DataFrame.
        column_names (list[str]): The list of column names to be uppercased.

    Returns:
        DataFrame: The DataFrame with the specified columns uppercased if they
                   contain only alphabetic characters.
    """
    for column_name in column_names:
        df = df.withColumn(
            column_name,
            when(
                df[column_name].isNotNull()
                & (regexp_replace(df[column_name], "[^A-Za-z]", "") == df[column_name]),
                upper(df[column_name]),
            ).otherwise(None),
        )
    return df


def order_columns(df: DataFrame, columns: list):
    """
    Orders the columns of a DataFrame based on the order
    specified in the config and then alphabetically

    Args:
        df (DataFrame): Input DataFrame to be ordered
        columns (list): List of column names to be ordered

    Returns:
        DataFrame: Ordered DataFrame
    """
    all_columns = df.columns

    ordered_columns = [col for col in columns if col in all_columns]
    fivetran_columns = [
        col for col in all_columns if col in ["_fivetran_deleted", "_fivetran_synced"]
    ]

    remaining_columns = [
        col for col in all_columns if col not in columns and col not in fivetran_columns
    ]

    # Sort remaining columns alphabetically
    remaining_columns.sort()

    final_column_order = ordered_columns + remaining_columns + fivetran_columns

    return df.select(final_column_order)


def select_columns(df: DataFrame, columns: list):
    """
    Selects specified columns from the DataFrame.
    If columns is empty, all columns will be selected.

    Args:
        df (DataFrame): Input DataFrame to be modified
        columns (list): List of column names to be selected. Use empty list []
                        to select all columns.

    Returns:
        DataFrame: DataFrame with only the specified columns
    """
    if not columns:  # If empty list, select all columns
        selected_columns = df.columns
    else:
        selected_columns = [col for col in columns if col in df.columns]
    return df.select(selected_columns)


def rename_columns(df: DataFrame, columns: list[dict]) -> DataFrame:
    """
    Rename columns in the DataFrame based on the mapping provided

    Args:
        df: Input DataFrame
        columns: List of dictionaries containing original_name and new_name
        mappings

    Returns:
        DataFrame with renamed columns
    """
    if not columns:
        return df

    for column in columns:
        df = df.withColumnRenamed(column["original_name"], column["new_name"])

    return df


def rename_columns_to_snake_case(df: DataFrame, columns: list[str]) -> DataFrame:
    """
    Rename columns to snake_case
    """
    return df.toDF(*[col.replace(" ", "_").lower() for col in df.columns])


def transform_has_to_bool(df: DataFrame, columns: list[tuple[str, str]]) -> DataFrame:
    """
    Converts specified string columns in a DataFrame to boolean columns based
    on the presence of a given substring.

    For each (column_name, match_string) tuple in `columns`, the function sets
    the column to True if the value contains
    the `match_string` (case-insensitive), otherwise sets it to False.

        df (DataFrame): The input Spark DataFrame.
        columns (list[tuple[str, str]]): A list of tuples, each containing the
                                         column name and the substring to match.

        DataFrame: A new DataFrame with the specified columns converted to
                   boolean values.
    """
    for column in columns:
        df = df.withColumn(
            column[0],
            when(
                upper(col(column[0])).contains(column[1].upper()),
                lit(True),
            )
            .otherwise(False)
            .cast("boolean"),
        )
    return df


def get_cdc_columns(args: list[str]) -> list[str]:
    """
    Returns the appropriate list of additional columns based on the load type.
    """
    is_bulk_load = is_argument(args, "bulk_load")
    is_history = is_argument(args, "history")

    if is_bulk_load and is_history:
        return [
            "bronze_commit_version",
            "is_active_record",
            "effective_start_date",
            "effective_end_date",
        ]
    elif not is_bulk_load and is_history:
        return [
            "_change_type",
            "_commit_version",
            "_commit_timestamp",
        ]

    return [
        "bronze_commit_version",
    ]


def cleanse(config: dict, args: list[str]) -> dict:
    """
    Cleanses the data based on the config

    Args:
        config (dict): Configuration dictionary containing table definitions

    Returns:
        dict: Copy of config with cleansed DataFrames
    """
    cdc_columns = get_cdc_columns(args)

    for values in config.values():
        df = filter(values["dataframe"])
        df = clean(df)
        df = rename_columns(df, values["rename_columns"])
        df = standardize_date_columns(df, values["standardize_date_columns"])
        df = select_columns(df, values["select_columns"] + cdc_columns)
        df = normalize_column_values(df, values["normalize_column_values"])
        values["dataframe"] = df

        logger.info(f"Cleansed {values['table_name']}")

    return config


def load_data(
    config: Dict[str, Any],
    starting_version: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Loads Spark DataFrames for each table in the config and adds them to the config
    dict.
    Supports reading full tables or Change Data Feed (CDC) or
    commit version.

    Args:
        config: Configuration dictionary containing table definitions. Each
                table definition should include at least "table_name" and "read_cdc".
        starting_version: Optional. A version number to start reading CDC changes from.
                          If only this is provided, changes are read up to the latest.

    Returns:
        config: The updated configuration dictionary, with a "dataframe" key
                added to each table's definition, containing the loaded
                DataFrame.

    Raises:
        ValueError: If conflicting options (both timestamp and commit version)
                    are provided for CDC, or if ending_timestamp is given without
                    starting_timestamp.
    """
    managed_table_client = ManagedTableClient()

    for values in config.values():
        table_name = values.get("table_name")
        read_cdc = values.get("read_cdc", False)

        if not table_name:
            raise ValueError(
                "Each table definition in the config must have a 'table_name'."
            )

        if read_cdc and starting_version:
            df = managed_table_client.read_starting_version(
                table_name=table_name,
                starting_version=starting_version,
            )
            values["dataframe"] = df
        else:
            df = managed_table_client.read(table_name)
            values["dataframe"] = df

    return config


def load_multiple_target_tables(
    config: Dict[str, Any],
    is_bulk_load: bool = False,
) -> Dict[str, Any]:
    """
    Loads Spark DataFrames for each table in the config and adds them to the config
    dict. It is used to load multiple tables to write in the config. It gets the max
    commit version of the target_table_name and reads that version from table_name.

    Supports reading full tables or Change Data Feed (CDC) or
    commit version.

    Args:
        config: Configuration dictionary containing table definitions. Each
                table definition should include at least "table_name, target_table_name
                and read_cdc".

    Returns:
        config: The updated configuration dictionary, with a "dataframe" key
                added to each table's definition, containing the loaded
                DataFrame.

    Raises:
        ValueError: If conflicting options (both timestamp and commit version)
                    are provided for CDC, or if ending_timestamp is given without
                    starting_timestamp.
    """
    managed_table_client = ManagedTableClient()

    for values in config.values():
        table_name = values.get("table_name")
        target_table_name = values.get("target_table_name")
        read_cdc = values.get("read_cdc", False)

        if not read_cdc and not table_name and not target_table_name:
            raise ValueError(
                "Each table definition in the config must have a 'read_cdc', "
                "'table_name', and 'target_table_name'."
            )

        if is_bulk_load:
            logger.info(f"Running bulk load for {table_name}")
            commit_version = managed_table_client.get_current_commit_version(table_name)
            df = managed_table_client.read(table_name)
            df = df.withColumn("bronze_commit_version", lit(commit_version))
            values["dataframe"] = df

        else:
            logger.info(f"Running CDC load for {table_name}")
            max_commit_version = managed_table_client.get_max_commit_version(
                target_table_name
            )
            df = managed_table_client.read_starting_version(
                table_name=table_name,
                starting_version=max_commit_version,
            )
            values["dataframe"] = df

    return config
