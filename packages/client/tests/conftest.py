# --- CONFTEST.PY FIXTURES ---

from __future__ import annotations  # keeps annotations as strings at runtime

import os
from collections.abc import Generator
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from dataplatform.client.managed_table_client import ManagedTableClient


import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# region: setup
@pytest.fixture(scope="session")
def spark_session() -> Generator[SparkSession, None, None]:
    """
    Create a Spark session for testing using the existing SparkSession class.
    This fixture uses the same logic as the SparkSession class in
    dataplatform.core.spark_session.

    Returns:
        SparkSession: A configured Spark session (either Databricks Connect
        or local PySpark)

    Raises:
        RuntimeError: If Spark is not available or not configured
    """
    if not os.getenv("DATABRICKS_CONFIG_FILE"):
        os.environ["DATABRICKS_CONFIG_FILE"] = os.path.expanduser("~/.databrickscfg")

    if not os.getenv("DATABRICKS_CONFIG_PROFILE"):
        os.environ["DATABRICKS_CONFIG_PROFILE"] = "DEFAULT"

    if not os.getenv("DATABRICKS_AUTH_TYPE"):
        os.environ["DATABRICKS_AUTH_TYPE"] = "pat"

    try:
        from dataplatform.core.spark_session import SparkSession

        spark = SparkSession().get_spark_session()

        # Test the connection
        spark.sql("SELECT 1 as test").collect()

        yield spark

        # Clean up
        if hasattr(spark, "stop"):
            spark.stop()

    except ImportError as err:
        raise RuntimeError("Spark not installed") from err
    except Exception as e:
        raise RuntimeError(f"Spark not available: {str(e)}") from e


@pytest.fixture(scope="session")
def managed_table_client(
    spark_session: SparkSession,
) -> Generator["ManagedTableClient", None, None]:
    spark = spark_session  # noqa: E402, F401, F841
    from dataplatform.client.managed_table_client import ManagedTableClient  # noqa: E402, F841

    client = ManagedTableClient()
    yield client


# endregion


# region Test 1: Handle multiple updates to same user
@pytest.fixture(scope="session")
def initial_silver_gym_subscriptions_history_df(
    spark_session: SparkSession,
) -> DataFrame:
    """
    Fixture that creates a DataFrame representing the initial silver table state.
    This uses the real spark_session's createDataFrame method.
    """
    data = [
        (
            "U002",
            "John Doe",
            "Basic",
            datetime.fromisoformat("2025-07-29T17:00:00.000+00:00"),
            None,
            True,
            1,
        )
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("effective_start_date", TimestampType(), True),
        StructField("effective_end_date", TimestampType(), True),
        StructField("is_active_record", BooleanType(), True),
        StructField("bronze_commit_version", LongType(), True),
    ])

    df = spark_session.createDataFrame(data, schema=schema)
    return df


@pytest.fixture(scope="session")
def cdc_changes_with_multiple_updates_to_same_user_df(
    spark_session: SparkSession,
) -> DataFrame:
    """
    Fixture that creates a DataFrame representing CDC changes with update events.

    Returns:
        DataFrame: A Spark DataFrame with CDC change events
                   including preimage and postimage updates.
    """
    # CDC data with update events
    cdf_data_update1_raw = [
        (
            "U002",
            "John Doe",
            "Elite",
            "update_preimage",
            datetime.fromisoformat("2025-07-30T09:00:00.000+00:00"),
            2,
        ),
        (
            "U002",
            "John Doe",
            "Gold",
            "update_postimage",
            datetime.fromisoformat("2025-07-30T09:00:00.000+00:00"),
            2,
        ),
        (
            "U002",
            "John Doe",
            "Gold",
            "update_preimage",
            datetime.fromisoformat("2025-07-31T10:00:00.000+00:00"),
            3,
        ),
        (
            "U002",
            "John Doe",
            "Platinum",
            "update_postimage",
            datetime.fromisoformat("2025-07-31T10:00:00.000+00:00"),
            3,
        ),
    ]

    # Define the schema for CDC changes DataFrame
    cdc_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("_change_type", StringType(), True),
        StructField("_commit_timestamp", TimestampType(), True),
        StructField("_commit_version", LongType(), True),
    ])

    # Create the DataFrame
    df = spark_session.createDataFrame(cdf_data_update1_raw, schema=cdc_schema)

    return df


@pytest.fixture(scope="session")
def cdc_records_to_close_out_history_for_multiple_updates_to_same_user_df(
    spark_session,
) -> DataFrame:
    """
    Fixture that creates a DataFrame representing CDC changes with update events.
    """
    data = [
        (
            "U002",
            datetime.fromisoformat("2025-07-30T09:00:00.000+00:00"),
            2,
        )
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("_commit_timestamp", TimestampType(), True),
        StructField("bronze_commit_version", LongType(), True),
    ])

    df = spark_session.createDataFrame(data, schema=schema)
    return df


@pytest.fixture(scope="session")
def cdc_new_history_entries_for_multiple_updates_to_same_user_df(
    spark_session,
) -> DataFrame:
    """
    Fixture that creates a DataFrame representing CDC changes with update events.
    """
    data = [
        (
            "U002",
            "John Doe",
            "Gold",
            2,
            datetime.fromisoformat("2025-07-30T09:00:00.000+00:00"),
            datetime.fromisoformat("2025-07-31T10:00:00.000+00:00"),
            False,
        ),
        (
            "U002",
            "John Doe",
            "Platinum",
            3,
            datetime.fromisoformat("2025-07-31T10:00:00.000+00:00"),
            None,
            True,
        ),
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("bronze_commit_version", LongType(), True),
        StructField("effective_start_date", TimestampType(), True),
        StructField("effective_end_date", TimestampType(), True),
        StructField("is_active_record", BooleanType(), True),
    ])

    return spark_session.createDataFrame(data, schema=schema)


@pytest.fixture(scope="session")
def silver_final_state_after_multiple_updates_to_same_user_df(
    spark_session,
) -> DataFrame:
    """
    Fixture that creates a DataFrame representing the final state of the silver gym
    subscriptions history table
    after CDC workflow, for assertion in integration tests.
    """
    # Should have 3 rows: the initial Basic record (closed),
    # Gold record (closed),
    # and Platinum record (active)
    data = [
        (
            "U002",
            "John Doe",
            "Basic",
            datetime.fromisoformat("2025-07-29T17:00:00.000+00:00"),
            datetime.fromisoformat("2025-07-30T09:00:00.000+00:00"),
            False,
            1,
        ),
        (
            "U002",
            "John Doe",
            "Gold",
            datetime.fromisoformat("2025-07-30T09:00:00.000+00:00"),
            datetime.fromisoformat("2025-07-31T10:00:00.000+00:00"),
            False,
            2,
        ),
        (
            "U002",
            "John Doe",
            "Platinum",
            datetime.fromisoformat("2025-07-31T10:00:00.000+00:00"),
            None,
            True,
            3,
        ),
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("effective_start_date", TimestampType(), True),
        StructField("effective_end_date", TimestampType(), True),
        StructField("is_active_record", BooleanType(), True),
        StructField("bronze_commit_version", LongType(), True),
    ])

    return spark_session.createDataFrame(data, schema=schema)


# endregion


@pytest.fixture(scope="session")
def cdc_changes_with_insert_and_update_to_same_user_df(
    spark_session: SparkSession,
) -> DataFrame:
    """
    Raw CDC data for testing prepare_new_history_entries
    with insert and update_postimage.
    """
    data = [
        (
            "U004",
            "John Doe",
            "travis.tomer@example.com",
            "Elite",
            "active",
            "insert",
            datetime.fromisoformat("2025-07-30T09:00:00.000+00:00"),
            2,
        ),
        (
            "U004",
            "John Doe",
            "travis.tomer@example.com",
            "Elite",
            "active",
            "update_postimage",
            datetime.fromisoformat("2025-08-01T05:06:38.000+00:00"),
            3,
        ),
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("status", StringType(), True),
        StructField("_change_type", StringType(), True),
        StructField("_commit_timestamp", TimestampType(), True),
        StructField("_commit_version", LongType(), True),
    ])

    return spark_session.createDataFrame(data, schema=schema)


@pytest.fixture(scope="session")
def empty_cdc_window_df(spark_session: SparkSession) -> DataFrame:
    """
    Fixture that creates an empty DataFrame representing an empty CDC window.
    """
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("_change_type", StringType(), True),
        StructField("_commit_timestamp", TimestampType(), True),
        StructField("_commit_version", LongType(), True),
    ])
    return spark_session.createDataFrame([], schema=schema)


@pytest.fixture(scope="session")
def cdc_window_with_only_insert_events_df(spark_session: SparkSession) -> DataFrame:
    """
    Fixture that creates a DataFrame representing a CDC window with only delete events.
    """
    data = [
        (
            "U005",
            "John Doe",
            "Gold",
            "insert",
            datetime.fromisoformat("2025-08-02T12:00:00.000+00:00"),
            4,
        ),
        (
            "U006",
            "John Doe",
            "Platinum",
            "insert",
            datetime.fromisoformat("2025-08-03T15:30:00.000+00:00"),
            5,
        ),
        (
            "U006",
            "John Doe",
            "Fitness",
            "insert",
            datetime.fromisoformat("2025-08-03T15:30:00.000+00:00"),
            5,
        ),
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("_change_type", StringType(), True),
        StructField("_commit_timestamp", TimestampType(), True),
        StructField("_commit_version", LongType(), True),
    ])

    return spark_session.createDataFrame(data, schema=schema)


@pytest.fixture(scope="session")
def cdc_window_with_only_delete_events_df(spark_session: SparkSession) -> DataFrame:
    """
    Fixture that creates a DataFrame representing a CDC window with only delete events.
    """
    data = [
        (
            "U005",
            "John Doe",
            "Gold",
            "delete",
            datetime.fromisoformat("2025-08-02T12:00:00.000+00:00"),
            4,
        ),
        (
            "U006",
            "John Doe",
            "Platinum",
            "delete",
            datetime.fromisoformat("2025-08-03T15:30:00.000+00:00"),
            5,
        ),
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("_change_type", StringType(), True),
        StructField("_commit_timestamp", TimestampType(), True),
        StructField("_commit_version", LongType(), True),
    ])

    return spark_session.createDataFrame(data, schema=schema)


@pytest.fixture(scope="session")
def cdc_window_with_insert_and_delete_events_df(
    spark_session: SparkSession,
) -> DataFrame:
    """
    Fixture that creates a DataFrame representing a
    CDC window with insert and delete events.
    """
    data = [
        (
            "U005",
            "John Doe",
            "Gold",
            "delete",
            datetime.fromisoformat("2025-08-02T12:00:00.000+00:00"),
            4,
        ),
        (
            "U006",
            "John Doe",
            "Platinum",
            "insert",
            datetime.fromisoformat("2025-08-03T15:30:00.000+00:00"),
            5,
        ),
        (
            "U006",
            "John Doe",
            "Platinum",
            "delete",
            datetime.fromisoformat("2025-08-03T15:40:00.000+00:00"),
            6,
        ),
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("_change_type", StringType(), True),
        StructField("_commit_timestamp", TimestampType(), True),
        StructField("_commit_version", LongType(), True),
    ])

    return spark_session.createDataFrame(data, schema=schema)


@pytest.fixture(scope="session")
def cdc_data_for_active_flag_test(spark_session: SparkSession) -> DataFrame:
    """
    Fixture provides raw CDC data to test is_active flag logic.
    This mimics insert/update_postimage events arriving in timestamp order.
    """
    data = [
        (
            "U004",
            "John Doe",
            "Elite",
            "insert",
            datetime.fromisoformat("2025-07-15T10:00:00.000+00:00"),
            1,
        ),
        (
            "U004",
            "John Doe",
            "Elite",
            "update_postimage",
            datetime.fromisoformat("2025-07-31T10:00:00.000+00:00"),
            2,
        ),
        (
            "U004",
            "John Doe",
            "Elite",
            "update_postimage",
            datetime.fromisoformat("2025-08-15T10:00:00.000+00:00"),
            3,
        ),
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("_change_type", StringType(), True),
        StructField("_commit_timestamp", TimestampType(), True),
        StructField("_commit_version", LongType(), True),
    ])

    return spark_session.createDataFrame(data, schema=schema)


@pytest.fixture
def empty_delta_table(spark_session: SparkSession) -> Generator[str, None, None]:
    """
    Creates an empty Delta table with a known schema for testing.

    Yields:
        str: Fully qualified table name of the empty Delta table.
    """
    table_name = "sandbox.tests.empty_table"
    empty_df = spark_session.createDataFrame(
        [], "user_id STRING, bronze_commit_version INT"
    )
    empty_df.write.format("delta").mode("overwrite").saveAsTable(table_name)

    yield table_name

    # Cleanup
    spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")


# region: Delete functionality test fixtures
@pytest.fixture(scope="session")
def initial_silver_table_with_active_records_df(
    spark_session: SparkSession,
) -> DataFrame:
    """
    Fixture that creates a DataFrame representing the initial silver table state
    with active records that will be deleted.
    """
    data = [
        (
            "U001",
            "John Doe",
            "Gold",
            datetime.fromisoformat("2025-07-29T17:00:00.000+00:00"),
            None,
            True,
            1,
        ),
        (
            "U002",
            "John Doe",
            "Platinum",
            datetime.fromisoformat("2025-07-30T09:00:00.000+00:00"),
            None,
            True,
            2,
        ),
        (
            "U003",
            "John Doe",
            "Basic",
            datetime.fromisoformat("2025-07-31T10:00:00.000+00:00"),
            None,
            True,
            3,
        ),
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("effective_start_date", TimestampType(), True),
        StructField("effective_end_date", TimestampType(), True),
        StructField("is_active_record", BooleanType(), True),
        StructField("bronze_commit_version", LongType(), True),
    ])

    return spark_session.createDataFrame(data, schema=schema)


@pytest.fixture(scope="session")
def cdc_changes_with_delete_events_df(spark_session: SparkSession) -> DataFrame:
    """
    Fixture that creates a DataFrame representing CDC changes with delete events.
    """
    data = [
        (
            "U001",
            "John Doe",
            "Gold",
            "delete",
            datetime.fromisoformat("2025-08-01T12:00:00.000+00:00"),
            4,
        ),
        (
            "U002",
            "John Doe",
            "Platinum",
            "delete",
            datetime.fromisoformat("2025-08-02T14:30:00.000+00:00"),
            5,
        ),
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("_change_type", StringType(), True),
        StructField("_commit_timestamp", TimestampType(), True),
        StructField("_commit_version", LongType(), True),
    ])

    return spark_session.createDataFrame(data, schema=schema)


@pytest.fixture(scope="session")
def cdc_records_to_close_out_history_for_deletes_df(
    spark_session: SparkSession,
) -> DataFrame:
    """
    Fixture that creates a DataFrame representing records that should be used
    to close out history when delete events occur.
    """
    data = [
        (
            "U001",
            datetime.fromisoformat("2025-08-01T12:00:00.000+00:00"),
            4,
        ),
        (
            "U002",
            datetime.fromisoformat("2025-08-02T14:30:00.000+00:00"),
            5,
        ),
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("_commit_timestamp", TimestampType(), True),
        StructField("bronze_commit_version", LongType(), True),
    ])

    return spark_session.createDataFrame(data, schema=schema)


@pytest.fixture(scope="session")
def silver_final_state_after_deletes_df(spark_session: SparkSession) -> DataFrame:
    """
    Fixture that creates a DataFrame representing the final state of the silver table
    after delete events are processed.
    """
    data = [
        (
            "U001",
            "John Doe",
            "Gold",
            datetime.fromisoformat("2025-07-29T17:00:00.000+00:00"),
            datetime.fromisoformat("2025-08-01T12:00:00.000+00:00"),
            False,
            1,
        ),
        (
            "U002",
            "John Doe",
            "Platinum",
            datetime.fromisoformat("2025-07-30T09:00:00.000+00:00"),
            datetime.fromisoformat("2025-08-02T14:30:00.000+00:00"),
            False,
            2,
        ),
        (
            "U003",
            "John Doe",
            "Basic",
            datetime.fromisoformat("2025-07-31T10:00:00.000+00:00"),
            None,
            True,
            3,
        ),
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("effective_start_date", TimestampType(), True),
        StructField("effective_end_date", TimestampType(), True),
        StructField("is_active_record", BooleanType(), True),
        StructField("bronze_commit_version", LongType(), True),
    ])

    return spark_session.createDataFrame(data, schema=schema)


@pytest.fixture(scope="session")
def cdc_changes_with_mixed_events_including_deletes_df(
    spark_session: SparkSession,
) -> DataFrame:
    """
    Fixture that creates a DataFrame representing CDC changes with mixed events
    including inserts, updates, and deletes.
    """
    data = [
        # Insert event
        (
            "U004",
            "John Doe",
            "Elite",
            "insert",
            datetime.fromisoformat("2025-08-03T10:00:00.000+00:00"),
            6,
        ),
        # Update preimage
        (
            "U003",
            "John Doe",
            "Basic",
            "update_preimage",
            datetime.fromisoformat("2025-08-04T11:00:00.000+00:00"),
            7,
        ),
        # Update postimage
        (
            "U003",
            "John Doe",
            "Premium",
            "update_postimage",
            datetime.fromisoformat("2025-08-04T11:00:00.000+00:00"),
            7,
        ),
        # Delete event
        (
            "U004",
            "John Doe",
            "Elite",
            "delete",
            datetime.fromisoformat("2025-08-05T16:00:00.000+00:00"),
            8,
        ),
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("_change_type", StringType(), True),
        StructField("_commit_timestamp", TimestampType(), True),
        StructField("_commit_version", LongType(), True),
    ])

    return spark_session.createDataFrame(data, schema=schema)


@pytest.fixture(scope="session")
def silver_final_state_after_mixed_events_df(spark_session: SparkSession) -> DataFrame:
    """
    Fixture that creates a DataFrame representing the final state of the silver table
    after mixed events (insert, update, delete) are processed.
    Starting from initial state with U001, U002, U003 as active records.
    """
    data = [
        (
            "U001",
            "John Doe",
            "Gold",
            datetime.fromisoformat("2025-07-29T17:00:00.000+00:00"),
            None,
            True,
            1,
        ),
        (
            "U002",
            "John Doe",
            "Platinum",
            datetime.fromisoformat("2025-07-30T09:00:00.000+00:00"),
            None,
            True,
            2,
        ),
        (
            "U003",
            "John Doe",
            "Basic",
            datetime.fromisoformat("2025-07-31T10:00:00.000+00:00"),
            datetime.fromisoformat("2025-08-04T11:00:00.000+00:00"),
            False,
            3,
        ),
        (
            "U003",
            "John Doe",
            "Premium",
            datetime.fromisoformat("2025-08-04T11:00:00.000+00:00"),
            None,
            True,
            7,
        ),
        (
            "U004",
            "John Doe",
            "Elite",
            datetime.fromisoformat("2025-08-03T10:00:00.000+00:00"),
            datetime.fromisoformat("2025-08-05T16:00:00.000+00:00"),
            False,
            6,
        ),
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("effective_start_date", TimestampType(), True),
        StructField("effective_end_date", TimestampType(), True),
        StructField("is_active_record", BooleanType(), True),
        StructField("bronze_commit_version", LongType(), True),
    ])

    return spark_session.createDataFrame(data, schema=schema)


# endregion


# region: Multiple natural keys test fixtures
@pytest.fixture(scope="session")
def initial_silver_table_with_multiple_natural_keys_df(
    spark_session: SparkSession,
) -> DataFrame:
    """
    Fixture that creates a DataFrame representing the initial silver table state
    with records identified by multiple natural keys (user_id + club_id).
    """
    data = [
        (
            "U001",
            "CLUB001",
            "John Doe",
            "Gold",
            datetime.fromisoformat("2025-07-29T17:00:00.000+00:00"),
            None,
            True,
            1,
        ),
        (
            "U001",
            "CLUB002",
            "John Doe",
            "Platinum",
            datetime.fromisoformat("2025-07-30T09:00:00.000+00:00"),
            None,
            True,
            2,
        ),
        (
            "U002",
            "CLUB001",
            "John Doe",
            "Basic",
            datetime.fromisoformat("2025-07-31T10:00:00.000+00:00"),
            None,
            True,
            3,
        ),
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("club_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("effective_start_date", TimestampType(), True),
        StructField("effective_end_date", TimestampType(), True),
        StructField("is_active_record", BooleanType(), True),
        StructField("bronze_commit_version", LongType(), True),
    ])

    return spark_session.createDataFrame(data, schema=schema)


@pytest.fixture(scope="session")
def cdc_changes_with_multiple_natural_keys_df(spark_session: SparkSession) -> DataFrame:
    """
    Fixture that creates a DataFrame representing CDC changes with multiple
    natural keys.
    This includes updates to the same user at different clubs, demonstrating
    how composite keys work.
    """
    data = [
        # Update for U001 at CLUB001
        (
            "U001",
            "CLUB001",
            "John Doe",
            "Gold",
            "update_preimage",
            datetime.fromisoformat("2025-08-01T12:00:00.000+00:00"),
            4,
        ),
        (
            "U001",
            "CLUB001",
            "John Doe",
            "Elite",
            "update_postimage",
            datetime.fromisoformat("2025-08-01T12:00:00.000+00:00"),
            4,
        ),
        # Update for U001 at CLUB002
        (
            "U001",
            "CLUB002",
            "John Doe",
            "Platinum",
            "update_preimage",
            datetime.fromisoformat("2025-08-02T14:30:00.000+00:00"),
            5,
        ),
        (
            "U001",
            "CLUB002",
            "John Doe",
            "Diamond",
            "update_postimage",
            datetime.fromisoformat("2025-08-02T14:30:00.000+00:00"),
            5,
        ),
        # Insert new record for U002 at CLUB002
        (
            "U002",
            "CLUB002",
            "John Doe",
            "Premium",
            "insert",
            datetime.fromisoformat("2025-08-03T16:00:00.000+00:00"),
            6,
        ),
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("club_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("_change_type", StringType(), True),
        StructField("_commit_timestamp", TimestampType(), True),
        StructField("_commit_version", LongType(), True),
    ])

    return spark_session.createDataFrame(data, schema=schema)


@pytest.fixture(scope="session")
def silver_final_state_after_multiple_natural_keys_df(
    spark_session: SparkSession,
) -> DataFrame:
    """
    Fixture that creates a DataFrame representing the final state of the silver
    table after CDC workflow with multiple natural keys.
    This shows how the same user can have different subscription tiers at
    different clubs.
    """
    data = [
        # U001 at CLUB001 - updated from Gold to Elite
        (
            "U001",
            "CLUB001",
            "John Doe",
            "Gold",
            datetime.fromisoformat("2025-07-29T17:00:00.000+00:00"),
            datetime.fromisoformat("2025-08-01T12:00:00.000+00:00"),
            False,
            1,
        ),
        (
            "U001",
            "CLUB001",
            "John Doe",
            "Elite",
            datetime.fromisoformat("2025-08-01T12:00:00.000+00:00"),
            None,
            True,
            4,
        ),
        # U001 at CLUB002 - updated from Platinum to Diamond
        (
            "U001",
            "CLUB002",
            "John Doe",
            "Platinum",
            datetime.fromisoformat("2025-07-30T09:00:00.000+00:00"),
            datetime.fromisoformat("2025-08-02T14:30:00.000+00:00"),
            False,
            2,
        ),
        (
            "U001",
            "CLUB002",
            "John Doe",
            "Diamond",
            datetime.fromisoformat("2025-08-02T14:30:00.000+00:00"),
            None,
            True,
            5,
        ),
        # U002 at CLUB001 - unchanged
        (
            "U002",
            "CLUB001",
            "John Doe",
            "Basic",
            datetime.fromisoformat("2025-07-31T10:00:00.000+00:00"),
            None,
            True,
            3,
        ),
        # U002 at CLUB002 - new record
        (
            "U002",
            "CLUB002",
            "John Doe",
            "Premium",
            datetime.fromisoformat("2025-08-03T16:00:00.000+00:00"),
            None,
            True,
            6,
        ),
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("club_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("effective_start_date", TimestampType(), True),
        StructField("effective_end_date", TimestampType(), True),
        StructField("is_active_record", BooleanType(), True),
        StructField("bronze_commit_version", LongType(), True),
    ])

    return spark_session.createDataFrame(data, schema=schema)


# endregion


# region: Multiple natural keys delete test fixtures
@pytest.fixture(scope="session")
def initial_silver_table_with_multiple_natural_keys_for_deletes_df(
    spark_session,
) -> DataFrame:
    """
    Fixture that creates a DataFrame representing the initial silver table state
    with records identified by multiple natural keys (user_id + club_id) that
    will be deleted.
    """
    data = [
        (
            "U001",
            "CLUB001",
            "John Doe",
            "Gold",
            datetime.fromisoformat("2025-07-29T17:00:00.000+00:00"),
            None,
            True,
            1,
        ),
        (
            "U001",
            "CLUB002",
            "John Doe",
            "Platinum",
            datetime.fromisoformat("2025-07-30T09:00:00.000+00:00"),
            None,
            True,
            2,
        ),
        (
            "U002",
            "CLUB001",
            "John Doe",
            "Basic",
            datetime.fromisoformat("2025-07-31T10:00:00.000+00:00"),
            None,
            True,
            3,
        ),
        (
            "U002",
            "CLUB002",
            "John Doe",
            "Premium",
            datetime.fromisoformat("2025-08-01T12:00:00.000+00:00"),
            None,
            True,
            4,
        ),
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("club_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("effective_start_date", TimestampType(), True),
        StructField("effective_end_date", TimestampType(), True),
        StructField("is_active_record", BooleanType(), True),
        StructField("bronze_commit_version", LongType(), True),
    ])

    return spark_session.createDataFrame(data, schema=schema)


@pytest.fixture(scope="session")
def cdc_changes_with_deletes_multiple_natural_keys_df(
    spark_session: SparkSession,
) -> DataFrame:
    """
    Fixture that creates a DataFrame representing CDC changes with delete events
    for records with multiple natural keys.
    """
    data = [
        # Delete U001 at CLUB001
        (
            "U001",
            "CLUB001",
            "John Doe",
            "Gold",
            "delete",
            datetime.fromisoformat("2025-08-02T14:00:00.000+00:00"),
            5,
        ),
        # Delete U002 at CLUB002
        (
            "U002",
            "CLUB002",
            "John Doe",
            "Premium",
            "delete",
            datetime.fromisoformat("2025-08-03T16:00:00.000+00:00"),
            6,
        ),
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("club_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("_change_type", StringType(), True),
        StructField("_commit_timestamp", TimestampType(), True),
        StructField("_commit_version", LongType(), True),
    ])

    return spark_session.createDataFrame(data, schema=schema)


@pytest.fixture(scope="session")
def cdc_records_to_close_out_history_for_deletes_multiple_natural_keys_df(
    spark_session,
) -> DataFrame:
    """
    Fixture that creates a DataFrame representing records that should be used
    to close out history when delete events occur for multiple natural keys.
    """
    data = [
        (
            "U001",
            "CLUB001",
            datetime.fromisoformat("2025-08-02T14:00:00.000+00:00"),
            5,
        ),
        (
            "U002",
            "CLUB002",
            datetime.fromisoformat("2025-08-03T16:00:00.000+00:00"),
            6,
        ),
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("club_id", StringType(), True),
        StructField("_commit_timestamp", TimestampType(), True),
        StructField("bronze_commit_version", LongType(), True),
    ])

    return spark_session.createDataFrame(data, schema=schema)


@pytest.fixture(scope="session")
def silver_final_state_after_deletes_multiple_natural_keys_df(
    spark_session,
) -> DataFrame:
    """
    Fixture that creates a DataFrame representing the final state of the silver table
    after delete events are processed for multiple natural keys.
    """
    data = [
        # U001 at CLUB001 - deleted (closed out)
        (
            "U001",
            "CLUB001",
            "John Doe",
            "Gold",
            datetime.fromisoformat("2025-07-29T17:00:00.000+00:00"),
            datetime.fromisoformat("2025-08-02T14:00:00.000+00:00"),
            False,
            1,
        ),
        # U001 at CLUB002 - unchanged (still active)
        (
            "U001",
            "CLUB002",
            "John Doe",
            "Platinum",
            datetime.fromisoformat("2025-07-30T09:00:00.000+00:00"),
            None,
            True,
            2,
        ),
        # U002 at CLUB001 - unchanged (still active)
        (
            "U002",
            "CLUB001",
            "John Doe",
            "Basic",
            datetime.fromisoformat("2025-07-31T10:00:00.000+00:00"),
            None,
            True,
            3,
        ),
        # U002 at CLUB002 - deleted (closed out)
        (
            "U002",
            "CLUB002",
            "John Doe",
            "Premium",
            datetime.fromisoformat("2025-08-01T12:00:00.000+00:00"),
            datetime.fromisoformat("2025-08-03T16:00:00.000+00:00"),
            False,
            4,
        ),
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("club_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("effective_start_date", TimestampType(), True),
        StructField("effective_end_date", TimestampType(), True),
        StructField("is_active_record", BooleanType(), True),
        StructField("bronze_commit_version", LongType(), True),
    ])

    return spark_session.createDataFrame(data, schema=schema)


@pytest.fixture(scope="session")
def cdc_changes_with_mixed_events_including_deletes_multiple_natural_keys_df(
    spark_session,
) -> DataFrame:
    """
    Fixture that creates a DataFrame representing CDC changes with mixed events
    including inserts, updates, and deletes for multiple natural keys.
    """
    data = [
        # Insert new record for U003 at CLUB001
        (
            "U003",
            "CLUB001",
            "John Doe",
            "Elite",
            "insert",
            datetime.fromisoformat("2025-08-04T10:00:00.000+00:00"),
            7,
        ),
        # Update U001 at CLUB002 from Platinum to Diamond
        (
            "U001",
            "CLUB002",
            "John Doe",
            "Platinum",
            "update_preimage",
            datetime.fromisoformat("2025-08-05T11:00:00.000+00:00"),
            8,
        ),
        (
            "U001",
            "CLUB002",
            "John Doe",
            "Diamond",
            "update_postimage",
            datetime.fromisoformat("2025-08-05T11:00:00.000+00:00"),
            8,
        ),
        # Delete U003 at CLUB001 (same record that was just inserted)
        (
            "U003",
            "CLUB001",
            "John Doe",
            "Elite",
            "delete",
            datetime.fromisoformat("2025-08-06T12:00:00.000+00:00"),
            9,
        ),
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("club_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("_change_type", StringType(), True),
        StructField("_commit_timestamp", TimestampType(), True),
        StructField("_commit_version", LongType(), True),
    ])

    return spark_session.createDataFrame(data, schema=schema)


@pytest.fixture(scope="session")
def silver_final_state_after_mixed_events_multiple_natural_keys_df(
    spark_session,
) -> DataFrame:
    """
    Fixture that creates a DataFrame representing the final state of the silver table
    after mixed events (insert, update, delete) are processed for multiple natural keys.
    Starting from initial state with U001 and U002 records.
    """
    data = [
        # U001 at CLUB001 - unchanged (still active)
        (
            "U001",
            "CLUB001",
            "John Doe",
            "Gold",
            datetime.fromisoformat("2025-07-29T17:00:00.000+00:00"),
            None,
            True,
            1,
        ),
        # U001 at CLUB002 - updated from Platinum to Diamond
        (
            "U001",
            "CLUB002",
            "John Doe",
            "Platinum",
            datetime.fromisoformat("2025-07-30T09:00:00.000+00:00"),
            datetime.fromisoformat("2025-08-05T11:00:00.000+00:00"),
            False,
            2,
        ),
        (
            "U001",
            "CLUB002",
            "John Doe",
            "Diamond",
            datetime.fromisoformat("2025-08-05T11:00:00.000+00:00"),
            None,
            True,
            8,
        ),
        # U002 at CLUB001 - unchanged (still active)
        (
            "U002",
            "CLUB001",
            "John Doe",
            "Basic",
            datetime.fromisoformat("2025-07-31T10:00:00.000+00:00"),
            None,
            True,
            3,
        ),
        # U002 at CLUB002 - unchanged (still active)
        (
            "U002",
            "CLUB002",
            "John Doe",
            "Premium",
            datetime.fromisoformat("2025-08-01T12:00:00.000+00:00"),
            None,
            True,
            4,
        ),
        # U003 at CLUB001 - inserted then deleted (closed out)
        (
            "U003",
            "CLUB001",
            "John Doe",
            "Elite",
            datetime.fromisoformat("2025-08-04T10:00:00.000+00:00"),
            datetime.fromisoformat("2025-08-06T12:00:00.000+00:00"),
            False,
            7,
        ),
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("club_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("effective_start_date", TimestampType(), True),
        StructField("effective_end_date", TimestampType(), True),
        StructField("is_active_record", BooleanType(), True),
        StructField("bronze_commit_version", LongType(), True),
    ])

    return spark_session.createDataFrame(data, schema=schema)


# endregion


# region: Empty table with inserts and post images test fixtures
@pytest.fixture(scope="session")
def cdc_changes_with_inserts_and_post_images_to_empty_table_df(
    spark_session,
) -> DataFrame:
    """
    Fixture that creates a DataFrame representing CDC changes with insert,
    update_preimage, and update_postimage events in the same CDC window,
    targeting an empty silver table.
    This tests the scenario where we have new records being inserted and existing
    records being updated in the same batch.
    """
    data = [
        # Insert new user
        (
            "U001",
            "John Doe",
            "Gold",
            "insert",
            datetime.fromisoformat("2025-08-01T10:00:00.000+00:00"),
            1,
        ),
        # Insert another new user
        (
            "U002",
            "John Doe",
            "Platinum",
            "insert",
            datetime.fromisoformat("2025-08-01T11:00:00.000+00:00"),
            1,
        ),
        # Update preimage for a user (this would close out an existing record)
        (
            "U003",
            "John Doe",
            "Basic",
            "update_preimage",
            datetime.fromisoformat("2025-08-01T12:00:00.000+00:00"),
            1,
        ),
        # Update postimage for the same user (this creates the new version)
        (
            "U003",
            "John Doe",
            "Premium",
            "update_postimage",
            datetime.fromisoformat("2025-08-01T12:00:00.000+00:00"),
            1,
        ),
        # Another update preimage
        (
            "U004",
            "John Doe",
            "Elite",
            "update_preimage",
            datetime.fromisoformat("2025-08-01T13:00:00.000+00:00"),
            1,
        ),
        # Another update postimage
        (
            "U004",
            "John Doe",
            "Diamond",
            "update_postimage",
            datetime.fromisoformat("2025-08-01T13:00:00.000+00:00"),
            1,
        ),
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("_change_type", StringType(), True),
        StructField("_commit_timestamp", TimestampType(), True),
        StructField("_commit_version", LongType(), True),
    ])

    return spark_session.createDataFrame(data, schema=schema)


@pytest.fixture(scope="session")
def silver_final_state_after_inserts_and_post_images_to_empty_table_df(
    spark_session,
) -> DataFrame:
    """
    Fixture that creates a DataFrame representing the expected final state
    of the silver table after processing inserts, update_preimage, and update_postimage
    events in an empty table scenario.
    """
    data = [
        # U001 - insert event (active record)
        (
            "U001",
            "John Doe",
            "Gold",
            datetime.fromisoformat("2025-08-01T10:00:00.000+00:00"),
            None,
            True,
            1,
        ),
        # U002 - insert event (active record)
        (
            "U002",
            "John Doe",
            "Platinum",
            datetime.fromisoformat("2025-08-01T11:00:00.000+00:00"),
            None,
            True,
            1,
        ),
        # U003 - update_postimage event (active record) - note the subscription_tier
        # changed to Premium
        (
            "U003",
            "John Doe",
            "Premium",
            datetime.fromisoformat("2025-08-01T12:00:00.000+00:00"),
            None,
            True,
            1,
        ),
        # U004 - update_postimage event (active record) - note the subscription_tier
        # changed to Diamond
        (
            "U004",
            "John Doe",
            "Diamond",
            datetime.fromisoformat("2025-08-01T13:00:00.000+00:00"),
            None,
            True,
            1,
        ),
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("effective_start_date", TimestampType(), True),
        StructField("effective_end_date", TimestampType(), True),
        StructField("is_active_record", BooleanType(), True),
        StructField("bronze_commit_version", LongType(), True),
    ])

    return spark_session.createDataFrame(data, schema=schema)


@pytest.fixture(scope="session")
def empty_silver_table_for_inserts_and_post_images_df(
    spark_session: SparkSession,
) -> DataFrame:
    """
    Fixture that creates an empty DataFrame representing an empty silver table
    for testing inserts and post images scenario.
    """
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("effective_start_date", TimestampType(), True),
        StructField("effective_end_date", TimestampType(), True),
        StructField("is_active_record", BooleanType(), True),
        StructField("bronze_commit_version", LongType(), True),
    ])

    return spark_session.createDataFrame([], schema=schema)


# endregion
# region: merge() test fixtures


@pytest.fixture(scope="session")
def pricing_base_daily_seed_df(spark_session: SparkSession) -> DataFrame:
    data = [
        (
            "1",
            "Club A",
            "UT",
            datetime.fromisoformat("2025-08-13T13:00:00.000+00:00"),
            "Premium",
            Decimal("5.00"),
            Decimal("20.00"),
            1,
        ),
    ]
    schema = StructType([
        StructField("club_id", StringType(), True),
        StructField("club_name", StringType(), True),
        StructField("state", StringType(), True),
        StructField("date", TimestampType(), True),
        StructField("membership_type", StringType(), True),
        StructField("enrollment_fee_default", DecimalType(10, 2), True),
        StructField("monthly_rate_default", DecimalType(10, 2), True),
        StructField("days_free_default", LongType(), True),
    ])
    return spark_session.createDataFrame(data, schema=schema)


@pytest.fixture(scope="session")
def pricing_base_daily_source_df(spark_session: SparkSession) -> DataFrame:
    data = [
        (
            "1",
            "Alpha",
            "UT",
            datetime.fromisoformat("2025-08-13T13:00:00.000+00:00"),
            "Premium",
            Decimal("15.00"),
            Decimal("35.00"),
            7,
        ),
        (
            "3",
            "Gamma",
            "CO",
            datetime.fromisoformat("2025-08-13T13:00:00.000+00:00"),
            "Elite",
            Decimal("99.00"),
            Decimal("199.00"),
            30,
        ),
    ]
    schema = StructType([
        StructField("club_id", StringType(), True),
        StructField("club_name", StringType(), True),
        StructField("state", StringType(), True),
        StructField("date", TimestampType(), True),
        StructField("membership_type", StringType(), True),
        StructField("enrollment_fee_default", DecimalType(10, 2), True),
        StructField("monthly_rate_default", DecimalType(10, 2), True),
        StructField("days_free_default", LongType(), True),
    ])

    return spark_session.createDataFrame(data, schema=schema)


@pytest.fixture(scope="session")
def pricing_daily_empty_target_df(spark_session: SparkSession) -> DataFrame:
    schema = StructType([
        StructField("club_id", StringType(), True),
        StructField("club_name", StringType(), True),
        StructField("membership_type", StringType(), True),
        StructField("promo_code", StringType(), True),
        StructField("date", TimestampType(), True),
        StructField("enrollment_fee_default", DecimalType(10, 2), True),
        StructField("enrollment_fee_promo", DecimalType(10, 2), True),
        StructField("monthly_rate_default", DecimalType(10, 2), True),
        StructField("monthly_rate_promo", DecimalType(10, 2), True),
        StructField("days_free_default", LongType(), True),
        StructField("days_free_promo", LongType(), True),
        StructField("displayed_high_pricing", DecimalType(10, 2), True),
        StructField("promotion_category", StringType(), True),
        StructField("call_to_action", StringType(), True),
    ])
    return spark_session.createDataFrame([], schema=schema)


@pytest.fixture(scope="session")
def pricing_daily_source_df(spark_session: SparkSession) -> DataFrame:
    data = [
        (
            "10",
            "Foxtrot",
            "Gold",
            "PROMO10",
            datetime.fromisoformat("2025-08-13T13:00:00.000+00:00"),
            Decimal("25.00"),
            Decimal("0.00"),
            Decimal("45.00"),
            Decimal("40.00"),
            0,
            7,
            Decimal("55.00"),
            "Summer",
            "Join now",
        ),
        (
            "11",
            "Hotel",
            "Basic",
            None,
            datetime.fromisoformat("2025-08-09T13:00:00.000+00:00"),
            Decimal("10.00"),
            Decimal("0.00"),
            Decimal("20.00"),
            Decimal("0.00"),
            0,
            0,
            Decimal("25.00"),
            None,
            None,
        ),
        (
            "12",
            "India",
            "Elite",
            "PROMO99",
            datetime.fromisoformat("2025-08-09T13:00:00.000+00:00"),
            Decimal("99.00"),
            Decimal("50.00"),
            Decimal("150.00"),
            Decimal("120.00"),
            0,
            14,
            Decimal("175.00"),
            "VIP",
            "Upgrade",
        ),
    ]
    schema = StructType([
        StructField("club_id", StringType(), True),
        StructField("club_name", StringType(), True),
        StructField("membership_type", StringType(), True),
        StructField("promo_code", StringType(), True),
        StructField("date", TimestampType(), True),
        StructField("enrollment_fee_default", DecimalType(10, 2), True),
        StructField("enrollment_fee_promo", DecimalType(10, 2), True),
        StructField("monthly_rate_default", DecimalType(10, 2), True),
        StructField("monthly_rate_promo", DecimalType(10, 2), True),
        StructField("days_free_default", LongType(), True),
        StructField("days_free_promo", LongType(), True),
        StructField("displayed_high_pricing", DecimalType(10, 2), True),
        StructField("promotion_category", StringType(), True),
        StructField("call_to_action", StringType(), True),
    ])

    return spark_session.createDataFrame(data, schema=schema)
