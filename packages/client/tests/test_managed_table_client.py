from datetime import datetime as dt

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from dataplatform.client.managed_table_client import ManagedTableClient
from dataplatform.core.spark_session import SparkSession

# === Table names used in tests ===
SILVER_MERGE_HISTORY_TEST_TABLE = "sandbox.tests.silver_merge_history_test"
OVERWRITE_ROUND_TRIP_TABLE = "sandbox.tests.test_overwrite_round_trip"
SILVER_SCD2_IDEMPOTENT_CHECK_TABLE = "sandbox.tests.silver_scd2_idempotent_check"
SILVER_MERGE_HISTORY_MULTIPLE_KEYS_TEST_TABLE = (
    "sandbox.tests.silver_merge_history_multiple_keys_test"
)
SILVER_DELETE_TEST_TABLE = "sandbox.tests.silver_delete_test"
SILVER_MIXED_EVENTS_TEST_TABLE = "sandbox.tests.silver_mixed_events_test"
SILVER_DELETE_MULTIPLE_KEYS_TEST_TABLE = (
    "sandbox.tests.silver_delete_multiple_keys_test"
)
SILVER_MIXED_EVENTS_MULTIPLE_KEYS_TEST_TABLE = (
    "sandbox.tests.silver_mixed_events_multiple_keys_test"
)
# ==================================


def assert_dataframes_equal(df1: DataFrame, df2: DataFrame) -> None:
    """
    Compares two Spark DataFrames by schema and sorted content.
    Raises AssertionError if they are not equal.
    """
    assert df1.schema == df2.schema, "Schemas do not match"
    df1_sorted = df1.sort(*df1.columns).collect()
    df2_sorted = df2.sort(*df2.columns).collect()
    assert df1_sorted == df2_sorted, "DataFrames content does not match"


# region: Basic Client Functionality Tests
def test_managed_table_client_initialization(
    managed_table_client: ManagedTableClient,
) -> None:
    """
    Test that the ManagedTableClient can be initialized properly.
    """
    assert managed_table_client is not None
    assert hasattr(managed_table_client, "_environment")


def test_managed_table_client_overwrite(
    managed_table_client: ManagedTableClient,
    initial_silver_gym_subscriptions_history_df: DataFrame,
) -> None:
    """
    Test the overwrite function.
    """
    managed_table_client.overwrite(
        initial_silver_gym_subscriptions_history_df,
        SILVER_MERGE_HISTORY_TEST_TABLE,
    )
    assert (
        managed_table_client.read(SILVER_MERGE_HISTORY_TEST_TABLE).count()
        == initial_silver_gym_subscriptions_history_df.count()
    )


def test_get_max_commit_version_populated_table(
    managed_table_client: ManagedTableClient,
) -> None:
    """
    Test the get_max_commit_version function.
    """
    assert (
        managed_table_client.get_max_commit_version(SILVER_MERGE_HISTORY_TEST_TABLE)
        == 1
    )


def test_get_max_commit_version_empty_table_returns_minus_one(
    managed_table_client: ManagedTableClient,
    empty_delta_table: DeltaTable,
) -> None:
    """
    Ensures get_max_commit_version returns -1 for an empty Delta table.
    """
    result = managed_table_client.get_max_commit_version(empty_delta_table)
    assert result == -1


def test_overwrite_and_read_round_trip(
    managed_table_client: ManagedTableClient,
    initial_silver_gym_subscriptions_history_df: DataFrame,
) -> None:
    """
    Verifies that overwrite() correctly writes a Delta table
    and read() retrieves the same content.

    This test writes a known DataFrame to a temporary Delta table,
    reads it back, and asserts that both schema and content are identical.
    """
    table_name = OVERWRITE_ROUND_TRIP_TABLE

    # Write to Delta table
    managed_table_client.overwrite(
        initial_silver_gym_subscriptions_history_df, table_name
    )

    # Read back
    df_read = managed_table_client.read(table_name)

    # Assert row count and full content match
    assert_dataframes_equal(df_read, initial_silver_gym_subscriptions_history_df)


def test_filter_cdc_events_valid_types_only(
    managed_table_client: ManagedTableClient,
    cdc_changes_with_multiple_updates_to_same_user_df: DataFrame,
) -> None:
    """
    Test that _filter_cdc_events returns only rows with matching event types.
    """

    filtered_df = managed_table_client._filter_cdc_events(
        cdc_changes_with_multiple_updates_to_same_user_df,
        ["insert", "update_postimage"],
    )

    distinct_types = [
        row["_change_type"]
        for row in filtered_df.select("_change_type").distinct().collect()
    ]
    assert set(distinct_types).issubset({"insert", "update_postimage"})


# endregion


# region: Empty Table and Post-Image Tests
def test_merge_history_with_inserts_and_post_images_to_empty_table(
    managed_table_client: ManagedTableClient,
    spark_session: SparkSession,
    cdc_changes_with_inserts_and_post_images_to_empty_table_df: DataFrame,
    silver_final_state_after_inserts_and_post_images_to_empty_table_df: DataFrame,
    empty_silver_table_for_inserts_and_post_images_df: DataFrame,
) -> None:
    """
    Test the full merge_history workflow with insert, update_preimage, and
    update_postimage events in the same CDC window, targeting an empty silver table.

    This test verifies that:
    1. Insert events create new active records with proper effective dates
    2. Update_preimage events are processed correctly (though they don't close out
       existing records in an empty table scenario)
    3. Update_postimage events create new active records with updated data
    4. All records have the correct is_active_record flag set
    5. The merge operation works correctly when starting from an empty table
    6. Subscription tier changes are properly reflected in the final state
    """
    table_name = "sandbox.tests.silver_inserts_and_post_images_empty_table"

    # Start with empty table
    managed_table_client.overwrite(
        empty_silver_table_for_inserts_and_post_images_df, table_name
    )

    # Verify table is empty
    assert spark_session.table(table_name).count() == 0

    # Act: Use the public merge_history method with mixed insert, update_preimage,
    # and update_postimage events
    managed_table_client.merge_history(
        source_df=cdc_changes_with_inserts_and_post_images_to_empty_table_df,
        target_table=table_name,
        key_columns=["user_id"],
    )

    # Assert: Verify the final state matches expected
    actual_df = spark_session.table(table_name).select(
        sorted(spark_session.table(table_name).columns)
    )

    # Create shorter alias for the long DataFrame name
    expected_state_df = (
        silver_final_state_after_inserts_and_post_images_to_empty_table_df
    )
    expected_df = expected_state_df.select(sorted(expected_state_df.columns))

    assert_dataframes_equal(actual_df, expected_df)

    # Additional assertions to verify specific behavior
    result_df = spark_session.table(table_name)

    # Verify we have the expected number of records
    assert result_df.count() == 4

    # Verify all records are active (since this is an empty table scenario)
    active_records = result_df.filter(col("is_active_record"))
    assert active_records.count() == 4

    # Verify all records have null effective_end_date (indicating they are active)
    null_end_date_records = result_df.filter(col("effective_end_date").isNull())
    assert null_end_date_records.count() == 4

    # Verify all records have the correct bronze_commit_version
    version_1_records = result_df.filter(col("bronze_commit_version") == 1)
    assert version_1_records.count() == 4

    # Verify we have the expected user IDs and their subscription tiers
    user_data = result_df.select("user_id", "subscription_tier").collect()
    user_tiers = {row.user_id: row.subscription_tier for row in user_data}

    assert "U001" in user_tiers and user_tiers["U001"] == "Gold"  # insert event
    assert "U002" in user_tiers and user_tiers["U002"] == "Platinum"  # insert event
    assert (
        "U003" in user_tiers and user_tiers["U003"] == "Premium"
    )  # update_postimage event (changed from Basic)
    assert (
        "U004" in user_tiers and user_tiers["U004"] == "Diamond"
    )  # update_postimage event (changed from Elite)

    # Verify the subscription tier changes reflect the postimage data, not preimage
    assert (
        user_tiers["U003"] == "Premium"
    )  # Should be the postimage value, not "Basic" from preimage
    assert (
        user_tiers["U004"] == "Diamond"
    )  # Should be the postimage value, not "Elite" from preimage


def test_convert_cdc_to_scd_records_with_inserts_and_post_images_to_empty_table(
    managed_table_client: ManagedTableClient,
    cdc_changes_with_inserts_and_post_images_to_empty_table_df: DataFrame,
) -> None:
    """
    Test the _convert_cdc_to_scd_records method specifically for the scenario
    where we have insert, update_preimage, and update_postimage events in the same
    CDC window.

    This test verifies that the method correctly processes all event types
    and assigns proper effective dates and active status.
    """
    result_df = managed_table_client._convert_cdc_to_scd_records(
        source_df=cdc_changes_with_inserts_and_post_images_to_empty_table_df,
        key_columns=["user_id"],
    )

    # Verify we get the expected number of records (only insert and update_postimage
    # events)
    assert result_df.count() == 4

    # Verify all records have the correct bronze_commit_version
    version_1_records = result_df.filter(col("bronze_commit_version") == 1)
    assert version_1_records.count() == 4

    # Verify all records are marked as active (since there are no subsequent events)
    active_records = result_df.filter(col("is_active_record"))
    assert active_records.count() == 4

    # Verify all records have null effective_end_date
    null_end_date_records = result_df.filter(col("effective_end_date").isNull())
    assert null_end_date_records.count() == 4

    # Verify the effective_start_dates match the commit timestamps
    result = (
        result_df.select(
            "user_id",
            "subscription_tier",
            "effective_start_date",
            "effective_end_date",
            "is_active_record",
        )
        .orderBy("effective_start_date")
        .collect()
    )

    assert len(result) == 4

    # Verify each record has the expected properties
    for row in result:
        assert row.effective_end_date is None
        assert row.is_active_record is True
        assert row.effective_start_date is not None

    # Since _change_type is dropped from the output, we can only verify the count
    # and that we have the expected number of records (insert and update_postimage
    # events)
    assert len(result) == 4  # Should have 4 records: 2 inserts + 2 update_postimages

    # Verify the subscription tiers reflect the postimage data
    user_tiers = {row.user_id: row.subscription_tier for row in result}
    assert user_tiers["U001"] == "Gold"  # insert event
    assert user_tiers["U002"] == "Platinum"  # insert event
    assert (
        user_tiers["U003"] == "Premium"
    )  # update_postimage event (not "Basic" from preimage)
    assert (
        user_tiers["U004"] == "Diamond"
    )  # update_postimage event (not "Elite" from preimage)


def test_get_oldest_record_with_preimages_to_empty_table(
    managed_table_client: ManagedTableClient,
    cdc_changes_with_inserts_and_post_images_to_empty_table_df: DataFrame,
) -> None:
    """
    Test the _get_oldest_record method specifically for the scenario
    where we have update_preimage events in the same CDC window as other events.

    This test verifies that the method correctly identifies preimage events
    that would be used to close out existing records (though in an empty table
    scenario, there are no existing records to close out).
    """
    # Debug: Print the input data
    print("=== Input CDC Data ===")
    cdc_changes_with_inserts_and_post_images_to_empty_table_df.show(truncate=False)

    result_df = managed_table_client._get_oldest_record(
        df=cdc_changes_with_inserts_and_post_images_to_empty_table_df,
        key_columns=["user_id"],
    )

    # Debug: Print the result
    print("=== Result from _get_oldest_record ===")
    result_df.show(truncate=False)

    # Verify we get the expected number of records (only update_preimage events)
    assert result_df.count() == 2

    # Verify all records have the correct bronze_commit_version
    version_1_records = result_df.filter(col("bronze_commit_version") == 1)
    assert version_1_records.count() == 2

    # Verify we have the expected user IDs (only those with update_preimage events)
    user_ids = [row.user_id for row in result_df.select("user_id").collect()]
    assert "U003" in user_ids  # update_preimage event
    assert "U004" in user_ids  # update_preimage event
    assert "U001" not in user_ids  # insert event - should not be included
    assert "U002" not in user_ids  # insert event - should not be included

    # Verify the commit timestamps are present and have the correct user IDs

    result_rows = result_df.collect()
    user_timestamps = {row.user_id: row._commit_timestamp for row in result_rows}

    # Verify we have timestamps for the expected users
    assert "U003" in user_timestamps, "Missing timestamp for U003"
    assert "U004" in user_timestamps, "Missing timestamp for U004"

    # Verify the timestamps are not None
    assert user_timestamps["U003"] is not None, "Timestamp for U003 is None"
    assert user_timestamps["U004"] is not None, "Timestamp for U004 is None"

    # Verify the timestamps are datetime objects
    assert isinstance(user_timestamps["U003"], dt), (
        f"Expected datetime for U003, got {type(user_timestamps['U003'])}"
    )
    assert isinstance(user_timestamps["U004"], dt), (
        f"Expected datetime for U004, got {type(user_timestamps['U004'])}"
    )


# endregion


# region: Delete Event Tests
def test_get_oldest_record_with_deletes(
    managed_table_client: ManagedTableClient,
    cdc_changes_with_delete_events_df: DataFrame,
    cdc_records_to_close_out_history_for_deletes_df: DataFrame,
) -> None:
    """
    Test that _get_oldest_record correctly identifies
    records to close out history when delete events occur.
    """
    prepared_records = managed_table_client._get_oldest_record(
        df=cdc_changes_with_delete_events_df,
        key_columns=["user_id"],
    )

    # Should return records for both deleted users
    assert prepared_records.count() == 2

    # Verify the content matches expected
    expected_df = cdc_records_to_close_out_history_for_deletes_df
    assert_dataframes_equal(prepared_records, expected_df)


def test_convert_cdc_to_scd_records_with_deletes_returns_empty(
    managed_table_client: ManagedTableClient,
    cdc_changes_with_delete_events_df: DataFrame,
) -> None:
    """
    Test that _convert_cdc_to_scd_records returns an empty DataFrame
    when only delete events are present, since deletes don't create new history entries.
    """
    prepared_records = managed_table_client._convert_cdc_to_scd_records(
        source_df=cdc_changes_with_delete_events_df,
        key_columns=["user_id"],
    )

    # Should return empty DataFrame since deletes don't create new history entries
    assert prepared_records.count() == 0


def test_merge_history_with_deletes_only(
    managed_table_client: ManagedTableClient,
    spark_session: SparkSession,
    initial_silver_table_with_active_records_df: DataFrame,
    cdc_changes_with_delete_events_df: DataFrame,
    silver_final_state_after_deletes_df: DataFrame,
) -> None:
    """
    Test the full merge_history workflow with delete events only.
    Verifies that delete events properly close out existing history records.
    """
    table_name = SILVER_DELETE_TEST_TABLE

    # Start with initial state
    managed_table_client.overwrite(
        initial_silver_table_with_active_records_df, table_name
    )

    # Act: Use the public merge_history method with delete events
    managed_table_client.merge_history(
        source_df=cdc_changes_with_delete_events_df,
        target_table=table_name,
        key_columns=["user_id"],
    )

    # Assert: Verify the final state matches expected
    actual_df = spark_session.table(table_name).select(
        sorted(spark_session.table(table_name).columns)
    )
    expected_df = silver_final_state_after_deletes_df.select(
        sorted(silver_final_state_after_deletes_df.columns)
    )

    assert_dataframes_equal(actual_df, expected_df)


def test_merge_history_with_mixed_events_including_deletes(
    managed_table_client: ManagedTableClient,
    spark_session: SparkSession,
    initial_silver_table_with_active_records_df: DataFrame,
    cdc_changes_with_mixed_events_including_deletes_df: DataFrame,
    silver_final_state_after_mixed_events_df: DataFrame,
) -> None:
    """
    Test the full merge_history workflow with mixed events including
    inserts, updates, and deletes in the same CDC window.
    """
    table_name = SILVER_MIXED_EVENTS_TEST_TABLE

    # Start with initial state
    managed_table_client.overwrite(
        initial_silver_table_with_active_records_df, table_name
    )

    # Act: Use the public merge_history method with mixed events
    managed_table_client.merge_history(
        source_df=cdc_changes_with_mixed_events_including_deletes_df,
        target_table=table_name,
        key_columns=["user_id"],
    )

    # Assert: Verify the final state matches expected
    actual_df = spark_session.table(table_name).select(
        sorted(spark_session.table(table_name).columns)
    )
    expected_df = silver_final_state_after_mixed_events_df.select(
        sorted(silver_final_state_after_mixed_events_df.columns)
    )

    assert_dataframes_equal(actual_df, expected_df)


def test_delete_events_priority_in_window_ordering(
    managed_table_client: ManagedTableClient,
    spark_session: SparkSession,
) -> None:
    """
    Test that delete events are given priority over update_preimage events
    in the window ordering when both exist for the same natural key.
    """
    # Create CDC data with both delete and update_preimage for same user
    data = [
        (
            "U001",
            "Alice Johnson",
            "Gold",
            "update_preimage",
            dt.fromisoformat("2025-08-01T10:00:00"),
            1,
        ),
        (
            "U001",
            "Alice Johnson",
            "Gold",
            "delete",
            dt.fromisoformat("2025-08-01T10:00:00"),
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

    cdc_df = spark_session.createDataFrame(data, schema=schema)

    # The delete event should be selected due to priority ordering
    prepared_records = managed_table_client._get_oldest_record(
        df=cdc_df,
        key_columns=["user_id"],
    )

    # Should return only one record (the delete event)
    assert prepared_records.count() == 1

    # Verify it's the delete event
    result = prepared_records.collect()[0]
    assert result.user_id == "U001"
    assert result._commit_timestamp == dt.fromisoformat("2025-08-01T10:00:00")


def test_delete_events_with_multiple_natural_keys(
    managed_table_client: ManagedTableClient,
    spark_session: SparkSession,
) -> None:
    """
    Test delete events with multiple natural key columns.
    """
    # Create CDC data with multiple natural keys
    data = [
        (
            "U001",
            "A",
            "Gold",
            "delete",
            dt.fromisoformat("2025-08-01T10:00:00.000+00:00"),
            1,
        ),
        (
            "U002",
            "B",
            "Platinum",
            "delete",
            dt.fromisoformat("2025-08-01T11:00:00.000+00:00"),
            2,
        ),
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("location_id", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("_change_type", StringType(), True),
        StructField("_commit_timestamp", TimestampType(), True),
        StructField("_commit_version", LongType(), True),
    ])

    cdc_df = spark_session.createDataFrame(data, schema=schema)

    # Test with multiple natural keys
    prepared_records = managed_table_client._get_oldest_record(
        df=cdc_df,
        key_columns=["user_id", "location_id"],
    )

    # Should return records for both composite keys
    assert prepared_records.count() == 2

    # Verify both records are present
    results = prepared_records.collect()
    user_location_pairs = [(row.user_id, row.location_id) for row in results]
    assert ("U001", "A") in user_location_pairs
    assert ("U002", "B") in user_location_pairs


# endregion


# region: Delete Events with Multiple Natural Keys Tests
def test_merge_history_with_deletes_multiple_natural_keys(
    managed_table_client: ManagedTableClient,
    spark_session: SparkSession,
    initial_silver_table_with_multiple_natural_keys_for_deletes_df: DataFrame,
    cdc_changes_with_deletes_multiple_natural_keys_df: DataFrame,
    silver_final_state_after_deletes_multiple_natural_keys_df: DataFrame,
) -> None:
    """
    Test the public merge_history method with multiple natural keys (composite keys)
    for delete events. This test verifies that the SCD2 merge workflow correctly
    handles delete events for records identified by multiple columns
    (e.g., user_id + club_id).
    """
    table_name = SILVER_DELETE_MULTIPLE_KEYS_TEST_TABLE

    # Start with initial state
    managed_table_client.overwrite(
        initial_silver_table_with_multiple_natural_keys_for_deletes_df, table_name
    )

    # Act: Use merge_history method with multiple natural keys and delete events
    managed_table_client.merge_history(
        source_df=cdc_changes_with_deletes_multiple_natural_keys_df,
        target_table=table_name,
        key_columns=["user_id", "club_id"],
    )

    # Assert: Verify the final state matches expected
    actual_df = spark_session.table(table_name).select(
        sorted(spark_session.table(table_name).columns)
    )
    expected_df = silver_final_state_after_deletes_multiple_natural_keys_df.select(
        sorted(silver_final_state_after_deletes_multiple_natural_keys_df.columns)
    )

    assert_dataframes_equal(actual_df, expected_df)


def test_get_oldest_record_with_deletes_multiple_natural_keys(
    managed_table_client: ManagedTableClient,
    cdc_changes_with_deletes_multiple_natural_keys_df: DataFrame,
    cdc_records_to_close_out_history_for_deletes_multiple_natural_keys_df: DataFrame,
) -> None:
    """
    Test that _get_oldest_record correctly identifies
    records to close out history when delete events occur for multiple natural keys.
    """
    prepared_records = managed_table_client._get_oldest_record(
        df=cdc_changes_with_deletes_multiple_natural_keys_df,
        key_columns=["user_id", "club_id"],
    )

    # Should return records for both deleted composite keys
    assert prepared_records.count() == 2

    # Verify the content matches expected
    expected_df = cdc_records_to_close_out_history_for_deletes_multiple_natural_keys_df
    assert_dataframes_equal(prepared_records, expected_df)


def test_convert_cdc_to_scd_records_with_deletes_multiple_natural_keys_returns_empty(
    managed_table_client: ManagedTableClient,
    cdc_changes_with_deletes_multiple_natural_keys_df: DataFrame,
) -> None:
    """
    Test that _convert_cdc_to_scd_records returns an empty DataFrame
    when only delete events are present for multiple natural keys,
    since deletes don't create new history entries.
    """
    prepared_records = managed_table_client._convert_cdc_to_scd_records(
        source_df=cdc_changes_with_deletes_multiple_natural_keys_df,
        key_columns=["user_id", "club_id"],
    )

    # Should return empty DataFrame since deletes don't create new history entries
    assert prepared_records.count() == 0


def test_merge_history_with_mixed_events_including_deletes_multiple_natural_keys(
    managed_table_client: ManagedTableClient,
    spark_session: SparkSession,
    initial_silver_table_with_multiple_natural_keys_for_deletes_df: DataFrame,
    cdc_changes_with_mixed_events_including_deletes_multiple_natural_keys_df: DataFrame,
    silver_final_state_after_mixed_events_multiple_natural_keys_df: DataFrame,
) -> None:
    """
    Test the full merge_history workflow with mixed events including
    inserts, updates, and deletes for multiple natural keys in the same CDC window.
    """
    table_name = SILVER_MIXED_EVENTS_MULTIPLE_KEYS_TEST_TABLE

    # Start with initial state
    managed_table_client.overwrite(
        initial_silver_table_with_multiple_natural_keys_for_deletes_df, table_name
    )

    # Act: Use merge_history method with mixed events and multiple natural keys
    managed_table_client.merge_history(
        source_df=cdc_changes_with_mixed_events_including_deletes_multiple_natural_keys_df,
        target_table=table_name,
        key_columns=["user_id", "club_id"],
    )

    # Assert: Verify the final state matches expected
    actual_df = spark_session.table(table_name).select(
        sorted(spark_session.table(table_name).columns)
    )
    expected_df = silver_final_state_after_mixed_events_multiple_natural_keys_df.select(
        sorted(silver_final_state_after_mixed_events_multiple_natural_keys_df.columns)
    )

    assert_dataframes_equal(actual_df, expected_df)


# endregion
