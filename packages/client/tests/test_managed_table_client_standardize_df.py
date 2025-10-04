"""
Test suite for ManagedTableClient._standardize_history_df method.

This module tests the standardization of DataFrames to include SCD Type 2 columns:
- effective_start_date: ISO 8601 timestamp for when the record becomes active
- effective_end_date: None for active records (set when record becomes inactive)
- is_active_record: Boolean flag indicating if the record is currently active
- bronze_commit_version: Version number from the source table's transaction history
"""

import re
from datetime import datetime

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from dataplatform.client.managed_table_client import ManagedTableClient

# Test table names used throughout the test suite
SOURCE_TABLE = (
    "sandbox.tests.standardize_df_source_table"  # Single source table with history
)
RESULT_TABLE_WITHOUT_CDC = (
    "sandbox.tests.result_df_without_cdc_columns"  # Results when CDC columns added
)
RESULT_TABLE_WITH_CDC = (
    "sandbox.tests.result_df_with_cdc_columns"  # Results when CDC columns already exist
)
RESULT_TABLE_PRESERVE_DATA = (
    "sandbox.tests.result_df_preserve_data"  # Results showing data preservation
)
RESULT_TABLE_EMPTY_DF = (
    "sandbox.tests.result_df_empty_dataframe"  # Results with empty DataFrame
)


class TestStandardizeHistoryDF:
    """Test cases for the _standardize_history_df method."""

    @pytest.fixture(scope="class")
    def source_table_with_history(self, spark_session):
        """
        Class-level fixture that creates a single source table with version history.
        This table will be used across all tests to simulate a real source table.
        """
        # Ensure clean slate - drop table if it exists from previous runs
        spark_session.sql(f"DROP TABLE IF EXISTS {SOURCE_TABLE}")

        # Create initial table (version 0)
        initial_data = [
            ("S001", "Source User 1", "Basic"),
            ("S002", "Source User 2", "Standard"),
        ]
        initial_schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("user_name", StringType(), True),
            StructField("subscription_tier", StringType(), True),
        ])
        initial_df = spark_session.createDataFrame(initial_data, schema=initial_schema)
        initial_df.write.format("delta").mode("overwrite").saveAsTable(SOURCE_TABLE)

        # Insert more data (version 1)
        insert_data = [("S003", "Source User 3", "Premium")]
        insert_df = spark_session.createDataFrame(insert_data, schema=initial_schema)
        insert_df.write.format("delta").mode("append").saveAsTable(SOURCE_TABLE)

        # Update data (version 2)
        spark_session.sql(f"""
            UPDATE {SOURCE_TABLE}
            SET subscription_tier = 'Gold'
            WHERE user_id = 'S001'
        """)

        # Delete data (version 3) - this should be the max version
        spark_session.sql(f"""
            DELETE FROM {SOURCE_TABLE}
            WHERE user_id = 'S002'
        """)

        # Debug: Check actual max version after setup
        max_version_row = spark_session.sql(f"""
            SELECT max(version) as max_version
            FROM (DESCRIBE HISTORY {SOURCE_TABLE})
        """).collect()[0]
        actual_max_version = max_version_row["max_version"]
        print(f"Source table setup complete. Max version: {actual_max_version}")

        yield SOURCE_TABLE

        print("CLEANUP DISABLED - Tables will persist for inspection")

    @pytest.fixture
    def df_without_cdc_columns(self, spark_session) -> DataFrame:
        """
        Fixture that creates a DataFrame without any CDC columns.
        This represents raw data that needs CDC columns added.
        """
        data = [
            ("U001", "Alice Johnson", "Gold"),
            ("U002", "Bob Smith", "Platinum"),
        ]
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("user_name", StringType(), True),
            StructField("subscription_tier", StringType(), True),
        ])
        return spark_session.createDataFrame(data, schema=schema)

    @pytest.fixture
    def df_with_all_cdc_columns(self, spark_session) -> DataFrame:
        """
        Fixture that creates a DataFrame with all CDC columns already present.
        This represents data that should be returned unchanged.
        """
        data = [
            (
                "U001",
                "Alice Johnson",
                "Gold",
                datetime.fromisoformat("2025-01-15T10:30:45.000+00:00"),
                None,  # effective_end_date
                True,  # is_active_record
                5,  # bronze_commit_version
            ),
            (
                "U002",
                "Bob Smith",
                "Platinum",
                datetime.fromisoformat("2025-01-15T10:30:45.000+00:00"),
                None,  # effective_end_date
                True,  # is_active_record
                5,  # bronze_commit_version
            ),
        ]
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("user_name", StringType(), True),
            StructField("subscription_tier", StringType(), True),
            StructField("effective_start_date", TimestampType(), True),
            StructField("effective_end_date", TimestampType(), True),
            StructField("is_active_record", BooleanType(), True),
            StructField("bronze_commit_version", IntegerType(), True),
        ])
        return spark_session.createDataFrame(data, schema=schema)

    def test_iso8601_timestamp_format(
        self,
        managed_table_client: ManagedTableClient,
    ):
        """
        Test that _get_iso8601_timestamp generates proper ISO 8601 format.

        The timestamp should match the format: YYYY-MM-DDTHH:MM:SS.000+00:00
        This is used for the effective_start_date column.
        """
        timestamp = managed_table_client._get_iso8601_timestamp()

        # Regex pattern for ISO 8601 format with milliseconds and timezone
        iso8601_pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}\+00:00$"

        assert re.match(iso8601_pattern, timestamp), (
            f"Timestamp '{timestamp}' does not match ISO 8601 format"
        )

        # Verify it can be parsed as datetime
        parsed_datetime = datetime.fromisoformat(timestamp)
        assert parsed_datetime is not None

        # Verify timezone is UTC
        assert parsed_datetime.tzinfo is not None
        # When parsing ISO 8601 with +00:00, Python represents it as 'UTC'
        assert (
            str(parsed_datetime.tzinfo) == "UTC"
            or str(parsed_datetime.tzinfo) == "UTC+00:00"
        )
        # Alternative: check the UTC offset is zero
        utc_offset = parsed_datetime.utcoffset()
        assert utc_offset is not None and utc_offset.total_seconds() == 0

    def test_standardize_df_without_cdc_columns(
        self,
        managed_table_client: ManagedTableClient,
        df_without_cdc_columns: DataFrame,
        source_table_with_history: str,
        spark_session,
    ):
        """
        Test that a DataFrame without CDC columns gets all CDC columns added.

        This test verifies:
        1. All four CDC columns are added (effective_start_date,
                    effective_end_date, is_active_record, bronze_commit_version)
        2. effective_start_date is set to current timestamp
        3. effective_end_date is set to None
        4. is_active_record is set to True
        5. bronze_commit_version comes from history lookup using source table
        6. Results are written to table for visual inspection
        """
        # Test the standardization using the source table with history
        result_df = managed_table_client._standardize_history_df(
            df_without_cdc_columns, source_table_with_history
        )

        # First, check what the actual max version is
        max_version = managed_table_client._get_max_version_from_history(
            source_table_with_history
        )
        print(f"Max version retrieved by _get_max_version_from_history: {max_version}")

        # Show the actual history for debugging
        history_df = spark_session.sql(f"DESCRIBE HISTORY {source_table_with_history}")
        print("Table history:")
        history_df.select("version", "operation", "timestamp").show()

        # Debug: Show DataFrame schemas and data before writing
        print(f"Original DataFrame columns: {df_without_cdc_columns.columns}")
        print(f"Result DataFrame columns: {result_df.columns}")
        print("Result DataFrame schema:")
        result_df.printSchema()
        print("Result DataFrame sample data:")
        result_df.show(truncate=False)

        # Write results to table for visual inspection
        spark_session.sql(f"DROP TABLE IF EXISTS {RESULT_TABLE_WITHOUT_CDC}")
        result_df.write.format("delta").mode("overwrite").saveAsTable(
            RESULT_TABLE_WITHOUT_CDC
        )
        print(f"Table written to: {RESULT_TABLE_WITHOUT_CDC}")

        # Check if ManagedTableClient would modify this table name
        actual_table_name = managed_table_client._get_table_name(
            RESULT_TABLE_WITHOUT_CDC
        )
        print(f"ManagedTableClient would use table name: {actual_table_name}")

        # Verify what was actually written to the table
        written_df = spark_session.table(RESULT_TABLE_WITHOUT_CDC)
        print(f"Columns in written table: {written_df.columns}")
        print("Data in written table:")
        written_df.show(truncate=False)

        # Verify all CDC columns are present
        expected_columns = {
            "user_id",
            "user_name",
            "subscription_tier",
            "effective_start_date",
            "effective_end_date",
            "is_active_record",
            "bronze_commit_version",
        }
        assert set(result_df.columns) == expected_columns

        # Collect results for detailed verification
        results = result_df.collect()
        assert len(results) == 2

        # Verify first row
        first_row = results[0]
        assert first_row["user_id"] == "U001"
        assert first_row["user_name"] == "Alice Johnson"
        assert first_row["subscription_tier"] == "Gold"
        assert (
            first_row["bronze_commit_version"] == max_version
        )  # Should be max version from source table history
        assert first_row["is_active_record"] is True
        assert first_row["effective_start_date"] is not None
        assert first_row["effective_end_date"] is None

        # Verify second row has same CDC values
        second_row = results[1]
        assert second_row["bronze_commit_version"] == max_version
        assert second_row["is_active_record"] is True
        assert second_row["effective_start_date"] is not None
        assert second_row["effective_end_date"] is None

        print(
            f"Results written to {RESULT_TABLE_WITHOUT_CDC} - "
            f"DataFrame WITHOUT CDC columns got CDC columns added"
        )

    def test_standardize_df_with_all_cdc_columns(
        self,
        managed_table_client: ManagedTableClient,
        df_with_all_cdc_columns: DataFrame,
        spark_session,
    ):
        """
        Test that a DataFrame with all CDC columns is returned unchanged.

        This test verifies:
        1. DataFrame is returned as-is when all CDC columns exist
                (effective_start_date, effective_end_date,
                is_active_record, bronze_commit_version)
        2. No modifications are made to existing data
        3. Column count and content remain the same
        4. No expensive table history lookup is performed (early return)
        5. Results are written to table for visual inspection
        """
        original_columns = df_with_all_cdc_columns.columns
        original_count = df_with_all_cdc_columns.count()
        original_data = df_with_all_cdc_columns.collect()

        # Since DataFrame has CDC columns, no table lookup should occur
        # We can pass any table name - it shouldn't be used
        result_df = managed_table_client._standardize_history_df(
            df_with_all_cdc_columns,
            "non_existent_table",  # This proves no table lookup happens
        )

        # Write results to table for visual inspection
        spark_session.sql(f"DROP TABLE IF EXISTS {RESULT_TABLE_WITH_CDC}")
        result_df.write.format("delta").mode("overwrite").saveAsTable(
            RESULT_TABLE_WITH_CDC
        )

        # Verify DataFrame is unchanged
        assert result_df.columns == original_columns
        assert result_df.count() == original_count

        result_data = result_df.collect()
        assert len(result_data) == len(original_data)

        # Verify data content is unchanged
        for original_row, result_row in zip(original_data, result_data, strict=False):
            assert original_row == result_row

        print(
            f"Results written to {RESULT_TABLE_WITH_CDC} - "
            f"DataFrame WITH CDC columns returned unchanged"
        )

    def test_cdc_columns_exist_method(
        self,
        managed_table_client: ManagedTableClient,
        df_without_cdc_columns: DataFrame,
        df_with_all_cdc_columns: DataFrame,
    ):
        """
        Test the _cdc_columns_exist helper method.

        This test verifies:
        1. Returns False when CDC columns are missing
        2. Returns True when all CDC columns are present
        """
        # Test with DataFrame without CDC columns
        assert not managed_table_client._cdc_columns_exist(df_without_cdc_columns)

        # Test with DataFrame with all CDC columns
        assert managed_table_client._cdc_columns_exist(df_with_all_cdc_columns)

    def test_cdc_columns_exist_with_partial_columns(
        self,
        managed_table_client: ManagedTableClient,
        spark_session,
    ):
        """
        Test _cdc_columns_exist with partial CDC columns.

        This verifies the method returns False when only some CDC columns exist.
        The method checks for: effective_start_date, effective_end_date,
                                is_active_record, bronze_commit_version.
        Note: We don't test _standardize_history_df with partial columns as
        noted in the user requirements - the pipeline would break in this case.
        """
        # DataFrame with only some CDC columns
        data = [("U001", "Alice", "Gold", 5)]
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("user_name", StringType(), True),
            StructField("subscription_tier", StringType(), True),
            StructField(
                "bronze_commit_version", IntegerType(), True
            ),  # Only one CDC column
        ])
        df_partial = spark_session.createDataFrame(data, schema=schema)

        # Should return False since not all CDC columns are present
        assert not managed_table_client._cdc_columns_exist(df_partial)

    def test_standardize_df_preserves_original_data(
        self,
        managed_table_client: ManagedTableClient,
        df_without_cdc_columns: DataFrame,
        source_table_with_history: str,
        spark_session,
    ):
        """
        Test that standardization preserves all original data columns and values.

        This test verifies:
        1. Original columns are preserved
        2. Original data values are unchanged
        3. Only CDC columns are added (effective_start_date, effective_end_date,
                                    is_active_record, bronze_commit_version)
        4. Results are written to table for visual inspection
        """
        original_data = df_without_cdc_columns.collect()

        result_df = managed_table_client._standardize_history_df(
            df_without_cdc_columns, source_table_with_history
        )

        # Write results to table for visual inspection
        spark_session.sql(f"DROP TABLE IF EXISTS {RESULT_TABLE_PRESERVE_DATA}")
        result_df.write.format("delta").mode("overwrite").saveAsTable(
            RESULT_TABLE_PRESERVE_DATA
        )

        result_data = result_df.collect()

        # Get the expected version from the source table
        expected_version = managed_table_client._get_max_version_from_history(
            source_table_with_history
        )

        # Verify original data is preserved
        for _i, (original_row, result_row) in enumerate(
            zip(original_data, result_data, strict=False)
        ):
            assert result_row["user_id"] == original_row["user_id"]
            assert result_row["user_name"] == original_row["user_name"]
            assert result_row["subscription_tier"] == original_row["subscription_tier"]

            # Verify CDC columns were added with correct version from source
            # table history
            assert result_row["bronze_commit_version"] == expected_version
            assert result_row["is_active_record"] is True
            assert result_row["effective_start_date"] is not None
            assert result_row["effective_end_date"] is None

        print(
            f"Results written to {RESULT_TABLE_PRESERVE_DATA} - "
            f"Original data preserved, CDC columns added"
        )

    def test_standardize_df_with_empty_dataframe(
        self,
        managed_table_client: ManagedTableClient,
        source_table_with_history: str,
        spark_session,
    ):
        """
        Test standardization with an empty DataFrame.

        This test verifies:
        1. Empty DataFrames are handled gracefully
        2. Schema is updated correctly even with no data
        3. Results are written to table for visual inspection
        """
        # Create empty DataFrame without CDC columns (same schema as our test fixtures)
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("user_name", StringType(), True),
            StructField("subscription_tier", StringType(), True),
        ])
        empty_df = spark_session.createDataFrame([], schema=schema)

        result_df = managed_table_client._standardize_history_df(
            empty_df, source_table_with_history
        )

        # Write results to table for visual inspection
        spark_session.sql(f"DROP TABLE IF EXISTS {RESULT_TABLE_EMPTY_DF}")
        result_df.write.format("delta").mode("overwrite").saveAsTable(
            RESULT_TABLE_EMPTY_DF
        )

        # Verify CDC columns are added to schema
        expected_columns = {
            "user_id",
            "user_name",
            "subscription_tier",
            "effective_start_date",
            "effective_end_date",
            "is_active_record",
            "bronze_commit_version",
        }
        assert set(result_df.columns) == expected_columns

        # Verify DataFrame is still empty
        assert result_df.count() == 0

        print(
            f"Results written to {RESULT_TABLE_EMPTY_DF} - "
            f"Empty DataFrame got CDC columns added to schema"
        )
