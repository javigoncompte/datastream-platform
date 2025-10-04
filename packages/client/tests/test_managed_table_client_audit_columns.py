from datetime import datetime

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
from dataplatform.client.managed_table_client import ManagedTableClient
from dataplatform.core.spark_session import SparkSession

PRICING_DAILY_TABLE = "sandbox.tests.pricing_daily_merge_test4"
PRICING_BASE_DAILY_TABLE = "sandbox.tests.pricing_base_daily_merge_test4"
BACKFILL_TIMESTAMP = datetime(1900, 1, 1, 0, 0, 0)


@pytest.fixture(autouse=True)
def _set_tz(spark_session):
    spark_session.conf.set("spark.sql.session.timeZone", "America/Denver")
    yield


def test_merge_adds_audit_columns(
    managed_table_client: ManagedTableClient,
    spark_session: SparkSession,
    pricing_base_daily_seed_df: DataFrame,
    pricing_base_daily_source_df: DataFrame,
) -> None:
    """Schema should gain created_date and last_modified_date after the first merge."""
    managed_table_client.overwrite(pricing_base_daily_seed_df, PRICING_BASE_DAILY_TABLE)

    before_cols = {
        f.name for f in spark_session.table(PRICING_BASE_DAILY_TABLE).schema.fields
    }
    assert "created_date" not in before_cols and "last_modified_date" not in before_cols

    managed_table_client.merge(
        source_df=pricing_base_daily_source_df,
        target_table=PRICING_BASE_DAILY_TABLE,
        key_columns=["club_id", "membership_type", "date"],
    )

    after_cols = {
        f.name for f in spark_session.table(PRICING_BASE_DAILY_TABLE).schema.fields
    }
    assert {"created_date", "last_modified_date"}.issubset(after_cols)


def test_existing_row_backfilled_to_fallback(
    managed_table_client: ManagedTableClient,
    spark_session: SparkSession,
    pricing_base_daily_seed_df: DataFrame,
    pricing_base_daily_source_df: DataFrame,
) -> None:
    """
    Pre-existing row (from seed) should get created_date backfilled to 1900-01-01
    after merge if it was NULL/missing. last_modified_date should be non-null.
    """

    spark_session.conf.set("spark.sql.session.timeZone", "America/Denver")
    key_date = datetime.fromisoformat("2025-08-13T13:00:00.000+00:00")

    managed_table_client.overwrite(pricing_base_daily_seed_df, PRICING_BASE_DAILY_TABLE)

    managed_table_client.merge(
        source_df=pricing_base_daily_source_df,
        target_table=PRICING_BASE_DAILY_TABLE,
        key_columns=["club_id", "membership_type", "date"],
    )

    df_after = spark_session.table(PRICING_BASE_DAILY_TABLE)
    existing_row = (
        df_after.filter(
            (col("club_id") == "1")
            & (col("membership_type") == "Premium")
            & (col("date") == lit(key_date))
        )
        .select("created_date", "last_modified_date")
        .collect()
    )
    assert len(existing_row) == 1
    created_existing = existing_row[0]["created_date"]
    modified_existing = existing_row[0]["last_modified_date"]

    assert created_existing is not None, (
        "Expected created_date to be populated on existing row"
    )
    # Compare as naive to avoid tz mismatch in environments that attach tzinfo
    assert created_existing.replace(tzinfo=None) == BACKFILL_TIMESTAMP, (
        f"Expected created_date to be {BACKFILL_TIMESTAMP}, got {created_existing}"
    )
    assert modified_existing is not None, (
        "Expected last_modified_date to be populated on existing row"
    )


def test_inserted_row_has_equal_non_null_audit_timestamps(
    managed_table_client: ManagedTableClient,
    spark_session: SparkSession,
    pricing_base_daily_seed_df: DataFrame,
    pricing_base_daily_source_df: DataFrame,
) -> None:
    """
    Newly inserted row should have non-null audit columns and
    created_date ~= last_modified_date at insert time.
    """
    key_date = datetime.fromisoformat("2025-08-13T13:00:00.000+00:00")

    managed_table_client.overwrite(pricing_base_daily_seed_df, PRICING_BASE_DAILY_TABLE)

    managed_table_client.merge(
        source_df=pricing_base_daily_source_df,
        target_table=PRICING_BASE_DAILY_TABLE,
        key_columns=["club_id", "membership_type", "date"],
    )

    df_after = spark_session.table(PRICING_BASE_DAILY_TABLE)
    inserted_row = (
        df_after.filter(
            (col("club_id") == "3")
            & (col("membership_type") == "Elite")
            & (col("date") == lit(key_date))
        )
        .select("created_date", "last_modified_date")
        .collect()
    )
    assert len(inserted_row) == 1
    created_inserted = inserted_row[0]["created_date"]
    modified_inserted = inserted_row[0]["last_modified_date"]

    assert created_inserted is not None and modified_inserted is not None
    delta_seconds = abs((modified_inserted - created_inserted).total_seconds())
    assert delta_seconds < 1.0, (
        f"Expected created_date == last_modified_date on insert; delta={delta_seconds}s"
    )
