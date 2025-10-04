import time
from datetime import datetime
from typing import Any, Dict

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col, lit
from dataplatform.client.managed_table_client import ManagedTableClient
from dataplatform.core.spark_session import SparkSession

# If merge() is a method on ManagedTableClient, we’ll call client.merge(...)
# using your existing managed_table_client fixture.

PRICING_DAILY_TABLE = "sandbox.tests.pricing_daily_merge"
PRICING_BASE_DAILY_TABLE = "sandbox.tests.pricing_base_daily_merge"


def _assert_row(df: DataFrame, where: Column, expected: Dict[str, Any]) -> None:
    filtered_df = df.filter(where)
    count = filtered_df.count()
    assert count == 1, f"Expected 1 row for condition {where}, got {count}"

    pdf = filtered_df.toPandas()  # materializes just the one row
    row = pdf.iloc[0]

    for k, v in expected.items():
        actual = row[k]
        assert str(actual) == str(v), f"{k}: expected {v}, got {actual}"


def test_merge_updates_and_inserts_base_daily(
    managed_table_client: "ManagedTableClient",
    spark_session: "SparkSession",
    pricing_base_daily_seed_df: DataFrame,
    pricing_base_daily_source_df: DataFrame,
) -> None:
    managed_table_client.overwrite(
        pricing_base_daily_seed_df,
        PRICING_BASE_DAILY_TABLE,
    )

    before_df = spark_session.table(PRICING_BASE_DAILY_TABLE)

    print("Seed row count:", spark_session.table(PRICING_BASE_DAILY_TABLE).count())

    assert before_df.count() == 1
    managed_table_client.merge(
        source_df=pricing_base_daily_source_df,
        target_table=PRICING_BASE_DAILY_TABLE,
        key_columns=["club_id", "membership_type", "date"],
    )

    after_df = spark_session.table(PRICING_BASE_DAILY_TABLE)

    print("Merged row count:", after_df.count())
    after_df.show()

    assert after_df.count() == 2

    _assert_row(
        after_df,
        (col("club_id") == "1")
        & (col("membership_type") == "Premium")
        & (col("date") == lit(datetime.fromisoformat("2025-08-13T13:00:00.000+00:00"))),
        {
            "enrollment_fee_default": "15.00",
            "monthly_rate_default": "35.00",
            "days_free_default": 7,
        },
    )

    _assert_row(
        after_df,
        (col("club_id") == "3")
        & (col("membership_type") == "Elite")
        & (col("date") == lit(datetime.fromisoformat("2025-08-13T13:00:00.000+00:00"))),
        {
            "enrollment_fee_default": "99.00",
            "monthly_rate_default": "199.00",
            "days_free_default": 30,
        },
    )


def test_merge_idempotent_base_daily(
    managed_table_client: ManagedTableClient,
    spark_session: SparkSession,
    pricing_base_daily_seed_df: DataFrame,
    pricing_base_daily_source_df: DataFrame,
) -> None:
    managed_table_client.overwrite(pricing_base_daily_seed_df, PRICING_BASE_DAILY_TABLE)

    managed_table_client.merge(
        source_df=pricing_base_daily_source_df,
        target_table=PRICING_BASE_DAILY_TABLE,
        key_columns=["club_id", "membership_type", "date"],
    )

    assert spark_session.table(PRICING_BASE_DAILY_TABLE).count() == 2

    managed_table_client.merge(
        source_df=pricing_base_daily_source_df,
        target_table=PRICING_BASE_DAILY_TABLE,
        key_columns=["club_id", "membership_type", "date"],
    )

    assert spark_session.table(PRICING_BASE_DAILY_TABLE).count() == 2


def test_merge_into_empty_table_inserts_all_rows(
    managed_table_client: ManagedTableClient,
    spark_session: SparkSession,
    pricing_daily_empty_target_df: DataFrame,
    pricing_daily_source_df: DataFrame,
) -> None:
    managed_table_client.overwrite(pricing_daily_empty_target_df, PRICING_DAILY_TABLE)

    assert spark_session.table(PRICING_DAILY_TABLE).count() == 0

    managed_table_client.merge(
        source_df=pricing_daily_source_df,
        target_table=PRICING_DAILY_TABLE,
        key_columns=["club_id", "membership_type", "date"],
    )

    after_df = spark_session.table(PRICING_DAILY_TABLE)
    assert after_df.count() == pricing_daily_source_df.count()

    assert (
        after_df.filter(
            (col("club_id") == "10")
            & (col("membership_type") == "Gold")
            & (
                col("date")
                == lit(datetime.fromisoformat("2025-08-13T13:00:00.000+00:00"))
            )
        ).count()
        == 1
    )


def test_merge_adds_audit_cols_and_insert_semantics(
    managed_table_client: ManagedTableClient,
    spark_session: SparkSession,
    pricing_base_daily_seed_df: DataFrame,
    pricing_base_daily_source_df: DataFrame,
) -> None:
    """
    First merge should:
      - Add created_date / last_modified_date if missing.
      - UPDATE the seeded 'today' row (fix business field).
      - INSERT the 'tomorrow' row with created_date == last_modified_date.
    """
    # Start with a table that has ONLY business columns
    managed_table_client.overwrite(pricing_base_daily_seed_df, PRICING_BASE_DAILY_TABLE)

    before_cols = {
        f.name for f in spark_session.table(PRICING_BASE_DAILY_TABLE).schema.fields
    }
    assert "created_date" not in before_cols and "last_modified_date" not in before_cols

    # Merge (upsert)
    managed_table_client.merge(
        source_df=pricing_base_daily_source_df,
        target_table=PRICING_BASE_DAILY_TABLE,
        key_columns=["club_id", "membership_type", "date"],
    )

    df_after = spark_session.table(PRICING_BASE_DAILY_TABLE)
    after_cols = {f.name for f in df_after.schema.fields}
    assert {"created_date", "last_modified_date"}.issubset(after_cols)

    # Grab the inserted row (club_id='3', Elite, 2025-08-13 in your sample fixtures)
    inserted = (
        df_after.filter(
            (col("club_id") == "3")
            & (col("membership_type") == "Elite")
            & (
                col("date")
                == lit(datetime.fromisoformat("2025-08-13T13:00:00.000+00:00"))
            )
        )
        .select("created_date", "last_modified_date")
        .collect()
    )

    assert len(inserted) == 1
    ins = inserted[0]

    # On INSERT: created_date == last_modified_date (allow sub-second jitter)
    assert ins["created_date"] is not None and ins["last_modified_date"] is not None
    diff = abs((ins["last_modified_date"] - ins["created_date"]).total_seconds())
    assert diff < 1.0, f"Expected created_date == last_modified_date; delta={diff}s"


def test_merge_updates_bump_last_modified_only(
    managed_table_client: ManagedTableClient,
    spark_session: SparkSession,
    pricing_base_daily_seed_df: DataFrame,
    pricing_base_daily_source_df: DataFrame,
) -> None:
    # 1) Start clean: seed a WRONG row so the first merge must UPDATE it
    managed_table_client.overwrite(pricing_base_daily_seed_df, PRICING_BASE_DAILY_TABLE)

    # First merge (fix business value) ->
    #   UPDATE for seeded row, INSERT for any missing keys
    managed_table_client.merge(
        source_df=pricing_base_daily_source_df,
        target_table=PRICING_BASE_DAILY_TABLE,
        key_columns=["club_id", "membership_type", "date"],
    )

    df1 = spark_session.table(PRICING_BASE_DAILY_TABLE)

    # Grab the UPDATED row (your seed/source fixtures’ “Premium” example)
    key_date = datetime.fromisoformat("2025-08-13T13:00:00.000+00:00")
    r1 = (
        df1.filter(
            (col("club_id") == "1")
            & (col("membership_type") == "Premium")
            & (col("date") == lit(key_date))
        )
        .select("monthly_rate_default", "created_date", "last_modified_date")
        .collect()[0]
    )

    # On first UPDATE:
    # - last_modified_date must be set
    # - created_date may be NULL (no backfill) or
    #       set if you implemented the COALESCE fix
    created_1, modified_1 = r1["created_date"], r1["last_modified_date"]
    assert modified_1 is not None

    # 2) Now force a second UPDATE (change a business value for the same key)
    time.sleep(1.1)  # ensure timestamps are strictly increasing
    src2 = pricing_base_daily_source_df.withColumn(
        "monthly_rate_default", lit("18.99").cast("decimal(10,2)")
    )
    managed_table_client.merge(
        source_df=src2,
        target_table=PRICING_BASE_DAILY_TABLE,
        key_columns=["club_id", "membership_type", "date"],
    )

    df2 = spark_session.table(PRICING_BASE_DAILY_TABLE)
    r2 = (
        df2.filter(
            (col("club_id") == "1")
            & (col("membership_type") == "Premium")
            & (col("date") == lit(key_date))
        )
        .select("monthly_rate_default", "created_date", "last_modified_date")
        .collect()[0]
    )

    # Business value updated AND last_modified_date bumped
    assert str(r2["monthly_rate_default"]) == "18.99"
    assert r2["last_modified_date"] > modified_1

    # created_date is immutable:
    # - If you didn’t add the COALESCE backfill in update_expr, it will remain NULL.
    # - If you did add it, it will be set once on the first update and remain stable.
    assert r2["created_date"] == created_1


def test_merge_created_date_immutable_last_modified_bumps(
    managed_table_client: ManagedTableClient,
    spark_session: SparkSession,
    pricing_base_daily_seed_df: DataFrame,
    pricing_base_daily_source_df: DataFrame,
) -> None:
    """
    Verify across three merges:
      1. created_date never changes once set.
      2. last_modified_date updates every time a record is merged and data changes.
    """
    # Fresh table with only business columns, 1 wrong row to force
    #   an UPDATE on first merge
    managed_table_client.overwrite(pricing_base_daily_seed_df, PRICING_BASE_DAILY_TABLE)

    # === First merge ===
    managed_table_client.merge(
        source_df=pricing_base_daily_source_df,
        target_table=PRICING_BASE_DAILY_TABLE,
        key_columns=["club_id", "membership_type", "date"],
    )

    df1 = spark_session.table(PRICING_BASE_DAILY_TABLE)

    row_u1 = (
        df1.filter(
            (col("club_id") == "1")
            & (col("membership_type") == "Premium")
            & (col("date") == lit(datetime.fromisoformat("2025-08-13T13:00:00+00:00")))
        )
        .select("created_date", "last_modified_date")
        .collect()[0]
    )
    row_i1 = (
        df1.filter(
            (col("club_id") == "3")
            & (col("membership_type") == "Elite")
            & (col("date") == lit(datetime.fromisoformat("2025-08-13T13:00:00+00:00")))
        )
        .select("created_date", "last_modified_date")
        .collect()[0]
    )

    created_u1, modified_u1 = row_u1["created_date"], row_u1["last_modified_date"]
    created_i1, modified_i1 = row_i1["created_date"], row_i1["last_modified_date"]

    assert created_u1 is not None and modified_u1 is not None
    assert created_i1 is not None and modified_i1 is not None

    # === Second merge ===
    time.sleep(1.1)
    src2 = pricing_base_daily_source_df.withColumn(
        "monthly_rate_default", lit("18.99").cast("decimal(10,2)")
    )
    managed_table_client.merge(
        source_df=src2,
        target_table=PRICING_BASE_DAILY_TABLE,
        key_columns=["club_id", "membership_type", "date"],
    )

    df2 = spark_session.table(PRICING_BASE_DAILY_TABLE)
    row_u2 = (
        df2.filter(
            (col("club_id") == "1")
            & (col("membership_type") == "Premium")
            & (col("date") == lit(datetime.fromisoformat("2025-08-13T13:00:00+00:00")))
        )
        .select("created_date", "last_modified_date")
        .collect()[0]
    )
    row_i2 = (
        df2.filter(
            (col("club_id") == "3")
            & (col("membership_type") == "Elite")
            & (col("date") == lit(datetime.fromisoformat("2025-08-13T13:00:00+00:00")))
        )
        .select("created_date", "last_modified_date")
        .collect()[0]
    )

    created_u2, modified_u2 = row_u2["created_date"], row_u2["last_modified_date"]
    created_i2, modified_i2 = row_i2["created_date"], row_i2["last_modified_date"]

    assert created_u2 == created_u1
    assert created_i2 == created_i1
    assert modified_u2 > modified_u1
    assert modified_i2 > modified_i1

    # === Third merge ===
    time.sleep(1.1)
    src3 = pricing_base_daily_source_df.withColumn(
        "monthly_rate_default", lit("500.99").cast("decimal(10,2)")
    )
    managed_table_client.merge(
        source_df=src3,
        target_table=PRICING_BASE_DAILY_TABLE,
        key_columns=["club_id", "membership_type", "date"],
    )

    df3 = spark_session.table(PRICING_BASE_DAILY_TABLE)
    row_u3 = (
        df3.filter(
            (col("club_id") == "1")
            & (col("membership_type") == "Premium")
            & (col("date") == lit(datetime.fromisoformat("2025-08-13T13:00:00+00:00")))
        )
        .select("created_date", "last_modified_date")
        .collect()[0]
    )
    row_i3 = (
        df3.filter(
            (col("club_id") == "3")
            & (col("membership_type") == "Elite")
            & (col("date") == lit(datetime.fromisoformat("2025-08-13T13:00:00+00:00")))
        )
        .select("created_date", "last_modified_date")
        .collect()[0]
    )

    created_u3, modified_u3 = row_u3["created_date"], row_u3["last_modified_date"]
    created_i3, modified_i3 = row_i3["created_date"], row_i3["last_modified_date"]

    assert created_u3 == created_u1
    assert created_i3 == created_i1
    assert modified_u3 > modified_u2
    assert modified_i3 > modified_i2
