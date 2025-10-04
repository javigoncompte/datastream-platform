import uuid

import pytest
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from ..table import _merge_table_with_keys, merge, read
from ..table_metadata import DeltaMetadataProvider


@pytest.fixture
def spark() -> SparkSession:
    """Create a SparkSession for Databricks cluster."""
    # Create a SparkSession (the entry point to Spark functionality) on
    # the cluster in the remote Databricks workspace. Unit tests do not
    # have access to this SparkSession by default.
    return SparkSession.builder.getOrCreate()


@pytest.fixture
def sample_schema() -> StructType:
    """Create a sample schema for test data."""
    return StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("value", IntegerType(), True),
    ])


@pytest.fixture
def target_data(spark: SparkSession, sample_schema: StructType) -> DataFrame:
    """Create sample target data."""
    data = [(1, "Alice", 100), (2, "Bob", 200), (3, "Charlie", 300)]
    return spark.createDataFrame(data, sample_schema)


@pytest.fixture
def source_data(spark: SparkSession, sample_schema: StructType) -> DataFrame:
    """Create sample source data for merging."""
    data = [
        (1, "Alice Updated", 150),
        (2, "Bob", 200),
        (4, "David", 400),
    ]
    return spark.createDataFrame(data, sample_schema)


@pytest.fixture
def test_table_name(spark: SparkSession, target_data: DataFrame) -> str:
    """Create a test Delta table in Databricks and return its name."""
    # Generate unique table name to avoid conflicts
    table_id = str(uuid.uuid4()).replace("-", "_")
    catalog = "main"  # or your preferred catalog
    schema = "default"  # or your preferred schema
    table_name = f"{catalog}.{schema}.test_table_{table_id}"

    # Create the table
    target_data.write.format("delta").mode("overwrite").saveAsTable(table_name)

    # Set up primary keys using metadata provider
    metadata_provider = DeltaMetadataProvider(spark, table_name)
    metadata_provider.set_property("primary_keys", "id")

    yield table_name

    # Cleanup: Drop the table after test
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    except Exception:
        pass  # Ignore cleanup errors


def test_read_table_basic(test_table_name: str):
    """Test basic table read operation."""
    # When
    result_df = read(test_table_name)

    # Then
    assert result_df is not None
    assert result_df.count() == 3
    assert "id" in result_df.columns
    assert "name" in result_df.columns
    assert "value" in result_df.columns


def test_read_table_to_polars(test_table_name: str):
    """Test table read with polars conversion."""
    try:
        # When
        result_df = read(test_table_name, to_polars=True)

        # Then
        assert result_df is not None
        # Note: This will return a polars DataFrame, not PySpark DataFrame
    except ImportError:
        pytest.skip("Polars not available in cluster")


def test_merge_with_primary_keys_basic(test_table_name: str, source_data: DataFrame):
    """Test basic merge operation using primary keys."""
    # Given
    initial_count = read(test_table_name).count()

    # When
    merge(test_table_name, source_data)

    # Then
    result_df = read(test_table_name)
    assert result_df.count() == 4  # 3 original + 1 new

    # Check Alice was updated
    alice_row = result_df.filter(F.col("id") == 1).first()
    assert alice_row is not None
    assert alice_row["name"] == "Alice Updated"
    assert alice_row["value"] == 150


def test_merge_with_explicit_condition(test_table_name: str, source_data: DataFrame):
    """Test merge with explicit join condition."""
    # Given
    condition = "target.id = source.id"

    # When
    merge(test_table_name, source_data, condition=condition)

    # Then
    result_df = read(test_table_name)
    assert result_df.count() == 4

    # Check David was inserted
    david_row = result_df.filter(F.col("id") == 4).first()
    assert david_row is not None
    assert david_row["name"] == "David"
    assert david_row["value"] == 400


def test_merge_with_custom_update_columns(test_table_name: str, source_data: DataFrame):
    """Test merge with specific column updates."""
    # Given
    when_matched_update = {"name": "source.name"}

    # When
    merge(test_table_name, source_data, when_matched_update=when_matched_update)

    # Then
    result_df = read(test_table_name)
    alice_row = result_df.filter(F.col("id") == 1).first()
    assert alice_row is not None
    assert alice_row["name"] == "Alice Updated"
    assert alice_row["value"] == 100  # Should remain unchanged


def test_merge_with_custom_insert_columns(test_table_name: str, source_data: DataFrame):
    """Test merge with custom insert values."""
    # Given
    when_not_matched_insert = {
        "id": "source.id",
        "name": "source.name",
        "value": "source.value",
    }

    # When
    merge(test_table_name, source_data, when_not_matched_insert=when_not_matched_insert)

    # Then
    result_df = read(test_table_name)
    david_row = result_df.filter(F.col("id") == 4).first()
    assert david_row is not None
    assert david_row["name"] == "David"


def test_merge_table_with_keys_hard_delete(
    test_table_name: str, source_data: DataFrame
):
    """Test merge with hard delete enabled."""
    # When
    _merge_table_with_keys(test_table_name, source_data, enable_hard_delete=True)

    # Then
    result_df = read(test_table_name)
    assert result_df.count() == 3  # Charlie should be deleted

    charlie_count = result_df.filter(F.col("id") == 3).count()
    assert charlie_count == 0


def test_merge_table_with_keys_no_hard_delete(
    test_table_name: str, source_data: DataFrame
):
    """Test merge without hard delete."""
    # When
    _merge_table_with_keys(test_table_name, source_data, enable_hard_delete=False)

    # Then
    result_df = read(test_table_name)
    assert result_df.count() == 4  # Charlie should remain

    charlie_count = result_df.filter(F.col("id") == 3).count()
    assert charlie_count == 1


def test_merge_table_with_keys_specific_columns(
    test_table_name: str, source_data: DataFrame
):
    """Test merge with specific column updates."""
    # Given
    update_columns: dict[str, str | Column] = {"name": "source.name"}

    # When
    _merge_table_with_keys(test_table_name, source_data, update_columns=update_columns)

    # Then
    result_df = read(test_table_name)
    alice_row = result_df.filter(F.col("id") == 1).first()
    assert alice_row is not None
    assert alice_row["name"] == "Alice Updated"
    assert alice_row["value"] == 100  # Should remain unchanged


def test_merge_table_with_keys_merge_options(
    test_table_name: str, source_data: DataFrame
):
    """Test merge with custom merge options."""
    # Given
    merge_options = {"spark.databricks.delta.schema.autoMerge.enabled": "true"}

    # When
    _merge_table_with_keys(test_table_name, source_data, merge_options=merge_options)

    # Then
    result_df = read(test_table_name)
    assert result_df.count() == 4

    # Check all expected records are present
    ids = [row["id"] for row in result_df.collect()]
    assert set(ids) == {1, 2, 3, 4}


def test_merge_schema_evolution(test_table_name: str, source_data: DataFrame):
    """Test merge with schema evolution enabled."""
    # Given
    source_with_new_column = source_data.withColumn("new_column", F.lit("test"))

    # When
    merge(test_table_name, source_with_new_column, with_schema_evolution=True)

    # Then
    result_df = read(test_table_name)
    assert "new_column" in result_df.columns

    # Check that new column has expected value for new records
    david_row = result_df.filter(F.col("id") == 4).first()
    assert david_row is not None
    assert david_row["new_column"] == "test"


def test_merge_without_primary_keys_fails(spark: SparkSession):
    """Test that merge fails gracefully when no primary keys or condition provided."""
    # Given
    table_id = str(uuid.uuid4()).replace("-", "_")
    table_name = f"main.default.no_pk_table_{table_id}"

    data = [(1, "test")]
    df = spark.createDataFrame(data, ["id", "name"])
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)

    source_df = spark.createDataFrame([(2, "test2")], ["id", "name"])

    try:
        # When/Then
        with pytest.raises(ValueError, match="Either provide a condition"):
            merge(table_name, source_df)
    finally:
        # Cleanup
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass
