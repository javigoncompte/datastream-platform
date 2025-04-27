from pathlib import Path

import pytest
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from ..table import Table


@pytest.fixture
def spark_session() -> SparkSession:
    """Create a test SparkSession."""
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
def target_data(spark_session: SparkSession, sample_schema: StructType) -> DataFrame:
    """Create sample target data."""
    data = [(1, "Alice", 100), (2, "Bob", 200), (3, "Charlie", 300)]
    return spark_session.createDataFrame(data, sample_schema)


@pytest.fixture
def source_data(spark_session: SparkSession, sample_schema: StructType) -> DataFrame:
    """Create sample source data for merging."""
    data = [
        (1, "Alice Updated", 150),
        (2, "Bob", 200),
        (4, "David", 400),
    ]
    return spark_session.createDataFrame(data, sample_schema)


@pytest.fixture
def test_table(
    spark_session: SparkSession, target_data: DataFrame, tmp_path: Path
) -> Table:
    """Create a test Delta table."""
    table_path = str(tmp_path / "test_table")
    target_data.write.format("delta").save(table_path)
    table = Table(name=f"delta.`{table_path}`")
    table.primary_keys = ["id"]  # type: ignore
    return table


def test_merge_with_keys_basic_update(test_table: Table, source_data: DataFrame):
    """Test basic merge operation with updates."""
    test_table.merge_with_keys(source_data)

    result_df = test_table.read()
    result = result_df

    assert result.count() == 4

    alice_row = result.filter(F.col("id") == 1).first()
    assert alice_row is not None
    assert alice_row["name"] == "Alice Updated"
    assert alice_row["value"] == 150


def test_merge_with_keys_specific_columns(test_table: Table, source_data: DataFrame):
    """Test merge with specific column updates."""
    update_columns: dict[str, str | Column] = {"name": "source.name"}
    test_table.merge_with_keys(source_data, update_columns=update_columns)

    result_df = test_table.read()
    result = result_df

    alice_row = result.filter(F.col("id") == 1).first()
    assert alice_row is not None
    assert alice_row["name"] == "Alice Updated"
    assert alice_row["value"] == 100


def test_merge_with_keys_hard_delete(test_table: Table, source_data: DataFrame):
    """Test merge with hard delete enabled."""
    test_table.merge_with_keys(source_data, enable_hard_delete=True)

    result_df = test_table.read()
    result = result_df

    assert result.count() == 3

    charlie_count = result.filter(F.col("id") == 3).count()
    assert charlie_count == 0


def test_merge_with_keys_no_hard_delete(test_table: Table, source_data: DataFrame):
    """Test merge without hard delete."""
    test_table.merge_with_keys(source_data, enable_hard_delete=False)

    result_df = test_table.read()
    result = result_df

    assert result.count() == 4

    charlie_count = result.filter(F.col("id") == 3).count()
    assert charlie_count == 1


def test_merge_with_keys_merge_options(test_table: Table, source_data: DataFrame):
    """Test merge with custom merge options."""
    merge_options = {"spark.databricks.delta.schema.autoMerge.enabled": "true"}
    test_table.merge_with_keys(source_data, merge_options=merge_options)

    result_df = test_table.read()
    result = result_df
    assert result.count() == 4
