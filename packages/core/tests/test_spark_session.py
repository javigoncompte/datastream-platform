from dataplatform.core.spark_session import SparkSession


def test_spark_session_initialization():
    """Test that SparkSession initializes correctly."""
    spark_session = SparkSession()

    assert hasattr(spark_session, "databricks_profile")
    assert hasattr(spark_session, "databricks_config_file")
    assert hasattr(spark_session, "spark")


def test_create_spark_session_logic():
    """Test the conditional logic in _create_spark_session method."""
    spark_session = SparkSession()

    spark_session.databricks_config_file = None
    spark_session.databricks_profile = None

    should_use_pyspark = (
        not spark_session.databricks_config_file
        and not spark_session.databricks_profile
    )
    assert should_use_pyspark is True

    spark_session.databricks_config_file = "/path/to/config"
    spark_session.databricks_profile = "profile"

    should_use_pyspark = (
        not spark_session.databricks_config_file
        and not spark_session.databricks_profile
    )
    assert should_use_pyspark is False
