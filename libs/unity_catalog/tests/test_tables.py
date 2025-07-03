"""Comprehensive tests for unity_catalog library modules."""

import pytest
from unittest.mock import Mock, MagicMock, patch
from dataclasses import FrozenInstanceError
from datetime import datetime
from enum import Enum

# Test that basic modules can be imported
def test_tables_module_import():
    """Test that the tables module can be imported."""
    from dataplatform.unity_catalog import tables
    assert tables is not None


def test_databricks_modules_import():
    """Test that all databricks submodules can be imported."""
    from dataplatform.unity_catalog.databricks import table
    from dataplatform.unity_catalog.databricks import table_metadata
    from dataplatform.unity_catalog.databricks import deltalake
    from dataplatform.unity_catalog.databricks import spark
    
    assert table is not None
    assert table_metadata is not None
    assert deltalake is not None
    assert spark is not None


class TestDeltaLakeEnums:
    """Test the DeltaLake enum classes."""
    
    def test_delta_write_mode_enum(self):
        """Test DeltaWriteMode enum values."""
        from dataplatform.unity_catalog.databricks.deltalake import DeltaWriteMode
        
        assert DeltaWriteMode.APPEND.value == "append"
        assert DeltaWriteMode.OVERWRITE.value == "overwrite"
        assert DeltaWriteMode.MERGE.value == "merge"
        assert DeltaWriteMode.MERGE_CDC.value == "merge_cdc"
        assert DeltaWriteMode.UPSERT.value == "upsert"
        
        # Test that it's properly an enum
        assert isinstance(DeltaWriteMode.APPEND, DeltaWriteMode)
        assert len(DeltaWriteMode) == 5
    
    def test_delta_delete_mode_enum(self):
        """Test DeltaDeleteMode enum values."""
        from dataplatform.unity_catalog.databricks.deltalake import DeltaDeleteMode
        
        assert DeltaDeleteMode.HARD.value == "hard"
        assert DeltaDeleteMode.SOFT.value == "soft"
        assert DeltaDeleteMode.NONE.value == "none"
        
        assert isinstance(DeltaDeleteMode.HARD, DeltaDeleteMode)
        assert len(DeltaDeleteMode) == 3


class TestDeltaMergeConfig:
    """Test the DeltaMergeConfig dataclass."""
    
    def test_delta_merge_config_creation(self):
        """Test creating a DeltaMergeConfig instance."""
        from dataplatform.unity_catalog.databricks.deltalake import DeltaMergeConfig, DeltaDeleteMode
        
        config = DeltaMergeConfig(join_fields=["id", "name"])
        
        assert config.join_fields == ["id", "name"]
        assert config.update_condition is None
        assert config.delete_mode == DeltaDeleteMode.NONE
        assert config.custom_set is None
        assert config.ignore_null_updates is False
        assert config.move_last_modified is False
        assert config.columns_to_ignore is None
    
    def test_delta_merge_config_with_values(self):
        """Test creating DeltaMergeConfig with all values set."""
        from dataplatform.unity_catalog.databricks.deltalake import DeltaMergeConfig, DeltaDeleteMode
        
        config = DeltaMergeConfig(
            join_fields=["id"],
            update_condition="source.updated_at > target.updated_at",
            delete_mode=DeltaDeleteMode.SOFT,
            custom_set={"updated_at": "current_timestamp()"},
            ignore_null_updates=True,
            move_last_modified=True,
            columns_to_ignore=["created_at"]
        )
        
        assert config.join_fields == ["id"]
        assert config.update_condition == "source.updated_at > target.updated_at"
        assert config.delete_mode == DeltaDeleteMode.SOFT
        assert config.custom_set == {"updated_at": "current_timestamp()"}
        assert config.ignore_null_updates is True
        assert config.move_last_modified is True
        assert config.columns_to_ignore == ["created_at"]


class TestTablePropertyType:
    """Test the TablePropertyType enum."""
    
    def test_table_property_type_values(self):
        """Test TablePropertyType enum values."""
        from dataplatform.unity_catalog.databricks.table_metadata import TablePropertyType
        
        assert TablePropertyType.BOOLEAN.value == "boolean"
        assert TablePropertyType.STRING.value == "string"
        assert TablePropertyType.INTEGER.value == "integer"
        assert TablePropertyType.DECIMAL.value == "decimal"
        
        assert len(TablePropertyType) == 4


class TestTableProperty:
    """Test the TableProperty dataclass."""
    
    def test_table_property_creation_valid(self):
        """Test creating valid TableProperty instances."""
        from dataplatform.unity_catalog.databricks.table_metadata import TableProperty, TablePropertyType
        
        # Boolean property
        bool_prop = TableProperty("enabled", True, TablePropertyType.BOOLEAN)
        assert bool_prop.key == "enabled"
        assert bool_prop.value is True
        assert bool_prop.property_type == TablePropertyType.BOOLEAN
        
        # String property
        str_prop = TableProperty("name", "test_table", TablePropertyType.STRING)
        assert str_prop.key == "name"
        assert str_prop.value == "test_table"
        assert str_prop.property_type == TablePropertyType.STRING
        
        # Integer property
        int_prop = TableProperty("version", 42, TablePropertyType.INTEGER)
        assert int_prop.key == "version"
        assert int_prop.value == 42
        assert int_prop.property_type == TablePropertyType.INTEGER
        
        # Decimal property
        decimal_prop = TableProperty("ratio", 3.14, TablePropertyType.DECIMAL)
        assert decimal_prop.key == "ratio"
        assert decimal_prop.value == 3.14
        assert decimal_prop.property_type == TablePropertyType.DECIMAL
    
    def test_table_property_validation_errors(self):
        """Test TableProperty validation raises appropriate errors."""
        from dataplatform.unity_catalog.databricks.table_metadata import TableProperty, TablePropertyType
        
        # Boolean validation
        with pytest.raises(ValueError, match="Property bool_key must be boolean"):
            TableProperty("bool_key", "not_bool", TablePropertyType.BOOLEAN)
        
        # Integer validation
        with pytest.raises(ValueError, match="Property int_key must be integer"):
            TableProperty("int_key", "not_int", TablePropertyType.INTEGER)
        
        # Decimal validation
        with pytest.raises(ValueError, match="Property decimal_key must be decimal"):
            TableProperty("decimal_key", "not_decimal", TablePropertyType.DECIMAL)


class TestTableMetadata:
    """Test the TableMetadata dataclass."""
    
    def test_table_metadata_creation(self):
        """Test creating a TableMetadata instance."""
        from dataplatform.unity_catalog.databricks.table_metadata import TableMetadata, TableProperty, TablePropertyType
        
        properties = {
            "test_prop": TableProperty("test_prop", "test_value", TablePropertyType.STRING)
        }
        
        metadata = TableMetadata(
            table_name="catalog.schema.table",
            catalog="catalog",
            schema="schema",
            table="table",
            primary_keys=["id"],
            constraints={"pk": "PRIMARY KEY"},
            last_modified_field="updated_at",
            created_date_field="created_at",
            is_deleted_field="is_deleted",
            tags=["tag1", "tag2"],
            ingestion_pattern="CDC",
            location="s3://bucket/path",
            last_update=datetime.now(),
            last_operation="MERGE",
            operation_metrics={"numOutputRows": 100},
            properties=properties
        )
        
        assert metadata.table_name == "catalog.schema.table"
        assert metadata.catalog == "catalog"
        assert metadata.schema == "schema"
        assert metadata.table == "table"
        assert metadata.primary_keys == ["id"]
        assert metadata.constraints == {"pk": "PRIMARY KEY"}
        assert metadata.last_modified_field == "updated_at"
        assert metadata.created_date_field == "created_at"
        assert metadata.is_deleted_field == "is_deleted"
        assert metadata.tags == ["tag1", "tag2"]
        assert metadata.ingestion_pattern == "CDC"
        assert metadata.location == "s3://bucket/path"
        assert metadata.last_operation == "MERGE"
        assert metadata.operation_metrics == {"numOutputRows": 100}
        assert "test_prop" in metadata.properties


@patch('dataplatform.unity_catalog.databricks.spark.DatabricksSession')
@patch('dataplatform.unity_catalog.databricks.spark.Config')
class TestSpark:
    """Test the Spark class."""
    
    def test_spark_init_default(self, mock_config, mock_session):
        """Test Spark initialization with defaults."""
        from dataplatform.unity_catalog.databricks.spark import Spark
        
        spark = Spark()
        assert spark.config is None
        assert spark.serverless is False
    
    def test_spark_init_with_config(self, mock_config, mock_session):
        """Test Spark initialization with config."""
        from dataplatform.unity_catalog.databricks.spark import Spark
        
        config = {"host": "test.databricks.com", "token": "test_token"}
        spark = Spark(config=config, serverless=True)
        
        assert spark.config == config
        assert spark.serverless is True
    
    def test_spark_session_creation_default(self, mock_config, mock_session):
        """Test SparkSession creation with default settings."""
        from dataplatform.unity_catalog.databricks.spark import Spark
        
        # Setup mocks
        mock_builder = Mock()
        mock_session.builder = mock_builder
        mock_builder.getOrCreate.return_value = Mock()
        
        spark = Spark()
        session = spark.spark_session
        
        # Verify builder was called but not configured
        mock_builder.getOrCreate.assert_called_once()
        mock_config.assert_not_called()
    
    def test_spark_session_creation_with_serverless(self, mock_config, mock_session):
        """Test SparkSession creation with serverless enabled."""
        from dataplatform.unity_catalog.databricks.spark import Spark
        
        # Setup mocks
        mock_builder = Mock()
        mock_session.builder = mock_builder
        mock_builder.serverless.return_value = mock_builder
        mock_builder.getOrCreate.return_value = Mock()
        
        spark = Spark(serverless=True)
        session = spark.spark_session
        
        # Verify serverless was called
        mock_builder.serverless.assert_called_once_with(True)
        mock_builder.getOrCreate.assert_called_once()
    
    def test_spark_session_creation_with_config(self, mock_config, mock_session):
        """Test SparkSession creation with config."""
        from dataplatform.unity_catalog.databricks.spark import Spark
        
        # Setup mocks
        mock_builder = Mock()
        mock_session.builder = mock_builder
        mock_builder.sdkConfig.return_value = mock_builder
        mock_builder.getOrCreate.return_value = Mock()
        mock_config_instance = Mock()
        mock_config.return_value = mock_config_instance
        
        config = {"host": "test.databricks.com"}
        spark = Spark(config=config)
        session = spark.spark_session
        
        # Verify config was applied
        mock_config.assert_called_once_with(**config)
        mock_builder.sdkConfig.assert_called_once_with(mock_config_instance)
        mock_builder.getOrCreate.assert_called_once()


@patch('dataplatform.unity_catalog.databricks.table_metadata.DeltaTable')
class TestDeltaMetadataProvider:
    """Test the DeltaMetadataProvider class."""
    
    def test_init(self, mock_delta_table):
        """Test DeltaMetadataProvider initialization."""
        from dataplatform.unity_catalog.databricks.table_metadata import DeltaMetadataProvider
        
        mock_spark = Mock()
        table_name = "catalog.schema.table"
        
        provider = DeltaMetadataProvider(mock_spark, table_name)
        
        assert provider.spark == mock_spark
        assert provider.table_name == table_name
        assert provider._metadata is None
        mock_delta_table.forName.assert_called_once_with(mock_spark, table_name)
    
    def test_catalog_parsing(self, mock_delta_table):
        """Test catalog name parsing."""
        from dataplatform.unity_catalog.databricks.table_metadata import DeltaMetadataProvider
        
        mock_spark = Mock()
        provider = DeltaMetadataProvider(mock_spark, "catalog.schema.table")
        
        assert provider._get_catalog() == "catalog"
    
    def test_schema_parsing(self, mock_delta_table):
        """Test schema name parsing."""
        from dataplatform.unity_catalog.databricks.table_metadata import DeltaMetadataProvider
        
        mock_spark = Mock()
        provider = DeltaMetadataProvider(mock_spark, "catalog.schema.table")
        
        assert provider._get_schema() == "schema"
    
    def test_table_parsing(self, mock_delta_table):
        """Test table name parsing."""
        from dataplatform.unity_catalog.databricks.table_metadata import DeltaMetadataProvider
        
        mock_spark = Mock()
        provider = DeltaMetadataProvider(mock_spark, "catalog.schema.table")
        
        assert provider._get_table() == "table"
    
    def test_set_property_valid(self, mock_delta_table):
        """Test setting a valid table property."""
        from dataplatform.unity_catalog.databricks.table_metadata import DeltaMetadataProvider, TablePropertyType
        
        mock_spark = Mock()
        provider = DeltaMetadataProvider(mock_spark, "catalog.schema.table")
        
        provider.set_property("test_key", "test_value", TablePropertyType.STRING)
        
        # Verify SQL was executed
        mock_spark.sql.assert_called_once()
        sql_call = mock_spark.sql.call_args[0][0]
        assert "ALTER TABLE catalog.schema.table" in sql_call
        assert "SET TBLPROPERTIES" in sql_call
        assert "'test_key' = 'test_value'" in sql_call
    
    def test_set_property_reserved_key_error(self, mock_delta_table):
        """Test setting a reserved property raises error."""
        from dataplatform.unity_catalog.databricks.table_metadata import DeltaMetadataProvider
        
        mock_spark = Mock()
        provider = DeltaMetadataProvider(mock_spark, "catalog.schema.table")
        
        with pytest.raises(ValueError, match="Cannot set reserved property: external"):
            provider.set_property("external", "true")
    
    def test_unset_property_valid(self, mock_delta_table):
        """Test unsetting a valid table property."""
        from dataplatform.unity_catalog.databricks.table_metadata import DeltaMetadataProvider
        
        mock_spark = Mock()
        provider = DeltaMetadataProvider(mock_spark, "catalog.schema.table")
        
        provider.unset_property("test_key")
        
        # Verify SQL was executed
        mock_spark.sql.assert_called_once()
        sql_call = mock_spark.sql.call_args[0][0]
        assert "ALTER TABLE catalog.schema.table" in sql_call
        assert "UNSET TBLPROPERTIES IF EXISTS" in sql_call
        assert "'test_key'" in sql_call
    
    def test_unset_property_reserved_key_error(self, mock_delta_table):
        """Test unsetting a reserved property raises error."""
        from dataplatform.unity_catalog.databricks.table_metadata import DeltaMetadataProvider
        
        mock_spark = Mock()
        provider = DeltaMetadataProvider(mock_spark, "catalog.schema.table")
        
        with pytest.raises(ValueError, match="Cannot unset reserved property: location"):
            provider.unset_property("location")
    
    def test_set_ingestion_pattern(self, mock_delta_table):
        """Test setting ingestion pattern."""
        from dataplatform.unity_catalog.databricks.table_metadata import DeltaMetadataProvider
        
        mock_spark = Mock()
        provider = DeltaMetadataProvider(mock_spark, "catalog.schema.table")
        
        provider.set_ingestion_pattern("cdc")
        
        # Verify SQL was executed with uppercase pattern
        mock_spark.sql.assert_called_once()
        sql_call = mock_spark.sql.call_args[0][0]
        assert "'dataplatform.ingestion.pattern' = 'CDC'" in sql_call
    
    def test_unset_ingestion_pattern(self, mock_delta_table):
        """Test unsetting ingestion pattern."""
        from dataplatform.unity_catalog.databricks.table_metadata import DeltaMetadataProvider
        
        mock_spark = Mock()
        provider = DeltaMetadataProvider(mock_spark, "catalog.schema.table")
        
        provider.unset_ingestion_pattern()
        
        # Verify SQL was executed
        mock_spark.sql.assert_called_once()
        sql_call = mock_spark.sql.call_args[0][0]
        assert "UNSET TBLPROPERTIES IF EXISTS" in sql_call
        assert "'dataplatform.ingestion.pattern'" in sql_call


@patch('dataplatform.unity_catalog.databricks.table.Spark')
@patch('dataplatform.unity_catalog.databricks.table.DeltaTable')
class TestTable:
    """Test the Table class."""
    
    def test_table_init(self, mock_delta_table, mock_spark_class):
        """Test Table initialization."""
        from dataplatform.unity_catalog.databricks.table import Table
        
        mock_spark_instance = Mock()
        mock_spark_class.return_value.spark_session = mock_spark_instance
        
        table = Table("catalog.schema.table")
        
        assert table.name == "catalog.schema.table"
        assert table.spark == mock_spark_instance
    
    def test_table_repr(self, mock_delta_table, mock_spark_class):
        """Test Table string representation."""
        from dataplatform.unity_catalog.databricks.table import Table
        
        table = Table("test_table")
        assert repr(table) == "Table(name=test_table)"
    
    def test_delta_table_property(self, mock_delta_table, mock_spark_class):
        """Test delta_table cached property."""
        from dataplatform.unity_catalog.databricks.table import Table
        
        mock_spark_instance = Mock()
        mock_spark_class.return_value.spark_session = mock_spark_instance
        mock_delta_table_instance = Mock()
        mock_delta_table.forName.return_value = mock_delta_table_instance
        
        table = Table("catalog.schema.table")
        delta_table = table.delta_table
        
        assert delta_table == mock_delta_table_instance
        mock_delta_table.forName.assert_called_once_with(mock_spark_instance, "catalog.schema.table")
    
    def test_read(self, mock_delta_table, mock_spark_class):
        """Test read method."""
        from dataplatform.unity_catalog.databricks.table import Table
        
        mock_delta_table_instance = Mock()
        mock_dataframe = Mock()
        mock_delta_table_instance.toDF.return_value = mock_dataframe
        mock_delta_table.forName.return_value = mock_delta_table_instance
        
        table = Table("test_table")
        result = table.read()
        
        assert result == mock_dataframe
        mock_delta_table_instance.toDF.assert_called_once()
    
    def test_merge_basic(self, mock_delta_table, mock_spark_class):
        """Test basic merge operation."""
        from dataplatform.unity_catalog.databricks.table import Table
        
        mock_delta_table_instance = Mock()
        mock_merge_builder = Mock()
        mock_delta_table_instance.alias.return_value.merge.return_value = mock_merge_builder
        mock_merge_builder.whenMatchedUpdate.return_value = mock_merge_builder
        mock_merge_builder.whenNotMatchedInsert.return_value = mock_merge_builder
        mock_delta_table.forName.return_value = mock_delta_table_instance
        
        table = Table("test_table")
        source_df = Mock()
        
        table.merge(
            source=source_df,
            condition="target.id = source.id",
            when_matched_update={"name": "source.name"},
            when_not_matched_insert={"id": "source.id", "name": "source.name"}
        )
        
        mock_merge_builder.whenMatchedUpdate.assert_called_once_with(set={"name": "source.name"})
        mock_merge_builder.whenNotMatchedInsert.assert_called_once_with(values={"id": "source.id", "name": "source.name"})
        mock_merge_builder.execute.assert_called_once()
    
    def test_update_with_condition(self, mock_delta_table, mock_spark_class):
        """Test update with condition."""
        from dataplatform.unity_catalog.databricks.table import Table
        
        mock_delta_table_instance = Mock()
        mock_delta_table.forName.return_value = mock_delta_table_instance
        
        table = Table("test_table")
        table.update(condition="id = 1", set_expr={"name": "'updated'"})
        
        mock_delta_table_instance.update.assert_called_once_with(condition="id = 1", set={"name": "'updated'"})
    
    def test_update_without_condition(self, mock_delta_table, mock_spark_class):
        """Test update without condition."""
        from dataplatform.unity_catalog.databricks.table import Table
        
        mock_delta_table_instance = Mock()
        mock_delta_table.forName.return_value = mock_delta_table_instance
        
        table = Table("test_table")
        table.update(set_expr={"name": "'updated'"})
        
        mock_delta_table_instance.update.assert_called_once_with(set={"name": "'updated'"})
    
    def test_delete_with_condition(self, mock_delta_table, mock_spark_class):
        """Test delete with condition."""
        from dataplatform.unity_catalog.databricks.table import Table
        
        mock_delta_table_instance = Mock()
        mock_delta_table.forName.return_value = mock_delta_table_instance
        
        table = Table("test_table")
        table.delete(condition="id = 1")
        
        mock_delta_table_instance.delete.assert_called_once_with("id = 1")
    
    def test_delete_without_condition(self, mock_delta_table, mock_spark_class):
        """Test delete without condition."""
        from dataplatform.unity_catalog.databricks.table import Table
        
        mock_delta_table_instance = Mock()
        mock_delta_table.forName.return_value = mock_delta_table_instance
        
        table = Table("test_table")
        table.delete()
        
        mock_delta_table_instance.delete.assert_called_once_with()
    
    def test_get_history(self, mock_delta_table, mock_spark_class):
        """Test get_history method."""
        from dataplatform.unity_catalog.databricks.table import Table
        
        mock_delta_table_instance = Mock()
        mock_history_df = Mock()
        mock_delta_table_instance.history.return_value = mock_history_df
        mock_delta_table.forName.return_value = mock_delta_table_instance
        
        table = Table("test_table")
        result = table.get_history()
        
        assert result == mock_history_df
        mock_delta_table_instance.history.assert_called_once()
    
    def test_vacuum_with_retention(self, mock_delta_table, mock_spark_class):
        """Test vacuum with retention hours."""
        from dataplatform.unity_catalog.databricks.table import Table
        
        mock_delta_table_instance = Mock()
        mock_vacuum_result = Mock()
        mock_delta_table_instance.vacuum.return_value = mock_vacuum_result
        mock_delta_table.forName.return_value = mock_delta_table_instance
        
        table = Table("test_table")
        result = table.vacuum(retention_hours=168.0)
        
        assert result == mock_vacuum_result
        mock_delta_table_instance.vacuum.assert_called_once_with(168.0)
    
    def test_vacuum_without_retention(self, mock_delta_table, mock_spark_class):
        """Test vacuum without retention hours."""
        from dataplatform.unity_catalog.databricks.table import Table
        
        mock_delta_table_instance = Mock()
        mock_vacuum_result = Mock()
        mock_delta_table_instance.vacuum.return_value = mock_vacuum_result
        mock_delta_table.forName.return_value = mock_delta_table_instance
        
        table = Table("test_table")
        result = table.vacuum()
        
        assert result == mock_vacuum_result
        mock_delta_table_instance.vacuum.assert_called_once_with(None)
    
    def test_restore_with_version(self, mock_delta_table, mock_spark_class):
        """Test restore with version."""
        from dataplatform.unity_catalog.databricks.table import Table
        
        mock_delta_table_instance = Mock()
        mock_delta_table.forName.return_value = mock_delta_table_instance
        
        table = Table("test_table")
        table.restore(version_as_of=5)
        
        mock_delta_table_instance.restore.assert_called_once_with(version=5)
    
    def test_restore_with_timestamp(self, mock_delta_table, mock_spark_class):
        """Test restore with timestamp."""
        from dataplatform.unity_catalog.databricks.table import Table
        
        mock_delta_table_instance = Mock()
        mock_delta_table.forName.return_value = mock_delta_table_instance
        
        table = Table("test_table")
        table.restore(timestamp_as_of="2023-01-01 00:00:00")
        
        mock_delta_table_instance.restore.assert_called_once_with(timestamp="2023-01-01 00:00:00")
    
    def test_restore_with_both_parameters_error(self, mock_delta_table, mock_spark_class):
        """Test restore with both parameters raises error."""
        from dataplatform.unity_catalog.databricks.table import Table
        
        table = Table("test_table")
        
        with pytest.raises(ValueError, match="Only one of version_as_of or timestamp_as_of should be specified"):
            table.restore(version_as_of=5, timestamp_as_of="2023-01-01 00:00:00")
    
    def test_restore_with_no_parameters_error(self, mock_delta_table, mock_spark_class):
        """Test restore with no parameters raises error."""
        from dataplatform.unity_catalog.databricks.table import Table
        
        table = Table("test_table")
        
        with pytest.raises(ValueError, match="Either version_as_of or timestamp_as_of must be specified"):
            table.restore()
    
    def test_get_cdc_version_with_transactions(self, mock_delta_table, mock_spark_class):
        """Test get_cdc_version with transaction history."""
        from dataplatform.unity_catalog.databricks.table import Table
        
        mock_delta_table_instance = Mock()
        mock_history_df = Mock()
        mock_transaction_df = Mock()
        mock_ordered_df = Mock()
        
        # Setup mock chain
        mock_delta_table_instance.history.return_value = mock_history_df
        mock_history_df.filter.return_value = mock_transaction_df
        mock_transaction_df.isEmpty.return_value = False
        mock_transaction_df.orderBy.return_value = mock_ordered_df
        mock_ordered_df.first.return_value = {"version": 42}
        
        mock_delta_table.forName.return_value = mock_delta_table_instance
        
        table = Table("test_table")
        version = table.get_cdc_version()
        
        assert version == 42
    
    def test_get_cdc_version_no_transactions(self, mock_delta_table, mock_spark_class):
        """Test get_cdc_version with no transaction history."""
        from dataplatform.unity_catalog.databricks.table import Table
        
        mock_delta_table_instance = Mock()
        mock_history_df = Mock()
        mock_transaction_df = Mock()
        mock_ordered_df = Mock()
        
        # Setup mock chain for empty transactions
        mock_delta_table_instance.history.return_value = mock_history_df
        mock_history_df.filter.return_value = mock_transaction_df
        mock_transaction_df.isEmpty.return_value = True
        mock_history_df.orderBy.return_value = mock_ordered_df
        mock_ordered_df.first.return_value = {"version": 10}
        
        mock_delta_table.forName.return_value = mock_delta_table_instance
        
        table = Table("test_table")
        version = table.get_cdc_version()
        
        assert version == 10
    
    def test_get_cdc_version_no_history(self, mock_delta_table, mock_spark_class):
        """Test get_cdc_version with no history."""
        from dataplatform.unity_catalog.databricks.table import Table
        
        mock_delta_table_instance = Mock()
        mock_history_df = Mock()
        mock_transaction_df = Mock()
        mock_ordered_df = Mock()
        
        # Setup mock chain for no history
        mock_delta_table_instance.history.return_value = mock_history_df
        mock_history_df.filter.return_value = mock_transaction_df
        mock_transaction_df.isEmpty.return_value = True
        mock_history_df.orderBy.return_value = mock_ordered_df
        mock_ordered_df.first.return_value = None
        
        mock_delta_table.forName.return_value = mock_delta_table_instance
        
        table = Table("test_table")
        version = table.get_cdc_version()
        
        assert version == 0
    
    def test_build_join_condition(self, mock_delta_table, mock_spark_class):
        """Test _build_join_condition method."""
        from dataplatform.unity_catalog.databricks.table import Table
        
        table = Table("test_table")
        condition = table._build_join_condition(["id", "name"])
        
        expected = "target.id = source.id AND target.name = source.name"
        assert condition == expected
    
    def test_build_update_condition_basic(self, mock_delta_table, mock_spark_class):
        """Test _build_update_condition method basic case."""
        from dataplatform.unity_catalog.databricks.table import Table
        
        mock_df = Mock()
        mock_df.columns = ["id", "name", "value"]
        
        table = Table("test_table")
        condition = table._build_update_condition(
            df=mock_df,
            join_fields=["id"],
            columns_to_ignore=None,
            ignore_null_updates=False
        )
        
        # Should build conditions for 'name' and 'value' (excluding 'id' which is join field)
        assert "source.`name`" in condition
        assert "source.`value`" in condition
        assert "target.`name`" in condition
        assert "target.`value`" in condition
        assert " OR " in condition
    
    def test_build_update_condition_ignore_nulls(self, mock_delta_table, mock_spark_class):
        """Test _build_update_condition with ignore_null_updates=True."""
        from dataplatform.unity_catalog.databricks.table import Table
        
        mock_df = Mock()
        mock_df.columns = ["id", "name"]
        
        table = Table("test_table")
        condition = table._build_update_condition(
            df=mock_df,
            join_fields=["id"],
            columns_to_ignore=None,
            ignore_null_updates=True
        )
        
        # Should only update when source is not null
        assert "source.`name` IS NOT NULL" in condition
        assert "target.`name` != source.`name`" in condition


# Edge case and integration tests
class TestUnityRCatalogIntegration:
    """Integration tests for unity_catalog components."""
    
    def test_all_modules_can_be_imported_together(self):
        """Test that all modules can be imported together without conflicts."""
        from dataplatform.unity_catalog.databricks.table import Table
        from dataplatform.unity_catalog.databricks.table_metadata import DeltaMetadataProvider, TableProperty, TablePropertyType
        from dataplatform.unity_catalog.databricks.deltalake import DeltaWriteMode, DeltaDeleteMode, DeltaMergeConfig
        from dataplatform.unity_catalog.databricks.spark import Spark
        
        # Verify all classes are available
        assert Table is not None
        assert DeltaMetadataProvider is not None
        assert TableProperty is not None
        assert TablePropertyType is not None
        assert DeltaWriteMode is not None
        assert DeltaDeleteMode is not None
        assert DeltaMergeConfig is not None
        assert Spark is not None
    
    def test_enum_interoperability(self):
        """Test that enums work properly with dataclasses."""
        from dataplatform.unity_catalog.databricks.deltalake import DeltaMergeConfig, DeltaDeleteMode
        from dataplatform.unity_catalog.databricks.table_metadata import TableProperty, TablePropertyType
        
        # Test enum usage in dataclass
        config = DeltaMergeConfig(
            join_fields=["id"],
            delete_mode=DeltaDeleteMode.SOFT
        )
        assert config.delete_mode == DeltaDeleteMode.SOFT
        
        # Test enum usage in property
        prop = TableProperty("enabled", True, TablePropertyType.BOOLEAN)
        assert prop.property_type == TablePropertyType.BOOLEAN