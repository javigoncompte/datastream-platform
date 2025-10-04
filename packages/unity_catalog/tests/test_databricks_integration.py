"""Integration tests for unity_catalog library using databricks-labs-pytester."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import tempfile
import shutil
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


class TestUnityRCatalogIntegration:
    """Integration tests that simulate real Unity Catalog scenarios."""
    
    def test_table_with_spark_session(self, spark):
        """Test Table class with actual Spark session from pytester."""
        from dataplatform.unity_catalog.databricks.table import Table
        from dataplatform.unity_catalog.databricks.deltalake import DeltaWriteMode
        
        # Create test data
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        columns = ["name", "age"]
        df = spark.createDataFrame(data, columns)
        
        # Create temporary table in memory
        df.createOrReplaceTempView("test_table")
        
        # Test with a delta table path (mocking the delta functionality)
        with patch('dataplatform.unity_catalog.databricks.table.DeltaTable') as mock_delta_table:
            mock_delta_instance = Mock()
            mock_delta_table.forName.return_value = mock_delta_instance
            mock_delta_instance.toDF.return_value = df
            
            table = Table("test_table")
            result_df = table.read()
            
            # Verify we can read the data
            assert result_df.count() == 3
            assert set(result_df.columns) == {"name", "age"}
            
            # Test the __repr__ method
            assert repr(table) == "Table(name=test_table)"

    def test_table_operations_with_real_data(self, spark):
        """Test Table operations with realistic data scenarios."""
        from dataplatform.unity_catalog.databricks.table import Table
        from dataplatform.unity_catalog.databricks.deltalake import DeltaMergeConfig, DeltaDeleteMode
        
        # Create realistic customer data
        customers_data = [
            (1, "John Doe", "john@example.com", "2023-01-01"),
            (2, "Jane Smith", "jane@example.com", "2023-01-02"),
            (3, "Bob Johnson", "bob@example.com", "2023-01-03")
        ]
        customers_schema = StructType([
            StructField("customer_id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("created_date", StringType(), True)
        ])
        customers_df = spark.createDataFrame(customers_data, customers_schema)
        
        # Create updates data
        updates_data = [
            (2, "Jane Doe", "jane.doe@example.com", "2023-01-02"),  # Name change
            (4, "Alice Brown", "alice@example.com", "2023-01-04")   # New customer
        ]
        updates_df = spark.createDataFrame(updates_data, customers_schema)
        
        with patch('dataplatform.unity_catalog.databricks.table.DeltaTable') as mock_delta_table:
            # Setup mocks
            mock_delta_instance = Mock()
            mock_delta_table.forName.return_value = mock_delta_instance
            mock_delta_instance.toDF.return_value = customers_df
            
            # Setup merge builder mock chain
            mock_target_alias = Mock()
            mock_delta_instance.alias.return_value = mock_target_alias
            mock_merge_builder = Mock()
            mock_target_alias.merge.return_value = mock_merge_builder
            mock_merge_builder.whenMatchedUpdate.return_value = mock_merge_builder
            mock_merge_builder.whenNotMatchedInsert.return_value = mock_merge_builder
            
            table = Table("customers")
            
            # Test merge operation
            table.merge(
                source=updates_df,
                condition="target.customer_id = source.customer_id",
                when_matched_update={"name": "source.name", "email": "source.email"},
                when_not_matched_insert={
                    "customer_id": "source.customer_id",
                    "name": "source.name", 
                    "email": "source.email",
                    "created_date": "source.created_date"
                }
            )
            
            # Verify merge was called correctly
            mock_target_alias.merge.assert_called_once()
            mock_merge_builder.whenMatchedUpdate.assert_called_once_with(
                set={"name": "source.name", "email": "source.email"}
            )
            mock_merge_builder.whenNotMatchedInsert.assert_called_once()
            mock_merge_builder.execute.assert_called_once()

    def test_table_with_primary_keys_simulation(self, spark):
        """Test Table operations with primary key constraints."""
        from dataplatform.unity_catalog.databricks.table import Table
        
        # Create test data with primary key
        data = [
            (1, "Product A", 100.0),
            (2, "Product B", 200.0),
            (3, "Product C", 300.0)
        ]
        schema = StructType([
            StructField("product_id", IntegerType(), False),
            StructField("product_name", StringType(), True),
            StructField("price", StringType(), True)
        ])
        products_df = spark.createDataFrame(data, schema)
        
        with patch('dataplatform.unity_catalog.databricks.table.DeltaTable') as mock_delta_table, \
             patch('dataplatform.unity_catalog.databricks.table.DeltaMetadataProvider') as mock_metadata_provider:
            
            # Setup mocks
            mock_delta_instance = Mock()
            mock_delta_table.forName.return_value = mock_delta_instance
            mock_delta_instance.toDF.return_value = products_df
            
            # Setup metadata provider to return primary keys
            mock_metadata_instance = Mock()
            mock_metadata_provider.return_value = mock_metadata_instance
            mock_metadata_instance.primary_keys = ["product_id"]
            
            # Setup merge chain
            mock_merge_builder = Mock()
            mock_delta_instance.alias.return_value.merge.return_value = mock_merge_builder
            mock_merge_builder.whenMatchedUpdateAll.return_value = mock_merge_builder
            mock_merge_builder.whenNotMatchedInsertAll.return_value = mock_merge_builder
            
            table = Table("products")
            
            # Test merge with primary keys
            source_df = spark.createDataFrame([
                (2, "Product B Updated", 250.0),
                (4, "Product D", 400.0)
            ], schema)
            
            table.merge_with_keys(source_df)
            
            # Verify primary keys were used in the condition
            mock_delta_instance.alias.assert_called_with("target")
            mock_merge_builder.whenMatchedUpdateAll.assert_called_once()
            mock_merge_builder.whenNotMatchedInsertAll.assert_called_once()
            mock_merge_builder.execute.assert_called_once()

    def test_metadata_provider_with_realistic_scenario(self, spark):
        """Test DeltaMetadataProvider with realistic table metadata scenarios."""
        from dataplatform.unity_catalog.databricks.table_metadata import (
            DeltaMetadataProvider, 
            TablePropertyType
        )
        
        # Mock SQL execution results
        mock_sql_results = [
            Mock(column_name="customer_id"),
            Mock(column_name="email")
        ]
        
        with patch('dataplatform.unity_catalog.databricks.table_metadata.DeltaTable'):
            # Setup spark mock to return primary key results
            spark.sql = Mock()
            spark.sql.return_value.collect.return_value = mock_sql_results
            
            provider = DeltaMetadataProvider(spark, "catalog.schema.customers")
            
            # Test catalog/schema/table parsing
            assert provider._get_catalog() == "catalog"
            assert provider._get_schema() == "schema"
            assert provider._get_table() == "customers"
            
            # Test primary key retrieval
            primary_keys = provider._get_primary_keys()
            assert primary_keys == ["customer_id", "email"]
            
            # Test setting properties
            provider.set_property("custom.retention.days", "30", TablePropertyType.STRING)
            
            # Verify SQL was called to set the property
            spark.sql.assert_called()
            sql_calls = [call[0][0] for call in spark.sql.call_args_list]
            assert any("ALTER TABLE catalog.schema.customers" in call for call in sql_calls)
            assert any("SET TBLPROPERTIES" in call for call in sql_calls)

    def test_end_to_end_table_lifecycle(self, spark):
        """Test complete table lifecycle: create, insert, update, delete, vacuum."""
        from dataplatform.unity_catalog.databricks.table import Table
        from dataplatform.unity_catalog.databricks.deltalake import DeltaWriteMode
        
        # Create initial data
        initial_data = [
            (1, "Order 1", "Pending"),
            (2, "Order 2", "Processing")
        ]
        schema = StructType([
            StructField("order_id", IntegerType(), False),
            StructField("order_name", StringType(), True),
            StructField("status", StringType(), True)
        ])
        orders_df = spark.createDataFrame(initial_data, schema)
        
        with patch('dataplatform.unity_catalog.databricks.table.DeltaTable') as mock_delta_table:
            # Setup comprehensive mocks
            mock_delta_instance = Mock()
            mock_delta_table.forName.return_value = mock_delta_instance
            mock_delta_instance.toDF.return_value = orders_df
            mock_delta_instance.history.return_value = Mock()
            mock_delta_instance.detail.return_value = Mock()
            mock_delta_instance.vacuum.return_value = Mock()
            mock_delta_instance.optimize.return_value = Mock()
            
            # Mock history for CDC version
            mock_history_df = Mock()
            mock_delta_instance.history.return_value = mock_history_df
            mock_filtered_df = Mock()
            mock_history_df.filter.return_value = mock_filtered_df
            mock_filtered_df.isEmpty.return_value = False
            mock_ordered_df = Mock()
            mock_filtered_df.orderBy.return_value = mock_ordered_df
            mock_ordered_df.first.return_value = {"version": 5}
            
            table = Table("orders")
            
            # Test 1: Read initial data
            result_df = table.read()
            assert result_df.count() == 2
            
            # Test 2: Update operation
            table.update(
                condition="order_id = 1",
                set_expr={"status": "'Completed'"}
            )
            mock_delta_instance.update.assert_called_with(
                condition="order_id = 1",
                set={"status": "'Completed'"}
            )
            
            # Test 3: Delete operation
            table.delete(condition="status = 'Cancelled'")
            mock_delta_instance.delete.assert_called_with("status = 'Cancelled'")
            
            # Test 4: Get table history
            history = table.get_history()
            mock_delta_instance.history.assert_called()
            
            # Test 5: Get table details
            details = table.get_details()
            mock_delta_instance.detail.assert_called()
            
            # Test 6: Vacuum operation
            vacuum_result = table.vacuum(retention_hours=168)
            mock_delta_instance.vacuum.assert_called_with(168)
            
            # Test 7: Optimize operation
            optimize_builder = table.optimize()
            mock_delta_instance.optimize.assert_called()
            
            # Test 8: Get CDC version
            cdc_version = table.get_cdc_version()
            assert cdc_version == 5
            
            # Test 9: Restore to previous version
            table.restore(version_as_of=3)
            mock_delta_instance.restore.assert_called_with(version=3)

    def test_spark_session_configuration(self, spark):
        """Test Spark session configuration and management."""
        from dataplatform.unity_catalog.databricks.spark import Spark
        
        with patch('dataplatform.unity_catalog.databricks.spark.DatabricksSession') as mock_databricks_session:
            # Setup mock builder chain
            mock_builder = Mock()
            mock_databricks_session.builder = mock_builder
            mock_builder.getOrCreate.return_value = spark
            
            # Test default configuration
            spark_manager = Spark()
            session = spark_manager.spark_session
            
            assert session == spark
            mock_builder.getOrCreate.assert_called_once()
            
            # Test with serverless configuration
            mock_builder.reset_mock()
            mock_builder.serverless.return_value = mock_builder
            
            spark_manager_serverless = Spark(serverless=True)
            session_serverless = spark_manager_serverless.spark_session
            
            mock_builder.serverless.assert_called_once_with(True)
            mock_builder.getOrCreate.assert_called_once()

    def test_complex_merge_scenarios(self, spark):
        """Test complex merge scenarios with CDC and soft deletes."""
        from dataplatform.unity_catalog.databricks.table import Table
        from dataplatform.unity_catalog.databricks.deltalake import DeltaMergeConfig, DeltaDeleteMode
        
        # Create source data with CDC flags
        source_data = [
            (1, "User 1", "user1@example.com", "update"),
            (2, "User 2", "user2@example.com", "insert"),
            (3, "User 3", "user3@example.com", "delete")
        ]
        source_schema = StructType([
            StructField("user_id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("_change_type", StringType(), True)
        ])
        source_df = spark.createDataFrame(source_data, source_schema)
        
        with patch('dataplatform.unity_catalog.databricks.table.DeltaTable') as mock_delta_table:
            # Setup comprehensive merge mock chain
            mock_delta_instance = Mock()
            mock_delta_table.forName.return_value = mock_delta_instance
            
            mock_target_alias = Mock()
            mock_delta_instance.alias.return_value = mock_target_alias
            
            mock_merge_builder = Mock()
            mock_target_alias.merge.return_value = mock_merge_builder
            mock_merge_builder.whenMatchedUpdate.return_value = mock_merge_builder
            mock_merge_builder.whenMatchedUpdateAll.return_value = mock_merge_builder
            mock_merge_builder.whenNotMatchedBySourceDelete.return_value = mock_merge_builder
            mock_merge_builder.whenNotMatchedInsertAll.return_value = mock_merge_builder
            
            table = Table("users")
            
            # Test complex merge with CDC and soft deletes
            config = DeltaMergeConfig(
                join_fields=["user_id"],
                delete_mode=DeltaDeleteMode.SOFT,
                custom_set={"updated_at": "current_timestamp()"},
                ignore_null_updates=True,
                columns_to_ignore=["_change_type"]
            )
            
            table.merge_changes(source_df, config)
            
            # Verify all merge operations were configured correctly
            mock_target_alias.merge.assert_called_once()
            mock_merge_builder.whenMatchedUpdate.assert_called()
            mock_merge_builder.whenNotMatchedInsertAll.assert_called()
            mock_merge_builder.execute.assert_called()

    def test_table_property_management(self, spark):
        """Test comprehensive table property management scenarios."""
        from dataplatform.unity_catalog.databricks.table_metadata import (
            DeltaMetadataProvider,
            TableProperty,
            TablePropertyType
        )
        
        # Mock property query results
        property_results = [
            Mock(key="delta.logRetentionDuration", value="interval 30 days"),
            Mock(key="delta.deletedFileRetentionDuration", value="interval 7 days"),
            Mock(key="dataplatform.ingestion.pattern", value="CDC")
        ]
        
        with patch('dataplatform.unity_catalog.databricks.table_metadata.DeltaTable'):
            spark.sql = Mock()
            spark.sql.return_value.collect.return_value = property_results
            
            provider = DeltaMetadataProvider(spark, "catalog.schema.events")
            
            # Test getting all table properties
            properties = provider._get_table_properties()
            
            assert len(properties) == 3
            assert "delta.logRetentionDuration" in properties
            assert properties["delta.logRetentionDuration"].value == "interval 30 days"
            assert properties["delta.logRetentionDuration"].property_type == TablePropertyType.STRING
            
            # Test setting complex property values
            provider.set_property("delta.appendOnly", True, TablePropertyType.BOOLEAN)
            provider.set_property("delta.minReaderVersion", 2, TablePropertyType.INTEGER)
            provider.set_property("delta.autoOptimize.optimizeWrite", True, TablePropertyType.BOOLEAN)
            
            # Verify multiple SQL calls were made
            assert spark.sql.call_count >= 3
            
            # Test ingestion pattern management
            provider.set_ingestion_pattern("TRUNCATE_LOAD")
            provider.unset_ingestion_pattern()
            
            # Verify pattern-specific SQL calls
            sql_calls = [call[0][0] for call in spark.sql.call_args_list]
            assert any("dataplatform.ingestion.pattern" in call for call in sql_calls)

    def test_error_handling_scenarios(self, spark):
        """Test error handling in various failure scenarios."""
        from dataplatform.unity_catalog.databricks.table import Table
        
        with patch('dataplatform.unity_catalog.databricks.table.DeltaTable') as mock_delta_table:
            # Test restore with conflicting parameters
            table = Table("test_table")
            
            with pytest.raises(ValueError, match="Only one of version_as_of or timestamp_as_of should be specified"):
                table.restore(version_as_of=5, timestamp_as_of="2023-01-01")
            
            with pytest.raises(ValueError, match="Either version_as_of or timestamp_as_of must be specified"):
                table.restore()
            
            # Test metadata provider with reserved properties
            from dataplatform.unity_catalog.databricks.table_metadata import DeltaMetadataProvider
            
            provider = DeltaMetadataProvider(spark, "catalog.schema.table")
            
            with pytest.raises(ValueError, match="Cannot set reserved property: external"):
                provider.set_property("external", "true")
            
            with pytest.raises(ValueError, match="Cannot unset reserved property: location"):
                provider.unset_property("location")

    def test_build_condition_helpers(self, spark):
        """Test helper methods for building SQL conditions."""
        from dataplatform.unity_catalog.databricks.table import Table
        
        # Create test DataFrame
        data = [("A", 1, "X"), ("B", 2, "Y")]
        columns = ["col1", "col2", "col3"]
        test_df = spark.createDataFrame(data, columns)
        
        with patch('dataplatform.unity_catalog.databricks.table.DeltaTable'):
            table = Table("test_table")
            
            # Test join condition building
            join_condition = table._build_join_condition(["col1", "col2"])
            expected_join = "target.col1 = source.col1 AND target.col2 = source.col2"
            assert join_condition == expected_join
            
            # Test update condition building
            update_condition = table._build_update_condition(
                df=test_df,
                join_fields=["col1"],
                columns_to_ignore=None,
                ignore_null_updates=False
            )
            
            # Should build conditions for col2 and col3 (excluding col1 which is join field)
            assert "source.`col2`" in update_condition
            assert "source.`col3`" in update_condition
            assert "target.`col2`" in update_condition
            assert "target.`col3`" in update_condition
            assert " OR " in update_condition
            
            # Test with ignore_null_updates=True
            update_condition_ignore_nulls = table._build_update_condition(
                df=test_df,
                join_fields=["col1"],
                columns_to_ignore=["col3"],
                ignore_null_updates=True
            )
            
            # Should only include col2 and handle nulls differently
            assert "source.`col2` IS NOT NULL" in update_condition_ignore_nulls
            assert "target.`col2` != source.`col2`" in update_condition_ignore_nulls
            assert "col3" not in update_condition_ignore_nulls