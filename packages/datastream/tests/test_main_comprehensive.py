"""Comprehensive tests for datastream.main module."""

import pytest
from unittest.mock import patch, MagicMock
from datastream.main import get_taxis, get_spark, main


class TestGetTaxis:
    """Test cases for get_taxis function."""

    def test_get_taxis_returns_dataframe(self):
        """Test that get_taxis returns a DataFrame."""
        # Mock SparkSession
        mock_spark = MagicMock()
        mock_dataframe = MagicMock()
        mock_spark.read.table.return_value = mock_dataframe
        
        result = get_taxis(mock_spark)
        
        assert result == mock_dataframe
        mock_spark.read.table.assert_called_once_with("samples.nyctaxi.trips")

    def test_get_taxis_with_none_spark(self):
        """Test get_taxis behavior with None spark session."""
        with pytest.raises(AttributeError):
            get_taxis(None)

    def test_get_taxis_correct_table_name(self):
        """Test that get_taxis uses the correct table name."""
        mock_spark = MagicMock()
        mock_dataframe = MagicMock()
        mock_spark.read.table.return_value = mock_dataframe
        
        get_taxis(mock_spark)
        
        # Verify the exact table name is used
        mock_spark.read.table.assert_called_once_with("samples.nyctaxi.trips")


class TestGetSpark:
    """Test cases for get_spark function."""

    @patch('datastream.main.DatabricksSession')
    def test_get_spark_with_databricks_connect(self, mock_databricks_session):
        """Test get_spark when DatabricksSession is available."""
        mock_session = MagicMock()
        mock_builder = MagicMock()
        mock_builder.getOrCreate.return_value = mock_session
        mock_databricks_session.builder = mock_builder
        
        result = get_spark()
        
        assert result == mock_session
        mock_builder.getOrCreate.assert_called_once()

    @patch('datastream.main.DatabricksSession', side_effect=ImportError("No module named 'databricks.connect'"))
    @patch('datastream.main.SparkSession')
    def test_get_spark_fallback_to_sparksession(self, mock_spark_session, mock_databricks_session):
        """Test get_spark falls back to SparkSession when DatabricksSession is not available."""
        mock_session = MagicMock()
        mock_builder = MagicMock()
        mock_builder.getOrCreate.return_value = mock_session
        mock_spark_session.builder = mock_builder
        
        result = get_spark()
        
        assert result == mock_session
        mock_builder.getOrCreate.assert_called_once()

    @patch('datastream.main.SparkSession')
    def test_get_spark_direct_sparksession_import(self, mock_spark_session):
        """Test get_spark when directly using SparkSession."""
        with patch('datastream.main.DatabricksSession', side_effect=ImportError()):
            mock_session = MagicMock()
            mock_builder = MagicMock()
            mock_builder.getOrCreate.return_value = mock_session
            mock_spark_session.builder = mock_builder
            
            result = get_spark()
            
            assert result == mock_session
            mock_builder.getOrCreate.assert_called_once()


class TestMain:
    """Test cases for main function."""

    @patch('datastream.main.get_taxis')
    @patch('datastream.main.get_spark')
    def test_main_function_calls(self, mock_get_spark, mock_get_taxis):
        """Test that main function calls the correct functions."""
        mock_spark = MagicMock()
        mock_dataframe = MagicMock()
        mock_get_spark.return_value = mock_spark
        mock_get_taxis.return_value = mock_dataframe
        
        main()
        
        mock_get_spark.assert_called_once()
        mock_get_taxis.assert_called_once_with(mock_spark)
        mock_dataframe.show.assert_called_once_with(5)

    @patch('datastream.main.get_taxis')
    @patch('datastream.main.get_spark')
    def test_main_function_exception_handling(self, mock_get_spark, mock_get_taxis):
        """Test main function behavior when exceptions occur."""
        mock_get_spark.side_effect = Exception("Spark initialization failed")
        
        with pytest.raises(Exception, match="Spark initialization failed"):
            main()

    @patch('datastream.main.get_taxis')
    @patch('datastream.main.get_spark')
    def test_main_dataframe_show_with_correct_limit(self, mock_get_spark, mock_get_taxis):
        """Test that main shows exactly 5 rows."""
        mock_spark = MagicMock()
        mock_dataframe = MagicMock()
        mock_get_spark.return_value = mock_spark
        mock_get_taxis.return_value = mock_dataframe
        
        main()
        
        mock_dataframe.show.assert_called_once_with(5)


class TestIntegration:
    """Integration test cases."""

    @patch('datastream.main.DatabricksSession')
    def test_full_integration_with_databricks(self, mock_databricks_session):
        """Test full integration flow with DatabricksSession."""
        # Setup mocks
        mock_session = MagicMock()
        mock_builder = MagicMock()
        mock_builder.getOrCreate.return_value = mock_session
        mock_databricks_session.builder = mock_builder
        
        mock_dataframe = MagicMock()
        mock_session.read.table.return_value = mock_dataframe
        
        # Execute main function
        main()
        
        # Verify the full flow
        mock_builder.getOrCreate.assert_called_once()
        mock_session.read.table.assert_called_once_with("samples.nyctaxi.trips")
        mock_dataframe.show.assert_called_once_with(5)

    @patch('datastream.main.DatabricksSession', side_effect=ImportError())
    @patch('datastream.main.SparkSession')
    def test_full_integration_with_sparksession_fallback(self, mock_spark_session, mock_databricks_session):
        """Test full integration flow with SparkSession fallback."""
        # Setup mocks
        mock_session = MagicMock()
        mock_builder = MagicMock()
        mock_builder.getOrCreate.return_value = mock_session
        mock_spark_session.builder = mock_builder
        
        mock_dataframe = MagicMock()
        mock_session.read.table.return_value = mock_dataframe
        
        # Execute main function
        main()
        
        # Verify the full flow
        mock_builder.getOrCreate.assert_called_once()
        mock_session.read.table.assert_called_once_with("samples.nyctaxi.trips")
        mock_dataframe.show.assert_called_once_with(5)


class TestModuleStructure:
    """Test cases for module structure and imports."""

    def test_module_imports(self):
        """Test that all expected imports are available."""
        import datastream.main as main_module
        
        assert hasattr(main_module, 'get_taxis')
        assert hasattr(main_module, 'get_spark')
        assert hasattr(main_module, 'main')
        assert hasattr(main_module, 'SparkSession')
        assert hasattr(main_module, 'DataFrame')

    def test_function_signatures(self):
        """Test that functions have expected signatures."""
        import inspect
        
        # Test get_taxis signature
        sig = inspect.signature(get_taxis)
        assert len(sig.parameters) == 1
        assert 'spark' in sig.parameters
        
        # Test get_spark signature
        sig = inspect.signature(get_spark)
        assert len(sig.parameters) == 0
        
        # Test main signature
        sig = inspect.signature(main)
        assert len(sig.parameters) == 0