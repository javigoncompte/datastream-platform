#!/usr/bin/env python3
"""
Test script to verify SQLAlchemy engine integration with Databricks.
"""

import os

from databricks_formatter import format_query_results
from dotenv import load_dotenv
from engine import execute_sql_query

# Load environment variables
load_dotenv()


def test_sqlalchemy_engine():
    """Test the SQLAlchemy engine with a simple query."""

    # Test 1: Simple system query
    print("=== Test 1: System Query ===")
    try:
        result = execute_sql_query(
            "SELECT 1 as test_column, 'hello' as test_string",
            catalog="default",
        )
        print("Result:", result)
        formatted = format_query_results(result)
        print("Formatted:")
        print(formatted)
    except Exception as e:
        print(f"Error in test 1: {e}")

    print("\n" + "=" * 50 + "\n")

    # Test 2: Check if we can connect to system catalog
    print("=== Test 2: System Catalog Connection ===")
    try:
        result = execute_sql_query("SELECT 1 as connection_test", catalog="system")
        print("System catalog connection result:", result)
    except Exception as e:
        print(f"Error in test 2: {e}")


if __name__ == "__main__":
    # Check environment variables
    required_vars = ["DATABRICKS_HOST", "DATABRICKS_TOKEN"]
    missing_vars = [var for var in required_vars if not os.environ.get(var)]

    if missing_vars:
        print(f"Missing required environment variables: {missing_vars}")
        print("Please set them in your .env file or environment")
    else:
        print("Environment variables found, running tests...")
        test_sqlalchemy_engine()
