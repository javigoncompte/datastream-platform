from pyspark.sql import DataFrame


def get_all_columns_from_config(table_config: dict, column_name: str) -> list:
    """
    Get all columns from a table config.

    Args:
        table_config: Config dictionary for a single table
    Returns:
        list: List of columns from the table config
    """
    all_columns = []
    for table in table_config.values():
        if column_name in table and table[column_name] and table[column_name] != [None]:
            all_columns.extend(table[column_name])

    return all_columns


def validate_renamed_columns_present(test_columns: set, config: dict) -> None:
    """
    Verifies that all renamed columns from config exist in the DataFrame
    using native PySpark.

    Args:
        df: DataFrame to validate
        config: Config dictionary for transformations

    Raises:
        ValueError: If any renamed columns are missing from the DataFrame
    """

    # Collect all renamed columns from all tables
    all_renamed_columns = get_all_columns_from_config(config, "rename_columns")

    # Check if all new column names exist in the DataFrame
    missing_columns = [
        rename_map["new_name"]
        for rename_map in all_renamed_columns
        if rename_map["new_name"] not in test_columns
    ]

    if missing_columns:
        raise ValueError(f"Missing renamed columns: {missing_columns}")


def validate_standardized_date_columns_present(test_columns: set, config: dict) -> None:
    """
    Verifies that all standardized date columns from config exist in the
    DataFrame
    with both _date and _timestamp suffixes.

    Args:
        df: DataFrame to validate
        config: Config dictionary for transformations
    Raises:
        ValueError: If any standardized date columns (with suffixes) are
                    missing
                    from the DataFrame
    """
    all_date_columns = get_all_columns_from_config(config, "standardize_date_columns")

    # Check if all date columns with suffixes exist in the DataFrame
    missing_columns = []

    for date_col in all_date_columns:
        new_name = date_col["new_name"]
        expected_date_col = f"{new_name}_date"
        expected_timestamp_col = f"{new_name}_timestamp"

        if expected_date_col not in test_columns:
            missing_columns.append(expected_date_col)
        if expected_timestamp_col not in test_columns:
            missing_columns.append(expected_timestamp_col)

    if missing_columns:
        raise ValueError(f"Missing standardized date columns: {missing_columns}")


def validate_columns_present(test_columns: set, config: dict) -> None:
    """
    Verifies that specified columns exist in the DataFrame.

    Args:
        df: DataFrame to validate
        config: Config dictionary for transformations

    Raises:
        ValueError: If any specified columns are missing from the DataFrame
    """
    all_select_columns = []
    for table_config in config.values():
        if "select_columns" in table_config:
            all_select_columns.extend(table_config["select_columns"])

    missing_columns = [col for col in all_select_columns if col not in test_columns]

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")


def validate(df: DataFrame, config: dict) -> None:
    """
    Validates the transformations applied to the DataFrame based on the
    config file.

    Args:
        df: DataFrame to validate
        config: Config dictionary for transformations

    Raises:
        ValueError: If any transformations are invalid
    """
    test_columns = set(df.columns)

    validate_renamed_columns_present(test_columns, config)
    validate_standardized_date_columns_present(test_columns, config)
    validate_columns_present(test_columns, config)
