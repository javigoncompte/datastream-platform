from typing import Any, Dict, List


def format_query_results(result: Dict[str, Any]) -> str:
    """Format query results from SQLAlchemy engine into a readable string."""

    if not result:
        return "No results or invalid result format."

    # Handle error cases first
    if result.get("status") == "error":
        return f"Error: {result.get('error', 'Unknown error')}"

    if result.get("status") == "failed":
        return f"Query failed: {result.get('error', 'Unknown error')}"

    column_names: List[str] = []
    data_rows_formatted: List[str] = []

    # Handle SQLAlchemy results from execute_databricks_sql
    if result.get("status") == "success" and "data" in result:
        print("Formatting results from SQLAlchemy engine output.")
        sdk_data = result.get("data", [])

        if not sdk_data:  # No rows, but query was successful
            if result.get("message") == "Query succeeded but returned no data.":
                return "Query succeeded but returned no data."
            return "Query succeeded but returned no data rows."

        # Get column names from the first row's keys
        if (
            isinstance(sdk_data, list)
            and len(sdk_data) > 0
            and isinstance(sdk_data[0], dict)
        ):
            column_names = list(sdk_data[0].keys())

        # Format each row
        for row_dict in sdk_data:
            row_values = []
            for col_name in column_names:  # Iterate in order of discovered column names
                value = row_dict.get(col_name)
                if value is None:
                    row_values.append("NULL")
                else:
                    # Handle different data types gracefully
                    if isinstance(value, (dict, list)):
                        row_values.append(
                            str(value)[:50] + "..."
                            if len(str(value)) > 50
                            else str(value)
                        )
                    else:
                        row_values.append(str(value))
            data_rows_formatted.append(" | ".join(row_values))

    # Handle legacy direct API style output (keeping for backward compatibility)
    elif "manifest" in result and "result" in result:
        print("Formatting results from legacy dbapi.execute_statement output.")
        if result["manifest"].get("schema") and result["manifest"]["schema"].get(
            "columns"
        ):
            columns_schema = result["manifest"]["schema"]["columns"]
            column_names = (
                [col["name"] for col in columns_schema if "name" in col]
                if columns_schema
                else []
            )

        if result["result"].get("data_array"):
            raw_rows = result["result"]["data_array"]
            for row_list in raw_rows:
                row_values = []
                for value in row_list:
                    if value is None:
                        row_values.append("NULL")
                    else:
                        row_values.append(str(value))
                data_rows_formatted.append(" | ".join(row_values))
    else:
        # Fallback for unrecognized formats
        return "Invalid or unrecognized result format."

    # Common formatting part for table output
    if not column_names:
        return "No column names found in the result."

    output_lines = []
    output_lines.append(" | ".join(column_names))
    output_lines.append(
        "-" * (sum(len(name) + 3 for name in column_names) - 1 if column_names else 0)
    )

    if not data_rows_formatted:
        output_lines.append("No data rows found.")
    else:
        output_lines.extend(data_rows_formatted)

    return "\n".join(output_lines)
