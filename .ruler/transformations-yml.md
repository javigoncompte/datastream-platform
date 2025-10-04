# Data Pipeline Configuration Rule

## Overview
This rule provides guidance for creating YAML configuration files for data transformation pipelines using the dataplatform transformation framework. These configs define how to transform bronze tables into silver/gold tables following the medallion architecture.

## YAML Configuration Structure

### Required Fields
Every table configuration must include:
- `table_name`: Fully qualified table name (e.g., "bronze.schema.table")
- `read_cdc`: Boolean flag for Change Data Capture (true/false)

### Optional Fields
- `rename_columns`: List of column rename mappings
- `standardize_date_columns`: List of date column standardizations
- `select_columns`: List of columns to include in output
- `normalize_column_values`: List of columns to normalize for joins/filters
- `order_columns`: List defining column ordering

## Configuration Examples

### Basic Table Configuration
```yaml
table_name: "bronze.schema.source_table"
read_cdc: false
rename_columns: []
standardize_date_columns: []
select_columns: []
normalize_column_values: []
```

### Full Transformation Configuration
```yaml
table_name: "bronze.pac_dbo.cw_template_pt_options_history"
read_cdc: true
rename_columns:
  - { original_name: "old_col", new_name: "new_col" }
  - { original_name: "created_at", new_name: "created_timestamp" }
standardize_date_columns:
  - { original_name: "created_at", new_name: "created" }
  - { original_name: "modified_at", new_name: "modified" }
select_columns:
  - "new_col"
  - "created_timestamp"
  - "created_date"
  - "modified_timestamp"
  - "modified_date"
normalize_column_values: ["code_column", "status_column"]
```

## Field Descriptions

### rename_columns
- **Purpose**: Maps source column names to target column names
- **Format**: List of objects with `original_name` and `new_name`
- **Use Case**: Standardize naming conventions, remove prefixes/suffixes

### standardize_date_columns
- **Purpose**: Creates both timestamp and date versions of date columns
- **Format**: List of objects with `original_name` and `new_name`
- **Output**: Creates `{new_name}_timestamp` and `{new_name}_date` columns
- **Requirement**: Source column must be timestamp type

### select_columns
- **Purpose**: Filters which columns appear in final output
- **Format**: List of column names
- **Special**: Empty list `[]` selects all columns
- **Note**: Include both original and transformed column names

### normalize_column_values
- **Purpose**: Standardizes values for join/filter operations
- **Format**: List of column names
- **Transformation**: Converts to lowercase, replaces non-alphanumeric with underscores
- **Use Case**: Ensure consistent matching across tables

### order_columns
- **Purpose**: Defines column ordering in output
- **Format**: List of column names in desired order
- **Behavior**: Specified columns first, then remaining columns alphabetically

## CDC Configuration

### Timestamp-based CDC
```yaml
read_cdc: true
# Use starting_timestamp and ending_timestamp parameters in load_data()
```

### Version-based CDC
```yaml
read_cdc: true
# Use commit_version parameter in load_data()
```

### Full CDC History
```yaml
read_cdc: true
# No additional parameters - reads from beginning of CDF history
```

## Best Practices

1. **Naming Conventions**
   - Use snake_case for column names
   - Be descriptive and consistent
   - Avoid abbreviations unless widely understood

2. **Date Handling**
   - Always include both timestamp and date versions for date columns
   - Use consistent naming patterns (e.g., `{entity}_{action}_timestamp`)

3. **Column Selection**
   - Only select columns that are actually needed
   - Include all transformed columns in select_columns
   - Consider downstream dependencies

4. **Normalization**
   - Normalize columns used for joins or filtering
   - Avoid normalizing columns used for display or business logic

5. **Documentation**
   - Add comments explaining business logic
   - Document any special transformations
   - Keep configs readable and maintainable

## Validation Rules

The framework automatically validates:
- All renamed columns exist in output
- All standardized date columns have both suffixes
- All selected columns are present
- No duplicate column names

## Common Patterns

### Identity Transformation
```yaml
# When you want to keep the table as-is
rename_columns: []
standardize_date_columns: []
select_columns: []  # Empty list selects all columns
```

### Simple Rename
```yaml
# When you only need to rename columns
rename_columns:
  - { original_name: "id", new_name: "entity_id" }
  - { original_name: "name", new_name: "entity_name" }
select_columns: []  # Keep all columns
```

### Date Standardization
```yaml
# When you need consistent date handling
standardize_date_columns:
  - { original_name: "created_at", new_name: "created" }
  - { original_name: "updated_at", new_name: "updated" }
select_columns:
  - "id"
  - "created_timestamp"
  - "created_date"
  - "updated_timestamp"
  - "updated_date"
```

## Troubleshooting

### Common Issues
1. **Missing columns**: Ensure all referenced columns exist in source table
2. **Type mismatches**: Date standardization requires timestamp source columns
3. **CDC errors**: Verify table supports Change Data Feed
4. **Validation failures**: Check that all transformed columns are included in select_columns

### Debug Steps
1. Verify source table schema
2. Check CDC support and options
3. Validate column mappings
4. Review transformation pipeline logs
5. Test with small data samples first
