# Datastream Package

A data processing package for reading CSV data with Polars and writing to Unity Catalog using SQLModel.

## Features

- **SQLModel Integration**: Type-safe database models with automatic validation
- **Polars CSV Reading**: Efficient CSV processing with Polars
- **Unity Catalog Support**: Write data to Databricks Unity Catalog
- **Digimon Data Processing**: Pre-built pipeline for processing Digimon dataset

## Installation

Install the package dependencies:

```bash
pip install -e .
```

## Usage

### Digimon Data Processing

The package includes a complete pipeline for processing Digimon data from CSV to Unity Catalog:

```python
from datastream.test_uc_library import DigimonDataProcessor

# Create processor
processor = DigimonDataProcessor(
    csv_path="path/to/DigiDB_digimonlist.csv",
    unity_catalog_table="main.default.digimon_catalog"
)

# Run the complete pipeline
processor.process_digimon_data()
```

### SQLModel Schema

The `DigimonModel` represents the structure of the Digimon data:

```python
from datastream.test_uc_library import DigimonModel

# Example record
digimon = DigimonModel(
    number=1,
    digimon="Kuramon",
    stage="Baby",
    type="Free",
    attribute="Neutral",
    memory=2,
    equip_slots=0,
    lv50_hp=590,
    lv50_sp=77,
    lv50_atk=79,
    lv50_def=69,
    lv50_int=68,
    lv50_spd=95
)
```

### Data Processing Pipeline

The processing pipeline consists of three main steps:

1. **CSV Reading**: Uses Polars to efficiently read CSV data
2. **Data Conversion**: Converts Polars DataFrame to SQLModel records
3. **Unity Catalog Writing**: Generates SQL statements for Unity Catalog

### Configuration

For Unity Catalog integration, set the following environment variables:

```bash
export DATABRICKS_SERVER_HOSTNAME="your-workspace.cloud.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"
export DATABRICKS_ACCESS_TOKEN="your-access-token"
```

### Database Schema

The Unity Catalog table is created with the following schema:

```sql
CREATE TABLE IF NOT EXISTS main.default.digimon_catalog (
    number INT NOT NULL,
    digimon STRING NOT NULL,
    stage STRING NOT NULL,
    type STRING NOT NULL,
    attribute STRING NOT NULL,
    memory INT NOT NULL,
    equip_slots INT NOT NULL,
    lv50_hp INT NOT NULL,
    lv50_sp INT NOT NULL,
    lv50_atk INT NOT NULL,
    lv50_def INT NOT NULL,
    lv50_int INT NOT NULL,
    lv50_spd INT NOT NULL,
    PRIMARY KEY (number)
) USING DELTA
```

## Dependencies

- `polars`: Fast CSV reading and data manipulation
- `sqlmodel`: Type-safe SQL models
- `databricks-sql-connector`: Unity Catalog integration
- `unity_catalog`: Unity Catalog library

## Running the Example

To run the Digimon data processing example:

```bash
cd packages/datastream/dataplatform/datastream
python test_uc_library.py
```

This will:
1. Read the Digimon CSV data from the fixtures folder
2. Convert it to SQLModel records
3. Generate SQL statements for Unity Catalog (without executing due to missing credentials)

## Notes

- The Unity Catalog write functionality generates SQL statements but doesn't execute them without proper Databricks credentials
- All data types are validated using SQLModel's type system
- The pipeline uses efficient batch processing for large datasets
- Delta Lake optimizations are enabled for the Unity Catalog table