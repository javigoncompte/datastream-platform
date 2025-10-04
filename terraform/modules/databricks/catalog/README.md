# Unity Catalog Management

This directory contains resources for managing Unity Catalog catalogs and their permissions.

## Files

- **`catalogs.tf`** - Unity Catalog catalog resources
- **`permissions.tf`** - Catalog permissions and access control
- **`azure-metastore.tf`** - Azure Unity Catalog metastore and workspace assignment
- **`aws-metastore.tf`** - AWS Unity Catalog metastore and workspace assignment

## Resources Managed

### Development Catalogs
- `dev_bronze` - Raw data ingestion
- `dev_silver` - Cleaned and validated data
- `dev_gold` - Business-ready aggregated data

### Production Catalogs
- `bronze` - Raw data ingestion
- `silver` - Cleaned and validated data
- `gold` - Business-ready aggregated data

## Metastores

### Azure Metastore
- **Name**: `${project_name}-azure-metastore`
- **Storage**: Azure Data Lake Storage (abfss://)
- **Region**: Configurable (default: East US)
- **Delta Sharing**: Optional support for data sharing

### AWS Metastore
- **Name**: `${project_name}-aws-metastore`
- **Storage**: S3 bucket
- **Region**: Configurable (default: us-east-1)
- **Delta Sharing**: Optional support for data sharing

## Permission Model

### Development Catalogs
- **All User Groups**: `ALL_PRIVILEGES` - Full read/write access for development and testing
- **Dev Service Principal Group**: `ALL_PRIVILEGES` - Full ownership for dev service principals

### Production Catalogs
- **Production Service Principal Group**: `ALL_PRIVILEGES` - Full ownership for production data pipelines
- **All User Groups**: Granular privileges - `USE_CATALOG`, `USE_SCHEMA`, `SELECT`, `EXECUTE`, `REFRESH`, `READ_VOLUME`

## Medallion Architecture

The catalogs follow the medallion architecture pattern:

1. **Bronze Layer**: Raw data ingestion from external sources
2. **Silver Layer**: Data cleaning, validation, and standardization
3. **Gold Layer**: Business-ready aggregated data for analytics and reporting

## Usage

```sql
-- Development environment
USE CATALOG dev_bronze;  -- Raw data
USE CATALOG dev_silver;  -- Cleaned data
USE CATALOG dev_gold;    -- Business-ready data

-- Production environment
USE CATALOG bronze;  -- Raw data
USE CATALOG silver;  -- Cleaned data
USE CATALOG gold;    -- Business-ready data
```
