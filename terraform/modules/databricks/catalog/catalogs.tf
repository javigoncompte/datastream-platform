# Unity Catalog Catalogs
# This file manages Unity Catalog catalogs for medallion architecture

# Development Environment Catalogs
resource "databricks_catalog" "dev_bronze" {
  name    = "dev_bronze"
  comment = "Development bronze catalog for raw data"
  
  # Add custom tags
  custom_tags = merge(var.common_tags, {
    "catalog_type" = "bronze"
    "environment"  = "dev"
    "data_layer"   = "raw"
    "purpose"      = "development-raw-data"
  })
}

resource "databricks_catalog" "dev_silver" {
  name    = "dev_silver"
  comment = "Development silver catalog for cleaned data"
  
  # Add custom tags
  custom_tags = merge(var.common_tags, {
    "catalog_type" = "silver"
    "environment"  = "dev"
    "data_layer"   = "cleaned"
    "purpose"      = "development-cleaned-data"
  })
}

resource "databricks_catalog" "dev_gold" {
  name    = "dev_gold"
  comment = "Development gold catalog for business-ready data"
  
  # Add custom tags
  custom_tags = merge(var.common_tags, {
    "catalog_type" = "gold"
    "environment"  = "dev"
    "data_layer"   = "business"
    "purpose"      = "development-business-data"
  })
}

# Production Environment Catalogs
resource "databricks_catalog" "bronze" {
  name    = "bronze"
  comment = "Production bronze catalog for raw data"
  
  # Add custom tags
  custom_tags = merge(var.common_tags, {
    "catalog_type" = "bronze"
    "environment"  = "prod"
    "data_layer"   = "raw"
    "purpose"      = "production-raw-data"
  })
}

resource "databricks_catalog" "silver" {
  name    = "silver"
  comment = "Production silver catalog for cleaned data"
  
  # Add custom tags
  custom_tags = merge(var.common_tags, {
    "catalog_type" = "silver"
    "environment"  = "prod"
    "data_layer"   = "cleaned"
    "purpose"      = "production-cleaned-data"
  })
}

resource "databricks_catalog" "gold" {
  name    = "gold"
  comment = "Production gold catalog for business-ready data"
  
  # Add custom tags
  custom_tags = merge(var.common_tags, {
    "catalog_type" = "gold"
    "environment"  = "prod"
    "data_layer"   = "business"
    "purpose"      = "production-business-data"
  })
}
