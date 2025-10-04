# Unity Catalog Permissions
# This file manages permissions for Unity Catalog catalogs

# Development Catalog Permissions - All groups get all privileges
resource "databricks_permissions" "dev_bronze_permissions" {
  catalog = databricks_catalog.dev_bronze.name
  
  access_control {
    group_name       = databricks_group.dev.display_name
    permission_level = "ALL_PRIVILEGES"
  }
  
  access_control {
    group_name       = databricks_group.dev_admin.display_name
    permission_level = "ALL_PRIVILEGES"
  }
  
  access_control {
    group_name       = databricks_group.admin.display_name
    permission_level = "ALL_PRIVILEGES"
  }
  
  # Dev service principal group gets ALL_PRIVILEGES
  access_control {
    group_name       = databricks_group.dev_service_principals.display_name
    permission_level = "ALL_PRIVILEGES"
  }
}

resource "databricks_permissions" "dev_silver_permissions" {
  catalog = databricks_catalog.dev_silver.name
  
  access_control {
    group_name       = databricks_group.dev.display_name
    permission_level = "ALL_PRIVILEGES"
  }
  
  access_control {
    group_name       = databricks_group.dev_admin.display_name
    permission_level = "ALL_PRIVILEGES"
  }
  
  access_control {
    group_name       = databricks_group.admin.display_name
    permission_level = "ALL_PRIVILEGES"
  }
  
  # Dev service principal group gets ALL_PRIVILEGES
  access_control {
    group_name       = databricks_group.dev_service_principals.display_name
    permission_level = "ALL_PRIVILEGES"
  }
}

resource "databricks_permissions" "dev_gold_permissions" {
  catalog = databricks_catalog.dev_gold.name
  
  access_control {
    group_name       = databricks_group.dev.display_name
    permission_level = "ALL_PRIVILEGES"
  }
  
  access_control {
    group_name       = databricks_group.dev_admin.display_name
    permission_level = "ALL_PRIVILEGES"
  }
  
  access_control {
    group_name       = databricks_group.admin.display_name
    permission_level = "ALL_PRIVILEGES"
  }
  
  # Dev service principal group gets ALL_PRIVILEGES
  access_control {
    group_name       = databricks_group.dev_service_principals.display_name
    permission_level = "ALL_PRIVILEGES"
  }
}

# Production Catalog Permissions - Only prod service principal can write
resource "databricks_permissions" "bronze_permissions" {
  catalog = databricks_catalog.bronze.name
  
  # Prod service principal group gets ALL_PRIVILEGES (write access)
  access_control {
    group_name       = databricks_group.prod_service_principals.display_name
    permission_level = "ALL_PRIVILEGES"
  }
}

# Additional granular permissions for groups on production catalogs
resource "databricks_grants" "bronze_grants" {
  catalog = databricks_catalog.bronze.name
  
  grant {
    principal  = databricks_group.dev.display_name
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT", "EXECUTE", "REFRESH", "READ_VOLUME"]
  }
  
  grant {
    principal  = databricks_group.dev_admin.display_name
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT", "EXECUTE", "REFRESH", "READ_VOLUME"]
  }
  
  grant {
    principal  = databricks_group.admin.display_name
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT", "EXECUTE", "REFRESH", "READ_VOLUME"]
  }
}

resource "databricks_permissions" "silver_permissions" {
  catalog = databricks_catalog.silver.name
  
  # Prod service principal group gets ALL_PRIVILEGES (write access)
  access_control {
    group_name       = databricks_group.prod_service_principals.display_name
    permission_level = "ALL_PRIVILEGES"
  }
}

# Additional granular permissions for groups on production catalogs
resource "databricks_grants" "silver_grants" {
  catalog = databricks_catalog.silver.name
  
  grant {
    principal  = databricks_group.dev.display_name
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT", "EXECUTE", "REFRESH", "READ_VOLUME"]
  }
  
  grant {
    principal  = databricks_group.dev_admin.display_name
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT", "EXECUTE", "REFRESH", "READ_VOLUME"]
  }
  
  grant {
    principal  = databricks_group.admin.display_name
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT", "EXECUTE", "REFRESH", "READ_VOLUME"]
  }
}

resource "databricks_permissions" "gold_permissions" {
  catalog = databricks_catalog.gold.name
  
  # Prod service principal group gets ALL_PRIVILEGES (write access)
  access_control {
    group_name       = databricks_group.prod_service_principals.display_name
    permission_level = "ALL_PRIVILEGES"
  }
}

# Additional granular permissions for groups on production catalogs
resource "databricks_grants" "gold_grants" {
  catalog = databricks_catalog.gold.name
  
  grant {
    principal  = databricks_group.dev.display_name
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT", "EXECUTE", "REFRESH", "READ_VOLUME"]
  }
  
  grant {
    principal  = databricks_group.dev_admin.display_name
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT", "EXECUTE", "REFRESH", "READ_VOLUME"]
  }
  
  grant {
    principal  = databricks_group.admin.display_name
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT", "EXECUTE", "REFRESH", "READ_VOLUME"]
  }
}
