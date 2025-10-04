# Azure Unity Catalog Metastore
# This file manages Azure Unity Catalog metastore and workspace assignment

# Azure Metastore
resource "databricks_metastore" "azure_metastore" {
  name          = "${var.project_name}-azure-metastore"
  storage_root  = "abfss://${var.azure_storage_container}@${var.azure_storage_account}.dfs.core.windows.net/"
  region        = var.azure_region
  
  # Add custom tags
  custom_tags = merge(var.common_tags, {
    "cloud_provider" = "azure"
    "environment"    = var.environment
    "purpose"        = "unity-catalog-metastore"
  })
  
  # Optional: Enable delta sharing
  delta_sharing_scope = var.enable_delta_sharing ? "INTERNAL_AND_EXTERNAL" : "INTERNAL"
  delta_sharing_recipient_token_lifetime_in_seconds = var.enable_delta_sharing ? 3600 : null
  delta_sharing_organization_name = var.enable_delta_sharing ? var.project_name : null
}

# Metastore Assignment for Azure Workspace
resource "databricks_metastore_assignment" "azure_workspace" {
  count = var.assign_azure_metastore ? 1 : 0
  
  workspace_id = var.azure_workspace_id
  metastore_id = databricks_metastore.azure_metastore.id
  
  # Assign default catalog
  default_catalog_name = var.azure_default_catalog_name
}
