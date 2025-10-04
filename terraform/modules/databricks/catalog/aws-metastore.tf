# AWS Unity Catalog Metastore
# This file manages AWS Unity Catalog metastore and workspace assignment

# AWS Metastore
resource "databricks_metastore" "aws_metastore" {
  name          = "${var.project_name}-aws-metastore"
  storage_root  = "s3://${var.aws_s3_bucket}/metastore/"
  region        = var.aws_region
  
  # Add custom tags
  custom_tags = merge(var.common_tags, {
    "cloud_provider" = "aws"
    "environment"    = var.environment
    "purpose"        = "unity-catalog-metastore"
  })
  
  # Optional: Enable delta sharing
  delta_sharing_scope = var.enable_delta_sharing ? "INTERNAL_AND_EXTERNAL" : "INTERNAL"
  delta_sharing_recipient_token_lifetime_in_seconds = var.enable_delta_sharing ? 3600 : null
  delta_sharing_organization_name = var.enable_delta_sharing ? var.project_name : null
}

# Metastore Assignment for AWS Workspace
resource "databricks_metastore_assignment" "aws_workspace" {
  count = var.assign_aws_metastore ? 1 : 0
  
  workspace_id = var.aws_workspace_id
  metastore_id = databricks_metastore.aws_metastore.id
  
  # Assign default catalog
  default_catalog_name = var.aws_default_catalog_name
}
