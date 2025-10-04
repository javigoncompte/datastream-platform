# Databricks Module
# This module manages all Databricks-related infrastructure

terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}

# Data source to get current user info
data "databricks_current_user" "me" {}

# Local values for common tags
locals {
  common_tags = {
    "project"     = var.project_name
    "environment" = var.environment
    "managed_by"  = "terraform"
    "created_by"  = var.service_principal_name
  }
}

# ML Pool submodule
module "ml_pool" {
  source = "./clusters/pools"
  
  # Pass only shared variables - ML pool submodule will use its own defaults
  databricks_host = var.databricks_host
  common_tags     = local.common_tags
}

# Standard Pool submodule
module "standard_pool" {
  source = "./clusters/pools/standard-pool"
  
  # Pass only shared variables to the submodule
  databricks_host = var.databricks_host
  common_tags     = local.common_tags
}

# Data source to get service principal info
data "databricks_service_principal" "sp" {
  display_name = var.service_principal_name
}
