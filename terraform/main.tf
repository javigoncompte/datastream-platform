# Root Terraform configuration for DataPlatform Infrastructure
# This manages all infrastructure using modules

terraform {
  required_version = ">= 1.0"
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}

# Local values for environment detection
locals {
  # Map DATABRICKS_CONFIG_PROFILE to environment and host
  profile_config = {
    "DEFAULT" = {
      environment = "dev"
      host        = "https://*.cloud.databricks.com"
    }
    "dev" = {
      environment = "dev"
      host        = "https://*.cloud.databricks.com"
    }
    "prod" = {
      environment = "prod"
      host        = "https://*.cloud.databricks.com"
    }
  }
  
  # Get current profile from environment variable or default to "DEFAULT"
  current_profile = var.databricks_profile != null ? var.databricks_profile : "DEFAULT"
  
  # Get configuration for current profile
  current_config = local.profile_config[local.current_profile]
}

# Configure the Databricks Provider
provider "databricks" {
  host = local.current_config.host
  
  # Use service principal authentication
  client_id     = var.databricks_client_id
  client_secret = var.databricks_client_secret
}

# Databricks Module
module "databricks" {
  source = "./modules/databricks"
  
  # Pass variables to the module
  databricks_host = local.current_config.host
  environment = local.current_config.environment
  project_name = var.project_name
  service_principal_name = var.service_principal_name != null ? var.service_principal_name : "sp-${local.current_config.environment}-ci"
  
}
