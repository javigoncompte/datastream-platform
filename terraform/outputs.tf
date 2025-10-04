# Root Outputs for DataPlatform Infrastructure

# Re-export module outputs
output "ml_pool_id" {
  description = "ID of the ML instance pool"
  value       = module.databricks.ml_pool_id
}

output "ml_pool_name" {
  description = "Name of the ML instance pool"
  value       = module.databricks.ml_pool_name
}

output "ml_pool_url" {
  description = "URL to view the ML instance pool in Databricks"
  value       = module.databricks.ml_pool_url
}

output "environment" {
  description = "Environment name"
  value       = var.environment
}

output "workspace_host" {
  description = "Databricks workspace host"
  value       = var.databricks_host
}

# User Outputs
output "test_user_id" {
  description = "ID of the test user"
  value       = module.databricks.test_user_id
}

output "test_user_name" {
  description = "Username of the test user"
  value       = module.databricks.test_user_name
}

output "qa_user_id" {
  description = "ID of the QA user"
  value       = module.databricks.qa_user_id
}

output "qa_user_name" {
  description = "Username of the QA user"
  value       = module.databricks.qa_user_name
}

output "prod_user_id" {
  description = "ID of the production user"
  value       = module.databricks.prod_user_id
}

output "prod_user_name" {
  description = "Username of the production user"
  value       = module.databricks.prod_user_name
}

# Group Outputs
output "dev_group_id" {
  description = "ID of the dev group"
  value       = module.databricks.dev_group_id
}

output "dev_group_name" {
  description = "Display name of the dev group"
  value       = module.databricks.dev_group_name
}

output "dev_admin_group_id" {
  description = "ID of the dev-admin group"
  value       = module.databricks.dev_admin_group_id
}

output "dev_admin_group_name" {
  description = "Display name of the dev-admin group"
  value       = module.databricks.dev_admin_group_name
}

output "admin_group_id" {
  description = "ID of the admin group"
  value       = module.databricks.admin_group_id
}

output "admin_group_name" {
  description = "Display name of the admin group"
  value       = module.databricks.admin_group_name
}

# Service Principal Group Outputs
output "prod_service_principals_group_id" {
  description = "ID of the prod-service-principals group"
  value       = module.databricks.prod_service_principals_group_id
}

output "prod_service_principals_group_name" {
  description = "Display name of the prod-service-principals group"
  value       = module.databricks.prod_service_principals_group_name
}

output "dev_service_principals_group_id" {
  description = "ID of the dev-service-principals group"
  value       = module.databricks.dev_service_principals_group_id
}

output "dev_service_principals_group_name" {
  description = "Display name of the dev-service-principals group"
  value       = module.databricks.dev_service_principals_group_name
}

# Service Principal Outputs
output "prod_service_principal_id" {
  description = "ID of the production service principal"
  value       = module.databricks.prod_service_principal_id
}

output "prod_service_principal_application_id" {
  description = "Application ID of the production service principal"
  value       = module.databricks.prod_service_principal_application_id
}

output "prod_service_principal_display_name" {
  description = "Display name of the production service principal"
  value       = module.databricks.prod_service_principal_display_name
}

output "prod_service_principal_secret_id" {
  description = "ID of the production service principal secret"
  value       = module.databricks.prod_service_principal_secret_id
}

output "prod_service_principal_secret_value" {
  description = "Secret value for the production service principal (sensitive)"
  value       = module.databricks.prod_service_principal_secret_value
  sensitive   = true
}

output "dev_service_principal_id" {
  description = "ID of the development service principal"
  value       = module.databricks.dev_service_principal_id
}

output "dev_service_principal_application_id" {
  description = "Application ID of the development service principal"
  value       = module.databricks.dev_service_principal_application_id
}

output "dev_service_principal_display_name" {
  description = "Display name of the development service principal"
  value       = module.databricks.dev_service_principal_display_name
}

output "dev_service_principal_secret_id" {
  description = "ID of the development service principal secret"
  value       = module.databricks.dev_service_principal_secret_id
}

output "dev_service_principal_secret_value" {
  description = "Secret value for the development service principal (sensitive)"
  value       = module.databricks.dev_service_principal_secret_value
  sensitive   = true
}

# CI/CD Service Principal Outputs
output "ci_cd_service_principal_id" {
  description = "ID of the CI/CD service principal"
  value       = module.databricks.ci_cd_service_principal_id
}

output "ci_cd_service_principal_application_id" {
  description = "Application ID of the CI/CD service principal"
  value       = module.databricks.ci_cd_service_principal_application_id
}

output "ci_cd_service_principal_display_name" {
  description = "Display name of the CI/CD service principal"
  value       = module.databricks.ci_cd_service_principal_display_name
}

output "ci_cd_service_principal_secret_id" {
  description = "ID of the CI/CD service principal secret"
  value       = module.databricks.ci_cd_service_principal_secret_id
}

output "ci_cd_service_principal_secret_value" {
  description = "Secret value for the CI/CD service principal (sensitive)"
  value       = module.databricks.ci_cd_service_principal_secret_value
  sensitive   = true
}

# Catalog Outputs
output "dev_bronze_catalog_name" {
  description = "Name of the dev_bronze catalog"
  value       = module.databricks.dev_bronze_catalog_name
}

output "dev_silver_catalog_name" {
  description = "Name of the dev_silver catalog"
  value       = module.databricks.dev_silver_catalog_name
}

output "dev_gold_catalog_name" {
  description = "Name of the dev_gold catalog"
  value       = module.databricks.dev_gold_catalog_name
}

output "bronze_catalog_name" {
  description = "Name of the bronze catalog"
  value       = module.databricks.bronze_catalog_name
}

output "silver_catalog_name" {
  description = "Name of the silver catalog"
  value       = module.databricks.silver_catalog_name
}

output "gold_catalog_name" {
  description = "Name of the gold catalog"
  value       = module.databricks.gold_catalog_name
}

# Catalog Permissions Outputs
output "dev_catalog_permissions_summary" {
  description = "Summary of development catalog permissions"
  value       = module.databricks.dev_catalog_permissions_summary
}

output "prod_catalog_permissions_summary" {
  description = "Summary of production catalog permissions"
  value       = module.databricks.prod_catalog_permissions_summary
}

# Metastore Outputs
output "azure_metastore_id" {
  description = "ID of the Azure metastore"
  value       = module.databricks.azure_metastore_id
}

output "azure_metastore_name" {
  description = "Name of the Azure metastore"
  value       = module.databricks.azure_metastore_name
}

output "azure_metastore_storage_root" {
  description = "Storage root of the Azure metastore"
  value       = module.databricks.azure_metastore_storage_root
}

output "azure_metastore_region" {
  description = "Region of the Azure metastore"
  value       = module.databricks.azure_metastore_region
}

output "aws_metastore_id" {
  description = "ID of the AWS metastore"
  value       = module.databricks.aws_metastore_id
}

output "aws_metastore_name" {
  description = "Name of the AWS metastore"
  value       = module.databricks.aws_metastore_name
}

output "aws_metastore_storage_root" {
  description = "Storage root of the AWS metastore"
  value       = module.databricks.aws_metastore_storage_root
}

output "aws_metastore_region" {
  description = "Region of the AWS metastore"
  value       = module.databricks.aws_metastore_region
}

# Metastore Assignment Outputs
output "azure_metastore_assignment_id" {
  description = "ID of the Azure metastore assignment"
  value       = module.databricks.azure_metastore_assignment_id
}

output "aws_metastore_assignment_id" {
  description = "ID of the AWS metastore assignment"
  value       = module.databricks.aws_metastore_assignment_id
}
