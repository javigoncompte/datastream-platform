# Outputs for Databricks Module
# These outputs reference the ML pool from the clusters/pools submodule

output "ml_pool_id" {
  description = "ID of the ML instance pool"
  value       = module.ml_pool.ml_pool_id
}

output "ml_pool_name" {
  description = "Name of the ML instance pool"
  value       = module.ml_pool.ml_pool_name
}

output "ml_pool_url" {
  description = "URL to view the ML instance pool in Databricks"
  value       = module.ml_pool.ml_pool_url
}

# Standard Pool Outputs
output "standard_pool_id" {
  description = "ID of the standard instance pool"
  value       = module.standard_pool.standard_pool_id
}

output "standard_pool_name" {
  description = "Name of the standard instance pool"
  value       = module.standard_pool.standard_pool_name
}

output "standard_pool_url" {
  description = "URL to view the standard instance pool in Databricks"
  value       = module.standard_pool.standard_pool_url
}

# User Outputs
output "test_user_id" {
  description = "ID of the test user"
  value       = databricks_user.test.id
}

output "test_user_name" {
  description = "Username of the test user"
  value       = databricks_user.test.user_name
}

output "qa_user_id" {
  description = "ID of the QA user"
  value       = databricks_user.qa.id
}

output "qa_user_name" {
  description = "Username of the QA user"
  value       = databricks_user.qa.user_name
}

output "prod_user_id" {
  description = "ID of the production user"
  value       = databricks_user.prod.id
}

output "prod_user_name" {
  description = "Username of the production user"
  value       = databricks_user.prod.user_name
}

# Group Outputs
output "dev_group_id" {
  description = "ID of the dev group"
  value       = databricks_group.dev.id
}

output "dev_group_name" {
  description = "Display name of the dev group"
  value       = databricks_group.dev.display_name
}

output "dev_admin_group_id" {
  description = "ID of the dev-admin group"
  value       = databricks_group.dev_admin.id
}

output "dev_admin_group_name" {
  description = "Display name of the dev-admin group"
  value       = databricks_group.dev_admin.display_name
}

output "admin_group_id" {
  description = "ID of the admin group"
  value       = databricks_group.admin.id
}

output "admin_group_name" {
  description = "Display name of the admin group"
  value       = databricks_group.admin.display_name
}

# Service Principal Group Outputs
output "prod_service_principals_group_id" {
  description = "ID of the prod-service-principals group"
  value       = databricks_group.prod_service_principals.id
}

output "prod_service_principals_group_name" {
  description = "Display name of the prod-service-principals group"
  value       = databricks_group.prod_service_principals.display_name
}

output "dev_service_principals_group_id" {
  description = "ID of the dev-service-principals group"
  value       = databricks_group.dev_service_principals.id
}

output "dev_service_principals_group_name" {
  description = "Display name of the dev-service-principals group"
  value       = databricks_group.dev_service_principals.display_name
}

# Service Principal Outputs
output "prod_service_principal_id" {
  description = "ID of the production service principal"
  value       = databricks_service_principal.prod_sp.id
}

output "prod_service_principal_application_id" {
  description = "Application ID of the production service principal"
  value       = databricks_service_principal.prod_sp.application_id
}

output "prod_service_principal_display_name" {
  description = "Display name of the production service principal"
  value       = databricks_service_principal.prod_sp.display_name
}

output "prod_service_principal_secret_id" {
  description = "ID of the production service principal secret"
  value       = databricks_service_principal_secret.prod_sp_secret.id
}

output "prod_service_principal_secret_value" {
  description = "Secret value for the production service principal (sensitive)"
  value       = databricks_service_principal_secret.prod_sp_secret.secret
  sensitive   = true
}

output "dev_service_principal_id" {
  description = "ID of the development service principal"
  value       = databricks_service_principal.dev_sp.id
}

output "dev_service_principal_application_id" {
  description = "Application ID of the development service principal"
  value       = databricks_service_principal.dev_sp.application_id
}

output "dev_service_principal_display_name" {
  description = "Display name of the development service principal"
  value       = databricks_service_principal.dev_sp.display_name
}

output "dev_service_principal_secret_id" {
  description = "ID of the development service principal secret"
  value       = databricks_service_principal_secret.dev_sp_secret.id
}

output "dev_service_principal_secret_value" {
  description = "Secret value for the development service principal (sensitive)"
  value       = databricks_service_principal_secret.dev_sp_secret.secret
  sensitive   = true
}

# CI/CD Service Principal Outputs
output "ci_cd_service_principal_id" {
  description = "ID of the CI/CD service principal"
  value       = databricks_service_principal.ci_cd_sp.id
}

output "ci_cd_service_principal_application_id" {
  description = "Application ID of the CI/CD service principal"
  value       = databricks_service_principal.ci_cd_sp.application_id
}

output "ci_cd_service_principal_display_name" {
  description = "Display name of the CI/CD service principal"
  value       = databricks_service_principal.ci_cd_sp.display_name
}

output "ci_cd_service_principal_secret_id" {
  description = "ID of the CI/CD service principal secret"
  value       = databricks_service_principal_secret.ci_cd_sp_secret.id
}

output "ci_cd_service_principal_secret_value" {
  description = "Secret value for the CI/CD service principal (sensitive)"
  value       = databricks_service_principal_secret.ci_cd_sp_secret.secret
  sensitive   = true
}

# Catalog Outputs
output "dev_bronze_catalog_name" {
  description = "Name of the dev_bronze catalog"
  value       = databricks_catalog.dev_bronze.name
}

output "dev_silver_catalog_name" {
  description = "Name of the dev_silver catalog"
  value       = databricks_catalog.dev_silver.name
}

output "dev_gold_catalog_name" {
  description = "Name of the dev_gold catalog"
  value       = databricks_catalog.dev_gold.name
}

output "bronze_catalog_name" {
  description = "Name of the bronze catalog"
  value       = databricks_catalog.bronze.name
}

output "silver_catalog_name" {
  description = "Name of the silver catalog"
  value       = databricks_catalog.silver.name
}

output "gold_catalog_name" {
  description = "Name of the gold catalog"
  value       = databricks_catalog.gold.name
}

# Catalog Permissions Outputs
output "dev_catalog_permissions_summary" {
  description = "Summary of development catalog permissions"
  value = {
    dev_bronze = "All groups have ALL_PRIVILEGES"
    dev_silver = "All groups have ALL_PRIVILEGES"
    dev_gold   = "All groups have ALL_PRIVILEGES"
  }
}

output "prod_catalog_permissions_summary" {
  description = "Summary of production catalog permissions"
  value = {
    bronze = "Prod SP Group has ALL_PRIVILEGES, groups have granular privileges (USE_CATALOG, USE_SCHEMA, SELECT, EXECUTE, REFRESH, READ_VOLUME)"
    silver = "Prod SP Group has ALL_PRIVILEGES, groups have granular privileges (USE_CATALOG, USE_SCHEMA, SELECT, EXECUTE, REFRESH, READ_VOLUME)"
    gold   = "Prod SP Group has ALL_PRIVILEGES, groups have granular privileges (USE_CATALOG, USE_SCHEMA, SELECT, EXECUTE, REFRESH, READ_VOLUME)"
  }
}

# Metastore Outputs
output "azure_metastore_id" {
  description = "ID of the Azure metastore"
  value       = databricks_metastore.azure_metastore.id
}

output "azure_metastore_name" {
  description = "Name of the Azure metastore"
  value       = databricks_metastore.azure_metastore.name
}

output "azure_metastore_storage_root" {
  description = "Storage root of the Azure metastore"
  value       = databricks_metastore.azure_metastore.storage_root
}

output "azure_metastore_region" {
  description = "Region of the Azure metastore"
  value       = databricks_metastore.azure_metastore.region
}

output "aws_metastore_id" {
  description = "ID of the AWS metastore"
  value       = databricks_metastore.aws_metastore.id
}

output "aws_metastore_name" {
  description = "Name of the AWS metastore"
  value       = databricks_metastore.aws_metastore.name
}

output "aws_metastore_storage_root" {
  description = "Storage root of the AWS metastore"
  value       = databricks_metastore.aws_metastore.storage_root
}

output "aws_metastore_region" {
  description = "Region of the AWS metastore"
  value       = databricks_metastore.aws_metastore.region
}

# Metastore Assignment Outputs
output "azure_metastore_assignment_id" {
  description = "ID of the Azure metastore assignment"
  value       = var.assign_azure_metastore ? databricks_metastore_assignment.azure_workspace[0].id : null
}

output "aws_metastore_assignment_id" {
  description = "ID of the AWS metastore assignment"
  value       = var.assign_aws_metastore ? databricks_metastore_assignment.aws_workspace[0].id : null
}
