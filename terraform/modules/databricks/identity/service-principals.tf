# Databricks Service Principal with OAuth Configuration
# This file manages service principal authentication for AWS deployments

# Create production service principal for OAuth token authentication
resource "databricks_service_principal" "prod_sp" {
  display_name = "prod-oauth-sp"
  application_id = var.prod_service_principal_application_id
  
  # Add custom tags
  custom_tags = merge(var.common_tags, {
    "purpose" = "oauth-authentication"
    "environment" = "prod"
    "deployment_target" = "aws"
    "auth_method" = "oauth"
  })
}

# Create production service principal secret for OAuth token authentication
resource "databricks_service_principal_secret" "prod_sp_secret" {
  service_principal_id = databricks_service_principal.prod_sp.id
  
  # Secret configuration
  comment = "OAuth secret for production service principal"
  
  # Secret will be automatically generated and stored securely
  # The secret value will be available in the output
}

# Create development service principal for OAuth token authentication
resource "databricks_service_principal" "dev_sp" {
  display_name = "dev-oauth-sp"
  application_id = var.dev_service_principal_application_id
  
  # Add custom tags
  custom_tags = merge(var.common_tags, {
    "purpose" = "oauth-authentication"
    "environment" = "dev"
    "deployment_target" = "aws"
    "auth_method" = "oauth"
  })
}

# Create development service principal secret for OAuth token authentication
resource "databricks_service_principal_secret" "dev_sp_secret" {
  service_principal_id = databricks_service_principal.dev_sp.id
  
  # Secret configuration
  comment = "OAuth secret for development service principal"
  
  # Secret will be automatically generated and stored securely
  # The secret value will be available in the output
}

# Optional: Create additional CI/CD service principal for different purposes
resource "databricks_service_principal" "ci_cd_sp" {
  display_name = "${var.environment}-ci-cd-sp"
  application_id = var.ci_cd_service_principal_application_id != null ? var.ci_cd_service_principal_application_id : null
  
  # Add custom tags
  custom_tags = merge(var.common_tags, {
    "purpose" = "ci-cd-authentication"
    "deployment_target" = "aws"
    "auth_method" = "oauth"
  })
}

# Create secret for CI/CD service principal
resource "databricks_service_principal_secret" "ci_cd_sp_secret" {
  service_principal_id = databricks_service_principal.ci_cd_sp.id
  
  # Secret configuration
  comment = "CI/CD secret for ${var.environment} service principal"
}
