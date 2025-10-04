# Databricks Groups
# This file manages groups and group memberships for the DataPlatform

# Create three groups: dev, dev-admin, admin
resource "databricks_group" "dev" {
  display_name = "dev"
  
  # Add custom tags
  custom_tags = merge(var.common_tags, {
    "group_type" = "development"
    "purpose" = "development-team"
  })
}

resource "databricks_group" "dev_admin" {
  display_name = "dev-admin"
  
  # Add custom tags
  custom_tags = merge(var.common_tags, {
    "group_type" = "development-admin"
    "purpose" = "development-administrators"
  })
}

resource "databricks_group" "admin" {
  display_name = "admin"
  
  # Add custom tags
  custom_tags = merge(var.common_tags, {
    "group_type" = "administration"
    "purpose" = "administrators"
  })
}

# Service Principal Groups
resource "databricks_group" "prod_service_principals" {
  display_name = "prod-service-principals"
  
  # Add custom tags
  custom_tags = merge(var.common_tags, {
    "group_type" = "service-principals"
    "environment" = "prod"
    "purpose" = "production-service-principals"
  })
}

resource "databricks_group" "dev_service_principals" {
  display_name = "dev-service-principals"
  
  # Add custom tags
  custom_tags = merge(var.common_tags, {
    "group_type" = "service-principals"
    "environment" = "dev"
    "purpose" = "development-service-principals"
  })
}

# Assign users to groups
# Test user goes to dev group
resource "databricks_group_member" "dev_test" {
  group_id  = databricks_group.dev.id
  member_id = databricks_user.test.id
}

# QA user goes to dev-admin group
resource "databricks_group_member" "dev_admin_qa" {
  group_id  = databricks_group.dev_admin.id
  member_id = databricks_user.qa.id
}

# Prod user goes to admin group
resource "databricks_group_member" "admin_prod" {
  group_id  = databricks_group.admin.id
  member_id = databricks_user.prod.id
}

# Assign service principals to their respective groups
# Production service principal goes to prod-service-principals group
resource "databricks_group_member" "prod_sp_group" {
  group_id  = databricks_group.prod_service_principals.id
  member_id = databricks_service_principal.prod_sp.id
}

# Development service principal goes to dev-service-principals group
resource "databricks_group_member" "dev_sp_group" {
  group_id  = databricks_group.dev_service_principals.id
  member_id = databricks_service_principal.dev_sp.id
}
