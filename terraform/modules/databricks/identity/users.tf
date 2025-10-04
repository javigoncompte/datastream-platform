# Databricks Users
# This file manages users for the DataPlatform

# Create three principals (users)
resource "databricks_user" "test" {
  user_name    = "test@${var.project_name}.com"
  display_name = "Test User"
  
  # Add custom tags
  custom_tags = merge(var.common_tags, {
    "role" = "test"
    "purpose" = "testing"
  })
}

resource "databricks_user" "qa" {
  user_name    = "qa@${var.project_name}.com"
  display_name = "QA User"
  
  # Add custom tags
  custom_tags = merge(var.common_tags, {
    "role" = "qa"
    "purpose" = "quality-assurance"
  })
}

resource "databricks_user" "prod" {
  user_name    = "prod@${var.project_name}.com"
  display_name = "Production User"
  
  # Add custom tags
  custom_tags = merge(var.common_tags, {
    "role" = "prod"
    "purpose" = "production"
  })
}
