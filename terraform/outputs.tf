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
