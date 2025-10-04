# Outputs for Pools Module
# These outputs expose the ML pool submodule outputs

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
