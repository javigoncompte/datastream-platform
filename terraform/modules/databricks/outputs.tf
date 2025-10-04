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
