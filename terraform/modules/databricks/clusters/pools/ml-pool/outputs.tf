# Outputs for ML Pool submodule

output "ml_pool_id" {
  description = "ID of the ML instance pool"
  value       = databricks_instance_pool.ml_pool.id
}

output "ml_pool_name" {
  description = "Name of the ML instance pool"
  value       = databricks_instance_pool.ml_pool.instance_pool_name
}

output "ml_pool_url" {
  description = "URL to view the ML instance pool in Databricks"
  value       = "${var.databricks_host}/#/setting/clusters/instance-pools/view/${databricks_instance_pool.ml_pool.id}"
}
