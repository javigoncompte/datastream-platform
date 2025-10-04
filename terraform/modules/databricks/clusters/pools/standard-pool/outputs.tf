# Outputs for Standard Pool submodule

output "standard_pool_id" {
  description = "ID of the standard instance pool"
  value       = databricks_instance_pool.standard_pool.id
}

output "standard_pool_name" {
  description = "Name of the standard instance pool"
  value       = databricks_instance_pool.standard_pool.instance_pool_name
}

output "standard_pool_url" {
  description = "URL to view the standard instance pool in Databricks"
  value       = "${var.databricks_host}/#/setting/clusters/instance-pools/view/${databricks_instance_pool.standard_pool.id}"
}
