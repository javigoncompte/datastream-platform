# Standard Instance Pool with 16.4 LTS Runtime
# This creates a permanent instance pool optimized for general workloads

resource "databricks_instance_pool" "standard_pool" {
  instance_pool_name = var.standard_pool_name
  node_type_id       = var.standard_pool_node_type
  min_idle_instances  = var.standard_pool_min_idle
  max_capacity        = var.standard_pool_max_capacity
  idle_instance_autotermination_minutes = var.standard_pool_idle_timeout

  # Disk configuration for general workloads
  disk_spec {
    disk_type {
      ebs_volume_type = "GENERAL_PURPOSE_SSD"
    }
    disk_size  = 100
    disk_count = 1
  }

  # Enable elastic disk for cost optimization (autopilot/autoscaling local storage)
  enable_elastic_disk = true

  # Use Photon preloaded runtimes
  preloaded_spark_versions = ["16.4.x-photon-scala2.12"]

  # Custom tags for resource management
  custom_tags = merge(var.common_tags, {
    "purpose"        = "general-workloads"
    "runtime"        = "16.4-lts"
    "spark_version"  = "3.5.2"
    "scala_version"  = "2.12"
    "pool_type"      = "standard"
    "photon_enabled" = "true"
    "autopilot_enabled" = "true"
  })
}
