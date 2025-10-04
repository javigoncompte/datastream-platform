# ML Instance Pool with 16.4 LTS ML Runtime
# This creates a permanent instance pool optimized for ML workloads

resource "databricks_instance_pool" "ml_pool" {
  instance_pool_name = var.ml_pool_name
  node_type_id       = var.ml_pool_node_type
  min_idle_instances  = var.ml_pool_min_idle
  max_capacity        = var.ml_pool_max_capacity
  idle_instance_autotermination_minutes = var.ml_pool_idle_timeout

  # Disk configuration for ML workloads
  disk_spec {
    disk_type {
      ebs_volume_type = "GENERAL_PURPOSE_SSD"
    }
    disk_size  = 100
    disk_count = 1
  }

  # Enable elastic disk for cost optimization
  enable_elastic_disk = true

  # Custom tags for resource management
  custom_tags = merge(var.common_tags, {
    "purpose"        = "ml-workloads"
    "runtime"        = "16.4-lts-ml"
    "spark_version"  = "3.5.2"
    "scala_version"  = "2.12"
    "pool_type"      = "ml"
  })
}