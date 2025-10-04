# Pools module configuration
# This file orchestrates the pool submodules

# ML Pool submodule
module "ml_pool" {
  source = "./ml-pool"
  
  # Pass only shared variables - ML pool submodule will use its own defaults
  databricks_host = var.databricks_host
  common_tags     = var.common_tags
}