# ML Pool submodule configuration
# This file includes the provider configuration for the ML pool submodule

terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}
