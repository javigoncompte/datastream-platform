# Standard Pool submodule configuration
# This file includes the provider configuration for the standard pool submodule

terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}
