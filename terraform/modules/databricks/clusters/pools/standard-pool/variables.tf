# Variables for Standard Pool submodule
# Only Databricks connection and environment variables are required

variable "databricks_host" {
  description = "Databricks workspace URL"
  type        = string
  default     = ""
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

# Standard Pool configuration
variable "standard_pool_name" {
  description = "Name of the standard instance pool"
  type        = string
  default     = "standard_pool"
}

variable "standard_pool_node_type" {
  description = "Node type for the standard instance pool"
  type        = string
  default     = "rd-fleet.xlarge"
}

variable "standard_pool_min_idle" {
  description = "Minimum number of idle instances in the pool"
  type        = number
  default     = 0
}

variable "standard_pool_max_capacity" {
  description = "Maximum number of instances in the pool"
  type        = number
  default     = 10
}

variable "standard_pool_idle_timeout" {
  description = "Auto-termination time for idle instances (minutes)"
  type        = number
  default     = 20
}

variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "dataplatform-data-platform"
}

variable "common_tags" {
  description = "Common tags to apply to resources"
  type        = map(string)
  default     = {}
}
