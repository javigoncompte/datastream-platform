# Variables for Databricks Module

variable "databricks_host" {
  description = "Databricks workspace URL"
  type        = string
}

variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "dataplatform-data-platform"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "service_principal_name" {
  description = "Name of the service principal for resource ownership tracking"
  type        = string
}


# contractors_group_name variable removed - no user permissions needed
