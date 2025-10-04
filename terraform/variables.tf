# Variables for DataPlatform Infrastructure

variable "databricks_profile" {
  description = "Databricks profile to use (determines environment and host automatically)"
  type        = string
  default     = null
}

variable "databricks_host" {
  description = "Databricks workspace URL (deprecated - use databricks_profile instead)"
  type        = string
  default     = null
}

variable "databricks_token" {
  description = "Databricks personal access token (alternative to service principal authentication)"
  type        = string
  sensitive   = true
  default     = null
}

# Service Principal Authentication Variables
variable "databricks_client_id" {
  description = "Databricks service principal client ID"
  type        = string
  sensitive   = true
  default     = null
}

variable "databricks_client_secret" {
  description = "Databricks service principal client secret"
  type        = string
  sensitive   = true
  default     = null
}

variable "service_principal_name" {
  description = "Name of the service principal for resource ownership tracking (defaults to sp-{environment}-ci)"
  type        = string
  default     = null
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "dataplatform-data-platform"
}

