# Variables for Pools Module
# Only shared variables are passed from the parent databricks module

variable "databricks_host" {
  description = "Databricks workspace URL"
  type        = string
}

variable "common_tags" {
  description = "Common tags to apply to resources"
  type        = map(string)
}
