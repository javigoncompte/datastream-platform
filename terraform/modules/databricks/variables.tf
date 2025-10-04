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

# Service Principal OAuth Configuration
variable "prod_service_principal_application_id" {
  description = "Application ID for the production OAuth service principal (required for AWS deployments)"
  type        = string
  default     = null
}

variable "dev_service_principal_application_id" {
  description = "Application ID for the development OAuth service principal (required for AWS deployments)"
  type        = string
  default     = null
}

variable "ci_cd_service_principal_application_id" {
  description = "Application ID for the CI/CD service principal (optional)"
  type        = string
  default     = null
}

# Metastore Configuration Variables
variable "azure_storage_account" {
  description = "Azure storage account name for metastore"
  type        = string
  default     = null
}

variable "azure_storage_container" {
  description = "Azure storage container name for metastore"
  type        = string
  default     = "metastore"
}

variable "azure_region" {
  description = "Azure region for metastore"
  type        = string
  default     = "East US"
}

variable "aws_s3_bucket" {
  description = "AWS S3 bucket name for metastore"
  type        = string
  default     = null
}

variable "aws_region" {
  description = "AWS region for metastore"
  type        = string
  default     = "us-east-1"
}

variable "enable_delta_sharing" {
  description = "Enable Delta Sharing for metastores"
  type        = bool
  default     = false
}

# Workspace Assignment Variables
variable "assign_azure_metastore" {
  description = "Whether to assign Azure metastore to workspace"
  type        = bool
  default     = false
}

variable "assign_aws_metastore" {
  description = "Whether to assign AWS metastore to workspace"
  type        = bool
  default     = false
}

variable "azure_workspace_id" {
  description = "Azure workspace ID for metastore assignment"
  type        = string
  default     = null
}

variable "aws_workspace_id" {
  description = "AWS workspace ID for metastore assignment"
  type        = string
  default     = null
}

variable "azure_default_catalog_name" {
  description = "Default catalog name for Azure workspace"
  type        = string
  default     = "main"
}

variable "aws_default_catalog_name" {
  description = "Default catalog name for AWS workspace"
  type        = string
  default     = "main"
}


# contractors_group_name variable removed - no user permissions needed
