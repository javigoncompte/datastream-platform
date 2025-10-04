# Terraform Infrastructure

This directory contains Terraform configurations for managing permanent infrastructure resources for the DataPlatform using a modular structure.

## Structure

```
terraform/
├── main.tf                    # Root configuration (uses modules)
├── variables.tf                # Root variables
├── outputs.tf                # Root outputs
├── terraform.tfvars          # Environment values
├── deploy.sh                 # Deployment script
├── README.md                 # This file
└── modules/
    └── databricks/
        ├── main.tf           # Main module orchestrator
        ├── variables.tf      # Module variables
        ├── outputs.tf        # Module outputs
        ├── README.md         # Module documentation
        ├── clusters/         # Compute resources
        │   └── pools/        # Instance pools
        ├── identity/         # Identity management
        │   ├── users.tf      # User resources
        │   ├── groups.tf     # Group resources
        │   └── service-principals.tf # Service principal resources
        └── catalog/          # Unity Catalog management
            ├── catalogs.tf    # Catalog resources
            └── permissions.tf # Catalog permissions
```

### Key Files

- **`main.tf`** - Root configuration that orchestrates modules
- **`variables.tf`** - Root-level input variables
- **`outputs.tf`** - Root-level outputs
- **`modules/databricks/`** - Databricks infrastructure module
  - **`identity/`** - User, group, and service principal management
  - **`catalog/`** - Unity Catalog catalogs and permissions
  - **`clusters/`** - Compute resources (instance pools)
- **`terraform.tfvars.example`** - Example variables file

## Resources Managed

### ML Instance Pool
- **Runtime**: 16.4 LTS ML (includes Apache Spark 3.5.2, Scala 2.12)
- **Node Type**: i3.xlarge (configurable)
- **Auto-scaling**: 0-10 instances (configurable)
- **Auto-termination**: 15 minutes (configurable)
- **Disk**: 100GB gp3 EBS volume
- **Access**: Service principal managed (no user permissions needed)

### Standard Instance Pool
- **Runtime**: 16.4 LTS Photon (includes Apache Spark 3.5.2, Scala 2.12)
- **Node Type**: rd-fleet.xlarge (configurable)
- **Auto-scaling**: 0-10 instances (configurable)
- **Auto-termination**: 20 minutes (configurable)
- **Disk**: 100GB GENERAL_PURPOSE_SSD
- **Features**: Elastic disk enabled, Photon preloaded

### Users and Groups
- **Users**: test, qa, prod (with email addresses based on project name)
- **Groups**: dev, dev-admin, admin
- **Service Principal Groups**: prod-service-principals, dev-service-principals
- **Group Memberships**:
  - `test` user → `dev` group
  - `qa` user → `dev-admin` group
  - `prod` user → `admin` group
  - `prod-oauth-sp` → `prod-service-principals` group
  - `dev-oauth-sp` → `dev-service-principals` group

### Service Principals (AWS OAuth)
- **Production Service Principal**: `prod-oauth-sp` with OAuth token authentication
- **Development Service Principal**: `dev-oauth-sp` with OAuth token authentication
- **CI/CD Service Principal**: `${environment}-ci-cd-sp` for CI/CD pipelines
- **Secrets**: Automatically generated secrets for OAuth token authentication
- **Purpose**: Programmatic access to Databricks resources for AWS deployments

### Unity Catalog Metastores
- **Azure Metastore**: `${project_name}-azure-metastore` with Azure Data Lake Storage
- **AWS Metastore**: `${project_name}-aws-metastore` with S3 storage
- **Delta Sharing**: Optional support for data sharing across organizations
- **Workspace Assignment**: Automatic assignment to specified workspaces
- **Purpose**: Centralized metadata management for Unity Catalog across cloud providers

### Unity Catalog Catalogs (Medallion Architecture)
- **Development Catalogs**: dev_bronze, dev_silver, dev_gold
- **Production Catalogs**: bronze, silver, gold
- **Data Layers**:
  - **Bronze**: Raw data ingestion
  - **Silver**: Cleaned and validated data
  - **Gold**: Business-ready aggregated data
- **Purpose**: Organized data storage following medallion architecture patterns

### Catalog Permissions
- **Development Catalogs**: All groups (dev, dev-admin, admin) and dev-service-principals group have `ALL_PRIVILEGES`
- **Production Catalogs**: 
  - Production Service Principal Group (`prod-service-principals`) has `ALL_PRIVILEGES` (ownership/write access)
  - All user groups have granular privileges: `USE_CATALOG`, `USE_SCHEMA`, `SELECT`, `EXECUTE`, `REFRESH`, `READ_VOLUME`
- **Security Model**: Ensures production data integrity with proper ownership separation

## Setup

### 1. Configure Variables

```bash
# Copy the example variables file
cp terraform.tfvars.example terraform.tfvars

# Edit with your values (environment and host are automatically determined)
vim terraform.tfvars
```

### 2. Set Databricks Credentials (Choose one method)

**Method A: Environment Variable (Recommended)**
```bash
export DATABRICKS_TOKEN="your-databricks-token-here"
```

**Method B: Add to terraform.tfvars**
```bash
# Add this line to terraform.tfvars
databricks_token = "your-databricks-token-here"
```

### 3. Configure Service Principal OAuth (AWS Deployments)

For AWS deployments, you'll need to configure OAuth service principals:

```bash
# Add to terraform.tfvars
prod_service_principal_application_id = "your-prod-oauth-service-principal-application-id"
dev_service_principal_application_id = "your-dev-oauth-service-principal-application-id"
ci_cd_service_principal_application_id = "your-ci-cd-service-principal-application-id"  # Optional
```

**Note**: The `ci_cd_service_principal_application_id` is optional and only needed if you want a separate service principal for CI/CD pipelines.

### 4. Configure Unity Catalog Metastores (Optional)

You can configure metastores for Azure, AWS, or both:

#### Azure Metastore Configuration
```bash
# Add to terraform.tfvars
azure_storage_account = "your-azure-storage-account"
azure_storage_container = "metastore"  # Optional
azure_region = "East US"  # Optional
assign_azure_metastore = true  # Set to true to assign to workspace
azure_workspace_id = "your-azure-workspace-id"  # Required if assigning
azure_default_catalog_name = "main"  # Optional
```

#### AWS Metastore Configuration
```bash
# Add to terraform.tfvars
aws_s3_bucket = "your-aws-s3-bucket"
aws_region = "us-east-1"  # Optional
assign_aws_metastore = true  # Set to true to assign to workspace
aws_workspace_id = "your-aws-workspace-id"  # Required if assigning
aws_default_catalog_name = "main"  # Optional
```

#### Delta Sharing Configuration
```bash
# Add to terraform.tfvars
enable_delta_sharing = true  # Optional, enables data sharing
```

### 5. Initialize Terraform

```bash
cd terraform
terraform init
```

### 6. Deploy Infrastructure

**Using the Deploy Script**
```bash
# Plan dev deployment
./deploy.sh DEFAULT plan

# Deploy to dev
./deploy.sh DEFAULT apply

# Plan prod deployment
./deploy.sh prod plan

# Deploy to prod
./deploy.sh prod apply

# Destroy infrastructure
./deploy.sh DEFAULT destroy
```

**Environment Mapping:**
- `DEFAULT` or `dev` → Dev environment (dbc-78c2ed4d-f375.cloud.databricks.com)
- `prod` → Prod environment (dbc-28f5121a-2591.cloud.databricks.com)

**Automatic Configuration:**
- **Environment**: Automatically determined from profile
- **Host**: Automatically determined from profile  
- **Access**: Service principal managed (no user permissions needed)

## Deploy Script Usage

The `deploy.sh` script provides a convenient way to deploy infrastructure with automatic environment detection.

### Script Usage
```bash
./deploy.sh [profile] [action]
```

### Parameters
- **profile**: Databricks profile to use (`DEFAULT`, `dev`, or `prod`)
- **action**: Terraform action (`plan`, `apply`, or `destroy`)

### Examples

**Development Environment:**
```bash
# Plan dev deployment
./deploy.sh DEFAULT plan

# Deploy to dev
./deploy.sh DEFAULT apply

# Destroy dev infrastructure
./deploy.sh DEFAULT destroy
```

**Production Environment:**
```bash
# Plan prod deployment
./deploy.sh prod plan

# Deploy to prod
./deploy.sh prod apply

# Destroy prod infrastructure
./deploy.sh prod destroy
```

### What the Script Does
1. **Sets DATABRICKS_CONFIG_PROFILE** automatically
2. **Checks for terraform.tfvars** file
3. **Validates Databricks credentials**
4. **Initializes Terraform** if needed
5. **Runs terraform commands** with smart environment detection
6. **Shows outputs** after successful deployment
7. **Confirms destructive actions** before proceeding

### Script Features
- **Automatic Profile Detection**: No need to manually set environment variables
- **Safety Checks**: Confirms before applying or destroying infrastructure
- **Error Handling**: Validates prerequisites before running
- **Output Display**: Shows terraform outputs after successful deployment

## Selective Deployment with Targeting

For more granular control over what gets deployed, you can use Terraform's `-target` option to deploy only specific modules or resources.

### Available Targets

**ML Pool Module:**
```bash
# Deploy only ML pool
terraform apply -target="module.databricks.module.ml_pool" -var="databricks_profile=prod" -auto-approve
```

**Future Modules (when added):**
```bash
# Deploy only DDL module (example)
terraform apply -target="module.databricks.module.ddl" -var="databricks_profile=prod" -auto-approve

# Deploy multiple specific modules
terraform apply -target="module.databricks.module.ml_pool" -target="module.databricks.module.ddl" -var="databricks_profile=prod" -auto-approve
```

### Targeting Benefits

- **Selective Deployment**: Deploy only what you need
- **Faster Deployments**: Skip unnecessary modules
- **Reduced Risk**: Minimize changes to production
- **Shared Configuration**: Still uses root provider and environment settings

### Common Targeting Patterns

**Development Workflow:**
```bash
# Deploy only ML pool to dev
terraform apply -target="module.databricks.module.ml_pool" -var="databricks_profile=dev" -auto-approve

# Plan changes to specific module
terraform plan -target="module.databricks.module.ml_pool" -var="databricks_profile=dev"
```

**Production Workflow:**
```bash
# Deploy only ML pool to prod
terraform apply -target="module.databricks.module.ml_pool" -var="databricks_profile=prod" -auto-approve

# Deploy multiple modules to prod
terraform apply -target="module.databricks.module.ml_pool" -target="module.databricks.module.ddl" -var="databricks_profile=prod" -auto-approve
```

### Finding Available Targets

To see all available targets in your configuration:
```bash
terraform state list
```

This will show all resources and modules that can be targeted.

## Usage in DABs

Once the pool is created, you can reference it in your DABs configurations:

```yaml
# In your project's databricks.yml
job_clusters:
  - job_cluster_key: ml_cluster
    new_cluster:
      spark_version: "16.4.x-cpu-ml-scala2.12"
      instance_pool_id: "YOUR_POOL_ID_HERE"  # Get from terraform output
      data_security_mode: SINGLE_USER
      autoscale:
        min_workers: 1
        max_workers: 4
```

## Unity Catalog Usage

The Unity Catalog catalogs follow the medallion architecture pattern for data organization:

### Development Environment
```sql
-- Use development catalogs for testing
USE CATALOG dev_bronze;  -- Raw data
USE CATALOG dev_silver;  -- Cleaned data
USE CATALOG dev_gold;    -- Business-ready data
```

### Production Environment
```sql
-- Use production catalogs for live data
USE CATALOG bronze;  -- Raw data
USE CATALOG silver;  -- Cleaned data
USE CATALOG gold;    -- Business-ready data
```

### Data Flow Pattern
1. **Bronze Layer**: Raw data ingestion from external sources
2. **Silver Layer**: Data cleaning, validation, and standardization
3. **Gold Layer**: Business-ready aggregated data for analytics and reporting

### Permission Model
The catalog permissions follow a security model that balances development flexibility with production data protection:

#### Development Catalogs (dev_bronze, dev_silver, dev_gold)
- **All User Groups**: `ALL_PRIVILEGES` - Full read/write access for development and testing
- **Dev Service Principal Group**: `ALL_PRIVILEGES` - Full ownership for dev service principals
- **Purpose**: Allow developers and dev service principals to experiment and test without restrictions

#### Production Catalogs (bronze, silver, gold)
- **Production Service Principal Group**: `ALL_PRIVILEGES` - Full ownership for production data pipelines
- **All User Groups**: Granular privileges - `USE_CATALOG`, `USE_SCHEMA`, `SELECT`, `EXECUTE`, `REFRESH`, `READ_VOLUME`
- **Purpose**: Protect production data integrity while allowing specific read operations for business users

#### Permission Levels Reference
- `ALL_PRIVILEGES`: Full access (create, read, write, delete, manage)
- `USE_CATALOG`: Access to use the catalog
- `USE_SCHEMA`: Access to use schemas within the catalog
- `SELECT`: Read data from tables
- `EXECUTE`: Execute functions and procedures
- `REFRESH`: Refresh materialized views and tables
- `READ_VOLUME`: Read data from volumes

## Terraform Outputs

After deployment, you can access the following outputs:

### Pool Outputs
- `ml_pool_id` - ID of the ML instance pool
- `ml_pool_name` - Name of the ML instance pool
- `ml_pool_url` - URL to view the ML instance pool
- `standard_pool_id` - ID of the standard instance pool
- `standard_pool_name` - Name of the standard instance pool
- `standard_pool_url` - URL to view the standard instance pool

### User Outputs
- `test_user_id` - ID of the test user
- `test_user_name` - Username of the test user
- `qa_user_id` - ID of the QA user
- `qa_user_name` - Username of the QA user
- `prod_user_id` - ID of the production user
- `prod_user_name` - Username of the production user

### Group Outputs
- `dev_group_id` - ID of the dev group
- `dev_group_name` - Display name of the dev group
- `dev_admin_group_id` - ID of the dev-admin group
- `dev_admin_group_name` - Display name of the dev-admin group
- `admin_group_id` - ID of the admin group
- `admin_group_name` - Display name of the admin group

### Service Principal Group Outputs
- `prod_service_principals_group_id` - ID of the prod-service-principals group
- `prod_service_principals_group_name` - Display name of the prod-service-principals group
- `dev_service_principals_group_id` - ID of the dev-service-principals group
- `dev_service_principals_group_name` - Display name of the dev-service-principals group

### Service Principal Outputs
- `prod_service_principal_id` - ID of the production service principal
- `prod_service_principal_application_id` - Application ID of the production service principal
- `prod_service_principal_display_name` - Display name of the production service principal
- `prod_service_principal_secret_id` - ID of the production service principal secret
- `prod_service_principal_secret_value` - Secret value for production authentication (sensitive)
- `dev_service_principal_id` - ID of the development service principal
- `dev_service_principal_application_id` - Application ID of the development service principal
- `dev_service_principal_display_name` - Display name of the development service principal
- `dev_service_principal_secret_id` - ID of the development service principal secret
- `dev_service_principal_secret_value` - Secret value for development authentication (sensitive)
- `ci_cd_service_principal_id` - ID of the CI/CD service principal
- `ci_cd_service_principal_application_id` - Application ID of the CI/CD service principal
- `ci_cd_service_principal_display_name` - Display name of the CI/CD service principal
- `ci_cd_service_principal_secret_id` - ID of the CI/CD service principal secret
- `ci_cd_service_principal_secret_value` - Secret value for CI/CD authentication (sensitive)

### Catalog Outputs
- `dev_bronze_catalog_name` - Name of the dev_bronze catalog
- `dev_silver_catalog_name` - Name of the dev_silver catalog
- `dev_gold_catalog_name` - Name of the dev_gold catalog
- `bronze_catalog_name` - Name of the bronze catalog
- `silver_catalog_name` - Name of the silver catalog
- `gold_catalog_name` - Name of the gold catalog

### Catalog Permissions Outputs
- `dev_catalog_permissions_summary` - Summary of development catalog permissions
- `prod_catalog_permissions_summary` - Summary of production catalog permissions (with granular privileges)

### Metastore Outputs
- `azure_metastore_id` - ID of the Azure metastore
- `azure_metastore_name` - Name of the Azure metastore
- `azure_metastore_storage_root` - Storage root of the Azure metastore
- `azure_metastore_region` - Region of the Azure metastore
- `aws_metastore_id` - ID of the AWS metastore
- `aws_metastore_name` - Name of the AWS metastore
- `aws_metastore_storage_root` - Storage root of the AWS metastore
- `aws_metastore_region` - Region of the AWS metastore
- `azure_metastore_assignment_id` - ID of the Azure metastore assignment
- `aws_metastore_assignment_id` - ID of the AWS metastore assignment

## Adding New Infrastructure

### Adding New Databricks Resources

To add new Databricks resources (clusters, jobs, etc.):

**For Cluster/Pool Resources:**
1. Add the resource to `modules/databricks/clusters/{resource-type}/main.tf`
2. Add variables to `modules/databricks/clusters/{resource-type}/variables.tf`
3. Add outputs to `modules/databricks/clusters/{resource-type}/outputs.tf`
4. Reference the submodule in `modules/databricks/main.tf`
5. Pass variables from root in `main.tf`
6. Update this README

**For Other Databricks Resources:**
1. Add the resource to `modules/databricks/new-resource.tf`
2. Add variables to `modules/databricks/variables.tf`
3. Add outputs to `modules/databricks/outputs.tf`
4. Pass variables from root in `main.tf`
5. Update this README

### Adding New Services (e.g., AWS, Networking)

To add infrastructure for other services:

1. Create `modules/aws/` directory
2. Add AWS resources there
3. Reference the module in root `main.tf`
4. Add variables and outputs as needed

### Example: Adding a New Databricks Cluster

```hcl
# In modules/databricks/clusters.tf
resource "databricks_cluster" "example" {
  cluster_name = "${var.environment}-example-cluster"
  # ... other configuration
}
```

## Benefits of Modular Structure

- **Scalable**: Easy to add new services and resources
- **Reusable**: Modules can be used across environments
- **Organized**: Clear separation of concerns
- **Maintainable**: Changes are isolated to specific modules
- **Testable**: Modules can be tested independently

## Best Practices

- Use consistent tagging with `local.common_tags`
- Follow naming conventions: `{environment}-{resource-name}`
- Document all resources in this README
- Use variables for all configurable values
- Keep sensitive values in environment variables
- Use modules for reusable components

## Environment Management

- **Dev**: `terraform apply -var="environment=dev"`
- **Prod**: `terraform apply -var="environment=prod"`

Each environment gets its own resources with environment-specific naming.
