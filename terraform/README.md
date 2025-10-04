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
        ├── main.tf           # Databricks module config
        ├── variables.tf      # Module variables
        ├── outputs.tf        # Module outputs
        └── clusters/
            └── pools/
                ├── main.tf   # ML pool resource + permissions
                ├── variables.tf # Pool-specific variables
                └── outputs.tf  # Pool-specific outputs
```

### Key Files

- **`main.tf`** - Root configuration that orchestrates modules
- **`variables.tf`** - Root-level input variables
- **`outputs.tf`** - Root-level outputs
- **`modules/databricks/`** - Databricks infrastructure module
- **`terraform.tfvars.example`** - Example variables file

## Resources Managed

### ML Instance Pool
- **Runtime**: 16.4 LTS ML (includes Apache Spark 3.5.2, Scala 2.12)
- **Node Type**: i3.xlarge (configurable)
- **Auto-scaling**: 0-10 instances (configurable)
- **Auto-termination**: 15 minutes (configurable)
- **Disk**: 100GB gp3 EBS volume
- **Access**: Service principal managed (no user permissions needed)

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

### 3. Initialize Terraform

```bash
cd terraform
terraform init
```

### 4. Deploy Infrastructure

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
