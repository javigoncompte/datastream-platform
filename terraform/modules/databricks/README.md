# Databricks Module

This module manages all Databricks-related infrastructure for the DataPlatform, including compute resources, identity management, and Unity Catalog.

## Structure

```
modules/databricks/
├── main.tf                    # Main module orchestrator
├── variables.tf              # Module variables
├── outputs.tf                # Module outputs
├── clusters/                 # Compute resources
│   └── pools/                # Instance pools
├── identity/                 # Identity management
│   ├── users.tf              # User resources
│   ├── groups.tf             # Group resources
│   └── service-principals.tf # Service principal resources
└── catalog/                  # Unity Catalog management
    ├── catalogs.tf           # Catalog resources
    └── permissions.tf        # Catalog permissions
```

## Resources Managed

### Compute Resources
- **ML Instance Pool**: 16.4 LTS ML runtime for machine learning workloads
- **Standard Instance Pool**: 16.4 LTS Photon runtime for general workloads

### Identity Management
- **Users**: test, qa, prod users with email-based usernames
- **Groups**: dev, dev-admin, admin groups with hierarchical permissions
- **Service Principals**: OAuth and CI/CD service principals for AWS deployments

### Unity Catalog
- **Development Catalogs**: dev_bronze, dev_silver, dev_gold
- **Production Catalogs**: bronze, silver, gold
- **Permissions**: Environment-specific access control

## Key Features

- **Environment Separation**: Clear separation between dev and prod resources
- **Medallion Architecture**: Bronze/Silver/Gold data layers
- **Security Model**: Role-based access control with service principal integration
- **AWS Integration**: OAuth service principals for programmatic access
- **Scalable Design**: Modular structure for easy expansion

## Usage

This module is used by the root Terraform configuration:

```hcl
module "databricks" {
  source = "./modules/databricks"
  
  databricks_host = local.current_config.host
  environment = local.current_config.environment
  project_name = var.project_name
  service_principal_name = var.service_principal_name
  
  # Service Principal OAuth Configuration
  service_principal_application_id = var.service_principal_application_id
  ci_cd_service_principal_application_id = var.ci_cd_service_principal_application_id
}
```

## Deployment

Use the deployment script from the root directory:

```bash
# Deploy to dev
./deploy.sh DEFAULT apply

# Deploy to prod
./deploy.sh prod apply
```

## Outputs

The module provides comprehensive outputs for:
- Pool IDs and URLs
- User and group information
- Service principal credentials
- Catalog names and permission summaries

See `outputs.tf` for the complete list of available outputs.
