# Identity Management

This directory contains resources for managing Databricks identity and access control.

## Files

- **`users.tf`** - User resources (test, qa, prod users)
- **`groups.tf`** - Group resources and group memberships
- **`service-principals.tf`** - Service principal resources for OAuth authentication

## Resources Managed

### Users
- `test@dataplatform-data-platform.com` - Test User
- `qa@dataplatform-data-platform.com` - QA User
- `prod@dataplatform-data-platform.com` - Production User

### Groups
- `dev` - Development team group
- `dev-admin` - Development administrators group
- `admin` - Full administrators group
- `prod-service-principals` - Production service principals group
- `dev-service-principals` - Development service principals group

### Group Memberships
- Test user → dev group
- QA user → dev-admin group
- Prod user → admin group
- Production service principal → prod-service-principals group
- Development service principal → dev-service-principals group

### Service Principals
- `prod-oauth-sp` - Production OAuth service principal
- `dev-oauth-sp` - Development OAuth service principal
- `${environment}-ci-cd-sp` - CI/CD service principal

## Usage

These resources are automatically created when the Databricks module is deployed. The users and groups are used for access control throughout the platform, while service principals provide programmatic access for AWS deployments.
