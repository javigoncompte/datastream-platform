#!/bin/bash

# Script to deploy DataPlatform Infrastructure
# Usage: ./deploy.sh [profile] [action] [target]
# Example: ./deploy.sh DEFAULT plan
# Example: ./deploy.sh prod apply
# Example: ./deploy.sh prod apply "module.databricks.module.ml_pool"
# Example: ML_POOL_NODE_TYPE=rd-fleet.xlarge ./deploy.sh DEFAULT apply
#
# Environment variables required for service principal authentication:
# - DATABRICKS_CLIENT_ID
# - DATABRICKS_CLIENT_SECRET
# - DATABRICKS_HOST (optional, auto-detected from profile)

set -e

PROFILE=${1:-DEFAULT}
ACTION=${2:-plan}
TARGET=${3:-""}

echo "DataPlatform Infrastructure Deployment"
echo "================================================"
echo "Profile: $PROFILE"
echo "Action: $ACTION"
echo "Target: ${TARGET:-"all modules"}"
echo ""

# Set the Databricks profile
export DATABRICKS_CONFIG_PROFILE=$PROFILE

# Check for service principal credentials
if [ -z "$DATABRICKS_CLIENT_ID" ] || [ -z "$DATABRICKS_CLIENT_SECRET" ]; then
    echo "Error: Service principal credentials not found!"
    echo "Required environment variables:"
    echo "  - DATABRICKS_CLIENT_ID"
    echo "  - DATABRICKS_CLIENT_SECRET"
    echo ""
    echo "Example:"
    echo "  export DATABRICKS_CLIENT_ID=your-client-id"
    echo "  export DATABRICKS_CLIENT_SECRET=your-client-secret"
    echo ""
    echo "To create a service principal:"
    echo "  1. Go to Databricks Admin Console > Identity and Access > Service Principals"
    echo "  2. Create a new service principal"
    echo "  3. Generate a client secret"
    echo "  4. Grant necessary permissions"
    exit 1
fi

echo "Service principal credentials found. Proceeding with deployment..."

# Check if terraform is installed
if ! command -v terraform &> /dev/null; then
    echo "Error: Terraform is not installed!"
    echo "Please install Terraform: https://www.terraform.io/downloads"
    exit 1
fi

# Initialize Terraform if needed
if [ ! -d ".terraform" ]; then
    echo "Initializing Terraform..."
    terraform init
fi

# Build terraform command
TF_CMD="terraform $ACTION -var=\"databricks_profile=$PROFILE\""

# Add ML pool node type override if specified
if [ -n "$ML_POOL_NODE_TYPE" ]; then
    TF_CMD="$TF_CMD -var=\"ml_pool_node_type=$ML_POOL_NODE_TYPE\""
fi

# Add target if specified
if [ -n "$TARGET" ]; then
    TF_CMD="$TF_CMD -target=\"$TARGET\""
fi

# Add auto-approve for apply/destroy actions
if [ "$ACTION" = "apply" ] || [ "$ACTION" = "destroy" ]; then
    TF_CMD="$TF_CMD -auto-approve"
fi

# Run Terraform command
echo "Running: $TF_CMD"
echo ""

# Execute the command
eval $TF_CMD

# Show outputs for successful apply
if [ "$ACTION" = "apply" ]; then
    echo ""
    echo "Infrastructure deployment completed!"
    echo ""
    echo "Outputs:"
    terraform output
fi
