#!/bin/bash

# Standard Pool Deployment Script
# This script deploys the standard instance pool to Databricks

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if required environment variables are set
check_environment() {
    print_status "Checking environment variables..."
    
    if [ -z "$DATABRICKS_CONFIG_PROFILE" ]; then
        print_warning "DATABRICKS_CONFIG_PROFILE not set, using default profile"
    else
        print_status "Using Databricks profile: $DATABRICKS_CONFIG_PROFILE"
    fi
}

# Validate Terraform configuration
validate_terraform() {
    print_status "Validating Terraform configuration..."
    terraform init
    terraform validate
    print_success "Terraform configuration is valid"
}

# Plan the deployment
plan_deployment() {
    print_status "Planning deployment..."
    terraform plan -out=tfplan
    print_success "Deployment plan created"
}

# Apply the deployment
apply_deployment() {
    print_status "Applying deployment..."
    terraform apply tfplan
    print_success "Standard pool deployed successfully"
}

# Show deployment summary
show_summary() {
    print_status "Deployment Summary:"
    echo "=================="
    terraform output
}

# Main deployment function
main() {
    print_status "Starting Standard Pool deployment..."
    
    check_environment
    validate_terraform
    plan_deployment
    
    # Ask for confirmation before applying
    echo
    read -p "Do you want to apply this deployment? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        apply_deployment
        show_summary
        print_success "Standard Pool deployment completed!"
    else
        print_warning "Deployment cancelled by user"
        exit 0
    fi
}

# Run main function
main "$@"
