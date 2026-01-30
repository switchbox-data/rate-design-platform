# EC2 Infrastructure for Rate Design Platform

This directory contains Terraform configuration for provisioning a shared EC2 instance for the rate design platform.

## Overview

The infrastructure includes:
- EC2 instance (Ubuntu 22.04) with configurable instance type
- Persistent EBS volume (mounted at `/ebs`) for user home directories and shared data
- S3 bucket mount (`s3://data.sb/` mounted at `/data.sb/`) for large data files
- IAM roles and security groups for secure access
- Automatic user account creation based on AWS IAM users

## Quick Start

### Prerequisites

1. AWS CLI installed and configured
2. Terraform installed (>= 1.0)
3. `.secrets/aws-sso-config.sh` file with AWS SSO configuration (same as other projects)

### Initial Setup (Admin)

Run once to provision the infrastructure:

```bash
just dev-setup
```

This will:
- Create the EC2 instance
- Set up the EBS volume and S3 mount
- Install system dependencies
- Install `just` on the instance

### User Login

Any authorized user can connect:

```bash
just dev-login
```

This will:
- Authenticate via AWS SSO
- Create your Linux user account (if first time)
- Clone the repository to `~/rate-design-platform/`
- Install Python dependencies with `uv sync`
- Open an interactive SSH session

## Directory Structure

On the EC2 instance:

- `/ebs/` - EBS volume (persistent)
  - `/ebs/home/username/` - User home directories (persistent)
  - `/ebs/shared/` - Shared data accessible to all users
- `/data.sb/` - S3 bucket mount (`s3://data.sb/`)
- User repos: `~/rate-design-platform/` (in user's home directory on EBS)

## Configuration

Edit `variables.tf` to customize:
- `instance_type` - EC2 instance type (default: `c5.2xlarge`)
- `ebs_volume_size` - EBS volume size in GB (default: 500)
- `aws_region` - AWS region (default: `us-west-2`)
- `s3_bucket_name` - S3 bucket to mount (default: `data.sb`)

## Changing Instance Type

To change the instance type:

1. Update `instance_type` in `variables.tf` or pass via command line:
   ```bash
   terraform apply -var="instance_type=c5.4xlarge"
   ```

2. Terraform will automatically:
   - Stop the instance
   - Change the instance type
   - Start the new instance
   - Reattach the EBS volume
   - All user data in `/ebs/home/` persists automatically

## EBS Volume Resizing

To increase the EBS volume size:

1. Update `ebs_volume_size` in `variables.tf`
2. Run `terraform apply`
3. The user-data script automatically detects the larger volume and runs `resize2fs`

**Note:** EBS volumes cannot be decreased in size.

## Authorization

Users need the following AWS IAM permissions:
- `ec2-instance-connect:SendSSHPublicKey`
- `ec2:DescribeInstances`

To grant access, add these permissions to the user's IAM role/user. Their Linux account will be created automatically on first login.

## Files

- `main.tf` - Main infrastructure (EC2, EBS, IAM, security groups)
- `variables.tf` - Configuration variables
- `outputs.tf` - Terraform outputs (instance ID, IPs, etc.)
- `user-data.sh` - Bootstrap script that runs on instance startup
- `.gitignore` - Ignores Terraform state files

## Troubleshooting

### Instance not accessible

- Check security groups allow SSH from your IP/VPC
- Verify AWS SSO login: `just aws`
- Check instance status: `aws ec2 describe-instances --instance-ids <instance-id>`

### S3 mount not working

- Verify IAM instance profile has S3 permissions
- Check S3 bucket name is correct in `variables.tf`
- Check logs: `sudo journalctl -u user-data` or `/var/log/user-data.log`

### User account issues

- Verify AWS IAM username is valid
- Check `/ebs/home/` directory permissions
- User accounts are created automatically on first login
