# EC2 Infrastructure for Rate Design Platform

This directory contains Terraform configuration for provisioning a shared EC2 instance for the rate design platform.

## Overview

The infrastructure includes:

- EC2 instance (Ubuntu 22.04) with configurable instance type
- Persistent EBS volume (mounted at `/ebs`) for user home directories and shared data
- S3 bucket mount (`s3://data.sb/` mounted at `/data.sb/`) for large data files
- IAM roles and security groups for secure access
- Automatic user account creation based on AWS IAM identity
- CloudWatch alarm to auto-stop the instance after sustained idle CPU

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
- Install system dependencies (including **Quarto** for manuscript-style reports)
- Install `just`, `uv`, `gh`, and AWS CLI on the instance via SSM (idempotent)
- Create a CloudWatch alarm to auto-stop the instance after idle period

### User Login

Any authorized user can connect:

```bash
just dev-login
```

This will:

- Authenticate via AWS SSO
- Auto-start the instance if it was stopped (e.g. by the idle alarm)
- Create your Linux user account (if first time)
- Clone the repository to `~/rate-design-platform/`
- Install Python dependencies with `uv sync`
- Open an interactive SSH session

### Starting a Stopped Instance

If you just want to wake the instance without a full login (e.g. before a batch CAIRO run):

```bash
just dev-start
```

Note: `just dev-login` also auto-starts stopped instances, so `dev-start` is only needed when you want to wake the instance without connecting interactively.

## Directory Structure

On the EC2 instance:

- `/ebs/` - EBS volume (persistent across instance replacements)
  - `/ebs/home/username/` - User home directories (persistent)
  - `/ebs/shared/` - Shared data accessible to all users
  - `/ebs/buildstock/` - Shared buildstock data
  - `/ebs/tmp/` - Temporary files (TMPDIR)
- `/data.sb/` - S3 bucket mount (`s3://data.sb/`)
- User repos: `~/rate-design-platform/` (in user's home directory on EBS)

## Configuration

Edit `variables.tf` to customize:

- `instance_type` - EC2 instance type (default: `m7i.xlarge`)
- `ebs_volume_size` - EBS volume size in GB (default: 500)
- `aws_region` - AWS region (default: `us-west-2`)
- `s3_bucket_name` - S3 bucket to mount (default: `data.sb`)
- `idle_cpu_threshold` - CPU % below which the instance is considered idle (default: 5)
- `idle_minutes` - Minutes of idle CPU before auto-stop (default: 120)

## Auto-Stop on Idle

A CloudWatch alarm monitors CPU utilization. When CPU stays below `idle_cpu_threshold` (default 5%) for `idle_minutes` (default 120 min / 2 hours), the instance is automatically stopped to save costs.

**This does not affect long-running jobs.** CAIRO runs, data processing, and other compute-heavy tasks keep CPU well above the threshold and prevent the alarm from firing. The alarm only triggers after sustained inactivity (e.g. everyone has disconnected and nothing is running).

To tune the idle behavior, override the variables in a `terraform.tfvars` file or pass `-var` to `terraform apply`:

```bash
# Stop after 3 hours instead of 2
terraform apply -var="idle_minutes=180"
```

When the instance is stopped:

- All data on the EBS volume (`/ebs/home/`, repos, configs) is preserved
- Running `just dev-login` will auto-start the instance (~30-60s startup)
- Or run `just dev-start` to wake it without connecting

## Changing Instance Type

To change the instance type:

1. Update `instance_type` in `variables.tf` or pass via command line:
   ```bash
   terraform apply -var="instance_type=m7i.2xlarge"
   ```

2. Terraform will **destroy the old instance and create a new one** with the new type. The persistent EBS volume is re-attached automatically.

**Important:** This terminates the running instance. Ensure no users have active processes before changing instance type. All data in `/ebs/home/` persists — only the root volume (OS, packages) is recreated. Users will need to run `just dev-login` to reconnect.

## EBS Volume Resizing

To increase the EBS volume size:

1. Update `ebs_volume_size` in `variables.tf`
2. Run `terraform apply`
3. The user-data script automatically detects the larger volume and runs `resize2fs`

**Note:** EBS volumes cannot be decreased in size.

## Authorization

Users need AWS SSO access to the account. Their Linux account is created automatically on first `just dev-login` based on their IAM identity.

## Files

- `main.tf` - Main infrastructure (EC2, EBS, IAM, security groups, CloudWatch alarm)
- `variables.tf` - Configuration variables
- `outputs.tf` - Terraform outputs (instance ID, IPs, etc.)
- `user-data.sh` - Bootstrap script that runs on instance startup
- `dev-setup.sh` - Admin script to provision/update the instance
- `dev-login.sh` - User login script (auto-starts stopped instances)
- `dev-start.sh` - Start a stopped instance without logging in
- `dev-teardown.sh` - Destroy instance but preserve data volume
- `dev-teardown-all.sh` - Destroy everything including data volume
- `create-volume.sh` - One-time EBS volume creation
- `first-login.sh` - First-login setup (gh auth + uv sync)
- `install-terraform.sh` - Terraform installer (dependency)

## Troubleshooting

### Instance not accessible

- Check if instance is stopped: `aws ec2 describe-instances --filters "Name=tag:Project,Values=rate-design-platform" --query 'Reservations[].Instances[].[InstanceId,State.Name]'`
- If stopped, run `just dev-start` or `just dev-login` (auto-starts)
- Verify AWS SSO login: `just aws`
- Check instance status: `aws ec2 describe-instances --instance-ids <instance-id>`

### S3 mount not working

- Verify IAM instance profile has S3 permissions
- Check S3 bucket name is correct in `variables.tf`
- Check logs: `sudo journalctl -u user-data` or `/var/log/user-data.log`

### User account issues

- Verify AWS identity: `aws sts get-caller-identity`
- Check `/ebs/home/` directory permissions
- User accounts are created automatically on first login
